// Package ingest 实现文档写路径：把调用方提交的文档嵌入为向量并写入 Qdrant。
//
// Phase 2 为同步 ingest（请求内完成 embed + upsert），不含分块与异步管道
// （Phase 3 引入 parent-child chunking 时再演进为异步）。幂等性由两层保证：
//   - 确定性 point ID：同 ID 重复 upsert 是覆盖而非追加（与 runtime indexer 一致）；
//   - content_hash：同 ID 同内容的重复提交跳过重嵌入，保护 embedding 网关配额。
//
// 与读路径的降级契约**刻意相反**：读路径任何内部错误都降级为 200+空结果
// （记忆只是增强项）；写路径的错误必须显式上抛（调用方据此决定重试与
// 进度标记），若也静默 200，调用方会把"未写入"误当"已写入"——
// 那是记忆静默丢失的唯一通道。因此写端点返回 4xx/5xx。
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/embed"
	"github.com/daqiaoliang-coder/rag-service/internal/sparse"
	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

// 哨兵错误：httpapi 据此映射状态码，其余一律视为后端故障（503）。
var (
	// ErrUnknownCollection 表示目标集合不在 rag-api 管理范围内。
	// Phase 2 只服务两个已知集合（memory / docs），任意建集合会让
	// 向量布局（命名/未命名）失去约束，写出不可召回的数据。
	ErrUnknownCollection = errors.New("ingest: unknown collection")
	// ErrInvalidDocument 表示文档校验失败（空 content / 非法 ID / 空租户）。
	ErrInvalidDocument = errors.New("ingest: invalid document")
	// ErrTooManyDocs 表示单请求文档数超上限。
	ErrTooManyDocs = errors.New("ingest: too many documents in one request")
)

// Document 是提交方视角的文档模型。ID 为十进制 uint64 字符串——
// memory 集合沿用 runtime 的 vector.PointID 派生值（影子对比的前提），
// docs 集合由调用方按 hash(tenant, doc, seq) 派生。
type Document struct {
	ID       string           `json:"id"`
	Content  string           `json:"content"`
	Metadata DocumentMetadata `json:"metadata"`
}

// DocumentMetadata 与 runtime vector.Point 的 payload 契约一一对应。
type DocumentMetadata struct {
	TenantID  string    `json:"tenant_id"` // 必填：写入侧的 fail-closed，无租户文档对所有检索不可见
	ThreadID  string    `json:"thread_id"`
	RunID     string    `json:"run_id"`
	NodeID    string    `json:"node_id"`
	Role      string    `json:"role"`
	CreatedAt time.Time `json:"created_at"`
}

// UpsertResponse 汇总一次提交的结果。
type UpsertResponse struct {
	Indexed int `json:"indexed"` // 本次实际嵌入并写入
	Skipped int `json:"skipped"` // content_hash 相同，幂等跳过（未调用 embedding）
}

// Service 是写路径管道。依赖以接口注入，单测用 fake 替换。
type Service struct {
	Embedder embed.Embedder
	Sparse   sparse.Encoder // docs 布局用；memory 布局不需要
	Store    vector.Writer

	// MemoryCollection 是默认向量布局（未命名 dense）的集合，与 runtime
	// 共用（默认 agent_memory）。DocsCollection 是命名向量布局
	// （dense+sparse）的集合（默认 rag_documents）。
	MemoryCollection string
	DocsCollection   string
	// MaxDocsPerRequest 限制单请求文档数：同步 ingest 在请求内完成整批
	// embedding，批量无上限会把网关配额与请求超时同时打穿。
	// 网关限速的节奏（批 16 + 500ms）由调用方（memory-indexer）控制，
	// 它自身已携带 BatchSize/BatchDelay 配置。
	MaxDocsPerRequest int
}

// DefaultDocsCollection 与 config 的缺省值保持一致。
const DefaultDocsCollection = "rag_documents"

func (s *Service) memoryCollection() string {
	if s != nil && s.MemoryCollection != "" {
		return s.MemoryCollection
	}
	return "agent_memory"
}

func (s *Service) docsCollection() string {
	if s != nil && s.DocsCollection != "" {
		return s.DocsCollection
	}
	return DefaultDocsCollection
}

// Upsert 校验并写入一批文档。返回前所有点已确认落库（Qdrant wait=true）。
func (s *Service) Upsert(ctx context.Context, collection string, docs []Document) (UpsertResponse, error) {
	if s == nil || s.Store == nil || s.Embedder == nil {
		return UpsertResponse{}, fmt.Errorf("ingest: service not configured (store/embedder required)")
	}
	hybrid, err := s.layout(collection)
	if err != nil {
		return UpsertResponse{}, err
	}
	if len(docs) == 0 {
		return UpsertResponse{}, fmt.Errorf("%w: empty documents", ErrInvalidDocument)
	}
	max := s.MaxDocsPerRequest
	if max <= 0 {
		max = 64
	}
	if len(docs) > max {
		return UpsertResponse{}, fmt.Errorf("%w: %d > %d", ErrTooManyDocs, len(docs), max)
	}

	// 校验 + 解析 ID + 计算 content_hash。
	type parsed struct {
		doc  Document
		id   uint64
		hash string
	}
	items := make([]parsed, 0, len(docs))
	for _, d := range docs {
		if d.Content == "" {
			return UpsertResponse{}, fmt.Errorf("%w: doc %q has empty content", ErrInvalidDocument, d.ID)
		}
		if d.Metadata.TenantID == "" {
			return UpsertResponse{}, fmt.Errorf("%w: doc %q has empty tenant_id (fail-closed)", ErrInvalidDocument, d.ID)
		}
		id, err := strconv.ParseUint(d.ID, 10, 64)
		if err != nil {
			return UpsertResponse{}, fmt.Errorf("%w: doc id %q is not a decimal uint64", ErrInvalidDocument, d.ID)
		}
		sum := sha256.Sum256([]byte(d.Content))
		items = append(items, parsed{doc: d, id: id, hash: hex.EncodeToString(sum[:])})
	}

	// 幂等检查：同 ID 且同 content_hash 的文档跳过重嵌入。
	// 崩溃恢复后的重扫（mark 未及写入）会反复提交同一批文档，
	// 没有这一步就会反复消耗 embedding 配额。
	ids := make([]uint64, 0, len(items))
	for _, it := range items {
		ids = append(ids, it.id)
	}
	existing, err := s.Store.Retrieve(ctx, collection, ids)
	if err != nil {
		return UpsertResponse{}, fmt.Errorf("ingest: idempotency check: %w", err)
	}
	pending := make([]parsed, 0, len(items))
	for _, it := range items {
		if old, ok := existing[it.id]; ok && old.ContentHash == it.hash {
			continue
		}
		pending = append(pending, it)
	}
	if len(pending) == 0 {
		return UpsertResponse{Skipped: len(items)}, nil
	}

	// 批量嵌入（一次调用摊薄网关往返）。
	texts := make([]string, 0, len(pending))
	for _, it := range pending {
		texts = append(texts, it.doc.Content)
	}
	vecs, err := s.Embedder.Embed(ctx, texts)
	if err != nil {
		return UpsertResponse{}, fmt.Errorf("ingest: embed %d docs: %w", len(texts), err)
	}
	if len(vecs) != len(texts) {
		return UpsertResponse{}, fmt.Errorf("ingest: embed returned %d vectors for %d texts", len(vecs), len(texts))
	}

	// 组装点。docs 布局同时编码稀疏向量；memory 布局保持 dense-only，
	// 与 runtime 直连写入的点结构完全一致（影子对比的物理前提）。
	points := make([]vector.DocPoint, 0, len(pending))
	for i, it := range pending {
		if len(vecs[i]) == 0 {
			return UpsertResponse{}, fmt.Errorf("ingest: empty vector for doc %q", it.doc.ID)
		}
		p := vector.DocPoint{
			ID:          it.id,
			Dense:       vecs[i],
			TenantID:    it.doc.Metadata.TenantID,
			ThreadID:    it.doc.Metadata.ThreadID,
			RunID:       it.doc.Metadata.RunID,
			NodeID:      it.doc.Metadata.NodeID,
			Role:        it.doc.Metadata.Role,
			Text:        it.doc.Content,
			CreatedAt:   it.doc.Metadata.CreatedAt,
			ContentHash: it.hash,
		}
		if hybrid && s.Sparse != nil {
			sv := s.Sparse.Encode(it.doc.Content)
			p.Sparse = &sv
		}
		points = append(points, p)
	}
	if hybrid {
		err = s.Store.UpsertHybrid(ctx, collection, points)
	} else {
		err = s.Store.UpsertDefault(ctx, collection, points)
	}
	if err != nil {
		return UpsertResponse{}, fmt.Errorf("ingest: upsert %d points: %w", len(points), err)
	}
	return UpsertResponse{Indexed: len(points), Skipped: len(items) - len(points)}, nil
}

// Delete 删除一个文档点，幂等。返回是否实际删除（存在→true）。
func (s *Service) Delete(ctx context.Context, collection string, id uint64) (bool, error) {
	if _, err := s.layout(collection); err != nil {
		return false, err
	}
	if s == nil || s.Store == nil {
		return false, fmt.Errorf("ingest: service not configured")
	}
	existing, err := s.Store.Retrieve(ctx, collection, []uint64{id})
	if err != nil {
		return false, fmt.Errorf("ingest: check before delete: %w", err)
	}
	if len(existing) == 0 {
		return false, nil
	}
	if err := s.Store.Delete(ctx, collection, id); err != nil {
		return false, fmt.Errorf("ingest: delete: %w", err)
	}
	return true, nil
}

// Get 按 ID 取回文档状态（GET /documents/{id} 的管道实现）。
func (s *Service) Get(ctx context.Context, collection string, id uint64) (vector.RetrievedDoc, bool, error) {
	if _, err := s.layout(collection); err != nil {
		return vector.RetrievedDoc{}, false, err
	}
	if s == nil || s.Store == nil {
		return vector.RetrievedDoc{}, false, fmt.Errorf("ingest: service not configured")
	}
	docs, err := s.Store.Retrieve(ctx, collection, []uint64{id})
	if err != nil {
		return vector.RetrievedDoc{}, false, fmt.Errorf("ingest: retrieve: %w", err)
	}
	d, ok := docs[id]
	return d, ok, nil
}

// layout 判定集合的向量布局：docs 集合 → 命名 dense+sparse，memory 集合
// → 未命名默认向量。未知集合拒绝。
func (s *Service) layout(collection string) (hybrid bool, err error) {
	if s == nil {
		return false, fmt.Errorf("ingest: service not configured")
	}
	switch collection {
	case s.docsCollection():
		return true, nil
	case s.memoryCollection():
		return false, nil
	default:
		return false, fmt.Errorf("%w: %q (managed: %q, %q)",
			ErrUnknownCollection, collection, s.memoryCollection(), s.docsCollection())
	}
}
