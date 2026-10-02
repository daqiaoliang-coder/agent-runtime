// Package ingest 实现文档写路径：把调用方提交的文档嵌入为向量并写入 Qdrant。
//
// 同步 ingest（请求内完成 embed + upsert），无异步管道。两条布局管道：
//   - memory：整文档单点（Phase 2 原逻辑，字节级不变——影子写入对比的契约对象）；
//   - docs：parent-child 分块（Phase 3），child 命中检索、parent 作为返回内容，
//     可选 LLM 上下文增强（contextual retrieval）。
//
// 幂等性由两层保证：
//   - 确定性 point ID：memory 用 runtime 的 PointID 派生；docs 子块用
//     hash(tenant|docID|seq) 派生——同 ID 重复 upsert 是覆盖而非追加；
//   - content_hash：memory 为 SHA256(content)；docs 为文档级
//     SHA256(ctxVersion + "\x00" + content)，锚定在 child0 上。
//     同 hash 的重复提交跳过重嵌入，保护 embedding 网关配额。
//
// 与读路径的降级契约**刻意相反**：读路径任何内部错误都降级为 200+空结果
// （记忆只是增强项）；写路径的错误必须显式上抛（调用方据此决定重试与
// 进度标记），若也静默 200，调用方会把"未写入"误当"已写入"——
// 那是记忆静默丢失的唯一通道。因此写端点返回 4xx/5xx。
// 上下文增强同理：增强失败 = 写入失败，绝不静默降级写入无前缀版本
// （那会被 content_hash 跳过，成为永久性的静默质量损失）。
package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/chunk"
	"github.com/daqiaoliang-coder/rag-service/internal/embed"
	"github.com/daqiaoliang-coder/rag-service/internal/sparse"
	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

// 哨兵错误：httpapi 据此映射状态码，其余一律视为后端故障（503）。
var (
	// ErrUnknownCollection 表示目标集合不在 rag-api 管理范围内。
	// 只服务两个已知集合（memory / docs），任意建集合会让
	// 向量布局（命名/未命名）失去约束，写出不可召回的数据。
	ErrUnknownCollection = errors.New("ingest: unknown collection")
	// ErrInvalidDocument 表示文档校验失败（空 content / 非法 ID / 空租户）。
	ErrInvalidDocument = errors.New("ingest: invalid document")
	// ErrTooManyDocs 表示单请求文档数超上限。
	ErrTooManyDocs = errors.New("ingest: too many documents in one request")
	// ErrTooManyChunks 表示单请求 docs 子块总数超上限（同步管道的时长护栏）。
	ErrTooManyChunks = errors.New("ingest: too many chunks in one request")
	// ErrTenantRequired 表示 docs 集合的端点缺少 tenant_id：子块点 ID 由
	// (tenant, docID, seq) 派生，URL 只携带 doc ID，缺租户无法定位子块。
	ErrTenantRequired = errors.New("ingest: tenant_id is required for docs collection")
)

// Document 是提交方视角的文档模型。ID 的约束按集合布局区分：
// memory 集合必须是十进制 uint64 字符串（runtime 的 vector.PointID 派生值，
// 影子对比的前提）；docs 集合是任意非空逻辑标识（如 "faq-deploy"），
// 子块点 ID 由服务内部按 hash(tenant|docID|seq) 派生。
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
	Indexed int `json:"indexed"` // 本次实际嵌入并写入的文档数
	Skipped int `json:"skipped"` // content_hash 相同，幂等跳过的文档数（未调用 embedding）
	Chunks  int `json:"chunks"`  // docs 布局：本次写入的子块点数（memory 恒为 0）
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

	// ChunkConfig 是 docs 路径的 parent-child 分块参数（零值回退默认）。
	ChunkConfig chunk.Config
	// Enhancer 是 docs 路径的上下文摘要增强器；nil 表示关闭
	// （写路径零 LLM 依赖）。非 nil 时增强失败 = 整批写入失败。
	Enhancer Enhancer
	// EnhanceConcurrency 是子块增强的并发信号量（缺省 4）：
	// 单请求上百子块，串行 LLM 调用会把请求时长推到分钟级。
	EnhanceConcurrency int
	// CtxVersion 是增强器标识（通常为 LLM 模型名），参与 docs 级
	// content_hash 派生：增强开关或模型变更后，同内容的 hash 随之改变，
	// 从而强制重嵌（见 docsCtxVersion）。
	CtxVersion string
	// MaxChunksPerRequest 限制单请求 docs 子块总数（缺省 256）。
	// 同步管道的时长护栏：子块数直接决定 embedding 批数与 LLM 增强次数，
	// 无上限则请求时长不可控。
	MaxChunksPerRequest int
}

// docs 路径的子批嵌入大小与缺省并发/块数上限。
const (
	defaultChunkEmbedBatch   = 64
	defaultEnhanceConcurrency = 4
	defaultMaxChunks         = 256
)

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
// 两条布局管道在此分流：docs → upsertDocs（分块/增强/子块点），
// memory → upsertMemory（Phase 2 原逻辑，字节级不变——影子写入对比契约）。
func (s *Service) Upsert(ctx context.Context, collection string, docs []Document) (UpsertResponse, error) {
	if s == nil || s.Store == nil || s.Embedder == nil {
		return UpsertResponse{}, fmt.Errorf("ingest: service not configured (store/embedder required)")
	}
	hybrid, err := s.layout(collection)
	if err != nil {
		return UpsertResponse{}, err
	}
	if hybrid {
		return s.upsertDocs(ctx, collection, docs)
	}
	return s.upsertMemory(ctx, collection, docs)
}

// upsertMemory 是 memory 集合的写入路径：整文档单点。
// ⚠️ 与 Phase 2 的 Upsert 逻辑逐行一致，不随 Phase 3 演进——
// INDEXER_BACKEND=shadow 的写入等价性验收以它为契约对象。
func (s *Service) upsertMemory(ctx context.Context, collection string, docs []Document) (UpsertResponse, error) {
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

	// 组装点。memory 布局保持 dense-only，与 runtime 直连写入的点结构
	// 完全一致（影子对比的物理前提）：不带稀疏向量、不带 docs 专属键。
	points := make([]vector.DocPoint, 0, len(pending))
	for i, it := range pending {
		if len(vecs[i]) == 0 {
			return UpsertResponse{}, fmt.Errorf("ingest: empty vector for doc %q", it.doc.ID)
		}
		points = append(points, vector.DocPoint{
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
		})
	}
	if err := s.Store.UpsertDefault(ctx, collection, points); err != nil {
		return UpsertResponse{}, fmt.Errorf("ingest: upsert %d points: %w", len(points), err)
	}
	return UpsertResponse{Indexed: len(points), Skipped: len(items) - len(points)}, nil
}

// docPlan 是 docs 写路径的中间产物：一篇文档的分块结果与幂等锚点。
type docPlan struct {
	doc    Document
	chunks []chunk.Chunk
	hash   string // 文档级 content_hash（含 ctxVersion），全部子块共享
	child0 uint64 // seq=0 子块的点 ID，幂等检查与 Get/Delete 的定位锚
}

// upsertDocs 是 docs 集合的写入路径：parent-child 分块 + 可选上下文增强。
// 流程：校验 → 分块 → child0 幂等检查 → 增强 → 嵌入 → 清孤儿 → 写入。
// 关键差异（相对 memory 路径）：
//   - 点 ID 由 (tenant|docID|seq) 派生，调用方 ID 是逻辑文档标识；
//   - 幂等锚是 child0 的文档级 hash（同步单批 wait=true 写入，子块要么
//     全部落库要么全部没有，child0 hash 一致 ⇒ 整篇是当前版本）；
//   - 内容/版本变更时按 doc_id 过滤整删旧子块——新版子块数可能少于旧版，
//     按 ID 覆盖写会留孤儿。
func (s *Service) upsertDocs(ctx context.Context, collection string, docs []Document) (UpsertResponse, error) {
	if len(docs) == 0 {
		return UpsertResponse{}, fmt.Errorf("%w: empty documents", ErrInvalidDocument)
	}
	maxDocs := s.MaxDocsPerRequest
	if maxDocs <= 0 {
		maxDocs = 64
	}
	if len(docs) > maxDocs {
		return UpsertResponse{}, fmt.Errorf("%w: %d > %d", ErrTooManyDocs, len(docs), maxDocs)
	}
	// docs 路径的 ID 是逻辑文档标识（任意非空字符串）：点 ID 由服务内部
	// 派生，不要求十进制 uint64——评测语料可用 "faq-deploy" 这类可读 ID。
	for _, d := range docs {
		if d.ID == "" {
			return UpsertResponse{}, fmt.Errorf("%w: doc id is empty", ErrInvalidDocument)
		}
		if d.Content == "" {
			return UpsertResponse{}, fmt.Errorf("%w: doc %q has empty content", ErrInvalidDocument, d.ID)
		}
		if d.Metadata.TenantID == "" {
			return UpsertResponse{}, fmt.Errorf("%w: doc %q has empty tenant_id (fail-closed)", ErrInvalidDocument, d.ID)
		}
	}

	// 分块 + 文档级 hash。version 汇总所有影响子块派生的参数，
	// 任一变更都会让同内容的 hash 改变，从而跳过幂等检查强制重嵌。
	version := s.docsCtxVersion()
	plans := make([]docPlan, 0, len(docs))
	total := 0
	for _, d := range docs {
		chunks := chunk.ChunkDocument(d.Content, s.ChunkConfig)
		if len(chunks) == 0 {
			return UpsertResponse{}, fmt.Errorf("%w: doc %q produces no chunks (blank content)", ErrInvalidDocument, d.ID)
		}
		total += len(chunks)
		sum := sha256.Sum256([]byte(version + "\x00" + d.Content))
		plans = append(plans, docPlan{
			doc:    d,
			chunks: chunks,
			hash:   hex.EncodeToString(sum[:]),
			child0: childPointID(d.Metadata.TenantID, d.ID, 0),
		})
	}
	maxChunks := s.MaxChunksPerRequest
	if maxChunks <= 0 {
		maxChunks = defaultMaxChunks
	}
	if total > maxChunks {
		return UpsertResponse{}, fmt.Errorf("%w: %d > %d", ErrTooManyChunks, total, maxChunks)
	}

	// 幂等检查：同 child0 且同文档级 hash 的文档整篇跳过。
	ids := make([]uint64, 0, len(plans))
	for _, p := range plans {
		ids = append(ids, p.child0)
	}
	existing, err := s.Store.Retrieve(ctx, collection, ids)
	if err != nil {
		return UpsertResponse{}, fmt.Errorf("ingest: idempotency check: %w", err)
	}
	pending := make([]docPlan, 0, len(plans))
	skipped := 0
	for _, p := range plans {
		if old, ok := existing[p.child0]; ok && old.ContentHash == p.hash {
			skipped++
			continue
		}
		pending = append(pending, p)
	}
	if len(pending) == 0 {
		return UpsertResponse{Skipped: skipped}, nil
	}

	// 可选上下文增强：为每个子块生成 LLM 定位摘要前缀。
	// 增强失败 = 整批失败（写路径契约：静默写入无前缀版本 + hash 跳过
	// = 永久性静默质量损失，比失败重试更糟）。
	var prefixes [][]string
	if s.Enhancer != nil {
		prefixes, err = s.enhanceChunks(ctx, pending)
		if err != nil {
			return UpsertResponse{}, err
		}
	}

	// 展开子块：嵌入文本 = 摘要前缀 + 子块正文（增强开启时）。
	// 前缀同时进入 dense 与 sparse 的编码对象（见下方 sparse 注释）。
	type childRef struct {
		plan docPlan
		seq  int
		text string
	}
	children := make([]childRef, 0, total)
	for i, p := range pending {
		for j, c := range p.chunks {
			text := c.Text
			if prefixes != nil && prefixes[i][j] != "" {
				text = prefixes[i][j] + "\n" + c.Text
			}
			children = append(children, childRef{plan: p, seq: j, text: text})
		}
	}

	// 批量嵌入：子批 64。单请求子块上限 256 ⇒ 最多 4 批；
	// 一次全量调用可能超出网关单请求文本数上限。
	texts := make([]string, len(children))
	for i, c := range children {
		texts[i] = c.text
	}
	vecs := make([][]float32, 0, len(texts))
	for start := 0; start < len(texts); start += defaultChunkEmbedBatch {
		end := start + defaultChunkEmbedBatch
		if end > len(texts) {
			end = len(texts)
		}
		batch, err := s.Embedder.Embed(ctx, texts[start:end])
		if err != nil {
			return UpsertResponse{}, fmt.Errorf("ingest: embed %d chunks: %w", len(texts), err)
		}
		if len(batch) != end-start {
			return UpsertResponse{}, fmt.Errorf("ingest: embed returned %d vectors for %d texts", len(batch), end-start)
		}
		vecs = append(vecs, batch...)
	}

	// 清孤儿：child0 存在但 hash 不匹配 ⇒ 有旧版子块。放在增强与嵌入之后、
	// 写入之前：把"删除旧版后失败"的窗口压缩到只剩 upsert 一步。
	for _, p := range pending {
		if _, ok := existing[p.child0]; ok {
			if err := s.Store.DeleteByDocID(ctx, collection, p.doc.Metadata.TenantID, p.doc.ID); err != nil {
				return UpsertResponse{}, fmt.Errorf("ingest: delete stale chunks of doc %q: %w", p.doc.ID, err)
			}
		}
	}

	// 组装子块点。sparse 编码与 dense 相同的"前缀+正文"文本：两路召回
	// 必须打分同一对象，否则混合结果语义错位。parent_text 冗余存父块全文，
	// 检索命中 child 后直接返回 parent，免去二次取回。
	points := make([]vector.DocPoint, 0, len(children))
	for i, c := range children {
		if len(vecs[i]) == 0 {
			return UpsertResponse{}, fmt.Errorf("ingest: empty vector for doc %q chunk %d", c.plan.doc.ID, c.seq)
		}
		p := vector.DocPoint{
			ID:          childPointID(c.plan.doc.Metadata.TenantID, c.plan.doc.ID, c.seq),
			Dense:       vecs[i],
			TenantID:    c.plan.doc.Metadata.TenantID,
			ThreadID:    c.plan.doc.Metadata.ThreadID,
			RunID:       c.plan.doc.Metadata.RunID,
			NodeID:      c.plan.doc.Metadata.NodeID,
			Role:        c.plan.doc.Metadata.Role,
			Text:        c.text,
			CreatedAt:   c.plan.doc.Metadata.CreatedAt,
			ContentHash: c.plan.hash,
			DocID:       c.plan.doc.ID,
			ChunkSeq:    c.seq,
			ParentText:  c.plan.chunks[c.seq].ParentText,
		}
		if s.Sparse != nil {
			sv := s.Sparse.Encode(c.text)
			p.Sparse = &sv
		}
		points = append(points, p)
	}
	if err := s.Store.UpsertHybrid(ctx, collection, points); err != nil {
		return UpsertResponse{}, fmt.Errorf("ingest: upsert %d chunk points: %w", len(points), err)
	}
	return UpsertResponse{Indexed: len(pending), Skipped: skipped, Chunks: len(points)}, nil
}

// enhanceChunks 并发为待写子块生成摘要前缀。首个错误取消其余在途调用
// （增强无部分成功语义：要么全部拿到前缀，要么整批失败）。
func (s *Service) enhanceChunks(ctx context.Context, plans []docPlan) ([][]string, error) {
	conc := s.EnhanceConcurrency
	if conc <= 0 {
		conc = defaultEnhanceConcurrency
	}
	sem := make(chan struct{}, conc)
	out := make([][]string, len(plans))
	for i, p := range plans {
		out[i] = make([]string, len(p.chunks))
	}
	gctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var (
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for i, p := range plans {
		for j, c := range p.chunks {
			wg.Add(1)
			go func(i, j int, doc Document, c chunk.Chunk) {
				defer wg.Done()
				mu.Lock()
				stopped := firstErr != nil
				mu.Unlock()
				if stopped {
					return
				}
				select {
				case sem <- struct{}{}:
				case <-gctx.Done():
					return
				}
				defer func() { <-sem }()
				prefix, err := s.Enhancer.Enhance(gctx, doc, c)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = fmt.Errorf("ingest: enhance doc %q chunk %d: %w", doc.ID, j, err)
						cancel()
					}
					mu.Unlock()
					return
				}
				out[i][j] = prefix
			}(i, j, p.doc, c)
		}
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return out, nil
}

// childPointID 由 (tenant|docID|seq) 派生子块点 ID，沿用 runtime
// vector.PointID 的同一约定（SHA-256 前 8 字节大端）。点 ID 确定性是
// "同文档重放不产生重复点"的唯一屏障，规则变更会使存量子块变孤儿。
func childPointID(tenantID, docID string, seq int) uint64 {
	h := sha256.Sum256([]byte(tenantID + "|" + docID + "|" + strconv.Itoa(seq)))
	return binary.BigEndian.Uint64(h[:8])
}

// docsCtxVersion 汇总所有影响子块派生的参数版本（分块参数 + 增强器标识）。
// 任一变更都会改变文档级 content_hash，同内容重放即触发重嵌——
// 这是"改分块/增强配置后存量文档自动重处理"的机制。
func (s *Service) docsCtxVersion() string {
	cfg := s.ChunkConfig.WithDefaults()
	v := fmt.Sprintf("p%d-c%d", cfg.ParentSize, cfg.ChildSize)
	if s.Enhancer != nil {
		v += "+enh:" + s.CtxVersion
	}
	return v
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

// GetDoc 查询 docs 集合一篇文档的状态：按 (tenant, docID) 派生 child0
// 点 ID 后取回。tenant 必填——URL 路径只携带 doc ID，而点 ID 派生缺它不可。
func (s *Service) GetDoc(ctx context.Context, collection, tenantID, docID string) (vector.RetrievedDoc, bool, error) {
	if _, err := s.layout(collection); err != nil {
		return vector.RetrievedDoc{}, false, err
	}
	if tenantID == "" {
		return vector.RetrievedDoc{}, false, fmt.Errorf("%w: GET /documents requires ?tenant_id for docs collection", ErrTenantRequired)
	}
	if s == nil || s.Store == nil {
		return vector.RetrievedDoc{}, false, fmt.Errorf("ingest: service not configured")
	}
	id := childPointID(tenantID, docID, 0)
	docs, err := s.Store.Retrieve(ctx, collection, []uint64{id})
	if err != nil {
		return vector.RetrievedDoc{}, false, fmt.Errorf("ingest: retrieve: %w", err)
	}
	d, ok := docs[id]
	return d, ok, nil
}

// DeleteDoc 按 doc_id 过滤删除一篇文档的全部子块，幂等。
// 返回是否实际删除（child0 存在 → true）。docs 集合必须整删：
// 文档更新后子块数可能减少，按点 ID 删会留孤儿。
func (s *Service) DeleteDoc(ctx context.Context, collection, tenantID, docID string) (bool, error) {
	if _, err := s.layout(collection); err != nil {
		return false, err
	}
	if tenantID == "" {
		return false, fmt.Errorf("%w: DELETE /documents requires ?tenant_id for docs collection", ErrTenantRequired)
	}
	if s == nil || s.Store == nil {
		return false, fmt.Errorf("ingest: service not configured")
	}
	child0 := childPointID(tenantID, docID, 0)
	existing, err := s.Store.Retrieve(ctx, collection, []uint64{child0})
	if err != nil {
		return false, fmt.Errorf("ingest: check before delete: %w", err)
	}
	if len(existing) == 0 {
		return false, nil
	}
	if err := s.Store.DeleteByDocID(ctx, collection, tenantID, docID); err != nil {
		return false, fmt.Errorf("ingest: delete doc %q: %w", docID, err)
	}
	return true, nil
}

// IsDocsCollection 报告集合是否为 docs 布局。httpapi 文档端点据此选择
// 参数形态：docs 的 ID 是逻辑文档标识且必须携带 tenant_id；memory 的
// ID 是十进制 uint64。未知集合返回 false，随后由 memory 路径的
// layout 检查拒绝（仍 404）。
func (s *Service) IsDocsCollection(collection string) bool {
	return s != nil && collection == s.docsCollection()
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
