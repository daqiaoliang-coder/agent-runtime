// Package vector 抽象 RAG 服务的向量库读写能力。
//
// 读语义镜像 runtime internal/adapters/vector：相同的 payload 键名契约、
// 相同的 fail-closed 过滤规则，保证 rag-api 与直连 Qdrant 的检索结果可做
// 影子对比。Phase 2 起承接写路径：memory 集合沿用 runtime 的默认向量布局
// （幂等点 ID + content_hash 跳过重嵌入），docs 集合采用命名向量布局
// （dense + sparse）以支持混合召回。
package vector

import (
	"context"
	"time"
)

// Payload 键名常量：必须与 runtime internal/adapters/vector 逐字段一致。
// 这是 rag-service 与 runtime 读写同一 Qdrant collection 的数据契约，
// 任何一方擅自变更键名都会导致召回错乱（过滤失效或字段读空）。
const (
	PayloadTenantID  = "tenant_id"
	PayloadThreadID  = "thread_id"
	PayloadRunID     = "run_id"
	PayloadNodeID    = "node_id"
	PayloadRole      = "role"
	PayloadText      = "text"
	PayloadCreatedAt = "created_at"
	// PayloadContentHash 是写路径幂等键（content 的 SHA-256 十六进制）。
	// 读路径不消费它，因此对 runtime 侧的影子对比无影响；它使"同 ID 同内容
	// 的重复提交"在 rag-api 侧被识别并跳过重嵌入，保护 embedding 网关配额。
	PayloadContentHash = "content_hash"
)

// docs 集合的命名向量名。memory 集合用默认（未命名）向量，与 runtime
// 的建集合语句一致，绝不能改用命名布局——那会让 runtime 直连写入的点
// 与 rag-api 写入的点互不兼容。
const (
	VectorNameDense  = "dense"
	VectorNameSparse = "sparse"
)

// Hit 是带相似度分数的检索结果，字段语义与 runtime vector.Hit 一致。
// 相比 contracts.Message（只有 Role/Content），这里额外透出 NodeID/RunID/Score，
// 供调用方做溯源与去重——这是独立服务相对进程内实现的增强，不是行为变更。
type Hit struct {
	PointID   uint64
	TenantID  string
	ThreadID  string
	RunID     string
	NodeID    string
	Role      string
	Text      string
	CreatedAt time.Time
	Score     float32
}

// Filter 是检索时的强制隔离条件（fail-closed）。
//
// TenantID 是越权防线：为空时 Qdrant 实现仍下发 must 匹配，结果是"查不到任何
// 数据"而非"查到全部数据"。ThreadID 为空表示不按会话过滤（存量数据 thread_id
// 为空串，无条件过滤会屏蔽历史记忆）。ExcludeRunID 非空时在源头排除当前 Run，
// 避免与调用方另行提供的当前 Run 历史重复注入上下文。
type Filter struct {
	TenantID     string
	ThreadID     string
	ExcludeRunID string
}

// Searcher 是 memory profile 的只读能力（dense-only）。
type Searcher interface {
	// Search 返回 topK 条最相似的点，按 Score 降序；MinScore 过滤不在此层做。
	Search(ctx context.Context, collection string, vec []float32, f Filter, topK int) ([]Hit, error)
}

// SparseVector 是稀疏向量（词项 → 权重），用于 docs profile 的词法召回。
// Indices 与 Values 按下标配对，须按 Indices 升序排列（Qdrant 要求）。
type SparseVector struct {
	Indices []uint32
	Values  []float32
}

// HybridSearcher 是 docs profile 的混合检索能力：dense 语义召回与 sparse
// 词法召回经 RRF 融合，返回 topK 条结果（融合分降序）。
//
// RRF 融合分数量级约 1/(k+rank)（k=60），与余弦相似度不可比，
// 因此 docs profile 的 MinScore 缺省必须为 0，不能沿用 memory 的 0.7。
type HybridSearcher interface {
	HybridSearch(ctx context.Context, collection string, dense []float32, sparse SparseVector, f Filter, topK int) ([]Hit, error)
}

// DocPoint 是一条待写入的文档向量。布局由写入方法决定：
// UpsertDefault 只写 Dense（memory 集合，未命名向量）；UpsertHybrid 同时写
// Dense 与 Sparse（docs 集合，命名向量）。ID 必须由调用方确定性派生——
// memory 集合沿用 runtime 的 vector.PointID，docs 集合用 hash(tenant,doc,seq)。
type DocPoint struct {
	ID          uint64
	Dense       []float32
	Sparse      *SparseVector
	TenantID    string
	ThreadID    string
	RunID       string
	NodeID      string
	Role        string
	Text        string
	CreatedAt   time.Time
	ContentHash string
}

// RetrievedDoc 是按 ID 取回的文档（GET status 与幂等检查用）。
type RetrievedDoc struct {
	ID          uint64
	TenantID    string
	ThreadID    string
	RunID       string
	NodeID      string
	Role        string
	Text        string
	CreatedAt   time.Time
	ContentHash string
}

// Writer 是写路径的向量库能力集。EnsureMemoryCollection 的语义与
// runtime memory-indexer 的 EnsureCollection 逐行等价（同一集合、同一
// 维度校验、同一 payload 索引），这是 remote/shadow 模式下集合仍可
// 与 runtime 直连写入互通的前提。
type Writer interface {
	// EnsureMemoryCollection 幂等确保 memory 布局集合存在（未命名 dense 向量）。
	EnsureMemoryCollection(ctx context.Context, collection string, dim int) error
	// EnsureDocsCollection 幂等确保 docs 布局集合存在（命名 dense+sparse 向量）。
	EnsureDocsCollection(ctx context.Context, collection string, dim int) error
	// UpsertDefault 以未命名向量布局写入（memory 集合）。
	UpsertDefault(ctx context.Context, collection string, points []DocPoint) error
	// UpsertHybrid 以命名向量布局写入 dense+sparse（docs 集合）。
	UpsertHybrid(ctx context.Context, collection string, points []DocPoint) error
	// Delete 按 ID 删除点，幂等（不存在的 ID 也算成功）。
	Delete(ctx context.Context, collection string, ids ...uint64) error
	// Retrieve 按 ID 批量取回点及其 payload；不存在的 ID 不出现在结果里。
	Retrieve(ctx context.Context, collection string, ids []uint64) (map[uint64]RetrievedDoc, error)
}
