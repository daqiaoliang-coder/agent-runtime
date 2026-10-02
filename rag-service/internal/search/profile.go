package search

// Profile 是检索画像：同一套读管道按延迟预算与召回策略服务不同场景。
// Phase 1 只实现 memory；提前建模 Profile 是为了让 API 形状一次定型，
// Phase 2/3 扩管道时调用方无需改接口。
type Profile string

const (
	// ProfileMemory 服务 agent 运行时记忆召回：≤400ms 预算，dense-only，无重排，
	// 行为等价于 runtime providers.VectorMemory.Search（Phase 1 验收基准）。
	ProfileMemory Profile = "memory"

	// ProfileDocs 服务文档检索：dense+sparse 混合召回 + RRF 融合，预算 1-2s。
	// 排序按相关性（融合分降序）而非时间——文档检索的目标是命中，
	// 不是还原对话顺序。rerank 与 parent-child chunking 留给 Phase 3。
	ProfileDocs Profile = "docs"

	// Phase 3 预留：ProfileResearch Profile = "research" —— 多跳研究，由调用方循环驱动。
)
