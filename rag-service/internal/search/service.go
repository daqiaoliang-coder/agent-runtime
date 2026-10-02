// Package search 实现检索管道。Phase 2 含两个 profile：
//   - memory：与 runtime providers.VectorMemory.Search 行为等价的语义召回
//     （Phase 1 验收基准，dense-only）；
//   - docs：文档检索，dense+sparse 混合召回 + RRF 融合，按相关性排序。
//
// memory 等价的定义（影子对比验收依据）：
//   - 相同输入（query/tenant/thread/excludeRun/topK/minScore）产生相同的召回结果
//     与相同排序（created_at 正序，同一时刻按 node_id 稳定排序）；
//   - 相同的降级契约：任何内部错误表现为"空结果 + 降级标记"而非错误，
//     对应 runtime 的 (nil, nil) + warn 日志；
//   - 相同的默认参数：TopK=10、MinScore=0.7、Collection=agent_memory。
package search

import (
	"context"
	"fmt"
	"log"
	"sort"
	"strconv"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/embed"
	"github.com/daqiaoliang-coder/rag-service/internal/rerank"
	"github.com/daqiaoliang-coder/rag-service/internal/sparse"
	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

// 降级原因常量，出现在 Response.Degraded 中供调用方决策（如回退直连 Qdrant）。
const (
	DegradedEmbedFailed  = "embed_failed"     // embedding 网关错误（超时/限流/5xx）
	DegradedEmbedEmpty   = "embed_empty"      // 网关返回空向量（配置错误的典型症状）
	DegradedVectorFailed = "vector_failed"    // 向量库错误
	DegradedSparseEmpty  = "sparse_empty"     // docs profile：查询无法编码出稀疏向量
	DegradedRerankSkipped = "rerank_skipped" // docs profile：重排失败/超时，保持 RRF 序
)

// 默认检索参数，与 runtime providers.DefaultMemory* 一致。
const (
	DefaultTopK     = 10
	DefaultMinScore = float32(0.7)
	// DefaultCollection 与 runtime 共用同一集合，这是影子对比的物理前提。
	DefaultCollection = "agent_memory"
	// DefaultDocsCollection 是 docs profile 的目标集合（命名 dense+sparse 布局）。
	DefaultDocsCollection = "rag_documents"
	// DefaultDocsBudget 是 docs profile 的延迟预算（dense+sparse+RRF+可选重排）。
	DefaultDocsBudget = 1500 * time.Millisecond
	// DefaultDocsOverfetch 是 docs profile 的子块过采样倍数：混合召回的头部
	// 结果可能同文档扎堆（一篇文档多个子块命中），取 overfetch×topK 个子块
	// 经 doc_id 去重后才能凑满 topK 篇不同文档。
	DefaultDocsOverfetch = 4
)

// Request 是一次检索请求。Profile 由 httpapi 层校验后传入。
type Request struct {
	Profile      Profile
	Query        string
	TenantID     string
	ThreadID     string
	ExcludeRunID string
	TopK         int      // <=0 时取 Service.TopK，仍 <=0 时取 DefaultTopK
	MinScore     *float32 // nil 时取 Service.MinScore；显式传 0 表示不过滤
}

// Result 是一条召回结果。相比 runtime 的 contracts.Message（仅 Role/Content），
// 额外透出溯源信息，供调用方审计与去重。DocID/ChunkSeq 是 docs profile 的
// 子块定位字段（命中子块所属文档与序号），memory 路径恒为零值。
type Result struct {
	Role      string    `json:"role"`
	Content   string    `json:"content"`
	NodeID    string    `json:"node_id"`
	RunID     string    `json:"run_id"`
	Score     float32   `json:"score"`
	CreatedAt time.Time `json:"created_at"`
	DocID     string    `json:"doc_id,omitempty"`
	ChunkSeq  int       `json:"chunk_seq,omitempty"`
}

// StageTimings 记录各阶段耗时（毫秒），供延迟预算观测与 SLO 对账。
type StageTimings struct {
	EmbedMS  float64 `json:"embed_ms"`
	SearchMS float64 `json:"search_ms"`
	RerankMS float64 `json:"rerank_ms"` // docs profile：cross-encoder 重排耗时（未启用/降级为 0）
	TotalMS  float64 `json:"total_ms"`
}

// Response 是检索响应。Degraded 非空表示本次发生降级（结果为空或部分可用），
// 调用方据此决定是否回退直连 Qdrant（Phase 1 的双跑开关语义）。
type Response struct {
	Results  []Result     `json:"results"`
	Timings  StageTimings `json:"timings"`
	Degraded []string     `json:"degraded"`
}

// Service 是检索管道的装配单元，依赖均以接口注入，单测可用 fake 替换。
type Service struct {
	Embedder       embed.Embedder
	Searcher       vector.Searcher       // memory profile（dense-only）
	Hybrid         vector.HybridSearcher // docs profile（dense+sparse+RRF）
	Sparse         sparse.Encoder        // docs profile 查询侧稀疏编码
	Collection     string                // memory 集合，空回退 DefaultCollection
	DocsCollection string                // docs 集合，空回退 DefaultDocsCollection
	TopK           int
	MinScore       float32
	// DocsMinScore 是 docs profile 的缺省阈值：RRF 融合分约 1/(60+rank)，
	// 与余弦相似度不可比，缺省必须为 0（不过滤）而非沿用 0.7。
	DocsMinScore float32
	Budget       time.Duration // memory profile 总预算，<=0 不设限（仅测试）
	DocsBudget   time.Duration // docs profile 总预算，<=0 回退 DefaultDocsBudget
	// Reranker 是 docs profile 的 cross-encoder 重排器；nil 表示关闭
	// （保持 RRF 融合序，零外部依赖）。失败/超时降级 rerank_skipped。
	Reranker rerank.Reranker
	// DocsOverfetch 是子块过采样倍数（缺省 4），见 DefaultDocsOverfetch。
	DocsOverfetch int
}

// Search 按 profile 分发检索。两个 profile 共享同一降级契约：
// 任何内部错误都转成空结果 + Degraded 标记 + warn 日志，绝不返回 error
// ——记忆与文档召回都是主链路的增强项，故障必须表现为"这次没有历史"。
func (s *Service) Search(ctx context.Context, req Request) Response {
	switch req.Profile {
	case ProfileDocs:
		return s.searchDocs(ctx, req)
	default: // 含空值：缺省 memory，与 httpapi 的归一化一致
		return s.searchMemory(ctx, req)
	}
}

// searchDocs 执行 docs profile 检索（Phase 3 管道）：
//
//	embed + sparse → HybridSearch(overfetch×topK 子块)
//	  → rerank(子块)（可选；失败降级 rerank_skipped 保持 RRF 序）
//	  → 按 doc_id 去重（每篇文档保留最高分子块）→ 截断 topK
//	  → Content = parent_text（子块命中，父块作上下文返回）
//
// 与 memory 的刻意差异：
//   - 排序保持相关性（重排分或融合分降序），不按 created_at 重排——
//     文档检索的目标是命中，不是还原对话顺序；
//   - MinScore 缺省 0：RRF 分与 rerank 分均与余弦相似度不可比。
func (s *Service) searchDocs(ctx context.Context, req Request) Response {
	start := time.Now()
	resp := Response{Results: []Result{}, Degraded: []string{}}
	if s == nil || s.Embedder == nil || s.Hybrid == nil || s.Sparse == nil {
		resp.Timings.TotalMS = msSince(start)
		return resp
	}
	if req.Query == "" || req.TenantID == "" {
		resp.Timings.TotalMS = msSince(start)
		return resp
	}
	budget := s.DocsBudget
	if budget <= 0 {
		budget = DefaultDocsBudget
	}
	var cancel context.CancelFunc
	ctx, cancel = context.WithTimeout(ctx, budget)
	defer cancel()

	topK := req.TopK
	if topK <= 0 {
		topK = s.TopK
	}
	if topK <= 0 {
		topK = DefaultTopK
	}
	overfetch := s.DocsOverfetch
	if overfetch <= 0 {
		overfetch = DefaultDocsOverfetch
	}
	fetchLimit := topK * overfetch
	if fetchLimit < topK {
		fetchLimit = topK
	}
	minScore := s.DocsMinScore
	if req.MinScore != nil {
		minScore = *req.MinScore
	}

	embedStart := time.Now()
	vecs, err := s.Embedder.Embed(ctx, []string{req.Query})
	embedMS := msSince(embedStart)
	if err != nil {
		s.degrade(DegradedEmbedFailed, err, req)
		resp.Degraded = append(resp.Degraded, DegradedEmbedFailed)
		resp.Timings.TotalMS = msSince(start)
		return resp
	}
	if len(vecs) != 1 || len(vecs[0]) == 0 {
		s.degrade(DegradedEmbedEmpty, fmt.Errorf("got %d vectors", len(vecs)), req)
		resp.Degraded = append(resp.Degraded, DegradedEmbedEmpty)
		resp.Timings.TotalMS = msSince(start)
		return resp
	}
	sv := s.Sparse.Encode(req.Query)
	if len(sv.Indices) == 0 {
		// 纯符号查询（如 "???"）编不出任何词项，词法路完全失效。
		// 仍降级而非报错：dense 路本可继续，但混合语义已破坏，返回空更诚实。
		s.degrade(DegradedSparseEmpty, fmt.Errorf("query has no lexical tokens"), req)
		resp.Degraded = append(resp.Degraded, DegradedSparseEmpty)
		resp.Timings.EmbedMS = embedMS
		resp.Timings.TotalMS = msSince(start)
		return resp
	}

	searchStart := time.Now()
	hits, err := s.Hybrid.HybridSearch(ctx, s.docsCollection(), vecs[0], sv, vector.Filter{
		TenantID:     req.TenantID,
		ThreadID:     req.ThreadID,
		ExcludeRunID: req.ExcludeRunID,
	}, fetchLimit)
	searchMS := msSince(searchStart)
	if err != nil {
		s.degrade(DegradedVectorFailed, err, req)
		resp.Degraded = append(resp.Degraded, DegradedVectorFailed)
		resp.Timings.EmbedMS = embedMS
		resp.Timings.SearchMS = searchMS
		resp.Timings.TotalMS = msSince(start)
		return resp
	}

	// 空文本子块丢弃，其余进入重排/去重管道。
	children := make([]vector.Hit, 0, len(hits))
	for _, h := range hits {
		if h.Text == "" {
			continue
		}
		children = append(children, h)
	}

	// 可选 cross-encoder 重排：替换 RRF 融合序。任何失败（网络/超时/
	// 协议异常）降级 rerank_skipped 并保持 RRF 序——读路径契约：重排是
	// 增强项，它的故障只能让结果"退回次优"，不能让检索失败。
	rerankMS := float64(0)
	reranked := false
	if s.Reranker != nil && len(children) > 1 {
		rerankStart := time.Now()
		texts := make([]string, len(children))
		for i, c := range children {
			texts[i] = c.Text
		}
		rr, rerr := s.Reranker.Rerank(ctx, req.Query, texts, len(texts))
		rerankMS = msSince(rerankStart)
		if rerr != nil {
			s.degrade(DegradedRerankSkipped, rerr, req)
			resp.Degraded = append(resp.Degraded, DegradedRerankSkipped)
		} else if aerr := applyRerank(children, rr); aerr != nil {
			s.degrade(DegradedRerankSkipped, aerr, req)
			resp.Degraded = append(resp.Degraded, DegradedRerankSkipped)
		} else {
			reranked = true
		}
	}

	// 阈值过滤。分数语义随重排与否切换：rerank 分与 RRF 分不可比，
	// 重排生效时只有请求显式携带的 min_score 才参与过滤（服务端缺省
	// 阈值是按 RRF 分语义配置的，套在 rerank 分上会误杀）。
	filterScore := minScore
	if reranked && req.MinScore == nil {
		filterScore = 0
	}
	kept := make([]vector.Hit, 0, len(children))
	for _, c := range children {
		if filterScore > 0 && c.Score < filterScore {
			continue
		}
		kept = append(kept, c)
	}

	// 按 doc_id 去重：kept 已按分数降序，首见即该文档最高分子块。
	// 无 doc_id 的点（Phase 2 遗留整文档点）按点 ID 自成一类。
	seenDocs := make(map[string]struct{}, len(kept))
	deduped := make([]vector.Hit, 0, topK)
	for _, c := range kept {
		key := c.DocID
		if key == "" {
			key = "point:" + strconv.FormatUint(c.PointID, 10)
		}
		if _, dup := seenDocs[key]; dup {
			continue
		}
		seenDocs[key] = struct{}{}
		deduped = append(deduped, c)
		if len(deduped) == topK {
			break
		}
	}

	for _, c := range deduped {
		content := c.ParentText
		if content == "" {
			// Phase 2 遗留整文档点无父块概念，退回自身全文。
			content = c.Text
		}
		resp.Results = append(resp.Results, Result{
			Role:      roleOrUser(c.Role),
			Content:   content,
			NodeID:    c.NodeID,
			RunID:     c.RunID,
			Score:     c.Score,
			CreatedAt: c.CreatedAt,
			DocID:     c.DocID,
			ChunkSeq:  c.ChunkSeq,
		})
	}
	resp.Timings = StageTimings{EmbedMS: embedMS, SearchMS: searchMS, RerankMS: rerankMS, TotalMS: msSince(start)}
	return resp
}

// applyRerank 用重排结果就地重排 children 并覆写分数。
// rr 必须恰好构成 children 下标的一次完整排列（本管道显式传 topN=
// len(texts)，后端返回残缺即协议异常），否则报错由调用方降级。
// 排序不信任后端：自行按分数降序，保证"首见即最高分"的去重前提。
func applyRerank(children []vector.Hit, rr []rerank.Result) error {
	if len(rr) != len(children) {
		return fmt.Errorf("rerank returned %d results for %d texts", len(rr), len(children))
	}
	seen := make([]bool, len(children))
	for _, r := range rr {
		if r.Index < 0 || r.Index >= len(children) || seen[r.Index] {
			return fmt.Errorf("rerank result is not a permutation: bad index %d", r.Index)
		}
		seen[r.Index] = true
	}
	sorted := make([]rerank.Result, len(rr))
	copy(sorted, rr)
	sort.SliceStable(sorted, func(i, j int) bool { return sorted[i].Score > sorted[j].Score })
	out := make([]vector.Hit, len(children))
	for i, r := range sorted {
		out[i] = children[r.Index]
		out[i].Score = r.Score
	}
	copy(children, out)
	return nil
}

// searchMemory 执行 memory profile 检索，执行顺序与 runtime VectorMemory.Search 一致：
//  1. 依赖与输入校验（空查询/空租户 → 空记忆，非错误、非降级）；
//  2. Budget 内 embed 查询文本；
//  3. 带 tenant/thread 过滤检索（fail-closed）；
//  4. MinScore 过滤 + 空文本丢弃；
//  5. (created_at, node_id) 正序稳定重排；
//  6. 还原角色（未知角色归 user，与 runtime roleOrUser 一致）。
func (s *Service) searchMemory(ctx context.Context, req Request) Response {
	start := time.Now()
	resp := Response{Results: []Result{}, Degraded: []string{}}
	if s == nil || s.Embedder == nil || s.Searcher == nil {
		resp.Timings.TotalMS = msSince(start)
		return resp
	}
	if req.Profile == "" {
		req.Profile = ProfileMemory
	}
	// 空查询无法向量化、空租户无权召回：返回空记忆而非错误（等价 runtime 契约）。
	if req.Query == "" || req.TenantID == "" {
		resp.Timings.TotalMS = msSince(start)
		return resp
	}
	if s.Budget > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.Budget)
		defer cancel()
	}

	topK := req.TopK
	if topK <= 0 {
		topK = s.TopK
	}
	if topK <= 0 {
		topK = DefaultTopK
	}
	minScore := s.MinScore
	if req.MinScore != nil {
		minScore = *req.MinScore
	}

	embedStart := time.Now()
	vecs, err := s.Embedder.Embed(ctx, []string{req.Query})
	embedMS := msSince(embedStart)
	if err != nil {
		s.degrade(DegradedEmbedFailed, err, req)
		resp.Degraded = append(resp.Degraded, DegradedEmbedFailed)
		resp.Timings.TotalMS = msSince(start)
		return resp
	}
	if len(vecs) != 1 || len(vecs[0]) == 0 {
		s.degrade(DegradedEmbedEmpty, fmt.Errorf("got %d vectors", len(vecs)), req)
		resp.Degraded = append(resp.Degraded, DegradedEmbedEmpty)
		resp.Timings.EmbedMS = embedMS
		resp.Timings.TotalMS = msSince(start)
		return resp
	}

	searchStart := time.Now()
	hits, err := s.Searcher.Search(ctx, s.collection(), vecs[0], vector.Filter{
		TenantID:     req.TenantID,
		ThreadID:     req.ThreadID,
		ExcludeRunID: req.ExcludeRunID,
	}, topK)
	searchMS := msSince(searchStart)
	if err != nil {
		s.degrade(DegradedVectorFailed, err, req)
		resp.Degraded = append(resp.Degraded, DegradedVectorFailed)
		resp.Timings.EmbedMS = embedMS
		resp.Timings.SearchMS = searchMS
		resp.Timings.TotalMS = msSince(start)
		return resp
	}

	// 阈值过滤统一在管道层做（与 runtime provider 层一致），
	// 使 Qdrant 实现与测试 fake 行为一致，测试结论才可信。
	kept := make([]vector.Hit, 0, len(hits))
	for _, h := range hits {
		if minScore > 0 && h.Score < minScore {
			continue
		}
		if h.Text == "" {
			continue
		}
		kept = append(kept, h)
	}
	// 按时间正序：对话历史必须还原真实先后顺序；同一时刻按 NodeID 排序保证稳定。
	sort.SliceStable(kept, func(i, j int) bool {
		if kept[i].CreatedAt.Equal(kept[j].CreatedAt) {
			return kept[i].NodeID < kept[j].NodeID
		}
		return kept[i].CreatedAt.Before(kept[j].CreatedAt)
	})
	for _, h := range kept {
		resp.Results = append(resp.Results, Result{
			Role:      roleOrUser(h.Role),
			Content:   h.Text,
			NodeID:    h.NodeID,
			RunID:     h.RunID,
			Score:     h.Score,
			CreatedAt: h.CreatedAt,
		})
	}
	resp.Timings = StageTimings{EmbedMS: embedMS, SearchMS: searchMS, TotalMS: msSince(start)}
	return resp
}

// collection 返回生效集合名，空配置回退默认值（与 runtime collection() 一致）。
func (s *Service) collection() string {
	if s == nil || s.Collection == "" {
		return DefaultCollection
	}
	return s.Collection
}

// docsCollection 返回 docs profile 的生效集合名。
func (s *Service) docsCollection() string {
	if s == nil || s.DocsCollection == "" {
		return DefaultDocsCollection
	}
	return s.DocsCollection
}

// degrade 统一记录降级：只打 warn，不向上抛。日志带租户/会话维度，
// 便于定位是哪个租户的网关或向量库出了问题（与 runtime 日志格式对齐）。
func (s *Service) degrade(reason string, err error, req Request) {
	log.Printf("search: degraded (%s) profile=%s tenant=%s thread=%s run=%s: %v",
		reason, req.Profile, req.TenantID, req.ThreadID, req.ExcludeRunID, err)
}

// roleOrUser 把 payload 中的角色字符串归一化。空值或未知值归为 user：
// 记忆作为上下文喂给 LLM 时，user 比空角色更安全（不会与系统指令混淆）。
// 与 runtime providers.roleOrUser 行为一致。
func roleOrUser(role string) string {
	switch role {
	case "system", "user", "assistant", "tool":
		return role
	default:
		return "user"
	}
}

func msSince(t time.Time) float64 {
	return float64(time.Since(t).Microseconds()) / 1000
}
