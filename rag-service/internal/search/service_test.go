package search

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/embed"
	"github.com/daqiaoliang-coder/rag-service/internal/rerank"
	"github.com/daqiaoliang-coder/rag-service/internal/sparse"
	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

// fakeEmbedder / fakeSearcher 用于在无真实网关与 Qdrant 的条件下
// 验证管道语义（降级契约、过滤、排序、默认值链）。
type fakeEmbedder struct {
	vecs [][]float32
	err  error
}

func (f fakeEmbedder) Embed(_ context.Context, _ []string) ([][]float32, error) {
	return f.vecs, f.err
}

type fakeSearcher struct {
	hits       []vector.Hit
	err        error
	calls      int
	lastTopK   int
	lastFilter vector.Filter
}

func (f *fakeSearcher) Search(_ context.Context, _ string, _ []float32, fl vector.Filter, topK int) ([]vector.Hit, error) {
	f.calls++
	f.lastTopK = topK
	f.lastFilter = fl
	return f.hits, f.err
}

func newTestService(e embed.Embedder, s vector.Searcher) *Service {
	return &Service{
		Embedder:   e,
		Searcher:   s,
		Collection: "agent_memory",
		TopK:       10,
		MinScore:   0.7,
		Budget:     400 * time.Millisecond,
	}
}

func TestSearchEmptyQueryReturnsEmpty(t *testing.T) {
	sh := &fakeSearcher{}
	svc := newTestService(fakeEmbedder{vecs: [][]float32{{1}}}, sh)
	resp := svc.Search(context.Background(), Request{Profile: ProfileMemory, Query: "", TenantID: "t1"})
	if len(resp.Results) != 0 || len(resp.Degraded) != 0 {
		t.Fatalf("want empty non-degraded response, got %+v", resp)
	}
	if sh.calls != 0 {
		t.Fatalf("empty query must not reach searcher, got %d calls", sh.calls)
	}
}

func TestSearchEmptyTenantReturnsEmpty(t *testing.T) {
	sh := &fakeSearcher{}
	svc := newTestService(fakeEmbedder{vecs: [][]float32{{1}}}, sh)
	// 空租户是越权防线：必须短路为空结果，绝不触发检索。
	resp := svc.Search(context.Background(), Request{Query: "hello", TenantID: ""})
	if len(resp.Results) != 0 || len(resp.Degraded) != 0 {
		t.Fatalf("want empty non-degraded response, got %+v", resp)
	}
	if sh.calls != 0 {
		t.Fatalf("empty tenant must not reach searcher, got %d calls", sh.calls)
	}
}

func TestSearchEmbedErrorDegrades(t *testing.T) {
	sh := &fakeSearcher{}
	svc := newTestService(fakeEmbedder{err: errors.New("gateway timeout")}, sh)
	resp := svc.Search(context.Background(), Request{Query: "hello", TenantID: "t1"})
	if len(resp.Results) != 0 {
		t.Fatalf("want empty results on embed failure, got %d", len(resp.Results))
	}
	if len(resp.Degraded) != 1 || resp.Degraded[0] != DegradedEmbedFailed {
		t.Fatalf("want degraded=[embed_failed], got %v", resp.Degraded)
	}
	if sh.calls != 0 {
		t.Fatalf("embed failure must not reach searcher, got %d calls", sh.calls)
	}
}

func TestSearchEmbedEmptyDegrades(t *testing.T) {
	sh := &fakeSearcher{}
	svc := newTestService(fakeEmbedder{vecs: nil}, sh)
	resp := svc.Search(context.Background(), Request{Query: "hello", TenantID: "t1"})
	if len(resp.Degraded) != 1 || resp.Degraded[0] != DegradedEmbedEmpty {
		t.Fatalf("want degraded=[embed_empty], got %v", resp.Degraded)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("want empty results, got %d", len(resp.Results))
	}
}

func TestSearchVectorErrorDegrades(t *testing.T) {
	sh := &fakeSearcher{err: errors.New("qdrant unavailable")}
	svc := newTestService(fakeEmbedder{vecs: [][]float32{{0.1, 0.2}}}, sh)
	resp := svc.Search(context.Background(), Request{Query: "hello", TenantID: "t1"})
	if len(resp.Degraded) != 1 || resp.Degraded[0] != DegradedVectorFailed {
		t.Fatalf("want degraded=[vector_failed], got %v", resp.Degraded)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("want empty results, got %d", len(resp.Results))
	}
}

func TestSearchFiltersAndOrders(t *testing.T) {
	now := time.Now()
	sh := &fakeSearcher{hits: []vector.Hit{
		{Text: "later reply", Role: "assistant", NodeID: "node-b", RunID: "run-1", Score: 0.9, CreatedAt: now.Add(time.Minute)},
		{Text: "low score", Role: "user", NodeID: "node-c", Score: 0.5, CreatedAt: now}, // 低于 MinScore，丢弃
		{Text: "", Role: "user", NodeID: "node-d", Score: 0.95, CreatedAt: now},         // 空文本，丢弃
		{Text: "same-time b", Role: "tool", NodeID: "node-b", Score: 0.85, CreatedAt: now},
		{Text: "same-time a", Role: "bogus", NodeID: "node-a", Score: 0.8, CreatedAt: now}, // 未知角色归 user
	}}
	svc := newTestService(fakeEmbedder{vecs: [][]float32{{1}}}, sh)
	resp := svc.Search(context.Background(), Request{
		Query: "hello", TenantID: "t1", ThreadID: "th1", ExcludeRunID: "run-current",
	})
	if len(resp.Degraded) != 0 {
		t.Fatalf("want no degradation, got %v", resp.Degraded)
	}
	wantOrder := []struct{ content, role, nodeID string }{
		{"same-time a", "user", "node-a"}, // 同一时刻按 NodeID 升序
		{"same-time b", "tool", "node-b"},
		{"later reply", "assistant", "node-b"}, // created_at 正序在后
	}
	if len(resp.Results) != len(wantOrder) {
		t.Fatalf("want %d results, got %d: %+v", len(wantOrder), len(resp.Results), resp.Results)
	}
	for i, w := range wantOrder {
		got := resp.Results[i]
		if got.Content != w.content || got.Role != w.role || got.NodeID != w.nodeID {
			t.Fatalf("result[%d]: want (%s,%s,%s), got (%s,%s,%s)",
				i, w.content, w.role, w.nodeID, got.Content, got.Role, got.NodeID)
		}
	}
	// 隔离过滤必须原样下传到向量层（fail-closed 语义在此生效）。
	if sh.lastFilter.TenantID != "t1" || sh.lastFilter.ThreadID != "th1" || sh.lastFilter.ExcludeRunID != "run-current" {
		t.Fatalf("filter not passed through: %+v", sh.lastFilter)
	}
}

func TestSearchMinScoreOverrideZeroDisablesFilter(t *testing.T) {
	now := time.Now()
	sh := &fakeSearcher{hits: []vector.Hit{
		{Text: "low but kept", Role: "user", NodeID: "n1", Score: 0.1, CreatedAt: now},
	}}
	svc := newTestService(fakeEmbedder{vecs: [][]float32{{1}}}, sh)
	zero := float32(0)
	resp := svc.Search(context.Background(), Request{Query: "q", TenantID: "t1", MinScore: &zero})
	if len(resp.Results) != 1 {
		t.Fatalf("explicit min_score=0 must disable threshold, got %d results", len(resp.Results))
	}
}

func TestSearchTopKFallbackChain(t *testing.T) {
	sh := &fakeSearcher{hits: nil}
	// 请求未指定 → 服务端配置 10。
	svc := newTestService(fakeEmbedder{vecs: [][]float32{{1}}}, sh)
	svc.Search(context.Background(), Request{Query: "q", TenantID: "t1"})
	if sh.lastTopK != 10 {
		t.Fatalf("want topK=10 from service config, got %d", sh.lastTopK)
	}
	// 服务端也未配置 → DefaultTopK 10。
	svc.TopK = 0
	svc.Search(context.Background(), Request{Query: "q", TenantID: "t1"})
	if sh.lastTopK != DefaultTopK {
		t.Fatalf("want topK=%d fallback, got %d", DefaultTopK, sh.lastTopK)
	}
	// 请求显式指定优先于一切。
	svc.Search(context.Background(), Request{Query: "q", TenantID: "t1", TopK: 3})
	if sh.lastTopK != 3 {
		t.Fatalf("want explicit topK=3, got %d", sh.lastTopK)
	}
}

func TestSearchCollectionFallback(t *testing.T) {
	// Service 未配置集合时必须回退到 agent_memory，与 runtime 默认一致。
	svc := &Service{Embedder: fakeEmbedder{vecs: [][]float32{{1}}}, Searcher: &fakeSearcher{}}
	if got := svc.collection(); got != DefaultCollection {
		t.Fatalf("want default collection %q, got %q", DefaultCollection, got)
	}
	if got := svc.docsCollection(); got != DefaultDocsCollection {
		t.Fatalf("want default docs collection %q, got %q", DefaultDocsCollection, got)
	}
}

// ==== docs profile（Phase 2：dense+sparse+RRF） ====

// fakeHybridSearcher 记录透传参数，用于验证 docs profile 的管道语义。
type fakeHybridSearcher struct {
	hits       []vector.Hit
	err        error
	calls      int
	lastDense  []float32
	lastSparse vector.SparseVector
	lastFilter vector.Filter
	lastTopK   int
	lastColl   string
}

func (f *fakeHybridSearcher) HybridSearch(_ context.Context, coll string, dense []float32, sv vector.SparseVector, fl vector.Filter, topK int) ([]vector.Hit, error) {
	f.calls++
	f.lastColl = coll
	f.lastDense = dense
	f.lastSparse = sv
	f.lastFilter = fl
	f.lastTopK = topK
	return f.hits, f.err
}

func newTestDocsService(e embed.Embedder, h vector.HybridSearcher) *Service {
	return &Service{
		Embedder:       e,
		Hybrid:         h,
		Sparse:         sparse.NewTF(), // 真实 TF 编码器：sparse_empty 语义依赖真实分词
		Collection:     "agent_memory",
		DocsCollection: "rag_documents",
		TopK:           10,
		MinScore:       0.7,
		DocsMinScore:   0,
		Budget:         400 * time.Millisecond,
		DocsBudget:     1500 * time.Millisecond,
	}
}

// TestSearchDocsKeepsRelevanceOrder docs profile 不按 created_at 重排：
// 文档检索的目标是命中（融合分降序），不是还原对话顺序。
func TestSearchDocsKeepsRelevanceOrder(t *testing.T) {
	now := time.Now()
	hy := &fakeHybridSearcher{hits: []vector.Hit{
		{PointID: 1, Text: "best match", Role: "assistant", NodeID: "n9", Score: 0.031, CreatedAt: now.Add(-2 * time.Hour)},
		{PointID: 2, Text: "second", Role: "user", NodeID: "n5", Score: 0.025, CreatedAt: now.Add(-time.Hour)},
		{PointID: 3, Text: "third", Role: "user", NodeID: "n1", Score: 0.016, CreatedAt: now},
	}}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{0.1}}}, hy)
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if len(resp.Results) != 3 {
		t.Fatalf("want 3 results, got %d: %+v", len(resp.Results), resp.Results)
	}
	want := []string{"best match", "second", "third"}
	for i, w := range want {
		if resp.Results[i].Content != w {
			t.Fatalf("relevance order broken: results[%d]=%q, want %q (all: %+v)", i, resp.Results[i].Content, w, resp.Results)
		}
	}
	if len(resp.Degraded) != 0 {
		t.Fatalf("want no degradation, got %v", resp.Degraded)
	}
}

// TestSearchDocsMinScoreDefaultsToZero RRF 融合分约 1/(60+rank)，量级远低于
// 余弦相似度；沿用 memory 的 0.7 会清空所有结果，docs 缺省必须为 0。
func TestSearchDocsMinScoreDefaultsToZero(t *testing.T) {
	now := time.Now()
	hy := &fakeHybridSearcher{hits: []vector.Hit{
		{Text: "tiny rrf score", Role: "user", NodeID: "n1", Score: 0.0005, CreatedAt: now},
	}}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if len(resp.Results) != 1 {
		t.Fatalf("low RRF score must survive DocsMinScore=0 default, got %d results", len(resp.Results))
	}
}

// TestSearchDocsPassesFilterAndTopK 隔离过滤必须透传到混合检索层；
// 检索条数是过采样语义（topK×overfetch，Phase 3 去重管道的前提），
// sparse 查询向量必须已编码（非空）。
func TestSearchDocsPassesFilterAndTopK(t *testing.T) {
	hy := &fakeHybridSearcher{}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{0.2, 0.3}}}, hy)
	svc.Search(context.Background(), Request{
		Profile: ProfileDocs, Query: "部署手册", TenantID: "t1", ThreadID: "th1",
		ExcludeRunID: "run-current", TopK: 5,
	})
	if hy.calls != 1 {
		t.Fatalf("want exactly 1 hybrid call, got %d", hy.calls)
	}
	if hy.lastColl != "rag_documents" {
		t.Fatalf("docs profile must target rag_documents, got %q", hy.lastColl)
	}
	if hy.lastFilter.TenantID != "t1" || hy.lastFilter.ThreadID != "th1" || hy.lastFilter.ExcludeRunID != "run-current" {
		t.Fatalf("filter not passed through: %+v", hy.lastFilter)
	}
	// DocsOverfetch 未配置 → 缺省 4：混合检索取 5×4=20 个子块，
	// 去重后才截断为 topK=5 篇文档。
	if hy.lastTopK != 20 {
		t.Fatalf("want fetch limit 20 (topK=5 × overfetch=4), got %d", hy.lastTopK)
	}
	if len(hy.lastDense) != 2 {
		t.Fatalf("dense vector not passed through, got %v", hy.lastDense)
	}
	if len(hy.lastSparse.Indices) == 0 {
		t.Fatal("sparse query vector must be encoded for CJK query")
	}
}

// TestSearchDocsSparseEmptyDegrades 纯符号查询编不出词法词项时降级
// sparse_empty（而非报错），且不触达混合检索。
func TestSearchDocsSparseEmptyDegrades(t *testing.T) {
	hy := &fakeHybridSearcher{}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "???", TenantID: "t1"})
	if len(resp.Degraded) != 1 || resp.Degraded[0] != DegradedSparseEmpty {
		t.Fatalf("want degraded=[sparse_empty], got %v", resp.Degraded)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("want empty results, got %d", len(resp.Results))
	}
	if hy.calls != 0 {
		t.Fatalf("sparse-empty query must not reach hybrid searcher, got %d calls", hy.calls)
	}
}

// TestSearchDocsHybridErrorDegrades 混合检索故障 → 空结果 + vector_failed，
// 与 memory profile 共享"绝不因召回失败拖垮主链路"的降级契约。
func TestSearchDocsHybridErrorDegrades(t *testing.T) {
	hy := &fakeHybridSearcher{err: errors.New("qdrant hybrid unavailable")}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if len(resp.Degraded) != 1 || resp.Degraded[0] != DegradedVectorFailed {
		t.Fatalf("want degraded=[vector_failed], got %v", resp.Degraded)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("want empty results, got %d", len(resp.Results))
	}
}

// TestSearchDocsEmptyQueryShortCircuits 空查询/空租户 → 空结果且非降级，
// 语义与 memory profile 一致（越权防线优先于一切管道逻辑）。
func TestSearchDocsEmptyQueryShortCircuits(t *testing.T) {
	hy := &fakeHybridSearcher{}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	for _, req := range []Request{
		{Profile: ProfileDocs, Query: "", TenantID: "t1"},
		{Profile: ProfileDocs, Query: "q", TenantID: ""},
	} {
		resp := svc.Search(context.Background(), req)
		if len(resp.Results) != 0 || len(resp.Degraded) != 0 {
			t.Fatalf("want empty non-degraded response for %+v, got %+v", req, resp)
		}
	}
	if hy.calls != 0 {
		t.Fatalf("empty query/tenant must not reach hybrid searcher, got %d calls", hy.calls)
	}
}

// TestSearchDocsEmbedErrorDegrades embedding 网关故障 → 空结果 + embed_failed。
func TestSearchDocsEmbedErrorDegrades(t *testing.T) {
	hy := &fakeHybridSearcher{}
	svc := newTestDocsService(fakeEmbedder{err: errors.New("gateway 503")}, hy)
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if len(resp.Degraded) != 1 || resp.Degraded[0] != DegradedEmbedFailed {
		t.Fatalf("want degraded=[embed_failed], got %v", resp.Degraded)
	}
	if hy.calls != 0 {
		t.Fatalf("embed failure must not reach hybrid searcher, got %d calls", hy.calls)
	}
}

// TestSearchDocsNotConfiguredReturnsEmpty 未装配 Hybrid/Sparse 的 Service
// （如纯读部署误配）必须返回空结果而非 panic。
func TestSearchDocsNotConfiguredReturnsEmpty(t *testing.T) {
	svc := &Service{Embedder: fakeEmbedder{vecs: [][]float32{{1}}}, Searcher: &fakeSearcher{}}
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "q", TenantID: "t1"})
	if len(resp.Results) != 0 || len(resp.Degraded) != 0 {
		t.Fatalf("want empty response, got %+v", resp)
	}
}

// ==== docs profile（Phase 3：rerank + 父块去重） ====

// fakeReranker 记录透传参数，可注入错误、自定义打分或自定义结果集
// （结果集非 nil 时直接透传，用于构造协议异常）。
type fakeReranker struct {
	err     error
	scores  []float32 // 与输入 texts 下标对应；nil 时按输入序恒等排列
	results []rerank.Result
	calls     int
	lastQuery string
	lastTexts []string
	lastTopN  int
}

func (f *fakeReranker) Rerank(_ context.Context, query string, texts []string, topN int) ([]rerank.Result, error) {
	f.calls++
	f.lastQuery = query
	f.lastTexts = texts
	f.lastTopN = topN
	if f.err != nil {
		return nil, f.err
	}
	if f.results != nil {
		return f.results, nil
	}
	out := make([]rerank.Result, len(texts))
	for i := range texts {
		s := float32(len(texts) - i)
		if f.scores != nil {
			s = f.scores[i]
		}
		out[i] = rerank.Result{Index: i, Score: s}
	}
	return out, nil
}

// TestSearchDocsRerankReorders cross-encoder 分替换 RRF 融合序：低 RRF 分
// 子块可被提到首位，Score 覆写为 rerank 分。重排输入必须是子块文本
// （small-to-big：child 命中检索，parent 只作出上下文返回时的内容）。
func TestSearchDocsRerankReorders(t *testing.T) {
	now := time.Now()
	hy := &fakeHybridSearcher{hits: []vector.Hit{
		{PointID: 1, Text: "chunk a", DocID: "doc-a", ChunkSeq: 0, Score: 0.031, CreatedAt: now},
		{PointID: 2, Text: "chunk b", DocID: "doc-b", ChunkSeq: 0, Score: 0.025, CreatedAt: now},
		{PointID: 3, Text: "chunk c", DocID: "doc-c", ChunkSeq: 0, Score: 0.016, CreatedAt: now},
	}}
	// rerank 视角：b 最相关，a 最不相关——与 RRF 序完全相反。
	rr := &fakeReranker{scores: []float32{0.1, 0.9, 0.5}}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	svc.Reranker = rr
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if len(resp.Degraded) != 0 {
		t.Fatalf("want no degradation, got %v", resp.Degraded)
	}
	want := []struct {
		content string
		score   float32
	}{
		{"chunk b", 0.9},
		{"chunk c", 0.5},
		{"chunk a", 0.1},
	}
	if len(resp.Results) != len(want) {
		t.Fatalf("want %d results, got %d: %+v", len(want), len(resp.Results), resp.Results)
	}
	for i, w := range want {
		if resp.Results[i].Content != w.content || resp.Results[i].Score != w.score {
			t.Fatalf("result[%d]: want (%s,%v), got (%s,%v)",
				i, w.content, w.score, resp.Results[i].Content, resp.Results[i].Score)
		}
	}
	if rr.calls != 1 {
		t.Fatalf("want exactly 1 rerank call, got %d", rr.calls)
	}
	if rr.lastQuery != "部署手册" {
		t.Fatalf("query not passed through: %q", rr.lastQuery)
	}
	if len(rr.lastTexts) != 3 || rr.lastTexts[0] != "chunk a" {
		t.Fatalf("rerank input must be child texts: %v", rr.lastTexts)
	}
	if rr.lastTopN != 3 {
		t.Fatalf("topN must cover all candidates, got %d", rr.lastTopN)
	}
}

// TestSearchDocsRerankFailureDegrades rerank 传输错误与协议异常（残缺排列）
// 均降级 rerank_skipped，保持 RRF 融合序与原始分数——重排是增强项，
// 其故障只能退回次优，不能让检索失败。
func TestSearchDocsRerankFailureDegrades(t *testing.T) {
	now := time.Now()
	hits := []vector.Hit{
		{PointID: 1, Text: "a", DocID: "doc-a", Score: 0.031, CreatedAt: now},
		{PointID: 2, Text: "b", DocID: "doc-b", Score: 0.025, CreatedAt: now},
	}
	cases := []struct {
		name string
		rr   *fakeReranker
	}{
		{"transport error", &fakeReranker{err: errors.New("tei timeout")}},
		{"incomplete permutation", &fakeReranker{results: []rerank.Result{{Index: 0, Score: 0.9}}}},
	}
	for _, tc := range cases {
		hy := &fakeHybridSearcher{hits: hits}
		svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
		svc.Reranker = tc.rr
		resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
		if len(resp.Degraded) != 1 || resp.Degraded[0] != DegradedRerankSkipped {
			t.Fatalf("%s: want degraded=[rerank_skipped], got %v", tc.name, resp.Degraded)
		}
		if len(resp.Results) != 2 || resp.Results[0].Content != "a" || resp.Results[1].Content != "b" {
			t.Fatalf("%s: RRF order must be preserved, got %+v", tc.name, resp.Results)
		}
		if resp.Results[0].Score != 0.031 {
			t.Fatalf("%s: original RRF score must be kept, got %v", tc.name, resp.Results[0].Score)
		}
	}
}

// TestSearchDocsRerankerNilDisabled Reranker=nil 表示关闭重排：零外部
// 调用（nil 装配本身保证），结果保持 RRF 序、无 rerank 降级与耗时。
func TestSearchDocsRerankerNilDisabled(t *testing.T) {
	now := time.Now()
	hy := &fakeHybridSearcher{hits: []vector.Hit{
		{PointID: 1, Text: "a", DocID: "doc-a", Score: 0.031, CreatedAt: now},
		{PointID: 2, Text: "b", DocID: "doc-b", Score: 0.025, CreatedAt: now},
	}}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	svc.Reranker = nil
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if len(resp.Degraded) != 0 {
		t.Fatalf("want no degradation with rerank disabled, got %v", resp.Degraded)
	}
	if len(resp.Results) != 2 || resp.Results[0].Content != "a" {
		t.Fatalf("RRF order must be preserved, got %+v", resp.Results)
	}
	if resp.Timings.RerankMS != 0 {
		t.Fatalf("rerank_ms must be 0 when disabled, got %v", resp.Timings.RerankMS)
	}
}

// TestSearchDocsRerankSkipsSingleCandidate 单子块重排无意义，不触发外部调用。
func TestSearchDocsRerankSkipsSingleCandidate(t *testing.T) {
	now := time.Now()
	hy := &fakeHybridSearcher{hits: []vector.Hit{
		{PointID: 1, Text: "only", DocID: "doc-a", Score: 0.031, CreatedAt: now},
	}}
	rr := &fakeReranker{}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	svc.Reranker = rr
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if rr.calls != 0 {
		t.Fatalf("single candidate must skip rerank, got %d calls", rr.calls)
	}
	if len(resp.Results) != 1 || len(resp.Degraded) != 0 {
		t.Fatalf("want 1 non-degraded result, got %+v", resp)
	}
}

// TestSearchDocsParentDedup 同文档多子块只保留最高分子块，Content 替换为
// 父块全文（small-to-big）；topK 截断的是去重后的文档数，不是子块数。
func TestSearchDocsParentDedup(t *testing.T) {
	now := time.Now()
	hy := &fakeHybridSearcher{hits: []vector.Hit{
		// doc-1 两个子块（seq0/seq1）：RRF 序在前的 seq0 是该文档最高分。
		{PointID: 11, Text: "doc1 child0", ParentText: "doc1 parent full text", DocID: "doc-1", ChunkSeq: 0, Score: 0.031, CreatedAt: now},
		{PointID: 21, Text: "doc2 child0", ParentText: "doc2 parent full text", DocID: "doc-2", ChunkSeq: 0, Score: 0.025, CreatedAt: now},
		{PointID: 12, Text: "doc1 child1", ParentText: "doc1 parent full text", DocID: "doc-1", ChunkSeq: 1, Score: 0.02, CreatedAt: now},
		// 空文本子块丢弃，不参与去重。
		{PointID: 99, Text: "", ParentText: "blank", DocID: "doc-9", ChunkSeq: 0, Score: 0.019, CreatedAt: now},
		{PointID: 31, Text: "doc3 child0", ParentText: "doc3 parent full text", DocID: "doc-3", ChunkSeq: 0, Score: 0.01, CreatedAt: now},
	}}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	want := []struct {
		docID   string
		content string
		seq     int
	}{
		{"doc-1", "doc1 parent full text", 0},
		{"doc-2", "doc2 parent full text", 0},
		{"doc-3", "doc3 parent full text", 0},
	}
	if len(resp.Results) != len(want) {
		t.Fatalf("want %d deduped results, got %d: %+v", len(want), len(resp.Results), resp.Results)
	}
	for i, w := range want {
		got := resp.Results[i]
		if got.DocID != w.docID || got.Content != w.content || got.ChunkSeq != w.seq {
			t.Fatalf("result[%d]: want (%s,%s,%d), got (%s,%s,%d)",
				i, w.docID, w.content, w.seq, got.DocID, got.Content, got.ChunkSeq)
		}
	}
	// topK=2：截断为前 2 篇不同文档。
	resp = svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1", TopK: 2})
	if len(resp.Results) != 2 || resp.Results[0].DocID != "doc-1" || resp.Results[1].DocID != "doc-2" {
		t.Fatalf("topK=2 must keep 2 distinct docs, got %+v", resp.Results)
	}
}

// TestSearchDocsRerankThenDedup 重排与去重的组合语义：doc-1 的最高分子块
// 经 rerank 从 seq0 变为 seq1，去重必须保留 rerank 视角的最高分（seq1）。
func TestSearchDocsRerankThenDedup(t *testing.T) {
	now := time.Now()
	hy := &fakeHybridSearcher{hits: []vector.Hit{
		{PointID: 11, Text: "d1c0", ParentText: "d1 parent", DocID: "doc-1", ChunkSeq: 0, Score: 0.031, CreatedAt: now},
		{PointID: 21, Text: "d2c0", ParentText: "d2 parent", DocID: "doc-2", ChunkSeq: 0, Score: 0.025, CreatedAt: now},
		{PointID: 12, Text: "d1c1", ParentText: "d1 parent", DocID: "doc-1", ChunkSeq: 1, Score: 0.02, CreatedAt: now},
	}}
	// rerank 分：doc-1/seq1 最高（0.95），doc-1/seq0 最低（0.2）。
	rr := &fakeReranker{scores: []float32{0.2, 0.5, 0.95}}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	svc.Reranker = rr
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if len(resp.Results) != 2 {
		t.Fatalf("want 2 distinct docs, got %d: %+v", len(resp.Results), resp.Results)
	}
	if resp.Results[0].DocID != "doc-1" || resp.Results[0].ChunkSeq != 1 || resp.Results[0].Score != 0.95 {
		t.Fatalf("doc-1 must keep rerank-best child (seq1), got %+v", resp.Results[0])
	}
	if resp.Results[1].DocID != "doc-2" {
		t.Fatalf("second result must be doc-2, got %+v", resp.Results[1])
	}
}

// TestSearchDocsOverfetchBoundary 过采样倍数边界：显式配置生效；
// DocsOverfetch=1 退化为 topK（无过采样）；未配置回退缺省 4。
func TestSearchDocsOverfetchBoundary(t *testing.T) {
	for _, tc := range []struct {
		overfetch int
		topK      int
		wantLimit int
	}{
		{overfetch: 1, topK: 5, wantLimit: 5},
		{overfetch: 2, topK: 5, wantLimit: 10},
		{overfetch: 0, topK: 5, wantLimit: 20}, // 未配置 → DefaultDocsOverfetch=4
	} {
		hy := &fakeHybridSearcher{}
		svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
		svc.DocsOverfetch = tc.overfetch
		svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1", TopK: tc.topK})
		if hy.lastTopK != tc.wantLimit {
			t.Fatalf("overfetch=%d topK=%d: want fetch limit %d, got %d",
				tc.overfetch, tc.topK, tc.wantLimit, hy.lastTopK)
		}
	}
}

// TestSearchDocsRerankScoreSemantics 重排生效时分数语义切换：rerank 分与
// RRF 分不可比，服务端 DocsMinScore（按 RRF 语义配置）不得套用在 rerank
// 分上；只有请求显式携带的 min_score 才参与过滤。
func TestSearchDocsRerankScoreSemantics(t *testing.T) {
	now := time.Now()
	hits := []vector.Hit{
		{PointID: 1, Text: "a", DocID: "doc-a", Score: 0.031, CreatedAt: now},
		{PointID: 2, Text: "b", DocID: "doc-b", Score: 0.025, CreatedAt: now},
	}
	// rerank 分刻意压到远低于 DocsMinScore 的量级：若误套服务端阈值会清空结果。
	rr := &fakeReranker{scores: []float32{0.001, 0.002}}

	// 未显式传 min_score：DocsMinScore=0.02 不套用，低 rerank 分保留。
	hy := &fakeHybridSearcher{hits: hits}
	svc := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy)
	svc.Reranker = rr
	svc.DocsMinScore = 0.02
	resp := svc.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1"})
	if len(resp.Results) != 2 {
		t.Fatalf("server-side DocsMinScore must not apply to rerank scores, got %d results: %+v", len(resp.Results), resp.Results)
	}

	// 显式传 min_score：按请求语义过滤（0.0015 介于两个 rerank 分之间）。
	hy2 := &fakeHybridSearcher{hits: hits}
	svc2 := newTestDocsService(fakeEmbedder{vecs: [][]float32{{1}}}, hy2)
	svc2.Reranker = rr
	svc2.DocsMinScore = 0.02
	explicit := float32(0.0015)
	resp2 := svc2.Search(context.Background(), Request{Profile: ProfileDocs, Query: "部署手册", TenantID: "t1", MinScore: &explicit})
	if len(resp2.Results) != 1 || resp2.Results[0].Content != "b" {
		t.Fatalf("explicit min_score must filter rerank scores, got %+v", resp2.Results)
	}
}
