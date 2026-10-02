package search

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/embed"
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
		{Text: "best match", Role: "assistant", NodeID: "n9", Score: 0.031, CreatedAt: now.Add(-2 * time.Hour)},
		{Text: "second", Role: "user", NodeID: "n5", Score: 0.025, CreatedAt: now.Add(-time.Hour)},
		{Text: "third", Role: "user", NodeID: "n1", Score: 0.016, CreatedAt: now},
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

// TestSearchDocsPassesFilterAndTopK 隔离过滤与 topK 必须透传到混合检索层，
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
	if hy.lastTopK != 5 {
		t.Fatalf("want topK=5, got %d", hy.lastTopK)
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
