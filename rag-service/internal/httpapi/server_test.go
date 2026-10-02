package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/ingest"
	"github.com/daqiaoliang-coder/rag-service/internal/search"
	"github.com/daqiaoliang-coder/rag-service/internal/sparse"
	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

type stubEmbedder struct {
	vecs [][]float32
	err  error
}

func (s stubEmbedder) Embed(_ context.Context, _ []string) ([][]float32, error) {
	return s.vecs, s.err
}

type stubSearcher struct {
	hits []vector.Hit
	err  error
}

func (s *stubSearcher) Search(_ context.Context, _ string, _ []float32, _ vector.Filter, _ int) ([]vector.Hit, error) {
	return s.hits, s.err
}

type stubHealth struct {
	err error
}

func (h stubHealth) Health(context.Context) error { return h.err }

func newTestService(searcher vector.Searcher, hybrid vector.HybridSearcher) *search.Service {
	return &search.Service{
		Embedder:       stubEmbedder{vecs: [][]float32{{1}}},
		Searcher:       searcher,
		Hybrid:         hybrid,
		Sparse:         sparse.NewTF(),
		Collection:     "agent_memory",
		DocsCollection: "rag_documents",
		TopK:           10,
		MinScore:       0.7,
		DocsMinScore:   0,
		Budget:         400 * time.Millisecond,
		DocsBudget:     1500 * time.Millisecond,
	}
}

func newTestHandler(token string, searcher vector.Searcher) http.Handler {
	return New(newTestService(searcher, nil), newTestIngest(), token, stubHealth{})
}

// fakeStore 是文档端点测试用的向量库替身（最小内存实现）。
type fakeStore struct {
	points map[string]map[uint64]vector.DocPoint
	err    error
}

func newFakeStore() *fakeStore { return &fakeStore{points: map[string]map[uint64]vector.DocPoint{}} }

func (f *fakeStore) EnsureMemoryCollection(context.Context, string, int) error { return nil }
func (f *fakeStore) EnsureDocsCollection(context.Context, string, int) error   { return nil }

func (f *fakeStore) UpsertDefault(_ context.Context, c string, pts []vector.DocPoint) error {
	if f.err != nil {
		return f.err
	}
	for _, p := range pts {
		if f.points[c] == nil {
			f.points[c] = map[uint64]vector.DocPoint{}
		}
		f.points[c][p.ID] = p
	}
	return nil
}

func (f *fakeStore) UpsertHybrid(ctx context.Context, c string, pts []vector.DocPoint) error {
	return f.UpsertDefault(ctx, c, pts)
}

func (f *fakeStore) Delete(_ context.Context, c string, ids ...uint64) error {
	if f.err != nil {
		return f.err
	}
	for _, id := range ids {
		delete(f.points[c], id)
	}
	return nil
}

func (f *fakeStore) Retrieve(_ context.Context, c string, ids []uint64) (map[uint64]vector.RetrievedDoc, error) {
	if f.err != nil {
		return nil, f.err
	}
	out := map[uint64]vector.RetrievedDoc{}
	for _, id := range ids {
		if p, ok := f.points[c][id]; ok {
			out[id] = vector.RetrievedDoc{
				ID: p.ID, TenantID: p.TenantID, Text: p.Text, ContentHash: p.ContentHash,
				CreatedAt: p.CreatedAt, Role: p.Role, NodeID: p.NodeID, RunID: p.RunID, ThreadID: p.ThreadID,
			}
		}
	}
	return out, nil
}

func newTestIngest() *ingest.Service {
	return &ingest.Service{
		Embedder:         stubEmbedder{vecs: [][]float32{{1, 2}}},
		Sparse:           sparse.NewTF(),
		Store:            newFakeStore(),
		MemoryCollection: "agent_memory",
		DocsCollection:   "rag_documents",
	}
}

func do(h http.Handler, method, target, token, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestHealthz(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	rec := do(h, http.MethodGet, "/healthz", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
}

func TestReadyz(t *testing.T) {
	svc := &search.Service{}
	// 后端健康 → 200。
	h := New(svc, nil, "", stubHealth{})
	if rec := do(h, http.MethodGet, "/readyz", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	// 后端不可用 → 503。
	h = New(svc, nil, "", stubHealth{err: context.DeadlineExceeded})
	if rec := do(h, http.MethodGet, "/readyz", "", ""); rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rec.Code)
	}
}

func TestAuth(t *testing.T) {
	h := newTestHandler("secret", &stubSearcher{})
	body := `{"query":"q","scope":{"tenant_id":"t1"}}`
	// 缺令牌 → 401；错误令牌 → 401；正确令牌 → 200。
	if rec := do(h, http.MethodPost, "/v1/search", "", body); rec.Code != http.StatusUnauthorized {
		t.Fatalf("missing token: want 401, got %d", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/v1/search", "wrong", body); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong token: want 401, got %d", rec.Code)
	}
	if rec := do(h, http.MethodPost, "/v1/search", "secret", body); rec.Code != http.StatusOK {
		t.Fatalf("correct token: want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	// 探针端点豁免鉴权：探针不携带业务凭证。
	if rec := do(h, http.MethodGet, "/healthz", "", ""); rec.Code != http.StatusOK {
		t.Fatalf("healthz must bypass auth, got %d", rec.Code)
	}
}

func TestSearchRejectsUnknownProfile(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	body := `{"query":"q","profile":"research","scope":{"tenant_id":"t1"}}`
	if rec := do(h, http.MethodPost, "/v1/search", "", body); rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for unknown profile, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestSearchRejectsInvalidBody(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	if rec := do(h, http.MethodPost, "/v1/search", "", "{not json"); rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for invalid json, got %d", rec.Code)
	}
}

func TestSearchMethodNotAllowed(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	if rec := do(h, http.MethodGet, "/v1/search", "", ""); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", rec.Code)
	}
}

func TestSearchHappyPath(t *testing.T) {
	now := time.Now()
	sh := &stubSearcher{hits: []vector.Hit{
		{Text: "b", Role: "assistant", NodeID: "n2", RunID: "r1", Score: 0.9, CreatedAt: now.Add(time.Second)},
		{Text: "a", Role: "user", NodeID: "n1", RunID: "r1", Score: 0.8, CreatedAt: now},
	}}
	h := newTestHandler("", sh)
	rec := do(h, http.MethodPost, "/v1/search", "", `{
		"query": "hello",
		"scope": {"tenant_id": "t1", "thread_id": "th1"},
		"exclude_run_id": "run-current"
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Results  []search.Result `json:"results"`
		Degraded []string        `json:"degraded"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Results) != 2 {
		t.Fatalf("want 2 results, got %d", len(resp.Results))
	}
	// created_at 正序：a 在前，b 在后。
	if resp.Results[0].Content != "a" || resp.Results[1].Content != "b" {
		t.Fatalf("want chronological order [a,b], got [%s,%s]",
			resp.Results[0].Content, resp.Results[1].Content)
	}
	if len(resp.Degraded) != 0 {
		t.Fatalf("want no degradation, got %v", resp.Degraded)
	}
}

func TestSearchEmptyQueryReturnsEmptyResults(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	rec := do(h, http.MethodPost, "/v1/search", "", `{"query":"","scope":{"tenant_id":"t1"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("empty query must be 200 (no-recall semantics), got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"results":[]`) {
		t.Fatalf("want empty results array, got %s", rec.Body.String())
	}
}

func TestSearchDegradationStillReturns200(t *testing.T) {
	// 向量库故障 → 200 + degraded 标记，绝不 5xx（runtime (nil,nil) 契约）。
	sh := &stubSearcher{err: context.DeadlineExceeded}
	h := newTestHandler("", sh)
	rec := do(h, http.MethodPost, "/v1/search", "", `{"query":"q","scope":{"tenant_id":"t1"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("degraded search must be 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), "vector_failed") {
		t.Fatalf("want vector_failed in degraded, got %s", rec.Body.String())
	}
}

// stubHybridSearcher 是 docs profile 的混合检索替身。
type stubHybridSearcher struct {
	hits []vector.Hit
	err  error
}

func (s *stubHybridSearcher) HybridSearch(_ context.Context, _ string, _ []float32, _ vector.SparseVector, _ vector.Filter, _ int) ([]vector.Hit, error) {
	return s.hits, s.err
}

func TestSearchDocsProfileKeepsRelevanceOrder(t *testing.T) {
	now := time.Now()
	// 融合分降序构造：分数高的文档 created_at 更旧。
	// docs profile 必须保持相关性顺序，不按时间重排（与 memory 的关键差异）。
	hy := &stubHybridSearcher{hits: []vector.Hit{
		{Text: "most relevant", Role: "assistant", NodeID: "n2", Score: 0.031, CreatedAt: now.Add(-time.Hour)},
		{Text: "less relevant", Role: "user", NodeID: "n1", Score: 0.016, CreatedAt: now},
	}}
	h := New(newTestService(&stubSearcher{}, hy), newTestIngest(), "", stubHealth{})
	rec := do(h, http.MethodPost, "/v1/search", "", `{
		"query": "部署手册",
		"profile": "docs",
		"scope": {"tenant_id": "t1"}
	}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Results  []search.Result `json:"results"`
		Degraded []string        `json:"degraded"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(resp.Results) != 2 || resp.Results[0].Content != "most relevant" {
		t.Fatalf("docs profile must keep relevance order, got %+v", resp.Results)
	}
	// 低融合分（0.016 << 0.7）必须保留：docs MinScore 缺省 0。
	if len(resp.Degraded) != 0 {
		t.Fatalf("want no degradation, got %v", resp.Degraded)
	}
}

// ==== 文档写路径端点（Phase 2） ====

// upsertDoc 是单条文档的最小合法请求体。
func upsertDoc(id, content string) string {
	return `{"documents": [{"id": "` + id + `", "content": "` + content +
		`", "metadata": {"tenant_id": "t1", "thread_id": "th1", "run_id": "r1", "node_id": "n1", "role": "assistant", "created_at": "2026-10-02T12:00:00Z"}}]}`
}

func TestUpsertDocuments(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	// 首次提交 → indexed=1。
	rec := do(h, http.MethodPost, "/v1/collections/agent_memory/documents", "", upsertDoc("1234567890", "部署手册"))
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var first struct {
		Indexed int `json:"indexed"`
		Skipped int `json:"skipped"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &first); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if first.Indexed != 1 || first.Skipped != 0 {
		t.Fatalf("want indexed=1 skipped=0, got %+v", first)
	}
	// 同内容重放 → 幂等跳过（不消耗 embedding 配额）。
	rec = do(h, http.MethodPost, "/v1/collections/agent_memory/documents", "", upsertDoc("1234567890", "部署手册"))
	if rec.Code != http.StatusOK {
		t.Fatalf("replay must be 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var replay struct {
		Indexed int `json:"indexed"`
		Skipped int `json:"skipped"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &replay); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if replay.Indexed != 0 || replay.Skipped != 1 {
		t.Fatalf("content_hash must skip re-embed: want indexed=0 skipped=1, got %+v", replay)
	}
}

func TestUpsertDocumentsValidation(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	cases := []struct {
		name   string
		target string
		body   string
		want   int
	}{
		{"unknown collection", "/v1/collections/other/documents", upsertDoc("1", "x"), http.StatusNotFound},
		{"empty documents", "/v1/collections/agent_memory/documents", `{"documents": []}`, http.StatusBadRequest},
		{"empty tenant", "/v1/collections/agent_memory/documents", `{"documents": [{"id": "1", "content": "x", "metadata": {"tenant_id": ""}}]}`, http.StatusBadRequest},
		{"bad id", "/v1/collections/agent_memory/documents", `{"documents": [{"id": "not-a-number", "content": "x", "metadata": {"tenant_id": "t1"}}]}`, http.StatusBadRequest},
		{"invalid json", "/v1/collections/agent_memory/documents", "{not json", http.StatusBadRequest},
	}
	for _, c := range cases {
		if rec := do(h, http.MethodPost, c.target, "", c.body); rec.Code != c.want {
			t.Errorf("%s: want %d, got %d: %s", c.name, c.want, rec.Code, rec.Body.String())
		}
	}
}

func TestUpsertDocumentsTooManyRejected(t *testing.T) {
	ing := newTestIngest()
	ing.MaxDocsPerRequest = 1
	h := New(newTestService(&stubSearcher{}, nil), ing, "", stubHealth{})
	body := `{"documents": [{"id": "1", "content": "a", "metadata": {"tenant_id": "t1"}}, {"id": "2", "content": "b", "metadata": {"tenant_id": "t1"}}]}`
	if rec := do(h, http.MethodPost, "/v1/collections/agent_memory/documents", "", body); rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400 for too many docs, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestDocumentEndpointsIngestNotConfigured(t *testing.T) {
	// 纯读部署（ingest=nil）中文档端点必须 503 而非 panic。
	h := New(newTestService(&stubSearcher{}, nil), nil, "", stubHealth{})
	for _, c := range []struct{ method, target string }{
		{http.MethodPost, "/v1/collections/agent_memory/documents"},
		{http.MethodGet, "/v1/collections/agent_memory/documents/1"},
		{http.MethodDelete, "/v1/collections/agent_memory/documents/1"},
	} {
		if rec := do(h, c.method, c.target, "", "{}"); rec.Code != http.StatusServiceUnavailable {
			t.Errorf("%s %s: want 503, got %d", c.method, c.target, rec.Code)
		}
	}
}

func TestDocumentEndpointsRequireAuth(t *testing.T) {
	h := newTestHandler("secret", &stubSearcher{})
	for _, c := range []struct{ method, target string }{
		{http.MethodPost, "/v1/collections/agent_memory/documents"},
		{http.MethodGet, "/v1/collections/agent_memory/documents/1"},
		{http.MethodDelete, "/v1/collections/agent_memory/documents/1"},
	} {
		if rec := do(h, c.method, c.target, "", "{}"); rec.Code != http.StatusUnauthorized {
			t.Errorf("%s %s: want 401, got %d", c.method, c.target, rec.Code)
		}
	}
}

func TestGetDocument(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	if rec := do(h, http.MethodPost, "/v1/collections/agent_memory/documents", "", upsertDoc("1234567890", "部署手册")); rec.Code != http.StatusOK {
		t.Fatalf("seed upsert: %d: %s", rec.Code, rec.Body.String())
	}
	// 已写入 → found:true + 文档内容。
	rec := do(h, http.MethodGet, "/v1/collections/agent_memory/documents/1234567890", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	if !strings.Contains(rec.Body.String(), `"found":true`) || !strings.Contains(rec.Body.String(), "部署手册") {
		t.Fatalf("want found:true with content, got %s", rec.Body.String())
	}
	// 未写入 → found:false 且省略 document 字段（区分"未写入"与"故障"）。
	rec = do(h, http.MethodGet, "/v1/collections/agent_memory/documents/999", "", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("missing doc must be 200, got %d", rec.Code)
	}
	if !strings.Contains(rec.Body.String(), `"found":false`) || strings.Contains(rec.Body.String(), "document") {
		t.Fatalf("want found:false without document field, got %s", rec.Body.String())
	}
	// 非 uint64 ID → 400。
	if rec := do(h, http.MethodGet, "/v1/collections/agent_memory/documents/abc", "", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id: want 400, got %d", rec.Code)
	}
	// 未知集合 → 404。
	if rec := do(h, http.MethodGet, "/v1/collections/other/documents/1", "", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown collection: want 404, got %d", rec.Code)
	}
}

func TestDeleteDocumentIdempotent(t *testing.T) {
	h := newTestHandler("", &stubSearcher{})
	if rec := do(h, http.MethodPost, "/v1/collections/agent_memory/documents", "", upsertDoc("1234567890", "部署手册")); rec.Code != http.StatusOK {
		t.Fatalf("seed upsert: %d", rec.Code)
	}
	// 存在 → deleted:true。
	if rec := do(h, http.MethodDelete, "/v1/collections/agent_memory/documents/1234567890", "", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"deleted":true`) {
		t.Fatalf("want 200 deleted:true, got %d: %s", rec.Code, rec.Body.String())
	}
	// 重复删除 → 幂等 200 + deleted:false（而非 404）。
	if rec := do(h, http.MethodDelete, "/v1/collections/agent_memory/documents/1234567890", "", ""); rec.Code != http.StatusOK || !strings.Contains(rec.Body.String(), `"deleted":false`) {
		t.Fatalf("want 200 deleted:false, got %d: %s", rec.Code, rec.Body.String())
	}
	// 非 uint64 ID → 400。
	if rec := do(h, http.MethodDelete, "/v1/collections/agent_memory/documents/abc", "", ""); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad id: want 400, got %d", rec.Code)
	}
	// 未知集合 → 404。
	if rec := do(h, http.MethodDelete, "/v1/collections/other/documents/1", "", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown collection: want 404, got %d", rec.Code)
	}
}
