package memory

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"agent-runtime/internal/model"
)

// ==== RemoteWriter：HTTP 契约与错误语义 ====

// remoteAPISpy 记录收到的请求，并按配置返回状态码。
type remoteAPISpy struct {
	status   int
	calls    int
	lastPath string
	lastAuth string
	lastBody remoteUpsertRequest
	readyz   int
}

func newRemoteAPISpy(status, readyz int) *remoteAPISpy {
	return &remoteAPISpy{status: status, readyz: readyz}
}

func (s *remoteAPISpy) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(s.readyz)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	mux.HandleFunc("POST /v1/collections/{name}/documents", func(w http.ResponseWriter, r *http.Request) {
		s.calls++
		s.lastPath = r.URL.Path
		s.lastAuth = r.Header.Get("Authorization")
		_ = json.NewDecoder(r.Body).Decode(&s.lastBody)
		w.WriteHeader(s.status)
		_, _ = w.Write([]byte(`{"indexed":1,"skipped":0}`))
	})
	return mux
}

func testDocs() []Document {
	return []Document{
		{ID: 1234567890, Text: "部署手册", Role: "assistant", TenantID: "t1", ThreadID: "th1", RunID: "r1", NodeID: "n1", CreatedAt: time.Date(2026, 10, 2, 12, 0, 0, 0, time.UTC)},
	}
}

// TestRemoteWriter_WriteContract 验证请求契约：路径、方法、Bearer 令牌、
// 文档 ID 十进制字符串化、metadata 字段完整——这是与 rag-api ingest 的对齐线。
func TestRemoteWriter_WriteContract(t *testing.T) {
	spy := newRemoteAPISpy(http.StatusOK, http.StatusOK)
	srv := httptest.NewServer(spy.handler())
	defer srv.Close()

	w := &RemoteWriter{BaseURL: srv.URL, Token: "secret", Client: srv.Client()}
	if err := w.Write(context.Background(), "agent_memory", testDocs()); err != nil {
		t.Fatalf("write: %v", err)
	}
	if spy.calls != 1 {
		t.Fatalf("want 1 upsert call, got %d", spy.calls)
	}
	if spy.lastPath != "/v1/collections/agent_memory/documents" {
		t.Fatalf("unexpected path %q", spy.lastPath)
	}
	if spy.lastAuth != "Bearer secret" {
		t.Fatalf("unexpected auth header %q", spy.lastAuth)
	}
	if len(spy.lastBody.Documents) != 1 {
		t.Fatalf("want 1 document, got %d", len(spy.lastBody.Documents))
	}
	d := spy.lastBody.Documents[0]
	if d.ID != "1234567890" {
		t.Errorf("id must be decimal string, got %q", d.ID)
	}
	if d.Content != "部署手册" || d.Metadata.TenantID != "t1" || d.Metadata.ThreadID != "th1" ||
		d.Metadata.RunID != "r1" || d.Metadata.NodeID != "n1" || d.Metadata.Role != "assistant" {
		t.Errorf("metadata contract broken: %+v", d)
	}
	if d.Metadata.CreatedAt.IsZero() {
		t.Error("created_at must be propagated")
	}
}

// TestRemoteWriter_WriteErrorPropagates 非 200 必须显式上抛（写路径契约，
// 与读路径 (nil,nil) 降级刻意相反）：静默吞错会让索引器把"未写入"误当"已写入"。
func TestRemoteWriter_WriteErrorPropagates(t *testing.T) {
	for _, status := range []int{http.StatusBadRequest, http.StatusNotFound, http.StatusServiceUnavailable, http.StatusInternalServerError} {
		spy := newRemoteAPISpy(status, http.StatusOK)
		srv := httptest.NewServer(spy.handler())
		w := &RemoteWriter{BaseURL: srv.URL, Client: srv.Client()}
		if err := w.Write(context.Background(), "agent_memory", testDocs()); err == nil {
			t.Errorf("status %d: expected error to propagate", status)
		}
		srv.Close()
	}
}

// TestRemoteWriter_EmptyBatchSkipsRequest 空批次不发请求：
// 与 Indexer "空节点直接标记"流程兼容，省一次 HTTP 往返。
func TestRemoteWriter_EmptyBatchSkipsRequest(t *testing.T) {
	spy := newRemoteAPISpy(http.StatusOK, http.StatusOK)
	srv := httptest.NewServer(spy.handler())
	defer srv.Close()
	w := &RemoteWriter{BaseURL: srv.URL, Client: srv.Client()}
	if err := w.Write(context.Background(), "agent_memory", nil); err != nil {
		t.Fatalf("empty batch must be no-op, got %v", err)
	}
	if spy.calls != 0 {
		t.Fatalf("empty batch must not hit API, got %d calls", spy.calls)
	}
}

// TestRemoteWriter_EnsureCollectionReadyz readyz 探活：就绪 → nil；
// 不可用 / 连接失败 → error（remote 模式下 rag-api 是唯一写入通道，早失败好过空转）。
func TestRemoteWriter_EnsureCollectionReadyz(t *testing.T) {
	// 就绪。
	spy := newRemoteAPISpy(http.StatusOK, http.StatusOK)
	srv := httptest.NewServer(spy.handler())
	w := &RemoteWriter{BaseURL: srv.URL, Client: srv.Client()}
	if err := w.EnsureCollection(context.Background(), "agent_memory", 1536); err != nil {
		t.Fatalf("ready service must pass: %v", err)
	}
	srv.Close()
	// 服务未就绪。
	spy = newRemoteAPISpy(http.StatusOK, http.StatusServiceUnavailable)
	srv = httptest.NewServer(spy.handler())
	w = &RemoteWriter{BaseURL: srv.URL, Client: srv.Client()}
	if err := w.EnsureCollection(context.Background(), "agent_memory", 1536); err == nil {
		t.Fatal("unready service must fail")
	}
	srv.Close()
	// 连接失败。
	w = &RemoteWriter{BaseURL: "http://127.0.0.1:1", Client: srv.Client()}
	if err := w.EnsureCollection(context.Background(), "agent_memory", 1536); err == nil {
		t.Fatal("connection failure must fail")
	}
}

// TestRemoteWriter_NotConfigured BaseURL/Client 缺失时报配置错误而非 panic。
func TestRemoteWriter_NotConfigured(t *testing.T) {
	w := &RemoteWriter{}
	if err := w.Write(context.Background(), "c", testDocs()); err == nil {
		t.Fatal("expected config error")
	}
	if err := w.EnsureCollection(context.Background(), "c", 8); err == nil {
		t.Fatal("expected config error")
	}
}

// TestIndexer_WithRemoteWriter 全链路接线：投影节点 → HTTP 提交 → 标记进度。
// 验证 Writer port 在 Indexer 内的组装正确性（ID 派生、payload 归属、先写后标）。
func TestIndexer_WithRemoteWriter(t *testing.T) {
	spy := newRemoteAPISpy(http.StatusOK, http.StatusOK)
	srv := httptest.NewServer(spy.handler())
	defer srv.Close()

	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "为什么延期", "因为依赖未就绪")}}
	ix := New(&RemoteWriter{BaseURL: srv.URL, Client: srv.Client()}, st, Config{
		Collection: "agent_memory", Dim: 1536,
		BatchSize: 16, BatchDelay: time.Nanosecond, PollInterval: time.Millisecond,
	})
	n, err := ix.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 1 || !st.hasMarked("n1") {
		t.Fatalf("node must be indexed and marked, got n=%d marked=%v", n, st.marked)
	}
	// 一个节点产出 input+output 两条文档。
	if len(spy.lastBody.Documents) != 2 {
		t.Fatalf("want 2 documents (input+output), got %d", len(spy.lastBody.Documents))
	}
	roles := map[string]string{}
	for _, d := range spy.lastBody.Documents {
		if d.Metadata.TenantID != "tenant-A" || d.Metadata.NodeID != "n1" {
			t.Errorf("metadata not propagated: %+v", d)
		}
		roles[d.Metadata.Role] = d.Content
	}
	if roles["user"] != "为什么延期" || roles["assistant"] != "因为依赖未就绪" {
		t.Errorf("input/output projection broken: %v", roles)
	}
}

// TestIndexer_WithRemoteWriterErrorDoesNotMark 远程写入失败绝不能标记进度，
// 否则该节点被永久跳过、记忆丢失（写路径错误上抛 → 整批退避 → 下轮重试）。
func TestIndexer_WithRemoteWriterErrorDoesNotMark(t *testing.T) {
	spy := newRemoteAPISpy(http.StatusInternalServerError, http.StatusOK)
	srv := httptest.NewServer(spy.handler())
	defer srv.Close()

	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "in", "out")}}
	ix := New(&RemoteWriter{BaseURL: srv.URL, Client: srv.Client()}, st, Config{
		Collection: "agent_memory", Dim: 1536,
		BatchSize: 16, BatchDelay: time.Nanosecond, PollInterval: time.Millisecond,
	})
	if _, err := ix.RunOnce(context.Background()); err == nil {
		t.Fatal("expected write error to surface")
	}
	if st.hasMarked("n1") {
		t.Fatal("node must NOT be marked when remote write failed")
	}
	if ix.FailureCount("n1") == 0 {
		t.Error("expected failure recorded for backoff")
	}
}

// ==== ShadowWriter：双写对比语义 ====

// fakeWriter 是 ShadowWriter 测试的后端替身。
type fakeWriter struct {
	ensureErr   error
	writeErr    error
	ensureCalls int
	writeCalls  int
	lastDocs    []Document
	lastColl    string
}

func (f *fakeWriter) EnsureCollection(context.Context, string, int) error {
	f.ensureCalls++
	return f.ensureErr
}

func (f *fakeWriter) Write(_ context.Context, coll string, docs []Document) error {
	f.writeCalls++
	f.lastColl = coll
	f.lastDocs = docs
	return f.writeErr
}

func shadowDocs() []Document { return testDocs() }

// TestShadowWriter_PrimaryResultWins 直连为准：Primary 成功即返回 nil，
// Secondary 的结果只进日志，绝不改变返回值。
func TestShadowWriter_PrimaryResultWins(t *testing.T) {
	prim := &fakeWriter{}
	sec := &fakeWriter{}
	w := &ShadowWriter{Primary: prim, Secondary: sec}
	if err := w.Write(context.Background(), "agent_memory", shadowDocs()); err != nil {
		t.Fatalf("primary ok must yield nil, got %v", err)
	}
	if prim.writeCalls != 1 || sec.writeCalls != 1 {
		t.Fatalf("both backends must be written: prim=%d sec=%d", prim.writeCalls, sec.writeCalls)
	}
}

// TestShadowWriter_PrimaryFailurePropagates Primary 失败必须上抛
// （索引器据此退避重试），Secondary 成功与否不改变结论。
func TestShadowWriter_PrimaryFailurePropagates(t *testing.T) {
	prim := &fakeWriter{writeErr: errors.New("qdrant down")}
	sec := &fakeWriter{}
	w := &ShadowWriter{Primary: prim, Secondary: sec}
	if err := w.Write(context.Background(), "agent_memory", shadowDocs()); err == nil {
		t.Fatal("primary failure must propagate")
	}
}

// TestShadowWriter_SecondaryFailureIsWarnOnly Secondary 失败仅告警：
// 影子期内线上行为与 local 后端完全一致，远程问题暴露在日志里而非故障里。
func TestShadowWriter_SecondaryFailureIsWarnOnly(t *testing.T) {
	prim := &fakeWriter{}
	sec := &fakeWriter{writeErr: errors.New("rag-api down")}
	w := &ShadowWriter{Primary: prim, Secondary: sec}
	if err := w.Write(context.Background(), "agent_memory", shadowDocs()); err != nil {
		t.Fatalf("secondary failure must not fail the batch, got %v", err)
	}
	if prim.writeCalls != 1 {
		t.Fatalf("primary must still have written, got %d calls", prim.writeCalls)
	}
}

// TestShadowWriter_BothFailReportsPrimary 双失败时错误以 Primary 为准
// （重试语义由主路径决定），Secondary 错误仅在诊断日志中。
func TestShadowWriter_BothFailReportsPrimary(t *testing.T) {
	primErr := errors.New("primary error")
	prim := &fakeWriter{writeErr: primErr}
	sec := &fakeWriter{writeErr: errors.New("secondary error")}
	w := &ShadowWriter{Primary: prim, Secondary: sec}
	err := w.Write(context.Background(), "agent_memory", shadowDocs())
	if !errors.Is(err, primErr) {
		t.Fatalf("want primary error, got %v", err)
	}
}

// TestShadowWriter_NilSecondaryDegradesToLocal Secondary 缺席时退化为纯本地，
// 影子是观测手段，不该因对比对象缺席而丢写入。
func TestShadowWriter_NilSecondaryDegradesToLocal(t *testing.T) {
	prim := &fakeWriter{}
	w := &ShadowWriter{Primary: prim}
	if err := w.Write(context.Background(), "agent_memory", shadowDocs()); err != nil {
		t.Fatalf("nil secondary must degrade to local, got %v", err)
	}
	if prim.writeCalls != 1 {
		t.Fatalf("primary must be written, got %d calls", prim.writeCalls)
	}
	if err := w.EnsureCollection(context.Background(), "agent_memory", 8); err != nil {
		t.Fatalf("ensure with nil secondary must pass, got %v", err)
	}
}

// TestShadowWriter_EnsureCollectionRequiresBoth 影子对比的前提是两路都活着：
// 任一后端 EnsureCollection 失败都应启动即失败，带病影子毫无验收价值。
func TestShadowWriter_EnsureCollectionRequiresBoth(t *testing.T) {
	// Primary 失败。
	w := &ShadowWriter{Primary: &fakeWriter{ensureErr: errors.New("no qdrant")}, Secondary: &fakeWriter{}}
	if err := w.EnsureCollection(context.Background(), "agent_memory", 8); err == nil {
		t.Fatal("primary ensure failure must fail startup")
	}
	// Secondary 失败。
	w = &ShadowWriter{Primary: &fakeWriter{}, Secondary: &fakeWriter{ensureErr: errors.New("rag-api down")}}
	if err := w.EnsureCollection(context.Background(), "agent_memory", 8); err == nil {
		t.Fatal("secondary ensure failure must fail startup")
	}
	// 两者就绪 → 通过。
	w = &ShadowWriter{Primary: &fakeWriter{}, Secondary: &fakeWriter{}}
	if err := w.EnsureCollection(context.Background(), "agent_memory", 8); err != nil {
		t.Fatalf("both ready must pass, got %v", err)
	}
}

// TestShadowWriter_EmptyBatchNoop 空批次不触达任何后端。
func TestShadowWriter_EmptyBatchNoop(t *testing.T) {
	prim := &fakeWriter{}
	sec := &fakeWriter{}
	w := &ShadowWriter{Primary: prim, Secondary: sec}
	if err := w.Write(context.Background(), "agent_memory", nil); err != nil {
		t.Fatalf("empty batch must be no-op, got %v", err)
	}
	if prim.writeCalls != 0 || sec.writeCalls != 0 {
		t.Fatalf("empty batch must not write: prim=%d sec=%d", prim.writeCalls, sec.writeCalls)
	}
}

// TestShadowWriter_NotConfigured Primary 缺失时报配置错误。
func TestShadowWriter_NotConfigured(t *testing.T) {
	w := &ShadowWriter{}
	if err := w.Write(context.Background(), "c", shadowDocs()); err == nil {
		t.Fatal("expected config error")
	}
	if err := w.EnsureCollection(context.Background(), "c", 8); err == nil {
		t.Fatal("expected config error")
	}
}
