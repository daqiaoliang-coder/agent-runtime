package providers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"agent-runtime/internal/contracts"
)

// remoteRecorder 记录假 rag-api 收到的请求，供契约断言。
type remoteRecorder struct {
	calls int
	path  string
	auth  string
	body  remoteSearchRequest
}

func (rec *remoteRecorder) record(r *http.Request) {
	b, _ := io.ReadAll(r.Body)
	rec.calls++
	rec.path = r.URL.Path
	rec.auth = r.Header.Get("Authorization")
	_ = json.Unmarshal(b, &rec.body)
}

// newRemoteTestServer 启动固定响应的假 rag-api；delay>0 时模拟慢响应。
func newRemoteTestServer(t *testing.T, status int, body string, delay time.Duration) (*httptest.Server, *remoteRecorder) {
	t.Helper()
	rec := &remoteRecorder{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec.record(r)
		if delay > 0 {
			time.Sleep(delay)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv, rec
}

const remoteOKBody = `{"results":[
  {"role":"assistant","content":"历史结论A","node_id":"n1","run_id":"run-1","score":0.93,"created_at":"2026-01-01T10:00:00Z"},
  {"role":"user","content":"早先问题B","node_id":"n2","run_id":"run-1","score":0.90,"created_at":"2026-01-01T11:00:00Z"},
  {"role":"weird","content":"","node_id":"n3","run_id":"run-1","score":0.88,"created_at":"2026-01-01T12:00:00Z"},
  {"role":"weird","content":"未知角色归user","node_id":"n4","run_id":"run-1","score":0.87,"created_at":"2026-01-01T13:00:00Z"}
],"timings":{"embed_ms":10,"search_ms":20,"total_ms":30},"degraded":[]}`

// TestRemoteMemory_SearchSuccess 验证成功路径的消息映射（顺序保留、空内容
// 跳过、未知角色归 user）与请求契约（profile/scope/exclude_run_id/top_k/min_score/鉴权头）。
func TestRemoteMemory_SearchSuccess(t *testing.T) {
	srv, rec := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 0)
	m := &RemoteMemory{BaseURL: srv.URL, Token: "secret", Client: srv.Client(), TopK: 7, MinScore: 0.5}

	got, err := m.Search(context.Background(), ec("tenant-A", "thread-1", "run-9"), "查询", 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 messages (empty content skipped), got %d", len(got))
	}
	if got[0].Role != contracts.RoleAssistant || got[0].Content != "历史结论A" {
		t.Errorf("msg[0] mismatch: %+v", got[0])
	}
	if got[1].Role != contracts.RoleUser || got[1].Content != "早先问题B" {
		t.Errorf("msg[1] mismatch: %+v", got[1])
	}
	if got[2].Role != contracts.RoleUser || got[2].Content != "未知角色归user" {
		t.Errorf("msg[2] should normalize unknown role to user: %+v", got[2])
	}

	// 请求契约：字段名与语义必须与 rag-api 对齐，这是影子对比的物理前提。
	if rec.calls != 1 || rec.path != "/v1/search" {
		t.Fatalf("expected 1 call to /v1/search, got calls=%d path=%s", rec.calls, rec.path)
	}
	if rec.auth != "Bearer secret" {
		t.Errorf("auth header mismatch: %q", rec.auth)
	}
	if rec.body.Query != "查询" || rec.body.Profile != "memory" {
		t.Errorf("query/profile mismatch: %+v", rec.body)
	}
	if rec.body.Scope.TenantID != "tenant-A" || rec.body.Scope.ThreadID != "thread-1" {
		t.Errorf("scope mismatch: %+v", rec.body.Scope)
	}
	if rec.body.ExcludeRunID != "run-9" {
		t.Errorf("exclude_run_id mismatch: %q", rec.body.ExcludeRunID)
	}
	if rec.body.TopK != 7 {
		t.Errorf("top_k should resolve to field TopK=7, got %d", rec.body.TopK)
	}
	if rec.body.MinScore == nil || *rec.body.MinScore != 0.5 {
		t.Errorf("min_score must be sent explicitly (0.5), got %v", rec.body.MinScore)
	}
}

// TestRemoteMemory_MinScoreExplicitZero MinScore=0（不过滤）也必须显式发送：
// 服务端对 nil 用自己的缺省阈值 0.7，不显式传递会造成两路行为漂移。
func TestRemoteMemory_MinScoreExplicitZero(t *testing.T) {
	srv, rec := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 0)
	m := &RemoteMemory{BaseURL: srv.URL, Client: srv.Client(), MinScore: 0}

	if _, err := m.Search(context.Background(), ec("t", "th", "r"), "q", 5); err != nil {
		t.Fatalf("search: %v", err)
	}
	if rec.body.MinScore == nil || *rec.body.MinScore != 0 {
		t.Errorf("min_score=0 must be sent explicitly, got %v", rec.body.MinScore)
	}
}

// TestRemoteMemory_TopKFallback topK 参数与字段均未配置时回退默认值。
func TestRemoteMemory_TopKFallback(t *testing.T) {
	srv, rec := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 0)
	m := &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()}

	if _, err := m.Search(context.Background(), ec("t", "th", "r"), "q", 0); err != nil {
		t.Fatalf("search: %v", err)
	}
	if rec.body.TopK != DefaultMemoryTopK {
		t.Errorf("top_k should fall back to DefaultMemoryTopK=%d, got %d", DefaultMemoryTopK, rec.body.TopK)
	}
}

// TestRemoteMemory_SearchDegrades 传输层与服务端错误必须降级为 (nil,nil)，
// 与 VectorMemory 契约一致，绝不让 Run 因记忆故障失败。
func TestRemoteMemory_SearchDegrades(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"server 500", http.StatusInternalServerError, `{"error":"boom"}`},
		{"unauthorized", http.StatusUnauthorized, `{"error":"unauthorized"}`},
		{"malformed json", http.StatusOK, `not-json`},
		{"degraded embed", http.StatusOK, `{"results":[],"timings":{},"degraded":["embed_failed"]}`},
		{"degraded vector", http.StatusOK, `{"results":[],"timings":{},"degraded":["vector_failed"]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv, _ := newRemoteTestServer(t, tc.status, tc.body, 0)
			m := &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()}
			got, err := m.Search(ctx, ec("t", "th", "r"), "q", 5)
			if err != nil {
				t.Fatalf("must degrade to (nil,nil), got error: %v", err)
			}
			if got != nil {
				t.Fatalf("must degrade to nil messages, got %+v", got)
			}
		})
	}
}

// TestRemoteMemory_NetworkError 服务器不可达时同样降级。
func TestRemoteMemory_NetworkError(t *testing.T) {
	srv, _ := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 0)
	url := srv.URL
	srv.Close() // 立即关闭，制造连接拒绝

	m := &RemoteMemory{BaseURL: url, Client: &http.Client{Timeout: time.Second}}
	got, err := m.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil || got != nil {
		t.Fatalf("must degrade to (nil,nil), got (%v, %v)", got, err)
	}
}

// TestRemoteMemory_Timeout 超出客户端超时必须降级且不阻塞。
func TestRemoteMemory_Timeout(t *testing.T) {
	srv, _ := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 300*time.Millisecond)
	m := &RemoteMemory{BaseURL: srv.URL, Client: &http.Client{Timeout: 30 * time.Millisecond}}

	start := time.Now()
	got, err := m.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("timeout must bound the call, took %v", elapsed)
	}
	if err != nil || got != nil {
		t.Fatalf("must degrade to (nil,nil), got (%v, %v)", got, err)
	}
}

// TestRemoteMemory_EmptyInputSkipsHTTP 空查询/空租户不发请求：
// 等价 VectorMemory 的入参检查，返回"合法空"而非降级。
func TestRemoteMemory_EmptyInputSkipsHTTP(t *testing.T) {
	srv, rec := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 0)
	m := &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()}

	for name, q := range map[string]string{"empty query": "", "query": "q"} {
		ecArg := ec("tenant", "th", "r")
		if name == "query" {
			ecArg = ec("", "th", "r") // empty tenant
		}
		got, err := m.Search(context.Background(), ecArg, q, 5)
		if err != nil || got != nil {
			t.Fatalf("%s: must return (nil,nil), got (%v, %v)", name, got, err)
		}
	}
	if rec.calls != 0 {
		t.Errorf("no HTTP request should be issued, got %d", rec.calls)
	}
}

// TestRemoteMemory_NotConfigured BaseURL/Client 缺失视为未启用（同 VectorMemory 缺依赖）。
func TestRemoteMemory_NotConfigured(t *testing.T) {
	m := &RemoteMemory{}
	if got, err := m.Search(context.Background(), ec("t", "th", "r"), "q", 5); err != nil || got != nil {
		t.Fatalf("must degrade to (nil,nil), got (%v, %v)", got, err)
	}
}

// TestRemoteMemory_LoadSave Load 无法表达语义召回返回空；Save 必须显式报错。
func TestRemoteMemory_LoadSave(t *testing.T) {
	m := &RemoteMemory{BaseURL: "http://x", Client: &http.Client{}}
	if msgs, err := m.Load(context.Background(), ec("t", "th", "r")); err != nil || msgs != nil {
		t.Fatalf("Load must return (nil,nil), got (%v, %v)", msgs, err)
	}
	if err := m.Save(context.Background(), ec("t", "th", "r"), nil); err == nil {
		t.Fatal("Save must return an explicit error (read-only)")
	}
}

// TestRemoteMemory_NoAuthHeaderWithoutToken 未配置令牌时不发送鉴权头
// （对应 rag-api 关闭鉴权的内网/本地部署形态）。
func TestRemoteMemory_NoAuthHeaderWithoutToken(t *testing.T) {
	srv, rec := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 0)
	m := &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()}

	if _, err := m.Search(context.Background(), ec("t", "th", "r"), "q", 5); err != nil {
		t.Fatalf("search: %v", err)
	}
	if rec.auth != "" {
		t.Errorf("no Authorization header expected, got %q", rec.auth)
	}
}
