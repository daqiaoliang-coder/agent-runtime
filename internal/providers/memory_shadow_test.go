package providers

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"agent-runtime/internal/contracts"
)

// fakeMemorySearcher 是可编程的 MemoryProvider+MemorySearcher 替身。
// delay 模拟慢响应且**不理会 ctx**，用于验证 ShadowMemory 不被失约的
// Secondary 拖死（生产 Secondary 是尊重 ctx 的 RemoteMemory）。
type fakeMemorySearcher struct {
	msgs  []contracts.Message
	err   error
	delay time.Duration
	calls int
}

func (f *fakeMemorySearcher) Load(context.Context, contracts.ExecutionContext) ([]contracts.Message, error) {
	return nil, nil
}

func (f *fakeMemorySearcher) Save(context.Context, contracts.ExecutionContext, []contracts.Message) error {
	return nil
}

func (f *fakeMemorySearcher) Search(_ context.Context, _ contracts.ExecutionContext, _ string, _ int) ([]contracts.Message, error) {
	f.calls++
	if f.delay > 0 {
		time.Sleep(f.delay)
	}
	return f.msgs, f.err
}

var (
	msgsA = []contracts.Message{
		{Role: contracts.RoleUser, Content: "问题"},
		{Role: contracts.RoleAssistant, Content: "结论"},
	}
	msgsB = []contracts.Message{
		{Role: contracts.RoleUser, Content: "不同的结果"},
	}
)

// TestShadowMemory_AlwaysReturnsPrimary 影子对比的核心不变量：无论远程返回什么，
// 注入 LLM 的上下文恒以直连（Primary）为准。
func TestShadowMemory_AlwaysReturnsPrimary(t *testing.T) {
	prim := &fakeMemorySearcher{msgs: msgsA}
	sec := &fakeMemorySearcher{msgs: msgsB}
	s := &ShadowMemory{Primary: prim, Secondary: sec}

	got, err := s.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != len(msgsA) || got[0].Content != msgsA[0].Content || got[1].Content != msgsA[1].Content {
		t.Fatalf("must return primary result, got %+v", got)
	}
	if prim.calls != 1 || sec.calls != 1 {
		t.Errorf("both sides must run exactly once, got primary=%d secondary=%d", prim.calls, sec.calls)
	}
}

// TestShadowMemory_Match 两路一致时正常返回 primary 结果。
func TestShadowMemory_Match(t *testing.T) {
	s := &ShadowMemory{
		Primary:   &fakeMemorySearcher{msgs: msgsA},
		Secondary: &fakeMemorySearcher{msgs: msgsA},
	}
	got, err := s.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected primary's 2 messages, got %d", len(got))
	}
}

// TestShadowMemory_SecondaryError 远程失败只影响对比日志，不影响结果。
func TestShadowMemory_SecondaryError(t *testing.T) {
	s := &ShadowMemory{
		Primary:   &fakeMemorySearcher{msgs: msgsA},
		Secondary: &fakeMemorySearcher{err: errors.New("remote down")},
	}
	got, err := s.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("primary result must survive secondary failure, got %d", len(got))
	}
}

// TestShadowMemory_SecondarySlowMustNotBlock 预算耗尽后放弃等待副路：
// 失约的 Secondary 不能拖住召回主链路（800ms 预算不变）。
func TestShadowMemory_SecondarySlowMustNotBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
	defer cancel()
	s := &ShadowMemory{
		Primary:   &fakeMemorySearcher{msgs: msgsA},
		Secondary: &fakeMemorySearcher{msgs: msgsB, delay: 300 * time.Millisecond},
	}

	start := time.Now()
	got, err := s.Search(ctx, ec("t", "th", "r"), "q", 5)
	if elapsed := time.Since(start); elapsed > 250*time.Millisecond {
		t.Fatalf("search must return within ctx budget, took %v", elapsed)
	}
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("primary result must be returned, got %d", len(got))
	}
}

// TestShadowMemory_NilSecondary 无对比对象时退化为纯本地，不丢记忆。
func TestShadowMemory_NilSecondary(t *testing.T) {
	prim := &fakeMemorySearcher{msgs: msgsA}
	s := &ShadowMemory{Primary: prim}

	got, err := s.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 || prim.calls != 1 {
		t.Fatalf("must degrade to primary-only, got %d msgs, %d calls", len(got), prim.calls)
	}
}

// TestShadowMemory_RemoteSecondary Secondary 为真实 RemoteMemory 时走详细接口：
// 远程返回不同结果仍以 primary 为准，且远程确实被咨询。
func TestShadowMemory_RemoteSecondary(t *testing.T) {
	srv, rec := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 0)
	s := &ShadowMemory{
		Primary:   &fakeMemorySearcher{msgs: msgsA},
		Secondary: &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()},
	}

	got, err := s.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 || got[0].Content != msgsA[0].Content {
		t.Fatalf("must return primary result even when remote differs, got %+v", got)
	}
	if rec.calls != 1 {
		t.Errorf("remote must be consulted exactly once, got %d", rec.calls)
	}
}

// TestShadowMemory_RemoteDegraded 远程自报降级时跳过对比（避免误报"结果不一致"），
// 结果仍以 primary 为准。
func TestShadowMemory_RemoteDegraded(t *testing.T) {
	srv, _ := newRemoteTestServer(t, http.StatusOK,
		`{"results":[],"timings":{},"degraded":["embed_failed"]}`, 0)
	s := &ShadowMemory{
		Primary:   &fakeMemorySearcher{msgs: msgsA},
		Secondary: &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()},
	}

	got, err := s.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("primary result must survive remote degradation, got %d", len(got))
	}
}

// TestShadowMemory_PrimaryNotSearcher Primary 无语义检索能力时跳过召回
// （能力探测语义与 ContextLoader 一致）。
func TestShadowMemory_PrimaryNotSearcher(t *testing.T) {
	s := &ShadowMemory{Primary: plainProvider{}, Secondary: &fakeMemorySearcher{}}
	got, err := s.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil || got != nil {
		t.Fatalf("must return (nil,nil), got (%v, %v)", got, err)
	}
}

// plainProvider 只实现 MemoryProvider（providers 包内的能力探测替身）。
type plainProvider struct{}

func (plainProvider) Load(context.Context, contracts.ExecutionContext) ([]contracts.Message, error) {
	return nil, nil
}
func (plainProvider) Save(context.Context, contracts.ExecutionContext, []contracts.Message) error {
	return nil
}

// ==== FallbackMemory ====

// TestFallbackMemory_RemoteOK 远程正常时直接返回远程结果，备路不被调用。
func TestFallbackMemory_RemoteOK(t *testing.T) {
	srv, _ := newRemoteTestServer(t, http.StatusOK, remoteOKBody, 0)
	fb := &fakeMemorySearcher{msgs: msgsB}
	f := &FallbackMemory{
		Primary:  &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()},
		Fallback: fb,
	}

	got, err := f.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 3 { // remoteOKBody 的 3 条非空结果
		t.Fatalf("expected remote's 3 messages, got %d", len(got))
	}
	if fb.calls != 0 {
		t.Errorf("fallback must not be called when remote succeeds, got %d calls", fb.calls)
	}
}

// TestFallbackMemory_RemoteErrorFallsBack 远程失败（5xx）时回退本地。
func TestFallbackMemory_RemoteErrorFallsBack(t *testing.T) {
	srv, _ := newRemoteTestServer(t, http.StatusInternalServerError, `{"error":"boom"}`, 0)
	fb := &fakeMemorySearcher{msgs: msgsB}
	f := &FallbackMemory{
		Primary:  &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()},
		Fallback: fb,
	}

	got, err := f.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || got[0].Content != msgsB[0].Content {
		t.Fatalf("expected fallback result, got %+v", got)
	}
	if fb.calls != 1 {
		t.Errorf("fallback must be called once, got %d", fb.calls)
	}
}

// TestFallbackMemory_RemoteDegradedFallsBack 远程自报降级（degraded 非空）时回退本地。
func TestFallbackMemory_RemoteDegradedFallsBack(t *testing.T) {
	srv, _ := newRemoteTestServer(t, http.StatusOK,
		`{"results":[],"timings":{},"degraded":["vector_failed"]}`, 0)
	fb := &fakeMemorySearcher{msgs: msgsB}
	f := &FallbackMemory{
		Primary:  &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()},
		Fallback: fb,
	}

	got, err := f.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || got[0].Content != msgsB[0].Content {
		t.Fatalf("expected fallback result on degraded remote, got %+v", got)
	}
}

// TestFallbackMemory_EmptyRemoteResultNoFallback 远程返回空结果属于合法答案，
// **不**触发回退——否则"无记忆"会被放大成一次多余的本地全量检索。
func TestFallbackMemory_EmptyRemoteResultNoFallback(t *testing.T) {
	srv, _ := newRemoteTestServer(t, http.StatusOK, `{"results":[],"timings":{},"degraded":[]}`, 0)
	fb := &fakeMemorySearcher{msgs: msgsB}
	f := &FallbackMemory{
		Primary:  &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()},
		Fallback: fb,
	}

	got, err := f.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("expected remote's empty answer, got %+v", got)
	}
	if fb.calls != 0 {
		t.Errorf("empty result is a valid answer; fallback must not run, got %d calls", fb.calls)
	}
}

// TestFallbackMemory_NoFallbackConfigured 未配置备路时，远程失败降级为空而非报错。
func TestFallbackMemory_NoFallbackConfigured(t *testing.T) {
	srv, _ := newRemoteTestServer(t, http.StatusInternalServerError, `{"error":"boom"}`, 0)
	f := &FallbackMemory{Primary: &RemoteMemory{BaseURL: srv.URL, Client: srv.Client()}}

	got, err := f.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil || got != nil {
		t.Fatalf("must degrade to (nil,nil), got (%v, %v)", got, err)
	}
}

// TestFallbackMemory_PlainPrimaryError 非 RemoteMemory 主路仅在显式 error 时回退
// （防御分支：普通 MemorySearcher 无法区分空结果与降级）。
func TestFallbackMemory_PlainPrimaryError(t *testing.T) {
	fb := &fakeMemorySearcher{msgs: msgsB}
	f := &FallbackMemory{
		Primary:  &fakeMemorySearcher{err: errors.New("boom")},
		Fallback: fb,
	}
	got, err := f.Search(context.Background(), ec("t", "th", "r"), "q", 5)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || fb.calls != 1 {
		t.Fatalf("expected fallback on primary error, got %+v calls=%d", got, fb.calls)
	}
}

// TestShadowFallback_LoadSave 组合实现的只读语义与底层一致。
func TestShadowFallback_LoadSave(t *testing.T) {
	s := &ShadowMemory{Primary: &fakeMemorySearcher{}}
	if msgs, err := s.Load(context.Background(), ec("t", "th", "r")); err != nil || msgs != nil {
		t.Fatalf("ShadowMemory.Load must delegate to primary (nil,nil), got (%v, %v)", msgs, err)
	}
	if err := s.Save(context.Background(), ec("t", "th", "r"), nil); err == nil {
		t.Error("ShadowMemory.Save must return explicit error")
	}
	f := &FallbackMemory{Primary: &fakeMemorySearcher{}}
	if msgs, err := f.Load(context.Background(), ec("t", "th", "r")); err != nil || msgs != nil {
		t.Fatalf("FallbackMemory.Load must delegate to primary (nil,nil), got (%v, %v)", msgs, err)
	}
	if err := f.Save(context.Background(), ec("t", "th", "r"), nil); err == nil {
		t.Error("FallbackMemory.Save must return explicit error")
	}
}
