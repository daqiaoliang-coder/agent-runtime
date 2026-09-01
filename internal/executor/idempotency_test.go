package executor

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/tool"
	"context"
	"database/sql"
	"fmt"
	"strings"
	"testing"
)

// fakeToolStore 在内存中模拟 tool_call 表的状态机，供执行器幂等逻辑单测。
type fakeToolStore struct {
	calls map[string]*model.ToolCall // keyed by callID（= idempotencyKey）
}

func newFakeToolStore() *fakeToolStore { return &fakeToolStore{calls: map[string]*model.ToolCall{}} }

func (f *fakeToolStore) ClaimToolCall(_ context.Context, tenant, callID, runID, nodeID, toolName, idempotencyKey, input string, attempt int) (bool, error) {
	if _, ok := f.calls[callID]; ok {
		return false, nil // 已存在
	}
	f.calls[callID] = &model.ToolCall{CallID: callID, TenantID: tenant, RunID: runID, NodeID: nodeID, ToolName: toolName, IdempotencyKey: idempotencyKey, Status: "RUNNING", Output: "", Attempt: attempt}
	return true, nil
}
func (f *fakeToolStore) GetToolCall(_ context.Context, tenant, idempotencyKey string) (*model.ToolCall, error) {
	tc, ok := f.calls[idempotencyKey]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return tc, nil
}
func (f *fakeToolStore) ReclaimToolCall(_ context.Context, tenant, callID string) (bool, error) {
	tc, ok := f.calls[callID]
	if !ok || tc.Status != "FAILED" {
		return false, nil
	}
	tc.Status = "RUNNING"
	tc.Attempt++
	return true, nil
}
func (f *fakeToolStore) CompleteToolCall(_ context.Context, tenant, callID, output string) error {
	tc, ok := f.calls[callID]
	if !ok || tc.Status != "RUNNING" {
		return nil
	}
	tc.Status = "SUCCESS"
	tc.Output = output
	return nil
}
func (f *fakeToolStore) FailToolCall(_ context.Context, tenant, callID string) error {
	tc, ok := f.calls[callID]
	if !ok || tc.Status != "RUNNING" {
		return nil
	}
	tc.Status = "FAILED"
	return nil
}

// scriptedTool 按预设脚本返回结果，便于测试失败/成功序列与执行计数。
type scriptedTool struct {
	name    string
	outputs []string
	errs    []error
	calls   int
}

func (s *scriptedTool) Name() string { return s.name }
func (s *scriptedTool) Execute(_ context.Context, _ string) (string, error) {
	i := s.calls
	s.calls++
	if i >= len(s.outputs) {
		return "", fmt.Errorf("scriptedTool: no more scripted outputs")
	}
	return s.outputs[i], s.errs[i]
}

func newToolNode() *model.Node {
	return &model.Node{ID: "n1", RunID: "r1", TenantID: "t1", Type: model.NodeTool, Name: "search", Input: "q", Attempt: 0}
}

// TestExecutor_ToolIdempotent_SuccessCached 命中 SUCCESS 时复用结果，不重复执行副作用。
func TestExecutor_ToolIdempotent_SuccessCached(t *testing.T) {
	store := newFakeToolStore()
	tl := &scriptedTool{name: "search", outputs: []string{"result-1"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolStore: store}
	n := newToolNode()

	out1, err := d.Execute(context.Background(), n)
	if err != nil || out1 != "result-1" {
		t.Fatalf("first exec: out=%q err=%v", out1, err)
	}
	if tl.calls != 1 {
		t.Fatalf("expected 1 tool exec, got %d", tl.calls)
	}
	// 第二次：命中 SUCCESS，应复用缓存，不再次执行。
	out2, err := d.Execute(context.Background(), n)
	if err != nil || out2 != "result-1" {
		t.Fatalf("second exec: out=%q err=%v", out2, err)
	}
	if tl.calls != 1 {
		t.Fatalf("SUCCESS should be cached; tool re-executed, calls=%d", tl.calls)
	}
}

// TestExecutor_ToolIdempotent_FailedThenRetry 失败后回收重试一次（脚本先错后对）。
func TestExecutor_ToolIdempotent_FailedThenRetry(t *testing.T) {
	store := newFakeToolStore()
	tl := &scriptedTool{name: "search", outputs: []string{"", "result-2"}, errs: []error{fmt.Errorf("transient"), nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolStore: store}
	n := newToolNode()

	// 第一次：工具失败 → 落 FAILED，返回错误。
	_, err := d.Execute(context.Background(), n)
	if err == nil || !strings.Contains(err.Error(), "transient") {
		t.Fatalf("first exec should fail, got %v", err)
	}
	if tl.calls != 1 {
		t.Fatalf("expected 1 call after failure, got %d", tl.calls)
	}
	// 第二次：命中 FAILED → 回收为 RUNNING → 重试 → 成功。
	out2, err := d.Execute(context.Background(), n)
	if err != nil || out2 != "result-2" {
		t.Fatalf("retry exec: out=%q err=%v", out2, err)
	}
	if tl.calls != 2 {
		t.Fatalf("expected 2 total calls after retry, got %d", tl.calls)
	}
}

// TestExecutor_ToolIdempotent_StaleRunningRefuses 停滞 RUNNING 拒绝重执行（非幂等工具安全）。
func TestExecutor_ToolIdempotent_StaleRunningRefuses(t *testing.T) {
	store := newFakeToolStore()
	tl := &scriptedTool{name: "search", outputs: []string{"should-not-run"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolStore: store}
	n := newToolNode()

	// 预置一个停滞 RUNNING 调用，模拟崩溃在途。
	key := idempotencyKey(n.RunID, n.ID, n.Name, n.Input)
	store.calls[key] = &model.ToolCall{CallID: key, TenantID: n.TenantID, Status: "RUNNING"}

	_, err := d.Execute(context.Background(), n)
	if err == nil || !strings.Contains(err.Error(), "stale RUNNING") {
		t.Fatalf("expected stale-RUNNING refusal, got %v", err)
	}
	if tl.calls != 0 {
		t.Fatalf("tool must NOT execute on stale RUNNING, calls=%d", tl.calls)
	}
}

func mustReg(tl *scriptedTool) *tool.Registry {
	r := tool.NewRegistry()
	r.Register(tl)
	return r
}
