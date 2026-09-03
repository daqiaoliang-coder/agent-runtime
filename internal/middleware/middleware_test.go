package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
	"testing"
)

type testLifecycle struct{ calls *[]string }

func (m testLifecycle) OnRunStart(context.Context, contracts.ExecutionContext) error {
	*m.calls = append(*m.calls, "start")
	return nil
}
func (m testLifecycle) OnRunFinish(context.Context, contracts.ExecutionContext, error) error {
	*m.calls = append(*m.calls, "finish")
	return nil
}

type testTool struct{ calls *[]string }

func (m testTool) Before(context.Context, contracts.ExecutionContext, contracts.ToolCallRequest) (contracts.ToolCallRequest, error) {
	*m.calls = append(*m.calls, "before")
	return contracts.ToolCallRequest{Name: "wrapped"}, nil
}
func (m testTool) After(context.Context, contracts.ExecutionContext, contracts.ToolCallRequest, contracts.ToolResult) (contracts.ToolResult, error) {
	*m.calls = append(*m.calls, "after")
	return contracts.ToolResult{Output: "ok"}, nil
}

func TestChains_Order(t *testing.T) {
	calls := []string{}
	lc := NewLifecycleChain(testLifecycle{&calls})
	if err := lc.OnRunStart(context.Background(), contracts.ExecutionContext{}); err != nil {
		t.Fatal(err)
	}
	if err := lc.OnRunFinish(context.Background(), contracts.ExecutionContext{}, nil); err != nil {
		t.Fatal(err)
	}
	if got := len(calls); got != 2 || calls[0] != "start" || calls[1] != "finish" {
		t.Fatalf("calls=%v", calls)
	}

	calls = nil
	tc := NewToolChain(testTool{&calls})
	req, err := tc.Before(context.Background(), contracts.ExecutionContext{}, contracts.ToolCallRequest{Name: "raw"})
	if err != nil || req.Name != "wrapped" {
		t.Fatalf("before req=%+v err=%v", req, err)
	}
	res, err := tc.After(context.Background(), contracts.ExecutionContext{}, req, contracts.ToolResult{})
	if err != nil || res.Output != "ok" {
		t.Fatalf("after res=%+v err=%v", res, err)
	}
	if len(calls) != 2 || calls[0] != "before" || calls[1] != "after" {
		t.Fatalf("calls=%v", calls)
	}
}
