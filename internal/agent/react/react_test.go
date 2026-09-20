package react

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"context"
	"errors"
	"strings"
	"testing"
)

type scriptedRunner struct{ calls int }

func (r *scriptedRunner) RunLLM(_ context.Context, req contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	r.calls++
	if r.calls == 1 {
		// 断言：如果有 ContextLoader 加载的历史，它应该在 messages 前面。
		return contracts.GenerateResponse{
			Message:    contracts.Message{Role: contracts.RoleAssistant, Content: "ok"},
			ToolCalls:  []contracts.ToolCall{{ID: "call-1", Name: "search", Arguments: "q"}},
		}, nil
	}
	return contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant, Content: "done"}}, nil
}
func (r *scriptedRunner) RunTool(_ context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return contracts.ToolResult{CallID: req.CallID, Output: "tool-output"}, nil
}

// capturingRunner 记录第一次 RunLLM 收到的 messages，用于断言 ContextLoader 注入。
type capturingRunner struct {
	got []contracts.Message
}

func (r *capturingRunner) RunLLM(_ context.Context, req contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	r.got = req.Messages
	return contracts.GenerateResponse{
		Message: contracts.Message{Role: contracts.RoleAssistant, Content: "final"},
	}, nil
}
func (r *capturingRunner) RunTool(_ context.Context, _ contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return contracts.ToolResult{Output: "x"}, nil
}

func TestEngine_ReactsThroughToolObservation(t *testing.T) {
	r := &scriptedRunner{}
	res, err := (&Engine{Runner: r, MaxIterations: 3}).Run(context.Background(), Input{Messages: []contracts.Message{{Role: contracts.RoleUser, Content: "find"}}})
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Content != "done" || res.Iterations != 2 {
		t.Fatalf("result=%+v", res)
	}
}

func TestEngine_LoadsContextFromLoader(t *testing.T) {
	loader := func(_ context.Context, _, _, _ string) ([]llm.Message, error) {
		return []llm.Message{
			{Role: llm.RoleUser, Content: "ancestor-q"},
			{Role: llm.RoleAssistant, Content: "ancestor-a"},
		}, nil
	}
	r := &capturingRunner{}
	_, err := (&Engine{Runner: r, ContextLoader: loader}).Run(context.Background(), Input{
		ExecutionContext: contracts.ExecutionContext{TenantID: "t1", RunID: "r1", NodeID: "n1"},
		Messages:         []contracts.Message{{Role: contracts.RoleUser, Content: "current"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.got) != 3 {
		t.Fatalf("expected 3 messages (2 ancestor + 1 current), got %d", len(r.got))
	}
	if r.got[0].Content != "ancestor-q" || r.got[1].Content != "ancestor-a" {
		t.Errorf("ancestor messages should be prepended: %+v", r.got)
	}
	if r.got[2].Content != "current" {
		t.Errorf("current message should follow: %+v", r.got)
	}
}

func TestEngine_ContextLoaderError_Propagates(t *testing.T) {
	loader := func(_ context.Context, _, _, _ string) ([]llm.Message, error) {
		return nil, errors.New("db down")
	}
	r := &capturingRunner{}
	_, err := (&Engine{Runner: r, ContextLoader: loader}).Run(context.Background(), Input{
		ExecutionContext: contracts.ExecutionContext{TenantID: "t1", RunID: "r1", NodeID: "n1"},
	})
	if err == nil {
		t.Fatal("ContextLoader error must propagate")
	}
}

func TestEngine_ContextLoader_SkippedWhenECEmpty(t *testing.T) {
	called := false
	loader := func(_ context.Context, _, _, _ string) ([]llm.Message, error) {
		called = true
		return nil, nil
	}
	r := &capturingRunner{}
	_, err := (&Engine{Runner: r, ContextLoader: loader}).Run(context.Background(), Input{
		ExecutionContext: contracts.ExecutionContext{}, // 空 EC
		Messages:         []contracts.Message{{Role: contracts.RoleUser, Content: "hi"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if called {
		t.Error("ContextLoader should not be called with empty ExecutionContext")
	}
	if len(r.got) != 1 || r.got[0].Content != "hi" {
		t.Errorf("expected only the input message, got %+v", r.got)
	}
}

func TestEngine_NilContextLoader_PreservesOldBehavior(t *testing.T) {
	r := &capturingRunner{}
	_, err := (&Engine{Runner: r}).Run(context.Background(), Input{
		ExecutionContext: contracts.ExecutionContext{TenantID: "t1", RunID: "r1", NodeID: "n1"},
		Messages:         []contracts.Message{{Role: contracts.RoleUser, Content: "only"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r.got) != 1 || r.got[0].Content != "only" {
		t.Errorf("nil ContextLoader should preserve old behavior, got %+v", r.got)
	}
}

func TestEngine_MasksToolResult_OverMaxRunes(t *testing.T) {
	longOutput := strings.Repeat("x", 100)
	r2 := &longToolRunner{output: longOutput}
	eng := &Engine{
		Runner:            r2,
		MaxIterations:     3,
		ToolMasking:       true,
		ToolOutputMaxRunes: 10,
	}
	res, err := eng.Run(context.Background(), Input{
		Messages: []contracts.Message{{Role: contracts.RoleUser, Content: "q"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Message.Content != "done" {
		t.Errorf("expected done, got %q", res.Message.Content)
	}
	// r2.got 是第二次 LLM 调用的消息，其中 index 2 是 tool result
	if len(r2.got) < 3 {
		t.Fatalf("expected at least 3 messages, got %d", len(r2.got))
	}
	toolMsg := r2.got[2]
	if !strings.Contains(toolMsg.Content, "Truncated") {
		t.Errorf("tool result should be truncated, got %q", toolMsg.Content)
	}
	if !strings.Contains(toolMsg.Content, "call_id=") {
		t.Errorf("truncated result should contain call_id pointer, got %q", toolMsg.Content)
	}
	if strings.Contains(toolMsg.Content, strings.Repeat("x", 100)) {
		t.Error("full output should not be present after truncation")
	}
}

func TestEngine_NoMasking_WhenToolMaskingFalse(t *testing.T) {
	longOutput := strings.Repeat("x", 100)
	r2 := &longToolRunner{output: longOutput}
	eng := &Engine{Runner: r2, MaxIterations: 3}
	_, err := eng.Run(context.Background(), Input{
		Messages: []contracts.Message{{Role: contracts.RoleUser, Content: "q"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(r2.got) < 3 {
		t.Fatalf("expected at least 3 messages, got %d", len(r2.got))
	}
	if r2.got[2].Content != longOutput {
		t.Errorf("tool result should be unmasked without ToolMasking, got %q", r2.got[2].Content)
	}
}

// longToolRunner 第一次返回 tool call（100 字符输出），第二次返回 "done"。
type longToolRunner struct {
	output string
	got    []contracts.Message
	calls  int
}

func (r *longToolRunner) RunLLM(_ context.Context, req contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	r.calls++
	r.got = req.Messages
	if r.calls == 1 {
		return contracts.GenerateResponse{
			Message:   contracts.Message{Role: contracts.RoleAssistant, Content: "thinking"},
			ToolCalls: []contracts.ToolCall{{ID: "call-99", Name: "search", Arguments: "q"}},
		}, nil
	}
	return contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant, Content: "done"}}, nil
}
func (r *longToolRunner) RunTool(_ context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return contracts.ToolResult{CallID: req.CallID, Output: r.output}, nil
}
