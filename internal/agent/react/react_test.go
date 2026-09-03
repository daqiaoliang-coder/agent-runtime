package react

import (
	"agent-runtime/internal/contracts"
	"context"
	"testing"
)

type scriptedRunner struct{ calls int }

func (r *scriptedRunner) RunLLM(_ context.Context, _ contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	r.calls++
	if r.calls == 1 {
		return contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant}, ToolCalls: []contracts.ToolCall{{ID: "c1", Name: "search", Arguments: "go"}}}, nil
	}
	return contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant, Content: "done"}}, nil
}
func (r *scriptedRunner) RunTool(_ context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return contracts.ToolResult{CallID: req.CallID, Output: "result"}, nil
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
