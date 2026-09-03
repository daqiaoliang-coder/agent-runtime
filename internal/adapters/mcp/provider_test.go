package mcp

import (
	"agent-runtime/internal/contracts"
	"context"
	"testing"
)

type fakeClient struct{}

func (fakeClient) ListTools(context.Context) ([]contracts.ToolDefinition, error) {
	return []contracts.ToolDefinition{{Name: "search"}}, nil
}
func (fakeClient) CallTool(_ context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return contracts.ToolResult{CallID: req.CallID, Output: req.Arguments}, nil
}

func TestProvider_DelegatesToClient(t *testing.T) {
	p := New(fakeClient{})
	tools, err := p.ListTools(context.Background())
	if err != nil || len(tools) != 1 || tools[0].Name != "search" {
		t.Fatalf("tools=%+v err=%v", tools, err)
	}
	result, err := p.CallTool(context.Background(), contracts.ToolCallRequest{CallID: "1", Name: "search", Arguments: "go"})
	if err != nil || result.Output != "go" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}
