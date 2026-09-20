package tool

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/model"
	"context"
	"errors"
	"testing"
)

type fakeNodeStore struct {
	node *model.Node
	err  error
}

func (f fakeNodeStore) GetNode(_ context.Context, _, _ string) (*model.Node, error) {
	return f.node, f.err
}

func withEC(ctx context.Context, tenant string) context.Context {
	return contracts.WithExecutionContext(ctx, contracts.ExecutionContext{TenantID: tenant, RunID: "r1", NodeID: "n1"})
}

func TestFetchToolResult_Name(t *testing.T) {
	if (FetchToolResult{}).Name() != "fetch_tool_result" {
		t.Error("unexpected name")
	}
}

func TestFetchToolResult_HappyPath(t *testing.T) {
	s := fakeNodeStore{node: &model.Node{ID: "t1", Type: model.NodeTool, Status: model.NodeSuccess, Output: "big-result"}}
	got, err := FetchToolResult{Store: s}.Execute(withEC(context.Background(), "tenant-A"), "t1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != "big-result" {
		t.Errorf("expected big-result, got %q", got)
	}
}

func TestFetchToolResult_MissingContext(t *testing.T) {
	_, err := FetchToolResult{Store: fakeNodeStore{}}.Execute(context.Background(), "t1")
	if err == nil {
		t.Fatal("expected error without ExecutionContext")
	}
}

func TestFetchToolResult_EmptyTenant(t *testing.T) {
	ctx := contracts.WithExecutionContext(context.Background(), contracts.ExecutionContext{})
	_, err := FetchToolResult{Store: fakeNodeStore{}}.Execute(ctx, "t1")
	if err == nil {
		t.Fatal("expected error with empty tenant")
	}
}

func TestFetchToolResult_EmptyNodeID(t *testing.T) {
	_, err := FetchToolResult{Store: fakeNodeStore{}}.Execute(withEC(context.Background(), "t"), "  ")
	if err == nil {
		t.Fatal("expected error for empty node_id")
	}
}

func TestFetchToolResult_NodeNotFound(t *testing.T) {
	s := fakeNodeStore{err: errors.New("not found")}
	_, err := FetchToolResult{Store: s}.Execute(withEC(context.Background(), "t"), "missing")
	if err == nil {
		t.Fatal("expected error for missing node")
	}
}

func TestFetchToolResult_WrongType(t *testing.T) {
	s := fakeNodeStore{node: &model.Node{ID: "l1", Type: model.NodeLLM, Status: model.NodeSuccess, Output: "answer"}}
	_, err := FetchToolResult{Store: s}.Execute(withEC(context.Background(), "t"), "l1")
	if err == nil {
		t.Fatal("expected error for non-TOOL node")
	}
}

func TestFetchToolResult_NotSuccess(t *testing.T) {
	s := fakeNodeStore{node: &model.Node{ID: "t2", Type: model.NodeTool, Status: "RUNNING", Output: ""}}
	_, err := FetchToolResult{Store: s}.Execute(withEC(context.Background(), "t"), "t2")
	if err == nil {
		t.Fatal("expected error for non-SUCCESS node")
	}
}
