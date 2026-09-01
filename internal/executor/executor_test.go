package executor

import (
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"agent-runtime/internal/tool"
	"context"
	"errors"
	"strings"
	"testing"
)

// TestDispatcher_LLMNode_CallsClient 验证 LLM 节点走 LLM 客户端分支。
func TestDispatcher_LLMNode_CallsClient(t *testing.T) {
	called := false
	stub := &llm.Stub{Responder: func(req llm.Request) string {
		called = true
		if len(req.Messages) == 0 || req.Messages[0].Role != llm.RoleUser {
			t.Errorf("expected user message, got %+v", req.Messages)
		}
		return "hello from llm"
	}}
	d := &Dispatcher{LLM: stub}
	out, err := d.Execute(context.Background(), &model.Node{Type: model.NodeLLM, Input: "hi"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !called {
		t.Error("expected LLM client to be called")
	}
	if out != "hello from llm" {
		t.Errorf("output = %q, want %q", out, "hello from llm")
	}
}

// TestDispatcher_LLM_PrependsContextHistory LLM 节点应从 ContextLoader 重建对话历史并前置。
func TestDispatcher_LLM_PrependsContextHistory(t *testing.T) {
	var got []llm.Message
	stub := &llm.Stub{Responder: func(req llm.Request) string {
		got = req.Messages
		return "ok"
	}}
	d := &Dispatcher{LLM: stub, ContextLoader: func(_ context.Context, _ string) ([]llm.Message, error) {
		return []llm.Message{{Role: llm.RoleAssistant, Content: "prior"}}, nil
	}}
	if _, err := d.Execute(context.Background(), &model.Node{Type: model.NodeLLM, Input: "next", RunID: "run-1"}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 2 || got[0].Content != "prior" || got[1].Content != "next" {
		t.Errorf("expected [prior, next], got %+v", got)
	}
}

// TestDispatcher_LLM_ContextLoaderError_Propagates ContextLoader 报错应传播，不静默吞掉。
func TestDispatcher_LLM_ContextLoaderError_Propagates(t *testing.T) {
	d := &Dispatcher{LLM: llm.Echo(), ContextLoader: func(_ context.Context, _ string) ([]llm.Message, error) {
		return nil, errors.New("db down")
	}}
	_, err := d.Execute(context.Background(), &model.Node{Type: model.NodeLLM, Input: "x", RunID: "r"})
	if err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("expected context load error, got %v", err)
	}
}

// TestDispatcher_ToolNode_InvokesTool 验证 TOOL 节点按 name 查 Registry 并执行。
func TestDispatcher_ToolNode_InvokesTool(t *testing.T) {
	reg := tool.NewRegistry()
	reg.Register(tool.Calculator{})
	d := &Dispatcher{Tools: reg}
	out, err := d.Execute(context.Background(), &model.Node{Type: model.NodeTool, Name: "calculator", Input: "2 + 3"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if out != "5" {
		t.Errorf("output = %q, want 5", out)
	}
}

// TestDispatcher_ToolNode_NotFound 未注册工具应返回错误，而非静默成功。
func TestDispatcher_ToolNode_NotFound(t *testing.T) {
	d := &Dispatcher{Tools: tool.NewRegistry()}
	_, err := d.Execute(context.Background(), &model.Node{Type: model.NodeTool, Name: "missing"})
	if err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("expected not-found error, got %v", err)
	}
}

// TestDispatcher_ToolError_Propagates 工具内部错误应被包装传播，供 worker 转为失败事件。
func TestDispatcher_ToolError_Propagates(t *testing.T) {
	reg := tool.NewRegistry()
	reg.Register(tool.Calculator{})
	d := &Dispatcher{Tools: reg}
	_, err := d.Execute(context.Background(), &model.Node{Type: model.NodeTool, Name: "calculator", Input: "bad"})
	if err == nil {
		t.Fatal("expected error for bad input")
	}
	if !strings.Contains(err.Error(), "calculator") {
		t.Errorf("error should wrap tool name: %v", err)
	}
}

// TestDispatcher_UnknownType_Errors 未识别类型应报错，避免静默失败。
func TestDispatcher_UnknownType_Errors(t *testing.T) {
	d := NewDefault()
	_, err := d.Execute(context.Background(), &model.Node{Type: "UNKNOWN"})
	if err == nil || !strings.Contains(err.Error(), "unknown node type") {
		t.Fatalf("expected unknown-type error, got %v", err)
	}
}

// TestDispatcher_NilConfig_Errors 缺失依赖（LLM/Registry）应显式报错而非 panic。
func TestDispatcher_NilConfig_Errors(t *testing.T) {
	d := &Dispatcher{}
	if _, err := d.Execute(context.Background(), &model.Node{Type: model.NodeLLM}); err == nil {
		t.Error("expected error when LLM client is nil")
	}
	if _, err := d.Execute(context.Background(), &model.Node{Type: model.NodeTool}); err == nil {
		t.Error("expected error when tool registry is nil")
	}
}

// TestNewDefault_Smoke 默认构造的 Dispatcher 应能执行 LLM 与工具节点。
func TestNewDefault_Smoke(t *testing.T) {
	d := NewDefault()
	if _, err := d.Execute(context.Background(), &model.Node{Type: model.NodeLLM, Input: "ping"}); err != nil {
		t.Errorf("default LLM exec: %v", err)
	}
	if out, err := d.Execute(context.Background(), &model.Node{Type: model.NodeTool, Name: "search", Input: "go"}); err != nil || !strings.Contains(out, "doc1") {
		t.Errorf("default tool exec: out=%q err=%v", out, err)
	}
}
