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

// fakeUsageClient 是返回固定 Usage 的 LLM 客户端，用于测试 token/cost tracking。
type fakeUsageClient struct {
	resp llm.Response
}

func (f *fakeUsageClient) Complete(_ context.Context, _ llm.Request) (llm.Response, error) {
	return f.resp, nil
}

// fakeUsageRecorder 记录收到的 LLMUsage，用于断言 executor 正确落库 token 用量。
type fakeUsageRecorder struct {
	recorded []model.LLMUsage
}

func (f *fakeUsageRecorder) RecordLLMUsage(_ context.Context, u model.LLMUsage) error {
	f.recorded = append(f.recorded, u)
	return nil
}

// TestDispatcher_LLM_RecordsUsage 配置 UsageRecorder 时，LLM 调用的 token 用量应被落库。
func TestDispatcher_LLM_RecordsUsage(t *testing.T) {
	rec := &fakeUsageRecorder{}
	d := &Dispatcher{
		LLM: &fakeUsageClient{resp: llm.Response{
			Content: "answer",
			Model:   "gpt-4o",
			Usage:   llm.Usage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
		}},
		UsageRecorder: rec,
		Pricer: func(model string, prompt, completion int) float64 {
			if model == "gpt-4o" {
				return float64(prompt)/1e6*2.5 + float64(completion)/1e6*10
			}
			return 0
		},
	}
	out, err := d.Execute(context.Background(), &model.Node{
		Type: model.NodeLLM, ID: "node-1", RunID: "run-1", TenantID: "tenant-A", Name: "gpt-4o", Input: "hi",
	})
	if err != nil || out != "answer" {
		t.Fatalf("execute: out=%q err=%v", out, err)
	}
	if len(rec.recorded) != 1 {
		t.Fatalf("expected 1 usage record, got %d", len(rec.recorded))
	}
	u := rec.recorded[0]
	if u.Model != "gpt-4o" || u.PromptTokens != 100 || u.CompletionTokens != 50 || u.TotalTokens != 150 {
		t.Errorf("unexpected usage: %+v", u)
	}
	// gpt-4o: 100/1e6*2.5 + 50/1e6*10 = 0.00025 + 0.0005 = 0.00075
	if u.Cost < 0.00074 || u.Cost > 0.00076 {
		t.Errorf("unexpected cost: %v", u.Cost)
	}
}

// TestDispatcher_LLM_NoUsage_SkipsRecording Usage.TotalTokens 为 0 时不落库（如 Stub）。
func TestDispatcher_LLM_NoUsage_SkipsRecording(t *testing.T) {
	rec := &fakeUsageRecorder{}
	d := &Dispatcher{
		LLM:           llm.Echo(),
		UsageRecorder: rec,
	}
	if _, err := d.Execute(context.Background(), &model.Node{Type: model.NodeLLM, Input: "hi"}); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(rec.recorded) != 0 {
		t.Errorf("expected no usage record for zero-token stub, got %d", len(rec.recorded))
	}
}
