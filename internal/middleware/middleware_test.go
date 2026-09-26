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

// tagModel 在请求/响应的文本上追加自己的标记，用于同时观测执行次序与值传递方向。
type tagModel struct {
	tag   string
	calls *[]string
}

func (m tagModel) Before(_ context.Context, _ contracts.ExecutionContext, req contracts.GenerateRequest) (contracts.GenerateRequest, error) {
	*m.calls = append(*m.calls, "before:"+m.tag)
	if len(req.Messages) > 0 {
		req.Messages[len(req.Messages)-1].Content += "+" + m.tag
	}
	return req, nil
}

func (m tagModel) After(_ context.Context, _ contracts.ExecutionContext, _ contracts.GenerateRequest, resp contracts.GenerateResponse) (contracts.GenerateResponse, error) {
	*m.calls = append(*m.calls, "after:"+m.tag)
	resp.Message.Content += "+" + m.tag
	return resp, nil
}

// TestModelChain_OrderingAndThreading 校验 ModelChain 的两条结构性约束：
//   - Before 正序、After 倒序（onion），与 ToolChain/LifecycleChain 同构；
//   - 改写必须沿正确方向传递：Before 的改写向后流到下一个中间件与模型，
//     After 的改写向前流回调用方（后注册者先改写）。
//
// 单条链只能证明"被调用了"，证明不了顺序；顺序错了（如 After 也正序）
// 在配对型中间件下会出现状态错配，且这类 bug 只在多条链叠加时才暴露。
func TestModelChain_OrderingAndThreading(t *testing.T) {
	calls := []string{}
	mc := NewModelChain(tagModel{"a", &calls}, tagModel{"b", &calls})

	req := contracts.GenerateRequest{Messages: []contracts.Message{{Role: contracts.RoleUser, Content: "q"}}}
	req, err := mc.Before(context.Background(), contracts.ExecutionContext{}, req)
	if err != nil {
		t.Fatal(err)
	}
	if got := req.Messages[0].Content; got != "q+a+b" {
		t.Errorf("Before rewrites must flow forward: content=%q, want %q", got, "q+a+b")
	}

	resp, err := mc.After(context.Background(), contracts.ExecutionContext{}, req, contracts.GenerateResponse{
		Message: contracts.Message{Role: contracts.RoleAssistant, Content: "r"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := resp.Message.Content; got != "r+b+a" {
		t.Errorf("After rewrites must flow backward: content=%q, want %q", got, "r+b+a")
	}

	want := []string{"before:a", "before:b", "after:b", "after:a"}
	if len(calls) != len(want) {
		t.Fatalf("calls=%v, want %v", calls, want)
	}
	for i := range want {
		if calls[i] != want[i] {
			t.Fatalf("calls=%v, want %v", calls, want)
		}
	}
}

// TestModelChain_EmptyIsTransparent 空链（nil items）必须完全透明：
// 装配层按配置决定是否挂载中间件，关闭时不应改变请求与响应。
func TestModelChain_EmptyIsTransparent(t *testing.T) {
	mc := NewModelChain()
	req := contracts.GenerateRequest{Model: "m", Messages: []contracts.Message{{Role: contracts.RoleUser, Content: "q"}}}
	got, err := mc.Before(context.Background(), contracts.ExecutionContext{}, req)
	if err != nil {
		t.Fatal(err)
	}
	if got.Model != "m" || len(got.Messages) != 1 || got.Messages[0].Content != "q" {
		t.Errorf("empty chain altered request: %+v", got)
	}
	resp, err := mc.After(context.Background(), contracts.ExecutionContext{}, req, contracts.GenerateResponse{
		Message: contracts.Message{Content: "r"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Message.Content != "r" {
		t.Errorf("empty chain altered response: %+v", resp)
	}
}
