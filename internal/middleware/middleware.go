// Package middleware provides small, domain-specific runtime chains for cross-cutting
// concerns. Middleware stays above the durable kernel and does not own persistence.
package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
)

// Lifecycle 是 Run 生命周期的钩子，常用于审计/指标/Tracing 注入等横切关注点。
type Lifecycle interface {
	OnRunStart(context.Context, contracts.ExecutionContext) error
	OnRunFinish(context.Context, contracts.ExecutionContext, error) error
}

// LifecycleChain 串行调用一组 Lifecycle。OnRunStart 正序执行；OnRunFinish 倒序执行，
// 形成 onion 模型以便在执行边界对称地配对 begin/end（如 span start/end）。
type LifecycleChain struct{ items []Lifecycle }

func NewLifecycleChain(items ...Lifecycle) *LifecycleChain { return &LifecycleChain{items: items} }

func (c *LifecycleChain) OnRunStart(ctx context.Context, ec contracts.ExecutionContext) error {
	for _, m := range c.items {
		if err := m.OnRunStart(ctx, ec); err != nil {
			return err
		}
	}
	return nil
}

func (c *LifecycleChain) OnRunFinish(ctx context.Context, ec contracts.ExecutionContext, runErr error) error {
	for i := len(c.items) - 1; i >= 0; i-- {
		if err := c.items[i].OnRunFinish(ctx, ec, runErr); err != nil {
			return err
		}
	}
	return nil
}

// Tool 是工具调用的拦截器，Before 用于改写请求/记录审计，After 用于改写结果/统计。
type Tool interface {
	Before(context.Context, contracts.ExecutionContext, contracts.ToolCallRequest) (contracts.ToolCallRequest, error)
	After(context.Context, contracts.ExecutionContext, contracts.ToolCallRequest, contracts.ToolResult) (contracts.ToolResult, error)
}

// ToolChain 串行调用一组 Tool。Before 正序执行；After 倒序执行，与 LifecycleChain 同构。
type ToolChain struct{ items []Tool }

func NewToolChain(items ...Tool) *ToolChain { return &ToolChain{items: items} }

func (c *ToolChain) Before(ctx context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest) (contracts.ToolCallRequest, error) {
	var err error
	for _, m := range c.items {
		req, err = m.Before(ctx, ec, req)
		if err != nil {
			return req, err
		}
	}
	return req, nil
}

func (c *ToolChain) After(ctx context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest, result contracts.ToolResult) (contracts.ToolResult, error) {
	var err error
	for i := len(c.items) - 1; i >= 0; i-- {
		result, err = c.items[i].After(ctx, ec, req, result)
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

// Model 是模型调用的拦截器，与 Tool 对称：模型链路同样是双边的——
// Before 作用于发给模型的请求（输入审核 / 提示词加固），
// After 作用于模型返回的响应（输出内容护栏 / 敏感信息脱敏）。
//
// 之所以必须双边而不是只做输出侧：内容护栏的输入侧同样无处可挂，
// 而输入（历史 + 当前 prompt）会被拼进请求发给外部模型，泄露代价与输出同量级。
type Model interface {
	Before(context.Context, contracts.ExecutionContext, contracts.GenerateRequest) (contracts.GenerateRequest, error)
	After(context.Context, contracts.ExecutionContext, contracts.GenerateRequest, contracts.GenerateResponse) (contracts.GenerateResponse, error)
}

// ModelChain 串行调用一组 Model。Before 正序执行；After 倒序执行，与 ToolChain 同构。
type ModelChain struct{ items []Model }

func NewModelChain(items ...Model) *ModelChain { return &ModelChain{items: items} }

func (c *ModelChain) Before(ctx context.Context, ec contracts.ExecutionContext, req contracts.GenerateRequest) (contracts.GenerateRequest, error) {
	var err error
	for _, m := range c.items {
		req, err = m.Before(ctx, ec, req)
		if err != nil {
			return req, err
		}
	}
	return req, nil
}

func (c *ModelChain) After(ctx context.Context, ec contracts.ExecutionContext, req contracts.GenerateRequest, resp contracts.GenerateResponse) (contracts.GenerateResponse, error) {
	var err error
	for i := len(c.items) - 1; i >= 0; i-- {
		resp, err = c.items[i].After(ctx, ec, req, resp)
		if err != nil {
			return resp, err
		}
	}
	return resp, nil
}

// Event 是运行时事件的变换器，用于在事件发射前过滤/脱敏/重塑事件负载。
type Event interface {
	Transform(context.Context, contracts.RuntimeEvent) (contracts.RuntimeEvent, error)
}

// EventChain 串行调用一组 Event，正序传递事件，任一环节出错即短路。
type EventChain struct{ items []Event }

func NewEventChain(items ...Event) *EventChain { return &EventChain{items: items} }

func (c *EventChain) Transform(ctx context.Context, ev contracts.RuntimeEvent) (contracts.RuntimeEvent, error) {
	var err error
	for _, m := range c.items {
		ev, err = m.Transform(ctx, ev)
		if err != nil {
			return ev, err
		}
	}
	return ev, nil
}
