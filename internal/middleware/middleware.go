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
