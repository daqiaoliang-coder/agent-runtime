// Package middleware provides small, domain-specific runtime chains for cross-cutting
// concerns. Middleware stays above the durable kernel and does not own persistence.
package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
)

type Lifecycle interface {
	OnRunStart(context.Context, contracts.ExecutionContext) error
	OnRunFinish(context.Context, contracts.ExecutionContext, error) error
}

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

type Tool interface {
	Before(context.Context, contracts.ExecutionContext, contracts.ToolCallRequest) (contracts.ToolCallRequest, error)
	After(context.Context, contracts.ExecutionContext, contracts.ToolCallRequest, contracts.ToolResult) (contracts.ToolResult, error)
}

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

type Event interface {
	Transform(context.Context, contracts.RuntimeEvent) (contracts.RuntimeEvent, error)
}

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
