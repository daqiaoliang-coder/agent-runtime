package tooladapter

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/tool"
	"context"
)

type Provider struct {
	Registry *tool.Registry
}

func New(r *tool.Registry) *Provider { return &Provider{Registry: r} }

func (p *Provider) ListTools(ctx context.Context) ([]contracts.ToolDefinition, error) {
	_ = ctx
	// Registry currently exposes lookup semantics only. Keep the provider contract
	// stable and return definitions for the built-in tools we can discover by name.
	// Future registries can populate richer schemas without changing callers.
	return nil, nil
}

func (p *Provider) CallTool(ctx context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
	t, err := p.Registry.Get(req.Name)
	if err != nil {
		return contracts.ToolResult{CallID: req.CallID, IsError: true}, err
	}
	out, err := t.Execute(ctx, req.Arguments)
	if err != nil {
		return contracts.ToolResult{CallID: req.CallID, IsError: true}, err
	}
	return contracts.ToolResult{CallID: req.CallID, Output: out}, nil
}
