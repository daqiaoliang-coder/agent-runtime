// Package mcp adapts an MCP client to the Runtime ToolProvider contract.
// The concrete MCP SDK is intentionally hidden behind Client so upgrading the SDK
// does not leak into Runtime interfaces.
package mcp

import (
	"agent-runtime/internal/contracts"
	"context"
)

type Client interface {
	ListTools(context.Context) ([]contracts.ToolDefinition, error)
	CallTool(context.Context, contracts.ToolCallRequest) (contracts.ToolResult, error)
}

type Provider struct{ Client Client }

func New(c Client) *Provider { return &Provider{Client: c} }
func (p *Provider) ListTools(ctx context.Context) ([]contracts.ToolDefinition, error) {
	return p.Client.ListTools(ctx)
}
func (p *Provider) CallTool(ctx context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return p.Client.CallTool(ctx, req)
}
