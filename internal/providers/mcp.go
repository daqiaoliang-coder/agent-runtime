package providers

import (
	"agent-runtime/internal/contracts"
	"context"
)

type MCPProvider interface {
	ListTools(context.Context) ([]contracts.ToolDefinition, error)
	CallTool(context.Context, contracts.ToolCallRequest) (contracts.ToolResult, error)
}
