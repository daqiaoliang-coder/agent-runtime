package main

import (
	"context"
	"fmt"
	"time"
)

// Tool 是 Agent 可调用的外部能力，Name 作为注册键，Execute 接收原始输入字符串。
type Tool interface {
	Name() string
	Execute(context.Context, string) (string, error)
}

type SearchTool struct{}

func (t *SearchTool) Name() string {
	return "search"
}

func (t *SearchTool) Execute(ctx context.Context, input string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(200 * time.Millisecond):
		return fmt.Sprintf("Search result for: %s", input), nil
	}
}

type ToolRegistry struct {
	tools map[string]Tool
}

func NewToolRegistry() *ToolRegistry {
	return &ToolRegistry{tools: make(map[string]Tool)}
}

func (r *ToolRegistry) Register(tool Tool) {
	r.tools[tool.Name()] = tool
}

func (r *ToolRegistry) Get(name string) (Tool, error) {
	tool, ok := r.tools[name]
	if !ok {
		return nil, fmt.Errorf("tool %s not found", name)
	}
	return tool, nil
}
