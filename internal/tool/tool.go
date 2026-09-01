// Package tool 抽象 Agent 可调用的外部能力（工具）。
// 节点执行器按 node.Name 在 Registry 查找并调用对应 Tool，实现"工具调用"语义。
package tool

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Tool 是一个外部能力单元。Name 为注册键，Execute 接收原始输入字符串。
// 实现需尊重 ctx（超时/取消），错误会被执行器包装后转为节点失败事件。
type Tool interface {
	Name() string
	Execute(ctx context.Context, input string) (string, error)
}

// Registry 按 Name 管理工具集合，线程不安全（初始化期注册）。
type Registry struct{ tools map[string]Tool }

func NewRegistry() *Registry { return &Registry{tools: make(map[string]Tool)} }

func (r *Registry) Register(t Tool) { r.tools[t.Name()] = t }

func (r *Registry) Get(name string) (Tool, error) {
	t, ok := r.tools[name]
	if !ok {
		return nil, fmt.Errorf("tool %q not found", name)
	}
	return t, nil
}

// Search 是演示用搜索工具，模拟网络检索延迟与结果。
type Search struct{}

func (Search) Name() string { return "search" }
func (Search) Execute(ctx context.Context, input string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}
	return fmt.Sprintf("search results for %q: [doc1, doc2]", input), nil
}

// Calculator 是演示用计算器，支持 "a + b" 形式的加法。
type Calculator struct{}

func (Calculator) Name() string { return "calculator" }
func (Calculator) Execute(ctx context.Context, input string) (string, error) {
	parts := strings.Split(strings.TrimSpace(input), "+")
	if len(parts) != 2 {
		return "", fmt.Errorf("calculator: expected 'a + b', got %q", input)
	}
	a, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil {
		return "", fmt.Errorf("calculator: parse a: %w", err)
	}
	b, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil {
		return "", fmt.Errorf("calculator: parse b: %w", err)
	}
	return fmt.Sprintf("%d", a+b), nil
}
