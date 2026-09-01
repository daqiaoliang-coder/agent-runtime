// Package executor 实现节点执行抽象，按 node.Type 分发到 LLM/Tool/SubAgent 执行器。
// 这是 Agent Runtime 中"执行"语义的核心：把 DAG 节点翻译成具体的 LLM 推理或工具调用。
package executor

import (
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"agent-runtime/internal/tool"
	"context"
	"fmt"
)

// Executor 抽象节点执行：输入节点，输出结果字符串与错误。
// 实现需尊重 ctx（超时/取消）；错误会被 worker 转为节点失败事件（AgentStepFailed）。
type Executor interface {
	Execute(ctx context.Context, n *model.Node) (string, error)
}

// Dispatcher 按 node.Type 路由到具体执行器。
// 未识别类型返回错误，避免静默失败。
type Dispatcher struct {
	LLM      llm.Client
	Tools    *tool.Registry
	SubAgent Executor // 子 Agent 执行器（递归运行子 Run），当前为占位实现
}

// Execute 根据 node.Type 分发：
//   - LLM 节点：将 node.Input 作为 user prompt 调用 LLM；
//   - TOOL 节点：以 node.Name 在 Registry 查找并执行；
//   - SUB_AGENT 节点：委托 SubAgent 执行器。
func (d *Dispatcher) Execute(ctx context.Context, n *model.Node) (string, error) {
	switch n.Type {
	case model.NodeLLM:
		return d.executeLLM(ctx, n)
	case model.NodeTool:
		return d.executeTool(ctx, n)
	case model.NodeSubAgent:
		if d.SubAgent != nil {
			return d.SubAgent.Execute(ctx, n)
		}
		return "", fmt.Errorf("sub-agent executor not configured")
	default:
		return "", fmt.Errorf("unknown node type %q", n.Type)
	}
}

func (d *Dispatcher) executeLLM(ctx context.Context, n *model.Node) (string, error) {
	if d.LLM == nil {
		return "", fmt.Errorf("llm client not configured")
	}
	resp, err := d.LLM.Complete(ctx, llm.Request{Messages: []llm.Message{{Role: llm.RoleUser, Content: n.Input}}})
	if err != nil {
		return "", fmt.Errorf("llm: %w", err)
	}
	return resp.Content, nil
}

func (d *Dispatcher) executeTool(ctx context.Context, n *model.Node) (string, error) {
	if d.Tools == nil {
		return "", fmt.Errorf("tool registry not configured")
	}
	t, err := d.Tools.Get(n.Name)
	if err != nil {
		return "", err
	}
	out, err := t.Execute(ctx, n.Input)
	if err != nil {
		return "", fmt.Errorf("tool %q: %w", n.Name, err)
	}
	return out, nil
}

// NewDefault 构造一个开箱即用的 Dispatcher：使用 Echo（Stub）LLM + Search/Calculator 工具。
// 便于本地演示与测试；生产中应注入真实 LLM 客户端（如 llm.NewOpenAIClient）。
func NewDefault() *Dispatcher {
	tools := tool.NewRegistry()
	tools.Register(tool.Search{})
	tools.Register(tool.Calculator{})
	return &Dispatcher{LLM: llm.Echo(), Tools: tools}
}
