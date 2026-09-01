// Package executor 实现节点执行抽象，按 node.Type 分发到 LLM/Tool/SubAgent 执行器。
// 这是 Agent Runtime 中"执行"语义的核心：把 DAG 节点翻译成具体的 LLM 推理或工具调用。
package executor

import (
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"agent-runtime/internal/tool"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// Executor 抽象节点执行：输入节点，输出结果字符串与错误。
// 实现需尊重 ctx（超时/取消）；错误会被 worker 转为节点失败事件（AgentStepFailed）。
type Executor interface {
	Execute(ctx context.Context, n *model.Node) (string, error)
}

// ToolCallStore 是执行器需要的工具调用幂等存储接口（*store.MySQL 天然实现）。
// 抽象为接口便于用 fake store 做单测，不依赖真实 MySQL。
type ToolCallStore interface {
	ClaimToolCall(ctx context.Context, tenant, callID, runID, nodeID, toolName, idempotencyKey, input string, attempt int) (bool, error)
	GetToolCall(ctx context.Context, tenant, idempotencyKey string) (*model.ToolCall, error)
	ReclaimToolCall(ctx context.Context, tenant, callID string) (bool, error)
	CompleteToolCall(ctx context.Context, tenant, callID, output string) error
	FailToolCall(ctx context.Context, tenant, callID string) error
}

// Dispatcher 按 node.Type 路由到具体执行器。未识别类型返回错误，避免静默失败。
type Dispatcher struct {
	LLM      llm.Client
	Tools    *tool.Registry
	ToolStore ToolCallStore // 工具调用幂等存储；为 nil 时工具退化为直接执行（测试/无 DB 场景）
	SubAgent Executor      // 子 Agent 执行器（递归运行子 Run），当前为占位实现
}

// Execute 根据 node.Type 分发：
//   - LLM 节点：将 node.Input 作为 user prompt 调用 LLM；
//   - TOOL 节点：以 node.Name 查 Registry 执行，经 tool_call 表保证幂等；
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

// executeTool 执行工具节点。配置了 ToolStore 时走幂等路径，否则退化为直接执行。
func (d *Dispatcher) executeTool(ctx context.Context, n *model.Node) (string, error) {
	if d.Tools == nil {
		return "", fmt.Errorf("tool registry not configured")
	}
	t, err := d.Tools.Get(n.Name)
	if err != nil {
		return "", err
	}
	if d.ToolStore == nil {
		out, err := t.Execute(ctx, n.Input)
		if err != nil {
			return "", fmt.Errorf("tool %q: %w", n.Name, err)
		}
		return out, nil
	}
	return d.executeToolIdempotent(ctx, n, t)
}

// executeToolIdempotent 实现工具调用的幂等：经 tool_call 表落库，
//   - 新建调用：执行工具，成功落 SUCCESS、失败落 FAILED；
//   - 命中 SUCCESS：复用已持久化输出，不重复执行副作用；
//   - 命中 FAILED：回收为 RUNNING 重试一次（失败通常发生在副作用之前）；
//   - 命中 RUNNING（崩溃在途）：副作用状态未知，拒绝盲目重执行（非幂等工具安全优先）。
func (d *Dispatcher) executeToolIdempotent(ctx context.Context, n *model.Node, t tool.Tool) (string, error) {
	callID := idempotencyKey(n.RunID, n.ID, n.Name, n.Input)
	isNew, err := d.ToolStore.ClaimToolCall(ctx, n.TenantID, callID, n.RunID, n.ID, n.Name, callID, n.Input, n.Attempt)
	if err != nil {
		return "", fmt.Errorf("claim tool call: %w", err)
	}
	if isNew {
		return d.runAndPersistTool(ctx, n, t, callID)
	}
	rec, err := d.ToolStore.GetToolCall(ctx, n.TenantID, callID)
	if err != nil {
		return "", fmt.Errorf("load tool call: %w", err)
	}
	switch rec.Status {
	case "SUCCESS":
		// 幂等命中：复用已持久化结果，跳过重复副作用。
		return rec.Output, nil
	case "FAILED":
		// 上次失败（副作用通常未发生）：回收为 RUNNING 后重试一次。
		reclaimed, err := d.ToolStore.ReclaimToolCall(ctx, n.TenantID, callID)
		if err != nil {
			return "", fmt.Errorf("reclaim tool call: %w", err)
		}
		if !reclaimed {
			return "", fmt.Errorf("tool call %s not reclaimable", callID)
		}
		return d.runAndPersistTool(ctx, n, t, callID)
	case "RUNNING":
		// 停滞的进行中调用：副作用状态未知，为非幂等工具安全拒绝盲目重执行。
		return "", fmt.Errorf("tool call %s stale RUNNING; refusing re-execution (non-idempotent safety)", callID)
	default:
		return "", fmt.Errorf("tool call %s unknown status %q", callID, rec.Status)
	}
}

// runAndPersistTool 执行工具并按结果更新 tool_call 状态。
func (d *Dispatcher) runAndPersistTool(ctx context.Context, n *model.Node, t tool.Tool, callID string) (string, error) {
	out, err := t.Execute(ctx, n.Input)
	if err != nil {
		_ = d.ToolStore.FailToolCall(ctx, n.TenantID, callID)
		return "", fmt.Errorf("tool %q: %w", n.Name, err)
	}
	if err := d.ToolStore.CompleteToolCall(ctx, n.TenantID, callID, out); err != nil {
		return "", fmt.Errorf("complete tool call: %w", err)
	}
	return out, nil
}

// idempotencyKey 由 (run,node,tool,input) 派生，跨重试稳定，同时用作 call_id 与 idempotency_key。
// sha256 hex 恰为 64 字符，匹配 call_id VARCHAR(64)。
func idempotencyKey(runID, nodeID, toolName, input string) string {
	h := sha256.Sum256([]byte(runID + "|" + nodeID + "|" + toolName + "|" + input))
	return hex.EncodeToString(h[:])
}

// NewDefault 构造一个开箱即用的 Dispatcher：使用 Echo（Stub）LLM + Search/Calculator 工具。
// 不带 ToolStore（工具直接执行），便于本地无 DB 演示与单元测试。
func NewDefault() *Dispatcher {
	tools := tool.NewRegistry()
	tools.Register(tool.Search{})
	tools.Register(tool.Calculator{})
	return &Dispatcher{LLM: llm.Echo(), Tools: tools}
}

// NewWithStore 在 NewDefault 基础上注入工具调用幂等存储，使 TOOL 节点经 tool_call 表保证幂等。
func NewWithStore(s ToolCallStore) *Dispatcher {
	d := NewDefault()
	d.ToolStore = s
	return d
}
