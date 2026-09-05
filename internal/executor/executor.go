// Package executor 实现节点执行抽象，按 node.Type 分发到 LLM/Tool/SubAgent 执行器。
// 这是 Agent Runtime 中"执行"语义的核心：把 DAG 节点翻译成具体的 LLM 推理或工具调用。
package executor

import (
	"agent-runtime/internal/adapters/llm"
	tooladapter "agent-runtime/internal/adapters/tool"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"agent-runtime/internal/providers"
	"agent-runtime/internal/tool"
	"agent-runtime/internal/trace"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
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

// UsageRecorder 持久化 LLM token 消耗与成本（*store.MySQL 天然实现）。
// 为 nil 时不落库（测试/无 DB 场景），token 用量仅存在于内存中。
type UsageRecorder interface {
	RecordLLMUsage(ctx context.Context, u model.LLMUsage) error
}

// Pricer 根据模型名与 token 数估算单次调用成本（美元）。
// worker 注入默认实现；为 nil 时 cost 记 0（仅追踪 token，不估算花费）。
type Pricer func(model string, promptTokens, completionTokens int) float64

// Dispatcher 按 node.Type 路由到具体执行器。未识别类型返回错误，避免静默失败。
type Dispatcher struct {
	// ModelProvider/ToolProvider are the v3 stable extension points. Legacy LLM/Tools
	// remain supported so existing deployments and tests do not need a flag day migration.
	ModelProvider providers.ModelProvider
	ToolProvider  providers.ToolProvider
	LLM           llm.Client
	Tools         *tool.Registry
	ToolStore     ToolCallStore                                                  // 工具调用幂等存储；为 nil 时工具退化为直接执行（测试/无 DB 场景）
	SubAgent      Executor                                                       // 子 Agent 执行器（递归运行子 Run），当前为占位实现
	ContextLoader func(ctx context.Context, runID string) ([]llm.Message, error) // 从 checkpoint 重建对话历史
	UsageRecorder UsageRecorder                                                  // LLM token/cost 持久化；为 nil 时不落库
	Pricer        Pricer                                                         // 成本估算函数；为 nil 时 cost 记 0
}

// Execute 根据 node.Type 分发：
//   - LLM 节点：将 node.Input 作为 user prompt 调用 LLM；
//   - TOOL 节点：以 node.Name 查 Registry 执行，经 tool_call 表保证幂等；
//   - SUB_AGENT 节点：委托 SubAgent 执行器。
func (d *Dispatcher) Execute(ctx context.Context, n *model.Node) (string, error) {
	// 轨迹 span：标记节点执行入口，携带类型/ID/租户维度，串联到 worker.handle 的子 span。
	ctx, span := trace.StartSpan(ctx, "executor.execute")
	defer span.End()
	span.SetAttributes(
		attribute.String("node.type", string(n.Type)),
		attribute.String("node.id", n.ID),
		attribute.String("node.name", n.Name),
		attribute.String("run.id", n.RunID),
		attribute.String("tenant.id", n.TenantID),
	)
	switch n.Type {
	case model.NodeLLM:
		out, err := d.executeLLM(ctx, n)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return out, err
	case model.NodeTool:
		out, err := d.executeTool(ctx, n)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return out, err
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
	ctx, span := trace.StartSpan(ctx, "executor.llm")
	defer span.End()
	// 从 checkpoint 重建对话历史：历史在前，当前 user prompt 在后，保证 Agent 上下文连续。
	msgs := []llm.Message{{Role: llm.RoleUser, Content: n.Input}}
	if d.ContextLoader != nil {
		hist, err := d.ContextLoader(ctx, n.RunID)
		if err != nil {
			return "", fmt.Errorf("load context: %w", err)
		}
		if len(hist) > 0 {
			msgs = append(hist, msgs...)
		}
	}
	var resp llm.Response
	if d.ModelProvider != nil {
		request := contracts.GenerateRequest{Model: modelForNode(n)}
		for _, m := range msgs {
			request.Messages = append(request.Messages, contracts.Message{Role: contracts.Role(m.Role), Content: m.Content})
		}
		generated, err := d.ModelProvider.Generate(ctx, request)
		if err != nil {
			return "", fmt.Errorf("llm: %w", err)
		}
		resp = llm.Response{
			Content: generated.Message.Content,
			Model:   generated.Model,
			Usage:   llm.Usage{PromptTokens: generated.Usage.PromptTokens, CompletionTokens: generated.Usage.CompletionTokens, TotalTokens: generated.Usage.TotalTokens},
		}
	} else {
		if d.LLM == nil {
			return "", fmt.Errorf("llm client not configured")
		}
		got, err := d.LLM.Complete(ctx, llm.Request{Model: modelForNode(n), Messages: msgs})
		if err != nil {
			return "", fmt.Errorf("llm: %w", err)
		}
		resp = got
	}
	// 将 token 用量与模型记入 span，便于在追踪系统中按 token 维度聚合分析。
	span.SetAttributes(
		attribute.String("llm.model", resp.Model),
		attribute.Int("llm.prompt_tokens", resp.Usage.PromptTokens),
		attribute.Int("llm.completion_tokens", resp.Usage.CompletionTokens),
		attribute.Int("llm.total_tokens", resp.Usage.TotalTokens),
	)
	// 记录 token 用量与估算成本（最佳努力，不影响主流程）。
	if d.UsageRecorder != nil && resp.Usage.TotalTokens > 0 {
		modelName := resp.Model
		if modelName == "" {
			modelName = modelForNode(n)
		}
		var cost float64
		if d.Pricer != nil {
			cost = d.Pricer(modelName, resp.Usage.PromptTokens, resp.Usage.CompletionTokens)
		}
		_ = d.UsageRecorder.RecordLLMUsage(ctx, model.LLMUsage{
			ID:               fmt.Sprintf("usage-%s-%d", n.ID, time.Now().UnixNano()),
			RunID:            n.RunID,
			NodeID:           n.ID,
			TenantID:         n.TenantID,
			Model:            modelName,
			PromptTokens:     resp.Usage.PromptTokens,
			CompletionTokens: resp.Usage.CompletionTokens,
			TotalTokens:      resp.Usage.TotalTokens,
			Cost:             cost,
		})
	}
	return resp.Content, nil
}

// executeTool 执行工具节点。配置了 ToolStore 时走幂等路径，否则退化为直接执行。
func (d *Dispatcher) executeTool(ctx context.Context, n *model.Node) (string, error) {
	ctx, span := trace.StartSpan(ctx, "executor.tool")
	defer span.End()
	span.SetAttributes(attribute.String("tool.name", n.Name))
	if d.ToolProvider != nil {
		if d.ToolStore == nil {
			result, err := d.ToolProvider.CallTool(ctx, contracts.ToolCallRequest{Name: n.Name, Arguments: n.Input})
			if err != nil {
				return "", fmt.Errorf("tool %q: %w", n.Name, err)
			}
			return result.Output, nil
		}
		return d.executeToolProviderIdempotent(ctx, n)
	}
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
func (d *Dispatcher) executeToolProviderIdempotent(ctx context.Context, n *model.Node) (string, error) {
	callID := idempotencyKey(n.RunID, n.ID, n.Name, n.Input)
	isNew, err := d.ToolStore.ClaimToolCall(ctx, n.TenantID, callID, n.RunID, n.ID, n.Name, callID, n.Input, n.Attempt)
	if err != nil {
		return "", fmt.Errorf("claim tool call: %w", err)
	}
	if isNew {
		return d.runAndPersistToolProvider(ctx, n, callID)
	}
	rec, err := d.ToolStore.GetToolCall(ctx, n.TenantID, callID)
	if err != nil {
		return "", fmt.Errorf("load tool call: %w", err)
	}
	switch rec.Status {
	case "SUCCESS":
		return rec.Output, nil
	case "FAILED":
		reclaimed, err := d.ToolStore.ReclaimToolCall(ctx, n.TenantID, callID)
		if err != nil {
			return "", fmt.Errorf("reclaim tool call: %w", err)
		}
		if !reclaimed {
			return "", fmt.Errorf("tool call %s not reclaimable", callID)
		}
		return d.runAndPersistToolProvider(ctx, n, callID)
	case "RUNNING":
		// 关键日志：停滞的 RUNNING 工具调用拒绝重执行，副作用状态未知，是运维侧定位"卡死"工具调用的关键信号。
		log.Printf("tool call refused re-execution call_id=%s run=%s node=%s tenant=%s (stale RUNNING)", callID, n.RunID, n.ID, n.TenantID)
		return "", fmt.Errorf("tool call %s stale RUNNING; refusing re-execution (non-idempotent safety)", callID)
	default:
		return "", fmt.Errorf("tool call %s unknown status %q", callID, rec.Status)
	}
}

func (d *Dispatcher) runAndPersistToolProvider(ctx context.Context, n *model.Node, callID string) (string, error) {
	result, err := d.ToolProvider.CallTool(ctx, contracts.ToolCallRequest{CallID: callID, Name: n.Name, Arguments: n.Input})
	if err != nil || result.IsError {
		_ = d.ToolStore.FailToolCall(ctx, n.TenantID, callID)
		if err != nil {
			return "", fmt.Errorf("tool %q: %w", n.Name, err)
		}
		return "", fmt.Errorf("tool %q returned error", n.Name)
	}
	if err := d.ToolStore.CompleteToolCall(ctx, n.TenantID, callID, result.Output); err != nil {
		return "", fmt.Errorf("complete tool call: %w", err)
	}
	return result.Output, nil
}

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

// modelForNode 返回 LLM 节点应使用的模型名。当前从 node.Name 取（LLM 节点的 Name 字段
// 可复用为模型标识），为空时由 LLM 客户端决定默认模型。
func modelForNode(n *model.Node) string { return n.Name }

// NewDefault 构造一个开箱即用的 Dispatcher：使用 Echo（Stub）LLM + Search/Calculator 工具。
// 不带 ToolStore（工具直接执行），便于本地无 DB 演示与单元测试。
func NewDefault() *Dispatcher {
	tools := tool.NewRegistry()
	tools.Register(tool.Search{})
	tools.Register(tool.Calculator{})
	return &Dispatcher{LLM: llm.Echo(), Tools: tools, ModelProvider: llmadapter.New(llm.Echo()), ToolProvider: tooladapter.New(tools)}
}

// NewWithStore 在 NewDefault 基础上注入工具调用幂等存储，使 TOOL 节点经 tool_call 表保证幂等。
func NewWithStore(s ToolCallStore) *Dispatcher {
	d := NewDefault()
	d.ToolStore = s
	return d
}

// StreamLLM exposes the v3 streaming contract without forcing callers to know the
// concrete model SDK. Native provider streaming is used when available; legacy clients
// are represented as a single text delta followed by completion.
func (d *Dispatcher) StreamLLM(ctx context.Context, n *model.Node) (<-chan contracts.ModelEvent, error) {
	if d.ModelProvider != nil {
		msgs := []contracts.Message{{Role: contracts.RoleUser, Content: n.Input}}
		if d.ContextLoader != nil {
			hist, err := d.ContextLoader(ctx, n.RunID)
			if err != nil {
				return nil, fmt.Errorf("load context: %w", err)
			}
			if len(hist) > 0 {
				msgs = make([]contracts.Message, 0, len(hist)+1)
				for _, m := range hist {
					msgs = append(msgs, contracts.Message{Role: contracts.Role(m.Role), Content: m.Content})
				}
				msgs = append(msgs, contracts.Message{Role: contracts.RoleUser, Content: n.Input})
			}
		}
		return d.ModelProvider.Stream(ctx, contracts.GenerateRequest{Model: modelForNode(n), Messages: msgs})
	}
	if d.LLM == nil {
		return nil, fmt.Errorf("llm client not configured")
	}
	resp, err := d.LLM.Complete(ctx, llm.Request{Model: modelForNode(n), Messages: []llm.Message{{Role: llm.RoleUser, Content: n.Input}}})
	if err != nil {
		return nil, err
	}
	ch := make(chan contracts.ModelEvent, 2)
	ch <- contracts.ModelEvent{Type: contracts.ModelEventTextDelta, Delta: resp.Content}
	ch <- contracts.ModelEvent{Type: contracts.ModelEventCompleted, Usage: contracts.Usage{PromptTokens: resp.Usage.PromptTokens, CompletionTokens: resp.Usage.CompletionTokens, TotalTokens: resp.Usage.TotalTokens}}
	close(ch)
	return ch, nil
}
