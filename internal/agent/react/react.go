// Package react implements a small provider-agnostic ReAct loop.
// The engine owns agent decisions; execution/persistence stays behind StepRunner so
// the same loop can run in-process or be backed by the durable runtime kernel.
package react

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/event"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/providers"
	"context"
	"fmt"
	"time"
	"unicode/utf8"
)

// StepRunner 解耦 ReAct 循环与具体执行：引擎只负责决策，LLM/工具调用由 Runner 承担。
// 这样同一引擎可跑在内存中，也可由 durable runtime kernel 承载执行。
type StepRunner interface {
	RunLLM(context.Context, contracts.GenerateRequest) (contracts.GenerateResponse, error)
	RunTool(context.Context, contracts.ToolCallRequest) (contracts.ToolResult, error)
}

// ProviderRunner 是基于 providers 接口的 StepRunner 实现，桥接到 v3 稳定扩展点。
type ProviderRunner struct {
	Model providers.ModelProvider
	Tool  providers.ToolProvider
}

func (r ProviderRunner) RunLLM(ctx context.Context, req contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	return r.Model.Generate(ctx, req)
}
func (r ProviderRunner) RunTool(ctx context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return r.Tool.CallTool(ctx, req)
}

// Engine 是无状态的 ReAct 推理引擎。ToolChain 在每次工具调用前后生效，
// Events 用于把推理/工具过程发布到运行时事件流。
type Engine struct {
	Runner        StepRunner
	ToolChain     *middleware.ToolChain
	Events        event.Sink
	MaxIterations int
	// ContextLoader 可选：开启后 Run 在循环前加载 DAG 祖先历史并 prepend。
	// nil 时完全保持旧行为（仅用 Input.Messages）。
	ContextLoader func(ctx context.Context, tenant, runID, nodeID string) ([]llm.Message, error)
	// ToolMasking 开启后对 ReAct 循环内的工具结果按 rune 截断到 ToolOutputMaxRunes。
	ToolMasking bool
	// ToolOutputMaxRunes 单条工具结果的字符上限；<=0 用默认 4000。
	ToolOutputMaxRunes int
}

// reactDefaultToolMaxRunes 是 ReAct 循环中工具结果截断的默认上限。
const reactDefaultToolMaxRunes = 4000

// Input 是一次 ReAct 会话的输入，ExecutionContext 携带租户/追踪身份。
type Input struct {
	ExecutionContext contracts.ExecutionContext
	Messages         []contracts.Message
	Tools            []contracts.ToolDefinition
}

// Result 是一次 ReAct 会话的输出，Iterations 反映思考-行动的循环次数。
type Result struct {
	Message    contracts.Message
	Iterations int
}

// Run 执行 ReAct 循环：思考(调 LLM)->若返回 ToolCalls 则执行工具并把结果回填给 LLM->继续，
// 直到 LLM 不再请求工具（给出最终回答）或达到 MaxIterations（默认 8，防止失控循环）。
// 每次工具调用经 ToolChain.Before/After 拦截，所有动作以 RuntimeEvent 形式发布到 Events。
func (e *Engine) Run(ctx context.Context, in Input) (Result, error) {
	if e.Runner == nil {
		return Result{}, fmt.Errorf("react: step runner not configured")
	}
	// 注入 ExecutionContext 到 ctx，使 fetch_tool_result 等工具能拿到租户。
	ctx = contracts.WithExecutionContext(ctx, in.ExecutionContext)

	max := e.MaxIterations
	if max <= 0 {
		max = 8
	}
	messages := append([]contracts.Message(nil), in.Messages...)

	// 加载 DAG 祖先历史并 prepend（与 executor.executeLLM 同构）。
	if e.ContextLoader != nil && in.ExecutionContext.TenantID != "" && in.ExecutionContext.RunID != "" {
		hist, err := e.ContextLoader(ctx, in.ExecutionContext.TenantID, in.ExecutionContext.RunID, in.ExecutionContext.NodeID)
		if err != nil {
			return Result{}, fmt.Errorf("react: load context: %w", err)
		}
		if len(hist) > 0 {
			prefix := make([]contracts.Message, 0, len(hist))
			for _, m := range hist {
				prefix = append(prefix, contracts.Message{Role: contracts.Role(m.Role), Content: m.Content})
			}
			messages = append(prefix, messages...)
		}
	}

	for i := 0; i < max; i++ {
		resp, err := e.Runner.RunLLM(ctx, contracts.GenerateRequest{Messages: messages, Tools: in.Tools})
		if err != nil {
			return Result{}, err
		}
		if len(resp.ToolCalls) == 0 {
			msg := resp.Message
			if msg.Role == "" {
				msg.Role = contracts.RoleAssistant
			}
			messages = append(messages, msg)
			e.emit(ctx, contracts.RuntimeEvent{ID: fmt.Sprintf("react-text-%d", time.Now().UnixNano()), RunID: in.ExecutionContext.RunID, TenantID: in.ExecutionContext.TenantID, Type: contracts.EventTextDelta, Timestamp: time.Now(), Data: map[string]any{"content": msg.Content}})
			return Result{Message: msg, Iterations: i + 1}, nil
		}
		messages = append(messages, resp.Message)
		for _, call := range resp.ToolCalls {
			req := contracts.ToolCallRequest{CallID: call.ID, Name: call.Name, Arguments: call.Arguments}
			if e.ToolChain != nil {
				req, err = e.ToolChain.Before(ctx, in.ExecutionContext, req)
				if err != nil {
					return Result{}, err
				}
			}
			e.emit(ctx, contracts.RuntimeEvent{ID: fmt.Sprintf("react-tool-%d", time.Now().UnixNano()), RunID: in.ExecutionContext.RunID, TenantID: in.ExecutionContext.TenantID, Type: contracts.EventToolCall, Timestamp: time.Now(), Data: req})
			result, err := e.Runner.RunTool(ctx, req)
			if err != nil {
				return Result{}, err
			}
			if e.ToolChain != nil {
				result, err = e.ToolChain.After(ctx, in.ExecutionContext, req, result)
				if err != nil {
					return Result{}, err
				}
			}
			content := result.Output
			if e.ToolMasking {
				content = capRunesForReact(content, req.CallID, e.toolOutputMaxRunes())
			}
			messages = append(messages, contracts.Message{Role: contracts.RoleTool, Content: content})
			e.emit(ctx, contracts.RuntimeEvent{ID: fmt.Sprintf("react-tool-result-%d", time.Now().UnixNano()), RunID: in.ExecutionContext.RunID, TenantID: in.ExecutionContext.TenantID, Type: contracts.EventToolResult, Timestamp: time.Now(), Data: result})
		}
	}
	return Result{}, fmt.Errorf("react: max iterations %d exceeded", max)
}

func (e *Engine) toolOutputMaxRunes() int {
	if e.ToolOutputMaxRunes <= 0 {
		return reactDefaultToolMaxRunes
	}
	return e.ToolOutputMaxRunes
}

// capRunesForReact 对 ReAct 循环内的工具结果做 rune 安全截断，附 call_id 指针。
// ReAct 无 model.Node，用 CallID 替代 node_id 作为原文定位指针。
func capRunesForReact(s, callID string, max int) string {
	if max <= 0 || utf8.RuneCountInString(s) <= max {
		return s
	}
	runes := []rune(s)
	return string(runes[:max]) + fmt.Sprintf(
		"\n\n[Truncated: result was %d characters; retrieve via fetch_tool_result tool with call_id=%q.]",
		len(runes), callID)
}

func (e *Engine) emit(ctx context.Context, ev contracts.RuntimeEvent) {
	if e.Events != nil {
		_ = e.Events.Emit(ctx, ev)
	}
}
