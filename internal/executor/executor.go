// Package executor 实现节点执行抽象，按 node.Type 分发到 LLM/Tool/SubAgent 执行器。
// 这是 Agent Runtime 中"执行"语义的核心：把 DAG 节点翻译成具体的 LLM 推理或工具调用。
package executor

import (
	"agent-runtime/internal/adapters/llm"
	tooladapter "agent-runtime/internal/adapters/tool"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/model"
	"agent-runtime/internal/providers"
	"agent-runtime/internal/tool"
	"agent-runtime/internal/trace"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"strings"
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
	MarkToolCallUnknown(ctx context.Context, tenant, callID string) error
}

// isAmbiguousFailure 判断错误是否为"远端可能已执行但响应丢失"的歧义失败
// （超时/取消/网络中断）。这类失败副作用状态未知，不应盲目标记 FAILED 后重试，
// 需标记为 UNKNOWN 以阻止重复副作用，等待人工确认或下游幂等查询解决。
func isAmbiguousFailure(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}
	return false
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
	ToolStore     ToolCallStore                                                          // 工具调用幂等存储；为 nil 时工具退化为直接执行（测试/无 DB 场景）
	SubAgent      Executor                                                               // 子 Agent 执行器（递归运行子 Run），当前为占位实现
	ContextLoader func(ctx context.Context, tenant, runID string) ([]llm.Message, error) // 从已提交节点重建对话历史
	UsageRecorder UsageRecorder                                                          // LLM token/cost 持久化；为 nil 时不落库
	Pricer        Pricer                                                                 // 成本估算函数；为 nil 时 cost 记 0
	// ToolChain 是工具调用的横切链（不可信输入防护 + 数据脱敏），为 nil 时不拦截。
	//
	// 挂在 Dispatcher 而非只挂在 react.Engine 的理由：生产路径上工具是由 DAG 的
	// TOOL 节点直接执行的，不经过 ReAct 循环。只接 Engine 会让真实部署完全绕过防护，
	// 那正是"代码写好了但链路是断的"——挂载点存在却不生效，等于没有。
	ToolChain *middleware.ToolChain
}

// executionContext 组装本次工具调用的执行身份。
//
// 优先取 ctx 中由 worker 注入的完整上下文（含 ThreadID / TraceID），
// 缺失时用节点自身字段兜底，保证未接线的调用方（测试、直接调用）也能拿到租户维度。
// NodeID 一律以当前节点覆写：人工闸门要靠它定位到具体审批锚点。
func (d *Dispatcher) executionContext(ctx context.Context, n *model.Node) contracts.ExecutionContext {
	ec, ok := contracts.ExecutionContextFrom(ctx)
	if !ok {
		ec = contracts.ExecutionContext{}
	}
	if ec.TenantID == "" {
		ec.TenantID = n.TenantID
	}
	if ec.RunID == "" {
		ec.RunID = n.RunID
	}
	ec.NodeID = n.ID
	return ec
}

// beforeTool 在**任何副作用与幂等认领之前**跑横切链的 Before。
//
// 顺序是这里最关键的设计决定。若在 ClaimToolCall 之后才拦截，被拦下的调用
// 已经写入一条 RUNNING 的 tool_call 记录；人工放行后重新调度时，
// 幂等逻辑读到 RUNNING 会判定"崩溃在途、拒绝盲目重执行"——
// 于是一次安全拦截把该节点永久锁死，审批放行了也跑不动。
// 拦截必须先于认领，才能保证"被拦下的调用不产生任何持久化痕迹"。
//
// 返回改写后的请求（当前内置中间件不改写，但链上任何一环都可以）与执行上下文。
func (d *Dispatcher) beforeTool(ctx context.Context, n *model.Node, callID string) (contracts.ExecutionContext, contracts.ToolCallRequest, error) {
	ec := d.executionContext(ctx, n)
	req := contracts.ToolCallRequest{CallID: callID, Name: n.Name, Arguments: n.Input}
	if d.ToolChain == nil {
		return ec, req, nil
	}
	// Before 的错误语义必须原样上抛，不可包装成普通失败：
	// ErrAwaitingApproval 要路由到人工挂起，ErrBlocked 要路由到不可重试终态。
	// 一旦在这里被包成 fmt.Errorf 而不保留 %w，上层 errors.Is 就判不出来，
	// 等待人工的节点会被重试策略反复重跑——这是护栏最典型的失效方式。
	req, err := d.ToolChain.Before(ctx, ec, req)
	return ec, req, err
}

// afterTool 跑横切链的 After（工具结果脱敏）。ToolChain 为 nil 时原样返回。
func (d *Dispatcher) afterTool(ctx context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest, result contracts.ToolResult) (contracts.ToolResult, error) {
	if d.ToolChain == nil {
		return result, nil
	}
	return d.ToolChain.After(ctx, ec, req, result)
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
	case model.NodeReflect:
		out, err := d.executeReflect(ctx, n)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return out, err
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
		hist, err := d.ContextLoader(ctx, n.TenantID, n.RunID)
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

// reflectDecision 是 REFLECT 节点输出的 JSON 结构。
// Action="replan" 触发 Resumer 续规划；Action="finish" 走正常收敛。
type reflectDecision struct {
	Action string `json:"action"`
	Reason string `json:"reason"`
}

const reflectSystemPrompt = `You are a reflection node in an agent runtime.
Evaluate the progress so far and decide whether to continue (replan) or finish.
Respond with ONLY a JSON object, no prose: {"action":"replan"|"finish","reason":"..."}`

// executeReflect 执行反思节点：加载 checkpoint 上下文，调 LLM 评估进度，
// 返回 JSON 决策 {"action":"replan"|"finish","reason":"..."}。
// worker 据此决定发 ReplanRequested 还是 AgentStepCompleted 事件。
func (d *Dispatcher) executeReflect(ctx context.Context, n *model.Node) (string, error) {
	ctx, span := trace.StartSpan(ctx, "executor.reflect")
	defer span.End()
	// 从 checkpoint 重建对话历史作为评估上下文。
	msgs := []llm.Message{
		{Role: llm.RoleSystem, Content: reflectSystemPrompt},
		{Role: llm.RoleUser, Content: n.Input},
	}
	if d.ContextLoader != nil {
		hist, err := d.ContextLoader(ctx, n.TenantID, n.RunID)
		if err != nil {
			return "", fmt.Errorf("load context for reflect: %w", err)
		}
		if len(hist) > 0 {
			// 历史在前，反思指令在后。
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
			return "", fmt.Errorf("reflect llm: %w", err)
		}
		resp = llm.Response{Content: generated.Message.Content, Model: generated.Model}
	} else {
		if d.LLM == nil {
			return "", fmt.Errorf("llm client not configured for reflect")
		}
		got, err := d.LLM.Complete(ctx, llm.Request{Model: modelForNode(n), Messages: msgs})
		if err != nil {
			return "", fmt.Errorf("reflect llm: %w", err)
		}
		resp = got
	}
	// 解析 LLM 输出为 reflectDecision；解析失败时默认 finish（不阻断流程）。
	var decision reflectDecision
	raw := extractJSON(resp.Content)
	if err := json.Unmarshal([]byte(raw), &decision); err != nil || (decision.Action != "replan" && decision.Action != "finish") {
		// LLM 未返回有效 JSON 或 action 不合法：默认 finish，避免卡死。
		decision = reflectDecision{Action: "finish", Reason: "reflect output unparseable, defaulting to finish"}
	}
	out, _ := json.Marshal(decision)
	span.SetAttributes(
		attribute.String("reflect.action", decision.Action),
		attribute.String("llm.model", resp.Model),
	)
	return string(out), nil
}

// extractJSON 从可能含 Markdown 代码块或前后说明文本的响应中提取首个 JSON 对象。
func extractJSON(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```")
		s = strings.TrimSuffix(s, "```")
		s = strings.TrimSpace(s)
	}
	start := strings.Index(s, "{")
	if start < 0 {
		return s
	}
	end := strings.LastIndex(s, "}")
	if end < start {
		return s
	}
	return s[start : end+1]
}

// executeTool 执行工具节点。这是防护链在生产路径上的**唯一生效点**：
// Before（不可信输入检测）→ 幂等认领 → 真实调用 → After（结果脱敏）。
//
// 三条顺序约束，每一条都对应一个真实故障模式：
//  1. Before 必须先于幂等认领，否则被拦下的调用会留下 RUNNING 记录，
//     人工放行后重跑会被幂等逻辑判为"崩溃在途"而永久拒绝（节点锁死）。
//  2. Before 的错误必须原样上抛，worker 依赖 errors.Is 区分
//     "等待人工"与"真失败"，包成普通错误会让审批节点被重试策略反复重跑。
//  3. 幂等键必须基于 Before **改写后**的入参计算，否则中间件改写参数后
//     会出现"同一逻辑调用两个键"或"不同调用同一个键"的错配。
func (d *Dispatcher) executeTool(ctx context.Context, n *model.Node) (string, error) {
	ctx, span := trace.StartSpan(ctx, "executor.tool")
	defer span.End()
	span.SetAttributes(attribute.String("tool.name", n.Name))

	// 第一步：横切链 Before。此处不生成 callID——幂等键要等入参确定后才算。
	ec, req, err := d.beforeTool(ctx, n, "")
	if err != nil {
		// 原样上抛，保留 ErrAwaitingApproval / ErrBlocked 的哨兵语义。
		return "", err
	}

	var run func(context.Context, contracts.ToolCallRequest) (contracts.ToolResult, error)
	switch {
	case d.ToolProvider != nil:
		run = d.ToolProvider.CallTool
	case d.Tools != nil:
		t, terr := d.Tools.Get(req.Name)
		if terr != nil {
			return "", terr
		}
		run = registryCall(t)
	default:
		return "", fmt.Errorf("tool registry not configured")
	}

	// 把 After（脱敏）包进调用闭包，而不是在函数返回后再做。
	// 这样落库的输出已经是脱敏后的：tool_call.output 是持久化数据，
	// 先落库再脱敏等于把敏感原文长期留在数据库里，比事件泄露更难回收。
	// 同时保证幂等 SUCCESS 命中复用的也是脱敏后的输出，不会因路径不同而泄露。
	redactedRun := func(ctx context.Context, r contracts.ToolCallRequest) (contracts.ToolResult, error) {
		res, rerr := run(ctx, r)
		if rerr != nil {
			return res, rerr
		}
		return d.afterTool(ctx, ec, r, res)
	}

	if d.ToolStore == nil {
		// 无幂等存储（测试 / 无 DB 场景）：直接执行，防护与脱敏照常生效。
		result, err := redactedRun(ctx, req)
		if err != nil {
			return "", fmt.Errorf("tool %q: %w", n.Name, err)
		}
		return result.Output, nil
	}
	req.CallID = idempotencyKey(n.RunID, n.ID, req.Name, req.Arguments)
	return d.executeToolIdempotent(ctx, n, req, redactedRun)
}

// registryCall 把 tool.Tool 适配成与 ToolProvider 同形的调用闭包，
// 使 Registry 与 Provider 两条路径共用同一个防护收口点。
func registryCall(t tool.Tool) func(context.Context, contracts.ToolCallRequest) (contracts.ToolResult, error) {
	return func(ctx context.Context, req contracts.ToolCallRequest) (contracts.ToolResult, error) {
		out, err := t.Execute(ctx, req.Arguments)
		if err != nil {
			return contracts.ToolResult{CallID: req.CallID, IsError: true}, err
		}
		return contracts.ToolResult{CallID: req.CallID, Output: out}, nil
	}
}

// executeToolIdempotent 实现工具调用的幂等：经 tool_call 表落库，
//   - 新建调用：执行 run，成功落 SUCCESS、确定失败落 FAILED、歧义失败落 UNKNOWN；
//   - 命中 SUCCESS：复用已持久化输出，不重复执行副作用；
//   - 命中 FAILED：回收为 RUNNING 重试一次（失败通常发生在副作用之前）；
//   - 命中 RUNNING（崩溃在途）：副作用状态未知，拒绝盲目重执行（非幂等工具安全优先）；
//   - 命中 UNKNOWN（超时/网络中断）：远端可能已执行，拒绝盲目重执行。
//
// run 是"真实调用 + 结果脱敏"的合成闭包（由 executeTool 构造）。把脱敏放在闭包内
// 而不是本函数返回后，是为了让**落库的输出已经是脱敏后的**——tool_call.output 是
// 持久化数据，先落库再脱敏等于把敏感原文长期留在数据库里，比事件泄露更难回收。
// 代价是 SUCCESS 幂等命中时复用的是脱敏后的输出，这要求 Redact 幂等
// （其占位符不满足任何规则的结构约束，正是为此设计）。
//
// 合并 Provider 与 Registry 两条幂等实现的理由：此前二者是逐字重复的两份代码，
// 任何一处修复（如 CAS 语义、歧义失败判定）都极易只改一份，
// 而防护链一旦只接在其中一条上，另一条就是绕过入口。
func (d *Dispatcher) executeToolIdempotent(ctx context.Context, n *model.Node, req contracts.ToolCallRequest, run func(context.Context, contracts.ToolCallRequest) (contracts.ToolResult, error)) (string, error) {
	callID := req.CallID
	isNew, err := d.ToolStore.ClaimToolCall(ctx, n.TenantID, callID, n.RunID, n.ID, req.Name, callID, req.Arguments, n.Attempt)
	if err != nil {
		return "", fmt.Errorf("claim tool call: %w", err)
	}
	if isNew {
		return d.runAndPersistTool(ctx, n, req, run)
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
		return d.runAndPersistTool(ctx, n, req, run)
	case "RUNNING", "UNKNOWN":
		// 关键日志：停滞的 RUNNING 或歧义 UNKNOWN 工具调用拒绝重执行，副作用状态未知，
		// 是运维侧定位"卡死"/"歧义"工具调用的关键信号。
		log.Printf("tool call refused re-execution call_id=%s run=%s node=%s tenant=%s (stale %s)", callID, n.RunID, n.ID, n.TenantID, rec.Status)
		return "", fmt.Errorf("tool call %s stale %s; refusing re-execution (non-idempotent safety)", callID, rec.Status)
	default:
		return "", fmt.Errorf("tool call %s unknown status %q", callID, rec.Status)
	}
}

// runAndPersistTool 执行工具并按结果更新 tool_call 状态。
// 歧义失败（超时/取消）标记为 UNKNOWN 以阻止盲目重试；确定失败标记为 FAILED 允许重试。
func (d *Dispatcher) runAndPersistTool(ctx context.Context, n *model.Node, req contracts.ToolCallRequest, run func(context.Context, contracts.ToolCallRequest) (contracts.ToolResult, error)) (string, error) {
	result, err := run(ctx, req)
	if err != nil {
		if isAmbiguousFailure(err) {
			_ = d.ToolStore.MarkToolCallUnknown(ctx, n.TenantID, req.CallID)
		} else {
			_ = d.ToolStore.FailToolCall(ctx, n.TenantID, req.CallID)
		}
		return "", fmt.Errorf("tool %q: %w", req.Name, err)
	}
	if result.IsError {
		// 工具返回了确定性错误结果（副作用未发生或工具自报错误），标记 FAILED 允许重试。
		_ = d.ToolStore.FailToolCall(ctx, n.TenantID, req.CallID)
		return "", fmt.Errorf("tool %q returned error", req.Name)
	}
	if err := d.ToolStore.CompleteToolCall(ctx, n.TenantID, req.CallID, result.Output); err != nil {
		return "", fmt.Errorf("complete tool call: %w", err)
	}
	return result.Output, nil
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
			hist, err := d.ContextLoader(ctx, n.TenantID, n.RunID)
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
