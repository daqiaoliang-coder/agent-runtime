// Package worker 实现任务执行单元。
// Worker 从 Redis 队列消费任务，认领节点、通过 Executor 执行，并经 Outbox 事务式记录完成/失败事件。
package worker

import (
	"agent-runtime/internal/adapters/credential"
	"agent-runtime/internal/adapters/llm"
	tooladapter "agent-runtime/internal/adapters/tool"
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/event"
	"agent-runtime/internal/executor"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/model"
	"agent-runtime/internal/obs"
	"agent-runtime/internal/policy"
	"agent-runtime/internal/providers"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/retry"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"agent-runtime/internal/tool"
	"agent-runtime/internal/trace"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	oteltrace "go.opentelemetry.io/otel/trace"
)

// Worker 是单个任务执行器实例。
// ID 用于租约归属标识；Exec 执行节点（默认为带 Stub LLM + 演示工具的 Dispatcher）。
// Events 字段预留给直接发布场景（当前完成/失败事件经 Outbox 投递）。
type Worker struct {
	Store         *store.MySQL
	Queue         *queue.RedisQueue
	Events        *event.RocketMQ
	RuntimeEvents event.Sink
	Exec          executor.Executor
	Policy        policy.Policy
	Approval      ApprovalRequester
	Retry         retry.Policy
	ID            string
	// EventChain 在事件离开 Runtime 前做变换（当前用于脱敏），为 nil 时原样发射。
	//
	// 这一层的必要性高于工具结果脱敏：同一份事件会进 SSE 推给前端、进日志、
	// 进可观测系统，是敏感信息扩散面最大的地方。工具结果即便在 After 漏了，这里还有一道。
	EventChain *middleware.EventChain
}

// ApprovalRequester is intentionally smaller than runtime.Runtime.
// It keeps worker -> runtime dependency inverted and testable.
type ApprovalRequester interface {
	Interrupt(context.Context, string, string, string, string) error
}

// 人工闸门的归属：worker **不持有**挂起能力，而是由 Guard 在拦截时自行落库
// （装配见 security.go 的 newGuard），随后才向 worker 抛出 ErrAwaitingApproval。
//
// 这个顺序是刻意的。若反过来 —— Guard 只报错、由 worker 去挂起 ——
// 那么"检测到中危"与"Run 已冻结"之间存在一个窗口：窗口内崩溃会让节点
// 既没执行也没挂起，被 recovery 扫回 READY 重跑，护栏等于没拦住。
// 先落库再报错，则崩溃后状态一定是 WAITING_HUMAN，语义闭合。
// worker 侧只负责把这一事实转成 HITL_REQUESTED 事件呈现给人工。
// （policy gateway 是另一条转人工路径：它在 ClaimNode 之前经 Approval 落库，
// 节点保持 READY 被同事务挂起，放行后由 ResumeRun 统一重新武装。）

// Handle 处理单个任务，流程：
//  1. 以任务携带的租户身份读取节点并以租约方式 Claim（CAS），竞争失败则直接返回；
//  2. 启动心跳协程定期续租，防止长耗时 LLM/工具调用因租约过期被恢复；
//  3. 通过 Executor 执行节点（LLM 推理或工具调用）；
//  4. 成功：CompleteNodeWithOutbox 写 AgentStepCompleted；失败：FailNodeWithOutbox 写 AgentStepFailed。
//
// 租户隔离：所有节点操作都带 t.TenantID，跨租户的 node_id 会被 WHERE tenant_id=? 拦截。
// 事件携带 TenantID，Resume Controller 据此继续做租户隔离。
func (w *Worker) Handle(ctx context.Context, t model.Task) error {
	// 轨迹根 span：串联从认领节点到执行完成的全流程，携带 run/node/tenant 维度。
	ctx, span := trace.StartSpan(ctx, "worker.handle")
	defer span.End()
	span.SetAttributes(
		attribute.String("run.id", t.RunID),
		attribute.String("node.id", t.NodeID),
		attribute.String("tenant.id", t.TenantID),
		attribute.Int("attempt", t.Attempt),
	)
	n, err := w.Store.GetNode(ctx, t.TenantID, t.NodeID)
	if err != nil {
		return err
	}
	// 状态机检查：明确区分取消、终态与可恢复暂停状态。
	//   - CANCEL_REQUESTED / 终态（SUCCESS/FAILED/CANCELLED）：取消尚未执行的节点，阻止其被认领。
	//     这是用户取消的最终防线：CancelRun 已取消 PENDING/READY 节点，但 RecoverExpired
	//     可能在此之后把崩溃的 RUNNING 节点重置回 PENDING 并补投递，此时 worker 须拒绝执行。
	//   - WAITING_HUMAN：不执行也不取消，等待人工审批恢复为 RUNNING 后重新调度。
	//   - PENDING：Run 尚未完成初始化调度，不执行不取消，由恢复扫描补投递。
	run, err := w.Store.GetRun(ctx, t.TenantID, t.RunID)
	if err != nil {
		return err
	}
	if run.Status != model.RunRunning {
		switch run.Status {
		case model.RunCancelRequested, model.RunSuccess, model.RunFailed, model.RunCancelled:
			if n.Status == model.NodePending || n.Status == model.NodeReady {
				_, _ = w.Store.CancelNode(ctx, t.TenantID, t.NodeID, n.Version)
			}
		}
		obs.From(ctx).InfoContext(ctx, "worker skip node",
			"worker_id", w.ID, "run_id", t.RunID, "node_id", t.NodeID, "run_status", run.Status)
		return nil
	}

	// Policy gate MUST run before ClaimNode. A step waiting for human approval
	// therefore remains READY instead of becoming RUNNING and holding a lease.
	if n.Type == model.NodeTool && w.Policy != nil {
		// 人工已放行的节点跳过瀑布（docs/permission-classifier.md §7
		// "approve (一次)：Bypass 按 node 放行"）：不查的话，放行后重新调度
		// 的节点会再次命中同一条规则、再次转人工，审批闭环退化成死循环
		// ——这正是 Guard.Bypass 早已修过的同一类问题在 policy gate 的镜像。
		// 查询失败按未放行处理（宁可再问一次人工，不可误放）。
		approved, aerr := w.Store.HasResolvedApproval(ctx, n.TenantID, n.RunID, n.ID)
		if aerr != nil {
			obs.From(ctx).WarnContext(ctx, "approval bypass lookup failed; re-evaluating policy",
				"run_id", n.RunID, "node_id", n.ID, "error", aerr)
		}
		if !(aerr == nil && approved) {
			decision, err := w.Policy.Evaluate(ctx, policy.Request{
				TenantID: n.TenantID,
				RunID:    n.RunID,
				NodeID:   n.ID,
				ToolName: n.Name,
				Input:    n.Input,
			})
			if err != nil {
				return fmt.Errorf("policy evaluate: %w", err)
			}
			switch decision.Decision {
			case policy.Deny:
				// Deny is a deterministic policy failure. Do not execute and do not
				// retry it as an infrastructure failure.
				if _, ferr := w.Store.FailNodeWithOutbox(ctx, n, model.OutboxMessage{
					ID:          fmt.Sprintf("policy-deny-%s-%d", n.ID, time.Now().UnixNano()),
					EventType:   "AgentPolicyDenied",
					AggregateID: n.RunID,
					// 审计口径（docs/permission-classifier.md §8）：只记层级/规则
					// 标识与理由，绝不记录工具入参原文。
					Payload: fmt.Sprintf(`{"node_id":%q,"policy_id":%q,"layer":%q,"reason":%q}`,
						n.ID, decision.PolicyID, decision.Layer, decision.Reason),
				}); ferr != nil {
					return fmt.Errorf("persist policy denial: %w", ferr)
				}
				return nil
			case policy.RequireApproval:
				if w.Approval == nil {
					return fmt.Errorf("policy requires approval but approval requester is not configured")
				}
				// Durable HITL is a Run-level interrupt in the current runtime.
				// Because the node has not been claimed yet, Resume can safely
				// re-queue the same READY node after approval.
				return w.Approval.Interrupt(
					ctx,
					n.TenantID,
					n.RunID,
					n.ID,
					fmt.Sprintf("policy=%s risk=%s layer=%s reason=%s", decision.PolicyID, decision.Risk, decision.Layer, decision.Reason),
				)
			}
		}
	}

	ok, err := w.Store.ClaimNode(ctx, n.TenantID, n.ID, n.Version, w.ID, 30*time.Second)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// 关键日志：节点认领成功，标记执行入口，便于在普通日志中追踪 worker 调度边界。
	obs.From(ctx).InfoContext(ctx, "worker claimed node",
		"worker_id", w.ID, "run_id", n.RunID, "node_id", n.ID, "tenant_id", n.TenantID,
		"node_type", n.Type, "attempt", n.Attempt)
	w.emitRuntimeEvent(ctx, contracts.RuntimeEvent{
		ID: fmt.Sprintf("runtime-event-%s-start-%d", n.ID, time.Now().UnixNano()), RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID,
		Type: contracts.EventNodeStarted, Timestamp: time.Now(), Data: map[string]any{"type": n.Type, "name": n.Name, "attempt": n.Attempt},
	})
	n, _ = w.Store.GetNode(ctx, n.TenantID, n.ID)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// 执行期间持续续租，避免长任务租约过期被抢占恢复。
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeat.C:
				_, _ = w.Store.RenewLease(ctx, n.TenantID, n.ID, w.ID, n.Version, 30*time.Second)
			}
		}
	}()

	// Token 预算检查：LLM/REFLECT 节点执行前校验累计 token 是否超限。
	// 超限时直接失败（不可重试），避免无意义的 LLM 调用与 token 浪费。
	if run.MaxTokens > 0 && (n.Type == model.NodeLLM || n.Type == model.NodeReflect) {
		used, terr := w.Store.RunTokenUsage(ctx, n.TenantID, n.RunID)
		if terr != nil {
			obs.From(ctx).WarnContext(ctx, "token usage check failed",
				"run_id", n.RunID, "node_id", n.ID, "error", terr)
		} else if used >= run.MaxTokens {
			budgetErr := fmt.Errorf("token budget exceeded: used %d >= limit %d", used, run.MaxTokens)
			span.RecordError(budgetErr)
			span.SetStatus(codes.Error, budgetErr.Error())
			next := n.Attempt + 1
			obs.From(ctx).WarnContext(ctx, "node dead-lettered (token budget)",
				"run_id", n.RunID, "node_id", n.ID, "attempt", n.Attempt)
			_ = w.Store.EnqueueDLQ(ctx, n.TenantID, n.RunID, n.ID, budgetErr.Error(), next, "")
			fe := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: "AgentStepFailed", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Error: budgetErr.Error(), Timestamp: time.Now()}
			injectTraceContext(ctx, &fe)
			payload, _ := json.Marshal(fe)
			_, _ = w.Store.FailNodeWithOutbox(ctx, n, model.OutboxMessage{ID: fe.ID, EventType: fe.Type, AggregateID: n.RunID, Payload: string(payload)})
			w.emitRuntimeEvent(ctx, contracts.RuntimeEvent{
				ID: fmt.Sprintf("runtime-event-%s-failed-%d", n.ID, time.Now().UnixNano()), RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID,
				Type: contracts.EventNodeFailed, Timestamp: time.Now(), Data: map[string]any{"error": budgetErr.Error(), "attempt": n.Attempt},
			})
			return nil
		}
	}

	// 注入执行上下文：Executor 接口只认节点不认身份，而防护链的每一次拦截
	// 都需要租户维度做审计、需要节点标识做人工闸门锚点。走 ctx 传递而非改接口，
	// 避免波及全部 Executor 实现（详见 contracts/context.go 的取舍说明）。
	// UserID 目前只能为空：agent_run / agent_node 均未持久化发起者用户，
	// 该字段要等接入认证层（Lifecycle.OnRunStart 校验凭证）后才有可靠来源。
	ctx = contracts.WithExecutionContext(ctx, contracts.ExecutionContext{
		TenantID: n.TenantID,
		ThreadID: run.ThreadID,
		RunID:    n.RunID,
		NodeID:   n.ID,
		TraceID:  oteltrace.SpanContextFromContext(ctx).TraceID().String(),
	})

	// 执行节点：替换原先的占位字符串拼接，真正发起 LLM 推理或工具调用。
	output, execErr := w.Exec.Execute(ctx, n)
	if execErr != nil {
		// 护栏转人工：Approver 已在拦截时把 Run 与节点一并挂起（WAITING_HUMAN），
		// 此处**不得**再动节点状态 —— 任何 Fail/Retry/Complete 都会覆盖挂起态，
		// 要么让节点被重试策略反复重跑（审批形同虚设），要么把等待人工变成终态失败。
		// 直接 ack 任务：Run 已冻结，人工放行后由 ResumeRun 重新投递该节点。
		if errors.Is(execErr, middleware.ErrAwaitingApproval) {
			obs.From(ctx).InfoContext(ctx, "node awaiting human approval",
				"run_id", n.RunID, "node_id", n.ID, "tenant_id", n.TenantID, "tool", n.Name)
			w.emitRuntimeEvent(ctx, contracts.RuntimeEvent{
				ID: fmt.Sprintf("runtime-event-%s-hitl-%d", n.ID, time.Now().UnixNano()), RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID,
				Type: contracts.EventHITLRequested, Timestamp: time.Now(),
				// 只放原因不放工具入参：入参可能正是攻击载荷，
				// 事件会进 SSE 与日志，抄进去等于二次扩散。
				Data: map[string]any{"reason": execErr.Error(), "tool": n.Name, "attempt": n.Attempt},
			})
			return nil
		}
		// 在 span 上记录执行错误，便于在追踪系统中按错误维度检索失败轨迹。
		span.RecordError(execErr)
		span.SetStatus(codes.Error, execErr.Error())
		next := n.Attempt + 1
		// 护栏拒绝（高危载荷，或中危但未装配人工闸门而 fail-closed）不可重试。
		// 重试同样的入参只会命中同样的规则：既浪费重试预算，
		// 又让 DLQ 里堆满重复的安全告警，淹没真正需要关注的故障。
		blocked := errors.Is(execErr, middleware.ErrBlocked)
		if !blocked && w.Retry.ShouldRetry(next) {
			// 仍可重试：指数退避，置回 READY 并安排 ready_at，ack 任务。
			// recovery 的 ReadyTasks 扫描会在 ready_at 到期后补投递，实现真正的退避重试。
			readyAt := time.Now().Add(w.Retry.Backoff(next))
			ok, rerr := w.Store.RetryNode(ctx, n.TenantID, n.ID, n.Version, readyAt)
			if rerr != nil {
				return fmt.Errorf("retry node: %w (exec err: %v)", rerr, execErr)
			}
			if !ok {
				// 版本/状态已变（可能被恢复抢占），不 ack，任务重投递由恢复机制收敛。
				return fmt.Errorf("retry cas conflict for %s (exec err: %v)", n.ID, execErr)
			}
			// 关键日志：节点执行失败但仍在重试预算内，记录退避点，便于观察重试节流。
			obs.From(ctx).WarnContext(ctx, "node retried",
				"run_id", n.RunID, "node_id", n.ID, "attempt_from", n.Attempt,
				"attempt_to", next, "backoff_until", readyAt.Format(time.RFC3339), "error", execErr)
			return nil
		}
		// 重试耗尽：入死信队列 + 失败事件，由 Resume 收敛 Run 为 FAILED。
		// 关键日志：重试耗尽进入死信队列，标志该节点不可恢复，需人工介入或下游兜底。
		obs.From(ctx).WarnContext(ctx, "node dead-lettered",
			"run_id", n.RunID, "node_id", n.ID, "attempt", n.Attempt, "error", execErr)
		_ = w.Store.EnqueueDLQ(ctx, n.TenantID, n.RunID, n.ID, execErr.Error(), next, output)
		fe := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: "AgentStepFailed", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Error: execErr.Error(), Timestamp: time.Now()}
		injectTraceContext(ctx, &fe)
		payload, _ := json.Marshal(fe)
		if _, ferr := w.Store.FailNodeWithOutbox(ctx, n, model.OutboxMessage{ID: fe.ID, EventType: fe.Type, AggregateID: n.RunID, Payload: string(payload)}); ferr != nil {
			return fmt.Errorf("persist failure: %w (exec err: %v)", ferr, execErr)
		}
		w.emitRuntimeEvent(ctx, contracts.RuntimeEvent{
			ID: fmt.Sprintf("runtime-event-%s-failed-%d", n.ID, time.Now().UnixNano()), RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID,
			Type: contracts.EventNodeFailed, Timestamp: time.Now(), Data: map[string]any{"error": execErr.Error(), "attempt": n.Attempt},
		})
		return nil
	}

	// REFLECT 节点：解析决策，若 "replan" 则发 ReplanRequested 事件代替 AgentStepCompleted。
	// 节点仍标记 SUCCESS（决策本身执行成功），仅事件类型不同，驱动 Resumer 走续规路径。
	eventType := "AgentStepCompleted"
	if n.Type == model.NodeReflect {
		var decision struct {
			Action string `json:"action"`
		}
		if jerr := json.Unmarshal([]byte(output), &decision); jerr == nil && decision.Action == "replan" {
			eventType = "ReplanRequested"
		}
	}
	e := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: eventType, RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Output: output, Timestamp: time.Now()}
	injectTraceContext(ctx, &e)
	payload, _ := json.Marshal(e)
	// 节点完成 + Outbox 事件在同一事务内提交，保证状态与事件一致。
	// 上下文不再通过 checkpoint 累积（存在读改写竞态），改由 ContextLoader 从已提交的
	// SUCCESS 节点派生，保证并行节点不会相互覆盖对话历史。
	_, err = w.Store.CompleteNodeWithOutbox(ctx, n, output, model.OutboxMessage{ID: e.ID, EventType: e.Type, AggregateID: n.RunID, Payload: string(payload)})
	if err != nil {
		return err
	}
	// 关键日志：节点执行完成，标志一次成功的 LLM 推理或工具调用落地。
	obs.From(ctx).InfoContext(ctx, "node completed",
		"run_id", n.RunID, "node_id", n.ID, "tenant_id", n.TenantID,
		"attempt", n.Attempt, "output_bytes", len(output))
	w.emitRuntimeEvent(ctx, contracts.RuntimeEvent{
		ID: fmt.Sprintf("runtime-event-%s-finished-%d", n.ID, time.Now().UnixNano()), RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID,
		Type: contracts.EventNodeFinished, Timestamp: time.Now(), Data: map[string]any{"output": output, "attempt": n.Attempt},
	})
	return nil
}

// injectTraceContext 将当前 ctx 的 W3C trace 上下文注入事件，
// 使其经 Outbox→RocketMQ 穿透到消费端后，resumer 能挂回原 Run 的 trace。
// 与 queue.Enqueue 的注入逻辑一致，保证两条异步边界（队列/事件）trace 不断链。
func injectTraceContext(ctx context.Context, e *model.Event) {
	if e.TraceContext == nil {
		e.TraceContext = map[string]string{}
	}
	otel.GetTextMapPropagator().Inject(ctx, propagation.MapCarrier(e.TraceContext))
}

// emitRuntimeEvent 发射运行时事件，发射前经 EventChain 变换（当前为脱敏）。
//
// 这是敏感信息扩散面最大的一道出口：同一份事件会进 SSE 推给前端、进日志、
// 进可观测系统。工具结果即便在 Tool.After 漏了脱敏，这里还有一道兜底，
// 因此两层叠加要求 Redact 幂等（其占位符不满足任何规则的结构约束，正是为此设计）。
//
// 变换失败时**仍然发射原事件**而不是丢弃：事件流是 Run 的可观测主干，
// 因为脱敏环节出错就丢弃全部事件，会让整个 Run 变成黑盒 ——
// 排障能力丢失的代价远大于单点泄露风险。降级行为记日志以便发现。
func (w *Worker) emitRuntimeEvent(ctx context.Context, ev contracts.RuntimeEvent) {
	if w.RuntimeEvents == nil {
		return
	}
	if w.EventChain != nil {
		transformed, terr := w.EventChain.Transform(ctx, ev)
		if terr != nil {
			obs.From(ctx).WarnContext(ctx, "event transform failed; emitting untransformed",
				"run_id", ev.RunID, "node_id", ev.NodeID, "event_type", ev.Type, "error", terr)
		} else {
			ev = transformed
		}
	}
	_ = w.RuntimeEvents.Emit(ctx, ev)
}

// NewFromEnv 从环境变量 WORKER_ID 读取标识，缺省时按时间戳生成。
// 默认使用 Echo（Stub）LLM + Search/Calculator 工具，并注入真实 store 启用 tool_call 幂等。
// 配置 OPENAI_BASE_URL + OPENAI_API_KEY 时切换为真实 OpenAI 兼容 HTTP 客户端。
func NewFromEnv(s *store.MySQL, q *queue.RedisQueue, r *event.RocketMQ) *Worker {
	id := os.Getenv("WORKER_ID")
	if id == "" {
		id = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	tools := tool.NewRegistry()
	tools.Register(tool.Search{})
	tools.Register(tool.Calculator{})
	tools.Register(tool.FetchToolResult{Store: s})
	// 密钥一律经托管层获取，不再直接 os.Getenv 后塞进结构体字段。
	// 这是"透明替换"的落地点：上层只持有 CredentialProvider，
	// 换成 FileProvider（Vault/KMS sidecar 周期性覆写文件）时 worker 一行都不用改，
	// 轮转由托管设施驱动、进程无需重启。为何不包缓存见 credentials.go 的取舍说明。
	creds := newCredentialsFromEnv()
	var client llm.Client = llm.Echo()
	if base := os.Getenv("OPENAI_BASE_URL"); base != "" {
		// 托管层取不到凭证时降级为 Stub：此时发起真实请求只会换来网关 401，
		// 把排查方向从"密钥没配"误导到"密钥无效"。降级则行为与未配置网关时一致。
		if _, cerr := creds.Credential(context.Background(), contracts.CredentialPurposeChat); cerr == nil {
			client = llm.NewOpenAIClientWithCredentials(base, creds, contracts.CredentialPurposeChat)
		} else {
			obs.From(context.Background()).WarnContext(context.Background(), "no chat credential available; falling back to stub LLM",
				"error", cerr)
		}
	}
	// ToolStore=s 使 TOOL 节点经 tool_call 表保证幂等（SUCCESS 复用、崩溃在途拒绝重执行）。
	// Retry=指数退避策略，失败可重试节点置回 READY 并按 ready_at 补投递，耗尽入 DLQ。
	// UsageRecorder=s 使 LLM 节点的 token 用量与成本落 llm_usage 表，支撑成本分析。
	// ModelProvider 提升为 mp 局部变量：压缩管线的摘要调用与主推理复用同一适配器。
	mp := llmadapter.New(client)
	disp := &executor.Dispatcher{LLM: client, Tools: tools, ModelProvider: mp, ToolProvider: tooladapter.New(tools), ToolStore: s, UsageRecorder: s, Pricer: DefaultPricer}

	// 安全中间件装配：不可信输入防护挂在工具调用前，数据脱敏同时挂在
	// 工具结果回传、模型输出与事件出域三处。人工闸门的落库能力在 newSecurityBundle 内部
	// 已交给护栏持有，worker 侧不需要再拿一份。
	// 顺序上先于 ContextLoader：压缩管线（Compactor）的摘要调用需要复用
	// ModelChain 护栏（摘要输入含全量历史，泄露面与主调用同量级，没有理由豁免）。
	sec := newSecurityBundle(s, q, creds)
	disp.ToolChain = sec.ToolChain
	disp.ModelChain = sec.ModelChain

	// P0 上下文优化（详见 internal/worker/context.go）：
	//  - 祖先作用域：节点只加载 agent_edge 传递闭包上的 SUCCESS 祖先，
	//    并行分支的无关产物不再灌入；CONTEXT_ANCESTOR_SCOPE=false 可退回全量；
	//  - 工具结果遮蔽：REFLECT 控制节点排除、窗口外/超长工具结果替换为带 node_id
	//    指针的占位符，原文始终保留在 agent_node.output；CONTEXT_TOOL_MASKING=false 可关；
	//  - 跨 Run 语义记忆仍按 MEMORY_ENABLED 装配，失败静默降级。
	// 历史按 (finished_at, node_id) 确定序派生，崩溃恢复也能重建且顺序稳定。
	ctxOpt := newContextOptionsFromEnv(creds)
	// 上下文压缩管线（docs/context-compaction.md）：CONTEXT_COMPACTION_ENABLED
	// 默认关闭，关闭时 ContextLoader 走无压缩路径、行为与接入前完全一致。
	// Events 留 nil：生产 cmd/worker 尚无 RuntimeEvent 出口，日志是唯一可观测
	// 信号（接入出口后在此补接即可，见 compaction.go 的取舍说明）。
	if compOpt := newCompactionOptionsFromEnv(); compOpt != nil {
		comp := &Compactor{
			Store:      s,
			Model:      mp,
			ModelChain: sec.ModelChain,
			Usage:      s,
			Pricer:     DefaultPricer,
			Opt:        *compOpt,
		}
		ctxOpt.Compaction = comp
		disp.CacheGen = comp.CacheGen
	}
	disp.ContextLoader = newContextLoader(s, ctxOpt)
	// Prompt 前缀缓存默认关闭：开启后向网关透传 Run 级 prompt_cache_key，
	// 配合稳定的消息布局复用 KV 缓存（需网关支持该 OpenAI 兼容扩展字段）。
	// 压缩启用时键经 CacheGen 带代数后缀：全量压缩改写前缀，旧代缓存整体作废。
	disp.PromptCache = envBool("LLM_PROMPT_CACHE", false)
	// 权限瀑布（docs/permission-classifier.md）：PERMISSION_WATERFALL_ENABLED
	// 默认关闭，关闭时 Policy 为 nil、gate 维持接入前的休眠状态；开启后
	// L1 规则 → L2 命令解析防线 → L3 分类器（可选）→ 默认送审。
	// Approval 是 RequireApproval 的落库出口：复用 runtime.Interrupt（与
	// newGuard 的护栏闸门同一事务语义——Run 置 WAITING_HUMAN + 挂起节点 +
	// 写 run_interrupt，先落库再返回，崩溃后状态闭合）。不接线的话 L5
	// 默认送审（瀑布最常见的终局）会在 Handle 里报错并按基础设施失败
	// 重试直至 DLQ，而不是转人工。
	permPolicy := newPolicyFromEnv(mp, sec.ModelChain, s)
	var approval ApprovalRequester
	if permPolicy != nil {
		approval = &runtime.Runtime{Store: s, Queue: q}
	}
	return &Worker{Store: s, Queue: q, Events: r, ID: id, Retry: retry.Default(), Exec: disp, EventChain: sec.EventChain, Policy: permPolicy, Approval: approval}
}

// newMemoryOptionsFromEnv 按环境变量装配读取路径的记忆能力。
//
// 装配条件是"三者齐备"：MEMORY_ENABLED 显式开启、向量库地址可达配置、
// 托管层能取到 embedding 凭证。任一缺失即返回 Memory=nil，
// 使 ContextLoader 退化为接入前的行为——**现有部署与测试零感知**。
//
// 刻意不在此处 log.Fatal：记忆是可选增强，配置不全时应当降级运行，
// 而不是让整个 worker 起不来。
func newMemoryOptionsFromEnv(creds contracts.CredentialProvider) MemoryOptions {
	if !envBool("MEMORY_ENABLED", false) {
		return MemoryOptions{}
	}
	// 探测 embedding 凭证是否可用。这里只看"取不取得到"，不取明文 ——
	// 明文由 embedder 在每次请求时经 port 现取，密钥不进本函数的任何变量。
	if _, err := creds.Credential(context.Background(), contracts.CredentialPurposeEmbedding); err != nil {
		// 没有 embedding 能力就无法做语义召回。此处静默降级：
		// 主链路的对话补全可能走 Stub，但记忆需要真实向量，二者要求不同。
		obs.From(context.Background()).WarnContext(context.Background(), "MEMORY_ENABLED but no embedding credential; long-term memory disabled",
			"error", err)
		return MemoryOptions{}
	}
	// embedding 复用与对话推理相同的网关配置，仅模型名与超时独立可调。
	// 走 WithCredentials 构造，密钥不进入结构体字段。
	embedder := llm.NewOpenAIEmbedderWithCredentials(
		envString("OPENAI_BASE_URL", ""),
		envString("EMBEDDING_MODEL", "text-embedding-3-small"),
		creds,
		contracts.CredentialPurposeEmbedding,
	)
	// 向量库 SDK 只接受 string 型 apiKey，无法接 port，因此走托管层的降级出口：
	// 明文短暂存在于局部变量，但不再由 os.Getenv 散落各处，
	// 换托管设施（文件 / Vault）时此处一行都不用改。
	// 这是 credential.Value 文档中说明的唯一正当用法。
	vs, err := vector.NewQdrant(
		envString("QDRANT_HOST", "localhost"),
		envInt("QDRANT_PORT", 6334),
		credential.Value(context.Background(), creds, contracts.CredentialPurposeVectorDB),
	)
	if err != nil {
		// 连接失败只降级、不致命：向量库可能稍后才就绪，
		// 而 worker 必须能起来处理节点（记忆缺失只是少了增强）。
		obs.From(context.Background()).WarnContext(context.Background(), "qdrant unavailable; long-term memory disabled",
			"error", err)
		return MemoryOptions{}
	}

	m := &providers.VectorMemory{
		Embedder:   embedder,
		Store:      vs,
		Collection: envString("QDRANT_COLLECTION", providers.DefaultMemoryCollection),
		TopK:       envInt("MEMORY_TOP_K", providers.DefaultMemoryTopK),
		MinScore:   envFloat32("MEMORY_MIN_SCORE", providers.DefaultMemoryMinScore),
	}
	obs.From(context.Background()).InfoContext(context.Background(), "long-term memory enabled",
		"collection", m.Collection, "top_k", m.TopK, "min_score", m.MinScore,
		"timeout", envDuration("MEMORY_SEARCH_TIMEOUT", DefaultMemorySearchTimeout).String())
	return MemoryOptions{
		Memory:           m,
		SearchTimeout:    envDuration("MEMORY_SEARCH_TIMEOUT", DefaultMemorySearchTimeout),
		MaxMessages:      envInt("MEMORY_MAX_MESSAGES", DefaultMaxMemoryMessages),
		MaxContextTokens: envInt("MEMORY_MAX_CONTEXT_TOKENS", DefaultMaxContextTokens),
	}
}

// modelPrices 按模型名记录每百万 token 的单价（美元），prompt 在前、completion 在后。
// 仅含常见模型作为演示；生产中应从配置中心动态加载。未命中的模型 cost 记 0。
var modelPrices = map[string][2]float64{
	"gpt-4o":        {2.5, 10},
	"gpt-4o-mini":   {0.15, 0.6},
	"gpt-4-turbo":   {10, 30},
	"gpt-3.5-turbo": {0.5, 1.5},
}

// DefaultPricer 根据 model 与 token 数估算单次调用成本（美元）。
// 价格按每百万 token 计；未在 modelPrices 中登记的模型返回 0（仅追踪 token）。
func DefaultPricer(model string, prompt, completion int) float64 {
	prices, ok := modelPrices[model]
	if !ok {
		return 0
	}
	return float64(prompt)/1e6*prices[0] + float64(completion)/1e6*prices[1]
}
