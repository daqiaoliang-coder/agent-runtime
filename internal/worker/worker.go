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
	idgen "agent-runtime/internal/id"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/model"
	"agent-runtime/internal/obs"
	"agent-runtime/internal/policy"
	"agent-runtime/internal/providers"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/retry"
	"agent-runtime/internal/store"
	"agent-runtime/internal/tool"
	"agent-runtime/internal/trace"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
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
	Store         WorkerStore
	Queue         *queue.RedisQueue
	Events        *event.RocketMQ
	RuntimeEvents event.Sink
	Exec          executor.Executor
	Policy        policy.Policy
	Approval      ApprovalRequester
	Retry         retry.Policy
	ID            string
	// ExecTimeout 约束单次节点执行（LLM 推理/工具调用）的最长时长，仅覆盖 Exec.Execute，
	// 不覆盖失败落库等兜底写入。0/负值用 DefaultExecTimeout。
	// 硬编码超时会误杀合法的长任务：多步工具循环与长输出的时长方差极大，
	// 超时必须可配置（WORKER_EXEC_TIMEOUT）。
	ExecTimeout time.Duration
	// leaseTTL / heartbeatInterval 是执行期租约参数（未导出，测试可缩短）。
	// 0 值回退 defaultLeaseTTL / defaultHeartbeatInterval。
	leaseTTL          time.Duration
	heartbeatInterval time.Duration
	// EventChain 在事件离开 Runtime 前做变换（当前用于脱敏），为 nil 时原样发射。
	//
	// 这一层的必要性高于工具结果脱敏：同一份事件会进 SSE 推给前端、进日志、
	// 进可观测系统，是敏感信息扩散面最大的地方。工具结果即便在 After 漏了，这里还有一道。
	EventChain *middleware.EventChain
	// CancelBus 广播仅加速取消检测，执行状态仍由 GetRun 确认。
	CancelBus CancelSignals
	// pollInterval 是 CancelBus 生效时 GetRun 的兜底轮询间隔（零值默认 30s）。
	pollInterval time.Duration
}

// DefaultExecTimeout 是单次节点执行的默认超时。覆盖绝大多数 LLM 推理与工具调用，
// 且远大于执行期租约（30s + 心跳续租），长任务靠心跳维持租约。
const DefaultExecTimeout = 10 * time.Minute

// defaultLeaseTTL / defaultHeartbeatInterval 是执行期租约的默认参数。
// 心跳间隔必须小于租约时长（30s / 10s），保证在租约过期前有续租机会。
const (
	defaultLeaseTTL          = 30 * time.Second
	defaultHeartbeatInterval = 10 * time.Second
)

// execTimeout 返回生效的执行超时。
func (w *Worker) execTimeout() time.Duration {
	if w.ExecTimeout > 0 {
		return w.ExecTimeout
	}
	return DefaultExecTimeout
}

// leaseTTLOrDefault 返回生效的执行期租约时长。
func (w *Worker) leaseTTLOrDefault() time.Duration {
	if w.leaseTTL > 0 {
		return w.leaseTTL
	}
	return defaultLeaseTTL
}

// heartbeatIntervalOrDefault 返回生效的心跳间隔。
func (w *Worker) heartbeatIntervalOrDefault() time.Duration {
	if w.heartbeatInterval > 0 {
		return w.heartbeatInterval
	}
	return defaultHeartbeatInterval
}

type WorkerStore interface {
	GetNode(context.Context, string, string) (*model.Node, error)
	GetRun(context.Context, string, string) (*model.Run, error)
	ClaimNode(context.Context, string, string, int64, string, time.Duration) (bool, error)
	RenewLease(context.Context, string, string, string, int64, time.Duration) (bool, error)
	RunTokenUsage(context.Context, string, string) (int, error)
	CompleteNodeWithOutbox(context.Context, *model.Node, string, model.OutboxMessage) (bool, error)
	FailNodeWithOutbox(context.Context, *model.Node, model.OutboxMessage) (bool, error)
	CancelNodeWithOutbox(context.Context, *model.Node, model.OutboxMessage) (bool, error)
	RetryNode(context.Context, string, string, string, int64, time.Time) (bool, error)
	EnqueueDLQ(context.Context, string, string, string, string, int, string) error
	CancelNode(context.Context, string, string, int64) (bool, error)
}

type CancelSignals interface {
	Cancelled(runID string) bool
}

var (
	errLeaseLost    = errors.New("node lease lost")
	errRunCancelled = errors.New("run cancelled by user")
)

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
	// 重投不能在认领前策略路径借用其他 worker 的租约，或改写挂起/终态节点。
	if n.Status != model.NodePending && n.Status != model.NodeReady {
		return nil
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
				Payload: fmt.Sprintf(`{"node_id":%q,"policy_id":%q,"reason":%q}`,
					n.ID, decision.PolicyID, decision.Reason),
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
				fmt.Sprintf("policy=%s risk=%s reason=%s", decision.PolicyID, decision.Risk, decision.Reason),
			)
		}
	}

	claimedAt := time.Now()
	ok, err := w.Store.ClaimNode(ctx, n.TenantID, n.ID, n.Version, w.ID, w.leaseTTLOrDefault())
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	// Claim CAS increments exactly once; a later read could observe another owner's version.
	claimed := *n
	claimed.Version++
	claimed.Status = model.NodeRunning
	claimed.LeaseOwner = w.ID
	n = &claimed
	ctx = contracts.WithExecutionContext(ctx, contracts.ExecutionContext{
		TenantID: n.TenantID, UserID: run.UserID, AuthMethod: run.AuthMethod,
		ThreadID: run.ThreadID, RunID: n.RunID, NodeID: n.ID,
		TraceID: oteltrace.SpanContextFromContext(ctx).TraceID().String(),
	})
	executionCtx, cancelExecution := context.WithCancelCause(ctx)
	defer cancelExecution(nil)
	execCtx, execCancel := context.WithTimeout(executionCtx, w.execTimeout())
	defer execCancel()
	stopWatch := w.watchExecution(executionCtx, n, claimedAt, cancelExecution)
	defer stopWatch()

	// 关键日志：节点认领成功，标记执行入口，便于在普通日志中追踪 worker 调度边界。
	obs.From(ctx).InfoContext(ctx, "worker claimed node",
		"worker_id", w.ID, "run_id", n.RunID, "node_id", n.ID, "tenant_id", n.TenantID,
		"node_type", n.Type, "attempt", n.Attempt)
	w.emitRuntimeEvent(ctx, contracts.RuntimeEvent{
		ID: fmt.Sprintf("runtime-event-%s-start-%d", n.ID, time.Now().UnixNano()), RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID,
		Type: contracts.EventNodeStarted, Timestamp: time.Now(), Data: map[string]any{"type": n.Type, "name": n.Name, "attempt": n.Attempt},
	})

	var output string
	var execErr error
	budgetExceeded := false
	if run.MaxTokens > 0 && (n.Type == model.NodeLLM || n.Type == model.NodeReflect) {
		used, terr := w.Store.RunTokenUsage(execCtx, n.TenantID, n.RunID)
		if terr != nil {
			obs.From(ctx).WarnContext(ctx, "token usage check failed",
				"run_id", n.RunID, "node_id", n.ID, "error", terr)
		} else if used >= run.MaxTokens {
			budgetExceeded = true
			execErr = fmt.Errorf("token budget exceeded: used %d >= limit %d", used, run.MaxTokens)
		}
	}
	if execErr == nil && execCtx.Err() == nil {
		output, execErr = w.Exec.Execute(execCtx, n)
	}
	stopWatch()
	if cause := context.Cause(executionCtx); cause != nil {
		if errors.Is(cause, errRunCancelled) {
			return w.handleRunCancelled(ctx, n)
		}
		return cause
	}
	if execCtx.Err() != nil {
		execErr = execCtx.Err()
	}
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
		if !blocked && !budgetExceeded && w.Retry.ShouldRetry(next) {
			// 仍可重试：指数退避，置回 READY 并安排 ready_at，ack 任务。
			// recovery 的 ReadyTasks 扫描会在 ready_at 到期后补投递，实现真正的退避重试。
			readyAt := time.Now().Add(w.Retry.Backoff(next))
			ok, rerr := w.Store.RetryNode(ctx, n.TenantID, n.ID, n.LeaseOwner, n.Version, readyAt)
			if rerr != nil {
				return fmt.Errorf("retry node: %w (exec err: %v)", rerr, execErr)
			}
			if !ok {
				// 版本/状态已变（可能被恢复抢占），不 ack，任务重投递由恢复机制收敛。
				return fmt.Errorf("%w: retry %s (exec err: %v)", errLeaseLost, n.ID, execErr)
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
		fe := model.Event{ID: idgen.New("event"), Type: "AgentStepFailed", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Error: execErr.Error(), Timestamp: time.Now()}
		injectTraceContext(ctx, &fe)
		payload, _ := json.Marshal(fe)
		ok, ferr := w.Store.FailNodeWithOutbox(ctx, n, model.OutboxMessage{ID: fe.ID, EventType: fe.Type, AggregateID: n.RunID, Payload: string(payload)})
		if ferr != nil {
			return fmt.Errorf("persist failure: %w (exec err: %v)", ferr, execErr)
		}
		if !ok {
			return errLeaseLost
		}
		_ = w.Store.EnqueueDLQ(ctx, n.TenantID, n.RunID, n.ID, execErr.Error(), next, output)
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
	e := model.Event{ID: idgen.New("event"), Type: eventType, RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Output: output, Timestamp: time.Now()}
	injectTraceContext(ctx, &e)
	payload, _ := json.Marshal(e)
	// 节点完成 + Outbox 事件在同一事务内提交，保证状态与事件一致。
	// 上下文不再通过 checkpoint 累积（存在读改写竞态），改由 ContextLoader 从已提交的
	// SUCCESS 节点派生，保证并行节点不会相互覆盖对话历史。
	ok, err = w.Store.CompleteNodeWithOutbox(ctx, n, output, model.OutboxMessage{ID: e.ID, EventType: e.Type, AggregateID: n.RunID, Payload: string(payload)})
	if err != nil {
		return err
	}
	if !ok {
		return errLeaseLost
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

func (w *Worker) watchExecution(ctx context.Context, n *model.Node, claimedAt time.Time, abort context.CancelCauseFunc) func() {
	watchCtx, cancel := context.WithCancel(ctx)
	ttl, interval := w.leaseTTLOrDefault(), w.heartbeatIntervalOrDefault()
	// Start timing before the DB call and reserve a margin for scheduling/network delays.
	deadline := claimedAt.Add(ttl - ttl/10)
	if !time.Now().Before(deadline) {
		abort(errLeaseLost)
	}
	watchdog := time.AfterFunc(time.Until(deadline), func() { abort(errLeaseLost) })
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer watchdog.Stop()
		heartbeat := time.NewTicker(interval)
		defer heartbeat.Stop()
		signals := time.NewTicker(min(100*time.Millisecond, interval))
		defer signals.Stop()
		poll := w.pollInterval
		if poll <= 0 {
			poll = 30 * time.Second
			if w.CancelBus == nil {
				poll = interval
			}
		}
		nextPoll := time.Now().Add(poll)
		for watchCtx.Err() == nil {
			select {
			case <-watchCtx.Done():
				return
			case <-heartbeat.C:
				started := time.Now()
				callCtx, stop := context.WithTimeout(watchCtx, min(interval, time.Until(deadline)))
				ok, err := w.Store.RenewLease(callCtx, n.TenantID, n.ID, n.LeaseOwner, n.Version, ttl)
				stop()
				if err != nil {
					obs.From(ctx).WarnContext(ctx, "lease renew failed", "node_id", n.ID, "error", err)
					continue
				}
				if !ok {
					abort(errLeaseLost)
					return
				}
				deadline = started.Add(ttl - ttl/10)
				if !time.Now().Before(deadline) {
					abort(errLeaseLost)
					return
				}
				watchdog.Reset(time.Until(deadline))
			case now := <-signals.C:
				signalled := w.CancelBus != nil && w.CancelBus.Cancelled(n.RunID)
				if !signalled && now.Before(nextPoll) {
					continue
				}
				nextPoll = now.Add(poll)
				callCtx, stop := context.WithTimeout(watchCtx, min(interval, time.Until(deadline)))
				run, err := w.Store.GetRun(callCtx, n.TenantID, n.RunID)
				stop()
				if err != nil {
					obs.From(ctx).WarnContext(ctx, "cancel check failed", "run_id", n.RunID, "error", err)
					continue
				}
				if run.Status == model.RunCancelRequested || run.Status == model.RunCancelled {
					abort(errRunCancelled)
					return
				}
			}
		}
	}()
	return func() {
		cancel()
		<-done
	}
}

func (w *Worker) handleRunCancelled(ctx context.Context, n *model.Node) error {
	e := model.Event{ID: idgen.New("event"), Type: "AgentStepFailed", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Error: errRunCancelled.Error(), Timestamp: time.Now()}
	injectTraceContext(ctx, &e)
	payload, _ := json.Marshal(e)
	ok, err := w.Store.CancelNodeWithOutbox(ctx, n, model.OutboxMessage{ID: e.ID, EventType: e.Type, AggregateID: n.RunID, Payload: string(payload)})
	if err != nil {
		return err
	}
	if !ok {
		return errLeaseLost
	}
	obs.From(ctx).InfoContext(ctx, "node cancelled", "run_id", n.RunID, "node_id", n.ID)
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

// NewFromEnv 从环境变量 WORKER_ID 读取标识，缺省时生成 UUIDv7。
// 默认使用 Echo（Stub）LLM + Search/Calculator 工具，并注入真实 store 启用 tool_call 幂等。
// 配置 OPENAI_BASE_URL + OPENAI_API_KEY 时切换为真实 OpenAI 兼容 HTTP 客户端。
func NewFromEnv(s *store.MySQL, q *queue.RedisQueue, r *event.RocketMQ) *Worker {
	id := os.Getenv("WORKER_ID")
	if id == "" {
		id = idgen.New("worker")
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
	if router, configured, rerr := llm.RouterFromEnv(creds); rerr != nil {
		obs.From(context.Background()).ErrorContext(context.Background(), "model provider configuration invalid; falling back to stub LLM", "error", rerr)
	} else if configured {
		client = router
	}
	// ToolStore=s 使 TOOL 节点经 tool_call 表保证幂等（SUCCESS 复用、崩溃在途拒绝重执行）。
	// Retry=指数退避策略，失败可重试节点置回 READY 并按 ready_at 补投递，耗尽入 DLQ。
	// UsageRecorder=s 使 LLM 节点的 token 用量与成本落 llm_usage 表，支撑成本分析。
	disp := &executor.Dispatcher{LLM: client, Tools: tools, ModelProvider: llmadapter.New(client), ToolProvider: tooladapter.New(tools), ToolStore: s, UsageRecorder: s, Pricer: DefaultPricer, DefaultModel: envString("LLM_DEFAULT_MODEL", "")}
	// P0 上下文优化（详见 internal/worker/context.go）：
	//  - 祖先作用域：节点只加载 agent_edge 传递闭包上的 SUCCESS 祖先，
	//    并行分支的无关产物不再灌入；CONTEXT_ANCESTOR_SCOPE=false 可退回全量；
	//  - 工具结果遮蔽：REFLECT 控制节点排除、窗口外/超长工具结果替换为带 node_id
	//    指针的占位符，原文始终保留在 agent_node.output；CONTEXT_TOOL_MASKING=false 可关；
	//  - 跨 Run 语义记忆仍按 MEMORY_ENABLED 装配，失败静默降级。
	// 历史按 (finished_at, node_id) 确定序派生，崩溃恢复也能重建且顺序稳定。
	disp.ContextLoader = newContextLoader(s, newContextOptionsFromEnv(creds))
	// Prompt 前缀缓存默认关闭：开启后向网关透传 Run 级 prompt_cache_key，
	// 配合稳定的消息布局复用 KV 缓存（需网关支持该 OpenAI 兼容扩展字段）。
	disp.PromptCache = envBool("LLM_PROMPT_CACHE", false)

	// 安全中间件装配：不可信输入防护挂在工具调用前，数据脱敏同时挂在
	// 工具结果回传、模型输出与事件出域三处。人工闸门的落库能力在 newSecurityBundle 内部
	// 已交给护栏持有，worker 侧不需要再拿一份。
	sec := newSecurityBundle(s, q, creds)
	disp.ToolChain = sec.ToolChain
	disp.ModelChain = sec.ModelChain
	w := &Worker{Store: s, Queue: q, Events: r, ID: id, Retry: retry.Default(), Exec: disp, EventChain: sec.EventChain,
		ExecTimeout: envDuration("WORKER_EXEC_TIMEOUT", DefaultExecTimeout)}
	if q != nil {
		w.CancelBus = q.NewCancelBus()
	}
	return w
}

// newMemoryOptionsFromEnv 按环境变量装配读取路径的记忆能力。
//
// 后端选择（MEMORY_BACKEND，读路径外移 rag-api 的迁移开关）：
//   - local（缺省）：直连 Qdrant 的 VectorMemory，与历史行为完全一致；
//   - remote：经 rag-api 检索（RemoteMemory）。MEMORY_REMOTE_FALLBACK=true
//     （缺省）且本地依赖可用时，远程失败/降级自动回退直连（FallbackMemory）；
//   - shadow：双跑对比（ShadowMemory）——本地与远程并行执行，结果恒以直连
//     为准，差异只打日志，用于切换前的等价性验收（Phase 1 影子对比）。
//
// 装配条件沿用"缺啥降啥"：MEMORY_ENABLED 未开启即整体关闭；本地侧
// （embedding 凭证 / Qdrant）缺失时降级为本地不可用；remote 侧要求
// RAG_API_URL。任何缺失都只 warn，绝不让 worker 起不来。
func newMemoryOptionsFromEnv(creds contracts.CredentialProvider) MemoryOptions {
	if !envBool("MEMORY_ENABLED", false) {
		return MemoryOptions{}
	}
	backend := envString("MEMORY_BACKEND", "local")
	switch backend {
	case "local", "remote", "shadow":
	default:
		obs.From(context.Background()).WarnContext(context.Background(), "unknown MEMORY_BACKEND; falling back to local",
			"backend", backend)
		backend = "local"
	}
	fallback := envBool("MEMORY_REMOTE_FALLBACK", true)
	timeout := envDuration("MEMORY_SEARCH_TIMEOUT", DefaultMemorySearchTimeout)

	// 本地直连仅在需要时装配：remote 且不回退时完全跳过，
	// worker 随之摆脱 embedding 凭证与 Qdrant 依赖（已外移到 rag-api）。
	needLocal := backend == "local" || backend == "shadow" || (backend == "remote" && fallback)
	var local providers.MemoryProvider
	if needLocal {
		local = newLocalVectorMemoryFromEnv(creds)
	}

	var remote providers.MemoryProvider
	if backend == "remote" || backend == "shadow" {
		if url := envString("RAG_API_URL", ""); url != "" {
			remote = &providers.RemoteMemory{
				BaseURL: url,
				// 服务间共享令牌：Phase 1 与 rag-api 同信任域部署，直接读 env；
				// 后续接入专属 CredentialPurpose 时改走托管层即可。
				Token:    envString("RAG_API_TOKEN", ""),
				Client:   &http.Client{Timeout: timeout}, // ctx 预算之外的兜底
				TopK:     envInt("MEMORY_TOP_K", providers.DefaultMemoryTopK),
				MinScore: envFloat32("MEMORY_MIN_SCORE", providers.DefaultMemoryMinScore),
			}
		} else {
			obs.From(context.Background()).WarnContext(context.Background(), "MEMORY_BACKEND requests rag-api but RAG_API_URL is empty; remote side disabled",
				"backend", backend)
		}
	}

	var mem providers.MemoryProvider
	switch backend {
	case "remote":
		switch {
		case remote == nil:
			// 远程未配置：降级为本地行为（local 可能为 nil，即记忆关闭）。
			mem = local
		case fallback && local != nil:
			mem = &providers.FallbackMemory{Primary: remote, Fallback: local}
		default:
			mem = remote
		}
	case "shadow":
		switch {
		case local == nil && remote == nil:
			mem = nil
		case local == nil:
			// 影子没有基准线（直连）就失去对比意义：退化为纯远程并大声告警。
			obs.From(context.Background()).WarnContext(context.Background(), "shadow memory without local baseline; degrading to remote-only")
			mem = remote
		default:
			// Secondary 允许为 nil：Search 内部退化为纯本地，行为不劣化。
			mem = &providers.ShadowMemory{Primary: local, Secondary: remote}
		}
	default: // local
		mem = local
	}
	if mem == nil {
		return MemoryOptions{}
	}
	obs.From(context.Background()).InfoContext(context.Background(), "long-term memory enabled",
		"backend", backend, "top_k", envInt("MEMORY_TOP_K", providers.DefaultMemoryTopK),
		"min_score", envFloat32("MEMORY_MIN_SCORE", providers.DefaultMemoryMinScore),
		"timeout", timeout.String())
	return MemoryOptions{
		Memory:           mem,
		SearchTimeout:    timeout,
		MaxMessages:      envInt("MEMORY_MAX_MESSAGES", DefaultMaxMemoryMessages),
		MaxContextTokens: envInt("MEMORY_MAX_CONTEXT_TOKENS", DefaultMaxContextTokens),
	}
}

// newLocalVectorMemoryFromEnv 装配直连 Qdrant 的本地记忆。
// embedding 凭证缺失或 Qdrant 构造失败时返回 nil（本地侧降级为不可用），
// 由调用方决定降级语义——记忆是增强项，缺依赖不该让 worker 起不来。
func newLocalVectorMemoryFromEnv(creds contracts.CredentialProvider) providers.MemoryProvider {
	// 探测 embedding 凭证是否可用。这里只看"取不取得到"，不取明文 ——
	// 明文由 embedder 在每次请求时经 port 现取，密钥不进本函数的任何变量。
	if _, err := creds.Credential(context.Background(), contracts.CredentialPurposeEmbedding); err != nil {
		// 没有 embedding 能力就无法做语义召回。此处静默降级：
		// 主链路的对话补全可能走 Stub，但记忆需要真实向量，二者要求不同。
		obs.From(context.Background()).WarnContext(context.Background(), "no embedding credential; local memory unavailable",
			"error", err)
		return nil
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
		obs.From(context.Background()).WarnContext(context.Background(), "qdrant unavailable; local memory unavailable",
			"error", err)
		return nil
	}
	return &providers.VectorMemory{
		Embedder:   embedder,
		Store:      vs,
		Collection: envString("QDRANT_COLLECTION", providers.DefaultMemoryCollection),
		TopK:       envInt("MEMORY_TOP_K", providers.DefaultMemoryTopK),
		MinScore:   envFloat32("MEMORY_MIN_SCORE", providers.DefaultMemoryMinScore),
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
