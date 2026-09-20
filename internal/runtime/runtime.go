package runtime

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/obs"
	"agent-runtime/internal/trace"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// CancelStore 把 Run 翻到 CANCEL_REQUESTED 并取消其 PENDING/READY 节点的原子操作。
// 与 HITLStore 同理：以独立接口暴露，*store.MySQL 天然满足，fakeStore 可按需实现。
type CancelStore interface {
	CancelRun(ctx context.Context, tenant, runID, reason string, version int64) (bool, error)
}

// Runtime 负责创建 Run 并完成初始调度。
// 它组合了持久化（Store）、任务队列（Queue）和规划器（Planner）。
// Store/Queue 为接口类型，便于单元测试注入 fake。
type Runtime struct {
	Store   Store
	Queue   Queue
	Planner Planner
}

// CreateRun 创建一次 Agent 运行，流程如下：
//  1. 生成 Run 记录并写入 MySQL；
//  2. 调用 Planner 生成 DAG 计划并落库；
//  3. 将无依赖的根节点标记为 READY 并投递到 Redis 队列；
//  4. 通过 CAS 将 Run 状态从 PENDING 切换为 RUNNING，保证并发安全。
//
// threadID 标识会话维度：同一 Thread 下的多个 Run 共享长期记忆（见 providers.MemorySearcher）。
// 传空串表示无会话隔离，记忆检索将跳过 thread_id 过滤。
//
// 所有落库与入队操作均携带租户身份（run.TenantID），保证后续跨进程链路可做租户隔离。
func (r *Runtime) CreateRun(ctx context.Context, tenant, agent, input, threadID string) (*model.Run, error) {
	// Run 创建的根 span：整条 Trace 的起点，串联 planner、入队与下游 worker 执行。
	ctx, span := trace.StartSpan(ctx, "run.create")
	defer span.End()
	id := fmt.Sprintf("run-%d", time.Now().UnixNano())
	span.SetAttributes(
		attribute.String("run.id", id),
		attribute.String("tenant.id", tenant),
		attribute.String("agent.id", agent),
		attribute.String("thread.id", threadID),
	)
	run := &model.Run{ID: id, TenantID: tenant, ThreadID: threadID, AgentID: agent, Status: model.RunPending, Input: input, MaxSteps: 50, MaxRounds: 10, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := r.Store.CreateRun(ctx, run); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	plan, err := r.Planner.Plan(ctx, run)
	if err != nil {
		// 计划失败：Run 已落库为 PENDING 但不会有任何节点，事件驱动的 Resumer 无法感知它。
		// 主动 CAS 收敛为 FAILED，避免留下永不终止的孤儿 Run（最佳努力：标记失败不掩盖原始错误）。
		reason := fmt.Sprintf("planning failed: %v", err)
		if ok, cerr := r.Store.UpdateRunCAS(ctx, run.TenantID, run.ID, run.Version, model.RunFailed, "", reason); cerr != nil {
			obs.From(ctx).ErrorContext(ctx, "run mark-failed on planning error failed",
				"run_id", run.ID, "tenant_id", run.TenantID, "error", cerr, "planning_error", err)
		} else if !ok {
			obs.From(ctx).WarnContext(ctx, "run mark-failed on planning error skipped (version conflict)",
				"run_id", run.ID, "tenant_id", run.TenantID)
		}
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	if err := r.Store.InsertPlan(ctx, run.ID, run.TenantID, plan); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	// 先将 Run 切换到 RUNNING，再入队根节点，避免 worker 抢先消费时看到 PENDING 而误取消。
	// 根节点已 MarkReady，即使入队失败，ReadyTasks 扫描也会在恢复周期中补投递。
	ok, _ := r.Store.UpdateRunCAS(ctx, run.TenantID, run.ID, run.Version, model.RunRunning, "", "")
	if !ok {
		err := fmt.Errorf("run version conflict")
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	// 仅入队无依赖的根节点，其余节点等待依赖完成后由 Resumer 推进。
	for _, n := range plan.Nodes {
		if len(n.DependsOn) == 0 {
			if err := r.Store.MarkReady(ctx, run.TenantID, n.ID); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
			if err := r.Queue.Enqueue(ctx, model.Task{RunID: run.ID, NodeID: n.ID, TenantID: run.TenantID}); err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return nil, err
			}
		}
	}
	// 关键日志：Run 创建并切到 RUNNING，标志一次 Agent 运行的真正起点，串联调度入口与下游 worker。
	obs.From(ctx).InfoContext(ctx, "run created",
		"run_id", run.ID, "tenant_id", run.TenantID, "agent_id", run.AgentID, "root_nodes", countRoots(plan.Nodes))
	span.SetAttributes(attribute.Int("plan.node_count", len(plan.Nodes)))
	return r.Store.GetRun(ctx, run.TenantID, run.ID)
}

// countRoots 统计 DAG 中无依赖的根节点数，用于在创建日志中反映初始并行度。
func countRoots(nodes []model.PlanNode) (n int) {
	for _, x := range nodes {
		if len(x.DependsOn) == 0 {
			n++
		}
	}
	return n
}

// EventJSON 将领域事件序列化为 JSON 字符串，便于投递到消息中间件。
func EventJSON(e model.Event) string { b, _ := json.Marshal(e); return string(b) }

// Cancel 请求取消一个运行中的 Run：通过 CAS 把 Run 从 RUNNING 切到 CANCEL_REQUESTED，
// 并在同一事务内把该 Run 下所有 PENDING/READY 节点置为 CANCELLED，阻止 worker 认领新节点。
// 正在执行的 RUNNING 节点不被中断：它们会自然完成，Resumer 据此收敛 Run 到 CANCELLED。
// 仅 RunRunning 状态可被取消；并发变更（version 不匹配）返回错误而非静默失败。
func (r *Runtime) Cancel(ctx context.Context, tenant, runID, reason string) error {
	cs, ok := r.Store.(CancelStore)
	if !ok {
		return fmt.Errorf("cancel store is not configured")
	}
	run, err := r.Store.GetRun(ctx, tenant, runID)
	if err != nil {
		return err
	}
	if run.Status != model.RunRunning {
		return fmt.Errorf("run %s cannot be cancelled from %s", runID, run.Status)
	}
	ok, err = cs.CancelRun(ctx, tenant, runID, reason, run.Version)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run %s changed while cancelling", runID)
	}
	// 关键日志：用户取消请求已落盘，Run 进入 CANCEL_REQUESTED，等待节点收敛到 CANCELLED。
	obs.From(ctx).InfoContext(ctx, "run cancel requested",
		"run_id", runID, "tenant_id", tenant, "reason", reason)
	return nil
}
