package runtime

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/model"
	"agent-runtime/internal/trace"
	"context"
	"fmt"
	"log"

	"go.opentelemetry.io/otel/attribute"
)

// Resumer 是事件驱动的 DAG 推进器。
// 它消费 RocketMQ 中的节点完成/失败事件，激活后继节点并在全部完成时收敛 Run。
// Store/Queue 为接口类型，便于单元测试注入 fake。
// Planner 用于多轮 Plan：ReplanRequested 事件触发时调用 Planner.Replan 续规划。
type Resumer struct {
	Store   Store
	Queue   Queue
	Planner Planner
}

// Handle 处理单个领域事件：
//   - AgentStepFailed：先读取当前 Run 版本再 CAS 标记 FAILED（修复：此前传 version=0 永不命中）；
//   - AgentStepCompleted：检查后继子节点的依赖是否全部就绪，就绪则标记 READY 并入队；
//   - 当 Run 下所有节点都已终态（SUCCESS/FAILED）时，按是否存在失败节点收敛为 SUCCESS/FAILED。
//
// 关键点：依赖就绪检查保证 DAG 拓扑顺序；CAS 保证收敛不会被并发重复；
// 所有查询携带事件中的 e.TenantID 做租户隔离，错误不再被静默吞掉。
func (r *Resumer) Handle(ctx context.Context, e model.Event) error {
	// 轨迹 span：标记 DAG 推进事件处理，携带事件类型与 run/tenant 维度。
	ctx, span := trace.StartSpan(ctx, "resumer.handle")
	defer span.End()
	span.SetAttributes(
		attribute.String("event.type", e.Type),
		attribute.String("event.id", e.ID),
		attribute.String("run.id", e.RunID),
		attribute.String("node.id", e.NodeID),
		attribute.String("tenant.id", e.TenantID),
	)
	if e.Type != "AgentStepCompleted" && e.Type != "AgentStepFailed" && e.Type != "ReplanRequested" {
		return nil
	}
	// 消费端幂等 Inbox：处理前查表，已处理过的事件直接跳过，去重 RocketMQ 至少一次投递的重复消息。
	seen, err := r.Store.InboxSeen(ctx, e.TenantID, e.ID)
	if err != nil {
		return fmt.Errorf("inbox seen: %w", err)
	}
	if seen {
		return nil
	}
	// 标记后模式：先推进事件，成功后再写 Inbox；处理中途崩溃不会误标完成，
	// 重投递可幂等重放（底层 CAS/状态机本身幂等）。
	if err := r.process(ctx, e); err != nil {
		return err
	}
	if err := r.Store.MarkInbox(ctx, e.TenantID, e.ID); err != nil {
		return fmt.Errorf("mark inbox: %w", err)
	}
	return nil
}

// process 执行事件推进逻辑（原 Handle 主体）。返回 error 时不写 Inbox，触发重投递。
func (r *Resumer) process(ctx context.Context, e model.Event) error {
	// ReplanRequested：REFLECT 节点决定续规，调 Planner.Replan 追加新节点到 DAG。
	if e.Type == "ReplanRequested" {
		run, err := r.Store.GetRun(ctx, e.TenantID, e.RunID)
		if err != nil {
			return fmt.Errorf("load run for replan: %w", err)
		}
		if run.Status == model.RunCancelRequested {
			return nil // 已取消，不续规
		}
		return r.handleReplan(ctx, e, run)
	}
	// 先获取 Run：Failed 与 Completed 两条路径都需要 version 做 CAS，且需要检查取消。
	run, err := r.Store.GetRun(ctx, e.TenantID, e.RunID)
	if err != nil {
		return fmt.Errorf("load run: %w", err)
	}
	// 取消优先：Run 处于 CANCEL_REQUESTED 时，不推进 DAG 也不标 FAILED，
	// 仅检查是否所有节点终态（含 CANCELLED）以收敛到 CANCELLED。
	if run.Status == model.RunCancelRequested {
		return r.tryConvergeCancelled(ctx, e, run)
	}
	if e.Type == "AgentStepFailed" {
		// Bug2 修复保持：run.Version 来自上方 GetRun，CAS 标记 FAILED。
		_, err = r.Store.UpdateRunCAS(ctx, e.TenantID, e.RunID, run.Version, model.RunFailed, "", e.Error)
		return err
	}
	children, err := r.Store.Children(ctx, e.TenantID, e.NodeID)
	if err != nil {
		return err
	}
	for _, child := range children {
		ready, err := r.Store.DependenciesReady(ctx, e.TenantID, child.NodeID)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}
		if err := r.Store.MarkReady(ctx, e.TenantID, child.NodeID); err != nil {
			return err
		}
		// child 由 Children 查询带出，已携带 TenantID，保持租户上下文继续投递。
		if err := r.Queue.Enqueue(ctx, child); err != nil {
			return err
		}
	}
	// 检查 Run 是否全部结束，是则收敛。
	complete, err := r.Store.RunComplete(ctx, e.TenantID, e.RunID)
	if err != nil {
		return err
	}
	if !complete {
		return nil
	}
	// Bug3 修复保持：不再用 _ 吞掉错误，DB 失败时显式返回。
	failed, err := r.Store.RunHasFailure(ctx, e.TenantID, e.RunID)
	if err != nil {
		return fmt.Errorf("check run failure: %w", err)
	}
	status := model.RunSuccess
	out := "completed"
	if failed {
		status = model.RunFailed
		out = "one or more nodes failed"
	}
	ok, err := r.Store.UpdateRunCAS(ctx, e.TenantID, e.RunID, run.Version, status, e.NodeID, out)
	if err != nil {
		return err
	}
	if !ok {
		// 版本冲突意味着并发已推进，非致命错误，返回 nil 避免事件无意义重试。
		return nil
	}
	// 关键日志：Run 收敛到终态，标志 DAG 全部节点结束，是 Runtime 最重要的一次状态跃迁。
	log.Printf("run settled run=%s tenant=%s status=%s trigger_node=%s", e.RunID, e.TenantID, status, e.NodeID)
	return nil
}

// tryConvergeCancelled 在 Run 处于 CANCEL_REQUESTED 时尝试收敛到 CANCELLED。
// 不推进 DAG（子节点要么已被 CancelRun 取消，要么已在终态）；仅当所有节点
// 均为终态（SUCCESS/FAILED/CANCELLED）时才 CAS 到 CANCELLED。
// RUNNING 节点完成后会再次投递事件触发本路径，直到可收敛。
func (r *Resumer) tryConvergeCancelled(ctx context.Context, e model.Event, run *model.Run) error {
	complete, err := r.Store.RunComplete(ctx, e.TenantID, e.RunID)
	if err != nil {
		return fmt.Errorf("check run complete for cancel: %w", err)
	}
	if !complete {
		return nil
	}
	ok, err := r.Store.UpdateRunCAS(ctx, e.TenantID, e.RunID, run.Version, model.RunCancelled, e.NodeID, "cancelled by user")
	if err != nil {
		return err
	}
	if !ok {
		// 版本冲突意味着并发已推进（或 Run 已偏离 CANCEL_REQUESTED），返回 nil 避免事件重试。
		return nil
	}
	log.Printf("run cancelled run=%s tenant=%s trigger_node=%s", e.RunID, e.TenantID, e.NodeID)
	return nil
}

// handleReplan 执行多轮 Plan 的续规流程：
// 1. 检查死循环防护（PlanningRound 上限、节点总数上限、token 预算上限）；
// 2. 查询已完成节点（含 outputs）作为 Planner 的上下文；
// 3. 调用 Planner.Replan 产出新一轮节点；
// 4. 将新节点无 DependsOn 的根链接到触发续规的 REFLECT 节点（e.NodeID）；
// 5. InsertPlan 追加节点 + 边到同一 Run 的 DAG；
// 6. 激活依赖已就绪的新节点（REFLECT 已 SUCCESS，直接挂接的根节点可立即执行）。
//
// 防护命中时：不追加新节点，直接 CAS 将 Run 收敛到 FAILED，避免 Planner 持续
// 输出 replan 决策导致无限续规。
func (r *Resumer) handleReplan(ctx context.Context, e model.Event, run *model.Run) error {
	if r.Planner == nil {
		return fmt.Errorf("replan requested but planner not configured")
	}
	completed, err := r.Store.CompletedNodes(ctx, e.TenantID, e.RunID)
	if err != nil {
		return fmt.Errorf("load completed nodes for replan: %w", err)
	}
	// 死循环防护：检查续规轮次、节点总数、token 预算是否超限。
	if reason := r.checkReplanLimits(ctx, e, run, completed); reason != "" {
		return r.convergeOnLimit(ctx, e, run, reason)
	}
	plan, err := r.Planner.Replan(ctx, run, completed)
	if err != nil {
		return fmt.Errorf("planner replan: %w", err)
	}
	// 链接：新节点中 DependsOn 为空的根节点挂接到触发续规的 REFLECT 节点。
	for i, n := range plan.Nodes {
		if len(n.DependsOn) == 0 {
			plan.Nodes[i].DependsOn = []string{e.NodeID}
		}
		if plan.Nodes[i].ParentNodeID == "" {
			plan.Nodes[i].ParentNodeID = e.NodeID
		}
	}
	if err := r.Store.InsertPlan(ctx, e.RunID, e.TenantID, plan); err != nil {
		return fmt.Errorf("insert replan nodes: %w", err)
	}
	// 激活依赖已就绪的新节点：REFLECT 节点已 SUCCESS，直接挂接的根节点立即可执行。
	for _, n := range plan.Nodes {
		ready, err := r.Store.DependenciesReady(ctx, e.TenantID, n.ID)
		if err != nil {
			return fmt.Errorf("check deps for replan node %s: %w", n.ID, err)
		}
		if !ready {
			continue
		}
		if err := r.Store.MarkReady(ctx, e.TenantID, n.ID); err != nil {
			return fmt.Errorf("mark ready replan node %s: %w", n.ID, err)
		}
		if err := r.Queue.Enqueue(ctx, model.Task{RunID: e.RunID, NodeID: n.ID, TenantID: e.TenantID}); err != nil {
			return fmt.Errorf("enqueue replan node %s: %w", n.ID, err)
		}
	}
	log.Printf("run replanned run=%s tenant=%s trigger_node=%s new_nodes=%d", e.RunID, e.TenantID, e.NodeID, len(plan.Nodes))
	return nil
}

// checkReplanLimits 检查多轮 Plan 的三道防线：续规轮次、节点总数、token 预算。
// 返回非空 reason 表示防线命中，调用方应拒绝续规并收敛 Run。
func (r *Resumer) checkReplanLimits(ctx context.Context, e model.Event, run *model.Run, completed []model.Node) string {
	// 防线 1：续规轮次上限。
	next := nextRound(completed)
	if run.MaxRounds > 0 && next > run.MaxRounds {
		return fmt.Sprintf("max rounds exceeded: next round %d > limit %d", next, run.MaxRounds)
	}
	// 防线 2：节点总数上限（复用 MaxSteps，含所有轮次的节点）。
	if run.MaxSteps > 0 {
		count, err := r.Store.CountNodes(ctx, e.TenantID, e.RunID)
		if err != nil {
			log.Printf("warn: count nodes for replan limits: %v", err)
		} else if count >= run.MaxSteps {
			return fmt.Sprintf("max steps exceeded: nodes %d >= limit %d", count, run.MaxSteps)
		}
	}
	// 防线 3：token 预算上限。
	if run.MaxTokens > 0 {
		used, err := r.Store.RunTokenUsage(ctx, e.TenantID, e.RunID)
		if err != nil {
			log.Printf("warn: token usage for replan limits: %v", err)
		} else if used >= run.MaxTokens {
			return fmt.Sprintf("token budget exhausted: used %d >= limit %d", used, run.MaxTokens)
		}
	}
	return ""
}

// convergeOnLimit 在防线命中时将 Run 直接 CAS 到 FAILED。
// REFLECT 节点已完成（触发 ReplanRequested），不追加新节点则所有节点均终态，
// 无需等待后续事件驱动收敛。
func (r *Resumer) convergeOnLimit(ctx context.Context, e model.Event, run *model.Run, reason string) error {
	ok, err := r.Store.UpdateRunCAS(ctx, e.TenantID, e.RunID, run.Version, model.RunFailed, e.NodeID, reason)
	if err != nil {
		return fmt.Errorf("converge on limit: %w", err)
	}
	if !ok {
		// 版本冲突：并发已推进，返回 nil 避免事件无意义重试。
		return nil
	}
	log.Printf("run failed on limit run=%s tenant=%s trigger_node=%s reason=%q", e.RunID, e.TenantID, e.NodeID, reason)
	return nil
}

// 保持对 event 包的引用，便于未来直接在此处使用 RocketMQ 消费者类型。
var _ = event.RocketMQ{}
