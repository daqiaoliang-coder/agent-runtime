package runtime

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/model"
	"agent-runtime/internal/trace"
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
)

// Resumer 是事件驱动的 DAG 推进器。
// 它消费 RocketMQ 中的节点完成/失败事件，激活后继节点并在全部完成时收敛 Run。
// Store/Queue 为接口类型，便于单元测试注入 fake。
type Resumer struct {
	Store Store
	Queue Queue
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
	if e.Type != "AgentStepCompleted" && e.Type != "AgentStepFailed" {
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
	if e.Type == "AgentStepFailed" {
		// 修复 Bug2：必须先拿到 Run 当前 version，再用 CAS 标记失败，否则 WHERE version=0 永不匹配。
		run, err := r.Store.GetRun(ctx, e.TenantID, e.RunID)
		if err != nil {
			return fmt.Errorf("load run for failure: %w", err)
		}
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
	// 修复 Bug3：不再用 _ 吞掉错误，DB 失败时显式返回，避免后续 run 为 nil 触发 panic。
	failed, err := r.Store.RunHasFailure(ctx, e.TenantID, e.RunID)
	if err != nil {
		return fmt.Errorf("check run failure: %w", err)
	}
	run, err := r.Store.GetRun(ctx, e.TenantID, e.RunID)
	if err != nil {
		return fmt.Errorf("load run for completion: %w", err)
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
	return nil
}

// 保持对 event 包的引用，便于未来直接在此处使用 RocketMQ 消费者类型。
var _ = event.RocketMQ{}
