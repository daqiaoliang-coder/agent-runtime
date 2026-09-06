package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
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
// 所有落库与入队操作均携带租户身份（run.TenantID），保证后续跨进程链路可做租户隔离。
func (r *Runtime) CreateRun(ctx context.Context, tenant, agent, input string) (*model.Run, error) {
	id := fmt.Sprintf("run-%d", time.Now().UnixNano())
	run := &model.Run{ID: id, TenantID: tenant, AgentID: agent, Status: model.RunPending, Input: input, MaxSteps: 50, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := r.Store.CreateRun(ctx, run); err != nil {
		return nil, err
	}
	plan, err := r.Planner.Plan(ctx, run)
	if err != nil {
		return nil, err
	}
	if err := r.Store.InsertPlan(ctx, run.ID, run.TenantID, plan); err != nil {
		return nil, err
	}
	// 仅入队无依赖的根节点，其余节点等待依赖完成后由 Resumer 推进。
	for _, n := range plan.Nodes {
		if len(n.DependsOn) == 0 {
			if err := r.Store.MarkReady(ctx, run.TenantID, n.ID); err != nil {
				return nil, err
			}
			if err := r.Queue.Enqueue(ctx, model.Task{RunID: run.ID, NodeID: n.ID, TenantID: run.TenantID}); err != nil {
				return nil, err
			}
		}
	}
	ok, _ := r.Store.UpdateRunCAS(ctx, run.TenantID, run.ID, run.Version, model.RunRunning, "", "")
	if !ok {
		return nil, fmt.Errorf("run version conflict")
	}
	// 关键日志：Run 创建并切到 RUNNING，标志一次 Agent 运行的真正起点，串联调度入口与下游 worker。
	log.Printf("run created run=%s tenant=%s agent=%s root_nodes=%d", run.ID, run.TenantID, run.AgentID, countRoots(plan.Nodes))
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
	log.Printf("run cancel requested run=%s tenant=%s reason=%q", runID, tenant, reason)
	return nil
}
