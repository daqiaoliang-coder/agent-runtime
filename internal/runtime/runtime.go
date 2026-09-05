package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

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
