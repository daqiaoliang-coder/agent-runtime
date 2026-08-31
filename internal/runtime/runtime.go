package runtime

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"context"
	"encoding/json"
	"fmt"
	"time"
)

type Runtime struct {
	Store   *store.MySQL
	Queue   *queue.RedisQueue
	Planner Planner
}

func (r *Runtime) CreateRun(ctx context.Context, tenant, agent, input string) (*model.Run, error) {
	id := fmt.Sprintf("run-%d", time.Now().UnixNano())
	run := &model.Run{ID: id, TenantID: tenant, AgentID: agent, Status: model.RunPending, Input: input, MaxSteps: 50, CreatedAt: time.Now(), UpdatedAt: time.Now()}
	if err := r.Store.CreateRun(ctx, run); err != nil {
		return nil, err
	}
	plan, err := r.Planner.Plan(run)
	if err != nil {
		return nil, err
	}
	if err := r.Store.InsertPlan(ctx, run.ID, plan); err != nil {
		return nil, err
	}
	for _, n := range plan.Nodes {
		if len(n.DependsOn) == 0 {
			if err := r.Store.MarkReady(ctx, n.ID); err != nil {
				return nil, err
			}
			if err := r.Queue.Enqueue(ctx, model.Task{RunID: run.ID, NodeID: n.ID}); err != nil {
				return nil, err
			}
		}
	}
	ok, _ := r.Store.UpdateRunCAS(ctx, run.ID, run.Version, model.RunRunning, "", "")
	if !ok {
		return nil, fmt.Errorf("run version conflict")
	}
	return r.Store.GetRun(ctx, run.ID)
}
func EventJSON(e model.Event) string { b, _ := json.Marshal(e); return string(b) }
