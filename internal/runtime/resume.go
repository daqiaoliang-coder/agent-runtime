package runtime

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/model"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"context"
	"fmt"
)

type Resumer struct {
	Store *store.MySQL
	Queue *queue.RedisQueue
}

func (r *Resumer) Handle(ctx context.Context, e model.Event) error {
	if e.Type != "AgentStepCompleted" && e.Type != "AgentStepFailed" {
		return nil
	}
	if e.Type == "AgentStepFailed" {
		_, err := r.Store.UpdateRunCAS(ctx, e.RunID, 0, model.RunFailed, "", e.Error)
		return err
	}
	children, err := r.Store.Children(ctx, e.NodeID)
	if err != nil {
		return err
	}
	for _, child := range children {
		ready, err := r.Store.DependenciesReady(ctx, child.NodeID)
		if err != nil {
			return err
		}
		if !ready {
			continue
		}
		if err := r.Store.MarkReady(ctx, child.NodeID); err != nil {
			return err
		}
		if err := r.Queue.Enqueue(ctx, child); err != nil {
			return err
		}
	}
	complete, err := r.Store.RunComplete(ctx, e.RunID)
	if err != nil {
		return err
	}
	if !complete {
		return nil
	}
	failed, _ := r.Store.RunHasFailure(ctx, e.RunID)
	run, _ := r.Store.GetRun(ctx, e.RunID)
	status := model.RunSuccess
	out := "completed"
	if failed {
		status = model.RunFailed
		out = "one or more nodes failed"
	}
	ok, err := r.Store.UpdateRunCAS(ctx, e.RunID, run.Version, status, e.NodeID, out)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run completion version conflict")
	}
	return nil
}

var _ = event.RocketMQ{}
