package runtime

import (
	"agent-runtime/internal/model"
	"context"
	"fmt"
)

// HITLStore keeps the interrupt record and Run state transition in one transaction.
// This prevents a crash from leaving WAITING_HUMAN without a durable interrupt record.
type HITLStore interface {
	InterruptRun(context.Context, string, string, string, string, int64) (bool, error)
	ResumeRun(context.Context, string, string, string, int64) (bool, error)
}

func (r *Runtime) Interrupt(ctx context.Context, tenant, runID, nodeID, reason string) error {
	h, ok := r.Store.(HITLStore)
	if !ok {
		return fmt.Errorf("hitl store is not configured")
	}
	run, err := r.Store.GetRun(ctx, tenant, runID)
	if err != nil {
		return err
	}
	if run.Status != model.RunRunning {
		return fmt.Errorf("run %s cannot be interrupted from %s", runID, run.Status)
	}
	ok, err = h.InterruptRun(ctx, tenant, runID, nodeID, reason, run.Version)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run %s changed while interrupting", runID)
	}
	return nil
}

func (r *Runtime) Resume(ctx context.Context, tenant, runID, decision string) error {
	h, ok := r.Store.(HITLStore)
	if !ok {
		return fmt.Errorf("hitl store is not configured")
	}
	run, err := r.Store.GetRun(ctx, tenant, runID)
	if err != nil {
		return err
	}
	if run.Status != model.RunWaitingHuman {
		return fmt.Errorf("run %s is not waiting for human: %s", runID, run.Status)
	}
	ok, err = h.ResumeRun(ctx, tenant, runID, decision, run.Version)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("run %s changed while resuming", runID)
	}
	return nil
}
