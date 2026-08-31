package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type Runtime struct {
	store     *MemoryStore
	planner   Planner
	executor  *Executor
	scheduler *Scheduler

	runCounter  atomic.Int64
	stepCounter atomic.Int64
}

func NewRuntime() *Runtime {
	store := NewMemoryStore()
	llm := &MockLLM{}

	tools := NewToolRegistry()
	tools.Register(&SearchTool{})

	runtime := &Runtime{
		store:   store,
		planner: &MockPlanner{},
	}

	runtime.executor = NewExecutor(store, llm, tools)
	runtime.scheduler = NewScheduler(4, runtime.executor)

	return runtime
}

func (r *Runtime) CreateRun(input string) (*AgentRun, error) {
	now := time.Now()

	run := &AgentRun{
		ID:        r.newRunID(),
		AgentID:   "default-agent",
		Status:    RunPending,
		MaxSteps:  20,
		Input:     input,
		CreatedAt: now,
		UpdatedAt: now,
	}

	if err := r.store.CreateRun(run); err != nil {
		return nil, err
	}
	return run, nil
}

func (r *Runtime) Run(ctx context.Context, runID string) error {
	run, err := r.store.GetRun(runID)
	if err != nil {
		return err
	}

	if err := r.store.UpdateRunCAS(run.ID, run.Version, func(run *AgentRun) {
		run.Status = RunRunning
		run.UpdatedAt = time.Now()
	}); err != nil {
		return err
	}

	for {
		run, err = r.store.GetRun(runID)
		if err != nil {
			return err
		}

		if run.Steps >= run.MaxSteps {
			return r.finishRun(run, RunFailed, "max steps exceeded")
		}

		if run.Status == RunCancelRequested {
			return r.finishRun(run, RunCancelled, "user cancelled")
		}

		action, err := r.planner.Plan(ctx, run)
		if err != nil {
			return r.finishRun(run, RunFailed, err.Error())
		}

		if action.Type == StepFinish {
			return r.finishRun(run, RunSuccess, "completed")
		}

		step := &AgentStep{
			ID:        r.newStepID(),
			RunID:     run.ID,
			Type:      action.Type,
			Name:      action.Name,
			Input:     action.Input,
			Status:    StepPending,
			CreatedAt: time.Now(),
		}

		if err := r.store.CreateStep(step); err != nil {
			return err
		}

		run, err = r.store.GetRun(runID)
		if err != nil {
			return err
		}

		if err := r.store.UpdateRunCAS(run.ID, run.Version, func(run *AgentRun) {
			run.CurrentStep = step.ID
			run.Steps++
			run.UpdatedAt = time.Now()
		}); err != nil {
			return err
		}

		r.scheduler.Submit(Task{StepID: step.ID})

		for {
			current, err := r.store.GetStep(step.ID)
			if err != nil {
				return err
			}

			if current.Status == StepSuccess {
				break
			}
			if current.Status == StepFailed {
				return r.finishRun(run, RunFailed, "step failed")
			}

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
}

func (r *Runtime) Cancel(runID string) error {
	run, err := r.store.GetRun(runID)
	if err != nil {
		return err
	}
	return r.store.UpdateRunCAS(run.ID, run.Version, func(run *AgentRun) {
		if run.Status == RunRunning {
			run.Status = RunCancelRequested
		}
	})
}

func (r *Runtime) finishRun(run *AgentRun, status RunStatus, output string) error {
	current, err := r.store.GetRun(run.ID)
	if err != nil {
		return err
	}

	err = r.store.UpdateRunCAS(run.ID, current.Version, func(run *AgentRun) {
		run.Status = status
		run.Output = output
		run.UpdatedAt = time.Now()
	})
	if err == nil {
		r.store.AddEvent(Event{
			ID: newID("event"), RunID: run.ID,
			Type: "RUN_FINISHED", Timestamp: time.Now(),
			Message: string(status),
		})
	}
	return err
}

func (r *Runtime) newRunID() string {
	return fmt.Sprintf("run-%d", r.runCounter.Add(1))
}

func (r *Runtime) newStepID() string {
	return fmt.Sprintf("step-%d", r.stepCounter.Add(1))
}

func newID(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}
