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

// NewRuntime 装配一个最小可用的运行时：内存存储 + Mock 规划器 + 4 个 worker。
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

// CreateRun 创建一次运行并写入存储，状态为 Pending，等待 Run 驱动。
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

// Run 是运行主循环：先把状态翻成 Running，然后反复「规划 → 建步骤 → 提交给调度器 → 等步骤完成」。
// 退出条件有三类：达到 MaxSteps、收到取消请求、规划器返回 StepFinish。
// 注意每一步的状态变更都走 CAS，避免和并发的取消请求互相覆盖。
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

		// 这里没有用 channel 回调，而是轮询存储里的步骤状态。
		// 简单粗暴，但和基于 CAS 的存储模型保持一致，也方便后续换持久化实现。
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

// Cancel 只是打个取消标记，真正的状态翻转交给主循环在下一轮检查时处理。
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

// finishRun 收尾：用最新版本做一次 CAS 把状态写定，再补一条 RUN_FINISHED 事件。
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
