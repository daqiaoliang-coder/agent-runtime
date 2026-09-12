package main

import (
	"context"
	"errors"
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

	if err := r.updateRunCAS(runID, nil, func(run *AgentRun) {
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

		// 先检查取消：用户意图优先于超步数的失败判定。
		if run.Status == RunCancelRequested {
			return r.finishRun(run, RunCancelled, "user cancelled")
		}

		if run.Steps >= run.MaxSteps {
			return r.finishRun(run, RunFailed, "max steps exceeded")
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

		// 派发步骤前带状态检查地重试 CAS：如果并发取消请求已经赢了版本竞争，
		// 就不能再派发，把刚创建的步骤标记为取消，回到循环顶部走取消收尾。
		err = r.updateRunCAS(runID, func(run *AgentRun) bool {
			return run.Status == RunRunning
		}, func(run *AgentRun) {
			run.CurrentStep = step.ID
			run.Steps++
			run.UpdatedAt = time.Now()
		})
		if err == errRunStateAborted {
			r.cancelOrphanStep(step)
			continue
		}
		if err != nil {
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
// 带状态检查地重试：主循环并发更新会造成版本冲突，不能一次冲突就丢掉用户的取消请求；
// 状态已不是 Running（比如已进入终态）则无需标记，返回 errRunStateAborted 由调用方处理。
func (r *Runtime) Cancel(runID string) error {
	return r.updateRunCAS(runID, func(run *AgentRun) bool {
		return run.Status == RunRunning
	}, func(run *AgentRun) {
		run.Status = RunCancelRequested
	})
}

// errRunStateAborted 表示 CAS 重试期间 Run 状态已偏离（通常是并发取消请求先到），本次更新应当中止。
var errRunStateAborted = errors.New("run state changed concurrently, update aborted")

// maxCASRetries 限制乐观锁冲突的重试次数，超过则报错，避免活锁。
const maxCASRetries = 16

// updateRunCAS 带重试的乐观锁更新：版本冲突时重新读取最新状态再试；
// check 非空且返回 false 时，说明状态已不再允许本次更新，返回 errRunStateAborted。
func (r *Runtime) updateRunCAS(runID string, check func(*AgentRun) bool, update func(*AgentRun)) error {
	for i := 0; i < maxCASRetries; i++ {
		run, err := r.store.GetRun(runID)
		if err != nil {
			return err
		}
		if check != nil && !check(run) {
			return errRunStateAborted
		}
		if err := r.store.UpdateRunCAS(run.ID, run.Version, update); err == nil {
			return nil
		}
	}
	return fmt.Errorf("update run %s: too many CAS conflicts", runID)
}

// cancelOrphanStep 把已创建但尚未派发的步骤标记为取消，避免留下悬空的 Pending 记录。
func (r *Runtime) cancelOrphanStep(step *AgentStep) {
	current, err := r.store.GetStep(step.ID)
	if err != nil || current.Status != StepPending {
		return
	}
	_ = r.store.UpdateStepCAS(step.ID, current.Version, func(s *AgentStep) {
		s.Status = StepCancelled
		s.FinishedAt = time.Now()
	})
}

// finishRun 收尾：只允许从合法源状态转入终态，版本冲突时重试。
// 取消请求优先于 Success/Failed：若准备写终态时发现 CancelRequested，
// 改写为 Cancelled，保证并发取消不会被成功覆盖。
func (r *Runtime) finishRun(run *AgentRun, status RunStatus, output string) error {
	for i := 0; i < maxCASRetries; i++ {
		current, err := r.store.GetRun(run.ID)
		if err != nil {
			return err
		}

		if current.Status == RunCancelRequested && status != RunCancelled {
			status = RunCancelled
			output = "user cancelled"
		}

		if !canFinish(current.Status, status) {
			// 状态已偏离预期转移路径（例如已经写过终态），放弃本次写入。
			return nil
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
			return nil
		}
		// 版本冲突，重读后再评估
	}
	return fmt.Errorf("finish run %s: too many CAS conflicts", run.ID)
}

// canFinish 定义终态的合法转移：
// Success/Failed 只能来自 Running，Cancelled 只能来自 CancelRequested。
func canFinish(from RunStatus, to RunStatus) bool {
	switch to {
	case RunSuccess, RunFailed:
		return from == RunRunning
	case RunCancelled:
		return from == RunCancelRequested
	default:
		return false
	}
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
