package main

// 并发取消竞态测试。
//
// 覆盖的场景：
//  1. 取消请求与运行主循环在任意时序下竞争（随机时机压力测试）
//  2. 取消落在步骤执行窗口内，必须收敛为 CANCELLED 且不再派发新步骤
//  3. 多个取消请求同时到达，恰好一个生效
//  4. 运行尚未开始时取消是无害空操作
//  5. at-least-once 重投递下工具幂等（配合计数工具）
//  6. 检查点累积已完成步骤且不重复
//
// 建议用 -race 跑：go test -race -count=1 .

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// startRuntime 装配运行时并启动 worker pool，测试结束时自动停掉。
func startRuntime(t *testing.T) *Runtime {
	t.Helper()
	rt := NewRuntime()
	ctx, cancel := context.WithCancel(context.Background())
	rt.scheduler.Start(ctx)
	t.Cleanup(cancel)
	return rt
}

// assertNoActiveSteps 断言指定 Run 没有停留在 PENDING / RUNNING 的步骤，
// 即不存在"创建后没人执行"的孤儿步骤。
func assertNoActiveSteps(t *testing.T, rt *Runtime, runID string) {
	t.Helper()
	rt.store.mu.RLock()
	defer rt.store.mu.RUnlock()
	for id, step := range rt.store.steps {
		if step.RunID != runID {
			continue
		}
		if step.Status == StepPending || step.Status == StepRunning {
			t.Fatalf("run %s 遗留活跃步骤 %s，状态=%s", runID, id, step.Status)
		}
	}
}

// countRunSteps 统计某个 Run 已创建的步骤数。
func countRunSteps(rt *Runtime, runID string) int {
	rt.store.mu.RLock()
	defer rt.store.mu.RUnlock()
	n := 0
	for _, step := range rt.store.steps {
		if step.RunID == runID {
			n++
		}
	}
	return n
}

// TestConcurrentCancelRandomTiming 让取消请求以随机延迟落在运行的任意生命周期位置，
// 反复多轮，验证：
//   - Run() 不会把版本冲突之类的内部错误泄漏给调用方
//   - 终态只能是 SUCCESS 或 CANCELLED（Mock 流程没有失败路径）
//   - 任何时刻都不遗留 PENDING / RUNNING 步骤（孤儿步骤必须被取消）
func TestConcurrentCancelRandomTiming(t *testing.T) {
	const iterations = 30

	for i := 0; i < iterations; i++ {
		rt := startRuntime(t)

		run, err := rt.CreateRun("race-test")
		if err != nil {
			t.Fatalf("iter %d: CreateRun: %v", i, err)
		}

		done := make(chan error, 1)
		go func() {
			done <- rt.Run(context.Background(), run.ID)
		}()

		// Mock 流程总时长约 800ms（300+200+300），0~1000ms 的随机延迟
		// 能覆盖"运行前 / 每个步骤执行中 / 步骤间隙 / 已结束后"所有窗口。
		time.Sleep(time.Duration(rand.Intn(1000)) * time.Millisecond)

		if err := rt.Cancel(run.ID); err != nil && !errors.Is(err, errRunStateAborted) {
			t.Fatalf("iter %d: Cancel 返回非预期错误: %v", i, err)
		}

		if err := <-done; err != nil {
			t.Fatalf("iter %d: Run 返回错误（CAS 冲突泄漏？）: %v", i, err)
		}

		final, err := rt.store.GetRun(run.ID)
		if err != nil {
			t.Fatalf("iter %d: GetRun: %v", i, err)
		}

		switch final.Status {
		case RunSuccess, RunCancelled:
			// 合法终态
		default:
			t.Fatalf("iter %d: 运行结束于非终态 %s", i, final.Status)
		}

		assertNoActiveSteps(t, rt, run.ID)
	}
}

// TestCancelMidRunIsRespected 用确定性时序验证中途取消：
// Mock 流程为 LLM(300ms) -> tool(200ms) -> LLM(300ms)，
// 在 350ms 时取消（落在第一、二步之间或第二步执行中），
// 运行必须以 CANCELLED 收敛，且第三步永远不会被创建。
func TestCancelMidRunIsRespected(t *testing.T) {
	rt := startRuntime(t)

	run, err := rt.CreateRun("mid-cancel")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- rt.Run(context.Background(), run.ID)
	}()

	time.Sleep(350 * time.Millisecond)

	if err := rt.Cancel(run.ID); err != nil {
		t.Fatalf("Cancel: %v", err)
	}

	if err := <-done; err != nil {
		t.Fatalf("Run 返回错误: %v", err)
	}

	final, err := rt.store.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != RunCancelled {
		t.Fatalf("期望 CANCELLED，实际 %s（取消被覆盖？）", final.Status)
	}
	if final.Output != "user cancelled" {
		t.Fatalf("期望输出 %q，实际 %q", "user cancelled", final.Output)
	}

	// 第三步（reasoning）不应被创建；受调度抖动影响，
	// 取消可能落在第二步派发前（孤儿取消）或执行中，步骤数应为 1 或 2。
	if n := countRunSteps(rt, run.ID); n > 2 {
		t.Fatalf("取消后仍创建了 %d 个步骤，期望不超过 2", n)
	}

	assertNoActiveSteps(t, rt, run.ID)
}

// TestConcurrentCancelBurst 让多个取消请求同时到达：
// 恰好一个成功翻转状态，其余必须安全地返回中止，不得报错。
func TestConcurrentCancelBurst(t *testing.T) {
	rt := startRuntime(t)

	run, err := rt.CreateRun("burst")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- rt.Run(context.Background(), run.ID)
	}()

	// 100ms 时运行必然处于 Running（第一步需要 300ms），取消必然能命中。
	time.Sleep(100 * time.Millisecond)

	const n = 10
	var succeeded, aborted int64
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := rt.Cancel(run.ID)
			switch {
			case err == nil:
				atomic.AddInt64(&succeeded, 1)
			case errors.Is(err, errRunStateAborted):
				atomic.AddInt64(&aborted, 1)
			default:
				t.Errorf("Cancel 返回非预期错误: %v", err)
			}
		}()
	}
	wg.Wait()

	if err := <-done; err != nil {
		t.Fatalf("Run 返回错误: %v", err)
	}

	final, err := rt.store.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != RunCancelled {
		t.Fatalf("期望 CANCELLED，实际 %s", final.Status)
	}
	if succeeded != 1 {
		t.Fatalf("期望恰好 1 个取消成功，实际 %d（aborted=%d）", succeeded, aborted)
	}
}

// TestCancelBeforeRunStarts 验证运行还没开始（PENDING）时取消是空操作：
// 取消请求被安全忽略，随后的 Run 正常跑完并成功。
func TestCancelBeforeRunStarts(t *testing.T) {
	rt := startRuntime(t)

	run, err := rt.CreateRun("early-cancel")
	if err != nil {
		t.Fatal(err)
	}

	if err := rt.Cancel(run.ID); !errors.Is(err, errRunStateAborted) {
		t.Fatalf("PENDING 状态下取消应返回 errRunStateAborted，实际 %v", err)
	}

	if err := rt.Run(context.Background(), run.ID); err != nil {
		t.Fatalf("Run: %v", err)
	}

	final, err := rt.store.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if final.Status != RunSuccess {
		t.Fatalf("期望 SUCCESS，实际 %s", final.Status)
	}
}

// countingTool 记录真实执行次数，用于验证幂等。
type countingTool struct {
	name  string
	calls atomic.Int64
}

func (c *countingTool) Name() string { return c.name }

func (c *countingTool) Execute(ctx context.Context, input string) (string, error) {
	c.calls.Add(1)
	return "result:" + input, nil
}

// TestToolIdempotencyOnRedelivery 模拟 at-least-once 的经典事故现场：
// 工具执行成功后、步骤状态落盘前 worker 崩溃，调度器重新投递同一个步骤。
// 重投递不得再次真实调用工具，且结果必须与首次一致。
func TestToolIdempotencyOnRedelivery(t *testing.T) {
	store := NewMemoryStore()
	tool := &countingTool{name: "counted"}
	tools := NewToolRegistry()
	tools.Register(tool)
	executor := NewExecutor(store, &MockLLM{}, tools)

	run := &AgentRun{
		ID: "run-idem", AgentID: "test", Status: RunRunning,
		MaxSteps: 10, CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
	if err := store.CreateRun(run); err != nil {
		t.Fatal(err)
	}

	step := &AgentStep{
		ID: "step-idem", RunID: run.ID, Type: StepTool, Name: "counted",
		Input: "x", Status: StepPending, CreatedAt: time.Now(),
	}
	if err := store.CreateStep(step); err != nil {
		t.Fatal(err)
	}

	// 首次执行
	if err := executor.Execute(context.Background(), step.ID); err != nil {
		t.Fatalf("首次执行: %v", err)
	}
	if got := tool.calls.Load(); got != 1 {
		t.Fatalf("首次执行后工具调用次数=%d，期望 1", got)
	}

	// 模拟重投递：把步骤拨回 Pending（如同崩溃恢复后重新入队）
	current, err := store.GetStep(step.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.UpdateStepCAS(step.ID, current.Version, func(s *AgentStep) {
		s.Status = StepPending
		s.Attempt++
	}); err != nil {
		t.Fatal(err)
	}

	// 重投递执行
	if err := executor.Execute(context.Background(), step.ID); err != nil {
		t.Fatalf("重投递执行: %v", err)
	}
	if got := tool.calls.Load(); got != 1 {
		t.Fatalf("重投递后工具调用次数=%d，期望仍为 1（幂等被破坏）", got)
	}

	current, err = store.GetStep(step.ID)
	if err != nil {
		t.Fatal(err)
	}
	if current.Status != StepSuccess {
		t.Fatalf("重投递后步骤状态=%s，期望 SUCCESS", current.Status)
	}
	if current.Output != "result:x" {
		t.Fatalf("重投递后输出=%q，期望与首次一致", current.Output)
	}
}

// TestCheckpointAccumulatesCompletedSteps 验证检查点随步骤推进累积，
// 断点恢复时能拿到完整的已完成列表，且重放不产生重复记录。
func TestCheckpointAccumulatesCompletedSteps(t *testing.T) {
	rt := startRuntime(t)

	run, err := rt.CreateRun("checkpoint-test")
	if err != nil {
		t.Fatal(err)
	}
	if err := rt.Run(context.Background(), run.ID); err != nil {
		t.Fatalf("Run: %v", err)
	}

	cp, err := rt.store.GetCheckpoint(run.ID)
	if err != nil {
		t.Fatalf("GetCheckpoint: %v", err)
	}

	// Mock 流程共 3 个步骤，全部完成后检查点应累积 3 条记录。
	if len(cp.Completed) != 3 {
		t.Fatalf("检查点 Completed=%v（%d 条），期望累积 3 条", cp.Completed, len(cp.Completed))
	}

	seen := make(map[string]bool, len(cp.Completed))
	for _, id := range cp.Completed {
		if seen[id] {
			t.Fatalf("检查点中步骤 %s 重复出现", id)
		}
		seen[id] = true
	}
}

// TestManyConcurrentRuns 在同一个运行时上并发跑多个 Run，
// 验证共享调度器、原子 ID 生成与存储并发访问的正确性。
func TestManyConcurrentRuns(t *testing.T) {
	rt := startRuntime(t)

	const n = 10
	var wg sync.WaitGroup
	errs := make(chan error, n*2)

	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			run, err := rt.CreateRun(fmt.Sprintf("concurrent-run-%d", i))
			if err != nil {
				errs <- err
				return
			}
			errs <- rt.Run(context.Background(), run.ID)
		}(i)
	}

	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("并发运行出错: %v", err)
		}
	}

	rt.store.mu.RLock()
	defer rt.store.mu.RUnlock()
	for id, run := range rt.store.runs {
		if run.Status != RunSuccess {
			t.Fatalf("run %s 状态=%s，期望全部 SUCCESS", id, run.Status)
		}
	}
}
