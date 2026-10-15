package worker

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/retry"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// —— fakeWorkerStore：实现 WorkerStore 接口，可分别注入各方法行为 ——

type fakeWorkerStore struct {
	getRunFn             func(ctx context.Context, tenant, id string) (*model.Run, error)
	getNodeFn            func(ctx context.Context, tenant, id string) (*model.Node, error)
	claimNodeFn          func(ctx context.Context, tenant, id string, version int64, owner string, lease time.Duration) (bool, error)
	renewLeaseFn         func(ctx context.Context, tenant, id, owner string, version int64, lease time.Duration) (bool, error)
	runTokenUsageFn      func(ctx context.Context, tenant, runID string) (int, error)
	completeNodeFn       func(ctx context.Context, node *model.Node, output string, e model.OutboxMessage) (bool, error)
	failNodeWithOutboxFn func(ctx context.Context, node *model.Node, e model.OutboxMessage) (bool, error)
	retryNodeFn          func(ctx context.Context, tenant, id string, version int64, readyAt time.Time) (bool, error)
	enqueueDLQFn         func(ctx context.Context, tenant, runID, nodeID, reason string, attempt int, payload string) error
	cancelNodeFn         func(ctx context.Context, tenant, id string, version int64) (bool, error)

	// 调用计数，供断言使用
	failNodeCalls     int32
	completeNodeCalls int32
	retryNodeCalls    int32
	dlqCalls          int32
	getRunCalls       int32
}

func (f *fakeWorkerStore) GetRun(ctx context.Context, tenant, id string) (*model.Run, error) {
	atomic.AddInt32(&f.getRunCalls, 1)
	if f.getRunFn != nil {
		return f.getRunFn(ctx, tenant, id)
	}
	return &model.Run{ID: id, TenantID: tenant, Status: model.RunRunning, Version: 1}, nil
}
func (f *fakeWorkerStore) GetNode(ctx context.Context, tenant, id string) (*model.Node, error) {
	if f.getNodeFn != nil {
		return f.getNodeFn(ctx, tenant, id)
	}
	return &model.Node{ID: id, TenantID: tenant, RunID: "run-1", Status: model.NodeReady, Version: 1, Type: model.NodeLLM, Input: "hello"}, nil
}
func (f *fakeWorkerStore) ClaimNode(ctx context.Context, tenant, id string, version int64, owner string, lease time.Duration) (bool, error) {
	if f.claimNodeFn != nil {
		return f.claimNodeFn(ctx, tenant, id, version, owner, lease)
	}
	return true, nil
}
func (f *fakeWorkerStore) RenewLease(ctx context.Context, tenant, id, owner string, version int64, lease time.Duration) (bool, error) {
	if f.renewLeaseFn != nil {
		return f.renewLeaseFn(ctx, tenant, id, owner, version, lease)
	}
	return true, nil
}
func (f *fakeWorkerStore) RunTokenUsage(ctx context.Context, tenant, runID string) (int, error) {
	if f.runTokenUsageFn != nil {
		return f.runTokenUsageFn(ctx, tenant, runID)
	}
	return 0, nil
}
func (f *fakeWorkerStore) CompleteNodeWithOutbox(ctx context.Context, node *model.Node, output string, e model.OutboxMessage) (bool, error) {
	atomic.AddInt32(&f.completeNodeCalls, 1)
	if f.completeNodeFn != nil {
		return f.completeNodeFn(ctx, node, output, e)
	}
	return true, nil
}
func (f *fakeWorkerStore) FailNodeWithOutbox(ctx context.Context, node *model.Node, e model.OutboxMessage) (bool, error) {
	atomic.AddInt32(&f.failNodeCalls, 1)
	if f.failNodeWithOutboxFn != nil {
		return f.failNodeWithOutboxFn(ctx, node, e)
	}
	return true, nil
}
func (f *fakeWorkerStore) RetryNode(ctx context.Context, tenant, id string, version int64, readyAt time.Time) (bool, error) {
	atomic.AddInt32(&f.retryNodeCalls, 1)
	if f.retryNodeFn != nil {
		return f.retryNodeFn(ctx, tenant, id, version, readyAt)
	}
	return true, nil
}
func (f *fakeWorkerStore) EnqueueDLQ(ctx context.Context, tenant, runID, nodeID, reason string, attempt int, payload string) error {
	atomic.AddInt32(&f.dlqCalls, 1)
	if f.enqueueDLQFn != nil {
		return f.enqueueDLQFn(ctx, tenant, runID, nodeID, reason, attempt, payload)
	}
	return nil
}
func (f *fakeWorkerStore) CancelNode(ctx context.Context, tenant, id string, version int64) (bool, error) {
	if f.cancelNodeFn != nil {
		return f.cancelNodeFn(ctx, tenant, id, version)
	}
	return true, nil
}

// stubExecutor 适配器：把 func(ctx, node) → (string, error) 转成 executor.Executor 接口。
// executor.Executor 签名就是 (ctx, *model.Node) → (string, error)，所以直接同形适配。
type stubExecutor func(ctx context.Context, n *model.Node) (string, error)

func (s stubExecutor) Execute(ctx context.Context, n *model.Node) (string, error) { return s(ctx, n) }

// —— 测试 ——

// TestWorker_Handle_RunCancelledMidExecution 验证 Path A：
// RUNNING 节点执行中，Run 被用户取消 → heartbeat 感知 CANCEL_REQUESTED →
// 触发 runCancel() → Executor 收到 ctx.Canceled → 走 handleRunCancelled 路径。
// 断言：FailNodeWithOutbox 被调用（handleRunCancelled），不进 retry 不进 DLQ。
func TestWorker_Handle_RunCancelledMidExecution(t *testing.T) {
	// GetRun 返回序列：前两次 RUNNING（认领检查 + 第一次 heartbeat），之后 CANCEL_REQUESTED。
	var getRunSeq int32 = 0
	getRun := func(ctx context.Context, tenant, id string) (*model.Run, error) {
		n := atomic.AddInt32(&getRunSeq, 1)
		if n <= 2 {
			return &model.Run{ID: id, TenantID: tenant, Status: model.RunRunning, Version: 1}, nil
		}
		return &model.Run{ID: id, TenantID: tenant, Status: model.RunCancelRequested, Version: 2}, nil
	}

	// Executor 阻塞在 ctx.Done()，直到 runCancel() 触发 ctx.Canceled
	exec := stubExecutor(func(ctx context.Context, n *model.Node) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})

	store := &fakeWorkerStore{getRunFn: getRun}
	w := &Worker{
		Store:             store,
		Exec:              exec,
		Retry:             retry.Default(),
		ID:                "w-test",
		heartbeatInterval: 10 * time.Millisecond, // 测试加速：10ms 就感知到取消
	}

	ctx := context.Background()
	task := model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}

	done := make(chan error, 1)
	go func() {
		done <- w.Handle(ctx, task)
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Handle returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not finish within timeout — runCancel was not propagated")
	}

	// 关键断言
	if atomic.LoadInt32(&store.failNodeCalls) != 1 {
		t.Errorf("FailNodeWithOutbox calls = %d, want 1 (handleRunCancelled)", atomic.LoadInt32(&store.failNodeCalls))
	}
	if atomic.LoadInt32(&store.completeNodeCalls) != 0 {
		t.Errorf("CompleteNodeWithOutbox calls = %d, want 0 (cancelled node must not be marked SUCCESS)", atomic.LoadInt32(&store.completeNodeCalls))
	}
	if atomic.LoadInt32(&store.retryNodeCalls) != 0 {
		t.Errorf("RetryNode calls = %d, want 0 (cancel must not trigger retry)", atomic.LoadInt32(&store.retryNodeCalls))
	}
	if atomic.LoadInt32(&store.dlqCalls) != 0 {
		t.Errorf("EnqueueDLQ calls = %d, want 0 (cancel must not enqueue DLQ)", atomic.LoadInt32(&store.dlqCalls))
	}
	if atomic.LoadInt32(&store.getRunCalls) < 2 {
		t.Errorf("GetRun calls = %d, want >= 2 (claim check + heartbeat poll)", atomic.LoadInt32(&store.getRunCalls))
	}
}

// fakeCancelSignals 是 CancelSignals 的内存实现（集成真实 CancelBus 的单测替代）。
type fakeCancelSignals struct {
	mu   sync.Mutex
	runs map[string]struct{}
}

func (f *fakeCancelSignals) Cancelled(id string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.runs[id]
	return ok
}

func (f *fakeCancelSignals) Clear(id string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.runs, id)
}

// TestWorker_Handle_CancelBusSignalStopsExecution 验证取消感知的 pub/sub 快路径：
// CancelBus 命中 + DB 确认 CANCEL_REQUESTED → runCancel。
// 关键断言 getRunCalls == 2：认领前置检查 + 信号确认各一次——信号路径不做
// 每 tick 的 GetRun 轮询（pollInterval 远大于测试时长，兜底轮询不该触发）。
func TestWorker_Handle_CancelBusSignalStopsExecution(t *testing.T) {
	// GetRun 序列：第 1 次 RUNNING（认领前置检查，让节点进入执行），
	// 第 2 次 CANCEL_REQUESTED（信号命中后的 DB 确认）。
	var getRunSeq int32 = 0
	getRun := func(ctx context.Context, tenant, id string) (*model.Run, error) {
		if atomic.AddInt32(&getRunSeq, 1) == 1 {
			return &model.Run{ID: id, TenantID: tenant, Status: model.RunRunning, Version: 1}, nil
		}
		return &model.Run{ID: id, TenantID: tenant, Status: model.RunCancelRequested, Version: 2}, nil
	}
	exec := stubExecutor(func(ctx context.Context, n *model.Node) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	})
	store := &fakeWorkerStore{getRunFn: getRun}
	bus := &fakeCancelSignals{runs: map[string]struct{}{"run-1": {}}}
	w := &Worker{
		Store:             store,
		Exec:              exec,
		Retry:             retry.Default(),
		ID:                "w-bus",
		heartbeatInterval: 10 * time.Millisecond,
		pollInterval:      10 * time.Second, // 兜底轮询在测试窗口内不应触发
		CancelBus:         bus,
	}

	done := make(chan error, 1)
	go func() {
		done <- w.Handle(context.Background(), model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"})
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Handle returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not finish — cancel signal was not propagated")
	}

	if atomic.LoadInt32(&store.failNodeCalls) != 1 {
		t.Errorf("FailNodeWithOutbox calls = %d, want 1 (handleRunCancelled)", atomic.LoadInt32(&store.failNodeCalls))
	}
	if got := atomic.LoadInt32(&store.getRunCalls); got != 2 {
		t.Errorf("GetRun calls = %d, want exactly 2 (claim precheck + signal confirm; no per-tick polling)", got)
	}
	bus.mu.Lock()
	_, stillThere := bus.runs["run-1"]
	bus.mu.Unlock()
	if stillThere {
		t.Error("signal should be cleared after consumption")
	}
}

// TestWorker_Handle_RunCancelled_PathB 验证 Path B 安全网：
// Exec.Execute 返回成功 (output, nil)，但 ctx 在最后一瞬间已被 runCancel 触发。
// 期望：走 handleRunCancelled 而不是 CompleteNodeWithOutbox。
func TestWorker_Handle_RunCancelled_PathB(t *testing.T) {
	// GetRun 序列：第一次 RUNNING（让节点被 claim + heartbeat 启动），
	// 之后 CANCEL_REQUESTED（heartbeat 触发 runCancel）。
	var getRunSeq int32 = 0
	getRun := func(ctx context.Context, tenant, id string) (*model.Run, error) {
		n := atomic.AddInt32(&getRunSeq, 1)
		if n <= 1 {
			return &model.Run{ID: id, TenantID: tenant, Status: model.RunRunning, Version: 1}, nil
		}
		return &model.Run{ID: id, TenantID: tenant, Status: model.RunCancelRequested, Version: 2}, nil
	}

	// Executor 等 ctx cancel 后返回成功（不是 ctx.Canceled error）—— 触发 Path B。
	exec := stubExecutor(func(ctx context.Context, n *model.Node) (string, error) {
		<-ctx.Done()
		return "partial output", nil // 成功但 ctx 已 cancel → Path B 安全网
	})

	store := &fakeWorkerStore{getRunFn: getRun}
	w := &Worker{
		Store:             store,
		Exec:              exec,
		Retry:             retry.Default(),
		ID:                "w-test",
		heartbeatInterval: 1 * time.Millisecond, // 极短 heartbeat，一启动就 cancel
	}

	ctx := context.Background()
	task := model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}

	done := make(chan error, 1)
	go func() { done <- w.Handle(ctx, task) }()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Handle returned error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Handle did not finish")
	}

	if atomic.LoadInt32(&store.failNodeCalls) != 1 {
		t.Errorf("FailNodeWithOutbox calls = %d, want 1 (Path B safety net)", atomic.LoadInt32(&store.failNodeCalls))
	}
	if atomic.LoadInt32(&store.completeNodeCalls) != 0 {
		t.Errorf("CompleteNodeWithOutbox calls = %d, want 0 (Path B: cancelled ctx must not SUCCESS)", atomic.LoadInt32(&store.completeNodeCalls))
	}
}
