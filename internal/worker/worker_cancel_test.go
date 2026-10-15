package worker

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/retry"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeWorkerStore struct {
	getRunFn             func(context.Context, string, string) (*model.Run, error)
	getNodeFn            func(context.Context, string, string) (*model.Node, error)
	claimNodeFn          func(context.Context, string, string, int64, string, time.Duration) (bool, error)
	renewLeaseFn         func(context.Context, string, string, string, int64, time.Duration) (bool, error)
	runTokenUsageFn      func(context.Context, string, string) (int, error)
	completeNodeFn       func(context.Context, *model.Node, string, model.OutboxMessage) (bool, error)
	failNodeWithOutboxFn func(context.Context, *model.Node, model.OutboxMessage) (bool, error)
	cancelWithOutboxFn   func(context.Context, *model.Node, model.OutboxMessage) (bool, error)
	retryNodeFn          func(context.Context, string, string, string, int64, time.Time) (bool, error)
	enqueueDLQFn         func(context.Context, string, string, string, string, int, string) error
	cancelNodeFn         func(context.Context, string, string, int64) (bool, error)
	failNodeCalls        int32
	completeNodeCalls    int32
	cancelNodeCalls      int32
	retryNodeCalls       int32
	dlqCalls             int32
	getRunCalls          int32
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
func (f *fakeWorkerStore) CancelNodeWithOutbox(ctx context.Context, node *model.Node, e model.OutboxMessage) (bool, error) {
	atomic.AddInt32(&f.cancelNodeCalls, 1)
	if f.cancelWithOutboxFn != nil {
		return f.cancelWithOutboxFn(ctx, node, e)
	}
	return true, nil
}
func (f *fakeWorkerStore) RetryNode(ctx context.Context, tenant, id, owner string, version int64, readyAt time.Time) (bool, error) {
	atomic.AddInt32(&f.retryNodeCalls, 1)
	if f.retryNodeFn != nil {
		return f.retryNodeFn(ctx, tenant, id, owner, version, readyAt)
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

type stubExecutor func(context.Context, *model.Node) (string, error)

func (s stubExecutor) Execute(ctx context.Context, n *model.Node) (string, error) { return s(ctx, n) }

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

func TestHandle_Cancellation(t *testing.T) {
	for _, tc := range []struct {
		name           string
		bus            *fakeCancelSignals
		returnsSuccess bool
		staleLease     bool
	}{
		{name: "poll without bus"},
		{name: "broadcast", bus: &fakeCancelSignals{runs: map[string]struct{}{"run-1": {}}}},
		{name: "missed broadcast", bus: &fakeCancelSignals{runs: map[string]struct{}{}}},
		{name: "executor ignores cancellation", returnsSuccess: true},
		{name: "cancel CAS rejected", staleLease: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int32
			s := &fakeWorkerStore{getRunFn: func(context.Context, string, string) (*model.Run, error) {
				status := model.RunRunning
				if calls.Add(1) > 1 {
					status = model.RunCancelRequested
				}
				return &model.Run{Status: status}, nil
			}}
			s.cancelWithOutboxFn = func(context.Context, *model.Node, model.OutboxMessage) (bool, error) {
				return !tc.staleLease, nil
			}
			w := &Worker{Store: s, ID: "w-cancel", Retry: retry.Default(), heartbeatInterval: 10 * time.Millisecond, pollInterval: 20 * time.Millisecond}
			if tc.bus != nil {
				w.CancelBus = tc.bus
			}
			w.Exec = stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) {
				<-ctx.Done()
				if tc.returnsSuccess {
					return "late output", nil
				}
				return "", ctx.Err()
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := w.Handle(ctx, model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"})
			if tc.staleLease && !errors.Is(err, errLeaseLost) || !tc.staleLease && err != nil {
				t.Fatalf("Handle=%v, staleLease=%v", err, tc.staleLease)
			}
			if s.cancelNodeCalls != 1 || s.failNodeCalls != 0 || s.completeNodeCalls != 0 || s.retryNodeCalls != 0 || s.dlqCalls != 0 {
				t.Fatalf("cancel=%d fail=%d complete=%d retry=%d dlq=%d", s.cancelNodeCalls, s.failNodeCalls, s.completeNodeCalls, s.retryNodeCalls, s.dlqCalls)
			}
			if tc.bus != nil && tc.bus.Cancelled("run-1") && calls.Load() != 2 {
				t.Fatalf("broadcast should require one confirmation, calls=%d", calls.Load())
			}
		})
	}
}

func TestHandle_CancellationDoesNotConsumeOtherNodesSignal(t *testing.T) {
	bus := &fakeCancelSignals{runs: map[string]struct{}{}}
	var cancelled atomic.Bool
	s := &fakeWorkerStore{getRunFn: func(context.Context, string, string) (*model.Run, error) {
		status := model.RunRunning
		if cancelled.Load() {
			status = model.RunCancelRequested
		}
		return &model.Run{Status: status}, nil
	}}
	started := make(chan struct{}, 2)
	w := &Worker{Store: s, ID: "w-shared", CancelBus: bus, heartbeatInterval: 10 * time.Millisecond, pollInterval: time.Hour,
		Exec: stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) {
			started <- struct{}{}
			<-ctx.Done()
			return "", ctx.Err()
		}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 2)
	for _, node := range []string{"node-1", "node-2"} {
		go func() { done <- w.Handle(ctx, model.Task{RunID: "run-1", NodeID: node, TenantID: "tenant-A"}) }()
	}
	for range 2 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("nodes did not start")
		}
	}
	cancelled.Store(true)
	bus.mu.Lock()
	bus.runs["run-1"] = struct{}{}
	bus.mu.Unlock()
	for range 2 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("signal did not reach both nodes")
		}
	}
	if !bus.Cancelled("run-1") || atomic.LoadInt32(&s.cancelNodeCalls) != 2 {
		t.Fatal("cancellation signal was consumed or a node missed it")
	}
}

func TestHandle_CancelConfirmationRetriesAfterDatabaseError(t *testing.T) {
	var calls atomic.Int32
	s := &fakeWorkerStore{getRunFn: func(context.Context, string, string) (*model.Run, error) {
		switch calls.Add(1) {
		case 1:
			return &model.Run{Status: model.RunRunning}, nil
		case 2:
			return nil, errors.New("database unavailable")
		default:
			return &model.Run{Status: model.RunCancelRequested}, nil
		}
	}}
	w := &Worker{Store: s, ID: "w", CancelBus: &fakeCancelSignals{runs: map[string]struct{}{"run-1": {}}}, heartbeatInterval: 10 * time.Millisecond, pollInterval: time.Hour,
		Exec: stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) { <-ctx.Done(); return "", ctx.Err() }),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Handle(ctx, model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); err != nil {
		t.Fatal(err)
	}
	if s.cancelNodeCalls != 1 || calls.Load() != 3 {
		t.Fatalf("cancel=%d confirmations=%d", s.cancelNodeCalls, calls.Load())
	}
}

func TestHandle_CancelSignalRequiresDatabaseConfirmation(t *testing.T) {
	confirmed := make(chan struct{})
	var reads atomic.Int32
	s := &fakeWorkerStore{getRunFn: func(context.Context, string, string) (*model.Run, error) {
		if reads.Add(1) == 2 {
			close(confirmed)
		}
		return &model.Run{Status: model.RunRunning}, nil
	}}
	w := &Worker{Store: s, ID: "w", CancelBus: &fakeCancelSignals{runs: map[string]struct{}{"run-1": {}}}, heartbeatInterval: 10 * time.Millisecond, pollInterval: time.Hour,
		Exec: stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) {
			select {
			case <-confirmed:
				return "still running", nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Handle(ctx, model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); err != nil {
		t.Fatal(err)
	}
	if s.completeNodeCalls != 1 || s.cancelNodeCalls+s.retryNodeCalls+s.failNodeCalls+s.dlqCalls != 0 {
		t.Fatal("an unconfirmed signal cancelled a running node")
	}
}
