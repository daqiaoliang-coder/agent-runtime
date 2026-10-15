//go:build integration

package worker

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/retry"
	agentruntime "agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

// These tests require an explicitly supplied isolated database; no shared-table cleanup helpers are used.
// Redis carries real cancellation broadcasts. Task delivery calls Handle directly, without an MQ broker.
type distributedFixture struct {
	ctx     context.Context
	stores  [2]*store.MySQL
	queues  [2]*queue.RedisQueue
	workers [2]*Worker
	buses   [2]*queue.CancelBus
	run     model.Run
	nodes   []*model.Node
}

type distributedExecutor func(context.Context, *model.Node) (string, error)

func (f distributedExecutor) Execute(ctx context.Context, n *model.Node) (string, error) {
	return f(ctx, n)
}

type distributedExecution struct {
	ctx  context.Context
	node model.Node
}

func newDistributedFixture(t *testing.T, count int) *distributedFixture {
	t.Helper()
	dsn, addr := os.Getenv("DATABASE_DSN"), os.Getenv("TEST_REDIS_ADDR")
	if dsn == "" || addr == "" {
		t.Skip("set DATABASE_DSN to an isolated migrated database and TEST_REDIS_ADDR (e.g. 127.0.0.1:6380)")
	}
	// Prevent optional memory clients from contacting Qdrant during production wiring.
	t.Setenv("MEMORY_ENABLED", "false")
	ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
	t.Cleanup(cancel)
	f := &distributedFixture{ctx: ctx, run: model.Run{
		ID: uuid.NewString(), TenantID: "distributed-" + uuid.NewString(),
		AgentID: "distributed-integration", Status: model.RunRunning, Input: "test", MaxSteps: 10,
	}}
	for i := range f.workers {
		s, err := store.New(ctx, dsn)
		if err != nil {
			t.Fatalf("connect worker %d to MySQL: %v", i, err)
		}
		f.stores[i] = s
		t.Cleanup(s.Close)
		q := queue.New(addr, "distributed-"+f.run.ID, "distributed-test")
		f.queues[i] = q
		t.Cleanup(func() { _ = q.Client.Close() })
		if err := q.Ping(ctx); err != nil {
			t.Fatalf("connect worker %d to Redis: %v", i, err)
		}
		w := NewFromEnv(s, q, nil)
		w.ID = fmt.Sprintf("distributed-%s-%d", f.run.ID, i)
		w.leaseTTL, w.heartbeatInterval = 500*time.Millisecond, 100*time.Millisecond
		w.ExecTimeout = 10 * time.Second
		bus, ok := w.CancelBus.(*queue.CancelBus)
		if !ok || bus == nil {
			t.Fatalf("NewFromEnv worker %d did not wire a real CancelBus: %T", i, w.CancelBus)
		}
		if err := bus.Start(ctx); err != nil {
			t.Fatalf("start worker %d CancelBus: %v", i, err)
		}
		f.workers[i], f.buses[i] = w, bus
	}
	if f.buses[0] == f.buses[1] {
		t.Fatal("workers must have independent cancellation subscriptions")
	}
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 3*time.Second)
		defer stop()
		for _, statement := range []struct {
			query string
			arg   string
		}{
			{"DELETE FROM agent_inbox WHERE tenant_id=?", f.run.TenantID},
			{"DELETE FROM event_outbox WHERE aggregate_id=?", f.run.ID},
			{"DELETE FROM agent_dlq WHERE run_id=?", f.run.ID},
			{"DELETE FROM agent_run WHERE run_id=?", f.run.ID},
		} {
			if _, err := f.stores[0].DB.ExecContext(cleanupCtx, statement.query, statement.arg); err != nil {
				t.Errorf("clean up test-owned rows: %v", err)
			}
		}
	})
	if err := f.stores[0].CreateRun(ctx, &f.run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	plan := model.Plan{}
	for i := 0; i < count; i++ {
		plan.Nodes = append(plan.Nodes, model.PlanNode{
			ID: fmt.Sprintf("%s-n%d", f.run.ID, i), Type: model.NodeLLM, Name: "stub", Input: "test",
		})
	}
	if err := f.stores[0].InsertPlan(ctx, f.run.ID, f.run.TenantID, plan); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	for _, n := range plan.Nodes {
		if err := f.stores[0].MarkReady(ctx, f.run.TenantID, n.ID); err != nil {
			t.Fatalf("MarkReady: %v", err)
		}
		f.nodes = append(f.nodes, f.node(t, n.ID))
	}
	return f
}

func (f *distributedFixture) node(t *testing.T, id string) *model.Node {
	t.Helper()
	n, err := f.stores[0].GetNode(f.ctx, f.run.TenantID, id)
	if err != nil {
		t.Fatalf("GetNode(%s): %v", id, err)
	}
	return n
}

func (f *distributedFixture) task(i int) model.Task {
	n := f.nodes[i]
	return model.Task{TenantID: n.TenantID, RunID: n.RunID, NodeID: n.ID, Attempt: n.Attempt}
}

func distributedAwait[T any](t *testing.T, label string, ch <-chan T) T {
	t.Helper()
	select {
	case value := <-ch:
		return value
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for %s", label)
		var zero T
		return zero
	}
}

func distributedHandle(t *testing.T, ctx context.Context, w *Worker, task model.Task) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	result, finished := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(finished)
		result <- w.Handle(ctx, task)
	}()
	t.Cleanup(func() {
		cancel()
		distributedAwait(t, "worker shutdown", finished)
	})
	return result
}

func (f *distributedFixture) events(t *testing.T) []model.Event {
	t.Helper()
	rows, err := f.stores[0].DB.QueryContext(f.ctx,
		"SELECT event_id,event_type,payload FROM event_outbox WHERE aggregate_id=? ORDER BY event_id", f.run.ID)
	if err != nil {
		t.Fatalf("read outbox: %v", err)
	}
	defer rows.Close()
	var events []model.Event
	for rows.Next() {
		var id, kind, payload string
		if err := rows.Scan(&id, &kind, &payload); err != nil {
			t.Fatalf("scan outbox: %v", err)
		}
		var e model.Event
		if err := json.Unmarshal([]byte(payload), &e); err != nil {
			t.Fatalf("decode outbox JSON: %v", err)
		}
		if e.ID != id || e.Type != kind || e.RunID != f.run.ID || e.TenantID != f.run.TenantID {
			t.Fatalf("outbox envelope mismatch: %+v", e)
		}
		events = append(events, e)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("read outbox rows: %v", err)
	}
	return events
}

func (f *distributedFixture) assertNoDLQ(t *testing.T) {
	t.Helper()
	var count int
	if err := f.stores[0].DB.QueryRowContext(f.ctx,
		"SELECT COUNT(*) FROM agent_dlq WHERE tenant_id=? AND run_id=?", f.run.TenantID, f.run.ID).Scan(&count); err != nil {
		t.Fatalf("count DLQ: %v", err)
	}
	if count != 0 {
		t.Fatalf("unexpected DLQ entries: %d", count)
	}
}

func (f *distributedFixture) assertCancelledAndConsume(t *testing.T) {
	t.Helper()
	for _, before := range f.nodes {
		n := f.node(t, before.ID)
		if n.Status != model.NodeCancelled || n.Attempt != before.Attempt || n.Version != before.Version+2 {
			t.Fatalf("cancel changed attempt or retried node: before=%+v after=%+v", before, n)
		}
		if n.LeaseOwner != "" || n.LeaseUntil != nil || n.FinishedAt.IsZero() || n.Output != "" {
			t.Fatalf("cancelled node retained a lease or result: %+v", n)
		}
	}
	f.assertNoDLQ(t)
	events := f.events(t)
	if len(events) != len(f.nodes) {
		t.Fatalf("want one cancellation outbox per node, got %d", len(events))
	}
	seenNodes := make(map[string]bool)
	for _, e := range events {
		n := f.node(t, e.NodeID)
		if seenNodes[e.NodeID] || e.Type != "AgentStepFailed" || e.Error != errRunCancelled.Error() || e.Attempt != n.Attempt {
			t.Fatalf("unexpected cancellation event: %+v", e)
		}
		seenNodes[e.NodeID] = true
	}
	run, err := f.stores[0].GetRun(f.ctx, f.run.TenantID, f.run.ID)
	if err != nil || run.Status != model.RunCancelRequested {
		t.Fatalf("before consuming, want CANCEL_REQUESTED: run=%+v err=%v", run, err)
	}
	// Test the real consumer logic directly from stored JSON; RocketMQ transport is intentionally not required.
	resumer := &agentruntime.Resumer{Store: f.stores[0], Queue: f.queues[0]}
	for _, e := range events {
		if err := resumer.Handle(f.ctx, e); err != nil {
			t.Fatalf("Resumer.Handle(%s): %v", e.ID, err)
		}
		if seen, err := f.stores[0].InboxSeen(f.ctx, e.TenantID, e.ID); err != nil || !seen {
			t.Fatalf("event was not consumed into inbox: seen=%v err=%v", seen, err)
		}
	}
	run, err = f.stores[0].GetRun(f.ctx, f.run.TenantID, f.run.ID)
	if err != nil || run.Status != model.RunCancelled {
		t.Fatalf("all cancellation events must leave Run CANCELLED, not FAILED: run=%+v err=%v", run, err)
	}
	version := run.Version
	for _, e := range events {
		if err := resumer.Handle(f.ctx, e); err != nil {
			t.Fatalf("duplicate Resumer.Handle: %v", err)
		}
	}
	run, err = f.stores[0].GetRun(f.ctx, f.run.TenantID, f.run.ID)
	if err != nil || run.Status != model.RunCancelled || run.Version != version {
		t.Fatalf("duplicate consumption changed terminal run: run=%+v err=%v", run, err)
	}
}

func TestDistributedWorker_CancelBroadcastAcrossWorkers(t *testing.T) {
	f := newDistributedFixture(t, 2)
	started := make(chan distributedExecution, 2)
	results := make([]<-chan error, 2)
	for i, w := range f.workers {
		// One node could retry; the other would go straight to DLQ if cancellation were treated as failure.
		if i == 1 {
			w.Retry = retry.Policy{}
		}
		w.Exec = distributedExecutor(func(ctx context.Context, n *model.Node) (string, error) {
			started <- distributedExecution{ctx: ctx, node: *n}
			<-ctx.Done()
			return "", ctx.Err()
		})
		results[i] = distributedHandle(t, f.ctx, w, f.task(i))
	}
	executions := []distributedExecution{
		distributedAwait(t, "first parallel executor", started),
		distributedAwait(t, "second parallel executor", started),
	}
	for _, execution := range executions {
		if n := f.node(t, execution.node.ID); n.Status != model.NodeRunning || n.LeaseOwner != execution.node.LeaseOwner {
			t.Fatalf("executor started without owning RUNNING node: %+v", n)
		}
	}
	rt := &agentruntime.Runtime{Store: f.stores[0], Queue: f.queues[0], CancelPub: f.queues[0]}
	if err := rt.Cancel(f.ctx, f.run.TenantID, f.run.ID, "distributed cancellation"); err != nil {
		t.Fatalf("Runtime.Cancel: %v", err)
	}
	for _, execution := range executions {
		distributedAwait(t, "broadcast cancelling executor", execution.ctx.Done())
		if !errors.Is(execution.ctx.Err(), context.Canceled) || !errors.Is(context.Cause(execution.ctx), errRunCancelled) {
			t.Fatalf("executor stopped for wrong reason: %v", context.Cause(execution.ctx))
		}
	}
	for i, result := range results {
		if err := distributedAwait(t, "cancelled Handle", result); err != nil {
			t.Fatalf("worker %d cancellation: %v", i, err)
		}
		if !f.buses[i].Cancelled(f.run.ID) {
			t.Fatalf("worker %d missed the real Redis broadcast", i)
		}
	}
	// The default 30s poll cannot satisfy the bounded waits above: this exercises the broadcast path.
	f.assertCancelledAndConsume(t)
}

func TestDistributedWorker_CancelPollingWithoutBroadcast(t *testing.T) {
	for _, nilBus := range []bool{false, true} {
		t.Run(fmt.Sprintf("nil_bus=%t", nilBus), func(t *testing.T) {
			f := newDistributedFixture(t, 1)
			w := f.workers[0]
			if nilBus {
				w.CancelBus = nil // Zero pollInterval must fall back to each heartbeat.
			} else {
				w.pollInterval = 150 * time.Millisecond
			}
			started := make(chan distributedExecution, 1)
			w.Exec = distributedExecutor(func(ctx context.Context, n *model.Node) (string, error) {
				started <- distributedExecution{ctx: ctx, node: *n}
				<-ctx.Done()
				return "", ctx.Err()
			})
			result := distributedHandle(t, f.ctx, w, f.task(0))
			execution := distributedAwait(t, "polling executor", started)
			// Deliberately omit publication while keeping both real subscriptions running.
			rt := &agentruntime.Runtime{Store: f.stores[0], Queue: f.queues[0]}
			if err := rt.Cancel(f.ctx, f.run.TenantID, f.run.ID, "broadcast unavailable"); err != nil {
				t.Fatalf("Runtime.Cancel without publisher: %v", err)
			}
			distributedAwait(t, "poll cancelling executor", execution.ctx.Done())
			if !errors.Is(context.Cause(execution.ctx), errRunCancelled) {
				t.Fatalf("poll stopped executor for wrong reason: %v", context.Cause(execution.ctx))
			}
			if err := distributedAwait(t, "polled Handle", result); err != nil {
				t.Fatalf("polled cancellation: %v", err)
			}
			for _, bus := range f.buses {
				if bus.Cancelled(f.run.ID) {
					t.Fatal("poll fallback was masked by an unexpected broadcast")
				}
			}
			f.assertCancelledAndConsume(t)
		})
	}
}

func TestDistributedWorker_CompetingClaimsExecuteOnce(t *testing.T) {
	f := newDistributedFixture(t, 1)
	var calls atomic.Int32
	started := make(chan distributedExecution, 2)
	release := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	exec := distributedExecutor(func(ctx context.Context, n *model.Node) (string, error) {
		calls.Add(1)
		started <- distributedExecution{ctx: ctx, node: *n}
		select {
		case <-release:
			return "single winner", nil
		case <-ctx.Done():
			return "", ctx.Err()
		}
	})
	gate := make(chan struct{})
	results := make([]<-chan error, 2)
	for i, w := range f.workers {
		w.Exec = exec
		// Both delivery attempts are released together, against independent MySQL pools.
		results[i] = distributedHandleAfterGate(t, f.ctx, w, f.task(0), gate)
	}
	close(gate)
	winner := distributedAwait(t, "winning executor", started)
	loser := -1
	select {
	case err := <-results[0]:
		loser = 0
		if err != nil {
			t.Fatalf("losing claim: %v", err)
		}
	case err := <-results[1]:
		loser = 1
		if err != nil {
			t.Fatalf("losing claim: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatalf("losing claim did not return while winner held execution; calls=%d", calls.Load())
	}
	if calls.Load() != 1 || winner.node.Version != f.nodes[0].Version+1 || winner.node.LeaseOwner != f.workers[1-loser].ID {
		t.Fatalf("claim did not grant a single fixed execution right: calls=%d node=%+v", calls.Load(), winner.node)
	}
	unblock()
	if err := distributedAwait(t, "winning Handle", results[1-loser]); err != nil {
		t.Fatalf("winning Handle: %v", err)
	}
	for _, w := range f.workers {
		if err := w.Handle(f.ctx, f.task(0)); err != nil {
			t.Fatalf("duplicate delivery: %v", err)
		}
	}
	n := f.node(t, f.nodes[0].ID)
	if calls.Load() != 1 || n.Status != model.NodeSuccess || n.Output != "single winner" || n.Attempt != 0 || n.Version != winner.node.Version+1 {
		t.Fatalf("duplicate delivery changed execution: calls=%d node=%+v", calls.Load(), n)
	}
	if events := f.events(t); len(events) != 1 || events[0].Type != "AgentStepCompleted" || events[0].Output != n.Output {
		t.Fatalf("want exactly one winning outbox: %+v", events)
	}
	f.assertNoDLQ(t)
}

func distributedHandleAfterGate(t *testing.T, ctx context.Context, w *Worker, task model.Task, gate <-chan struct{}) <-chan error {
	t.Helper()
	ctx, cancel := context.WithCancel(ctx)
	result, finished := make(chan error, 1), make(chan struct{})
	go func() {
		defer close(finished)
		select {
		case <-gate:
			result <- w.Handle(ctx, task)
		case <-ctx.Done():
			result <- ctx.Err()
		}
	}()
	t.Cleanup(func() {
		cancel()
		distributedAwait(t, "competing worker shutdown", finished)
	})
	return result
}

func TestDistributedWorker_ExpiredLeaseTakeoverFencesOldResults(t *testing.T) {
	for _, resultKind := range []string{"success", "retryable_error", "terminal_error"} {
		t.Run(resultKind, func(t *testing.T) {
			f := newDistributedFixture(t, 1)
			oldWorker, newWorker := f.workers[0], f.workers[1]
			if resultKind == "terminal_error" {
				oldWorker.Retry = retry.Policy{}
			}
			oldStarted, newStarted := make(chan distributedExecution, 1), make(chan distributedExecution, 1)
			oldRelease, newRelease := make(chan struct{}), make(chan struct{})
			unblockOld := sync.OnceFunc(func() { close(oldRelease) })
			unblockNew := sync.OnceFunc(func() { close(newRelease) })
			defer unblockOld()
			defer unblockNew()
			oldWorker.Exec = distributedExecutor(func(ctx context.Context, n *model.Node) (string, error) {
				oldStarted <- distributedExecution{ctx: ctx, node: *n}
				// Model an executor returning late even after its context was cancelled.
				<-oldRelease
				if resultKind != "success" {
					return "stale output", errors.New("late execution failure")
				}
				return "stale output", nil
			})
			newWorker.Exec = distributedExecutor(func(ctx context.Context, n *model.Node) (string, error) {
				newStarted <- distributedExecution{ctx: ctx, node: *n}
				select {
				case <-newRelease:
					return "new owner output", nil
				case <-ctx.Done():
					return "", ctx.Err()
				}
			})
			oldResult := distributedHandle(t, f.ctx, oldWorker, f.task(0))
			oldExecution := distributedAwait(t, "old executor", oldStarted)
			oldNode := oldExecution.node
			if oldNode.Version != f.nodes[0].Version+1 || oldNode.LeaseOwner != oldWorker.ID {
				t.Fatalf("old worker did not retain its claim token: %+v", oldNode)
			}
			res, err := f.stores[0].DB.ExecContext(f.ctx,
				"UPDATE agent_node SET lease_until=DATE_SUB(NOW(6), INTERVAL 1 SECOND) WHERE node_id=? AND tenant_id=? AND lease_owner=? AND version=? AND status=?",
				oldNode.ID, oldNode.TenantID, oldNode.LeaseOwner, oldNode.Version, model.NodeRunning)
			if err != nil {
				t.Fatalf("force expired lease: %v", err)
			}
			if affected, err := res.RowsAffected(); err != nil || affected != 1 {
				t.Fatalf("force expired lease: affected=%d err=%v", affected, err)
			}
			// Expiry alone must fence writes, even before recovery changes owner or version.
			distributedAssertFenced(t, f, &oldNode)
			tasks, err := f.stores[1].RecoverExpired(f.ctx, 100)
			if err != nil {
				t.Fatalf("RecoverExpired: %v", err)
			}
			var recovered *model.Task
			for i := range tasks {
				if tasks[i].NodeID == oldNode.ID && tasks[i].TenantID == oldNode.TenantID {
					recovered = &tasks[i]
				}
			}
			if recovered == nil || recovered.Attempt != oldNode.Attempt {
				t.Fatalf("recovery did not return the unchanged-attempt task: %+v", tasks)
			}
			newResult := distributedHandle(t, f.ctx, newWorker, *recovered)
			newExecution := distributedAwait(t, "takeover executor", newStarted)
			if newExecution.node.Version != oldNode.Version+2 || newExecution.node.LeaseOwner != newWorker.ID {
				t.Fatalf("takeover execution right: old=%+v new=%+v", oldNode, newExecution.node)
			}
			distributedAwait(t, "lease loss cancelling old executor", oldExecution.ctx.Done())
			if !errors.Is(context.Cause(oldExecution.ctx), errLeaseLost) {
				t.Fatalf("old executor stopped for wrong reason: %v", context.Cause(oldExecution.ctx))
			}
			distributedAssertFenced(t, f, &oldNode)
			wrongOwner := newExecution.node
			wrongOwner.LeaseOwner = oldWorker.ID
			distributedAssertFenced(t, f, &wrongOwner)
			wrongVersion := newExecution.node
			wrongVersion.Version = oldNode.Version
			distributedAssertFenced(t, f, &wrongVersion)
			unblockNew()
			if err := distributedAwait(t, "takeover commit", newResult); err != nil {
				t.Fatalf("takeover Handle: %v", err)
			}
			unblockOld()
			if err := distributedAwait(t, "stale Handle", oldResult); !errors.Is(err, errLeaseLost) {
				t.Fatalf("stale worker must report lease loss, got %v", err)
			}
			n := f.node(t, oldNode.ID)
			if n.Status != model.NodeSuccess || n.Output != "new owner output" || n.Version != newExecution.node.Version+1 || n.Attempt != oldNode.Attempt {
				t.Fatalf("stale result overwrote takeover: %+v", n)
			}
			if events := f.events(t); len(events) != 1 || events[0].Type != "AgentStepCompleted" || events[0].Output != n.Output {
				t.Fatalf("stale result emitted an outbox event: %+v", events)
			}
			f.assertNoDLQ(t)
		})
	}
}

func distributedAssertFenced(t *testing.T, f *distributedFixture, n *model.Node) {
	t.Helper()
	s := f.stores[0]
	e := model.OutboxMessage{ID: uuid.NewString(), EventType: "AgentStepFailed", AggregateID: n.RunID, Payload: "{}"}
	for _, operation := range []struct {
		name string
		call func() (bool, error)
	}{
		{"renew", func() (bool, error) {
			return s.RenewLease(f.ctx, n.TenantID, n.ID, n.LeaseOwner, n.Version, 500*time.Millisecond)
		}},
		{"complete", func() (bool, error) { return s.CompleteNodeWithOutbox(f.ctx, n, "stale output", e) }},
		{"fail", func() (bool, error) { return s.FailNodeWithOutbox(f.ctx, n, e) }},
		{"cancel", func() (bool, error) { return s.CancelNodeWithOutbox(f.ctx, n, e) }},
		{"retry", func() (bool, error) {
			return s.RetryNode(f.ctx, n.TenantID, n.ID, n.LeaseOwner, n.Version, time.Now())
		}},
	} {
		if ok, err := operation.call(); err != nil || ok {
			t.Fatalf("%s accepted invalid execution right: owner=%s version=%d ok=%v err=%v", operation.name, n.LeaseOwner, n.Version, ok, err)
		}
	}
}
