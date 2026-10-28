package queue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestCancelBus_CancelledDoesNotConsumeOrRefresh(t *testing.T) {
	at := time.Now().Add(-cancelSignalTTL / 2)
	bus := &CancelBus{runs: map[string]time.Time{"run": at}}
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				if !bus.Cancelled("run") {
					t.Error("concurrent readers must all observe the same signal")
					return
				}
			}
		}()
	}
	wg.Wait()
	if bus.Cancelled("unknown") {
		t.Error("unknown run should not be cancelled")
	}
	if got := bus.runs["run"]; !got.Equal(at) {
		t.Errorf("queries changed timestamp from %v to %v", at, got)
	}
}

func TestCancelBus_TTL(t *testing.T) {
	now := time.Now()
	bus := &CancelBus{runs: map[string]time.Time{
		"fresh":   now,
		"expired": now.Add(-cancelSignalTTL - time.Second),
		"at-ttl":  now.Add(-cancelSignalTTL),
	}}
	for _, runID := range []string{"expired", "at-ttl"} {
		if bus.Cancelled(runID) {
			t.Errorf("%s must not be observed after TTL", runID)
		}
	}
	if !bus.Cancelled("fresh") {
		t.Error("fresh signal expired too soon")
	}
	bus.record("expired")
	if !bus.Cancelled("expired") {
		t.Error("a new broadcast must refresh an expired signal")
	}
}

func TestCancelBus_RecordCleansUnqueriedExpiredSignals(t *testing.T) {
	bus := &CancelBus{runs: make(map[string]time.Time)}
	expired := time.Now().Add(-2 * cancelSignalTTL)
	for i := 0; i < 1000; i++ {
		bus.runs[fmt.Sprintf("unqueried-%d", i)] = expired
	}
	fresh := time.Now().Add(-cancelSignalTTL / 2)
	bus.runs["fresh"] = fresh

	// No Cancelled call: receiving the next signal alone must prune old history.
	bus.record("new")
	if len(bus.runs) != 2 {
		t.Fatalf("record retained %d signals, want only fresh and new", len(bus.runs))
	}
	if !bus.runs["fresh"].Equal(fresh) {
		t.Error("record removed or refreshed another run's unexpired signal")
	}
	if !bus.Cancelled("new") || !bus.Cancelled("fresh") {
		t.Error("record must preserve all unexpired signals")
	}
}

func TestCancelBus_ConcurrentRecordAndQuery(t *testing.T) {
	bus := &CancelBus{runs: make(map[string]time.Time)}
	bus.record("run")
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				bus.record("run")
				if !bus.Cancelled("run") {
					t.Error("signal disappeared during concurrent recording")
					return
				}
			}
		}()
	}
	wg.Wait()
}

func TestCancelBus_ClosedClient(t *testing.T) {
	q := New("localhost:1", "unused", "unused")
	if err := q.Client.Close(); err != nil {
		t.Fatal(err)
	}
	bus := q.NewCancelBus()
	if err := bus.Start(context.Background()); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("Start on closed client = %v, want redis.ErrClosed", err)
	}
	if err := q.PublishCancel(context.Background(), "run"); !errors.Is(err, redis.ErrClosed) {
		t.Errorf("PublishCancel on closed client = %v, want redis.ErrClosed", err)
	}
}
