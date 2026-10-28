package worker

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/model"
	"agent-runtime/internal/policy"
	"agent-runtime/internal/retry"
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"
)

func TestHandle_AbortsWhenLeaseLost(t *testing.T) {
	for _, mode := range []string{"cas conflict", "database unavailable", "blocked renewal", "late success"} {
		t.Run(mode, func(t *testing.T) {
			var renewals atomic.Int32
			s := &fakeWorkerStore{renewLeaseFn: func(ctx context.Context, tenant, node, owner string, version int64, ttl time.Duration) (bool, error) {
				renewals.Add(1)
				if owner != "worker-1" || version != 2 {
					t.Errorf("renew used owner=%s version=%d", owner, version)
				}
				switch mode {
				case "database unavailable":
					return false, errors.New("database unavailable")
				case "blocked renewal":
					<-ctx.Done()
					return false, ctx.Err()
				default:
					return false, nil
				}
			}}
			w := &Worker{Store: s, ID: "worker-1", Retry: retry.Default(), leaseTTL: 100 * time.Millisecond, heartbeatInterval: 10 * time.Millisecond, pollInterval: time.Hour,
				Exec: stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) {
					<-ctx.Done()
					if mode == "late success" {
						return "stale result", nil
					}
					return "", ctx.Err()
				}),
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := w.Handle(ctx, model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"})
			if !errors.Is(err, errLeaseLost) {
				t.Fatalf("Handle=%v, want lease lost", err)
			}
			if renewals.Load() == 0 {
				t.Fatal("heartbeat never ran")
			}
			if s.failNodeCalls+s.completeNodeCalls+s.cancelNodeCalls+s.retryNodeCalls+s.dlqCalls != 0 {
				t.Fatal("lost owner attempted to persist a result or retry")
			}
		})
	}
}

func TestHandle_ExecTimeoutScopedToExecution(t *testing.T) {
	for _, returnsSuccess := range []bool{false, true} {
		s := &fakeWorkerStore{retryNodeFn: func(ctx context.Context, tenant, node, owner string, version int64, readyAt time.Time) (bool, error) {
			if ctx.Err() != nil {
				t.Errorf("persistence context cancelled: %v", ctx.Err())
			}
			if version != 2 || owner != "worker-1" {
				t.Errorf("retry used version=%d owner=%s", version, owner)
			}
			return true, nil
		}}
		w := &Worker{Store: s, ID: "worker-1", Retry: retry.Default(), ExecTimeout: 20 * time.Millisecond, heartbeatInterval: time.Second,
			Exec: stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) {
				<-ctx.Done()
				if returnsSuccess {
					return "too late", nil
				}
				return "", ctx.Err()
			}),
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := w.Handle(ctx, model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"})
		cancel()
		if err != nil {
			t.Fatal(err)
		}
		if s.retryNodeCalls != 1 || s.completeNodeCalls != 0 || s.cancelNodeCalls != 0 {
			t.Fatalf("retry=%d complete=%d cancel=%d", s.retryNodeCalls, s.completeNodeCalls, s.cancelNodeCalls)
		}
	}
}

func TestHandle_PinsClaimVersionAndExecutionIdentity(t *testing.T) {
	var reads int
	s := &fakeWorkerStore{
		getNodeFn: func(context.Context, string, string) (*model.Node, error) {
			reads++
			if reads > 1 {
				return &model.Node{Version: 99, LeaseOwner: "new-owner"}, nil
			}
			return &model.Node{ID: "node-1", RunID: "run-1", TenantID: "tenant-A", Status: model.NodeReady, Type: model.NodeLLM, Version: 7}, nil
		},
		getRunFn: func(context.Context, string, string) (*model.Run, error) {
			return &model.Run{Status: model.RunRunning, UserID: "user-1", AuthMethod: "jwt-hs256", ThreadID: "thread-1"}, nil
		},
		completeNodeFn: func(ctx context.Context, n *model.Node, _ string, _ model.OutboxMessage) (bool, error) {
			if n.Version != 8 || n.LeaseOwner != "worker-1" || n.Status != model.NodeRunning {
				t.Errorf("wrong execution token: %+v", n)
			}
			return true, nil
		},
	}
	w := &Worker{Store: s, ID: "worker-1", Exec: stubExecutor(func(ctx context.Context, n *model.Node) (string, error) {
		ec, ok := contracts.ExecutionContextFrom(ctx)
		if !ok || ec.UserID != "user-1" || ec.AuthMethod != "jwt-hs256" || ec.ThreadID != "thread-1" || ec.TenantID != "tenant-A" || ec.RunID != "run-1" || ec.NodeID != "node-1" {
			t.Errorf("execution identity missing: %+v", ec)
		}
		if n.Version != 8 || n.LeaseOwner != "worker-1" {
			t.Errorf("executor received another claim: %+v", n)
		}
		return "done", nil
	})}
	if err := w.Handle(context.Background(), model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); err != nil {
		t.Fatal(err)
	}
	if reads != 1 {
		t.Fatalf("GetNode called %d times, wanted claim snapshot only", reads)
	}
}

func TestHandle_StaleResultDoesNotEmitTerminalEventOrDLQ(t *testing.T) {
	for _, failure := range []bool{false, true} {
		sink := &recordingSink{}
		s := &fakeWorkerStore{
			completeNodeFn:       func(context.Context, *model.Node, string, model.OutboxMessage) (bool, error) { return false, nil },
			failNodeWithOutboxFn: func(context.Context, *model.Node, model.OutboxMessage) (bool, error) { return false, nil },
		}
		w := &Worker{Store: s, ID: "w", RuntimeEvents: sink, Exec: stubExecutor(func(context.Context, *model.Node) (string, error) {
			if failure {
				return "", errors.New("tool failed")
			}
			return "stale output", nil
		})}
		if err := w.Handle(context.Background(), model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); !errors.Is(err, errLeaseLost) {
			t.Fatalf("Handle=%v", err)
		}
		if len(sink.events) != 1 || sink.events[0].Type != contracts.EventNodeStarted {
			t.Fatalf("stale result emitted events: %+v", sink.events)
		}
		if s.dlqCalls != 0 {
			t.Fatal("stale result wrote DLQ")
		}
	}
}

func TestHandle_StopsHeartbeatAndInflightQueryBeforeReturning(t *testing.T) {
	renewStarted := make(chan struct{})
	renewStopped := make(chan struct{})
	s := &fakeWorkerStore{renewLeaseFn: func(ctx context.Context, _ string, _ string, _ string, _ int64, _ time.Duration) (bool, error) {
		close(renewStarted)
		<-ctx.Done()
		close(renewStopped)
		return false, ctx.Err()
	}}
	w := &Worker{Store: s, ID: "w", heartbeatInterval: 20 * time.Millisecond, pollInterval: time.Hour,
		Exec: stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) {
			select {
			case <-renewStarted:
				return "done", nil
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
	select {
	case <-renewStopped:
	default:
		t.Fatal("Handle returned with heartbeat query still running")
	}
}

func TestHandle_ClaimLatencyConsumesLeaseBudget(t *testing.T) {
	s := &fakeWorkerStore{claimNodeFn: func(ctx context.Context, _ string, _ string, _ int64, _ string, _ time.Duration) (bool, error) {
		timer := time.NewTimer(40 * time.Millisecond)
		defer timer.Stop()
		select {
		case <-timer.C:
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}}
	w := &Worker{Store: s, ID: "w", leaseTTL: 20 * time.Millisecond, Exec: stubExecutor(func(context.Context, *model.Node) (string, error) {
		t.Error("executor ran after initial lease budget expired")
		return "", nil
	})}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := w.Handle(ctx, model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); !errors.Is(err, errLeaseLost) {
		t.Fatalf("Handle=%v", err)
	}
}

func TestHandle_SuccessfulRenewalsExtendExecution(t *testing.T) {
	var renewals atomic.Int32
	ready := make(chan struct{})
	s := &fakeWorkerStore{renewLeaseFn: func(context.Context, string, string, string, int64, time.Duration) (bool, error) {
		if renewals.Add(1) == 12 {
			close(ready)
		}
		return true, nil
	}}
	w := &Worker{Store: s, ID: "w", leaseTTL: 100 * time.Millisecond, heartbeatInterval: 15 * time.Millisecond, pollInterval: time.Hour,
		Exec: stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) {
			select {
			case <-ready:
				return "done", nil
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
	if s.completeNodeCalls != 1 {
		t.Fatal("renewed execution did not complete")
	}
}

func TestHandle_DuplicateDeliveryDoesNotUseAnotherWorkersLease(t *testing.T) {
	for _, status := range []model.NodeStatus{model.NodeRunning, model.NodeWaitingHuman, model.NodeSuccess, model.NodeFailed, model.NodeCancelled} {
		t.Run(string(status), func(t *testing.T) {
			s := &fakeWorkerStore{getNodeFn: func(context.Context, string, string) (*model.Node, error) {
				return &model.Node{ID: "node-1", RunID: "run-1", TenantID: "tenant-A", Type: model.NodeTool, Status: status, Version: 3, LeaseOwner: "other-worker"}, nil
			}}
			w := &Worker{Store: s, ID: "w", Policy: policy.Func(func(context.Context, policy.Request) (policy.DecisionResult, error) {
				t.Error("duplicate delivery reached the pre-claim policy gate")
				return policy.DecisionResult{Decision: policy.Deny}, nil
			})}
			if err := w.Handle(context.Background(), model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); err != nil {
				t.Fatal(err)
			}
			if s.failNodeCalls != 0 {
				t.Fatal("duplicate delivery borrowed another owner's execution right")
			}
		})
	}
}

func TestHandle_AwaitingApprovalDoesNotWriteOrRetry(t *testing.T) {
	s := &fakeWorkerStore{}
	w := &Worker{Store: s, ID: "w", Retry: retry.Default(), Exec: stubExecutor(func(context.Context, *model.Node) (string, error) {
		return "", middleware.ErrAwaitingApproval
	})}
	if err := w.Handle(context.Background(), model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); err != nil {
		t.Fatal(err)
	}
	if s.failNodeCalls+s.completeNodeCalls+s.cancelNodeCalls+s.retryNodeCalls+s.dlqCalls != 0 {
		t.Fatal("worker changed the suspended node")
	}
}

func TestHandle_TokenBudgetFailureIsFenced(t *testing.T) {
	for _, accepted := range []bool{false, true} {
		s := &fakeWorkerStore{
			getRunFn: func(context.Context, string, string) (*model.Run, error) {
				return &model.Run{Status: model.RunRunning, MaxTokens: 10}, nil
			},
			runTokenUsageFn: func(context.Context, string, string) (int, error) { return 10, nil },
			failNodeWithOutboxFn: func(ctx context.Context, n *model.Node, _ model.OutboxMessage) (bool, error) {
				if ctx.Err() != nil || n.LeaseOwner != "w" || n.Version != 2 {
					t.Error("budget failure lost its persistence context or claim token")
				}
				return accepted, nil
			},
		}
		w := &Worker{Store: s, ID: "w", Retry: retry.Default(), Exec: stubExecutor(func(context.Context, *model.Node) (string, error) {
			t.Error("exceeded budget still invoked executor")
			return "", nil
		})}
		err := w.Handle(context.Background(), model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"})
		if accepted && err != nil || !accepted && !errors.Is(err, errLeaseLost) {
			t.Fatalf("accepted=%v err=%v", accepted, err)
		}
		if s.failNodeCalls != 1 || s.retryNodeCalls != 0 || s.completeNodeCalls != 0 || (s.dlqCalls == 1) != accepted {
			t.Fatalf("fail=%d retry=%d complete=%d dlq=%d", s.failNodeCalls, s.retryNodeCalls, s.completeNodeCalls, s.dlqCalls)
		}
	}
}

func TestHandle_ParentCancellationDoesNotRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := &fakeWorkerStore{}
	w := &Worker{Store: s, ID: "w", Retry: retry.Default(), Exec: stubExecutor(func(ctx context.Context, _ *model.Node) (string, error) {
		cancel()
		return "", ctx.Err()
	})}
	if err := w.Handle(ctx, model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Handle=%v", err)
	}
	if s.failNodeCalls+s.completeNodeCalls+s.cancelNodeCalls+s.retryNodeCalls+s.dlqCalls != 0 {
		t.Fatal("shutdown was treated as a node failure or user cancellation")
	}
}
