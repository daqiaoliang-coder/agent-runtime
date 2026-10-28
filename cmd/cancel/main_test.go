package main

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/store"
	"context"
	"errors"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

type cancelPublisherFunc func(context.Context, string) error

func (f cancelPublisherFunc) PublishCancel(ctx context.Context, runID string) error {
	return f(ctx, runID)
}

// Exercise the same assembly as main, including the real Runtime.Cancel and
// MySQL transaction. A non-nil publisher alone would not catch a missing call.
func TestNewCancelRuntime_CancelPublishesAfterCommit(t *testing.T) {
	dbErr := errors.New("database unavailable")
	publishErr := errors.New("redis unavailable")
	for _, tc := range []struct {
		name      string
		getErr    error
		casRows   int64
		nodesErr  error
		commitErr error
		pubErr    error
		wantErr   bool
		wantPub   int
	}{
		{name: "committed", casRows: 1, wantPub: 1},
		{name: "publish failure is best effort", casRows: 1, pubErr: publishErr, wantPub: 1},
		{name: "read failure does not publish", getErr: dbErr, wantErr: true},
		{name: "CAS conflict does not publish", wantErr: true},
		{name: "node update failure does not publish", casRows: 1, nodesErr: dbErr, wantErr: true},
		{name: "commit failure does not publish", casRows: 1, commitErr: dbErr, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			s := &store.MySQL{DB: db}
			t.Cleanup(s.Close)
			const tenant, runID, reason = "tenant-cancel", "run-cancel", "user cancelled"
			query := mock.ExpectQuery(`SELECT .* FROM agent_run WHERE run_id=\? AND tenant_id=\?`).
				WithArgs(runID, tenant)
			if tc.getErr != nil {
				query.WillReturnError(tc.getErr)
			} else {
				now := time.Now()
				query.WillReturnRows(sqlmock.NewRows([]string{
					"run_id", "tenant_id", "thread_id", "agent_id", "user_id", "auth_method", "status", "version",
					"input", "output", "current_node_id", "max_steps", "steps", "max_rounds", "max_tokens", "created_at", "updated_at",
				}).AddRow(runID, tenant, "", "agent", "", "", model.RunRunning, 7, "input", "", "", 50, 0, 10, 0, now, now))
				mock.ExpectBegin()
				mock.ExpectExec(regexp.QuoteMeta(`UPDATE agent_run SET status=?,output=NULLIF(?,''),version=version+1,updated_at=NOW(6) WHERE run_id=? AND tenant_id=? AND version=? AND status=?`)).
					WithArgs(model.RunCancelRequested, reason, runID, tenant, int64(7), model.RunRunning).
					WillReturnResult(sqlmock.NewResult(0, tc.casRows))
				if tc.casRows == 0 {
					mock.ExpectRollback()
				} else {
					nodes := mock.ExpectExec(regexp.QuoteMeta(`UPDATE agent_node SET status=?,version=version+1,lease_owner=NULL,lease_until=NULL,finished_at=NOW(6) WHERE run_id=? AND tenant_id=? AND status IN (?,?)`)).
						WithArgs(model.NodeCancelled, runID, tenant, model.NodePending, model.NodeReady)
					if tc.nodesErr != nil {
						nodes.WillReturnError(tc.nodesErr)
						mock.ExpectRollback()
					} else {
						nodes.WillReturnResult(sqlmock.NewResult(0, 2))
						commit := mock.ExpectCommit()
						if tc.commitErr != nil {
							commit.WillReturnError(tc.commitErr)
						}
					}
				}
			}

			ctx := context.Background()
			published := 0
			pub := cancelPublisherFunc(func(gotCtx context.Context, gotRunID string) error {
				published++
				if gotCtx != ctx || gotRunID != runID {
					t.Errorf("PublishCancel args = (%v, %q), want original context and %q", gotCtx, gotRunID, runID)
				}
				if err := mock.ExpectationsWereMet(); err != nil {
					t.Errorf("published before cancellation committed: %v", err)
				}
				return tc.pubErr
			})
			rt := newCancelRuntime(s, pub)
			err = rt.Cancel(ctx, tenant, runID, reason)
			if (err != nil) != tc.wantErr {
				t.Errorf("Cancel error = %v, wantErr %v", err, tc.wantErr)
			}
			if published != tc.wantPub {
				t.Errorf("PublishCancel calls = %d, want %d", published, tc.wantPub)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Error(err)
			}
		})
	}
}

func TestEnv(t *testing.T) {
	for _, key := range []string{"REDIS_ADDR", "REDIS_STREAM", "REDIS_GROUP"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "")
			if got := env(key, "fallback"); got != "fallback" {
				t.Fatalf("empty env = %q, want fallback", got)
			}
			t.Setenv(key, "configured")
			if got := env(key, "fallback"); got != "configured" {
				t.Fatalf("env = %q, want configured", got)
			}
		})
	}
}
