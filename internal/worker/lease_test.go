package worker

import (
	"agent-runtime/internal/model"
	"agent-runtime/internal/retry"
	"agent-runtime/internal/store"
	"context"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// blockingExec 阻塞等待 ctx 取消后返回，模拟长耗时的 LLM/工具调用。
// 用通道记录返回的错误，供测试断言"执行确实被中止"。
type blockingExec struct{ result chan error }

func newBlockingExec() *blockingExec { return &blockingExec{result: make(chan error, 1)} }

func (b *blockingExec) Execute(ctx context.Context, _ *model.Node) (string, error) {
	<-ctx.Done()
	err := ctx.Err()
	b.result <- err
	return "", err
}

// TestHandle_AbortsWhenLeaseLost 验证续租丢失必须中止本地执行（防双跑）：
// RenewLease 返回 false（节点已被接管或被 recovery 重置）后，
//   - Exec.Execute 必须以 context.Canceled 被中止，不得继续产生副作用；
//   - 失败落库走健康的父 ctx（不因中止而失败）；
//   - 节点按可重试失败处理（RetryNode 置回 READY）。
//
// 此前 RenewLease 的结果被 `_ =` 吞掉：租约丢失后 worker 继续执行，
// 与接管者形成同一节点双执行——LLM 节点无幂等表保护，双跑即双倍 token 与双倍副作用。
func TestHandle_AbortsWhenLeaseLost(t *testing.T) {
	s, mock, cleanup := newWorkerMockStore(t)
	defer cleanup()

	nodeRow := func(id string, status model.NodeStatus, version int64, owner string) *sqlmock.Rows {
		now := time.Now()
		return sqlmock.NewRows([]string{
			"node_id", "run_id", "tenant_id", "parent_node_id", "type", "name",
			"input", "output", "status", "attempt", "version",
			"lease_owner", "lease_until", "planning_round",
			"created_at", "started_at", "finished_at",
		}).AddRow(id, "run-1", "tenant-A", "", model.NodeLLM, "step", "in", "", status, 0, version, owner, now, 1, now, now, now)
	}
	runRows := sqlmock.NewRows([]string{
		"run_id", "tenant_id", "thread_id", "agent_id", "user_id", "auth_method",
		"status", "version", "input", "output", "current_node_id",
		"max_steps", "steps", "max_rounds", "max_tokens", "created_at", "updated_at",
	}).AddRow("run-1", "tenant-A", "", "agent-1", "user-1", "jwt-hs256", model.RunRunning, 1, "in", "", "", 50, 0, 10, 0, time.Now(), time.Now())

	// Handle 的 store 调用序列：
	// GetNode(READY) -> GetRun(RUNNING) -> ClaimNode -> GetNode(RUNNING, 新 version)
	// -> RenewLease(0 行 = lease lost) -> [失败路径] EnqueueDLQ -> FailNodeWithOutbox(tx) -> RetryNode
	mock.ExpectQuery("agent_node WHERE node_id").WillReturnRows(nodeRow("node-1", model.NodeReady, 1, ""))
	mock.ExpectQuery("FROM agent_run WHERE run_id").WillReturnRows(runRows)
	mock.ExpectExec("version=version\\+1,lease_owner").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("agent_node WHERE node_id").WillReturnRows(nodeRow("node-1", model.NodeRunning, 2, "worker-1"))
	// lease lost：UPDATE 影响行数 0。execErr=context.Canceled 属可重试失败，
	// 走 RetryNode 置回 READY（EnqueueDLQ/失败事务只在重试耗尽时）。
	mock.ExpectExec("lease_until=DATE_ADD").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ready_at=\\?,attempt=attempt\\+1").WillReturnResult(sqlmock.NewResult(0, 1))

	exec := newBlockingExec()
	w := &Worker{
		Store: s, Exec: exec, Retry: retry.Default(), ID: "worker-1",
		heartbeatInterval: 10 * time.Millisecond, leaseTTL: time.Second,
	}
	err := w.Handle(context.Background(), model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"})
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	// 执行必须以取消被中止：租约丢失后不得继续。
	select {
	case execErr := <-exec.result:
		if execErr != context.Canceled {
			t.Errorf("exec should be aborted with context.Canceled, got %v", execErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("exec was never aborted; lease loss did not stop local execution")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met (failure path must run on healthy ctx): %v", err)
	}
}

// TestHandle_ExecTimeoutScopedToExecution 验证执行超时只约束 Exec.Execute：
// 超时触发后失败落库必须用健康的父 ctx 正常完成（此前超时 ctx 同时覆盖落库，
// 兜底写入全部立即失败，节点状态只能靠 PEL 重投间接收敛）。
func TestHandle_ExecTimeoutScopedToExecution(t *testing.T) {
	s, mock, cleanup := newWorkerMockStore(t)
	defer cleanup()

	nodeRow := func(id string, status model.NodeStatus, version int64, owner string) *sqlmock.Rows {
		now := time.Now()
		return sqlmock.NewRows([]string{
			"node_id", "run_id", "tenant_id", "parent_node_id", "type", "name",
			"input", "output", "status", "attempt", "version",
			"lease_owner", "lease_until", "planning_round",
			"created_at", "started_at", "finished_at",
		}).AddRow(id, "run-1", "tenant-A", "", model.NodeLLM, "step", "in", "", status, 0, version, owner, now, 1, now, now, now)
	}
	runRows := sqlmock.NewRows([]string{
		"run_id", "tenant_id", "thread_id", "agent_id", "user_id", "auth_method",
		"status", "version", "input", "output", "current_node_id",
		"max_steps", "steps", "max_rounds", "max_tokens", "created_at", "updated_at",
	}).AddRow("run-1", "tenant-A", "", "agent-1", "user-1", "jwt-hs256", model.RunRunning, 1, "in", "", "", 50, 0, 10, 0, time.Now(), time.Now())

	// 调用序列同上，但 RenewLease 正常续租（无 lease 丢失），失败由执行超时触发。
	mock.ExpectQuery("agent_node WHERE node_id").WillReturnRows(nodeRow("node-1", model.NodeReady, 1, ""))
	mock.ExpectQuery("FROM agent_run WHERE run_id").WillReturnRows(runRows)
	mock.ExpectExec("version=version\\+1,lease_owner").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectQuery("agent_node WHERE node_id").WillReturnRows(nodeRow("node-1", model.NodeRunning, 2, "worker-1"))
	// 失败路径（超时后，ctx 健康）：execErr=context.DeadlineExceeded 属可重试失败，
	// 走 RetryNode 置回 READY（EnqueueDLQ/失败事务只在重试耗尽时）。
	mock.ExpectExec("ready_at=\\?,attempt=attempt\\+1").WillReturnResult(sqlmock.NewResult(0, 1))

	exec := newBlockingExec()
	w := &Worker{
		Store: s, Exec: exec, Retry: retry.Default(), ID: "worker-1",
		ExecTimeout: 30 * time.Millisecond, // 远小于执行体阻塞时长，必然超时
		heartbeatInterval: 50 * time.Millisecond, leaseTTL: time.Second,
	}
	if err := w.Handle(context.Background(), model.Task{RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A"}); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	select {
	case execErr := <-exec.result:
		if execErr != context.DeadlineExceeded {
			t.Errorf("exec should time out with context.DeadlineExceeded, got %v", execErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("exec was never aborted by exec timeout")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met (failure path must run on healthy ctx): %v", err)
	}
}

// newWorkerMockStore 与 store 包的 newMockStore 同构，供 worker 包测试使用。
func newWorkerMockStore(t *testing.T) (*store.MySQL, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new: %v", err)
	}
	return &store.MySQL{DB: db}, mock, func() { _ = db.Close() }
}
