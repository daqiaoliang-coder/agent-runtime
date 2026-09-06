package store

import (
	"agent-runtime/internal/model"
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

// newMockStore 构造一个基于 sqlmock 的 MySQL 实例，用于在不依赖真实 MySQL 的情况下
// 验证 SQL 查询/参数。返回清理函数关闭连接。
func newMockStore(t *testing.T) (*MySQL, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new: %v", err)
	}
	return &MySQL{DB: db}, mock, func() { _ = db.Close() }
}

// TestGetRun_IncludesTenantFilter 验证 Bug1 修复（数据层）：
// GetRun 查询必须带 tenant_id 过滤，且参数顺序为 (run_id, tenant_id)。
func TestGetRun_IncludesTenantFilter(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	now := time.Now()
	rows := sqlmock.NewRows([]string{"run_id", "tenant_id", "agent_id", "status", "version", "input", "output", "current_node_id", "max_steps", "steps", "created_at", "updated_at"}).
		AddRow("run-1", "tenant-A", "agent-1", model.RunRunning, 1, "in", "", "", 50, 0, now, now)
	// 查询必须包含 tenant_id（regexp 部分匹配），且参数为 (run_id, tenant)。
	mock.ExpectQuery("tenant_id").
		WithArgs("run-1", "tenant-A").
		WillReturnRows(rows)
	r, err := s.GetRun(context.Background(), "tenant-A", "run-1")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if r.ID != "run-1" || r.TenantID != "tenant-A" {
		t.Errorf("unexpected run: %+v", r)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestGetRun_TenantMismatch_ReturnsNotFound 租户不匹配时不应返回记录。
func TestGetRun_TenantMismatch_ReturnsNotFound(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectQuery("tenant_id").
		WithArgs("run-1", "tenant-B").
		WillReturnError(sql.ErrNoRows)
	r, err := s.GetRun(context.Background(), "tenant-B", "run-1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows, got %v", err)
	}
	if r != nil && r.ID != "" {
		t.Errorf("expected no run, got %+v", r)
	}
}

// TestUpdateRunCAS_IncludesTenantFilter UPDATE 必须带 tenant_id 过滤。
func TestUpdateRunCAS_IncludesTenantFilter(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	// 参数顺序：status, currentNode, output, run_id, tenant, version
	mock.ExpectExec("tenant_id").
		WithArgs(model.RunSuccess, "", "", "run-1", "tenant-A", int64(3)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	ok, err := s.UpdateRunCAS(context.Background(), "tenant-A", "run-1", 3, model.RunSuccess, "", "")
	if err != nil {
		t.Fatalf("UpdateRunCAS: %v", err)
	}
	if !ok {
		t.Errorf("expected CAS success (1 row affected)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestUpdateRunCAS_TenantMismatch_NoRowAffected 错误租户不应影响任何行。
func TestUpdateRunCAS_TenantMismatch_NoRowAffected(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("tenant_id").
		WithArgs(model.RunFailed, "", "", "run-1", "tenant-B", int64(1)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	ok, err := s.UpdateRunCAS(context.Background(), "tenant-B", "run-1", 1, model.RunFailed, "", "")
	if err != nil {
		t.Fatalf("UpdateRunCAS: %v", err)
	}
	if ok {
		t.Errorf("expected no row affected for mismatched tenant")
	}
}

// TestGetNode_IncludesTenantFilter 节点查询必须带 tenant_id。
func TestGetNode_IncludesTenantFilter(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	now := time.Now()
	// 列顺序与 GetNode 的 Scan 一致（lease_until/started_at/finished_at 可为 NULL）。
	rows := sqlmock.NewRows([]string{"node_id", "run_id", "tenant_id", "parent_node_id", "type", "name", "input", "output", "status", "attempt", "version", "lease_owner", "lease_until", "planning_round", "created_at", "started_at", "finished_at"}).
		AddRow("node-1", "run-1", "tenant-A", "", model.NodeLLM, "reason", "in", "", model.NodePending, 0, int64(0), "", nil, 1, now, nil, nil)
	mock.ExpectQuery("tenant_id").
		WithArgs("node-1", "tenant-A").
		WillReturnRows(rows)
	n, err := s.GetNode(context.Background(), "tenant-A", "node-1")
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	if n.ID != "node-1" || n.TenantID != "tenant-A" {
		t.Errorf("unexpected node: %+v", n)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestClaimNode_IncludesTenantFilter 节点认领 UPDATE 必须带 tenant_id 过滤。
func TestClaimNode_IncludesTenantFilter(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	// 参数顺序：status, owner, sec, node_id, tenant, version, NodePending, NodeReady
	mock.ExpectExec("tenant_id").
		WithArgs(model.NodeRunning, "worker-1", 30, "node-1", "tenant-A", int64(0), model.NodePending, model.NodeReady).
		WillReturnResult(sqlmock.NewResult(0, 1))
	ok, err := s.ClaimNode(context.Background(), "tenant-A", "node-1", 0, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimNode: %v", err)
	}
	if !ok {
		t.Errorf("expected claim success")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestClaimNode_TenantMismatch_NoRowAffected 错误租钥不应认领其他租户的节点。
func TestClaimNode_TenantMismatch_NoRowAffected(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("tenant_id").
		WithArgs(model.NodeRunning, "worker-1", 30, "node-1", "tenant-B", int64(0), model.NodePending, model.NodeReady).
		WillReturnResult(sqlmock.NewResult(0, 0))
	ok, err := s.ClaimNode(context.Background(), "tenant-B", "node-1", 0, "worker-1", 30*time.Second)
	if err != nil {
		t.Fatalf("ClaimNode: %v", err)
	}
	if ok {
		t.Errorf("expected no claim for mismatched tenant")
	}
}

// TestClaimToolCall_InsertsRunningWithTenant 验证工具调用幂等抢占：
// INSERT IGNORE 必须写入 tenant_id 与 status='RUNNING'，1 行受影响时返回 true。
func TestClaimToolCall_InsertsRunningWithTenant(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	// 参数顺序：callID, tenant, runID, nodeID, toolName, idempotencyKey, input, attempt
	mock.ExpectExec("INSERT IGNORE INTO tool_call").
		WithArgs("call-1", "tenant-A", "run-1", "node-1", "search", "call-1", "q", 0).
		WillReturnResult(sqlmock.NewResult(0, 1))
	ok, err := s.ClaimToolCall(context.Background(), "tenant-A", "call-1", "run-1", "node-1", "search", "call-1", "q", 0)
	if err != nil {
		t.Fatalf("ClaimToolCall: %v", err)
	}
	if !ok {
		t.Errorf("expected new claim (1 row affected)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestClaimToolCall_ConflictReturnsFalse 幂等键冲突（0 行受影响）应返回 false 而非报错。
func TestClaimToolCall_ConflictReturnsFalse(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("INSERT IGNORE INTO tool_call").
		WithArgs("call-1", "tenant-A", "run-1", "node-1", "search", "call-1", "q", 0).
		WillReturnResult(sqlmock.NewResult(0, 0))
	ok, err := s.ClaimToolCall(context.Background(), "tenant-A", "call-1", "run-1", "node-1", "search", "call-1", "q", 0)
	if err != nil {
		t.Fatalf("ClaimToolCall: %v", err)
	}
	if ok {
		t.Errorf("expected false on idempotency-key conflict")
	}
}

// TestGetToolCall_TenantFilter 读取工具调用必须按租户 + 幂等键过滤。
func TestGetToolCall_TenantFilter(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	rows := sqlmock.NewRows([]string{"call_id", "tenant_id", "run_id", "node_id", "tool_name", "idempotency_key", "status", "output", "attempt"}).
		AddRow("call-1", "tenant-A", "run-1", "node-1", "search", "call-1", "SUCCESS", "result-1", 0)
	mock.ExpectQuery("FROM tool_call").
		WithArgs("call-1", "tenant-A").
		WillReturnRows(rows)
	tc, err := s.GetToolCall(context.Background(), "tenant-A", "call-1")
	if err != nil {
		t.Fatalf("GetToolCall: %v", err)
	}
	if tc.Status != "SUCCESS" || tc.Output != "result-1" || tc.TenantID != "tenant-A" {
		t.Errorf("unexpected tool call: %+v", tc)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestGetToolCall_TenantMismatch_NoRow 错误租户读不到其他租户的工具调用。
func TestGetToolCall_TenantMismatch_NoRow(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectQuery("FROM tool_call").
		WithArgs("call-1", "tenant-B").
		WillReturnError(sql.ErrNoRows)
	_, err := s.GetToolCall(context.Background(), "tenant-B", "call-1")
	if !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("expected sql.ErrNoRows for mismatched tenant, got %v", err)
	}
}

// TestReclaimToolCall_OnlyFailed 仅 FAILED 状态可回收为 RUNNING。
func TestReclaimToolCall_OnlyFailed(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("UPDATE tool_call SET status='RUNNING'").
		WithArgs("call-1", "tenant-A").
		WillReturnResult(sqlmock.NewResult(0, 1))
	ok, err := s.ReclaimToolCall(context.Background(), "tenant-A", "call-1")
	if err != nil {
		t.Fatalf("ReclaimToolCall: %v", err)
	}
	if !ok {
		t.Errorf("expected reclaim success for FAILED call")
	}
}

// TestCompleteToolCall_SetsSuccess 成功落库必须写 output 并置 SUCCESS。
func TestCompleteToolCall_SetsSuccess(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("status='SUCCESS'").
		WithArgs("result-1", "call-1", "tenant-A").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := s.CompleteToolCall(context.Background(), "tenant-A", "call-1", "result-1"); err != nil {
		t.Fatalf("CompleteToolCall: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestRetryNode_SetsReadyWithReadyAt 重试回退：RUNNING→READY 并写 ready_at、attempt+1。
func TestRetryNode_SetsReadyWithReadyAt(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	readyAt := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
	mock.ExpectExec("ready_at=\\?,attempt=attempt\\+1").
		WithArgs(model.NodeReady, readyAt, "node-1", "tenant-A", int64(0), model.NodeRunning).
		WillReturnResult(sqlmock.NewResult(0, 1))
	ok, err := s.RetryNode(context.Background(), "tenant-A", "node-1", 0, readyAt)
	if err != nil {
		t.Fatalf("RetryNode: %v", err)
	}
	if !ok {
		t.Errorf("expected retry success")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestEnqueueDLQ_InsertsWithTenant 死信写入必须携带 tenant_id。
func TestEnqueueDLQ_InsertsWithTenant(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("INSERT INTO agent_dlq").
		WithArgs(sqlmock.AnyArg(), "run-1", "node-1", "tenant-A", "boom", 4, "").
		WillReturnResult(sqlmock.NewResult(1, 1))
	if err := s.EnqueueDLQ(context.Background(), "tenant-A", "run-1", "node-1", "boom", 4, ""); err != nil {
		t.Fatalf("EnqueueDLQ: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestReadyTasks_GatesOnReadyAt ReadyTasks 扫描 SQL 必须含 ready_at 到期闸门。
func TestReadyTasks_GatesOnReadyAt(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	rows := sqlmock.NewRows([]string{"node_id", "run_id", "tenant_id", "attempt"}).
		AddRow("node-1", "run-1", "tenant-A", 2)
	mock.ExpectQuery("ready_at").WillReturnRows(rows)
	tasks, err := s.ReadyTasks(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReadyTasks: %v", err)
	}
	if len(tasks) != 1 || tasks[0].NodeID != "node-1" {
		t.Errorf("unexpected tasks: %+v", tasks)
	}
}

// TestInboxSeen_FalseWhenNoRow 无记录返回 false（未处理）。
func TestInboxSeen_FalseWhenNoRow(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectQuery("FROM agent_inbox").
		WithArgs("evt-1", "tenant-A").
		WillReturnError(sql.ErrNoRows)
	seen, err := s.InboxSeen(context.Background(), "tenant-A", "evt-1")
	if err != nil || seen {
		t.Fatalf("expected (false,nil), got (%v,%v)", seen, err)
	}
}

// TestInboxSeen_TrueWhenRecorded 有记录返回 true（已处理）。
func TestInboxSeen_TrueWhenRecorded(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectQuery("FROM agent_inbox").
		WithArgs("evt-1", "tenant-A").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))
	seen, err := s.InboxSeen(context.Background(), "tenant-A", "evt-1")
	if err != nil || !seen {
		t.Fatalf("expected (true,nil), got (%v,%v)", seen, err)
	}
}

// TestMarkInbox_Inserts 写入 Inbox 携带 event_id + tenant_id。
func TestMarkInbox_Inserts(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("INSERT IGNORE INTO agent_inbox").
		WithArgs("evt-1", "tenant-A").
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := s.MarkInbox(context.Background(), "tenant-A", "evt-1"); err != nil {
		t.Fatalf("MarkInbox: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestLoadCheckpoint_ReadsState 读取检查点返回 graph 版本、当前节点、状态 JSON。
func TestLoadCheckpoint_ReadsState(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectQuery("FROM checkpoint").
		WithArgs("run-1").
		WillReturnRows(sqlmock.NewRows([]string{"graph_version", "current_node_id", "state_json"}).
			AddRow(int64(3), "node-1", []byte(`{"messages":[],"node_outputs":null}`)))
	v, node, state, err := s.LoadCheckpoint(context.Background(), "run-1")
	if err != nil || v != 3 || node != "node-1" || len(state) == 0 {
		t.Fatalf("LoadCheckpoint: v=%d node=%s state=%s err=%v", v, node, string(state), err)
	}
}

// TestSaveCheckpoint_UpsertsState 检查点写入为 upsert（ON DUPLICATE KEY UPDATE）。
func TestSaveCheckpoint_UpsertsState(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("INSERT INTO checkpoint").
		WithArgs("run-1", int64(2), "node-1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))
	if err := s.SaveCheckpoint(context.Background(), "run-1", 2, "node-1", model.RunContext{Messages: []model.ChatTurn{{Role: "user", Content: "hi"}}}); err != nil {
		t.Fatalf("SaveCheckpoint: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestRecordLLMUsage_InsertsWithDimensions LLM 用量落库必须携带 run/node/tenant/model 维度。
func TestRecordLLMUsage_InsertsWithDimensions(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()
	mock.ExpectExec("INSERT INTO llm_usage").
		WithArgs("usage-1", "run-1", "node-1", "tenant-A", "gpt-4o", 100, 50, 150, 0.000625).
		WillReturnResult(sqlmock.NewResult(0, 1))
	err := s.RecordLLMUsage(context.Background(), model.LLMUsage{
		ID: "usage-1", RunID: "run-1", NodeID: "node-1", TenantID: "tenant-A",
		Model: "gpt-4o", PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150,
		Cost: 0.000625,
	})
	if err != nil {
		t.Fatalf("RecordLLMUsage: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}
