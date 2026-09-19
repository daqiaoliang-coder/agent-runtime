package store

import (
	"agent-runtime/internal/model"
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

// 本文件验证人工闸门链路的**存储层契约**。
//
// 这三条 SQL 的正确性无法靠上层测试覆盖：runtime 层用替身验证的是状态机流转，
// 而"节点是否与 Run 在同一事务内挂起""放行后是否有节点可重新调度"
// 完全取决于这里的 SQL 与事务边界。写错不会立刻报错，
// 而是表现为极难排查的运行卡死 —— 护栏接上了，人工也批了，流程却再也不动。

// TestInterruptRun_SuspendsNodeInSameTx 中断必须在同一事务内挂起节点。
//
// 只切 Run 不切节点的后果：节点仍停在 RUNNING 且租约会过期，
// recovery 的 RecoverExpired（只捞 status=RUNNING）会把它重置回 READY 重新投递 ——
// 于是"人工还没批，节点又被跑了一遍"，护栏形同虚设。
// 置为 WAITING_HUMAN 后，ClaimNode 的 status IN (PENDING,READY)
// 与 RecoverExpired 的 status=RUNNING 都捞不到它，节点被真正冻住。
func TestInterruptRun_SuspendsNodeInSameTx(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectBegin()
	// Run 置 WAITING_HUMAN（CAS）
	mock.ExpectExec("UPDATE agent_run").
		WithArgs(model.RunWaitingHuman, "n1", "guard: suspicious input", "run-1", "tenant-A", int64(3), model.RunRunning).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 审批记录落库。注意 'WAITING' 在 SQL 里是字面量而非占位符，
	// 因此只有 5 个参数 —— 多传一个会让断言与实际不符而误判为回归。
	mock.ExpectExec("INSERT INTO run_interrupt").
		WithArgs(sqlmock.AnyArg(), "run-1", "tenant-A", "n1", "guard: suspicious input").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// 关键：节点挂起 + 释放租约，且必须在同一事务内（Begin 与 Commit 之间）
	mock.ExpectExec("UPDATE agent_node").
		WithArgs(model.NodeWaitingHuman, "n1", "tenant-A", model.NodeRunning, model.NodeReady).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ok, err := s.InterruptRun(context.Background(), "tenant-A", "run-1", "n1", "guard: suspicious input", 3)
	if err != nil {
		t.Fatalf("InterruptRun: %v", err)
	}
	if !ok {
		t.Error("expected CAS success")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met (node suspension missing or outside tx?): %v", err)
	}
}

// TestInterruptRun_EmptyNodeID_SkipsNodeUpdate Run 级中断无节点可挂时不得报错。
//
// nodeID 为空对应非护栏触发的 Run 级中断，此时没有具体节点可挂，
// 若仍执行 UPDATE agent_node 会用空标识匹配，语义不明。
func TestInterruptRun_EmptyNodeID_SkipsNodeUpdate(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE agent_run").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("INSERT INTO run_interrupt").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// 不应出现 UPDATE agent_node
	mock.ExpectCommit()

	ok, err := s.InterruptRun(context.Background(), "tenant-A", "run-1", "", "run level", 1)
	if err != nil {
		t.Fatalf("InterruptRun: %v", err)
	}
	if !ok {
		t.Error("expected CAS success")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unexpected node update for empty nodeID: %v", err)
	}
}

// TestInterruptRun_CASConflict_NoInterruptRecord CAS 失败时不得留下孤儿审批记录。
//
// 并发中断（version 不匹配）必须整体回滚：若审批记录已插入而 Run 状态没变，
// 后续人工放行会去恢复一个从未挂起的 Run，状态机会走进未定义分支。
func TestInterruptRun_CASConflict_NoInterruptRecord(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectExec("UPDATE agent_run").
		WithArgs(model.RunWaitingHuman, "n1", "reason", "run-1", "tenant-A", int64(99), model.RunRunning).
		WillReturnResult(sqlmock.NewResult(0, 0)) // 0 行受影响 = CAS 失败
	mock.ExpectRollback()

	ok, err := s.InterruptRun(context.Background(), "tenant-A", "run-1", "n1", "reason", 99)
	if err != nil {
		t.Fatalf("InterruptRun: %v", err)
	}
	if ok {
		t.Error("expected CAS failure to report false")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expected rollback without interrupt insert: %v", err)
	}
}

// TestResumeRun_RearmsNodeAndReturnsTask 放行必须重新武装节点并返回待投递任务。
//
// 重新调度必须与状态切换同事务，否则存在一个致命窗口 ——
// Run 已 RUNNING 但节点仍 WAITING_HUMAN，此时进程崩溃，
// ReadyTasks 只捞 READY、RecoverExpired 只捞 RUNNING，两边都捞不到它，
// 该 Run 永久卡死且无任何告警。
//
// 返回 tasks 是给调用方投递用的：少了这一步，Run 回到 RUNNING 却没有节点在跑。
func TestResumeRun_RearmsNodeAndReturnsTask(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectBegin()
	// 先查出待恢复节点（必须在 run_interrupt 被改成 RESOLVED 之前）
	mock.ExpectQuery("SELECT node_id FROM run_interrupt").
		WithArgs("run-1", "tenant-A").
		WillReturnRows(sqlmock.NewRows([]string{"node_id"}).AddRow("n1"))
	// Run 回到 RUNNING
	mock.ExpectExec("UPDATE agent_run").
		WithArgs(model.RunRunning, "approved", "run-1", "tenant-A", int64(4), model.RunWaitingHuman).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 审批记录置 RESOLVED
	mock.ExpectExec("UPDATE run_interrupt").
		WithArgs("approved", "run-1", "tenant-A").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 节点重新武装为 READY，且清空 ready_at 使其立即可被捞取
	mock.ExpectExec("UPDATE agent_node").
		WithArgs(model.NodeReady, "n1", "tenant-A", model.NodeWaitingHuman).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	ok, tasks, err := s.ResumeRun(context.Background(), "tenant-A", "run-1", "approved", 4)
	if err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	if !ok {
		t.Fatal("expected CAS success")
	}
	if len(tasks) != 1 {
		t.Fatalf("expected 1 task to re-enqueue, got %d", len(tasks))
	}
	got := tasks[0]
	if got.NodeID != "n1" || got.RunID != "run-1" || got.TenantID != "tenant-A" {
		t.Errorf("task lost identity: %+v", got)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestResumeRun_NoAttemptIncrement 放行不得消耗重试预算。
//
// 人工放行后的执行是"首次执行"，不是失败重试。若自增 attempt，
// 一次护栏拦截就白吃掉一次重试额度；而护栏拦截恰恰是需要人看完再决定的场景，
// 多次拦截累积下来会让节点提前进入死信队列。
func TestResumeRun_NoAttemptIncrement(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT node_id FROM run_interrupt").
		WillReturnRows(sqlmock.NewRows([]string{"node_id"}).AddRow("n1"))
	mock.ExpectExec("UPDATE agent_run").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE run_interrupt").WillReturnResult(sqlmock.NewResult(0, 1))
	// 断言 SQL 文本中不含 attempt 自增
	mock.ExpectExec("UPDATE agent_node SET status=\\?,ready_at=NULL,lease_owner=NULL,lease_until=NULL,version=version\\+1").
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if _, _, err := s.ResumeRun(context.Background(), "tenant-A", "run-1", "approved", 1); err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("node re-arm SQL must not increment attempt: %v", err)
	}
}

// TestResumeRun_CASConflict_ReturnsFalse CAS 失败时不得返回任务。
//
// 返回非空任务会让调用方投递一个状态未恢复的节点，
// worker 认领时发现 Run 不是 RUNNING 就跳过，白白产生一次调度与日志噪音。
func TestResumeRun_CASConflict_ReturnsFalse(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT node_id FROM run_interrupt").
		WillReturnRows(sqlmock.NewRows([]string{"node_id"}).AddRow("n1"))
	mock.ExpectExec("UPDATE agent_run").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	ok, tasks, err := s.ResumeRun(context.Background(), "tenant-A", "run-1", "approved", 99)
	if err != nil {
		t.Fatalf("ResumeRun: %v", err)
	}
	if ok {
		t.Error("expected CAS failure")
	}
	if len(tasks) != 0 {
		t.Errorf("expected no tasks on CAS failure, got %+v", tasks)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// TestHasResolvedApproval_ScopedToTenantRunNode 放行判定必须收窄到三元组。
//
// 这是护栏 Bypass 的判定依据。若只按 tenant 或只按 run 查，
// 一次放行会变成"该租户/该 Run 下所有节点永久豁免"——
// 攻击者只要诱导一次人工放行，后续所有注入载荷都能畅通无阻。
// 一次放行只解一次锁。
func TestHasResolvedApproval_ScopedToTenantRunNode(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectQuery("tenant_id=\\? AND run_id=\\? AND node_id=\\? AND status='RESOLVED'").
		WithArgs("tenant-A", "run-1", "n1").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	ok, err := s.HasResolvedApproval(context.Background(), "tenant-A", "run-1", "n1")
	if err != nil {
		t.Fatalf("HasResolvedApproval: %v", err)
	}
	if !ok {
		t.Error("expected approval to be found")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("query scope wrong: %v", err)
	}
}

// TestHasResolvedApproval_EmptyNodeID_ShortCircuits 空节点标识不得查库。
//
// 空 NodeID 会匹配到所有 node_id 为空的历史记录（Run 级中断写入的就是空值），
// 让一次 Run 级中断意外变成全局豁免。短路返回 false 才安全。
func TestHasResolvedApproval_EmptyNodeID_ShortCircuits(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	// 不应有任何查询发生
	ok, err := s.HasResolvedApproval(context.Background(), "tenant-A", "run-1", "")
	if err != nil {
		t.Fatalf("HasResolvedApproval: %v", err)
	}
	if ok {
		t.Error("empty node id must never report approved")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("empty node id must not hit the database: %v", err)
	}
}
