package store

import (
	"context"
	"testing"
	"time"

	"agent-runtime/internal/model"

	"github.com/DATA-DOG/go-sqlmock"
)

// 本文件验证 P2 人工审批闭环的**存储层契约**：人工否决（RejectRun）与
// 授权学习规则（permission_rule）。
//
// 与 hitl_test.go 同一立场上写这些断言：状态机语义在 runtime 层用替身
// 验证过了，这里盯住的是 SQL 与事务边界——否决是否同事务置 FAILED 并
// 投递收敛事件、学习规则的查重/软删口径是否如注释所述。这类错误静态
// 看不出来，运行时表现为"批了却不动""学了的规则还在拦"这类极难排查
// 的行为漂移。

// TestRejectRun_FailsSuspendedNodeInSameTx 否决必须同事务内：
// 解决 interrupt → 节点置 FAILED → 写 AgentStepFailed Outbox 事件。
//
// 节点不置 FAILED 而是重新武装的话，重新调度会再次命中瀑布、再次转人工；
// 不投 AgentStepFailed 的话 Resumer 不认（它只消费 Completed/Failed/
// ReplanRequested），Run 永远挂着 WAITING_HUMAN 前史无法收敛。
func TestRejectRun_FailsSuspendedNodeInSameTx(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectBegin()
	// 先捞待否决的挂起节点（必须在 interrupt 置 RESOLVED 之前）
	mock.ExpectQuery("SELECT node_id FROM run_interrupt").
		WithArgs("run-1", "tenant-A").
		WillReturnRows(sqlmock.NewRows([]string{"node_id"}).AddRow("n1"))
	// Run 切回 RUNNING（收敛交给 Resumer）
	mock.ExpectExec("UPDATE agent_run").
		WithArgs(model.RunRunning, "denied by alice", "run-1", "tenant-A", int64(2), model.RunWaitingHuman).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// interrupt 解决并记录否决决策
	mock.ExpectExec("UPDATE run_interrupt").
		WithArgs("denied by alice", "run-1", "tenant-A").
		WillReturnResult(sqlmock.NewResult(0, 1))
	// 关键：节点 FAILED 必须在同一事务内
	mock.ExpectExec("UPDATE agent_node").
		WithArgs(model.NodeFailed, "n1", "tenant-A", model.NodeWaitingHuman).
		WillReturnResult(sqlmock.NewResult(0, 1))
	// AgentStepFailed Outbox 事件（Resumer 的收敛入口）
	mock.ExpectExec("INSERT INTO event_outbox").
		WithArgs(sqlmock.AnyArg(), "AgentStepFailed", "run-1", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	ok, err := s.RejectRun(context.Background(), "tenant-A", "run-1", "denied by alice", 2)
	if err != nil {
		t.Fatalf("RejectRun: %v", err)
	}
	if !ok {
		t.Fatal("expected CAS success")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met (node failure or outbox event outside tx?): %v", err)
	}
}

// TestRejectRun_VersionConflict 版本冲突（并发审批）必须返回 false 而不是报错——
// 调用方据此引导审批人刷新状态，两个审批人同时点同一条是正常并发。
func TestRejectRun_VersionConflict(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT node_id FROM run_interrupt").
		WithArgs("run-1", "tenant-A").
		WillReturnRows(sqlmock.NewRows([]string{"node_id"}))
	mock.ExpectExec("UPDATE agent_run").
		WithArgs(model.RunRunning, "denied by alice", "run-1", "tenant-A", int64(1), model.RunWaitingHuman).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	ok, err := s.RejectRun(context.Background(), "tenant-A", "run-1", "denied by alice", 1)
	if err != nil {
		t.Fatalf("RejectRun: %v", err)
	}
	if ok {
		t.Error("stale version must not reject")
	}
}

// TestHasResolvedApproval_ExcludesDeniedDecisions 人工否决同样把 interrupt
// 置 RESOLVED——放行判定必须把 deny 开头的 decision 排除掉，否则
// "已否决"会被 Bypass 误读成"已放行"。
func TestHasResolvedApproval_ExcludesDeniedDecisions(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectQuery("decision IS NULL OR decision NOT LIKE 'deny%'").
		WithArgs("tenant-A", "run-1", "n1").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))

	ok, err := s.HasResolvedApproval(context.Background(), "tenant-A", "run-1", "n1")
	if err != nil {
		t.Fatalf("HasResolvedApproval: %v", err)
	}
	if ok {
		t.Error("a denied interrupt must never count as approval")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("query must exclude denied decisions: %v", err)
	}
}

// TestSavePermissionRule_SkipsDuplicate 重复裁决同一"always allow"不得堆出
// 重复行：判定无害（聚合取最严）但污染配置面、模糊审计时间线。
func TestSavePermissionRule_SkipsDuplicate(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT COUNT").
		WithArgs("tenant-A", "run-1", "shell", "rm -rf /tmp/build", "allow", "run").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	// 不应出现 INSERT

	if err := s.SavePermissionRule(context.Background(), model.PermissionRule{
		ID: "rule-1", TenantID: "tenant-A", RunID: "run-1", Tool: "shell",
		Pattern: "rm -rf /tmp/build", Effect: "allow", Source: "run", LearnedBy: "alice",
	}); err != nil {
		t.Fatalf("SavePermissionRule: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("duplicate rule must not be inserted: %v", err)
	}
}

// TestSavePermissionRule_TenantScopeStoresNullRunID 租户级规则的 run_id 必须落
// NULL 而不是空串：ListActivePermissionRules 用 run_id IS NULL 区分作用域，
// 空串会让租户规则永远匹配不到（放行了却还拦，授权假象）。
func TestSavePermissionRule_TenantScopeStoresNullRunID(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectQuery("SELECT COUNT").
		WithArgs("tenant-A", "shell", "rm *", "allow", "tenant").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("INSERT INTO permission_rule").
		WithArgs(sqlmock.AnyArg(), "tenant-A", nil, "shell", "rm *", "allow", "tenant", "alice").
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := s.SavePermissionRule(context.Background(), model.PermissionRule{
		ID: "rule-2", TenantID: "tenant-A", Tool: "shell",
		Pattern: "rm *", Effect: "allow", Source: "tenant", LearnedBy: "alice",
	}); err != nil {
		t.Fatalf("SavePermissionRule: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("tenant rule must store NULL run_id: %v", err)
	}
}

// TestListActivePermissionRules_Scope 作用域必须是"租户级 + 本 Run 级"：
// 只查 Run 级会让租户 always allow 失效；不做 run 过滤会把别的 Run 的
// 一次性授权泄漏成本租户授权。
func TestListActivePermissionRules_Scope(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	now := time.Now()
	mock.ExpectQuery("run_id IS NULL OR run_id=\\?").
		WithArgs("tenant-A", "run-1").
		WillReturnRows(sqlmock.NewRows([]string{"rule_id", "tenant_id", "run_id", "tool", "pattern", "effect", "source", "learned_by", "learned_at"}).
			AddRow("rule-1", "tenant-A", "", "shell", "rm -rf /tmp/build", "allow", "run", "alice", now).
			AddRow("rule-2", "tenant-A", "", "shell", "kubectl *", "deny", "learned", "bob", now))

	rules, err := s.ListActivePermissionRules(context.Background(), "tenant-A", "run-1")
	if err != nil {
		t.Fatalf("ListActivePermissionRules: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}
	if rules[0].Source != "run" || rules[1].Effect != "deny" {
		t.Fatalf("rule fields mismatch: %+v", rules)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("scope query wrong: %v", err)
	}
}

// TestRevokePermissionRule_RevokedIsIdempotent 撤销幂等：已撤销的再撤销
// 返回 false，不制造虚假的"撤销成功"。
func TestRevokePermissionRule_RevokedIsIdempotent(t *testing.T) {
	s, mock, cleanup := newMockStore(t)
	defer cleanup()

	mock.ExpectExec("UPDATE permission_rule SET revoked_at").
		WithArgs("rule-1", "tenant-A").
		WillReturnResult(sqlmock.NewResult(0, 1))
	ok, err := s.RevokePermissionRule(context.Background(), "tenant-A", "rule-1")
	if err != nil || !ok {
		t.Fatalf("revoke: ok=%v err=%v", ok, err)
	}

	mock.ExpectExec("UPDATE permission_rule SET revoked_at").
		WithArgs("rule-1", "tenant-A").
		WillReturnResult(sqlmock.NewResult(0, 0))
	ok, err = s.RevokePermissionRule(context.Background(), "tenant-A", "rule-1")
	if err != nil {
		t.Fatalf("revoke twice: %v", err)
	}
	if ok {
		t.Error("revoking an already-revoked rule must return false")
	}
}
