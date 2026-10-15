//go:build integration

// Package store 集成测试：验证 MySQL 持久化层在真实数据库下的关键可靠性不变量。
// 运行方式：
//
//	DOCKER=1 go test -tags=integration ./internal/store/ -run TestIntegration -v
//
// 依赖 deploy/docker-compose.yml 启动的 MySQL（默认 3308 端口）。
package store

import (
	"agent-runtime/internal/model"
	"context"
	"database/sql"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// testDSN 返回集成测试用的 DSN，可通过环境变量覆盖。
func testDSN() string {
	if v := os.Getenv("DATABASE_DSN"); v != "" {
		return v
	}
	return "agent:agent@tcp(localhost:3308)/agent_runtime?parseTime=true"
}

// newIntegrationStore 连接真实 MySQL 并清理关键表，返回实例与清理函数。
// 每个测试独立、互不污染：TRUNCATE 在 setup 中执行，保证起始干净状态。
func newIntegrationStore(t *testing.T) *MySQL {
	t.Helper()
	ctx := context.Background()
	s, err := New(ctx, testDSN())
	if err != nil {
		t.Fatalf("connect mysql: %v (是否已 docker compose up?)", err)
	}
	// 清理所有业务表，保证测试隔离
	// 外键约束会阻止 DELETE，先关闭外键检查再 TRUNCATE
	if _, err := s.DB.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0"); err != nil {
		t.Fatalf("disable FK: %v", err)
	}
	for _, tbl := range []string{"planner_decision", "llm_usage", "checkpoint", "agent_inbox", "event_outbox", "agent_dlq", "tool_call", "agent_edge", "agent_node", "agent_run", "run_interrupt"} {
		if _, err := s.DB.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", tbl)); err != nil {
			t.Logf("truncate %s: %v", tbl, err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1"); err != nil {
		t.Fatalf("enable FK: %v", err)
	}
	return s
}

// TestIntegration_CreateRunAndGet 验证 P0-1 修复：CreateRun 列/参数匹配，
// 写入后可通过 GetRun 读回，租户隔离生效。
func TestIntegration_CreateRunAndGet(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{
		ID: "it-run-1", TenantID: "tenant-it", AgentID: "agent-1",
		Status: model.RunPending, Version: 1, Input: "hello",
		MaxSteps: 10, MaxRounds: 3, MaxTokens: 1000,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v (P0-1: 列/参数是否匹配?)", err)
	}
	got, err := s.GetRun(ctx, "tenant-it", "it-run-1")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.ID != "it-run-1" || got.TenantID != "tenant-it" || got.Status != model.RunPending {
		t.Errorf("unexpected run: %+v", got)
	}
	if got.Version != 1 || got.Input != "hello" {
		t.Errorf("version/input mismatch: %+v", got)
	}
	// 租户隔离：错误租户读不到
	_, err = s.GetRun(ctx, "tenant-other", "it-run-1")
	if err != sql.ErrNoRows {
		t.Errorf("expected sql.ErrNoRows for mismatched tenant, got %v", err)
	}
}

// TestIntegration_UpdateRunCAS_VersionConflict 验证乐观锁：
// 同版本可更新一次，过期版本 CAS 失败返回 false。
func TestIntegration_UpdateRunCAS_VersionConflict(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-cas-1", TenantID: "tenant-it", Status: model.RunPending, Version: 1}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	// 第一次 CAS：version=1 -> RUNNING，成功
	ok, err := s.UpdateRunCAS(ctx, "tenant-it", "it-cas-1", 1, model.RunRunning, "n1", "")
	if err != nil || !ok {
		t.Fatalf("first CAS should succeed: ok=%v err=%v", ok, err)
	}
	// 第二次 CAS：再用 version=1，应失败（已变 2）
	ok, err = s.UpdateRunCAS(ctx, "tenant-it", "it-cas-1", 1, model.RunSuccess, "", "done")
	if err != nil {
		t.Fatalf("CAS error: %v", err)
	}
	if ok {
		t.Error("stale version CAS should return false")
	}
	// 正确版本 CAS：version=2 -> SUCCESS
	ok, err = s.UpdateRunCAS(ctx, "tenant-it", "it-cas-1", 2, model.RunSuccess, "", "done")
	if err != nil || !ok {
		t.Errorf("CAS with current version should succeed: ok=%v err=%v", ok, err)
	}
}

// TestIntegration_InsertPlanAndDAG 验证 DAG 写入与拓扑查询：
// InsertPlan 写入节点 + 边，DependenciesReady/Children 返回正确结果。
func TestIntegration_InsertPlanAndDAG(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-dag-1", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	plan := model.Plan{Nodes: []model.PlanNode{
		{ID: "it-dag-1:a", Type: model.NodeTool, Name: "search", Input: "q"},
		{ID: "it-dag-1:b", Type: model.NodeTool, Name: "search", Input: "q2"},
		{ID: "it-dag-1:c", Type: model.NodeLLM, Name: "reason", DependsOn: []string{"it-dag-1:a", "it-dag-1:b"}},
	}}
	if err := s.InsertPlan(ctx, "it-dag-1", "tenant-it", plan); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	// 初始：c 的依赖未就绪（a/b 仍 PENDING）
	ready, err := s.DependenciesReady(ctx, "tenant-it", "it-dag-1:c")
	if err != nil {
		t.Fatal(err)
	}
	if ready {
		t.Error("c deps should not be ready before a/b complete")
	}
	// 将 a 标记为 READY 并完成
	if err := s.MarkReady(ctx, "tenant-it", "it-dag-1:a"); err != nil {
		t.Fatal(err)
	}
	nA, _ := s.GetNode(ctx, "tenant-it", "it-dag-1:a")
	if _, err := s.ClaimNode(ctx, "tenant-it", "it-dag-1:a", nA.Version, "w1", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	nA2, _ := s.GetNode(ctx, "tenant-it", "it-dag-1:a")
	if _, err := s.CompleteNode(ctx, "tenant-it", "it-dag-1:a", nA2.Version, "a-out"); err != nil {
		t.Fatal(err)
	}
	// a 完成后 c 仍不就绪（b 未完成）
	ready, _ = s.DependenciesReady(ctx, "tenant-it", "it-dag-1:c")
	if ready {
		t.Error("c deps should not be ready with only a done")
	}
	// Children(a) 应返回 c
	children, err := s.Children(ctx, "tenant-it", "it-dag-1:a")
	if err != nil {
		t.Fatal(err)
	}
	if len(children) != 1 || children[0].NodeID != "it-dag-1:c" {
		t.Errorf("Children(a) should return [c], got %+v", children)
	}
}

// completeItNode 是集成测试辅助：READY → CLAIM → COMPLETE。
func completeItNode(t *testing.T, s *MySQL, tenant, nodeID, output string) {
	t.Helper()
	ctx := context.Background()
	if err := s.MarkReady(ctx, tenant, nodeID); err != nil {
		t.Fatalf("MarkReady(%s): %v", nodeID, err)
	}
	n, _ := s.GetNode(ctx, tenant, nodeID)
	if _, err := s.ClaimNode(ctx, tenant, nodeID, n.Version, "w-it", 30*time.Second); err != nil {
		t.Fatalf("ClaimNode(%s): %v", nodeID, err)
	}
	n2, _ := s.GetNode(ctx, tenant, nodeID)
	if _, err := s.CompleteNode(ctx, tenant, nodeID, n2.Version, output); err != nil {
		t.Fatalf("CompleteNode(%s): %v", nodeID, err)
	}
}

// TestIntegration_CompletedAncestorNodes 验证 DAG 祖先作用域查询：
// 菱形依赖去重、跨层传递闭包、无关分支排除、仅返回 SUCCESS。
func TestIntegration_CompletedAncestorNodes(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	const runID, tenant = "it-anc-1", "tenant-it"
	run := &model.Run{ID: runID, TenantID: tenant, Status: model.RunRunning, Version: 1}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	// 拓扑：a ┐
	//       ├─ c ─ d    （x 与 d 无任何路径关系）
	//       b ┘
	plan := model.Plan{Nodes: []model.PlanNode{
		{ID: runID + ":a", Type: model.NodeTool, Name: "search", Input: "qa"},
		{ID: runID + ":b", Type: model.NodeTool, Name: "search", Input: "qb"},
		{ID: runID + ":c", Type: model.NodeLLM, Name: "reason", DependsOn: []string{runID + ":a", runID + ":b"}},
		{ID: runID + ":d", Type: model.NodeLLM, Name: "finish", DependsOn: []string{runID + ":c"}},
		{ID: runID + ":x", Type: model.NodeTool, Name: "search", Input: "qx"},
	}}
	if err := s.InsertPlan(ctx, runID, tenant, plan); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	completeItNode(t, s, tenant, runID+":a", "a-out")
	completeItNode(t, s, tenant, runID+":b", "b-out")
	completeItNode(t, s, tenant, runID+":c", "c-out")
	completeItNode(t, s, tenant, runID+":x", "x-out")
	// d 保持 PENDING：即使存在该节点行，也不应作为他人祖先出现（且查询只取 SUCCESS）。

	got, err := s.CompletedAncestorNodes(ctx, tenant, runID, runID+":d")
	if err != nil {
		t.Fatalf("CompletedAncestorNodes(d): %v", err)
	}
	var ids []string
	for _, n := range got {
		ids = append(ids, n.ID)
	}
	want := []string{runID + ":a", runID + ":b", runID + ":c"}
	if len(ids) != len(want) {
		t.Fatalf("ancestors of d = %v, want %v (x must be excluded, diamond must dedup)", ids, want)
	}
	for i, w := range want {
		if ids[i] != w {
			t.Errorf("ancestors of d = %v, want ordered %v", ids, want)
			break
		}
	}

	// c 的祖先只有 a/b：c 自己不算，d/x 也不在闭包内。
	gotC, err := s.CompletedAncestorNodes(ctx, tenant, runID, runID+":c")
	if err != nil {
		t.Fatalf("CompletedAncestorNodes(c): %v", err)
	}
	if len(gotC) != 2 || gotC[0].ID != runID+":a" || gotC[1].ID != runID+":b" {
		t.Errorf("ancestors of c = %+v, want [a b]", gotC)
	}

	// 根节点无上游。
	gotA, err := s.CompletedAncestorNodes(ctx, tenant, runID, runID+":a")
	if err != nil {
		t.Fatalf("CompletedAncestorNodes(a): %v", err)
	}
	if len(gotA) != 0 {
		t.Errorf("ancestors of root a = %+v, want empty", gotA)
	}

	// 租户隔离：错误租户查不到任何祖先。
	gotOther, err := s.CompletedAncestorNodes(ctx, "other-tenant", runID, runID+":d")
	if err != nil {
		t.Fatalf("CompletedAncestorNodes(other tenant): %v", err)
	}
	if len(gotOther) != 0 {
		t.Errorf("tenant isolation broken: %+v", gotOther)
	}
}

// TestIntegration_ClaimNode_LeaseAndRecover 验证租约机制与崩溃恢复：
// 认领 -> 租约过期 -> RecoverExpired 重置为 READY -> ReadyTasks 可重新扫到。
// 这是 P0-7 不变量：恢复流程自身可恢复。
func TestIntegration_ClaimNode_LeaseAndRecover(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-lease-1", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	plan := model.Plan{Nodes: []model.PlanNode{
		{ID: "it-lease-1:n1", Type: model.NodeLLM, Name: "step", Input: "do"},
	}}
	if err := s.InsertPlan(ctx, "it-lease-1", "tenant-it", plan); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(ctx, "tenant-it", "it-lease-1:n1"); err != nil {
		t.Fatal(err)
	}
	n, _ := s.GetNode(ctx, "tenant-it", "it-lease-1:n1")
	// 认领，租约 1 秒（快速过期便于测试）
	ok, err := s.ClaimNode(ctx, "tenant-it", "it-lease-1:n1", n.Version, "w-crash", 1*time.Second)
	if err != nil || !ok {
		t.Fatalf("ClaimNode: ok=%v err=%v", ok, err)
	}
	// 确认 RUNNING
	n2, _ := s.GetNode(ctx, "tenant-it", "it-lease-1:n1")
	if n2.Status != model.NodeRunning || n2.LeaseOwner != "w-crash" {
		t.Fatalf("node should be RUNNING by w-crash: %+v", n2)
	}
	// 等待租约过期
	time.Sleep(1500 * time.Millisecond)
	// RecoverExpired 应扫到并重置为 READY
	tasks, err := s.RecoverExpired(ctx, 10)
	if err != nil {
		t.Fatalf("RecoverExpired: %v", err)
	}
	found := false
	for _, tk := range tasks {
		if tk.NodeID == "it-lease-1:n1" {
			found = true
		}
	}
	if !found {
		t.Fatalf("RecoverExpired should return n1, got %+v", tasks)
	}
	n3, _ := s.GetNode(ctx, "tenant-it", "it-lease-1:n1")
	if n3.Status != model.NodeReady {
		t.Errorf("after recover, node should be READY, got %s", n3.Status)
	}
	if n3.LeaseOwner != "" {
		t.Errorf("after recover, lease_owner should be cleared, got %q", n3.LeaseOwner)
	}
	// ReadyTasks 应扫到（ready_at=NULL 视为立即可投递）
	rt, err := s.ReadyTasks(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	saw := false
	for _, tk := range rt {
		if tk.NodeID == "it-lease-1:n1" {
			saw = true
		}
	}
	if !saw {
		t.Errorf("ReadyTasks should include recovered n1, got %+v", rt)
	}
}

// TestIntegration_CompleteNodeWithOutbox 验证事务 Outbox 原子性：
// 节点完成与事件写入同事务，要么都成功要么都回滚。
func TestIntegration_CompleteNodeWithOutbox(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-outbox-1", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	plan := model.Plan{Nodes: []model.PlanNode{
		{ID: "it-outbox-1:n1", Type: model.NodeLLM, Name: "step", Input: "do"},
	}}
	if err := s.InsertPlan(ctx, "it-outbox-1", "tenant-it", plan); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkReady(ctx, "tenant-it", "it-outbox-1:n1"); err != nil {
		t.Fatal(err)
	}
	n, _ := s.GetNode(ctx, "tenant-it", "it-outbox-1:n1")
	_, _ = s.ClaimNode(ctx, "tenant-it", "it-outbox-1:n1", n.Version, "w1", 30*time.Second)
	n2, _ := s.GetNode(ctx, "tenant-it", "it-outbox-1:n1")

	evt := model.OutboxMessage{ID: "evt-outbox-1", EventType: "AgentStepCompleted", AggregateID: "it-outbox-1", Payload: `{"node":"n1"}`}
	ok, err := s.CompleteNodeWithOutbox(ctx, n2, "done", evt)
	if err != nil || !ok {
		t.Fatalf("CompleteNodeWithOutbox: ok=%v err=%v", ok, err)
	}
	// 节点应 SUCCESS
	n3, _ := s.GetNode(ctx, "tenant-it", "it-outbox-1:n1")
	if n3.Status != model.NodeSuccess || n3.Output != "done" {
		t.Errorf("node should be SUCCESS/done: %+v", n3)
	}
	// Outbox 应有一条 PENDING 记录
	msgs, err := s.ClaimOutbox(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, m := range msgs {
		if m.ID == "evt-outbox-1" {
			found = true
		}
	}
	if !found {
		t.Errorf("outbox should contain evt-outbox-1, got %+v", msgs)
	}
}

// TestIntegration_CompleteNodeWithOutbox_StaleVersionFails 验证 CAS 保护：
// 过期版本提交应失败，且 Outbox 不应写入（事务回滚）。
func TestIntegration_CompleteNodeWithOutbox_StaleVersionFails(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-outbox-2", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	_ = s.CreateRun(ctx, run)
	_ = s.InsertPlan(ctx, "it-outbox-2", "tenant-it", model.Plan{Nodes: []model.PlanNode{
		{ID: "it-outbox-2:n1", Type: model.NodeLLM, Name: "s"},
	}})
	_ = s.MarkReady(ctx, "tenant-it", "it-outbox-2:n1")
	n, _ := s.GetNode(ctx, "tenant-it", "it-outbox-2:n1")
	_, _ = s.ClaimNode(ctx, "tenant-it", "it-outbox-2:n1", n.Version, "w1", 30*time.Second)
	n2, _ := s.GetNode(ctx, "tenant-it", "it-outbox-2:n1")
	// 用旧版本提交
	evt := model.OutboxMessage{ID: "evt-stale-1", EventType: "AgentStepCompleted", AggregateID: "it-outbox-2"}
	ok, err := s.CompleteNodeWithOutbox(ctx, n, "done", evt) // n.Version 是旧版本
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ok {
		t.Error("stale version CompleteNodeWithOutbox should return false")
	}
	// Outbox 不应有 evt-stale-1
	msgs, _ := s.ClaimOutbox(ctx, 10)
	for _, m := range msgs {
		if m.ID == "evt-stale-1" {
			t.Error("outbox should NOT contain stale-evt (tx rolled back)")
		}
	}
	_ = n2 // keep reference
}

// TestIntegration_ToolCallIdempotency 验证工具调用幂等：
// 相同幂等键第二次 ClaimToolCall 返回 false，成功结果可被复用。
// 注意：tool_call 表有外键指向 agent_run/agent_node，需先建立父记录。
func TestIntegration_ToolCallIdempotency(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	// 先建 Run + Plan（提供外键父记录）
	run := &model.Run{ID: "it-tc-1", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertPlan(ctx, "it-tc-1", "tenant-it", model.Plan{Nodes: []model.PlanNode{
		{ID: "it-tc-1:n1", Type: model.NodeTool, Name: "search"},
	}}); err != nil {
		t.Fatal(err)
	}

	// 第一次认领：成功
	ok, err := s.ClaimToolCall(ctx, "tenant-it", "call-1", "it-tc-1", "it-tc-1:n1", "search", "idem-key-1", "query", 0)
	if err != nil || !ok {
		t.Fatalf("first ClaimToolCall should succeed: ok=%v err=%v", ok, err)
	}
	// 第二次认领（相同幂等键）：返回 false
	ok, err = s.ClaimToolCall(ctx, "tenant-it", "call-1", "it-tc-1", "it-tc-1:n1", "search", "idem-key-1", "query", 0)
	if err != nil {
		t.Fatalf("second ClaimToolCall error: %v", err)
	}
	if ok {
		t.Error("second ClaimToolCall with same idempotency key should return false")
	}
	// 完成工具调用
	if err := s.CompleteToolCall(ctx, "tenant-it", "call-1", "search-result"); err != nil {
		t.Fatal(err)
	}
	// 复用：GetToolCall 按 idempotency_key 查询，复用结果
	tc, err := s.GetToolCall(ctx, "tenant-it", "idem-key-1")
	if err != nil {
		t.Fatalf("GetToolCall by idempotency key: %v", err)
	}
	if tc.Status != "SUCCESS" || tc.Output != "search-result" {
		t.Errorf("expected SUCCESS/search-result, got %+v", tc)
	}
	if tc.CallID != "call-1" || tc.TenantID != "tenant-it" {
		t.Errorf("unexpected tool call identity: %+v", tc)
	}
}

// TestIntegration_ToolCallUnknownStatus 验证 P0-4：
// MarkToolCallUnknown 将 RUNNING 置为 UNKNOWN，且仅对 RUNNING 生效。
func TestIntegration_ToolCallUnknownStatus(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-tc-u1", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	_ = s.CreateRun(ctx, run)
	_ = s.InsertPlan(ctx, "it-tc-u1", "tenant-it", model.Plan{Nodes: []model.PlanNode{
		{ID: "it-tc-u1:n1", Type: model.NodeTool, Name: "search"},
	}})

	_, _ = s.ClaimToolCall(ctx, "tenant-it", "call-u1", "it-tc-u1", "it-tc-u1:n1", "search", "idem-u1", "q", 0)
	if err := s.MarkToolCallUnknown(ctx, "tenant-it", "call-u1"); err != nil {
		t.Fatalf("MarkToolCallUnknown: %v", err)
	}
	// GetToolCall 按 idempotency_key 查询
	tc, err := s.GetToolCall(ctx, "tenant-it", "idem-u1")
	if err != nil {
		t.Fatalf("GetToolCall: %v", err)
	}
	if tc.Status != "UNKNOWN" {
		t.Errorf("expected UNKNOWN, got %s", tc.Status)
	}
	// 对已 UNKNOWN 的再标记应不改变状态（UPDATE WHERE status='RUNNING' 不命中）
	_ = s.MarkToolCallUnknown(ctx, "tenant-it", "call-u1")
	tc2, _ := s.GetToolCall(ctx, "tenant-it", "idem-u1")
	if tc2.Status != "UNKNOWN" {
		t.Errorf("re-marking UNKNOWN should be idempotent, got %s", tc2.Status)
	}
}

// TestIntegration_InboxDedup 验证消费端幂等：InboxSeen/MarkInbox 配合去重。
func TestIntegration_InboxDedup(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	seen, err := s.InboxSeen(ctx, "tenant-it", "evt-dedup-1")
	if err != nil || seen {
		t.Fatalf("first InboxSeen should be false, got (%v,%v)", seen, err)
	}
	if err := s.MarkInbox(ctx, "tenant-it", "evt-dedup-1"); err != nil {
		t.Fatal(err)
	}
	seen, err = s.InboxSeen(ctx, "tenant-it", "evt-dedup-1")
	if err != nil || !seen {
		t.Errorf("after MarkInbox, InboxSeen should be true, got (%v,%v)", seen, err)
	}
}

// TestIntegration_DecisionPersistAndReuse 验证 P0-5：
// SaveDecision 持久化后，GetDecision 命中返回原计划，未保存时返回 false。
func TestIntegration_DecisionPersistAndReuse(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-dec-1", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	_ = s.CreateRun(ctx, run)

	plan := model.Plan{Nodes: []model.PlanNode{
		{ID: "it-dec-1:r2:finish", Type: model.NodeLLM, Name: "finish"},
	}}
	// 未保存：GetDecision 返回 false
	_, exists, err := s.GetDecision(ctx, "it-dec-1", "tenant-it", "reflect-1", 2)
	if err != nil || exists {
		t.Fatalf("GetDecision before save should be false, got (%v,%v)", exists, err)
	}
	// 保存
	if err := s.SaveDecision(ctx, "it-dec-1", "tenant-it", "reflect-1", 2, plan); err != nil {
		t.Fatalf("SaveDecision: %v", err)
	}
	// 复用：命中
	got, exists, err := s.GetDecision(ctx, "it-dec-1", "tenant-it", "reflect-1", 2)
	if err != nil || !exists {
		t.Fatalf("GetDecision after save should hit, got (%v,%v)", exists, err)
	}
	if len(got.Nodes) != 1 || got.Nodes[0].ID != "it-dec-1:r2:finish" {
		t.Errorf("reused plan mismatch: %+v", got)
	}
	// 重复保存（幂等）
	if err := s.SaveDecision(ctx, "it-dec-1", "tenant-it", "reflect-1", 2, plan); err != nil {
		t.Errorf("idempotent SaveDecision should not error: %v", err)
	}
}

// TestIntegration_RunConvergence 验证 Run 收敛逻辑：
// 所有节点终态 + 无失败 -> RunComplete=true, RunHasFailure=false。
func TestIntegration_RunConvergence(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-conv-1", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	_ = s.CreateRun(ctx, run)
	_ = s.InsertPlan(ctx, "it-conv-1", "tenant-it", model.Plan{Nodes: []model.PlanNode{
		{ID: "it-conv-1:a", Type: model.NodeLLM, Name: "a"},
		{ID: "it-conv-1:b", Type: model.NodeLLM, Name: "b"},
	}})

	// 未完成：RunComplete=false
	complete, _ := s.RunComplete(ctx, "tenant-it", "it-conv-1")
	if complete {
		t.Error("RunComplete should be false before all nodes done")
	}

	// 完成 a
	_ = s.MarkReady(ctx, "tenant-it", "it-conv-1:a")
	na, _ := s.GetNode(ctx, "tenant-it", "it-conv-1:a")
	_, _ = s.ClaimNode(ctx, "tenant-it", "it-conv-1:a", na.Version, "w", 30*time.Second)
	na2, _ := s.GetNode(ctx, "tenant-it", "it-conv-1:a")
	_, _ = s.CompleteNode(ctx, "tenant-it", "it-conv-1:a", na2.Version, "a-out")
	complete, _ = s.RunComplete(ctx, "tenant-it", "it-conv-1")
	if complete {
		t.Error("RunComplete should be false with only a done")
	}

	// 失败 b
	_ = s.MarkReady(ctx, "tenant-it", "it-conv-1:b")
	nb, _ := s.GetNode(ctx, "tenant-it", "it-conv-1:b")
	_, _ = s.ClaimNode(ctx, "tenant-it", "it-conv-1:b", nb.Version, "w", 30*time.Second)
	nb2, _ := s.GetNode(ctx, "tenant-it", "it-conv-1:b")
	_, _ = s.FailNode(ctx, "tenant-it", "it-conv-1:b", nb2.Version)

	// 全终态：RunComplete=true, RunHasFailure=true
	complete, _ = s.RunComplete(ctx, "tenant-it", "it-conv-1")
	if !complete {
		t.Error("RunComplete should be true when all nodes terminal")
	}
	failed, _ := s.RunHasFailure(ctx, "tenant-it", "it-conv-1")
	if !failed {
		t.Error("RunHasFailure should be true with b failed")
	}
}

// TestIntegration_CancelRunChain 验证取消链：
// CancelRun 在单事务内把 Run 置 CANCEL_REQUESTED 并取消所有 PENDING/READY 节点。
// 这是事务原子性的关键：Run 状态变更与节点取消要么都成功要么都回滚。
func TestIntegration_CancelRunChain(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-cancel-1", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	_ = s.CreateRun(ctx, run)
	_ = s.InsertPlan(ctx, "it-cancel-1", "tenant-it", model.Plan{Nodes: []model.PlanNode{
		{ID: "it-cancel-1:a", Type: model.NodeLLM, Name: "a"},
		{ID: "it-cancel-1:b", Type: model.NodeLLM, Name: "b"},
	}})
	_ = s.MarkReady(ctx, "tenant-it", "it-cancel-1:a")
	_ = s.MarkReady(ctx, "tenant-it", "it-cancel-1:b")

	// CancelRun：单事务内 Run -> CANCEL_REQUESTED + 所有 PENDING/READY 节点 -> CANCELLED
	ok, err := s.CancelRun(ctx, "tenant-it", "it-cancel-1", "user-request", 1)
	if err != nil || !ok {
		t.Fatalf("CancelRun: ok=%v err=%v", ok, err)
	}
	r, _ := s.GetRun(ctx, "tenant-it", "it-cancel-1")
	if r.Status != model.RunCancelRequested {
		t.Errorf("run should be CANCEL_REQUESTED, got %s", r.Status)
	}
	// 事务原子性：两个节点都应已被取消
	for _, nid := range []string{"it-cancel-1:a", "it-cancel-1:b"} {
		n, _ := s.GetNode(ctx, "tenant-it", nid)
		if n.Status != model.NodeCancelled {
			t.Errorf("node %s should be CANCELLED, got %s", nid, n.Status)
		}
	}
}

func TestIntegration_CancelRun_LeavesRunningForWorker(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	run := &model.Run{ID: "it-cancel-2", TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	_ = s.CreateRun(ctx, run)
	_ = s.InsertPlan(ctx, "it-cancel-2", "tenant-it", model.Plan{Nodes: []model.PlanNode{
		{ID: "it-cancel-2:run", Type: model.NodeLLM, Name: "running"},
		{ID: "it-cancel-2:ready", Type: model.NodeLLM, Name: "ready"},
	}})
	// 一个节点 READY，一个被认领为 RUNNING
	_ = s.MarkReady(ctx, "tenant-it", "it-cancel-2:ready")
	_ = s.MarkReady(ctx, "tenant-it", "it-cancel-2:run")
	nRun, _ := s.GetNode(ctx, "tenant-it", "it-cancel-2:run")
	_, _ = s.ClaimNode(ctx, "tenant-it", "it-cancel-2:run", nRun.Version, "w1", 30*time.Second)

	ok, err := s.CancelRun(ctx, "tenant-it", "it-cancel-2", "user", 1)
	if err != nil || !ok {
		t.Fatalf("CancelRun: ok=%v err=%v", ok, err)
	}
	// READY 节点应被批量取消
	nReady, _ := s.GetNode(ctx, "tenant-it", "it-cancel-2:ready")
	if nReady.Status != model.NodeCancelled {
		t.Errorf("ready node should be CANCELLED, got %s", nReady.Status)
	}
	// RUNNING 节点应保持 RUNNING（由 worker 完成后检查 Run 状态来处理）
	nRunning, _ := s.GetNode(ctx, "tenant-it", "it-cancel-2:run")
	if nRunning.Status != model.NodeRunning {
		t.Errorf("running node should stay RUNNING (worker handles cancel), got %s", nRunning.Status)
	}
	ok, err = s.CancelNode(ctx, "tenant-it", "it-cancel-2:run", nRunning.Version)
	if err != nil {
		t.Fatalf("CancelNode error: %v", err)
	}
	if ok {
		t.Error("CancelNode should NOT cancel RUNNING node (only PENDING/READY)")
	}
	// 验证节点仍是 RUNNING
	nStillRunning, _ := s.GetNode(ctx, "tenant-it", "it-cancel-2:run")
	if nStillRunning.Status != model.NodeRunning {
		t.Errorf("RUNNING node should remain RUNNING after CancelNode attempt, got %s", nStillRunning.Status)
	}
	e := model.OutboxMessage{ID: "it-cancel-2:event", EventType: "AgentStepFailed", AggregateID: run.ID, Payload: `{}`}
	ok, err = s.CancelNodeWithOutbox(ctx, nRunning, e)
	if err != nil || !ok {
		t.Fatalf("owned cancellation: ok=%v err=%v", ok, err)
	}
	cancelled := getLeaseTestNode(t, s, nRunning.ID)
	if cancelled.Status != model.NodeCancelled || cancelled.Version != nRunning.Version+1 || cancelled.Attempt != nRunning.Attempt || cancelled.LeaseOwner != "" || cancelled.LeaseUntil != nil || cancelled.FinishedAt.IsZero() {
		t.Fatalf("cancelled node: %+v", cancelled)
	}
	var readyAt sql.NullTime
	if err := s.DB.QueryRowContext(ctx, `SELECT ready_at FROM agent_node WHERE node_id=?`, nRunning.ID).Scan(&readyAt); err != nil || readyAt.Valid {
		t.Fatalf("cancelled node scheduled for retry: readyAt=%v err=%v", readyAt, err)
	}
	assertLeaseTestOutbox(t, s, e.ID, 1)
	var eventType string
	if err := s.DB.QueryRowContext(ctx, `SELECT event_type FROM event_outbox WHERE event_id=?`, e.ID).Scan(&eventType); err != nil || eventType != "AgentStepFailed" {
		t.Fatalf("cancellation event: type=%s err=%v", eventType, err)
	}
	complete, err := s.RunComplete(ctx, run.TenantID, run.ID)
	if err != nil || !complete {
		t.Fatalf("cancel convergence: complete=%v err=%v", complete, err)
	}
	var dlqCount int
	if err := s.DB.QueryRowContext(ctx, `SELECT COUNT(*) FROM agent_dlq WHERE node_id=?`, nRunning.ID).Scan(&dlqCount); err != nil || dlqCount != 0 {
		t.Fatalf("cancellation produced DLQ: count=%d err=%v", dlqCount, err)
	}
}

// TestIntegration_RecordLLMUsage 验证 P0-6 修复：RecordLLMUsage 列/占位符/参数
// 三者严格一致，真实 MySQL 下 INSERT 必须成功且维度可读回。
// 此前 10 个占位符对 9 列，prepare 阶段报错且被调用方静默吞掉，
// llm_usage 永远为空——sqlmock 不校验列/值数量，只有真库能发现这类缺陷。
func TestIntegration_RecordLLMUsage(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	// 外键父记录：llm_usage 无外键，但保持与其他用例一致的建立顺序。
	u := model.LLMUsage{
		ID: "it-usage-1", RunID: "it-usage-run", NodeID: "it-usage-node", TenantID: "tenant-it",
		Model: "gpt-4o", PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150, Cost: 0.000625,
	}
	if err := s.RecordLLMUsage(ctx, u); err != nil {
		t.Fatalf("RecordLLMUsage: %v (列/占位符/参数是否一致?)", err)
	}
	// 汇总读回：token 预算防线依赖该表，落不进去预算就是 0。
	total, err := s.RunTokenUsage(ctx, "tenant-it", "it-usage-run")
	if err != nil {
		t.Fatalf("RunTokenUsage: %v", err)
	}
	if total != 150 {
		t.Errorf("RunTokenUsage = %d, want 150", total)
	}
}

// TestIntegration_PendingStaleTasks 验证 PENDING 补偿扫描的三重限定：
// 只捞 PENDING + 滞留超阈值 + Run 为 RUNNING + 依赖全 SUCCESS 的节点；
// 新节点、依赖未就绪节点、CANCELLED Run 的节点都不该被补投。
func TestIntegration_PendingStaleTasks(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()

	const runID, tenant = "it-pend-1", "tenant-it"
	run := &model.Run{ID: runID, TenantID: tenant, Status: model.RunRunning, Version: 1}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	// 拓扑：a (根) ──> b ；x (根) 属于另一个已取消的 Run
	plan := model.Plan{Nodes: []model.PlanNode{
		{ID: runID + ":a", Type: model.NodeLLM, Name: "a"},
		{ID: runID + ":b", Type: model.NodeLLM, Name: "b", DependsOn: []string{runID + ":a"}},
	}}
	if err := s.InsertPlan(ctx, runID, tenant, plan); err != nil {
		t.Fatal(err)
	}
	// 新插入的 PENDING 节点（created_at 刚刚）不应被补投。
	stale, err := s.PendingStaleTasks(ctx, time.Minute, 10)
	if err != nil {
		t.Fatalf("PendingStaleTasks: %v", err)
	}
	if len(stale) != 0 {
		t.Errorf("freshly inserted PENDING nodes must not be scanned, got %+v", stale)
	}
	// 把 a 的 created_at 回拨到阈值之外（模拟 MarkReady 失败遗留的调度缺口）。
	if _, err := s.DB.ExecContext(ctx,
		`UPDATE agent_node SET created_at = DATE_SUB(NOW(6), INTERVAL 2 MINUTE) WHERE node_id=? AND tenant_id=?`,
		runID+":a", tenant); err != nil {
		t.Fatal(err)
	}
	// b 依赖未就绪，即使回拨也不应被补投；a 依赖已满足，应被补投。
	stale, err = s.PendingStaleTasks(ctx, time.Minute, 10)
	if err != nil {
		t.Fatalf("PendingStaleTasks: %v", err)
	}
	if len(stale) != 1 || stale[0].NodeID != runID+":a" {
		t.Fatalf("stale tasks = %+v, want only %s:a (b deps not ready)", stale, runID)
	}
	// 补投：MarkReady 后节点不再被重复扫到。
	if err := s.MarkReady(ctx, tenant, runID+":a"); err != nil {
		t.Fatal(err)
	}
	stale, _ = s.PendingStaleTasks(ctx, time.Minute, 10)
	if len(stale) != 0 {
		t.Errorf("after MarkReady, stale scan should be empty, got %+v", stale)
	}
	// 已取消 Run 的 PENDING 节点不补投：构造第二个 Run 并回拨节点。
	cRun := &model.Run{ID: "it-pend-2", TenantID: tenant, Status: model.RunCancelled, Version: 1}
	_ = s.CreateRun(ctx, cRun)
	_ = s.InsertPlan(ctx, "it-pend-2", tenant, model.Plan{Nodes: []model.PlanNode{
		{ID: "it-pend-2:x", Type: model.NodeLLM, Name: "x"},
	}})
	if _, err := s.DB.ExecContext(ctx,
		`UPDATE agent_node SET created_at = DATE_SUB(NOW(6), INTERVAL 2 MINUTE) WHERE node_id='it-pend-2:x'`); err != nil {
		t.Fatal(err)
	}
	stale, _ = s.PendingStaleTasks(ctx, time.Minute, 10)
	for _, tk := range stale {
		if tk.RunID == "it-pend-2" {
			t.Errorf("nodes of non-RUNNING runs must not be scanned, got %+v", tk)
		}
	}
}

func insertLeaseTestNode(t *testing.T, s *MySQL, runID string, claim bool) *model.Node {
	t.Helper()
	ctx := context.Background()
	run := &model.Run{ID: runID, TenantID: "tenant-it", Status: model.RunRunning, Version: 1}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatal(err)
	}
	if err := s.InsertPlan(ctx, runID, run.TenantID, model.Plan{Nodes: []model.PlanNode{
		{ID: runID + ":n1", Type: model.NodeLLM, Name: "step"},
	}}); err != nil {
		t.Fatal(err)
	}
	n := getLeaseTestNode(t, s, runID+":n1")
	if claim {
		ok, err := s.ClaimNode(ctx, n.TenantID, n.ID, n.Version, "worker-1", time.Minute)
		if err != nil || !ok {
			t.Fatalf("claim: ok=%v err=%v", ok, err)
		}
		n = getLeaseTestNode(t, s, n.ID)
	}
	return n
}

func getLeaseTestNode(t *testing.T, s *MySQL, nodeID string) *model.Node {
	t.Helper()
	n, err := s.GetNode(context.Background(), "tenant-it", nodeID)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func assertLeaseTestOutbox(t *testing.T, s *MySQL, eventID string, want int) {
	t.Helper()
	var count int
	if err := s.DB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM event_outbox WHERE event_id=?`, eventID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != want {
		t.Fatalf("outbox %s: count=%d, want %d", eventID, count, want)
	}
}

func leaseTestWrite(s *MySQL, op string, n *model.Node, e model.OutboxMessage) (bool, error) {
	ctx := context.Background()
	switch op {
	case "renew":
		return s.RenewLease(ctx, n.TenantID, n.ID, n.LeaseOwner, n.Version, 2*time.Minute)
	case "complete":
		return s.CompleteNodeWithOutbox(ctx, n, "done", e)
	case "fail":
		return s.FailNodeWithOutbox(ctx, n, e)
	case "retry":
		return s.RetryNode(ctx, n.TenantID, n.ID, n.LeaseOwner, n.Version, time.Now().Add(time.Minute))
	case "cancel":
		return s.CancelNodeWithOutbox(ctx, n, e)
	default:
		panic("unknown lease operation: " + op)
	}
}

func TestIntegration_ClaimNode_DoubleClaim(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	n := insertLeaseTestNode(t, s, "it-double-claim", false)
	start := make(chan struct{})
	type result struct {
		owner string
		ok    bool
		err   error
	}
	results := make(chan result, 2)
	for _, owner := range []string{"worker-1", "worker-2"} {
		go func(owner string) {
			<-start
			ok, err := s.ClaimNode(context.Background(), n.TenantID, n.ID, n.Version, owner, time.Minute)
			results <- result{owner: owner, ok: ok, err: err}
		}(owner)
	}
	close(start)
	var winners int
	var winner string
	for i := 0; i < 2; i++ {
		r := <-results
		if r.err != nil {
			t.Errorf("claim by %s: %v", r.owner, r.err)
		}
		if r.ok {
			winners++
			winner = r.owner
		}
	}
	got := getLeaseTestNode(t, s, n.ID)
	if winners != 1 || got.Version != n.Version+1 || got.Status != model.NodeRunning || got.LeaseOwner != winner {
		t.Fatalf("winners=%d, node=%+v", winners, got)
	}
	ok, err := s.ClaimNode(context.Background(), got.TenantID, got.ID, got.Version, "worker-3", time.Minute)
	if err != nil || ok {
		t.Fatalf("double claim with current version: ok=%v err=%v", ok, err)
	}
}

func TestIntegration_LeaseMicroseconds(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()
	n := insertLeaseTestNode(t, s, "it-microsecond", false)
	ok, err := s.ClaimNode(ctx, n.TenantID, n.ID, n.Version, "worker-1", 900123*time.Microsecond)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	var micros int64
	if err := s.DB.QueryRowContext(ctx, `SELECT TIMESTAMPDIFF(MICROSECOND,started_at,lease_until) FROM agent_node WHERE node_id=?`, n.ID).Scan(&micros); err != nil {
		t.Fatal(err)
	}
	if micros != 900123 {
		t.Fatalf("claim lease=%d microseconds, want 900123", micros)
	}
	ok, err = s.RenewLease(ctx, n.TenantID, n.ID, "worker-1", n.Version+1, 750123*time.Microsecond)
	if err != nil || !ok {
		t.Fatalf("renew: ok=%v err=%v", ok, err)
	}
	if err := s.DB.QueryRowContext(ctx, `SELECT TIMESTAMPDIFF(MICROSECOND,NOW(6),lease_until) FROM agent_node WHERE node_id=?`, n.ID).Scan(&micros); err != nil {
		t.Fatal(err)
	}
	if micros <= 0 || micros > 750123 {
		t.Fatalf("remaining lease=%d microseconds, want (0,750123]", micros)
	}
	if got := getLeaseTestNode(t, s, n.ID); got.Version != n.Version+1 {
		t.Fatalf("renew changed claim version: %+v", got)
	}
}

func TestIntegration_LeaseWriteGuards(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()
	for _, op := range []string{"renew", "complete", "fail", "retry", "cancel"} {
		for _, fault := range []string{"wrong_owner", "empty_owner", "stale_version", "wrong_tenant", "expired", "missing_lease", "PENDING", "READY", "WAITING_HUMAN", "SUCCESS", "FAILED", "CANCELLED"} {
			t.Run(op+"/"+fault, func(t *testing.T) {
				n := insertLeaseTestNode(t, s, "it-guard-"+op+"-"+fault, true)
				request := *n
				switch fault {
				case "wrong_owner":
					request.LeaseOwner = "worker-other"
				case "empty_owner":
					request.LeaseOwner = ""
				case "stale_version":
					request.Version--
				case "wrong_tenant":
					request.TenantID = "tenant-other"
				case "expired":
					_, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE node_id=?`, n.ID)
					if err != nil {
						t.Fatal(err)
					}
				case "missing_lease":
					if _, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET lease_until=NULL WHERE node_id=?`, n.ID); err != nil {
						t.Fatal(err)
					}
				default:
					if _, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=? WHERE node_id=?`, fault, n.ID); err != nil {
						t.Fatal(err)
					}
				}
				before := getLeaseTestNode(t, s, n.ID)
				e := model.OutboxMessage{ID: n.ID + ":event", EventType: "AgentStepFailed", AggregateID: n.RunID, Payload: `{}`}
				ok, err := leaseTestWrite(s, op, &request, e)
				if err != nil || ok {
					t.Fatalf("rejected write: ok=%v err=%v", ok, err)
				}
				if after := getLeaseTestNode(t, s, n.ID); !reflect.DeepEqual(before, after) {
					t.Fatalf("rejected write changed node: before=%+v after=%+v", before, after)
				}
				assertLeaseTestOutbox(t, s, e.ID, 0)
			})
		}
	}
}

func TestIntegration_LeaseTakeover(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()
	for _, op := range []string{"renew", "complete", "fail", "retry", "cancel"} {
		t.Run(op, func(t *testing.T) {
			old := insertLeaseTestNode(t, s, "it-takeover-"+op, true)
			if _, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET lease_until=DATE_SUB(NOW(6),INTERVAL 1 SECOND) WHERE node_id=?`, old.ID); err != nil {
				t.Fatal(err)
			}
			tasks, err := s.RecoverExpired(ctx, 10)
			if err != nil || len(tasks) != 1 || tasks[0].NodeID != old.ID {
				t.Fatalf("recover: tasks=%+v err=%v", tasks, err)
			}
			ready := getLeaseTestNode(t, s, old.ID)
			if ready.Status != model.NodeReady || ready.Version != old.Version+1 || ready.LeaseOwner != "" || ready.LeaseUntil != nil {
				t.Fatalf("recovered node: %+v", ready)
			}
			ok, err := s.ClaimNode(ctx, ready.TenantID, ready.ID, ready.Version, "worker-2", time.Minute)
			if err != nil || !ok {
				t.Fatalf("takeover claim: ok=%v err=%v", ok, err)
			}
			current := getLeaseTestNode(t, s, old.ID)
			if current.Version != ready.Version+1 || current.LeaseOwner != "worker-2" {
				t.Fatalf("takeover node: %+v", current)
			}
			e := model.OutboxMessage{ID: old.ID + ":event", EventType: "AgentStepFailed", AggregateID: old.RunID, Payload: `{}`}
			for _, mismatch := range []string{"old_lease", "old_owner", "old_version"} {
				request := *old
				if mismatch == "old_owner" {
					request.Version = current.Version
				}
				if mismatch == "old_version" {
					request.LeaseOwner = current.LeaseOwner
				}
				ok, err := leaseTestWrite(s, op, &request, e)
				if err != nil || ok {
					t.Fatalf("%s write: ok=%v err=%v", mismatch, ok, err)
				}
				if after := getLeaseTestNode(t, s, old.ID); !reflect.DeepEqual(current, after) {
					t.Fatalf("old worker changed takeover node: %+v", after)
				}
				assertLeaseTestOutbox(t, s, e.ID, 0)
			}
			ok, err = leaseTestWrite(s, op, current, e)
			if err != nil || !ok {
				t.Fatalf("current worker write: ok=%v err=%v", ok, err)
			}
			after := getLeaseTestNode(t, s, old.ID)
			if op == "renew" {
				if after.Status != model.NodeRunning || after.Version != current.Version || after.LeaseOwner != current.LeaseOwner || after.LeaseUntil == nil || !after.LeaseUntil.After(*current.LeaseUntil) {
					t.Fatalf("renewed node: %+v", after)
				}
				assertLeaseTestOutbox(t, s, e.ID, 0)
				return
			}
			wantStatus := map[string]model.NodeStatus{"complete": model.NodeSuccess, "fail": model.NodeFailed, "retry": model.NodeReady, "cancel": model.NodeCancelled}[op]
			if after.Status != wantStatus || after.Version != current.Version+1 || after.LeaseOwner != "" || after.LeaseUntil != nil {
				t.Fatalf("committed node: %+v", after)
			}
			if op == "retry" {
				if after.Attempt != current.Attempt+1 {
					t.Fatalf("retry attempt=%d, want %d", after.Attempt, current.Attempt+1)
				}
				assertLeaseTestOutbox(t, s, e.ID, 0)
			} else {
				if after.Attempt != current.Attempt || after.FinishedAt.IsZero() {
					t.Fatalf("terminal node: %+v", after)
				}
				assertLeaseTestOutbox(t, s, e.ID, 1)
			}
		})
	}
}

func TestIntegration_PolicyDenyGuards(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()
	future, past := time.Now().Add(time.Hour), time.Now().Add(-time.Hour)
	for i, tc := range []struct {
		name   string
		status model.NodeStatus
		owner  any
		lease  any
		allow  bool
	}{
		{name: "pending_null", status: model.NodePending, allow: true},
		{name: "pending_empty", status: model.NodePending, owner: "", allow: true},
		{name: "ready_null", status: model.NodeReady, allow: true},
		{name: "ready_empty", status: model.NodeReady, owner: "", allow: true},
		{name: "pending_owned", status: model.NodePending, owner: "worker-1"},
		{name: "ready_owned", status: model.NodeReady, owner: "worker-1"},
		{name: "pending_leased", status: model.NodePending, lease: future},
		{name: "ready_expired", status: model.NodeReady, lease: past},
		{name: "running", status: model.NodeRunning, owner: "worker-1", lease: future},
		{name: "running_unowned", status: model.NodeRunning},
		{name: "waiting_human", status: model.NodeWaitingHuman},
		{name: "success", status: model.NodeSuccess},
		{name: "failed", status: model.NodeFailed},
		{name: "cancelled", status: model.NodeCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			n := insertLeaseTestNode(t, s, fmt.Sprintf("it-deny-%d", i), false)
			if _, err := s.DB.ExecContext(ctx, `UPDATE agent_node SET status=?,lease_owner=?,lease_until=? WHERE node_id=?`, tc.status, tc.owner, tc.lease, n.ID); err != nil {
				t.Fatal(err)
			}
			before := getLeaseTestNode(t, s, n.ID)
			e := model.OutboxMessage{ID: n.ID + ":event", EventType: "AgentStepFailed", AggregateID: n.RunID, Payload: `{}`}
			ok, err := s.FailNodeWithOutbox(ctx, n, e)
			if err != nil || ok != tc.allow {
				t.Fatalf("policy denial: ok=%v err=%v, want %v", ok, err, tc.allow)
			}
			after := getLeaseTestNode(t, s, n.ID)
			if tc.allow {
				if after.Status != model.NodeFailed || after.Version != before.Version+1 || after.FinishedAt.IsZero() {
					t.Fatalf("policy denial node: %+v", after)
				}
				assertLeaseTestOutbox(t, s, e.ID, 1)
			} else {
				if !reflect.DeepEqual(before, after) {
					t.Fatalf("policy denial changed protected node: before=%+v after=%+v", before, after)
				}
				assertLeaseTestOutbox(t, s, e.ID, 0)
			}
		})
	}
}

func TestIntegration_LeaseOutboxFailureRollsBack(t *testing.T) {
	s := newIntegrationStore(t)
	defer s.Close()
	ctx := context.Background()
	for _, op := range []string{"complete", "fail", "cancel"} {
		t.Run(op, func(t *testing.T) {
			n := insertLeaseTestNode(t, s, "it-rollback-"+op, true)
			e := model.OutboxMessage{ID: n.ID + ":event", EventType: "AgentStepFailed", AggregateID: n.RunID, Payload: `{}`}
			if _, err := s.DB.ExecContext(ctx, `INSERT INTO event_outbox(event_id,event_type,aggregate_id,payload) VALUES(?,?,?,?)`, e.ID, e.EventType, e.AggregateID, e.Payload); err != nil {
				t.Fatal(err)
			}
			ok, err := leaseTestWrite(s, op, n, e)
			if err == nil || ok {
				t.Fatalf("duplicate outbox insert: ok=%v err=%v, want failure", ok, err)
			}
			if after := getLeaseTestNode(t, s, n.ID); !reflect.DeepEqual(n, after) {
				t.Fatalf("outbox failure did not roll back node: before=%+v after=%+v", n, after)
			}
			assertLeaseTestOutbox(t, s, e.ID, 1)
			e.ID += "-retry"
			ok, err = leaseTestWrite(s, op, n, e)
			if err != nil || !ok {
				t.Fatalf("retry transaction: ok=%v err=%v", ok, err)
			}
			assertLeaseTestOutbox(t, s, e.ID, 1)
		})
	}
}
