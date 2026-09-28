package approval

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"agent-runtime/internal/model"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"

	"github.com/DATA-DOG/go-sqlmock"
)

// ---- 测试替身 ----

// fakeRunStore 只驱动 Runtime.Resume/Reject 走到的状态机路径
// （GetRun + ResumeRun/RejectRun + Queue 投递），其余 Store 方法为空实现。
// 状态转移语义与 store.MySQL 对齐：Resume 重新武装节点并返回待投递任务；
// Reject 切回 RUNNING、不返回任何任务（节点已 FAILED）。
type fakeRunStore struct {
	run      *model.Run
	resumed  bool
	rejected bool
}

func (s *fakeRunStore) CreateRun(context.Context, *model.Run) error { return nil }
func (s *fakeRunStore) GetRun(_ context.Context, _, _ string) (*model.Run, error) {
	cp := *s.run
	return &cp, nil
}
func (s *fakeRunStore) UpdateRunCAS(context.Context, string, string, int64, model.RunStatus, string, string) (bool, error) {
	return false, nil
}
func (s *fakeRunStore) InsertPlan(context.Context, string, string, model.Plan) error { return nil }
func (s *fakeRunStore) MarkReady(context.Context, string, string) error              { return nil }
func (s *fakeRunStore) Children(context.Context, string, string) ([]model.Task, error) {
	return nil, nil
}
func (s *fakeRunStore) DependenciesReady(context.Context, string, string) (bool, error) {
	return true, nil
}
func (s *fakeRunStore) RunComplete(context.Context, string, string) (bool, error) { return true, nil }
func (s *fakeRunStore) RunHasFailure(context.Context, string, string) (bool, error) {
	return false, nil
}
func (s *fakeRunStore) CompletedNodes(context.Context, string, string) ([]model.Node, error) {
	return nil, nil
}
func (s *fakeRunStore) CountNodes(context.Context, string, string) (int, error) { return 0, nil }
func (s *fakeRunStore) RunTokenUsage(context.Context, string, string) (int, error) {
	return 0, nil
}
func (s *fakeRunStore) InboxSeen(context.Context, string, string) (bool, error) { return false, nil }
func (s *fakeRunStore) MarkInbox(context.Context, string, string) error         { return nil }
func (s *fakeRunStore) InterruptRun(context.Context, string, string, string, string, int64) (bool, error) {
	return false, nil
}
func (s *fakeRunStore) ResumeRun(_ context.Context, _, runID, decision string, version int64) (bool, []model.Task, error) {
	if s.run.Version != version {
		return false, nil, nil
	}
	s.resumed = true
	s.run.Status = model.RunRunning
	s.run.Output = decision
	s.run.Version++
	return true, []model.Task{{RunID: runID, NodeID: "n1", TenantID: "t1"}}, nil
}
func (s *fakeRunStore) RejectRun(_ context.Context, _, _, decision string, version int64) (bool, error) {
	if s.run.Version != version {
		return false, nil
	}
	s.rejected = true
	s.run.Status = model.RunRunning
	s.run.Output = decision
	s.run.Version++
	return true, nil
}

type fakeQueue struct{ enqueued []model.Task }

func (q *fakeQueue) Enqueue(_ context.Context, t model.Task) error {
	q.enqueued = append(q.enqueued, t)
	return nil
}

func newMockStore(t *testing.T) (*store.MySQL, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new: %v", err)
	}
	return &store.MySQL{DB: db}, mock, func() { _ = db.Close() }
}

func interruptRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"interrupt_id", "run_id", "tenant_id", "node_id", "reason", "status", "decision", "created_at", "resolved_at",
	})
}

func waitingInterrupt(id, runID, nodeID string) *sqlmock.Rows {
	return interruptRows().AddRow(id, runID, "t1", nodeID, "l5_default: human approval required", "WAITING", "", time.Now(), nil)
}

func nodeRows() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"node_id", "run_id", "tenant_id", "parent_node_id", "type", "name", "input", "output",
		"status", "attempt", "version", "lease_owner", "lease_until", "planning_round", "created_at", "started_at", "finished_at",
	}).AddRow("n1", "r1", "t1", "", "tool", "shell", "kubectl get pods", "",
		"READY", 0, 1, "", nil, 0, time.Now(), nil, nil)
}

// newTestServer 组装被测服务：sqlmock 支撑 Store 面，fakeRunStore 驱动
// Runtime 状态机，fakeQueue 观测 approve 后的重新投递。
func newTestServer(t *testing.T, learn bool) (*Server, *fakeRunStore, *fakeQueue, sqlmock.Sqlmock, func()) {
	t.Helper()
	st, mock, cleanup := newMockStore(t)
	fs := &fakeRunStore{run: &model.Run{ID: "r1", TenantID: "t1", Status: model.RunWaitingHuman, Version: 3, UpdatedAt: time.Now()}}
	q := &fakeQueue{}
	srv := &Server{Store: st, Runtime: &runtime.Runtime{Store: fs, Queue: q}, LearnEnabled: learn}
	return srv, fs, q, mock, cleanup
}

func doJSON(t *testing.T, h http.Handler, method, path, body string, headers map[string]string) (int, map[string]any) {
	t.Helper()
	var rd *bytes.Reader
	if body != "" {
		rd = bytes.NewReader([]byte(body))
	} else {
		rd = bytes.NewReader(nil)
	}
	req := httptest.NewRequest(method, path, rd)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatalf("response is not JSON: %q", rec.Body.String())
	}
	return rec.Code, out
}

// ---- 列表 ----

// TestListApprovals_EnrichesNodeSnapshot 列表必须补齐节点快照（工具名/
// 入参）——那是审批人判定的必要输入；节点已消失（Run 级中断，node_id
// 为空）的条目不得让整个列表 500，留空快照照常返回。
func TestListApprovals_EnrichesNodeSnapshot(t *testing.T) {
	srv, _, _, mock, cleanup := newTestServer(t, false)
	defer cleanup()
	mock.ExpectQuery("FROM run_interrupt WHERE tenant_id=\\? AND status='WAITING'").
		WithArgs("t1", 100).
		WillReturnRows(waitingInterrupt("in-1", "r1", "n1").
			AddRow("in-2", "r2", "t1", "", "run level interrupt", "WAITING", "", time.Now(), nil))
	mock.ExpectQuery("FROM agent_node WHERE node_id=\\?").
		WithArgs("n1", "t1").
		WillReturnRows(nodeRows())

	code, out := doJSON(t, srv.Handler(), http.MethodGet, "/v1/tenants/t1/approvals", "", nil)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	items, _ := out["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d: %v", len(items), out)
	}
	first, _ := items[0].(map[string]any)
	if first["tool"] != "shell" || first["input"] != "kubectl get pods" {
		t.Fatalf("node snapshot missing on first item: %v", first)
	}
	// 空 node_id 不触发 GetNode（去补齐也是空查询），快照留空照常返回。
	second, _ := items[1].(map[string]any)
	if second["tool"] != nil && second["tool"] != "" {
		t.Fatalf("run-level interrupt must have empty snapshot, got %v", second)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("expectations not met: %v", err)
	}
}

// ---- 裁决闭环 ----

// TestResolveApproveOnce_ResumesAndRequeues "approve（一次）"的完整闭环：
// Resume 重新武装被挂起节点并把任务投回队列——不投递的话 Run 回到
// RUNNING 却没有节点在跑，人工批了流程却不动。once 语义不写任何规则。
func TestResolveApproveOnce_ResumesAndRequeues(t *testing.T) {
	srv, fs, q, mock, cleanup := newTestServer(t, false)
	defer cleanup()
	mock.ExpectQuery("FROM run_interrupt WHERE interrupt_id=\\?").
		WithArgs("in-1", "t1").
		WillReturnRows(waitingInterrupt("in-1", "r1", "n1"))

	code, out := doJSON(t, srv.Handler(), http.MethodPost,
		"/v1/tenants/t1/approvals/in-1/resolve", `{"action":"approve"}`, nil)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if out["status"] != "approved" || out["scope"] != "once" {
		t.Fatalf("unexpected response: %v", out)
	}
	if out["rule_id"] != "" {
		t.Fatalf("once scope must not learn a rule, got %v", out["rule_id"])
	}
	if !fs.resumed || fs.rejected {
		t.Fatalf("resumed=%v rejected=%v", fs.resumed, fs.rejected)
	}
	if len(q.enqueued) != 1 {
		t.Fatalf("approved node must be re-enqueued exactly once, got %d", len(q.enqueued))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("no rule must be saved for once scope: %v", err)
	}
}

// TestResolveApproveRunLearnsRule "always allow（本 Run）"必须先写回学习
// 规则再 Resume：写回幂等（重复 resolve 重放同一请求，规则查重跳过、
// 状态 CAS 拒绝二套裁决）；反过来先 Resume 再写回，失败重试会留下
// "放行了却没学"的半套状态。规则归因（source/run_id/learned_by）是
// 审计面：授权行为必须可归因。
func TestResolveApproveRunLearnsRule(t *testing.T) {
	srv, fs, _, mock, cleanup := newTestServer(t, true)
	defer cleanup()
	mock.ExpectQuery("FROM run_interrupt WHERE interrupt_id=\\?").
		WithArgs("in-1", "t1").
		WillReturnRows(waitingInterrupt("in-1", "r1", "n1"))
	// buildLearnRule 两次查节点：一次取 pattern 缺省值（完整入参），一次取工具名。
	mock.ExpectQuery("FROM agent_node WHERE node_id=\\?").WithArgs("n1", "t1").WillReturnRows(nodeRows())
	mock.ExpectQuery("FROM agent_node WHERE node_id=\\?").WithArgs("n1", "t1").WillReturnRows(nodeRows())
	// 查重（run 级作用域）→ 未命中 → INSERT 带本 Run 的 run_id。
	mock.ExpectQuery("FROM permission_rule WHERE tenant_id=\\? AND run_id=\\?").
		WithArgs("t1", "r1", "shell", "kubectl get pods", "allow", "run").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("INSERT INTO permission_rule").
		WithArgs(sqlmock.AnyArg(), "t1", "r1", "shell", "kubectl get pods", "allow", "run", "ops@example.com").
		WillReturnResult(sqlmock.NewResult(0, 1))

	code, out := doJSON(t, srv.Handler(), http.MethodPost,
		"/v1/tenants/t1/approvals/in-1/resolve",
		`{"action":"approve","scope":"run"}`, map[string]string{"X-Approver": "ops@example.com"})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if out["rule_id"] == "" {
		t.Fatalf("run scope must return the learned rule id: %v", out)
	}
	if !fs.resumed {
		t.Fatal("approve must resume the run after learning")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("rule must be saved run-scoped before resume: %v", err)
	}
}

// TestResolveDeny_RejectsWithoutRequeue 否决绝不能重新投递：被挂起节点
// 已置 FAILED，投递等于调度器去捞一个已失败的节点。
func TestResolveDeny_RejectsWithoutRequeue(t *testing.T) {
	srv, fs, q, mock, cleanup := newTestServer(t, false)
	defer cleanup()
	mock.ExpectQuery("FROM run_interrupt WHERE interrupt_id=\\?").
		WithArgs("in-1", "t1").
		WillReturnRows(waitingInterrupt("in-1", "r1", "n1"))

	code, out := doJSON(t, srv.Handler(), http.MethodPost,
		"/v1/tenants/t1/approvals/in-1/resolve", `{"action":"deny"}`, nil)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if out["status"] != "denied" {
		t.Fatalf("unexpected response: %v", out)
	}
	if !fs.rejected || fs.resumed {
		t.Fatalf("rejected=%v resumed=%v", fs.rejected, fs.resumed)
	}
	if len(q.enqueued) != 0 {
		t.Fatalf("deny must not re-enqueue, got %+v", q.enqueued)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("plain deny must not write rules: %v", err)
	}
}

// TestResolveDenyLearn_WritesTenantDenyRule deny 的可选写回：source=learned
// 的 deny 规则是租户级的（run_id 落 NULL）——否决的语义是"这类操作
// 不该发生"，与具体 Run 无关。
func TestResolveDenyLearn_WritesTenantDenyRule(t *testing.T) {
	srv, fs, _, mock, cleanup := newTestServer(t, true)
	defer cleanup()
	mock.ExpectQuery("FROM run_interrupt WHERE interrupt_id=\\?").
		WithArgs("in-1", "t1").
		WillReturnRows(waitingInterrupt("in-1", "r1", "n1"))
	mock.ExpectQuery("FROM agent_node WHERE node_id=\\?").WithArgs("n1", "t1").WillReturnRows(nodeRows())
	mock.ExpectQuery("FROM agent_node WHERE node_id=\\?").WithArgs("n1", "t1").WillReturnRows(nodeRows())
	// 查重（租户级作用域，run_id IS NULL）→ INSERT 的 run_id 参数为 nil。
	mock.ExpectQuery("FROM permission_rule WHERE tenant_id=\\? AND run_id IS NULL").
		WithArgs("t1", "shell", "kubectl get pods", "deny", "learned").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	mock.ExpectExec("INSERT INTO permission_rule").
		WithArgs(sqlmock.AnyArg(), "t1", nil, "shell", "kubectl get pods", "deny", "learned", "ops@example.com").
		WillReturnResult(sqlmock.NewResult(0, 1))

	code, out := doJSON(t, srv.Handler(), http.MethodPost,
		"/v1/tenants/t1/approvals/in-1/resolve",
		`{"action":"deny","learn":true}`, map[string]string{"X-Approver": "ops@example.com"})
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	if !fs.rejected {
		t.Fatal("deny+learn must still reject the run")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("learned deny must be tenant-scoped: %v", err)
	}
}

// ---- 入口校验与状态机守卫 ----

// TestResolveValidation 学习入口的三道守卫。共同的底线：校验失败的
// 裁决绝不能落到 Resume/Reject——半套状态（放行了却没学、学错了作用域）
// 比报错难排查得多。
func TestResolveValidation(t *testing.T) {
	t.Run("learn scope requires PERMISSION_LEARN_ENABLED", func(t *testing.T) {
		srv, fs, _, mock, cleanup := newTestServer(t, false)
		defer cleanup()
		mock.ExpectQuery("FROM run_interrupt WHERE interrupt_id=\\?").
			WithArgs("in-1", "t1").
			WillReturnRows(waitingInterrupt("in-1", "r1", "n1"))
		code, out := doJSON(t, srv.Handler(), http.MethodPost,
			"/v1/tenants/t1/approvals/in-1/resolve",
			`{"action":"approve","scope":"tenant"}`, map[string]string{"X-Approver": "ops@example.com"})
		if code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %v", code, out)
		}
		if fs.resumed || fs.rejected {
			t.Fatal("rejected resolution must not touch the run")
		}
	})
	t.Run("learn scope requires X-Approver", func(t *testing.T) {
		srv, fs, _, mock, cleanup := newTestServer(t, true)
		defer cleanup()
		mock.ExpectQuery("FROM run_interrupt WHERE interrupt_id=\\?").
			WithArgs("in-1", "t1").
			WillReturnRows(waitingInterrupt("in-1", "r1", "n1"))
		code, _ := doJSON(t, srv.Handler(), http.MethodPost,
			"/v1/tenants/t1/approvals/in-1/resolve", `{"action":"approve","scope":"run"}`, nil)
		if code != http.StatusBadRequest {
			t.Fatalf("expected 400 without approver, got %d", code)
		}
		if fs.resumed || fs.rejected {
			t.Fatal("anonymous learning must not touch the run")
		}
	})
	t.Run("invalid action rejected", func(t *testing.T) {
		srv, fs, _, mock, cleanup := newTestServer(t, true)
		defer cleanup()
		mock.ExpectQuery("FROM run_interrupt WHERE interrupt_id=\\?").
			WithArgs("in-1", "t1").
			WillReturnRows(waitingInterrupt("in-1", "r1", "n1"))
		code, _ := doJSON(t, srv.Handler(), http.MethodPost,
			"/v1/tenants/t1/approvals/in-1/resolve", `{"action":"maybe"}`, nil)
		if code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", code)
		}
		if fs.resumed || fs.rejected {
			t.Fatal("invalid action must not touch the run")
		}
	})
}

// TestResolveConflictOnResolvedInterrupt 非 WAITING 状态返回 409：幂等入口
// 不重复裁决。已 RESOLVED 的记录再点一次"approve"，若返回 200 会让
// 调用方以为第二次裁决也生效了（实际 Resume 的 CAS 会拒绝，但那是
// 500 噪音——语义错误应该在入口就拦下）。
func TestResolveConflictOnResolvedInterrupt(t *testing.T) {
	srv, fs, _, mock, cleanup := newTestServer(t, false)
	defer cleanup()
	mock.ExpectQuery("FROM run_interrupt WHERE interrupt_id=\\?").
		WithArgs("in-1", "t1").
		WillReturnRows(interruptRows().AddRow("in-1", "r1", "t1", "n1", "done", "RESOLVED", "approved by ops", time.Now(), time.Now()))

	code, out := doJSON(t, srv.Handler(), http.MethodPost,
		"/v1/tenants/t1/approvals/in-1/resolve", `{"action":"approve"}`, nil)
	if code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %v", code, out)
	}
	if fs.resumed || fs.rejected {
		t.Fatal("resolved interrupt must not be re-adjudicated")
	}
}

// ---- 规则配置面 ----

// TestListRules 配置面列表含已撤销规则：撤销动作留在账上，
// "何时收回授权"与"何时授予"同样是审计问题。
func TestListRules(t *testing.T) {
	srv, _, _, mock, cleanup := newTestServer(t, false)
	defer cleanup()
	mock.ExpectQuery("FROM permission_rule WHERE tenant_id=\\?").
		WithArgs("t1", 200).
		WillReturnRows(sqlmock.NewRows([]string{
			"rule_id", "tenant_id", "run_id", "tool", "pattern", "effect", "source", "learned_by", "learned_at", "revoked_at",
		}).AddRow("rule-1", "t1", "r1", "shell", "kubectl get pods", "allow", "run", "ops@example.com", time.Now(), nil).
			AddRow("rule-2", "t1", "", "shell", "rm *", "deny", "learned", "sec@example.com", time.Now(), time.Now()))

	code, out := doJSON(t, srv.Handler(), http.MethodGet, "/v1/tenants/t1/permission-rules", "", nil)
	if code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %v", code, out)
	}
	items, _ := out["items"].([]any)
	if len(items) != 2 {
		t.Fatalf("expected active + revoked rules, got %d: %v", len(items), out)
	}
}

// TestRevokeRule 幂等语义：已撤销的再撤销返回 404 而不是假的
// "撤销成功"——配置面分不清"刚撤掉"和"早就撤了"，会掩盖重复操作。
func TestRevokeRule(t *testing.T) {
	t.Run("revokes active rule", func(t *testing.T) {
		srv, _, _, mock, cleanup := newTestServer(t, false)
		defer cleanup()
		mock.ExpectExec("UPDATE permission_rule SET revoked_at").
			WithArgs("rule-1", "t1").
			WillReturnResult(sqlmock.NewResult(0, 1))
		code, out := doJSON(t, srv.Handler(), http.MethodDelete, "/v1/tenants/t1/permission-rules/rule-1", "", nil)
		if code != http.StatusOK || out["revoked"] != true {
			t.Fatalf("expected 200 {revoked:true}, got %d: %v", code, out)
		}
	})
	t.Run("already revoked returns 404", func(t *testing.T) {
		srv, _, _, mock, cleanup := newTestServer(t, false)
		defer cleanup()
		mock.ExpectExec("UPDATE permission_rule SET revoked_at").
			WithArgs("rule-1", "t1").
			WillReturnResult(sqlmock.NewResult(0, 0))
		code, _ := doJSON(t, srv.Handler(), http.MethodDelete, "/v1/tenants/t1/permission-rules/rule-1", "", nil)
		if code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", code)
		}
	})
}
