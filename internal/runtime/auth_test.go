package runtime

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/model"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
)

// 本文件验证认证链路的**接线**，而不是 Verifier 的密码学能力
// （后者由 internal/adapters/auth/jwt_test.go 覆盖）。
//
// 出发点是这次改造最容易被忽略、也最致命的一种失效：
// 认证器写好了、单测全绿，但 CreateRun 忘了调用它，或者调用了却忘了把
// 身份写进 Run —— 于是 agent_run.user_id 永远为空，worker 注入的
// ExecutionContext.UserID 也永远为空，护栏的"权限收窄"退化成空谈，
// 而所有测试依然通过。因此下面的断言一律落在**持久化的 Run 对象**与
// **哨兵错误**上，而不是"认证器被调用了几次"。

// captureStore 捕获 CreateRun 收到的 Run，用于断言身份是否真的被持久化。
//
// 不复用包内的 fakeStore：它的 CreateRun 直接丢弃参数（签名是
// CreateRun(context.Context, *model.Run) error { return nil }），
// 而"身份有没有被写进 Run"恰恰是本文件唯一关心的事实。
type captureStore struct {
	created []*model.Run
	getRun  *model.Run
	// casLost 为 true 时 UpdateRunCAS 返回 false，模拟"并发已推进、本次 CAS 输了"。
	//
	// 刻意用「标记失败」而不是「标记成功」：零值即 CAS 成功，
	// 这样绝大多数用例不必显式设置它。反过来写会让每个用到 CreateRun 的用例
	// 都必须记得加 casOK:true，漏一个就是一批与本次改动无关的失败。
	casLost     bool
	cancelCalls int
}

func (s *captureStore) CreateRun(_ context.Context, r *model.Run) error {
	cp := *r
	s.created = append(s.created, &cp)
	return nil
}
func (s *captureStore) GetRun(context.Context, string, string) (*model.Run, error) {
	if s.getRun != nil {
		cp := *s.getRun
		return &cp, nil
	}
	if len(s.created) > 0 {
		cp := *s.created[len(s.created)-1]
		return &cp, nil
	}
	return nil, errors.New("no run")
}
func (s *captureStore) UpdateRunCAS(_ context.Context, _, _ string, _ int64, status model.RunStatus, node, output string) (bool, error) {
	// casLost 模拟"并发已推进，本次 CAS 输了"。
	//
	// 不能用 version 不匹配来模拟：Resumer 传的正是它刚从 GetRun 读到的 version，
	// 两者恒等，于是"冲突"分支永远不会被走到，用例看起来通过了其实什么都没测。
	if s.casLost {
		return false, nil
	}
	if s.getRun != nil {
		s.getRun.Status, s.getRun.CurrentNodeID, s.getRun.Output = status, node, output
		s.getRun.Version++
	}
	return true, nil
}
func (s *captureStore) InsertPlan(context.Context, string, string, model.Plan) error { return nil }
func (s *captureStore) MarkReady(context.Context, string, string) error              { return nil }
func (s *captureStore) Children(context.Context, string, string) ([]model.Task, error) {
	return nil, nil
}
func (s *captureStore) DependenciesReady(context.Context, string, string) (bool, error) {
	return true, nil
}
func (s *captureStore) RunComplete(context.Context, string, string) (bool, error) { return true, nil }
func (s *captureStore) RunHasFailure(context.Context, string, string) (bool, error) {
	return false, nil
}
func (s *captureStore) CompletedNodes(context.Context, string, string) ([]model.Node, error) {
	return nil, nil
}
func (s *captureStore) CountNodes(context.Context, string, string) (int, error)    { return 0, nil }
func (s *captureStore) RunTokenUsage(context.Context, string, string) (int, error) { return 0, nil }
func (s *captureStore) InboxSeen(context.Context, string, string) (bool, error)    { return false, nil }
func (s *captureStore) MarkInbox(context.Context, string, string) error            { return nil }

// captureQueue 记录入队的任务，用于证明"认证失败时没有任何节点被投递"。
type captureQueue struct{ tasks []model.Task }

func (q *captureQueue) Enqueue(_ context.Context, t model.Task) error {
	q.tasks = append(q.tasks, t)
	return nil
}

// recordLifecycle 记录生命周期钩子的调用，用于证明 OnRunStart 拿到了完整身份。
type recordLifecycle struct {
	startEC   []contracts.ExecutionContext
	finishEC  []contracts.ExecutionContext
	finishErr []error
	startErr  error
}

func (l *recordLifecycle) OnRunStart(_ context.Context, ec contracts.ExecutionContext) error {
	l.startEC = append(l.startEC, ec)
	return l.startErr
}
func (l *recordLifecycle) OnRunFinish(_ context.Context, ec contracts.ExecutionContext, err error) error {
	l.finishEC = append(l.finishEC, ec)
	l.finishErr = append(l.finishErr, err)
	return nil
}

// staticAuth 返回一个把固定 token 映射为固定身份的认证器。
// 用 contracts.AuthenticatorFunc 而非 adapters/auth 的实现：
// 本文件验证的是 Runtime 的接线，不重复验证 JWT 解析。
func staticAuth(id contracts.Identity, err error) contracts.Authenticator {
	return contracts.AuthenticatorFunc(func(context.Context, string) (contracts.Identity, error) {
		return id, err
	})
}

// withToken 把凭证注入 ctx，模拟入口进程从请求头取出凭证后的状态。
func withToken(token string) context.Context {
	return contracts.WithAuthToken(context.Background(), token)
}

// TestCreateRun_PersistsAuthenticatedIdentity 是本次改造的核心断言。
//
// 身份必须落在**持久化的 Run 对象**上，而不只是存在于内存：
// Run 由 cmd/runtime 创建、由 cmd/worker 执行，两者隔着 MySQL 与 Redis。
// 只放进 ExecutionContext 的话，worker 侧读库还原时拿到的是空值，
// 护栏三层防御的第 1 层（Tool 调用带发起者身份）就完全落不了地。
func TestCreateRun_PersistsAuthenticatedIdentity(t *testing.T) {
	s := &captureStore{}
	q := &captureQueue{}
	rt := &Runtime{Store: s, Queue: q, Planner: DemoPlanner{},
		Authenticator: staticAuth(contracts.Identity{
			UserID: "user-42", TenantID: "tenant-A", Subject: "user-42",
			Scopes: []string{"run:write"}, Method: "jwt-hs256",
		}, nil)}

	run, err := rt.CreateRun(withToken("tok"), "tenant-A", "demo", "goal", "thread-1")
	if err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if len(s.created) != 1 {
		t.Fatalf("expected exactly 1 persisted run, got %d", len(s.created))
	}
	persisted := s.created[0]
	if persisted.UserID != "user-42" {
		t.Errorf("persisted UserID = %q, want user-42 (identity must survive to the DB)", persisted.UserID)
	}
	if persisted.AuthMethod != "jwt-hs256" {
		t.Errorf("persisted AuthMethod = %q, want jwt-hs256", persisted.AuthMethod)
	}
	// 返回值也必须带身份：调用方（cmd/runtime）会打印它，
	// 而 GetRun 之后若身份丢失，说明写入与读取不对称。
	if run.UserID != "user-42" {
		t.Errorf("returned run.UserID = %q, want user-42", run.UserID)
	}
	if len(q.tasks) == 0 {
		t.Error("expected root nodes to be enqueued for a successful run")
	}
}

// TestCreateRun_RejectsInvalidTokenWithoutTrace 认证失败必须 fail-closed，
// 且**不留任何持久化痕迹**。
//
// 这个断言针对的是一种很现实的事故形态：认证在 Store.CreateRun 之后才执行，
// 于是每一次撞库尝试都在 agent_run 里留下一行 PENDING 记录。
// 攻击者用无效 token 打几万发，业务表就被垃圾数据填满，
// 而这些记录本该只出现在审计日志里。
func TestCreateRun_RejectsInvalidTokenWithoutTrace(t *testing.T) {
	s := &captureStore{}
	q := &captureQueue{}
	rt := &Runtime{Store: s, Queue: q, Planner: DemoPlanner{},
		Authenticator: staticAuth(contracts.Identity{}, contracts.ErrInvalidSignature)}

	_, err := rt.CreateRun(withToken("bad-token"), "tenant-A", "demo", "goal", "")
	if !errors.Is(err, contracts.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
	// 未授权的尝试不得污染业务表，也不得投递任何任务。
	if len(s.created) != 0 {
		t.Errorf("denied run was persisted: %+v", s.created)
	}
	if len(q.tasks) != 0 {
		t.Errorf("denied run enqueued tasks: %+v", q.tasks)
	}
}

// TestCreateRun_MissingTokenWhenAuthenticatorConfigured 配置了认证器却没带凭证，
// 必须报 ErrMissingToken 而不是"当作匿名放行"。
//
// 这是最容易被写错的一支：直觉上"没有 token"很像"不需要认证"，
// 但既然装配了认证器，就说明本服务要求身份；此时放行等于给了一个
// 不带任何凭证就能创建 Run 的后门。
func TestCreateRun_MissingTokenWhenAuthenticatorConfigured(t *testing.T) {
	s := &captureStore{}
	rt := &Runtime{Store: s, Queue: &captureQueue{}, Planner: DemoPlanner{},
		Authenticator: staticAuth(contracts.Identity{UserID: "u"}, nil)}

	// 不注入 token 的裸 ctx。
	_, err := rt.CreateRun(context.Background(), "tenant-A", "demo", "goal", "")
	if !errors.Is(err, contracts.ErrMissingToken) {
		t.Fatalf("expected ErrMissingToken, got %v", err)
	}
	if len(s.created) != 0 {
		t.Error("run with no token must not be persisted")
	}
}

// TestCreateRun_TenantMismatchIsForbidden 租户越权必须归为 ErrForbidden 而非认证失败。
//
// 两者的处置相反：认证失败可能是密钥配错（运维问题，可能瞬时），
// 越权则一定是配置串号或攻击尝试。混成一个错误会让重试策略
// 对越权请求反复重试 —— 每次都同样失败，只是把 DLQ 塞满。
func TestCreateRun_TenantMismatchIsForbidden(t *testing.T) {
	s := &captureStore{}
	rt := &Runtime{Store: s, Queue: &captureQueue{}, Planner: DemoPlanner{},
		Authenticator: staticAuth(contracts.Identity{UserID: "u-1", TenantID: "tenant-A", Method: "jwt-hs256"}, nil)}

	// 凭证属于 tenant-A，却试图在 tenant-B 创建 Run。
	_, err := rt.CreateRun(withToken("tok"), "tenant-B", "demo", "goal", "")
	if !errors.Is(err, contracts.ErrTenantMismatch) {
		t.Fatalf("expected ErrTenantMismatch, got %v", err)
	}
	if !errors.Is(err, contracts.ErrForbidden) {
		t.Error("tenant mismatch must classify as forbidden, not unauthenticated")
	}
	if errors.Is(err, contracts.ErrUnauthenticated) {
		t.Error("tenant mismatch must NOT classify as unauthenticated (retry semantics differ)")
	}
	if len(s.created) != 0 {
		t.Error("cross-tenant run must not be persisted")
	}
}

// TestCreateRun_IdentityWithoutTenantClaimIsAllowed 凭证未声明租户时不做交叉校验。
//
// 部分 IdP 不签发 tenant 声明，若把"凭证没有租户"当成"租户不匹配"，
// 这类部署会在升级后集体无法创建 Run。此时应当放行并依赖
// RequireIdentity 之类的显式策略来收紧，而不是隐式拒绝。
func TestCreateRun_IdentityWithoutTenantClaimIsAllowed(t *testing.T) {
	s := &captureStore{}
	rt := &Runtime{Store: s, Queue: &captureQueue{}, Planner: DemoPlanner{},
		Authenticator: staticAuth(contracts.Identity{UserID: "u-1", TenantID: "", Method: "static-token"}, nil)}

	if _, err := rt.CreateRun(withToken("tok"), "tenant-A", "demo", "goal", ""); err != nil {
		t.Fatalf("expected run without tenant claim to be allowed, got %v", err)
	}
	if len(s.created) != 1 || s.created[0].UserID != "u-1" {
		t.Errorf("expected persisted identity, got %+v", s.created)
	}
}

// TestCreateRun_BackwardCompatibleWithoutAuthenticator 未配置认证器时，
// CreateRun 的行为必须与接入认证之前**完全一致**。
//
// 这是"无 flag day"的验收标准：仓库里有大量测试与部署直接构造
// &Runtime{Store,Queue,Planner}，任何强制认证都会让它们集体失败。
// 身份留空是明确语义（"未经证明"），而不是错误。
func TestCreateRun_BackwardCompatibleWithoutAuthenticator(t *testing.T) {
	s := &captureStore{}
	q := &captureQueue{}
	rt := &Runtime{Store: s, Queue: q, Planner: DemoPlanner{}}

	run, err := rt.CreateRun(context.Background(), "tenant-A", "demo", "goal", "thread-1")
	if err != nil {
		t.Fatalf("unauthenticated CreateRun must still succeed, got %v", err)
	}
	if run.UserID != "" || run.AuthMethod != "" {
		t.Errorf("expected empty identity, got user=%q method=%q", run.UserID, run.AuthMethod)
	}
	if len(q.tasks) == 0 {
		t.Error("expected nodes to be enqueued as before")
	}
}

// TestCreateRun_RequireIdentityRejectsAnonymous RequireIdentity 是生产环境应当开启的开关：
// 开启后无身份的 Run 一律拒绝，即使没有配置认证器。
//
// 覆盖"配了 RequireIdentity 却忘了配 Authenticator"这个部署错误 ——
// 此时若静默放行，运维会以为认证已经生效，而实际上所有请求都在匿名通过。
func TestCreateRun_RequireIdentityRejectsAnonymous(t *testing.T) {
	s := &captureStore{}
	rt := &Runtime{Store: s, Queue: &captureQueue{}, Planner: DemoPlanner{}, RequireIdentity: true}

	_, err := rt.CreateRun(context.Background(), "tenant-A", "demo", "goal", "")
	if !errors.Is(err, contracts.ErrUnauthenticated) {
		t.Fatalf("expected ErrUnauthenticated when RequireIdentity is set, got %v", err)
	}
	if len(s.created) != 0 {
		t.Error("anonymous run must not be persisted when identity is required")
	}
}

// TestCreateRun_LifecycleReceivesScopes 生命周期钩子必须拿到**含 scope 的完整身份**。
//
// 这是 Scopes 唯一能被看到的时机：它不落库（agent_run 只有 user_id 与 auth_method），
// 因此 scope 校验只能在入口进程完成。若这里传的是从库里还原的身份（Scopes 为空），
// AuthLifecycle.RequiredScopes 就永远校验不到任何东西 —— 一个静默失效的安全检查。
func TestCreateRun_LifecycleReceivesScopes(t *testing.T) {
	lc := &recordLifecycle{}
	rt := &Runtime{Store: &captureStore{}, Queue: &captureQueue{}, Planner: DemoPlanner{},
		Authenticator: staticAuth(contracts.Identity{
			UserID: "u-1", TenantID: "tenant-A", Scopes: []string{"run:write"}, Method: "jwt-hs256",
		}, nil),
		Lifecycle: lc}

	if _, err := rt.CreateRun(withToken("tok"), "tenant-A", "demo", "goal", "thread-1"); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	if len(lc.startEC) != 1 {
		t.Fatalf("expected OnRunStart called once, got %d", len(lc.startEC))
	}
	ec := lc.startEC[0]
	if ec.UserID != "u-1" || ec.AuthMethod != "jwt-hs256" {
		t.Errorf("OnRunStart got identity user=%q method=%q", ec.UserID, ec.AuthMethod)
	}
	// scope 必须在钩子里可见：它不落库（agent_run 只有 user_id 与 auth_method），
	// 因此入口进程是唯一能做 scope 校验的地方。这里拿不到就等于校验永久失效。
	id := contracts.Identity{Scopes: ec.Scopes}
	if !id.HasScope("run:write") {
		t.Errorf("OnRunStart must receive scopes (they are never persisted), got %v", ec.Scopes)
	}
	if ec.RunID == "" || ec.TenantID != "tenant-A" {
		t.Errorf("OnRunStart got tenant=%q run=%q", ec.TenantID, ec.RunID)
	}
}

// TestCreateRun_LifecycleDenyPreventsPersistence 钩子拒绝时 Run 不得落库。
//
// 钩子在 Store.CreateRun **之前**执行，这个顺序是本用例要钉住的事实：
// 顺序反了，被策略拒绝的 Run 会留下一行 PENDING 记录，
// 且因为没有任何节点，事件驱动的 Resumer 永远感知不到它 —— 孤儿 Run。
func TestCreateRun_LifecycleDenyPreventsPersistence(t *testing.T) {
	s := &captureStore{}
	lc := &recordLifecycle{startErr: errors.New("policy denied")}
	rt := &Runtime{Store: s, Queue: &captureQueue{}, Planner: DemoPlanner{}, Lifecycle: lc}

	if _, err := rt.CreateRun(context.Background(), "tenant-A", "demo", "goal", ""); err == nil {
		t.Fatal("expected CreateRun to fail when lifecycle denies")
	}
	if len(s.created) != 0 {
		t.Errorf("denied run was persisted: %+v", s.created)
	}
}

// 租户交叉校验的用例见 TestCreateRun_TenantMismatchIsForbidden。
//
// 这里刻意**没有** AuthLifecycle 侧的租户校验用例：OnRunStart 只收到一个
// ExecutionContext，凭证在入口进程校验后就随 ctx 销毁了，此处拿不到第二个租户，
// 交叉校验在结构上不可能成立（详见 middleware/auth.go 的 check 注释）。
// 写一个"看起来在校验租户"的用例只会掩盖这个事实。

// TestAuthLifecycle_RequireIdentity AuthLifecycle 在 RequireIdentity 下拒绝匿名 Run。
//
// 这一支覆盖的是 worker/resume 侧：那些进程不创建 Run，但会在 Run 启动边界上
// 复核身份。存量 Run（认证接入前创建、user_id 为空）在开启该开关后应当被拒绝。
func TestAuthLifecycle_RequireIdentity(t *testing.T) {
	strict := &middleware.AuthLifecycle{RequireIdentity: true}
	err := strict.OnRunStart(context.Background(), contracts.ExecutionContext{
		TenantID: "tenant-A", RunID: "r1", UserID: "",
	})
	if !errors.Is(err, contracts.ErrUnauthenticated) {
		t.Fatalf("expected anonymous run to be rejected, got %v", err)
	}

	// 未开启时放行，但审计里必须标记为未认证（method=none），
	// 否则"这个集群所有 Run 都没有身份"这件事完全不可见。
	var recs []middleware.AuditRecord
	loose := &middleware.AuthLifecycle{}
	loose.OnAudit = func(_ context.Context, r middleware.AuditRecord) { recs = append(recs, r) }
	if err := loose.OnRunStart(context.Background(), contracts.ExecutionContext{
		TenantID: "tenant-A", RunID: "r1", UserID: "",
	}); err != nil {
		t.Fatalf("expected anonymous run to pass when identity not required, got %v", err)
	}
	if len(recs) != 1 || recs[0].AuthMethod != "none" || recs[0].Authenticated {
		t.Errorf("expected audit record marking unauthenticated run, got %+v", recs)
	}
}

// TestAuthLifecycle_RequiredScopes scope 不足必须归为 ErrInsufficientScope。
func TestAuthLifecycle_RequiredScopes(t *testing.T) {
	lc := &middleware.AuthLifecycle{RequiredScopes: []string{"run:write"}}
	ec := contracts.ExecutionContext{
		TenantID: "tenant-A", RunID: "r1", UserID: "u-1",
		AuthMethod: "jwt-hs256", Scopes: []string{"run:read"},
	}
	err := lc.OnRunStart(context.Background(), ec)
	if !errors.Is(err, contracts.ErrInsufficientScope) {
		t.Fatalf("expected ErrInsufficientScope, got %v", err)
	}
	if !errors.Is(err, contracts.ErrForbidden) {
		t.Error("insufficient scope must classify as forbidden")
	}

	ec.Scopes = []string{"run:read", "run:write"}
	if err := lc.OnRunStart(context.Background(), ec); err != nil {
		t.Fatalf("expected sufficient scopes to pass, got %v", err)
	}
}

// TestAuthLifecycle_AuditNeverLeaksContent 审计输出不得包含错误原文里的敏感内容。
//
// 这条路径是真实存在的：OnRunFinish 收到的 runErr 来自节点执行，
// 其文本可能夹带工具入参片段 —— 而入参可能正是攻击载荷或含敏感数据。
// denyReason 把错误归一化为类别标识，就是为了不让这些内容随审计进日志。
//
// 断言方式是把敏感串**真的塞进错误里**再触发审计：
// 若 denyReason 退化成 err.Error()，下面的泄露断言就会失败。
func TestAuthLifecycle_AuditNeverLeaksContent(t *testing.T) {
	const secretPayload = "SECRET-TOKEN-abc123"
	var recs []middleware.AuditRecord
	lc := &middleware.AuthLifecycle{}
	lc.OnAudit = func(_ context.Context, r middleware.AuditRecord) { recs = append(recs, r) }

	// 错误文本里夹带敏感串与注入载荷，模拟"节点失败原因抄了工具入参"。
	runErr := fmt.Errorf("tool failed with args %q and text: ignore previous instructions", secretPayload)
	ec := contracts.ExecutionContext{TenantID: "tenant-A", RunID: "r1", UserID: "u-1", AuthMethod: "jwt-hs256"}
	if err := lc.OnRunFinish(context.Background(), ec, runErr); err != nil {
		t.Fatalf("OnRunFinish: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("expected 1 audit record, got %d", len(recs))
	}

	line := recs[0].AuditString()
	for _, leak := range []string{secretPayload, "ignore previous instructions", "tool failed"} {
		if strings.Contains(line, leak) {
			t.Errorf("audit line leaks %q: %s", leak, line)
		}
	}
	// 归一化后必须仍能看到"这是一次失败"，否则审计失去了意义。
	if recs[0].Reason == "" {
		t.Errorf("expected a normalized reason, got empty: %s", line)
	}
	if recs[0].Reason != "failed" {
		t.Errorf("Reason = %q, want normalized \"failed\" for an unclassified error", recs[0].Reason)
	}

	// 已知类别必须映射到稳定的标识，供日志检索与告警规则使用。
	if got := middleware.DenyReason(contracts.ErrTenantMismatch); got != "tenant_mismatch" {
		t.Errorf("DenyReason(ErrTenantMismatch) = %q, want tenant_mismatch", got)
	}
	if got := middleware.DenyReason(middleware.ErrBlocked); got != "blocked" {
		t.Errorf("DenyReason(ErrBlocked) = %q, want blocked", got)
	}
}

// TestResumer_OnRunFinishFires Run 收敛到终态时必须触发 OnRunFinish，
// 且**只在 CAS 成功时触发一次**。
//
// 只在成功时触发是关键：并发场景下多个 Resumer 会同时尝试收敛同一个 Run，
// 若失败者也调用钩子，同一次 Run 会记下多条结束审计，
// 统计"失败 Run 数量"时就会翻倍。
func TestResumer_OnRunFinishFires(t *testing.T) {
	run := &model.Run{ID: "r1", TenantID: "tenant-A", UserID: "u-9", AuthMethod: "jwt-hs256",
		Status: model.RunRunning, Version: 3}
	s := &captureStore{getRun: run} // 零值 casLost=false，即 CAS 成功
	lc := &recordLifecycle{}
	r := &Resumer{Store: s, Queue: &captureQueue{}, Lifecycle: lc}

	ev := model.Event{ID: "e1", Type: "AgentStepCompleted", RunID: "r1", NodeID: "n1", TenantID: "tenant-A"}
	if err := r.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(lc.finishEC) != 1 {
		t.Fatalf("expected OnRunFinish called once, got %d", len(lc.finishEC))
	}
	// 结束审计必须带出**从库里还原的身份**：这正是身份落库的价值 ——
	// Resumer 与 CreateRun 在不同进程、不同时刻，靠持久化才能还原发起者。
	got := lc.finishEC[0]
	if got.UserID != "u-9" || got.AuthMethod != "jwt-hs256" {
		t.Errorf("OnRunFinish lost identity: user=%q method=%q", got.UserID, got.AuthMethod)
	}
	if got.RunID != "r1" || got.TenantID != "tenant-A" {
		t.Errorf("OnRunFinish got run=%q tenant=%q", got.RunID, got.TenantID)
	}
	if lc.finishErr[0] != nil {
		t.Errorf("expected nil error for a successful run, got %v", lc.finishErr[0])
	}
}

// TestResumer_OnRunFinishNotFiredOnCASConflict CAS 失败（并发已推进）时不得触发钩子。
func TestResumer_OnRunFinishNotFiredOnCASConflict(t *testing.T) {
	run := &model.Run{ID: "r1", TenantID: "tenant-A", Status: model.RunRunning, Version: 99}
	// casLost 让 UpdateRunCAS 直接返回 false，模拟并发已推进的场景。
	s := &captureStore{getRun: run, casLost: true}
	lc := &recordLifecycle{}
	r := &Resumer{Store: s, Queue: &captureQueue{}, Lifecycle: lc}

	ev := model.Event{ID: "e1", Type: "AgentStepCompleted", RunID: "r1", NodeID: "n1", TenantID: "tenant-A"}
	if err := r.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(lc.finishEC) != 0 {
		t.Errorf("OnRunFinish must not fire when CAS lost the race, got %d calls", len(lc.finishEC))
	}
}

// TestResumer_OnRunFinishCarriesFailure Run 因节点失败而收敛时，
// 结束审计必须带上失败原因类别，而不是 nil。
//
// 没有这一支，"失败 Run"与"成功 Run"在审计里长得一样，
// 无法回答"这个用户的失败 Run 里有多少是安全拦截导致的"。
func TestResumer_OnRunFinishCarriesFailure(t *testing.T) {
	run := &model.Run{ID: "r1", TenantID: "tenant-A", UserID: "u-9", Status: model.RunRunning, Version: 3}
	s := &captureStore{getRun: run}
	lc := &recordLifecycle{}
	r := &Resumer{Store: s, Queue: &captureQueue{}, Lifecycle: lc}

	ev := model.Event{ID: "e2", Type: "AgentStepFailed", RunID: "r1", NodeID: "n1",
		TenantID: "tenant-A", Error: "node exploded"}
	if err := r.Handle(context.Background(), ev); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if len(lc.finishEC) != 1 {
		t.Fatalf("expected OnRunFinish called once on failure path, got %d", len(lc.finishEC))
	}
	if lc.finishErr[0] == nil {
		t.Error("expected a non-nil run error on the failure path")
	}
	if lc.finishEC[0].UserID != "u-9" {
		t.Errorf("failure audit lost identity: %q", lc.finishEC[0].UserID)
	}
}
