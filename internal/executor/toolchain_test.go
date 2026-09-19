package executor

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/model"
	"context"
	"errors"
	"strings"
	"testing"
)

// 本文件验证的是**接线本身**，而不是中间件的能力。
//
// 中间件（guard / redact）各自有充分的单元测试，但那些测试都直接调用
// Before/After，无法证明"生产路径上真的会经过它们"。此前仓库的状态恰恰是：
// 中间件写好了、测试全绿、却没有任何一处被装配 —— 挂载点存在但不生效。
// 因此下面的用例一律通过 Dispatcher.Execute 走完整工具路径，
// 用可观测的副作用（是否被拦、落库内容、幂等键）来证明链路已通。

// spyMiddleware 记录 Before/After 的调用次序与观察到的数据。
// 它同时充当改写器，用于验证幂等键基于改写后的入参。
type spyMiddleware struct {
	beforeCalls int
	afterCalls  int
	sawArgs     []string
	sawOutputs  []string
	sawEC       []contracts.ExecutionContext
	// rewriteTo 非空时，Before 把入参改写为该值。
	rewriteTo string
	// err 非 nil 时，Before 返回该错误（用于验证哨兵语义透传）。
	err error
}

func (m *spyMiddleware) Before(_ context.Context, ec contracts.ExecutionContext, req contracts.ToolCallRequest) (contracts.ToolCallRequest, error) {
	m.beforeCalls++
	m.sawArgs = append(m.sawArgs, req.Arguments)
	m.sawEC = append(m.sawEC, ec)
	if m.err != nil {
		return req, m.err
	}
	if m.rewriteTo != "" {
		req.Arguments = m.rewriteTo
	}
	return req, nil
}

func (m *spyMiddleware) After(_ context.Context, _ contracts.ExecutionContext, _ contracts.ToolCallRequest, result contracts.ToolResult) (contracts.ToolResult, error) {
	m.afterCalls++
	m.sawOutputs = append(m.sawOutputs, result.Output)
	return result, nil
}

var _ middleware.Tool = (*spyMiddleware)(nil)

// TestToolChain_WiredIntoToolPath 防护链必须在工具执行路径上真实生效。
//
// 这是接线的最低验收标准：Before 与 After 都被调用，且 After 拿到工具的真实输出。
func TestToolChain_WiredIntoToolPath(t *testing.T) {
	spy := &spyMiddleware{}
	tl := &scriptedTool{name: "search", outputs: []string{"raw-output"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolChain: middleware.NewToolChain(spy)}

	out, err := d.Execute(context.Background(), newToolNode())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if spy.beforeCalls != 1 {
		t.Errorf("Before not invoked on tool path: calls=%d", spy.beforeCalls)
	}
	if spy.afterCalls != 1 {
		t.Errorf("After not invoked on tool path: calls=%d", spy.afterCalls)
	}
	if out != "raw-output" {
		t.Errorf("output=%q, want %q", out, "raw-output")
	}
	if len(spy.sawOutputs) != 1 || spy.sawOutputs[0] != "raw-output" {
		t.Errorf("After did not observe tool output: %v", spy.sawOutputs)
	}
	if tl.calls != 1 {
		t.Errorf("tool executed %d times, want 1", tl.calls)
	}
}

// TestToolChain_ExecutionContextCarriesNodeID 中间件必须能拿到节点级身份。
//
// 人工闸门要靠 NodeID 定位审批锚点。拿不到它，挂起会落在错误的位置，
// 人工放行后 ResumeRun 捞不到待恢复节点，Run 永远停在 WAITING_HUMAN。
func TestToolChain_ExecutionContextCarriesNodeID(t *testing.T) {
	spy := &spyMiddleware{}
	tl := &scriptedTool{name: "search", outputs: []string{"ok"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolChain: middleware.NewToolChain(spy)}

	n := newToolNode()
	if _, err := d.Execute(context.Background(), n); err != nil {
		t.Fatalf("execute: %v", err)
	}
	if len(spy.sawEC) != 1 {
		t.Fatalf("expected 1 execution context, got %d", len(spy.sawEC))
	}
	ec := spy.sawEC[0]
	if ec.TenantID != n.TenantID || ec.RunID != n.RunID || ec.NodeID != n.ID {
		t.Errorf("execution context lost node identity: %+v (node=%+v)", ec, n)
	}
}

// TestToolChain_CtxInjectedContextWins ctx 中已注入的上下文应被优先采用。
//
// worker 会注入完整身份（含 ThreadID / TraceID），执行器不得用节点字段覆盖它们，
// 否则跨 Run 的记忆检索与链路追踪会丢掉会话与轨迹维度。
func TestToolChain_CtxInjectedContextWins(t *testing.T) {
	spy := &spyMiddleware{}
	tl := &scriptedTool{name: "search", outputs: []string{"ok"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolChain: middleware.NewToolChain(spy)}

	n := newToolNode()
	ctx := contracts.WithExecutionContext(context.Background(), contracts.ExecutionContext{
		TenantID: n.TenantID, UserID: "user-42", ThreadID: "thread-7",
		RunID: n.RunID, NodeID: n.ID, TraceID: "trace-abc",
	})
	if _, err := d.Execute(ctx, n); err != nil {
		t.Fatalf("execute: %v", err)
	}
	ec := spy.sawEC[0]
	if ec.UserID != "user-42" || ec.ThreadID != "thread-7" || ec.TraceID != "trace-abc" {
		t.Errorf("injected context was overwritten: %+v", ec)
	}
}

// TestToolChain_InterceptBeforeIdempotencyClaim 拦截必须先于幂等认领。
//
// 这是本次接线里最关键的一条顺序约束。若在 ClaimToolCall 之后才拦截，
// 被拦下的调用会留下一条 RUNNING 的 tool_call 记录；人工放行后重新调度时，
// 幂等逻辑读到 RUNNING 会判定"崩溃在途、拒绝盲目重执行"——
// 于是一次安全拦截把该节点永久锁死，审批放行了也跑不动。
func TestToolChain_InterceptBeforeIdempotencyClaim(t *testing.T) {
	store := newFakeToolStore()
	spy := &spyMiddleware{err: middleware.ErrBlocked}
	tl := &scriptedTool{name: "search", outputs: []string{"never"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolStore: store, ToolChain: middleware.NewToolChain(spy)}

	_, err := d.Execute(context.Background(), newToolNode())
	if !errors.Is(err, middleware.ErrBlocked) {
		t.Fatalf("expected ErrBlocked, got %v", err)
	}
	if len(store.calls) != 0 {
		t.Errorf("blocked call left %d tool_call record(s) behind: %+v", len(store.calls), store.calls)
	}
	if tl.calls != 0 {
		t.Errorf("blocked call still executed the tool %d time(s)", tl.calls)
	}
}

// TestToolChain_SentinelErrorsPreserved 哨兵错误必须原样上抛。
//
// worker 依赖 errors.Is 区分"等待人工"与"真失败"。
// 一旦被包装成普通错误（丢失 %w），审批节点会被重试策略反复重跑。
func TestToolChain_SentinelErrorsPreserved(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
		want error
	}{
		{"awaiting approval", middleware.ErrAwaitingApproval, middleware.ErrAwaitingApproval},
		{"blocked", middleware.ErrBlocked, middleware.ErrBlocked},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spy := &spyMiddleware{err: tc.err}
			tl := &scriptedTool{name: "search", outputs: []string{"never"}, errs: []error{nil}}
			d := &Dispatcher{Tools: mustReg(tl), ToolChain: middleware.NewToolChain(spy)}

			_, err := d.Execute(context.Background(), newToolNode())
			if !errors.Is(err, tc.want) {
				t.Fatalf("sentinel lost: got %v, want errors.Is(%v)", err, tc.want)
			}
		})
	}
}

// TestToolChain_IdempotencyKeyUsesRewrittenArgs 幂等键必须基于改写后的入参。
//
// 中间件可以改写入参（Guard 当前不改，但链上任何一环都可以）。
// 若幂等键仍用原始入参计算，改写后会出现"同一逻辑调用两个键"的错配，
// 导致重复执行副作用 —— 幂等保护形同失效。
func TestToolChain_IdempotencyKeyUsesRewrittenArgs(t *testing.T) {
	store := newFakeToolStore()
	spy := &spyMiddleware{rewriteTo: `{"q":"rewritten"}`}
	tl := &scriptedTool{name: "search", outputs: []string{"ok"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolStore: store, ToolChain: middleware.NewToolChain(spy)}

	n := newToolNode()
	n.Input = `{"q":"original"}`
	if _, err := d.Execute(context.Background(), n); err != nil {
		t.Fatalf("execute: %v", err)
	}
	// 期望的键由改写后的入参派生。
	wantKey := idempotencyKey(n.RunID, n.ID, n.Name, `{"q":"rewritten"}`)
	if _, ok := store.calls[wantKey]; !ok {
		keys := make([]string, 0, len(store.calls))
		for k := range store.calls {
			keys = append(keys, k)
		}
		t.Fatalf("idempotency key not derived from rewritten args; keys=%v want=%s", keys, wantKey)
	}
	staleKey := idempotencyKey(n.RunID, n.ID, n.Name, `{"q":"original"}`)
	if _, ok := store.calls[staleKey]; ok {
		t.Errorf("idempotency key derived from original args, middleware rewrite was ignored")
	}
}

// TestToolChain_RedactionBeforePersist 脱敏必须在落库之前。
//
// tool_call.output 是持久化数据。先落库再脱敏等于把敏感原文长期留在数据库里，
// 比事件泄露更难回收（事件是流式的，库里的数据会一直在）。
func TestToolChain_RedactionBeforePersist(t *testing.T) {
	store := newFakeToolStore()
	// salt 必须非空：空盐值下 token 派生会 fail-safe 降级为纯掩码（见 Policy.replacement），
	// 那会让本用例断言的伪标识形式不成立。
	policy := middleware.NewPolicy(middleware.SensitivityInternal, "test-salt")
	redactor := middleware.NewRedactor(policy)
	tl := &scriptedTool{name: "search", outputs: []string{"contact 13800138000 now"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolStore: store, ToolChain: middleware.NewToolChain(redactor)}

	out, err := d.Execute(context.Background(), newToolNode())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if strings.Contains(out, "13800138000") {
		t.Errorf("returned output still contains raw phone number: %q", out)
	}
	if len(store.calls) != 1 {
		t.Fatalf("expected 1 persisted tool call, got %d", len(store.calls))
	}
	for _, tc := range store.calls {
		if strings.Contains(tc.Output, "13800138000") {
			t.Errorf("persisted tool_call.output leaked raw phone number: %q", tc.Output)
		}
		if tc.Output == "" {
			t.Error("persisted output is empty; redaction dropped content instead of masking it")
		}
	}
}

// TestToolChain_GuardEndToEndWithRealMiddleware 用真实护栏验证端到端接线。
//
// 前面的用例用替身证明"链被调用了"，本用例用真实的 Guard + Approver 证明
// "高危载荷确实被拦下、工具确实没执行" —— 这才是防护生效的实质证据。
func TestToolChain_GuardEndToEndWithRealMiddleware(t *testing.T) {
	var approved middleware.ApprovalRequest
	approvals := 0
	guard := middleware.NewGuard(middleware.ApproverFunc(func(_ context.Context, req middleware.ApprovalRequest) error {
		approvals++
		approved = req
		return nil
	}))
	tl := &scriptedTool{name: "shell_exec", outputs: []string{"never"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolChain: middleware.NewToolChain(guard)}

	n := &model.Node{
		ID: "n1", RunID: "r1", TenantID: "t1", Type: model.NodeTool,
		Name: "shell_exec", Input: `{"cmd":"ls; rm -rf /"}`, Attempt: 0,
	}
	_, err := d.Execute(context.Background(), n)
	if err == nil {
		t.Fatal("high-risk payload was not intercepted")
	}
	if tl.calls != 0 {
		t.Errorf("tool executed %d time(s) despite interception", tl.calls)
	}
	if !errors.Is(err, middleware.ErrBlocked) && !errors.Is(err, middleware.ErrAwaitingApproval) {
		t.Errorf("unexpected error semantics: %v", err)
	}
	// 若走的是转人工，审批请求必须带上节点标识，否则挂起会失去锚点。
	if approvals > 0 {
		if approved.NodeID != n.ID {
			t.Errorf("approval request lost node id: %+v", approved)
		}
		if approved.TenantID != n.TenantID || approved.RunID != n.RunID {
			t.Errorf("approval request lost tenant/run identity: %+v", approved)
		}
	}
}

// TestToolChain_NilChainIsNoop 未装配防护链时行为与改造前一致。
//
// 这条保护的是既有部署与测试：ToolChain 为 nil 时必须完全透明，
// 否则接入这套能力会变成一次 flag day 迁移。
func TestToolChain_NilChainIsNoop(t *testing.T) {
	store := newFakeToolStore()
	tl := &scriptedTool{name: "search", outputs: []string{"plain result"}, errs: []error{nil}}
	d := &Dispatcher{Tools: mustReg(tl), ToolStore: store}

	out, err := d.Execute(context.Background(), newToolNode())
	if err != nil {
		t.Fatalf("execute: %v", err)
	}
	if out != "plain result" {
		t.Errorf("output=%q, want %q", out, "plain result")
	}
	// 幂等键仍由原始入参派生，与改造前一致。
	wantKey := idempotencyKey("r1", "n1", "search", "q")
	if _, ok := store.calls[wantKey]; !ok {
		t.Errorf("idempotency key changed when ToolChain is nil; keys=%v", store.calls)
	}
}
