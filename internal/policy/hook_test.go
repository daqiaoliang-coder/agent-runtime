package policy

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"agent-runtime/internal/contracts"
)

// ---- L4 收紧层语义（docs §2.1 短路规则 / §6 可收紧不可放宽 / §9 失败语义）----

// hookServer 起一个记录调用次数、返回固定判定的策略服务。
type hookServer struct {
	srv      *httptest.Server
	hits     int
	lastBody hookRequest
	respond  func() (int, string) // (status, body)
}

func newHookServer(t *testing.T, status int, body string) *hookServer {
	t.Helper()
	h := &hookServer{respond: func() (int, string) { return status, body }}
	h.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.hits++
		_ = json.NewDecoder(r.Body).Decode(&h.lastBody)
		status, body := h.respond()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(h.srv.Close)
	return h
}

// allowClassifier 返回一个必判 allow 且引用有效授权的分类器（携带下行
// 给收紧层复核的"推断性授权"）。
func allowClassifier() *ClassifierStage {
	fm := &fakeClassifierModel{respond: func(_ contracts.GenerateRequest) (string, error) {
		return `{"semantics":{"action":"read_only","targets":["internal.example"],"summary":"read internal api"},
		 "decision":"allow","matched_grant":"允许访问 *.internal 域名的只读接口","reason":"read-only call to authorized domain"}`, nil
	}}
	cs := newClassifierStage(fm)
	return cs
}

// missStages 是全部未命中的确定性层（空规则表），让瀑布走到分类器/收紧层。
func missStages() (Stage, Stage) {
	empty := NewRuleSet(nil)
	return &RulesStage{Rules: empty}, &ParserStage{Rules: empty}
}

func TestHookTightensClassifierAllow(t *testing.T) {
	hook := newHookServer(t, 200, `{"decision":"deny","reason":"egress to internal requires ticket"}`)
	l1, l2 := missStages()
	w := NewWaterfall(l1, l2, allowClassifier(), &HookStage{Opt: HookOptions{Endpoints: []string{hook.srv.URL}}})

	res, err := w.Evaluate(context.Background(), classifierTestReq())
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Deny {
		t.Fatalf("hook must be able to tighten classifier allow; got %v (%s)", res.Decision, res.Reason)
	}
	if res.Layer != LayerHook {
		t.Fatalf("tightened decision must be attributed to the hook layer, got %q", res.Layer)
	}
	if hook.hits != 1 {
		t.Fatalf("hook must be consulted exactly once, got %d", hook.hits)
	}
}

func TestHookConcursClassifierAllow(t *testing.T) {
	// Hook 说 allow：不构成收紧，判定作者仍是分类器（layer 溯源不变）。
	hook := newHookServer(t, 200, `{"decision":"allow","reason":"compliance ok"}`)
	l1, l2 := missStages()
	w := NewWaterfall(l1, l2, allowClassifier(), &HookStage{Opt: HookOptions{Endpoints: []string{hook.srv.URL}}})

	res, err := w.Evaluate(context.Background(), classifierTestReq())
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Allow {
		t.Fatalf("concurring hook must keep the classifier allow, got %v", res.Decision)
	}
	if res.Layer != LayerClassifier {
		t.Fatalf("un-tightened decision keeps the original layer, got %q", res.Layer)
	}
}

func TestClassifierAllowStandsWithoutHook(t *testing.T) {
	// 回归守卫：未装配收紧层时分类器 allow 立即生效（P1 行为零变化，
	// hasRefinerAfter 为 false 的分支）。
	l1, l2 := missStages()
	w := NewWaterfall(l1, l2, allowClassifier())

	res, err := w.Evaluate(context.Background(), classifierTestReq())
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Allow || res.Layer != LayerClassifier {
		t.Fatalf("expected classifier allow without hook, got %v/%q", res.Decision, res.Layer)
	}
}

func TestHookCannotRelaxUnmatched(t *testing.T) {
	// 未命中任何层的调用默认送审——Hook 的 allow 无法把 ask 放宽成 allow。
	// 这是"可收紧不可放宽"在未命中路径上的体现：未识别 → 人工是结构兜底。
	hook := newHookServer(t, 200, `{"decision":"allow","reason":"trusted command"}`)
	l1, l2 := missStages()
	w := NewWaterfall(l1, l2, &HookStage{Opt: HookOptions{Endpoints: []string{hook.srv.URL}}})

	res, err := w.Evaluate(context.Background(), classifierTestReq())
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != RequireApproval {
		t.Fatalf("hook allow must not relax the unmatched default; got %v", res.Decision)
	}
	if res.Layer != LayerDefault {
		t.Fatalf("un-tightened default keeps l5 attribution, got %q", res.Layer)
	}
}

func TestHookDeniesUnmatched(t *testing.T) {
	// 未命中路径上 Hook 可以把默认 ask 收紧为 deny：合规服务对"确定性
	// 规则没覆盖到"的调用给出明确否决，省掉一轮人工。
	hook := newHookServer(t, 200, `{"decision":"deny","reason":"kubectl is banned by corp policy"}`)
	l1, l2 := missStages()
	w := NewWaterfall(l1, l2, &HookStage{Opt: HookOptions{Endpoints: []string{hook.srv.URL}}})

	res, err := w.Evaluate(context.Background(), Request{TenantID: "t1", RunID: "r1", NodeID: "n1", ToolName: "shell", Input: "kubectl get pods"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Deny || res.Layer != LayerHook {
		t.Fatalf("expected hook deny on unmatched, got %v/%q", res.Decision, res.Layer)
	}
}

func TestHookDeterministicAllowNotConsulted(t *testing.T) {
	// L1 的 allow 是确定性授权，立即终局——收紧层连咨询的资格都没有
	//（§2.1"确定性层有放行资格"；§6"无法翻越"由层序保证）。
	hook := newHookServer(t, 200, `{"decision":"deny","reason":"should never be asked"}`)
	rules := NewRuleSet([]Rule{{Tool: "read_file", Pattern: "notes.txt", Effect: Allow, Source: "builtin"}})
	w := NewWaterfall(&RulesStage{Rules: rules}, &ParserStage{Rules: rules}, &HookStage{Opt: HookOptions{Endpoints: []string{hook.srv.URL}}})

	res, err := w.Evaluate(context.Background(), Request{TenantID: "t1", RunID: "r1", NodeID: "n1", ToolName: "read_file", Input: "notes.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Allow || res.Layer != LayerRules {
		t.Fatalf("deterministic allow must terminate before the hook, got %v/%q", res.Decision, res.Layer)
	}
	if hook.hits != 0 {
		t.Fatalf("hook must not be consulted for deterministic allows, got %d hits", hook.hits)
	}
}

func TestHookFailClosedOnClassifierAllow(t *testing.T) {
	// 超时 / 非 200 / 非法 JSON：该实例一律按 ask 计入聚合——分类器 allow
	// 被收紧为送审，绝不静默放行（§9）。
	cases := []struct {
		name   string
		server func(t *testing.T) *hookServer
	}{
		{"timeout", func(t *testing.T) *hookServer {
			h := newHookServer(t, 200, `{"decision":"allow"}`)
			h.respond = func() (int, string) { time.Sleep(300 * time.Millisecond); return 200, `{"decision":"allow"}` }
			return h
		}},
		{"http 500", func(t *testing.T) *hookServer { return newHookServer(t, 500, "boom") }},
		{"invalid json", func(t *testing.T) *hookServer { return newHookServer(t, 200, `<html>login page</html>`) }},
		{"unknown decision", func(t *testing.T) *hookServer { return newHookServer(t, 200, `{"decision":"yolo"}`) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			hook := tc.server(t)
			l1, l2 := missStages()
			w := NewWaterfall(l1, l2, allowClassifier(), &HookStage{Opt: HookOptions{Endpoints: []string{hook.srv.URL}, Timeout: 50 * time.Millisecond}})

			res, err := w.Evaluate(context.Background(), classifierTestReq())
			if err != nil {
				t.Fatal(err)
			}
			if res.Decision != RequireApproval {
				t.Fatalf("hook failure must fail-closed to approval, got %v (%s)", res.Decision, res.Reason)
			}
		})
	}
}

func TestHookMultiInstanceTakesStrictest(t *testing.T) {
	// §6：多实例按 Chain 语义取最严——一个 allow + 一个 deny = deny。
	allowSrv := newHookServer(t, 200, `{"decision":"allow"}`)
	denySrv := newHookServer(t, 200, `{"decision":"deny","reason":"second opinion wins"}`)
	l1, l2 := missStages()
	w := NewWaterfall(l1, l2, allowClassifier(), &HookStage{Opt: HookOptions{Endpoints: []string{allowSrv.srv.URL, denySrv.srv.URL}}})

	res, err := w.Evaluate(context.Background(), classifierTestReq())
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Deny {
		t.Fatalf("strictest of (allow, deny) must be deny, got %v", res.Decision)
	}
	if allowSrv.hits != 1 || denySrv.hits != 1 {
		t.Fatalf("all instances must be consulted (chain semantics), got allow=%d deny=%d", allowSrv.hits, denySrv.hits)
	}
}

func TestHookRequestContract(t *testing.T) {
	// §6 请求契约：decision_so_far 携带瀑布当前判定（allow），标识字段
	// 齐全——合规服务要能按 run/node 维度做策略与审计。
	hook := newHookServer(t, 200, `{"decision":"allow"}`)
	l1, l2 := missStages()
	w := NewWaterfall(l1, l2, allowClassifier(), &HookStage{Opt: HookOptions{Endpoints: []string{hook.srv.URL}}})

	if _, err := w.Evaluate(context.Background(), classifierTestReq()); err != nil {
		t.Fatal(err)
	}
	b := hook.lastBody
	if b.Event != "PreToolUse" || b.TenantID != "t1" || b.RunID != "r1" || b.NodeID != "n1" {
		t.Fatalf("identity fields mismatch: %+v", b)
	}
	if b.Tool != "shell" || b.NormalizedInput != classifierTestReq().Input {
		t.Fatalf("tool/input mismatch: %+v", b)
	}
	if b.DecisionSoFar != string(Allow) {
		t.Fatalf("decision_so_far must carry the classifier allow, got %q", b.DecisionSoFar)
	}
}

func TestHookReasonTruncated(t *testing.T) {
	// 恶意/失控的 Hook 服务不得把超长文本灌进 interrupt 与日志。
	long := strings.Repeat("x", 10_000)
	hook := newHookServer(t, 200, `{"decision":"deny","reason":"`+long+`"}`)
	stage := &HookStage{Opt: HookOptions{Endpoints: []string{hook.srv.URL}}}

	res, hit, err := stage.Refine(context.Background(), classifierTestReq(), defaultAskResult())
	if err != nil || !hit {
		t.Fatalf("refine: hit=%v err=%v", hit, err)
	}
	if len(res.Reason) > hookReasonMax {
		t.Fatalf("reason must be capped at %d, got %d", hookReasonMax, len(res.Reason))
	}
}
