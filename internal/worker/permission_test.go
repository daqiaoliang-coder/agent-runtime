package worker

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"agent-runtime/internal/policy"
	"agent-runtime/internal/store"

	"github.com/DATA-DOG/go-sqlmock"
)

// 接线 smoke：瀑布关闭时 gate 必须维持休眠（Policy 为 nil——此前
// NewFromEnv 从未装配 Policy，默认挂 CommandPolicy 会激活休眠的 gate
// 改变默认部署行为）；开启时 Policy 与 Approval 必须同时就位——
// Approval 缺席的话，L5 默认送审（瀑布最常见的终局）会在 Handle 里
// 报错并按基础设施失败重试直至 DLQ，而不是转人工。
func TestNewFromEnvPolicyWiring(t *testing.T) {
	t.Run("waterfall disabled keeps gate dormant", func(t *testing.T) {
		w := NewFromEnv(nil, nil, nil)
		if w.Policy != nil {
			t.Fatalf("Policy must stay nil when waterfall disabled (dormant gate), got %T", w.Policy)
		}
		if w.Approval != nil {
			t.Fatalf("Approval must stay nil when waterfall disabled, got %T", w.Approval)
		}
	})

	t.Run("waterfall enabled wires policy and approval together", func(t *testing.T) {
		t.Setenv("PERMISSION_WATERFALL_ENABLED", "true")
		w := NewFromEnv(nil, nil, nil)
		if w.Policy == nil {
			t.Fatal("Policy must be wired when waterfall enabled")
		}
		if w.Approval == nil {
			t.Fatal("Approval must be wired when waterfall enabled; otherwise require_approval dead-ends into error-retry")
		}
	})
}

// 接线：PERMISSION_HOOK_URL 非空时 HookStage 必须装进瀑布且最后装配——
// 行为证据是"未命中默认 ask 的调用被 hook 收紧为 deny"。如果装配在
// 分类器之前、或压根没装，这次 Evaluate 会送审而不是否决。
func TestNewFromEnvHooksWiredLast(t *testing.T) {
	t.Setenv("PERMISSION_WATERFALL_ENABLED", "true")
	hits := 0
	hook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits++
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"decision":"deny","reason":"corp policy bans kubectl"}`))
	}))
	t.Cleanup(hook.Close)
	t.Setenv("PERMISSION_HOOK_URL", hook.URL)

	p := newPolicyFromEnv(nil, nil, nil)
	res, err := p.Evaluate(context.Background(), policy.Request{
		TenantID: "t1", RunID: "r1", NodeID: "n1",
		ToolName: "shell", Input: "kubectl get pods",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != policy.Deny || res.Layer != policy.LayerHook {
		t.Fatalf("hook must be wired last and tighten unmatched calls, got %v/%q", res.Decision, res.Layer)
	}
	if hits != 1 {
		t.Fatalf("hook endpoint must be consulted exactly once, got %d", hits)
	}
}

// 接线：PERMISSION_LEARN_ENABLED 开了但 store 缺席（NewFromEnv(nil,…) 的
// 单进程部署形态）——学习层必须静默不装而不是 panic，瀑布退回纯静态表：
// 未命中默认送审（与 P0/P1 一致），静态 deny 防线原样生效。
func TestNewFromEnvLearnEnabledWithoutStore(t *testing.T) {
	t.Setenv("PERMISSION_WATERFALL_ENABLED", "true")
	t.Setenv("PERMISSION_LEARN_ENABLED", "true")

	p := newPolicyFromEnv(nil, nil, nil)
	res, err := p.Evaluate(context.Background(), policy.Request{
		TenantID: "t1", RunID: "r1", NodeID: "n1",
		ToolName: "shell", Input: "kubectl get pods",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != policy.RequireApproval || res.Layer != policy.LayerDefault {
		t.Fatalf("static-only waterfall must keep the P0/P1 default, got %v/%q", res.Decision, res.Layer)
	}
	res, err = p.Evaluate(context.Background(), policy.Request{
		TenantID: "t1", RunID: "r1", NodeID: "n1",
		ToolName: "shell", Input: "rm -rf /",
	})
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != policy.Deny {
		t.Fatalf("static deny must survive the missing store, got %v", res.Decision)
	}
}

// splitList：保序、去空。Hook 端点按配置序全部咨询且审计的 RuleID 记
// 端点，map 去重会丢序——这里钉住保序语义。
func TestSplitList(t *testing.T) {
	got := splitList(" http://a:8080 , http://b:8080 ,, http://c:8080 ,")
	want := []string{"http://a:8080", "http://b:8080", "http://c:8080"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("order-preserving split: got %v want %v", got, want)
	}
	if got := splitList("  ,, "); len(got) != 0 {
		t.Fatalf("blank config must yield empty list, got %v", got)
	}
}

// ---- learnedRulesStage（docs §7 学习闭环的判定侧证据）----

func newPolicyMockStore(t *testing.T) (*store.MySQL, sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock new: %v", err)
	}
	return &store.MySQL{DB: db}, mock, func() { _ = db.Close() }
}

// TestLearnedRulesStage_LearnedAllowHitsL1 审批写回的 run 级 allow 必须
// 在 L1 放行同模式调用——否则 "always allow" 只放行了被审的那一次，
// 下一次同型调用再次送审，审批面被同一件事反复打扰。
// 用非 shell 工具（http_fetch）验证：L1 对非 shell 工具有放行资格，
// 归因字段（RuleID=source:tool:pattern）证明放行来自学来的规则。
func TestLearnedRulesStage_LearnedAllowHitsL1(t *testing.T) {
	s, mock, cleanup := newPolicyMockStore(t)
	defer cleanup()
	mock.ExpectQuery("FROM permission_rule").
		WithArgs("t1", "r1").
		WillReturnRows(sqlmock.NewRows([]string{
			"rule_id", "tenant_id", "run_id", "tool", "pattern", "effect", "source", "learned_by", "learned_at",
		}).AddRow("rule-1", "t1", "r1", "http_fetch", "https://reports.internal/*", "allow", "run", "ops@example.com", time.Now()))

	stage := &learnedRulesStage{Store: s, Base: policy.BuiltinRules()}
	res, hit, err := stage.Evaluate(context.Background(), policy.Request{
		TenantID: "t1", RunID: "r1", NodeID: "n1",
		ToolName: "http_fetch", Input: "https://reports.internal/q3",
	})
	if err != nil || !hit {
		t.Fatalf("hit=%v err=%v", hit, err)
	}
	if res.Decision != policy.Allow || res.Layer != policy.LayerRules {
		t.Fatalf("learned allow must be honored at L1, got %v/%q", res.Decision, res.Layer)
	}
	if res.RuleID != "run:http_fetch:https://reports.internal/*" {
		t.Fatalf("attribution must point at the learned rule, got %q", res.RuleID)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("rules must be loaded per (tenant, run) scope: %v", err)
	}
}

// TestLearnedRulesStage_DatabaseFailureKeepsStaticRules 查库失败必须退回
// 静态规则表，而不是把判定整体丢弃：learned allow 缺席只是多送审
// （安全方向）；静态 deny/allow 若跟着失效，一次 DB 抖动就会让
// "rm -rf /" 混过 L1，或让放行过的只读命令在故障期间全部转人工。
func TestLearnedRulesStage_DatabaseFailureKeepsStaticRules(t *testing.T) {
	s, mock, cleanup := newPolicyMockStore(t)
	defer cleanup()
	mock.ExpectQuery("FROM permission_rule").WillReturnError(errors.New("db down"))
	mock.ExpectQuery("FROM permission_rule").WillReturnError(errors.New("db down"))
	mock.ExpectQuery("FROM permission_rule").WillReturnError(errors.New("db down"))

	stage := &learnedRulesStage{Store: s, Base: policy.BuiltinRules()}
	ctx := context.Background()

	res, hit, err := stage.Evaluate(ctx, policy.Request{TenantID: "t1", RunID: "r1", ToolName: "shell", Input: "rm -rf /"})
	if err != nil || !hit {
		t.Fatalf("static deny: hit=%v err=%v", hit, err)
	}
	if res.Decision != policy.Deny {
		t.Fatalf("static deny must survive the lookup failure, got %v", res.Decision)
	}

	res, hit, err = stage.Evaluate(ctx, policy.Request{TenantID: "t1", RunID: "r1", ToolName: "shell", Input: "ls"})
	if err != nil || !hit {
		t.Fatalf("static allow: hit=%v err=%v", hit, err)
	}
	if res.Decision != policy.Allow {
		t.Fatalf("static allow must survive the lookup failure, got %v", res.Decision)
	}

	// 未命中按 miss 返回，交由瀑布后续层与默认送审兜底。
	_, hit, err = stage.Evaluate(ctx, policy.Request{TenantID: "t1", RunID: "r1", ToolName: "shell", Input: "kubectl get pods"})
	if err != nil || hit {
		t.Fatalf("unmatched must stay a miss for later layers: hit=%v err=%v", hit, err)
	}
}
