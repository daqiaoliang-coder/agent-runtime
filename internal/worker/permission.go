// 权限瀑布的装配层（docs/permission-classifier.md §10 配置项）。
//
// 为什么在 worker 而不在 policy 包：环境变量读取、fail-fast 决策与
// 依赖注入（模型端口/护栏/记账）都是部署决策，policy 包保持纯逻辑、
// 可被任意壳层复用（与 newContextOptionsFromEnv 同一口径）。
//
// 灰度语义：PERMISSION_WATERFALL_ENABLED 默认关闭，旁路时回退
// CommandPolicy（docs §3.2"保留兼容入口"）——不开瀑布的部署行为不变。
package worker

import (
	"context"
	"log"
	"strings"

	"agent-runtime/internal/executor"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/obs"
	"agent-runtime/internal/policy"
	"agent-runtime/internal/providers"
)

// newPolicyFromEnv 装配 Worker.Policy。
//
// L1 规则配置非法直接 log.Fatal（docs §9：fail-fast，不静默丢防线）——
// 与记忆等可选增强的降级语义不同，规则表是安全边界本身，带病运行的
// 代价是防线静默缺失。
func newPolicyFromEnv(mp providers.ModelProvider, chain *middleware.ModelChain, usage executor.UsageRecorder) policy.Policy {
	if !envBool("PERMISSION_WATERFALL_ENABLED", false) {
		return policy.DefaultCommandPolicy()
	}
	envRules, err := policy.ParseRulesEnv(envString("PERMISSION_RULES_ENV", ""))
	if err != nil {
		log.Fatalf("permission: invalid PERMISSION_RULES_ENV: %v", err)
	}
	rules := policy.NewRuleSet(append(policy.BuiltinRules(), envRules...))

	stages := []policy.Stage{
		&policy.RulesStage{Rules: rules},
		&policy.ParserStage{Rules: rules},
	}

	// L3 分类器：默认关闭；开启但模型未配置时不装配。docs §10"缺省复用
	// 主模型"在当前装配里没有稳定取值——主推理模型来自 Run/节点配置，
	// env 层无从预知（与压缩摘要不同，那里有 llm_usage 锚点可回退），
	// 故 PERMISSION_CLASSIFIER_MODEL 是显式必填项。
	if envBool("PERMISSION_CLASSIFIER_ENABLED", false) {
		if m := envString("PERMISSION_CLASSIFIER_MODEL", ""); m != "" {
			denyPatterns := make([]string, 0)
			for _, r := range rules.Rules() {
				if r.Effect == policy.Deny {
					denyPatterns = append(denyPatterns, r.Pattern)
				}
			}
			grant := envString("PERMISSION_GRANT_BOUNDARY", "")
			if strings.TrimSpace(grant) == "" {
				obs.From(context.Background()).WarnContext(context.Background(),
					"PERMISSION_CLASSIFIER_ENABLED but PERMISSION_GRANT_BOUNDARY is empty; classifier allows are impossible (fail-closed to approval)")
			}
			stages = append(stages, &policy.ClassifierStage{
				Model:  mp,
				Chain:  chain,
				Usage:  usage,
				Pricer: DefaultPricer,
				Opt: policy.ClassifierOptions{
					Model:         m,
					GrantBoundary: grant,
					DenyPatterns:  denyPatterns,
					Timeout:       envDuration("PERMISSION_CLASSIFIER_TIMEOUT", policy.DefaultClassifierTimeout),
					CacheTTL:      envDuration("PERMISSION_DECISION_CACHE_TTL", policy.DefaultDecisionCacheTTL),
				},
			})
		} else {
			obs.From(context.Background()).WarnContext(context.Background(),
				"PERMISSION_CLASSIFIER_ENABLED but PERMISSION_CLASSIFIER_MODEL is empty; L3 classifier disabled")
		}
	}
	return policy.NewWaterfall(stages...)
}
