// 权限瀑布的装配层（docs/permission-classifier.md §10 配置项）。
//
// 为什么在 worker 而不在 policy 包：环境变量读取、fail-fast 决策与
// 依赖注入（模型端口/护栏/记账）都是部署决策，policy 包保持纯逻辑、
// 可被任意壳层复用（与 newContextOptionsFromEnv 同一口径）。
package worker

import (
	"context"
	"log"
	"strings"

	"agent-runtime/internal/middleware"
	"agent-runtime/internal/obs"
	"agent-runtime/internal/policy"
	"agent-runtime/internal/providers"
	"agent-runtime/internal/store"
)

// newPolicyFromEnv 装配 Worker.Policy。
//
// 灰度语义：PERMISSION_WATERFALL_ENABLED 默认关闭，关闭时返回 nil——
// gate 维持接入前的休眠状态（NewFromEnv 此前从未装配 Policy，若默认挂
// CommandPolicy 反而会激活休眠的 gate、改变默认部署行为：deny token
// 子串开始杀节点、approval-tools 全部报错）。CommandPolicy 保留为库级
// 兼容入口（docs §3.2"保留兼容入口"），不进默认装配。
//
// L1 规则配置非法直接 log.Fatal（docs §9：fail-fast，不静默丢防线）——
// 与记忆等可选增强的降级语义不同，规则表是安全边界本身，带病运行的
// 代价是防线静默缺失。
//
// s 是审批学习规则的存储来源（PERMISSION_LEARN_ENABLED 开启时动态合并），
// 同时充当 L3 的记账端口；测试装配传 nil 时学习层静默不装。
func newPolicyFromEnv(mp providers.ModelProvider, chain *middleware.ModelChain, s *store.MySQL) policy.Policy {
	if !envBool("PERMISSION_WATERFALL_ENABLED", false) {
		return nil
	}
	envRules, err := policy.ParseRulesEnv(envString("PERMISSION_RULES_ENV", ""))
	if err != nil {
		log.Fatalf("permission: invalid PERMISSION_RULES_ENV: %v", err)
	}
	rules := policy.NewRuleSet(append(policy.BuiltinRules(), envRules...))

	// L1/L2：PERMISSION_LEARN_ENABLED 开启时换装动态规则层——审批写回的
	// run/tenant/learned 规则按 (tenant, run) 合并进静态表（§7"同模式后续
	// 调用 L1 直接 allow"）；关闭时纯静态层，零额外 DB 查询、行为与
	// P0/P1 一致。
	var l1l2 policy.Stage = &policy.RulesStage{Rules: rules}
	l2 := policy.Stage(&policy.ParserStage{Rules: rules})
	if envBool("PERMISSION_LEARN_ENABLED", false) {
		if s != nil {
			dyn := &learnedRulesStage{Store: s, Base: rules.Rules()}
			l1l2, l2 = dyn, dyn
		} else {
			obs.From(context.Background()).WarnContext(context.Background(),
				"PERMISSION_LEARN_ENABLED but store is not configured; learned rules disabled")
		}
	}
	stages := []policy.Stage{l1l2, l2}

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
				Usage:  s,
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

	// L4 外部 Hook（§6）：PERMISSION_HOOK_URL 缺省不启用；逗号分隔多端点，
	// 全部咨询、按 Chain 语义取最严。装配在最后——它前面的确定性层与
	// 分类器 ask/deny 短路时它不被咨询（"可收紧不可放宽"由层序保证）。
	if hooks := splitList(envString("PERMISSION_HOOK_URL", "")); len(hooks) > 0 {
		stages = append(stages, &policy.HookStage{
			Opt: policy.HookOptions{
				Endpoints: hooks,
				Timeout:   envDuration("PERMISSION_HOOK_TIMEOUT", policy.DefaultHookTimeout),
			},
		})
	}
	return policy.NewWaterfall(stages...)
}

// splitList 把逗号分隔的配置拆成有序去空列表。与 csvNames 的差异：
// Hook 端点要保序（审计的 RuleID 记端点，多实例语义按配置序全部咨询），
// map 会丢序。
func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
