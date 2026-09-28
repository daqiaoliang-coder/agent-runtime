// 动态学习规则层（docs/permission-classifier.md §7）：把审批写回的
// run/tenant/learned 规则按 (tenant, run) 合并进静态规则表。
//
// 为什么在 worker 而不在 policy 包：policy 包刻意不持有持久化（包注释），
// 学习规则的读取需要 store；匹配语义（特异性排序、deny 免竞争、
// allow 全消费）全部复用 policy 的 RulesStage/ParserStage，本文件只做
// "查库 → 合并 → 委托"。
//
// 一个 stage 同时承担 L1/L2 语义（内部先 RulesStage 后 ParserStage，
// 与静态瀑布的层序完全一致），换取每次判定只查一次库——两次语义
// 分两个 stage 会翻倍 DB 查询，而待审队列是低频人工路径、
// permission_rule 表极小，单次索引查询的代价可以接受。
package worker

import (
	"context"

	"agent-runtime/internal/obs"
	"agent-runtime/internal/policy"
	"agent-runtime/internal/store"
)

// learnedRulesStage 实现 policy.Stage：判定时把 (tenant, run) 作用域的
// 学习规则合并进静态基础规则，再走 L1 → L2 的既有匹配语义。
type learnedRulesStage struct {
	Store *store.MySQL
	// Base 是 builtin + env 的静态规则（启动时装配，判定时只读）。
	Base []policy.Rule
}

func (s *learnedRulesStage) Evaluate(ctx context.Context, req policy.Request) (policy.DecisionResult, bool, error) {
	set, err := s.mergedRuleSet(ctx, req.TenantID, req.RunID)
	if err != nil {
		// 查库失败退回静态规则表：learned allow 缺席 → 多送审（安全方向）；
		// learned deny 缺席 → 基础 deny 仍生效、未覆盖的回落送审（人审兜底）。
		// 绝不因学习层故障把已配置的确定性防线整体丢弃——那会把
		// "查库失败"放大成"全量 allow"（瀑布默认对命中不了 deny 的调用
		// 只能给 ask，但静态 allow/deny 的既有判定也必须保住）。
		obs.From(ctx).WarnContext(ctx, "learned rules lookup failed; falling back to static rules only",
			"tenant_id", req.TenantID, "run_id", req.RunID, "node_id", req.NodeID, "error", err)
		set = policy.NewRuleSet(s.Base)
	}
	// L1 语义（裸分词）：命中即收口，与静态瀑布的层序一致。
	if res, hit, err := (&policy.RulesStage{Rules: set}).Evaluate(ctx, req); hit || err != nil {
		return res, hit, err
	}
	// L1 未命中 → L2 语义（AST 归一化、分段判定）。
	return (&policy.ParserStage{Rules: set}).Evaluate(ctx, req)
}

// mergedRuleSet 返回静态基础规则 + (tenant, run) 学习规则的合并表。
// 跨来源聚合按 deny > ask > allow（policy.RuleSet.Aggregate），租户的
// "always allow" 绝不可能翻越部署级 deny——写回只在审批面发生，
// 判定侧没有第二条路。
func (s *learnedRulesStage) mergedRuleSet(ctx context.Context, tenant, runID string) (*policy.RuleSet, error) {
	learned, err := s.Store.ListActivePermissionRules(ctx, tenant, runID)
	if err != nil {
		return nil, err
	}
	if len(learned) == 0 {
		return policy.NewRuleSet(s.Base), nil
	}
	merged := make([]policy.Rule, 0, len(s.Base)+len(learned))
	merged = append(merged, s.Base...)
	for _, r := range learned {
		// effect 的取值域在写入端（approval 服务）校验；这里信任库内数据，
		// 非法值会因不等于任何 Decision 常量而在聚合中按未命中处理（fail-closed）。
		merged = append(merged, policy.Rule{Tool: r.Tool, Pattern: r.Pattern, Effect: policy.Decision(r.Effect), Source: r.Source})
	}
	return policy.NewRuleSet(merged), nil
}
