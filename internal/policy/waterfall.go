// 本文件实现权限瀑布的分层决策组合器（docs/permission-classifier.md §2）。
//
// 与 policy.Chain 的分工：Chain 把全部策略并列执行取最严，适合"独立策略
// 并列制衡"；瀑布要求**有序短路**——先确定性后概率性，省掉不必要的 LLM
// 调用（白名单命令在 LLM 网关整体宕机时照常执行，§11 验收口径 2）。
// Chain 不废弃，Stage 内部仍可以是 Chain。
//
// 短路规则（fail-closed，§2.1）：
//
//	任何层返回 Deny            → 立即终止，Deny
//	L1/L2 命中 allow           → 立即终止，Allow（确定性层有放行资格）
//	L3 分类器 allow            → 携带下行给 L4 收紧层复核，无收紧层则 Allow
//	其余一切（未命中/ask/失败） → REQUIRE_APPROVAL（L5 终局）
//
// 结构不变量：放行只能出自确定性层或分类器的明确授权推理；Deny 判定权
// 永远不经过 LLM——L1 的 deny 规则在分类器之前已经短路。L4 收紧层
// （RefiningStage）是唯一能拿到"当前判定"的层：只能收紧不能放宽，
// 确定性层的 allow 在它之前已短路，它连收紧的资格都没有。
package policy

import (
	"context"
	"strings"
)

// Stage 是瀑布的一层。返回 (判定结果, 是否命中)；未命中时瀑布继续向下，
// 由下一层接管。Evaluate 返回 error 仅用于编程错误——运行时故障（LLM 不可
// 用、解析失败等）都应内部消化为"未命中"或 ask 结果，不阻断瀑布。
type Stage interface {
	Evaluate(ctx context.Context, req Request) (DecisionResult, bool, error)
}

// StageFunc 让纯函数可直接作层使用。
type StageFunc func(ctx context.Context, req Request) (DecisionResult, bool, error)

func (f StageFunc) Evaluate(ctx context.Context, req Request) (DecisionResult, bool, error) {
	return f(ctx, req)
}

// RefiningStage 是瀑布的"收紧层"（L4 外部 Hook）。与普通层的差异：
// 它拿得到瀑布携带下行的当前判定（soFar），语义是复核而非独立裁决——
// 最终判定只能比 soFar 更严（可收紧不可放宽，§6）。soFar 的来源只有
// 两种：分类器 allow（携带下行的推断性授权）或瀑布默认送审（未命中）；
// 确定性层与分类器的 ask/deny 在它之前已短路，轮不到它复核。
type RefiningStage interface {
	Refine(ctx context.Context, req Request, soFar DecisionResult) (DecisionResult, bool, error)
}

// refinableAllow 标记"allow 需被下游收紧层复核"的层。只有 L3 分类器实现：
// 它的 allow 是 matched_grant 推理出的授权，外部合规有权收紧（§2.1
// "L3 分类器 allow → 继续走 L4"）；L1/L2 的 allow 是运维显式配置的
// 确定性授权，立即终局，Hook 无权收紧（"确定性层有放行资格"）。
type refinableAllow interface{ allowRefinable() bool }

func allowRefinable(s Stage) bool {
	r, ok := s.(refinableAllow)
	return ok && r.allowRefinable()
}

// 层标识（DecisionResult.Layer 的取值域，docs §8）。
const (
	LayerRules      = "l1_rules"
	LayerParser     = "l2_parser"
	LayerClassifier = "l3_classifier"
	LayerHook       = "l4_hook"
	LayerDefault    = "l5_default"
)

// Waterfall 按序短路组合各层，整体实现 Policy（注入 Worker.Policy，
// gate 位置沿用 policy-gateway：ClaimNode 之前、仅 NodeTool）。
type Waterfall struct{ stages []Stage }

func NewWaterfall(stages ...Stage) *Waterfall {
	return &Waterfall{stages: stages}
}

// hasRefinerAfter 报告位置 i 之后是否还有收紧层。没有收紧层时，
// 分类器 allow 按原语义立即返回——P0/P1 行为零变化（PERMISSION_HOOK_URL
// 缺省不启用时就是这条路径）。
func (w *Waterfall) hasRefinerAfter(i int) bool {
	for _, s := range w.stages[i+1:] {
		if _, ok := s.(RefiningStage); ok {
			return true
		}
	}
	return false
}

func (w *Waterfall) defaultResult() DecisionResult {
	return defaultAskResult()
}

// defaultAskResult 是瀑布的 L5 终局（未命中任何层的默认送审），也是
// 收紧层在"无携带判定"时的 soFar 基线。
func defaultAskResult() DecisionResult {
	return DecisionResult{
		Decision: RequireApproval, Risk: RiskMedium, PolicyID: "waterfall-default",
		Reason: "no deterministic rule matched and no classifier authorization; human approval required",
		Layer:  LayerDefault,
	}
}

func stageErrorResult(err error) DecisionResult {
	return DecisionResult{
		Decision: RequireApproval, Risk: RiskHigh, PolicyID: "waterfall-error",
		Reason: "stage error, fail-closed to approval: " + err.Error(), Layer: LayerDefault,
	}
}

func (w *Waterfall) Evaluate(ctx context.Context, req Request) (DecisionResult, error) {
	// carry 是携带下行的判定：分类器 allow 时不立即返回，等下游收紧层
	// 复核。收紧层未命中（hit=false）时 carry 在收尾处生效——"否则 Allow"。
	var carry *DecisionResult
	for i, s := range w.stages {
		if s == nil {
			continue
		}
		if r, ok := s.(RefiningStage); ok {
			soFar := w.defaultResult()
			if carry != nil {
				soFar = *carry
			}
			res, hit, err := r.Refine(ctx, req, soFar)
			if err != nil {
				// 层内错误按 fail-closed 收口为 ask：任何失败模式都不得落到 Allow（§9）。
				return stageErrorResult(err), nil
			}
			if hit {
				return res, nil
			}
			continue
		}
		res, hit, err := s.Evaluate(ctx, req)
		if err != nil {
			return stageErrorResult(err), nil
		}
		if !hit {
			continue
		}
		if res.Decision == Allow && allowRefinable(s) && w.hasRefinerAfter(i) {
			carry = &res
			continue
		}
		return res, nil
	}
	if carry != nil {
		return *carry, nil
	}
	// L5 终局：未命中任何层的调用一律送审——默认策略即边界之外的兜底。
	return w.defaultResult(), nil
}

// ---- L1 硬规则层（§3）----

// RulesStage 在裸分词上做规则匹配：非 shell 工具的完整判定层，
// shell 工具的快速 deny/ask 路径（放行必须等 L2 的结构化确认——
// $()/链式/重定向在裸分词下不可见，"npm install $(rm -rf /)" 的词向量
// 与合法安装无异）。
type RulesStage struct{ Rules *RuleSet }

func (s *RulesStage) Evaluate(_ context.Context, req Request) (DecisionResult, bool, error) {
	tokens := strings.Fields(strings.TrimSpace(req.Input))
	if len(tokens) == 0 {
		return DecisionResult{}, false, nil
	}
	allowEligible := !IsShellTool(req.ToolName)
	decision, rule, ok := s.Rules.Aggregate(req.ToolName, [][]string{tokens}, nil, allowEligible)
	if !ok {
		return DecisionResult{}, false, nil
	}
	return resultFromRule(decision, rule, LayerRules), true, nil
}

// ---- L2 命令解析防线（§4）----

// ParserStage 对 shell 家族工具做 AST 归一化后的结构化规则匹配：
// 链式拆段逐段判定（一段未识别整体不放行）、命令替换内嵌调用参与
// deny/ask（"cat $(rm -rf /)" 在此拦截）、非 Clean 形态（替换/展开/
// 重定向/未建模构造）压制放行并下泄 L3。
type ParserStage struct{ Rules *RuleSet }

func (s *ParserStage) Evaluate(_ context.Context, req Request) (DecisionResult, bool, error) {
	if !IsShellTool(req.ToolName) {
		return DecisionResult{}, false, nil
	}
	rep := AnalyzeShell(req.Input)
	if !rep.Parsed {
		// 防线 4：解析失败/方言不支持 → 交 L3，绝不静默放行。
		return DecisionResult{}, false, nil
	}
	segments := make([][]string, 0, len(rep.Segments))
	for _, seg := range rep.Segments {
		if t := seg.Tokens(); len(t) > 0 {
			segments = append(segments, t)
		}
	}
	inner := make([][]string, 0, len(rep.Inner))
	for _, seg := range rep.Inner {
		if t := seg.Tokens(); len(t) > 0 {
			inner = append(inner, t)
		}
	}
	decision, rule, ok := s.Rules.Aggregate(req.ToolName, segments, inner, rep.Clean())
	if !ok {
		return DecisionResult{}, false, nil
	}
	return resultFromRule(decision, rule, LayerParser), true, nil
}

// resultFromRule 把规则命中翻译为 DecisionResult。deny/ask 的风险分级
// 沿用 CommandPolicy 的口径（Critical/High）；allow 一律 Low——规则匹配
// 本身不区分细粒度风险，细粒度留给分类器与 Hook。
func resultFromRule(d Decision, r Rule, layer string) DecisionResult {
	res := DecisionResult{
		PolicyID: r.id(),
		RuleID:   r.id(),
		Layer:    layer,
	}
	switch d {
	case Deny:
		res.Decision, res.Risk = Deny, RiskCritical
		res.Reason = "matched deny rule (pattern: " + r.Pattern + ")"
	case RequireApproval:
		res.Decision, res.Risk = RequireApproval, RiskHigh
		res.Reason = "matched approval rule (pattern: " + r.Pattern + ")"
	default:
		res.Decision, res.Risk = Allow, RiskLow
		res.Reason = "matched allow rule (pattern: " + r.Pattern + ")"
	}
	return res
}
