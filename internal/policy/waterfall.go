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
//	L3 分类器 allow            → 终止，Allow（L4 Hook 为 P2，落地后在此收紧）
//	其余一切（未命中/ask/失败） → REQUIRE_APPROVAL（L5 终局）
//
// 结构不变量：放行只能出自确定性层或分类器的明确授权推理；Deny 判定权
// 永远不经过 LLM——L1 的 deny 规则在分类器之前已经短路。
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

// 层标识（DecisionResult.Layer 的取值域，docs §8）。
const (
	LayerRules      = "l1_rules"
	LayerParser     = "l2_parser"
	LayerClassifier = "l3_classifier"
	LayerDefault    = "l5_default"
)

// Waterfall 按序短路组合各层，整体实现 Policy（注入 Worker.Policy，
// gate 位置沿用 policy-gateway：ClaimNode 之前、仅 NodeTool）。
type Waterfall struct{ stages []Stage }

func NewWaterfall(stages ...Stage) *Waterfall {
	return &Waterfall{stages: stages}
}

func (w *Waterfall) Evaluate(ctx context.Context, req Request) (DecisionResult, error) {
	for _, s := range w.stages {
		if s == nil {
			continue
		}
		res, hit, err := s.Evaluate(ctx, req)
		if err != nil {
			// 层内错误按 fail-closed 收口为 ask：任何失败模式都不得落到 Allow（§9）。
			return DecisionResult{
				Decision: RequireApproval, Risk: RiskHigh, PolicyID: "waterfall-error",
				Reason: "stage error, fail-closed to approval: " + err.Error(), Layer: LayerDefault,
			}, nil
		}
		if hit {
			return res, nil
		}
	}
	// L5 终局：未命中任何层的调用一律送审——默认策略即边界之外的兜底。
	return DecisionResult{
		Decision: RequireApproval, Risk: RiskMedium, PolicyID: "waterfall-default",
		Reason: "no deterministic rule matched and no classifier authorization; human approval required",
		Layer:  LayerDefault,
	}, nil
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
