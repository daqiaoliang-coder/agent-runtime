// 本文件实现权限瀑布的 L1 硬规则层（docs/permission-classifier.md §3）：
// Rule 模型、来源分层、模式匹配与特异性排序。
//
// 模式匹配替换 CommandPolicy.DenyTokens 的子串匹配（§3.2），在两个方向上
// 同时收敛失真：
//   - 误杀：echo "rm -rf / is dangerous" —— 引号内是一整个 token，模式
//     [rm -rf /] 按词序不匹配 [echo "rm -rf / is dangerous"]；
//   - 误放：rm -rf --no-preserve-root / —— 模式全消费语义下不匹配任何
//     allow 规则，落到 ask 由人工判定。
//
// 匹配语义刻意按 Effect 不对称（宁可多问，不可错放）：
//   - deny/ask 规则：前缀匹配——模式匹配命令的前 N 个词即可命中
//     （"mkfs" 拦下 "mkfs /dev/sda"，"kubectl delete namespace" 拦下
//     带 namespace 名的完整调用），多余参数不影响命中；
//   - allow 规则：全消费匹配——命令的每个词都必须被模式覆盖
//     （尾缀 "*" 吃掉剩余全部词），绝不因"长得像"而放行。
package policy

import (
	"fmt"
	"strings"
)

// Rule 是一条硬规则。Tool 为工具名（"*" 匹配任意工具，大小写不敏感）；
// Pattern 按空白分词，支持 "*"（单独一词：末位匹配剩余全部词，其余位置
// 匹配恰好一词）与 "xxx*"（前缀词：匹配任一以 xxx 开头的词）；
// Effect 为判定效果；Source 标识来源（builtin | env，P2 扩展 tenant/run/learned）。
type Rule struct {
	Tool    string
	Pattern string
	Effect  Decision
	Source  string
}

// id 返回规则的稳定标识，用于 DecisionResult.RuleID 与审计。
func (r Rule) id() string {
	return r.Source + ":" + r.Tool + ":" + r.Pattern
}

// RuleSet 是有序规则表。匹配策略：同 Pattern 多来源取最严（§3.1），
// 不同 Pattern 按特异性（具体词数优先）排序——租户可用更具体的 allow
// 细化部署级 ask，反之亦然；特异性相同时取最严。
type RuleSet struct {
	rules []Rule
}

func NewRuleSet(rules []Rule) *RuleSet { return &RuleSet{rules: rules} }

func (rs *RuleSet) Rules() []Rule { return rs.rules }

// matchTokens 判断模式分词是否匹配命令分词。full 为 true 时要求全消费
// （allow 语义），否则前缀命中即可（deny/ask 语义）。
// 大小写不敏感：deny 方向多拦是安全的，allow 方向工具名/命令名本身
// 大小写惯例不敏感。
func matchTokens(pattern []string, tokens []string, full bool) bool {
	for i, pt := range pattern {
		last := i == len(pattern)-1
		if pt == "*" {
			if last {
				// 尾缀 *：匹配剩余全部词（含零个）。全消费语义下到此即完成。
				return true
			}
			if i >= len(tokens) {
				return false
			}
			continue // 恰好一词，继续按位对齐
		}
		if i >= len(tokens) {
			return false
		}
		if strings.HasSuffix(pt, "*") && len(pt) > 1 {
			if !strings.HasPrefix(tokens[i], pt[:len(pt)-1]) {
				return false
			}
			continue
		}
		if !strings.EqualFold(tokens[i], pt) {
			return false
		}
	}
	if full {
		// allow 语义：模式耗尽后命令不得还有未覆盖的词。
		return len(pattern) == len(tokens)
	}
	return true
}

// specificity 返回 (具体词数, 通配词数)。用于不同 Pattern 间的优先排序：
// 具体词数多者优先；相同时通配少者优先——"git status"（恰好匹配）比
// "git status *"（吃任意尾参）覆盖范围更窄，才是更具体的规则。
func specificity(r Rule) (int, int) {
	concrete, wildcards := 0, 0
	for _, pt := range strings.Fields(r.Pattern) {
		if pt == "*" {
			wildcards++
		} else {
			concrete++
		}
	}
	return concrete, wildcards
}

// vectorBest 返回单个词向量命中的最优规则：特异性优先（具体词数、
// 反向通配数），同特异性取最严效果（同 Pattern 多来源聚合即 §3.1 的
// "取最严"）。includeAllow=false 时排除 allow 规则（Inner 向量与 L1 裸
// 分词的 shell 工具：永不据此放行）。
func (rs *RuleSet) vectorBest(tool string, tokens []string, includeAllow bool) (Rule, bool) {
	tool = strings.ToLower(strings.TrimSpace(tool))
	var best Rule
	found := false
	var bestConcrete, bestWildcards int
	for _, r := range rs.rules {
		if r.Effect == Allow && !includeAllow {
			continue
		}
		if r.Tool != "*" && !strings.EqualFold(r.Tool, tool) {
			continue
		}
		if !matchRule(r, tokens) {
			continue
		}
		concrete, wildcards := specificity(r)
		better := !found ||
			concrete > bestConcrete ||
			(concrete == bestConcrete && wildcards < bestWildcards) ||
			(concrete == bestConcrete && wildcards == bestWildcards && decisionRank(r.Effect) > decisionRank(best.Effect))
		if better {
			best, found, bestConcrete, bestWildcards = r, true, concrete, wildcards
		}
	}
	return best, found
}

// Aggregate 是规则层对一次调用的完整判定（§2.1 短路语义的规则侧实现）：
//
//	segments 为顶层各段的词向量（shell 链式拆段后），inner 为命令替换/
//	复合命令内嵌调用的词向量（一定会执行，deny/ask 必须覆盖，永不作为
//	放行依据）；非 shell 工具只有一个 segment。
//
// 聚合口径（两级）：
//  1. 每段取 vectorBest（特异性优先）——具体 allow 可压过同段的泛化 ask，
//     这是"后者可细化前者"（§3.1）的落点；inner 段只取非 allow 命中；
//  2. 跨段取最严——任一段 deny 即整体 deny；任一段 ask 即整体 ask；
//     全部段的最优命中都是 allow 且 allowEligible 时整体 allow
//     （"一段未识别，整体不放行"，§4 防线 2）。
//
// allowEligible=false 时压制放行（L1 裸分词的 shell 工具、L2 非 Clean 形态）：
// deny/ask 命中照常生效，否则按未命中下泄。
func (rs *RuleSet) Aggregate(tool string, segments, inner [][]string, allowEligible bool) (Decision, Rule, bool) {
	var denyRule, askRule Rule
	hasDeny, hasAsk := false, false
	allAllow := allowEligible && len(segments) > 0
	for _, seg := range segments {
		r, ok := rs.vectorBest(tool, seg, true)
		if !ok || r.Effect != Allow {
			allAllow = false
		}
		switch {
		case ok && r.Effect == Deny && (!hasDeny || specificityGE(r, denyRule)):
			denyRule, hasDeny = r, true
		case ok && r.Effect == RequireApproval && !hasDeny && (!hasAsk || specificityGE(r, askRule)):
			askRule, hasAsk = r, true
		}
	}
	for _, in := range inner {
		r, ok := rs.vectorBest(tool, in, false)
		if !ok {
			continue
		}
		switch {
		case r.Effect == Deny && (!hasDeny || specificityGE(r, denyRule)):
			denyRule, hasDeny = r, true
		case r.Effect == RequireApproval && !hasDeny && (!hasAsk || specificityGE(r, askRule)):
			askRule, hasAsk = r, true
		}
	}
	switch {
	case hasDeny:
		return Deny, denyRule, true
	case hasAsk:
		return RequireApproval, askRule, true
	case allAllow:
		r, _ := rs.vectorBest(tool, segments[0], true)
		return Allow, r, true
	}
	return Allow, Rule{}, false
}

// specificityGE 报告 r 的特异性不低于 other（用于同效果命中的代表性选择）：
// 具体词数更多，或相同且通配更少。
func specificityGE(r, other Rule) bool {
	rc, rw := specificity(r)
	oc, ow := specificity(other)
	return rc > oc || (rc == oc && rw <= ow)
}

// matchRule 判断单条规则是否命中一组命令分词。
func matchRule(r Rule, tokens []string) bool {
	pattern := strings.Fields(r.Pattern)
	if len(pattern) == 0 || len(tokens) == 0 {
		return false
	}
	return matchTokens(pattern, tokens, r.Effect == Allow)
}

// ParseRulesEnv 解析 PERMISSION_RULES_ENV 注入的部署级规则
// （格式 "tool:pattern:effect" 逗号分隔；tool 可为 "*"；effect 取
// allow/require_approval/deny，别名 ask 等价 require_approval）。
// 任何非法条目都返回错误——L1 配置非法必须启动失败，不静默丢防线（§9）。
func ParseRulesEnv(s string) ([]Rule, error) {
	var rules []Rule
	for _, item := range strings.Split(s, ",") {
		item = strings.TrimSpace(item)
		if item == "" {
			continue
		}
		// 首个 ":" 定 tool，末个 ":" 定 effect，中间整体是 pattern——
		// pattern 可能自身含 ":"（如 URL 模式 https://api.internal/*），
		// 固定 SplitN 会把协议头误当分隔符。
		first := strings.Index(item, ":")
		last := strings.LastIndex(item, ":")
		if first < 0 || first == last {
			return nil, fmt.Errorf("invalid permission rule %q: want tool:pattern:effect", item)
		}
		tool := strings.TrimSpace(item[:first])
		pattern := strings.TrimSpace(item[first+1 : last])
		effectRaw := strings.TrimSpace(item[last+1:])
		if tool == "" || pattern == "" {
			return nil, fmt.Errorf("invalid permission rule %q: empty tool or pattern", item)
		}
		var effect Decision
		switch strings.ToLower(effectRaw) {
		case "allow":
			effect = Allow
		case "ask", "require_approval":
			effect = RequireApproval
		case "deny":
			effect = Deny
		default:
			return nil, fmt.Errorf("invalid permission rule %q: unknown effect %q", item, effectRaw)
		}
		rules = append(rules, Rule{Tool: tool, Pattern: pattern, Effect: effect, Source: "env"})
	}
	return rules, nil
}

// BuiltinRules 是 DefaultCommandPolicy 演进而来的保守默认（§3.1 builtin 层）：
// deny 集（前缀语义）对应旧 DenyTokens 的分词化——fork bomb 是函数声明，
// AST 归一化落入"未识别"交 L3/人工，不再单列；allow 集是 docs §4 防线 1
// 的只读白名单，保证确定性放行能力不依赖 LLM 存活。
func BuiltinRules() []Rule {
	deny := []string{
		"rm -rf /", "rm -fr /",
		"mkfs", "dd if=*",
		"shutdown", "reboot", "halt", "poweroff",
		"kubectl delete namespace", "kubectl delete ns",
	}
	allow := []string{
		"ls", "ls *", "pwd",
		"cat", "cat *", "grep", "grep *",
		"git status", "git status *",
		"npm view", "npm view *",
	}
	rules := make([]Rule, 0, len(deny)+len(allow))
	for _, p := range deny {
		rules = append(rules, Rule{Tool: "*", Pattern: p, Effect: Deny, Source: "builtin"})
	}
	for _, p := range allow {
		rules = append(rules, Rule{Tool: "*", Pattern: p, Effect: Allow, Source: "builtin"})
	}
	return rules
}
