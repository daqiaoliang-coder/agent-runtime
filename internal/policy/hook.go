// 本文件实现权限瀑布的 L4 外部 Hook（docs/permission-classifier.md §6）。
//
// Claude Code 的 Hook 是用户本机的子进程；分布式 Runtime 里 Worker 容器内
// 起子进程不可控、不可审计，因此对齐为 HTTP 策略服务：企业合规/风控/
// 审计旁路在这里注入判定。装配形态是瀑布的收紧层（RefiningStage）——
// 与 policy.Policy 同一挂载点的不同实现形态。
//
// 核心语义——可收紧不可放宽：
//   - Hook 收到瀑布携带下行的当前判定（decision_so_far），它只可能是
//     分类器 allow（推断性授权）或瀑布默认送审（未命中任何层）；
//   - 最终判定 = 最严(上游判定, 各 Hook 实例判定)。Hook 说 allow 而上游
//     是 ask 时维持 ask——"未识别 → 人工"是结构兜底，不因外部服务放行
//     而失效；
//   - 确定性层（L1/L2）的 allow/deny/ask 在瀑布里早于本层短路，Hook 根本
//     不被咨询——"Hook 的 allow 无法翻越 L1 deny"由顺序保证（§6），
//     不依赖 Hook 服务自觉。
//
// 失败语义（§9）：超时/非 200/非法 JSON/未知判定值，该实例一律按 ask
// 计入聚合——fail-closed，Hook 故障的代价是收紧为送审，绝不静默放行。
//
// 多实例：Endpoints 按配置序全部咨询、按 Chain 语义取最严（§6"多 Hook
// 实例按 Chain 语义取最严"）。代价是延迟按实例数串行累加（每实例默认
// 2s 上限）；合规旁路通常单实例，多实例留给需要多部门制衡的部署。
//
// 审计口径（§8）：请求把入参发给策略服务是 §6 的契约（它是受信判定方，
// 不是日志系统）；但 DecisionResult 只携带 Hook 的 reason，本层不打印
// 任何入参原文，reason 上限截断防止恶意/失控的 Hook 服务把超长文本
// 灌进 interrupt 与日志。
package policy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// DefaultHookTimeout 单个 Hook 实例的调用上限（§10：默认 2s）。
const DefaultHookTimeout = 2 * time.Second

// hookReasonMax 截断 Hook 返回的 reason：它最终会进 run_interrupt.reason
// （VARCHAR(1024)）与日志流，放任外部服务写超长文本既撑爆存储也污染审计。
const hookReasonMax = 512

// HookOptions 是 L4 的纯配置。
type HookOptions struct {
	// Endpoints 是策略服务端点列表（PERMISSION_HOOK_URL，逗号分隔）。
	// 为空时不应装配本层。
	Endpoints []string
	// Timeout 单实例调用上限；<=0 用默认值。
	Timeout time.Duration
}

func (o HookOptions) timeout() time.Duration {
	if o.Timeout <= 0 {
		return DefaultHookTimeout
	}
	return o.Timeout
}

// HookStage 实现 RefiningStage：对瀑布交给它的判定做外部复核。
type HookStage struct {
	Opt    HookOptions
	Client *http.Client // nil 时用 http.DefaultClient（超时由每次请求的 ctx 控制）
}

// hookRequest 是 §6 的请求契约。NormalizedInput 字段名对齐设计文档；
// 当前形态即工具入参字符串（L2 结构化产物是 AST 中间表示，序列化口径
// 留给后续需要时再定——先把调用送到合规服务，比字段形态完美更重要）。
type hookRequest struct {
	Event           string `json:"event"`
	TenantID        string `json:"tenant_id"`
	RunID           string `json:"run_id"`
	NodeID          string `json:"node_id"`
	Tool            string `json:"tool"`
	NormalizedInput string `json:"normalized_input"`
	Risk            string `json:"risk"`
	DecisionSoFar   string `json:"decision_so_far"`
}

type hookResponse struct {
	Decision string `json:"decision"`
	Reason   string `json:"reason"`
}

// Evaluate 让 HookStage 也能作为普通 Stage 组合（Chain 或瀑布之外）：
// 以"未命中默认送审"为 soFar 委托 Refine——独立形态下 Hook 只能把
// 默认 ask 收紧为 deny，不能放行，语义与瀑布内一致。瀑布内走 Refine
// （携带真实 soFar），本方法不会被调用。
func (s *HookStage) Evaluate(ctx context.Context, req Request) (DecisionResult, bool, error) {
	return s.Refine(ctx, req, defaultAskResult())
}

// Refine 逐实例咨询并按最严聚合。hit 恒为 true：Hook 一旦装配，它对到达
// 本层的每个调用都给出复核结论（哪怕结论是"维持原判"），瀑布在此收口。
// 返回值携带上游原判还是收紧结果，取决于谁 authored 最终判定——
// 收紧时 Layer=l4_hook + Hook 的 reason（审计要能回答"是合规服务拦的"）；
// 维持原判时原样返回 soFar（判定作者没变，layer/rule_id 保持溯源）。
func (s *HookStage) Refine(ctx context.Context, req Request, soFar DecisionResult) (DecisionResult, bool, error) {
	client := s.Client
	if client == nil {
		client = http.DefaultClient
	}
	final := soFar
	for _, ep := range s.Opt.Endpoints {
		d, reason, err := s.callOne(ctx, client, ep, req, soFar)
		if err != nil {
			// fail-closed：该实例按 ask 计入聚合。不向上抛错——运行时故障
			// 在瀑布里被收口为 ask，与 §9 的失败语义一致。
			d, reason = RequireApproval, "hook unreachable, fail-closed: "+err.Error()
		}
		if decisionRank(d) > decisionRank(final.Decision) {
			final = hookResult(d, ep, reason)
		}
	}
	return final, true, nil
}

// hookResult 构造收紧后的判定。Risk 与 L1 规则层同口径：deny=Critical、
// ask=High；RuleID 记端点——多实例部署下审计要能回答"哪个合规服务拦的"。
func hookResult(d Decision, endpoint, reason string) DecisionResult {
	res := DecisionResult{
		Decision: d,
		PolicyID: "permission-hook",
		Layer:    LayerHook,
		RuleID:   endpoint,
		Reason:   reason,
	}
	switch d {
	case Deny:
		res.Risk = RiskCritical
	default:
		res.Risk = RiskHigh
	}
	return res
}

// callOne 向单个端点发起一次咨询。任何不符合契约的响应都返回错误，
// 由调用方 fail-closed 为 ask。
func (s *HookStage) callOne(ctx context.Context, client *http.Client, endpoint string, req Request, soFar DecisionResult) (Decision, string, error) {
	cctx, cancel := context.WithTimeout(ctx, s.Opt.timeout())
	defer cancel()
	body, err := json.Marshal(hookRequest{
		Event:           "PreToolUse",
		TenantID:        req.TenantID,
		RunID:           req.RunID,
		NodeID:          req.NodeID,
		Tool:            req.ToolName,
		NormalizedInput: req.Input,
		Risk:            string(soFar.Risk),
		DecisionSoFar:   string(soFar.Decision),
	})
	if err != nil {
		return "", "", fmt.Errorf("marshal hook request: %w", err)
	}
	hreq, err := http.NewRequestWithContext(cctx, http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		return "", "", fmt.Errorf("build hook request: %w", err)
	}
	hreq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(hreq)
	if err != nil {
		return "", "", fmt.Errorf("call hook: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		// 3xx 也按失败处理：合规服务重定向到登录页等人类资源时，
		// 跟随过去拿到的 200 HTML 会在 JSON 解析处失败——不如在状态码
		// 这一层就 fail-closed，语义更直白。
		return "", "", fmt.Errorf("hook returned status %d", resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", "", fmt.Errorf("read hook response: %w", err)
	}
	var out hookResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return "", "", fmt.Errorf("hook response not valid JSON: %w", err)
	}
	var d Decision
	switch strings.ToLower(strings.TrimSpace(out.Decision)) {
	case "allow":
		d = Allow
	case "ask", "require_approval":
		d = RequireApproval
	case "deny":
		d = Deny
	default:
		return "", "", fmt.Errorf("hook returned unknown decision %q", out.Decision)
	}
	reason := strings.TrimSpace(out.Reason)
	if len(reason) > hookReasonMax {
		reason = reason[:hookReasonMax]
	}
	return d, reason, nil
}
