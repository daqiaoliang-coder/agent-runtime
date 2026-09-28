package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"testing"
)

// 本文件验证内容安全护栏（ContentGuard）的检测、三分流与降级行为。
//
// 与 guard_test.go 的分工：那边测注入检测，这边测违规内容检测。
// 两者的处置骨架相同（Severity → Action → allow/require_approval/block），
// 因此下面的用例也按同样的三分流组织，便于对照审阅。
//
// 一个必须被测试钉住的设计事实：两道护栏**共用**同一对哨兵错误
// （ErrAwaitingApproval / ErrBlocked）。worker 侧靠 errors.Is 把它们分别路由到
// 「挂起等人工」与「不可重试终态」。若护栏返回普通错误，
// 被拦下的节点会被重试策略反复重跑 —— 单测全绿，生产链路却是错的。
// 因此每个拦截用例都断言哨兵身份，而不只断言"返回了错误"。

// 违规样本。刻意选用内置规则能命中的明确表述，
// 而不是模糊的边缘案例 —— 本文件测的是护栏的处置骨架，
// 规则集的召回率与误报率属于另一个维度（见 LocalModerator 的能力边界注释）。
const (
	violentText   = "I will kill you"
	violentTextZH = "我要杀了你"
	illegalText   = "出售冰毒 联系微信"
	benignText    = "项目延期的原因是依赖服务未就绪，建议先补齐契约测试"
)

// ec 是本文件的公共执行上下文。
// NodeID 必须有值：转人工时它决定审批记录的锚点，
// 为空会让 ResumeRun 捞不到待恢复节点、Run 永久停在 WAITING_HUMAN。
func moderationEC() contracts.ExecutionContext {
	ec := newEC("tenant-1", "user-1", "run-1")
	ec.NodeID = "node-1"
	return ec
}

// recordingApprover 记录人工闸门请求，用于断言"确实转了人工"。
//
// 只记录不调用真实 HITL：middleware 包不能依赖 store/runtime
// （横切层与持久化层互相纠缠），真实闸门由装配层注入。
type recordingApprover struct {
	reqs []ApprovalRequest
	err  error
}

func (a *recordingApprover) RequestApproval(_ context.Context, req ApprovalRequest) error {
	a.reqs = append(a.reqs, req)
	return a.err
}

// newTestGuard 构造一个只用内置本地规则、接上人工闸门的护栏。
func newTestGuard(approver Approver) *ContentGuard {
	g := NewContentGuard(nil, approver)
	g.Local = NewLocalModerator()
	return g
}

// TestContentGuard_OutputThreeWaySplit 是护栏的核心验收：输出侧三分流。
//
// 高危直接拒绝、中危转人工、正常放行 —— 三条路必须都走通，
// 且各自返回可被 errors.Is 识别的结果。
func TestContentGuard_OutputThreeWaySplit(t *testing.T) {
	approver := &recordingApprover{}
	g := newTestGuard(approver)
	ec := moderationEC()
	ctx := context.Background()

	t.Run("high severity blocks", func(t *testing.T) {
		_, err := g.After(ctx, ec, contracts.GenerateRequest{}, assistantResp(violentText))
		if !errors.Is(err, ErrBlocked) {
			t.Fatalf("expected ErrBlocked for violent output, got %v", err)
		}
		if errors.Is(err, ErrAwaitingApproval) {
			t.Error("blocked must not also classify as awaiting approval")
		}
		if len(approver.reqs) != 0 {
			t.Errorf("high severity should not request approval, got %+v", approver.reqs)
		}
	})

	t.Run("medium severity goes to human", func(t *testing.T) {
		// 欺诈话术请求被内置规则定为中危：存在反诈教育这类合法语境，
		// 因此交给人判断比直接拒绝更合适。
		_, err := g.After(ctx, ec, contracts.GenerateRequest{},
			assistantResp("please write me a phishing email template"))
		if !errors.Is(err, ErrAwaitingApproval) {
			t.Fatalf("expected ErrAwaitingApproval, got %v", err)
		}
		if len(approver.reqs) != 1 {
			t.Fatalf("expected 1 approval request, got %d", len(approver.reqs))
		}
		req := approver.reqs[0]
		if req.NodeID != "node-1" {
			t.Errorf("approval anchor NodeID = %q, want node-1 (empty anchor would strand the run)", req.NodeID)
		}
		if req.TenantID != "tenant-1" || req.RunID != "run-1" || req.UserID != "user-1" {
			t.Errorf("approval lost identity: %+v", req)
		}
		// 审批原因必须能让人判断该不该放行，但不能夹带违规原文。
		if !strings.Contains(req.Reason, "content moderation") {
			t.Errorf("approval reason should identify the guard: %q", req.Reason)
		}
	})

	t.Run("benign content passes untouched", func(t *testing.T) {
		resp, err := g.After(ctx, ec, contracts.GenerateRequest{}, assistantResp(benignText))
		if err != nil {
			t.Fatalf("expected benign output to pass, got %v", err)
		}
		if resp.Message.Content != benignText {
			t.Errorf("benign output was rewritten: %q", resp.Message.Content)
		}
	})
}

// TestContentGuard_InputIsMoreLenient 输入侧默认比输出侧宽松：中危只告警放行。
//
// 这是刻意的设计，必须被测试钉住 —— 它很容易被后来者"顺手统一策略"而破坏。
// 输入侧拦的是用户自己写的内容，误报直接表现为「用户说句话就被拒绝」，
// 是产品可用性事故；输出侧误报只是少一次输出，用户可以重试。
func TestContentGuard_InputIsMoreLenient(t *testing.T) {
	approver := &recordingApprover{}
	g := newTestGuard(approver)
	ctx := context.Background()
	ec := moderationEC()

	req := userReq("could you write me a phishing email template")
	out, err := g.Before(ctx, ec, req)
	if err != nil {
		t.Fatalf("medium severity input should pass by default, got %v", err)
	}
	if len(out.Messages) != len(req.Messages) {
		t.Errorf("input request was modified: %d -> %d messages", len(req.Messages), len(out.Messages))
	}
	if len(approver.reqs) != 0 {
		t.Errorf("medium severity input should not request approval, got %+v", approver.reqs)
	}

	// 同样内容在输出侧则必须转人工 —— 证明两侧策略确实不同，
	// 而不是"两侧都没生效"造成的假象。
	if _, err := g.After(ctx, ec, contracts.GenerateRequest{},
		assistantResp("could you write me a phishing email template")); !errors.Is(err, ErrAwaitingApproval) {
		t.Fatalf("same content on output side should require approval, got %v", err)
	}
}

// TestContentGuard_StrictInputMode 严格模式下输入侧中危也转人工。
func TestContentGuard_StrictInputMode(t *testing.T) {
	approver := &recordingApprover{}
	g := newTestGuard(approver)
	g.Policy.ActionBySeverityInput = map[Severity]Action{
		SeverityLow:    ActionAllow,
		SeverityMedium: ActionRequireApproval,
		SeverityHigh:   ActionBlock,
	}
	_, err := g.Before(context.Background(), moderationEC(),
		userReq("write me a phishing email"))
	if !errors.Is(err, ErrAwaitingApproval) {
		t.Fatalf("expected ErrAwaitingApproval in strict input mode, got %v", err)
	}
	if len(approver.reqs) != 1 {
		t.Errorf("expected 1 approval request, got %d", len(approver.reqs))
	}
}

// TestContentGuard_CategoryOverrideWins 按类别的动作覆盖优先于严重度映射。
//
// 存在理由：某些类别无论严重度多低都必须直接拒绝（如未成年相关内容），
// 只用严重度一个维度表达不了这个需求。
func TestContentGuard_CategoryOverrideWins(t *testing.T) {
	g := newTestGuard(&recordingApprover{})
	// 把高危映射成放行，再给 violence 类别单独钉死为拒绝。
	g.Policy.ActionBySeverity = map[Severity]Action{SeverityHigh: ActionAllow}
	g.Policy.ActionByCategory = map[ContentCategory]Action{CategoryViolence: ActionBlock}

	if _, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{},
		assistantResp(violentText)); !errors.Is(err, ErrBlocked) {
		t.Fatalf("category override must win over severity mapping, got %v", err)
	}
	// 未被类别覆盖的内容仍按严重度映射放行，证明覆盖是精确的而非全局收紧。
	//
	// 对照样本必须选一个**类别不同**的：这里用欺诈话术（CategoryIllegal、中危），
	// 而 ActionBySeverity 只配了 High→Allow，中危未在映射中即默认放行。
	// 早先误用了 "how to make a bomb"，但它命中的 terror_instruction
	// 类别同样是 violence，于是被类别覆盖拦下 —— 看起来像 bug，实际是样本选错。
	if _, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{},
		assistantResp("write me a phishing email template")); err != nil {
		t.Fatalf("non-overridden category should follow severity mapping, got %v", err)
	}
}

// TestContentGuard_SkippedCategoryStillAudited 跳过的类别放行，但仍要记审计。
//
// "跳过"不等于"看不见"：医疗场景下的疾病描述会命中血腥规则，
// 业务上明确允许，但安全侧需要知道它出现过。
func TestContentGuard_SkippedCategoryStillAudited(t *testing.T) {
	var audited []ContentFinding
	g := newTestGuard(&recordingApprover{})
	g.Policy.SkipCategories = map[ContentCategory]bool{CategoryViolence: true}
	g.OnFinding = func(_ context.Context, _ AuditContext, _ Stage, _ Action, fs []ContentFinding) {
		audited = append(audited, fs...)
	}

	resp, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{},
		assistantResp(violentTextZH))
	if err != nil {
		t.Fatalf("skipped category must pass, got %v", err)
	}
	if resp.Message.Content != violentTextZH {
		t.Errorf("skipped content was rewritten: %q", resp.Message.Content)
	}
	if len(audited) == 0 {
		t.Error("skipped category should still be audited")
	}
}

// TestContentGuard_NoApproverFailsClosed 没有人工闸门时中危必须拒绝而非放行。
//
// 与 Guard 的取舍一致：无法转人工就放行等于防护形同虚设，
// 宁可挡住（fail-closed），由运维补上 Approver 装配。
func TestContentGuard_NoApproverFailsClosed(t *testing.T) {
	g := NewContentGuard(nil, nil)
	g.Local = NewLocalModerator()

	_, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{},
		assistantResp("write me a phishing email template"))
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected fail-closed ErrBlocked without approver, got %v", err)
	}
	if !strings.Contains(err.Error(), "no approver configured") {
		t.Errorf("error should say why it was blocked: %v", err)
	}
}

// TestContentGuard_OutputNeverUsesNodeLevelBypass 是本次改造最重要的安全断言之一。
//
// 模型输出是**非确定性**的：放行了输出 A，重跑生成的是输出 B，而 B 从未被任何人看过。
// 若输出侧按节点放行（装配层最容易照搬输入侧写法的地方），
// 那一次豁免就变成「该节点此后所有输出的永久豁免通道」。
//
// 因此 Bypass 即使被装配，也只能作用于输入侧。
func TestContentGuard_OutputNeverUsesNodeLevelBypass(t *testing.T) {
	inputBypassed, outputBypassed := 0, 0
	g := newTestGuard(&recordingApprover{})
	g.Bypass = func(_ context.Context, _ contracts.ExecutionContext, stage Stage) bool {
		// 模拟一个"照搬输入侧写法"的错误装配：无条件放行。
		if stage == StageInput {
			inputBypassed++
		} else {
			outputBypassed++
		}
		return true
	}

	ctx, ec := context.Background(), moderationEC()

	// 输入侧：Bypass 生效，违规内容被放行（这是防死循环所需的正确行为）。
	if _, err := g.Before(ctx, ec, userReq(violentText)); err != nil {
		t.Fatalf("input side bypass should pass, got %v", err)
	}
	if inputBypassed != 1 {
		t.Errorf("expected input bypass consulted once, got %d", inputBypassed)
	}

	// 输出侧：即使 Bypass 返回 true，也必须照常审核并拦截。
	_, err := g.After(ctx, ec, contracts.GenerateRequest{}, assistantResp(violentText))
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("output side must NOT honor node-level bypass, got %v", err)
	}
	if outputBypassed != 0 {
		t.Errorf("output side must not consult node-level bypass at all, got %d calls", outputBypassed)
	}
}

// TestContentGuard_OutputBypassRequiresFingerprint 输出侧放行必须绑定内容指纹。
//
// 同一份内容放行一次即可（避免重复转人工），换一份内容必须重新审核。
func TestContentGuard_OutputBypassRequiresFingerprint(t *testing.T) {
	approved := map[string]bool{}
	g := newTestGuard(&recordingApprover{})
	g.OutputBypass = func(_ context.Context, _ contracts.ExecutionContext, fp string) bool {
		return approved[fp]
	}
	ctx, ec := context.Background(), moderationEC()

	// 未放行的指纹：照常拦截。
	if _, err := g.After(ctx, ec, contracts.GenerateRequest{}, assistantResp(violentText)); !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected block for unapproved fingerprint, got %v", err)
	}
	// 人工放行过这一份内容。
	approved[ContentFingerprint(violentText)] = true
	if _, err := g.After(ctx, ec, contracts.GenerateRequest{}, assistantResp(violentText)); err != nil {
		t.Fatalf("expected approved fingerprint to pass, got %v", err)
	}
	// 换一段不同的违规内容：指纹不同，必须重新拦截。
	if _, err := g.After(ctx, ec, contracts.GenerateRequest{}, assistantResp(illegalText)); !errors.Is(err, ErrBlocked) {
		t.Fatalf("a different output must be re-moderated, got %v", err)
	}
}

// TestContentFingerprint 指纹必须稳定、区分内容、且不泄露原文。
func TestContentFingerprint(t *testing.T) {
	a, b := ContentFingerprint(violentText), ContentFingerprint(violentText)
	if a != b {
		t.Errorf("fingerprint not stable: %q != %q", a, b)
	}
	if a == ContentFingerprint(illegalText) {
		t.Error("different content must yield different fingerprints")
	}
	if strings.Contains(a, violentText) {
		t.Errorf("fingerprint leaks content: %q", a)
	}
	// SHA-256 十六进制固定 64 字符；短哈希的碰撞会让两段内容共享同一条放行记录。
	if len(a) != 64 {
		t.Errorf("expected 64-char SHA-256 hex, got %d chars", len(a))
	}
}

// TestContentGuard_InputOnlyAuditsLastUserMessage 输入侧默认只审最后一条 user 消息。
//
// 全量审核历史会大面积误伤：历史里的工具结果可能引用网页正文
// （新闻报道里的暴力事件、医疗文档里的疾病描述），那是合法的数据引用。
func TestContentGuard_InputOnlyAuditsLastUserMessage(t *testing.T) {
	var seen []string
	g := newTestGuard(&recordingApprover{})
	// 用一个会记录被审文本的 Moderator，直接观察"哪些内容进了审核"。
	g.Moderator = ModeratorFunc(func(_ context.Context, req ModerationRequest) (ModerationVerdict, error) {
		seen = append(seen, req.Text)
		return ModerationVerdict{Service: "spy"}, nil
	})

	req := contracts.GenerateRequest{Messages: []contracts.Message{
		{Role: contracts.RoleSystem, Content: "你是安全助手，不要输出暴力内容"},
		{Role: contracts.RoleUser, Content: violentText}, // 历史里的违规内容
		{Role: contracts.RoleAssistant, Content: "我不能这样做"},
		{Role: contracts.RoleUser, Content: benignText}, // 当前输入
	}}
	if _, err := g.Before(context.Background(), moderationEC(), req); err != nil {
		t.Fatalf("before: %v", err)
	}
	if len(seen) != 1 || seen[0] != benignText {
		t.Errorf("expected only the last user message to be moderated, got %q", seen)
	}

	// 显式开启全量审核时，system 之外的消息都要审（system 是部署方自己写的，永远不审）。
	seen = nil
	g.ModerateAllMessages = true
	if _, err := g.Before(context.Background(), moderationEC(), req); err != nil {
		t.Fatalf("before (all): %v", err)
	}
	if len(seen) != 3 {
		t.Fatalf("expected 3 messages moderated, got %d: %q", len(seen), seen)
	}
	for _, s := range seen {
		if strings.Contains(s, "你是安全助手") {
			t.Errorf("system prompt must never be moderated: %q", s)
		}
	}
}

// TestContentGuard_ServiceFailureFallsBackToLocal 是降级路径的核心断言。
//
// 这里钉住的是一个**真实发生过的缺陷**：外部服务失败时，
// 原实现在错误分支里直接 continue 跳过了这段文本 —— 本地规则从未执行，
// 文档承诺的「降级但不裸奔」实际退化成完全放行。
// 这种失效在服务正常时永远测不出来，只有服务真挂了才暴露，
// 而那时正是最需要护栏的时候。
func TestContentGuard_ServiceFailureFallsBackToLocal(t *testing.T) {
	approver := &recordingApprover{}
	g := newTestGuard(approver)
	g.Moderator = ModeratorFunc(func(context.Context, ModerationRequest) (ModerationVerdict, error) {
		return ModerationVerdict{}, errors.New("moderation service unreachable")
	})
	// 默认策略即 fallback_local，此处显式写出以便阅读。
	g.Policy.OnServiceError = ServiceErrorFallbackLocal

	var failures int
	g.OnServiceFailure = func(context.Context, AuditContext, Stage, error) { failures++ }

	// 外部服务挂了，但违规内容仍必须被本地规则拦下。
	_, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{},
		assistantResp(violentTextZH))
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("local fallback must still block violent content, got %v", err)
	}
	// 降级必须可观测：否则"外部服务已经挂了几天、一直在用本地弱规则"完全不可见。
	if failures == 0 {
		t.Error("service failure must be reported via OnServiceFailure")
	}
}

// TestContentGuard_ServiceFailureFailClosed fail_closed 策略下服务故障即拒绝，
// 且错误必须同时是 ErrBlocked（不可重试）与 ErrModerationUnavailable（可区分故障）。
func TestContentGuard_ServiceFailureFailClosed(t *testing.T) {
	g := newTestGuard(&recordingApprover{})
	g.Moderator = ModeratorFunc(func(context.Context, ModerationRequest) (ModerationVerdict, error) {
		return ModerationVerdict{}, errors.New("moderation service unreachable")
	})
	g.Policy.OnServiceError = ServiceErrorFailClosed

	_, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{}, assistantResp(benignText))
	if !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected ErrBlocked under fail_closed, got %v", err)
	}
	// 必须能与"内容违规"区分开：前者服务恢复后同样内容就能过，后者重试无意义。
	if !errors.Is(err, ErrModerationUnavailable) {
		t.Errorf("expected ErrModerationUnavailable to distinguish outage from violation, got %v", err)
	}
}

// TestContentGuard_ServiceFailureFailOpen 放行，但必须留痕。
//
// fail_open 是唯一会让内容完全不受审的分支，因此可观测性不是可选项。
func TestContentGuard_ServiceFailureFailOpen(t *testing.T) {
	g := newTestGuard(&recordingApprover{})
	g.Moderator = ModeratorFunc(func(context.Context, ModerationRequest) (ModerationVerdict, error) {
		return ModerationVerdict{}, errors.New("moderation service unreachable")
	})
	g.Policy.OnServiceError = ServiceErrorFailOpen

	var failures int
	g.OnServiceFailure = func(context.Context, AuditContext, Stage, error) { failures++ }

	resp, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{}, assistantResp(violentText))
	if err != nil {
		t.Fatalf("expected fail_open to pass, got %v", err)
	}
	if resp.Message.Content != violentText {
		t.Errorf("fail_open should not rewrite content, got %q", resp.Message.Content)
	}
	if failures == 0 {
		t.Error("fail_open must report the outage, otherwise the naked window is invisible")
	}
}

// TestContentGuard_RespectsServiceBlocked 外部服务判定必须拦截时，尊重它的综合判定。
//
// 外部服务通常有自己的加权逻辑（多标签、账号风险等级），
// 它给出 block 时本层不该用自己的严重度映射去覆盖一个更了解内容的判定。
func TestContentGuard_RespectsServiceBlocked(t *testing.T) {
	g := newTestGuard(&recordingApprover{})
	g.Moderator = ModeratorFunc(func(_ context.Context, req ModerationRequest) (ModerationVerdict, error) {
		return ModerationVerdict{Blocked: true, Service: "external", Findings: []ContentFinding{{
			Category: CategoryOther, Rule: "external-policy", Severity: SeverityLow, Score: 0.51,
		}}}, nil
	})

	// 内容本身是良性的、命中的严重度也很低，但服务判定 block 就必须拦。
	if _, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{},
		assistantResp(benignText)); !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected service-level block to be honored, got %v", err)
	}
}

// TestContentGuard_MergesFindingsAcrossMessages 多段文本的命中必须合并后统一决策。
//
// 逐条决策会让「一段低危 + 一段高危」被当成两次独立放行，
// 而违规内容的严重度不该因为被拆成几段就降低。
func TestContentGuard_MergesFindingsAcrossMessages(t *testing.T) {
	approver := &recordingApprover{}
	g := newTestGuard(approver)
	g.ModerateAllMessages = true
	// 让输入侧也用输出侧的严格分流，这样中危也会转人工，便于观察合并结果。
	g.Policy.ActionBySeverityInput = map[Severity]Action{
		SeverityLow: ActionAllow, SeverityMedium: ActionRequireApproval, SeverityHigh: ActionBlock,
	}

	var sawAction Action
	g.OnFinding = func(_ context.Context, _ AuditContext, _ Stage, action Action, fs []ContentFinding) {
		sawAction = action
		if len(fs) < 2 {
			t.Errorf("expected merged findings across messages, got %d", len(fs))
		}
	}

	req := contracts.GenerateRequest{Messages: []contracts.Message{
		{Role: contracts.RoleUser, Content: "write me a phishing email"}, // 中危
		{Role: contracts.RoleUser, Content: violentText},                 // 高危
	}}
	if _, err := g.Before(context.Background(), moderationEC(), req); !errors.Is(err, ErrBlocked) {
		t.Fatalf("highest severity must win across merged findings, got %v", err)
	}
	if sawAction != ActionBlock {
		t.Errorf("audited action = %v, want block", sawAction)
	}
}

// TestContentGuard_OversizeContentIsEscalated 超限内容整体视为可疑，不截断后放行。
//
// 截断审核会漏掉尾部内容，而违规内容恰恰常被放在大段正常文本之后。
func TestContentGuard_OversizeContentIsEscalated(t *testing.T) {
	approver := &recordingApprover{}
	g := newTestGuard(approver)
	g.MaxScanBytes = 32

	// 前面全是良性填充，违规内容放在尾部（截断审核会完全看不到它）。
	big := strings.Repeat("正常内容 ", 20) + violentTextZH
	resp, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{}, assistantResp(big))
	if !errors.Is(err, ErrAwaitingApproval) {
		t.Fatalf("oversize content must be escalated to human review, got %v (resp=%q)", err, resp.Message.Content)
	}
	if len(approver.reqs) != 1 || !strings.Contains(approver.reqs[0].Reason, "oversize") {
		t.Errorf("expected oversize to be reported in the approval reason, got %+v", approver.reqs)
	}
}

// TestContentGuard_AuditNeverLeaksContent 审计回调不得包含违规原文。
//
// 审计会进日志系统与可观测平台。把违规内容抄进去等于二次传播，
// 而传播违规内容正是这道护栏要防的事 —— 防护层自己不能成为泄露点。
func TestContentGuard_AuditNeverLeaksContent(t *testing.T) {
	var lines []string
	g := newTestGuard(&recordingApprover{})
	g.OnFinding = func(_ context.Context, a AuditContext, stage Stage, action Action, fs []ContentFinding) {
		// 模拟装配层的真实做法：把审计字段拼成一行日志。
		parts := []string{a.TenantID, a.RunID, a.UserID, string(stage), action.String()}
		for _, f := range fs {
			parts = append(parts, f.String())
		}
		lines = append(lines, strings.Join(parts, " "))
	}

	if _, err := g.After(context.Background(), moderationEC(), contracts.GenerateRequest{},
		assistantResp(violentTextZH)); !errors.Is(err, ErrBlocked) {
		t.Fatalf("expected block, got %v", err)
	}
	if len(lines) == 0 {
		t.Fatal("expected an audit line")
	}
	for _, line := range lines {
		if strings.Contains(line, violentTextZH) || strings.Contains(line, "杀") {
			t.Errorf("audit line leaks moderated content: %s", line)
		}
	}
	// 审计仍须包含定位所需的信息，否则"不泄露"是以"没用"为代价换来的。
	if !strings.Contains(lines[0], "tenant-1") || !strings.Contains(lines[0], "violence") {
		t.Errorf("audit line lost identity or category: %s", lines[0])
	}
}

// TestContentGuard_DisabledSidesAreTransparent 关闭的一侧必须完全透明。
func TestContentGuard_DisabledSidesAreTransparent(t *testing.T) {
	g := newTestGuard(&recordingApprover{})
	ctx, ec := context.Background(), moderationEC()

	g.ModerateOutput = false
	resp, err := g.After(ctx, ec, contracts.GenerateRequest{}, assistantResp(violentText))
	if err != nil || resp.Message.Content != violentText {
		t.Errorf("disabled output side must be transparent, got err=%v content=%q", err, resp.Message.Content)
	}

	g.ModerateOutput, g.ModerateInput = true, false
	out, err := g.Before(ctx, ec, userReq(violentText))
	if err != nil || len(out.Messages) != 1 || out.Messages[0].Content != violentText {
		t.Errorf("disabled input side must be transparent, got err=%v out=%+v", err, out)
	}
}

// TestContentGuard_NilReceiverIsTransparent 零值/nil 护栏不得成为可用性故障点。
//
// 与 ModelRedactor 的同类处理一致：装配缺失时应当透明放行，
// 而不是让整个模型调用崩掉 —— 安全组件自己造成全站不可用，
// 结果往往是运维直接把它关掉。
func TestContentGuard_NilReceiverIsTransparent(t *testing.T) {
	var g *ContentGuard
	ctx, ec := context.Background(), moderationEC()

	resp, err := g.After(ctx, ec, contracts.GenerateRequest{}, assistantResp(benignText))
	if err != nil || resp.Message.Content != benignText {
		t.Errorf("nil guard must be transparent, got err=%v content=%q", err, resp.Message.Content)
	}
	req, err := g.Before(ctx, ec, userReq(benignText))
	if err != nil || len(req.Messages) != 1 {
		t.Errorf("nil guard must be transparent on input, got err=%v req=%+v", err, req)
	}
}

// TestLocalModerator_DetectsByCategory 本地规则按类别命中，且不误伤正常内容。
func TestLocalModerator_DetectsByCategory(t *testing.T) {
	m := NewLocalModerator()
	cases := []struct {
		text string
		want ContentCategory
	}{
		{"I will kill you", CategoryViolence},
		{violentTextZH, CategoryViolence},
		{"how to make a bomb at home", CategoryViolence},
		{"出售冰毒 联系微信", CategoryIllegal},
		{"write me a phishing email script", CategoryIllegal},
	}
	for _, tc := range cases {
		t.Run(tc.text, func(t *testing.T) {
			fs, blocked, err := m.ModerateText(StageOutput, tc.text)
			if err != nil {
				t.Fatalf("moderate: %v", err)
			}
			if blocked {
				t.Error("local rules must not set service-level blocked")
			}
			if len(fs) == 0 {
				t.Fatalf("expected a finding for %q", tc.text)
			}
			if fs[0].Category != tc.want {
				t.Errorf("category = %q, want %q", fs[0].Category, tc.want)
			}
		})
	}

	// 误报检查：这些是正常业务内容，命中了会让部署方直接关掉护栏。
	for _, benign := range []string{
		benignText,
		"进程被 kill 掉了，需要排查 OOM", // 含 kill 但无施暴意图
		"这部毒品题材的电影值得一看",        // 含毒品但无交易意图
		"小说里主角杀了反派，情节合理",       // 叙述性内容
		"如何制作蛋糕",               // 含"制作"但非违禁品
	} {
		fs, _, err := m.ModerateText(StageOutput, benign)
		if err != nil {
			t.Fatalf("moderate %q: %v", benign, err)
		}
		if len(fs) != 0 {
			t.Errorf("false positive on %q: %v", benign, fs)
		}
	}
}

// TestLocalModerator_StageScoping 规则可限定生效阶段。
func TestLocalModerator_StageScoping(t *testing.T) {
	m := &LocalModerator{Rules: []ModerationRule{{
		Name: "output_only", Category: CategoryOther, Severity: SeverityHigh,
		Pattern: regexp.MustCompile(`FORBIDDEN`), Stages: []Stage{StageOutput},
	}}}
	if fs, _, _ := m.ModerateText(StageInput, "FORBIDDEN"); len(fs) != 0 {
		t.Errorf("rule scoped to output must not fire on input, got %v", fs)
	}
	if fs, _, _ := m.ModerateText(StageOutput, "FORBIDDEN"); len(fs) != 1 {
		t.Errorf("rule scoped to output must fire on output, got %v", fs)
	}
}

// TestLocalModerator_FindingsAreStable 命中顺序必须稳定可复现。
//
// 审计日志若每次顺序不同，就无法做基于日志的聚合与告警比对。
func TestLocalModerator_FindingsAreStable(t *testing.T) {
	m := NewLocalModerator()
	text := violentTextZH + " 出售冰毒 " + violentTextZH
	first, _, _ := m.ModerateText(StageOutput, text)
	for i := 0; i < 20; i++ {
		again, _, _ := m.ModerateText(StageOutput, text)
		if fmt.Sprint(again) != fmt.Sprint(first) {
			t.Fatalf("findings not stable across runs:\nfirst=%v\nagain=%v", first, again)
		}
	}
	if len(first) < 3 {
		t.Fatalf("expected at least 3 findings, got %d", len(first))
	}
}

// TestContentGuard_ModelInterfaceSatisfied 编译期契约：护栏必须满足 Model 接口，
// 否则 ModelChain 根本装不进它 —— 挂载点存在而中间件接不上，等于没有护栏。
func TestContentGuard_ModelInterfaceSatisfied(t *testing.T) {
	var _ Model = (*ContentGuard)(nil)
	var _ Moderator = (*LocalModerator)(nil)

	chain := NewModelChain(NewContentGuard(nil, nil))
	if chain == nil {
		t.Fatal("content guard must be accepted by ModelChain")
	}
}

// assistantResp 构造模型响应。
func assistantResp(content string) contracts.GenerateResponse {
	return contracts.GenerateResponse{
		Message: contracts.Message{Role: contracts.RoleAssistant, Content: content},
		Model:   "gpt-4o",
	}
}

// userReq 构造只含一条 user 消息的模型请求。
func userReq(content string) contracts.GenerateRequest {
	return contracts.GenerateRequest{
		Model:    "gpt-4o",
		Messages: []contracts.Message{{Role: contracts.RoleUser, Content: content}},
	}
}
