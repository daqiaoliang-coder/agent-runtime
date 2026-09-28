package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// 本文件实现「内容安全护栏」：检测模型输入与输出中的违规内容（暴恐、色情、辱骂、
// 违法交易等），并按严重度分流处置。
//
// 与 guard.go（不可信输入防护）的分工必须分清，两者常被混为一谈：
//
//	guard.go     防的是「注入」——不可信数据被执行引擎当成指令。
//	             威胁是**控制流被劫持**，判据是文本结构（指令覆盖、分隔符逃逸）。
//	moderation   防的是「违规内容」——内容本身违法违规，无论它是否试图控制什么。
//	             威胁是**内容合规风险**，判据是语义类别（暴恐、色情、辱骂）。
//
// 一段纯粹的暴恐描述不含任何注入特征，guard 放过它是对的；
// 一句 "ignore previous instructions" 不违规，moderation 放过它也是对的。
// 两者互补而非重叠，因此各自独立成文件、独立开关、独立规则集。
//
// 复用的部分同样重要：Severity / Action / Threat / Approver / ErrAwaitingApproval /
// ErrBlocked 全部沿用 guard.go 的定义。这不只是少写代码 ——
// worker.Handle 已按 errors.Is 把这两个哨兵分别路由到「转人工挂起」与「不可重试终态」，
// 另立一套错误会让护栏的拦截在 worker 侧退化成普通失败被反复重试，
// 那正是 guard.go 反复警告的失效方式。

// ContentCategory 是违规内容的语义类别。
//
// 类别是策略配置的单位：不同类别的风险性质不同，处置也应当不同。
// 例如「违法交易」通常直接拒绝，而「辱骂」在多数业务里更适合转人工或替换处理。
// 把所有违规揉成一个严重度维度，会让策略只能一刀切。
type ContentCategory string

const (
	CategoryViolence ContentCategory = "violence" // 暴恐：暴力威胁、恐怖主义、极端内容
	CategoryPorn     ContentCategory = "porn"     // 色情：露骨性内容、未成年相关
	CategoryAbuse    ContentCategory = "abuse"    // 辱骂：人身攻击、歧视、仇恨言论
	CategoryIllegal  ContentCategory = "illegal"  // 违法：毒品、武器、诈骗、赌博等交易信息
	CategoryPrivacy  ContentCategory = "privacy"  // 隐私：他人身份信息的违规披露
	CategoryOther    ContentCategory = "other"    // 其他：由外部审核服务返回的未归类标签
)

// Stage 标识被审核内容处于链路的哪一侧。
//
// 分开标记的理由：输入侧与输出侧的风险与误报代价并不对称，策略必须能分别配置。
// 输出违规会被落库为 agent_node.output 并被 ContextLoader 回灌进后续节点的 prompt，
// 是持久化的、会自我放大的；输入违规则在模型看到之前就被拦住，代价只是一次请求。
// 而输入侧的**误报**代价远高于输出侧 —— 用户自己的提问被拒绝，
// 直接表现为产品不可用。因此默认策略对输出更严、对输入更宽（见 NewModerationPolicy）。
type Stage string

const (
	StageInput  Stage = "input"  // 发往模型的请求（历史 + 当前 prompt）
	StageOutput Stage = "output" // 模型返回的响应
)

// ContentFinding 是一次内容违规命中。
//
// 与 guard.Threat 同构，但承载的是语义类别而非注入规则：
// 两者都只记录**位置与长度、不记录原文**。理由完全一致 ——
// 审计记录会进日志系统与可观测平台，把违规内容抄进去等于二次传播，
// 而违规内容的传播本身就是这套护栏要防的事。防护层不能成为泄露点。
type ContentFinding struct {
	Category ContentCategory
	Rule     string
	Severity Severity
	Offset   int
	Length   int
	// Score 是外部审核服务给出的置信度 [0,1]；本地规则命中时恒为 1。
	// 保留它是为了让策略能按置信度而非仅按类别分流 ——
	// 外部服务的低置信命中更适合转人工，高置信命中才适合直接拒绝。
	Score float64
	// Label 是外部服务返回的原始标签（如阿里云内容安全的 label 字段）。
	// 与 Category 分开保留：Category 是本系统的归一化分类，
	// Label 是服务方的细粒度标签，排查误判时需要后者才能定位到具体规则。
	Label string
}

func (f ContentFinding) String() string {
	return fmt.Sprintf("%s[%s] sev=%s off=%d len=%d score=%.2f", f.Rule, f.Category, f.Severity, f.Offset, f.Length, f.Score)
}

// ModerationRequest 是一次审核请求。
type ModerationRequest struct {
	// Text 为待审核内容。
	Text string
	// Stage 标识输入侧还是输出侧，外部服务可据此选用不同的检测模型。
	Stage Stage
	// Scene 是业务场景标识（如 "chat" / "tool_result"），供外部服务做场景化策略。
	Scene string
}

// ModerationVerdict 是一次审核结果。
type ModerationVerdict struct {
	Findings []ContentFinding
	// Blocked 表示外部服务已自行判定为「必须拦截」。
	//
	// 保留这个字段而不是只用 Findings 推断：外部服务通常有自己的综合判定逻辑
	// （多标签加权、账号风险等级），它给出 block 时本层应当尊重，
	// 而不是用自己的严重度映射去覆盖一个更了解内容的判定。
	Blocked bool
	// Service 是产出该结果的服务标识（如 "aliyun-green" / "local-rules"）。
	// 审计需要它：同一段内容被本地规则放过、被外部服务拦下时，
	// 只有服务标识能解释这个差异。
	Service string
}

// Moderator 是内容审核服务的 port。
//
// 与 CredentialProvider / ModelProvider 同级的稳定扩展点：把「用什么服务审核、
// 请求怎么构造、标签怎么归一化」全部关在适配器内部，
// 更换审核服务（阿里云内容安全 → 网易易盾 → 自建模型）只改 adapters 层。
//
// 契约要求：
//   - 实现必须并发安全（同一 Moderator 会被多个执行协程共享）；
//   - 必须尊重 ctx 的超时与取消 —— 审核通常是一次网络调用；
//   - 审核失败必须返回错误而非空 Verdict：本层需要区分「没违规」与「没审出来」，
//     两者的处置完全相反（前者放行，后者按 OnServiceError 策略处理）。
type Moderator interface {
	Moderate(ctx context.Context, req ModerationRequest) (ModerationVerdict, error)
}

// moderatorFunc 便于用闭包实现 Moderator（测试与轻量装配场景）。
type moderatorFunc func(ctx context.Context, req ModerationRequest) (ModerationVerdict, error)

func (f moderatorFunc) Moderate(ctx context.Context, req ModerationRequest) (ModerationVerdict, error) {
	return f(ctx, req)
}

// ModeratorFunc 把闭包适配成 Moderator，用法同 ApproverFunc。
func ModeratorFunc(f func(ctx context.Context, req ModerationRequest) (ModerationVerdict, error)) Moderator {
	return moderatorFunc(f)
}

// OnServiceError 决定外部审核服务不可用时怎么办。
//
// 这是内容护栏最容易被忽视、也最容易出事的一个配置项。
// 两种朴素做法都不对：
//   - 一律 fail-open（放行）：审核服务抖动的几分钟里，所有违规内容畅通无阻，
//     而这些内容会被落库并回灌进后续推理 —— 事故窗口短，污染却是持久的。
//   - 一律 fail-closed（拦截）：审核服务成了整条 Agent 链路的单点，
//     它一挂全站不可用，运维会为了恢复可用性而临时关掉护栏，然后忘记打开。
//
// 因此默认是第三条路：**降级到本地规则**。外部服务不可用时用内置规则兜底，
// 覆盖面窄但总比没有强，同时保持可用性。这个默认值把「安全」与「可用」的取舍
// 从二选一变成了「降级但不裸奔」。
type OnServiceError int

const (
	// ServiceErrorFallbackLocal 降级到本地规则（默认）。
	ServiceErrorFallbackLocal OnServiceError = iota
	// ServiceErrorFailClosed 拦截本次调用，返回 ErrBlocked。
	ServiceErrorFailClosed
	// ServiceErrorFailOpen 放行并记审计。**仅建议在审核服务极不稳定、
	// 且业务内容风险很低（如纯内部工具）时使用**，因为它会在故障窗口内完全裸奔。
	ServiceErrorFailOpen
)

func (o OnServiceError) String() string {
	switch o {
	case ServiceErrorFallbackLocal:
		return "fallback_local"
	case ServiceErrorFailClosed:
		return "fail_closed"
	case ServiceErrorFailOpen:
		return "fail_open"
	}
	return fmt.Sprintf("on_service_error-%d", int(o))
}

// ModerationPolicy 决定「什么类别、什么严重度、在哪一侧 → 采取什么动作」。
//
// 结构与 GuardPolicy 同构，但按 Stage 分成两套映射，理由见 Stage 的注释。
type ModerationPolicy struct {
	// ActionBySeverity 是输出侧的严重度→动作映射；未列出的按 ActionAllow 处理。
	// 为 nil 时用 defaultOutputActionBySeverity。
	ActionBySeverity map[Severity]Action
	// ActionBySeverityInput 是输入侧的映射；为 nil 时用 defaultInputActionBySeverity。
	ActionBySeverityInput map[Severity]Action
	// ActionByCategory 是**按类别**的动作覆盖，优先级高于严重度映射。
	//
	// 存在理由：某些类别无论严重度多低都必须直接拒绝（如未成年相关的色情内容），
	// 而另一些类别即使严重度高也只适合转人工（如涉政表述，机器判定的误报率天然偏高）。
	// 只用严重度一个维度表达不了这两类需求。
	ActionByCategory map[ContentCategory]Action
	// SkipCategories 列出的类别完全不处置（仍会审核并记审计）。
	// 用于「业务上明确允许」的内容，例如医疗场景下的疾病描述会命中暴力/血腥规则，
	// 但那是正常业务内容。跳过必须显式配置，默认不跳过任何类别。
	SkipCategories map[ContentCategory]bool
	// MinSeverityForAudit 是进入审计回调的最低严重度，<=0 时取 SeverityLow（全审计）。
	MinSeverityForAudit Severity
	// OnServiceError 决定外部服务失败时的处置，零值为 ServiceErrorFallbackLocal。
	OnServiceError OnServiceError
}

// 输出侧默认映射：低危告警放行、中危转人工、高危拒绝。
// 与 guard.go 的 defaultActionBySeverity 完全一致 —— 同一套系统里
// 两个护栏用不同的默认分流，会让运维无法形成稳定预期。
var defaultOutputActionBySeverity = map[Severity]Action{
	SeverityLow:    ActionAllow,
	SeverityMedium: ActionRequireApproval,
	SeverityHigh:   ActionBlock,
}

// 输入侧默认映射：低危与中危都只告警放行，只有高危才拒绝。
//
// 比输出侧宽松，是刻意的。输入侧拦截的是**用户自己写的内容**，
// 误报直接表现为「用户说了句话就被拒绝」，是产品可用性事故；
// 而输出侧拦截的是模型生成的内容，误报只是少了一次输出，用户可以重试。
// 两者的误报代价差一个量级，策略就不该相同。
//
// 中危在输入侧仍会记审计（MinSeverityForAudit 默认 SeverityLow），
// 因此「放行了什么」是可追溯的，不是静默通过。
var defaultInputActionBySeverity = map[Severity]Action{
	SeverityLow:    ActionAllow,
	SeverityMedium: ActionAllow,
	SeverityHigh:   ActionBlock,
}

// NewModerationPolicy 用默认分流构造策略。
func NewModerationPolicy() *ModerationPolicy {
	return &ModerationPolicy{
		ActionBySeverity:      defaultOutputActionBySeverity,
		ActionBySeverityInput: defaultInputActionBySeverity,
	}
}

// actionFor 计算某次命中在给定阶段的最终动作。
func (p *ModerationPolicy) actionFor(stage Stage, cat ContentCategory, sev Severity) Action {
	if p == nil {
		p = NewModerationPolicy()
	}
	if p.SkipCategories[cat] {
		return ActionAllow
	}
	// 类别覆盖优先：它表达的是「这个类别的业务定性」，比严重度更强。
	if a, ok := p.ActionByCategory[cat]; ok {
		return a
	}
	mapping := p.ActionBySeverity
	if stage == StageInput {
		if p.ActionBySeverityInput != nil {
			mapping = p.ActionBySeverityInput
		} else {
			mapping = defaultInputActionBySeverity
		}
	} else if mapping == nil {
		mapping = defaultOutputActionBySeverity
	}
	if a, ok := mapping[sev]; ok {
		return a
	}
	return ActionAllow
}

// maxAction 返回一组动作中最严格的那个，语义同 guard.go 的 decide。
// Action 的枚举顺序是 allow < require_approval < block，因此可直接比大小。
func maxAction(as []Action) Action {
	top := ActionAllow
	for _, a := range as {
		if a > top {
			top = a
		}
	}
	return top
}

// ContentGuard 是把内容审核接到 Model.Before / Model.After 的中间件。
//
// 挂载点由 executor.Dispatcher.ModelChain 提供，两侧语义不同：
//   - Before（输入侧）：只审核**最后一条 user 消息**。
//   - After（输出侧）：审核完整的 resp.Message.Content。
//
// 输入侧为什么只审最后一条，而不是整个 Messages：
// 历史消息里的工具结果可能引用了网页正文（新闻报道里的暴力事件、
// 医疗文档里的疾病描述），这些是**合法的数据引用**，不是模型在生成违规内容。
// 对全量历史做审核会让这类正常业务大面积误伤，而它们早在产生时
// 就已经由各自的挂载点处理过（模型输出经 After、工具结果经 Redactor.After）。
// 真正未经审核就进入 prompt 的，只有用户当前这一次输入。
//
// 需要审核全量历史的部署可以把 ModerateAllMessages 置 true，
// 但应当同时把输入侧策略调宽松，否则误报会淹没真实告警。
type ContentGuard struct {
	// Moderator 是外部审核服务；为 nil 时只用 Local 规则。
	// 两者都为空时护栏不做任何检测（等价于未装配），但不会报错 ——
	// 与 Guard 对 Approver 为 nil 的处理一致：能力缺失应当降级，不该让链路崩掉。
	Moderator Moderator
	// Local 是本地规则审核器，为 nil 时用 NewLocalModerator() 的内置规则。
	// 它既是「未配置外部服务时的全部能力」，也是外部服务失败时的降级兜底。
	Local *LocalModerator
	// Policy 为处置策略；为 nil 时用 NewModerationPolicy()。
	Policy *ModerationPolicy
	// Approver 为人工闸门实现；为 nil 时中危降级为拒绝（fail-closed），
	// 与 Guard 的取舍一致：无法转人工就放行等于防护形同虚设。
	Approver Approver
	// Bypass 是**输入侧**的动态放行判定（可 nil），语义与 Guard.Bypass 相同：
	// 解决「转人工 → 放行 → 重新调度 → 再次命中」的死循环。
	// 输入内容在重跑时是确定的，因此「该节点已获放行」足以代表「这份内容已获放行」。
	//
	// **仅作用于 Before**。输出侧请用 OutputBypass —— 那里内容非确定，
	// 无条件放行会让一次豁免变成永久通道，详见 After 的注释。
	Bypass func(ctx context.Context, ec contracts.ExecutionContext, stage Stage) bool
	// OutputBypass 是**输出侧**的放行判定（可 nil），必须绑定内容指纹。
	//
	// 回调收到的是 ContentFingerprint(content) 而非内容本身：
	// 装配层通常实现为「查该节点是否存在针对此指纹的 RESOLVED 审批记录」，
	// 于是同一份输出不会被重复转人工，而新的输出仍会照常审核。
	// 不给指纹而给内容，会让违规原文流入装配层与其日志。
	OutputBypass func(ctx context.Context, ec contracts.ExecutionContext, fingerprint string) bool
	// OnFinding 为审计回调（可 nil）。回调不含违规原文，理由见 ContentFinding。
	OnFinding func(ctx context.Context, ec AuditContext, stage Stage, action Action, findings []ContentFinding)
	// OnServiceFailure 为审核服务故障回调（可 nil）。
	//
	// 与 OnFinding 分开，因为两者要回答的问题不同：OnFinding 回答「拦下了什么」，
	// 本回调回答「审核能力本身是否正常」。后者是可用性告警，
	// 缺失它会让「外部审核服务已经挂了几天、期间一直在用本地弱规则兜底」
	// 这件事完全不可见 —— 而降级与正常工作的审计输出看起来是一样的。
	//
	// 回调只拿到错误本身，不含待审内容：错误会进日志与告警通道，
	// 把内容抄进去等于让故障告警成为新的泄露点。
	OnServiceFailure func(ctx context.Context, ec AuditContext, stage Stage, err error)
	// ModerateInput / ModerateOutput 分别控制两侧是否生效，默认都开启。
	// 拆开是为了能只审核输出（输入误报代价高的业务）或只审核输入。
	ModerateInput  bool
	ModerateOutput bool
	// ModerateAllMessages 为 true 时输入侧审核全部消息而非仅最后一条 user 消息。
	// 默认 false，理由见 ContentGuard 的类型注释。
	ModerateAllMessages bool
	// MaxScanBytes 限制单次审核的字节数，<=0 时取 DefaultMaxScanBytes。
	// 超限内容整体视为可疑并转人工，与 Guard 的处理一致：
	// 截断审核会漏掉尾部内容，而违规内容恰恰常被放在大段正常文本之后。
	MaxScanBytes int
	// Scene 是传给外部服务的业务场景标识，空串时按 Stage 自动取值。
	Scene string
}

// NewContentGuard 构造两侧都开启的内容护栏。
func NewContentGuard(mod Moderator, approver Approver) *ContentGuard {
	return &ContentGuard{
		Moderator:      mod,
		Local:          NewLocalModerator(),
		Policy:         NewModerationPolicy(),
		Approver:       approver,
		ModerateInput:  true,
		ModerateOutput: true,
	}
}

var _ Model = (*ContentGuard)(nil)

// Before 审核发往模型的请求（输入侧）。
//
// 返回 (req, nil) 表示放行；(req, ErrAwaitingApproval) 表示已转人工；
// (req, ErrBlocked) 表示拒绝。错误原样上抛不做包装，
// 否则 worker 侧的 errors.Is 判不出来，等待人工的节点会被重试策略反复重跑。
func (g *ContentGuard) Before(ctx context.Context, ec contracts.ExecutionContext, req contracts.GenerateRequest) (contracts.GenerateRequest, error) {
	if g == nil || !g.ModerateInput || len(req.Messages) == 0 {
		return req, nil
	}
	texts := g.inputTexts(req)
	if len(texts) == 0 {
		return req, nil
	}
	// 输入内容在一次 Run 内是确定的（同样的 Messages 重跑还是同样的内容），
	// 因此 Bypass 在这里的语义是干净的：放行过的就是这一份。
	if g.bypass(ctx, ec, StageInput) {
		g.auditBypass(ctx, ec, StageInput)
		return req, nil
	}
	if err := g.moderate(ctx, ec, StageInput, texts); err != nil {
		return req, err
	}
	return req, nil
}

// After 审核模型返回的响应（输出侧）。
//
// **这里刻意不调用 Bypass**，与输入侧不同，原因是一个必须记录在案的语义差异：
//
// 输入侧内容是确定的 —— 节点重跑时 Messages 完全一样，人工放行过的就是这一份，
// 因此 Bypass 能正确终止「命中 → 转人工 → 放行 → 重跑 → 再次命中」的死循环。
//
// 输出侧内容是**非确定性**的 —— 放行了输出 A，重跑生成的是输出 B，
// 而 B 从未被任何人看过。此时若因为「该节点有一条 RESOLVED 审批记录」就放行 B，
// 那条记录就从「针对 A 的一次性豁免」变成了「该节点此后所有输出的永久豁免通道」。
// 而 HasResolvedApproval 只按 (tenant, run, node) 查询、不绑定内容，
// 装配层若照搬输入侧的 Bypass 实现，得到的正是这个结果。
//
// 因此输出侧每次都重新审核。这不会造成死循环，而是正确行为：
// 每段不同的违规输出都应当被人工看一次；模型若稳定输出违规内容，
// 人工可以持续拒绝直到合规，或直接取消 Run。
// 需要「同一份输出不重复转人工」的部署，用 OutputBypass 显式绑定内容指纹。
func (g *ContentGuard) After(ctx context.Context, ec contracts.ExecutionContext, _ contracts.GenerateRequest, resp contracts.GenerateResponse) (contracts.GenerateResponse, error) {
	if g == nil || !g.ModerateOutput || resp.Message.Content == "" {
		return resp, nil
	}
	// 只有装配层显式提供了内容绑定的放行判定，输出侧才可能被跳过。
	// 传入指纹而非内容本身，是为了让回调不必持有违规原文（它会进日志）。
	if g.OutputBypass != nil && g.OutputBypass(ctx, ec, ContentFingerprint(resp.Message.Content)) {
		g.auditBypass(ctx, ec, StageOutput)
		return resp, nil
	}
	if err := g.moderate(ctx, ec, StageOutput, []string{resp.Message.Content}); err != nil {
		// resp 原样返回：拦截时内容由 worker 侧按哨兵错误处理
		// （ErrBlocked 走失败终态、ErrAwaitingApproval 走挂起），
		// 此处改写内容没有意义 —— 它不会被落库，也不会进事件流。
		return resp, err
	}
	return resp, nil
}

// ContentFingerprint 计算内容的稳定指纹，用于把审批记录与**具体那一份内容**绑定。
//
// 为什么需要它：见 After 的注释。输出侧的放行必须绑定内容，
// 否则一次豁免会退化成该节点的永久豁免通道。指纹让「人工看过的是哪一段」
// 变成一个可持久化、可比对的短字符串，而不必把违规原文存进审批表
// （那等于在数据库里长期保存一份违规内容，比存在日志里更难清理）。
//
// 用 SHA-256 而不是短哈希：审批记录会长期留存，
// 短哈希的碰撞会让两段不同内容共享同一条放行记录。
func ContentFingerprint(content string) string {
	sum := sha256.Sum256([]byte(content))
	return hex.EncodeToString(sum[:])
}

// inputTexts 取出需要审核的输入文本。
//
// 默认只取最后一条 user 消息；ModerateAllMessages 为 true 时取全部。
// system 消息在任何模式下都不审核：它是部署方自己写的提示词，
// 属于可信输入，审核它只会因为提示词里写了「不要输出暴力内容」而自我误伤。
func (g *ContentGuard) inputTexts(req contracts.GenerateRequest) []string {
	if g.ModerateAllMessages {
		var out []string
		for _, m := range req.Messages {
			if m.Role == contracts.RoleSystem || m.Content == "" {
				continue
			}
			out = append(out, m.Content)
		}
		return out
	}
	for i := len(req.Messages) - 1; i >= 0; i-- {
		m := req.Messages[i]
		if m.Role == contracts.RoleUser && m.Content != "" {
			return []string{m.Content}
		}
	}
	return nil
}

// moderate 对一组文本执行审核并按策略处置。
//
// 多个文本的结果**合并**后统一决策，而不是逐条决策：
// 逐条决策会让「一条低危 + 一条中危」被当成两次独立放行，
// 而合并后最高严重度生效 —— 违规内容的严重度不该因为被拆成几段就降低。
func (g *ContentGuard) moderate(ctx context.Context, ec contracts.ExecutionContext, stage Stage, texts []string) error {
	limit := g.MaxScanBytes
	if limit <= 0 {
		limit = DefaultMaxScanBytes
	}
	var all []ContentFinding
	oversize := false
	for _, t := range texts {
		if t == "" {
			continue
		}
		scan := t
		if len(t) > limit {
			scan = t[:limit]
			oversize = true
		}
		fs, blocked, err := g.moderateOne(ctx, ec, stage, scan)
		if err != nil {
			// 只有 fail-closed 策略会走到这里，错误已 wrap ErrBlocked，
			// 原样上抛以保留哨兵语义（包装成普通错误会让 worker 无法识别为不可重试）。
			return err
		}
		all = append(all, fs...)
		if blocked {
			// 外部服务已判定必须拦截，尊重它的综合判定，不再用本地严重度映射覆盖。
			return g.deny(stage, all, "moderation service blocked this content")
		}
	}
	if oversize {
		// 超限：不截断后放行（会漏掉尾部内容），按中危计入并转人工。
		all = append(all, ContentFinding{
			Rule: "oversize_content", Category: CategoryOther, Severity: SeverityMedium,
			Offset: limit, Length: 0, Score: 1,
		})
	}
	if len(all) == 0 {
		return nil
	}
	return g.decide(ctx, ec, stage, all)
}

// moderateOne 对单段文本执行审核，返回命中、服务级拦截标记与错误。
//
// 优先用外部服务，失败时按策略降级。降级发生在这一层而不是调用方，
// 是为了让「用哪套规则审」这件事只有一处决定 —— 此前把降级写在调用方的
// 错误分支里，结果那条分支直接 continue 跳过了文本，本地规则根本没跑，
// 文档承诺的「降级但不裸奔」实际退化成完全放行。这种失效在测试里看不出来
// （外部服务正常时永远走不到），只有服务真挂了才会暴露，而那时正是最需要护栏的时候。
//
// 返回的 error 只在 ServiceErrorFailClosed 时非 nil，且已 wrap ErrBlocked，
// 调用方原样上抛即可。
func (g *ContentGuard) moderateOne(ctx context.Context, ec contracts.ExecutionContext, stage Stage, text string) ([]ContentFinding, bool, error) {
	if g.Moderator == nil {
		return g.local().ModerateText(stage, text)
	}
	v, err := g.Moderator.Moderate(ctx, ModerationRequest{
		Text: text, Stage: stage, Scene: g.scene(stage),
	})
	if err == nil {
		return v.Findings, v.Blocked, nil
	}

	// 外部服务故障必须可观测：静默降级会让「外部服务已经挂了几天」这件事
	// 完全 invisible，而这段时间里审核质量已经降级到本地规则的水平。
	if g.OnServiceFailure != nil {
		g.OnServiceFailure(ctx, auditContextOf(ec), stage, err)
	}

	switch g.policy().OnServiceError {
	case ServiceErrorFailClosed:
		// fail-closed 用 ErrBlocked 而不是普通错误：普通错误会被重试策略反复重跑，
		// 而审核服务不可用是持续状态，重试只会把 DLQ 塞满同样的失败。
		//
		// 同时 wrap ErrModerationUnavailable，让调用方能区分「内容违规」与
		// 「审核能力故障」：前者重试无意义，后者服务恢复后同样的内容就能过。
		return nil, false, fmt.Errorf("%w: %w (stage=%s)", ErrBlocked, ErrModerationUnavailable, stage)
	case ServiceErrorFailOpen:
		// 放行。这是三种策略里唯一会让内容完全不受审的分支，
		// 因此必须依赖上面的 OnServiceFailure 留痕，否则故障窗口内的裸奔无人知晓。
		return nil, false, nil
	default: // ServiceErrorFallbackLocal
		// 真正的降级：改用本地规则。本地规则是纯内存正则，不会再失败，
		// 因此这条路径之后一定有明确的审核结论。
		return g.local().ModerateText(stage, text)
	}
}

// decide 按策略对合并后的命中做出处置。
func (g *ContentGuard) decide(ctx context.Context, ec contracts.ExecutionContext, stage Stage, findings []ContentFinding) error {
	p := g.policy()
	var actions []Action
	audited := make([]ContentFinding, 0, len(findings))
	minAudit := p.MinSeverityForAudit
	if minAudit <= 0 {
		minAudit = SeverityLow
	}
	for _, f := range findings {
		actions = append(actions, p.actionFor(stage, f.Category, f.Severity))
		if f.Severity >= minAudit {
			audited = append(audited, f)
		}
	}
	action := maxAction(actions)
	if g.OnFinding != nil && len(audited) > 0 {
		g.OnFinding(ctx, auditContextOf(ec), stage, action, audited)
	}

	switch action {
	case ActionBlock:
		return g.deny(stage, findings, "")
	case ActionRequireApproval:
		if g.Approver == nil {
			// fail-closed：没有人工闸门可转时不放行，与 Guard 的取舍一致。
			return fmt.Errorf("%w: content requires approval but no approver configured (stage=%s)",
				ErrBlocked, stage)
		}
		if err := g.Approver.RequestApproval(ctx, ApprovalRequest{
			TenantID: ec.TenantID, UserID: ec.UserID, RunID: ec.RunID, NodeID: ec.NodeID,
			ToolName: string(stage), // 复用 ToolName 字段承载 stage：审批记录需要知道是哪一侧被拦
			Reason:   fmt.Sprintf("content moderation (%s): %d finding(s), highest=%s, categories=%s, rules=%s", stage, len(findings), maxFindingSeverity(findings), categoryNames(findings), findingRuleNames(findings)),
		}); err != nil {
			return fmt.Errorf("middleware: request content approval for stage %s: %w", stage, err)
		}
		return fmt.Errorf("%w: content at stage %s", ErrAwaitingApproval, stage)
	case ActionAllow:
		return nil
	}
	return nil
}

// deny 构造拒绝错误。
//
// reason 为空时从命中里生成，含严重度、类别与规则名，但**不含原文**，
// 理由同 ContentFinding：错误信息会进日志、进事件的 error 字段、进 DLQ。
//
// 规则名必须带上：类别只有六个，其中 other 是个筐 ——
// 超限内容、未归类的外部标签都落在里面。只写 categories=other，
// 审批人和排障的人都无法判断到底命中了什么。
func (g *ContentGuard) deny(stage Stage, findings []ContentFinding, reason string) error {
	if reason == "" {
		reason = fmt.Sprintf("highest=%s, categories=%s, rules=%s",
			maxFindingSeverity(findings), categoryNames(findings), findingRuleNames(findings))
	}
	return fmt.Errorf("%w: content blocked at stage %s (%s)", ErrBlocked, stage, reason)
}

// findingRuleNames 汇总去重后的规则名，语义同 guard.go 的 ruleNames。
//
// 刻意输出规则名而不是命中的文本片段：规则名是本系统自己定义的稳定标识，
// 可以安全地进日志、进审批界面、进告警规则；
// 文本片段则是违规内容本身，抄进去等于二次传播。
func findingRuleNames(fs []ContentFinding) string {
	seen := make(map[string]bool, len(fs))
	var parts []string
	for _, f := range fs {
		if f.Rule != "" && !seen[f.Rule] {
			seen[f.Rule] = true
			parts = append(parts, f.Rule)
		}
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, ",")
}

func (g *ContentGuard) bypass(ctx context.Context, ec contracts.ExecutionContext, stage Stage) bool {
	return g.Bypass != nil && g.Bypass(ctx, ec, stage)
}

// auditBypass 记录一次「人工已放行」的审计。
//
// 必须记录：Bypass 是一次性豁免，若不留下痕迹，
// 事后无法解释「这段违规内容为什么过去了」。
func (g *ContentGuard) auditBypass(ctx context.Context, ec contracts.ExecutionContext, stage Stage) {
	if g.OnFinding == nil {
		return
	}
	g.OnFinding(ctx, auditContextOf(ec), stage, ActionAllow, []ContentFinding{{
		Rule: "human_bypass", Category: CategoryOther, Severity: SeverityLow, Score: 1,
	}})
}

func (g *ContentGuard) policy() *ModerationPolicy {
	if g.Policy != nil {
		return g.Policy
	}
	return NewModerationPolicy()
}

func (g *ContentGuard) local() *LocalModerator {
	if g.Local != nil {
		return g.Local
	}
	return NewLocalModerator()
}

func (g *ContentGuard) scene(stage Stage) string {
	if g.Scene != "" {
		return g.Scene
	}
	return string(stage)
}

// maxFindingSeverity 返回命中集中的最高严重度，语义同 guard.go 的 maxSeverity。
func maxFindingSeverity(fs []ContentFinding) Severity {
	var top Severity
	for _, f := range fs {
		if f.Severity > top {
			top = f.Severity
		}
	}
	return top
}

// categoryNames 汇总去重后的类别名，用于错误信息与审计原因。
func categoryNames(fs []ContentFinding) string {
	seen := make(map[ContentCategory]bool, len(fs))
	var parts []string
	for _, f := range fs {
		if !seen[f.Category] {
			seen[f.Category] = true
			parts = append(parts, string(f.Category))
		}
	}
	return strings.Join(parts, ",")
}

// ModerationRule 是一条本地内容规则。
//
// 与 InjectionRule 同构，但按 ContentCategory 而非注入类别归类。
type ModerationRule struct {
	Name     string
	Category ContentCategory
	// Pattern 命中即视为存在该类违规。RE2 语法，不支持环视。
	Pattern  *regexp.Regexp
	Severity Severity
	// Stages 限定该规则适用的阶段；为空表示两侧都适用。
	// 限定的意义在于压误报：某些表述在用户输入里是正常提问
	// （"怎么举报暴力内容"），在模型输出里才是违规。
	Stages []Stage
}

// appliesTo 报告规则是否适用于该阶段。
func (r *ModerationRule) appliesTo(stage Stage) bool {
	if len(r.Stages) == 0 {
		return true
	}
	for _, s := range r.Stages {
		if s == stage {
			return true
		}
	}
	return false
}

// LocalModerator 是基于正则规则的本地审核器，实现 Moderator 接口。
//
// **能力边界必须说清，这是本类型最重要的一段注释**：
// 正则规则做内容安全，召回率与准确率都远不及专用模型，
// 它挡得住的是「直白的违规表述」，挡不住改写、谐音、多语言混排、图片与音频。
// 它的定位是**兜底与降级**，不是生产环境的完整方案：
//   - 未接外部审核服务时，它是唯一能力，聊胜于无；
//   - 接了外部服务时，它是服务故障期间的降级路径（见 OnServiceError）。
//
// 因此内置规则刻意保持**极小且高置信**：只收那些几乎不可能误报的表述。
// 宁可漏报交给外部服务，也不要用一堆模糊规则制造误报 ——
// 误报会让部署方关掉整个护栏，那比漏报的后果严重得多。
//
// 生产部署应当实现 Moderator 接一个真实的内容安全服务，
// 而不是往 DefaultModerationRules 里不断堆关键词。
type LocalModerator struct {
	Rules []ModerationRule
}

// NewLocalModerator 构造使用内置规则的本地审核器。
func NewLocalModerator() *LocalModerator {
	return &LocalModerator{Rules: DefaultModerationRules}
}

var _ Moderator = (*LocalModerator)(nil)

// DefaultModerationRules 是内置的高置信规则集。
//
// 规模刻意很小（见 LocalModerator 的能力边界说明）。
// 每条都要求「明确的行为意图或明确的违规主体」，
// 而不是单个敏感词 —— 单词命中的误报率高到无法接受
// （"杀死进程"、"毒品题材的电影推荐"都会中招）。
var DefaultModerationRules = []ModerationRule{
	// —— 暴恐：要求出现明确的施暴意图，而不是仅提及暴力词汇 ——
	{
		Name: "violence_threat", Category: CategoryViolence, Severity: SeverityHigh,
		// "我要杀了你" / "I will kill you" 一类的第一人称施暴威胁。
		// 限定人称与将来时，避免命中叙述性内容（"小说里主角杀了反派"）。
		Pattern: regexp.MustCompile(`(?i)\b(i\s+will|i'?m\s+going\s+to|i\s+want\s+to)\s+(kill|murder|attack|bomb|hurt)\s+(you|him|her|them|everyone)\b`),
	},
	{
		Name: "violence_threat_zh", Category: CategoryViolence, Severity: SeverityHigh,
		Pattern: regexp.MustCompile(`(我要|我会|我这就)(杀|砍|炸|弄死|打死)(了)?(你|他|她|你们|所有人)`),
	},
	{
		Name: "terror_instruction", Category: CategoryViolence, Severity: SeverityHigh,
		// 索取制造爆炸物/武器的具体方法。这是明确的高危请求，几乎不存在合法语境。
		Pattern: regexp.MustCompile(`(?i)\bhow\s+to\s+(make|build|assemble|synthesize)\s+(a\s+)?(bomb|explosive|ied|pipe\s+bomb|nerve\s+agent)\b`),
	},
	{
		Name: "terror_instruction_zh", Category: CategoryViolence, Severity: SeverityHigh,
		Pattern: regexp.MustCompile(`(怎么|如何|怎样)(制作|制造|自制|合成)(炸弹|爆炸物|燃烧瓶|神经毒剂)`),
	},

	// —— 违法交易：要求出现交易意图，而不是仅提及违禁品名称 ——
	{
		Name: "illegal_trade", Category: CategoryIllegal, Severity: SeverityHigh,
		Pattern: regexp.MustCompile(`(?i)\b(buy|purchase|order|sell|ship)\s+(me\s+)?(illegally|on\s+the\s+dark\s+web|without\s+prescription)\b`),
	},
	{
		Name: "illegal_trade_zh", Category: CategoryIllegal, Severity: SeverityHigh,
		Pattern: regexp.MustCompile(`(出售|购买|求购|代购|批发)(冰毒|海洛因|枪支|弹药|军火|假币|公民个人信息)`),
	},
	{
		Name: "fraud_request", Category: CategoryIllegal, Severity: SeverityMedium,
		// 索取诈骗话术。定为中危而非高危：也存在反诈教育这类合法语境，
		// 因此更适合转人工判断而不是直接拒绝。
		Pattern: regexp.MustCompile(`(?i)\b(write|give|create)\s+(me\s+)?(a\s+)?(phishing|scam|fraud)\s+(email|message|script|template)\b`),
	},

	// —— 色情：只收明确的露骨请求，避免命中性教育、医学咨询等正常语境 ——
	{
		Name: "explicit_request", Category: CategoryPorn, Severity: SeverityHigh,
		Pattern: regexp.MustCompile(`(?i)\b(write|generate|create|describe)\s+(me\s+)?(explicit|graphic)\s+(sexual|porn|erotica)\s*(content|story|scene|material)?\b`),
	},
	{
		Name: "csae_request", Category: CategoryPorn, Severity: SeverityHigh,
		// 未成年相关内容。无论严重度映射如何配置都应当直接拒绝，
		// 因此装配层可通过 ActionByCategory 把它钉死为 ActionBlock。
		Pattern: regexp.MustCompile(`(?i)\b(child|kid|minor|underage|teen)\s+(porn|pornography|sex|nude|nudes)\b`),
	},

	// —— 辱骂：要求出现明确的攻击对象与侮辱性表述 ——
	{
		Name: "hate_speech", Category: CategoryAbuse, Severity: SeverityMedium,
		Pattern: regexp.MustCompile(`(?i)\b(all|every)\s+(member\s+of\s+)?(muslims?|jews?|blacks?|whites?|asians?|immigrants?|women|men)\s+(are|should\s+be)\s+(inferior|subhuman|vermin|evil|rapists?|criminals?)\b`),
	},
	{
		Name: "hate_speech_zh", Category: CategoryAbuse, Severity: SeverityMedium,
		Pattern: regexp.MustCompile(`(所有|全部)(某?族人|女人|男人|外地人|同性恋)(都|全是)(该死|下贱|低等|垃圾|去死)`),
	},
}

// Moderate 实现 Moderator 接口，使 LocalModerator 可以直接作为外部服务注入。
func (m *LocalModerator) Moderate(_ context.Context, req ModerationRequest) (ModerationVerdict, error) {
	findings, _, err := m.ModerateText(req.Stage, req.Text)
	if err != nil {
		return ModerationVerdict{}, err
	}
	return ModerationVerdict{Findings: findings, Service: "local-rules"}, nil
}

// ModerateText 按阶段扫描文本，返回命中、服务级拦截标记与错误。
//
// 与 Detector.Scan 一样返回**全部**命中而非首个：处置决策需要看最高严重度，
// 审计需要完整事实。只报第一个会让「一个中危掩盖后面的高危」这种组合漏网。
//
// 本地规则不产出 Blocked=true：服务级拦截是外部服务的综合判定能力，
// 本地规则的处置完全由 ModerationPolicy 的严重度映射决定。
func (m *LocalModerator) ModerateText(stage Stage, text string) ([]ContentFinding, bool, error) {
	if text == "" || m == nil {
		return nil, false, nil
	}
	rules := m.Rules
	if len(rules) == 0 {
		rules = DefaultModerationRules
	}
	var out []ContentFinding
	for i := range rules {
		r := &rules[i]
		if r.Pattern == nil || !r.appliesTo(stage) {
			continue
		}
		for _, loc := range r.Pattern.FindAllStringIndex(text, -1) {
			out = append(out, ContentFinding{
				Category: r.Category, Rule: r.Name, Severity: r.Severity,
				Offset: loc[0], Length: loc[1] - loc[0], Score: 1,
			})
		}
	}
	// 按位置排序，使审计记录稳定可复现（同 Detector.Scan 的理由）。
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && (out[j].Offset < out[j-1].Offset ||
			(out[j].Offset == out[j-1].Offset && out[j].Rule < out[j-1].Rule)); j-- {
			out[j], out[j-1] = out[j-1], out[j]
		}
	}
	return out, false, nil
}

// ErrModerationUnavailable 表示审核服务不可用且策略要求 fail-closed。
//
// 单独定义是为了让调用方能把「审核服务挂了」与「内容违规」区分开：
// 前者是可重试的基础设施故障（服务恢复后同样的内容就能过），
// 后者是不可重试的终态（同样的内容重试还是违规）。
// 目前 decide 路径用 ErrBlocked 表达拦截，本错误保留给需要区分两者的部署。
var ErrModerationUnavailable = errors.New("middleware: content moderation service unavailable")
