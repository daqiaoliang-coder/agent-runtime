package worker

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/middleware"
	"agent-runtime/internal/runtime"
	"agent-runtime/internal/store"
	"context"
	"errors"
	"log"
	"strings"
)

// 本文件负责把安全中间件**装配进生产链路**。
//
// 装配之所以单独成文件，是因为中间件本身（middleware 包）只提供能力与挂载点，
// 而"在哪个进程、用哪套策略、人工闸门接到哪里"是部署决策 —— 混在 NewFromEnv 里
// 会让一个几百行的装配函数继续膨胀，也让安全策略难以单独审阅和调整。

// 安全链的开关与策略环境变量。
//
// 默认**开启**：安全能力默认关闭等于没有。若默认关闭，部署方多半永远不会打开，
// 那这套防护就只剩测试里的绿色勾。需要关闭时用显式的环境变量，留下可审计的决定。
const (
	EnvGuardEnabled      = "SECURITY_GUARD_ENABLED"      // 不可信输入防护开关，默认 true
	EnvRedactEnabled     = "SECURITY_REDACT_ENABLED"     // 数据脱敏开关，默认 true
	EnvRedactLevel       = "SECURITY_REDACT_LEVEL"       // 脱敏生效门槛，默认 internal
	EnvHighRiskTools     = "SECURITY_HIGH_RISK_TOOLS"    // 高危工具名单（逗号分隔），从严处置
	EnvAllowedTools      = "SECURITY_ALLOWED_TOOLS"      // 豁免工具名单（逗号分隔），完全不扫描
	EnvModerationEnabled = "SECURITY_MODERATION_ENABLED" // 内容安全护栏开关，默认 true
	// EnvModerationOnError 决定外部审核服务不可用时的处置：
	// fallback_local（默认）/ fail_closed / fail_open。
	// 默认降级到本地规则而非二选一，理由见 middleware.OnServiceError 的注释。
	EnvModerationOnError = "SECURITY_MODERATION_ON_ERROR"
	// EnvModerationInput / EnvModerationOutput 分别控制输入侧与输出侧审核，默认都开。
	// 拆开的理由：输入侧拦的是用户自己写的内容，误报直接表现为产品不可用，
	// 有些业务会想只审输出。见 middleware.Stage 的注释。
	EnvModerationInput  = "SECURITY_MODERATION_INPUT"
	EnvModerationOutput = "SECURITY_MODERATION_OUTPUT"
	// EnvModerationStrictInput 为 true 时，输入侧改用与输出侧相同的严格分流
	// （中危转人工而非放行）。默认 false，即输入侧更宽松。
	EnvModerationStrictInput = "SECURITY_MODERATION_STRICT_INPUT"
)

// SecurityBundle 是装配好的一组安全中间件，供 Worker 与 Dispatcher 分别取用。
//
// Redactor 服务于 Tool、Model、Event 三个挂载点（模型挂载点经 ModelRedactor 视图，
// 底层是同一个 Redactor 实例），因此三条链**共享同一套规则与门槛**：
// 若各挂各的实例，会出现"工具结果没脱敏但模型输出脱敏了"这种只在特定路径上
// 泄露的不一致，排查成本极高。共享实例从结构上排除了这种偏差。
type SecurityBundle struct {
	ToolChain  *middleware.ToolChain
	ModelChain *middleware.ModelChain
	EventChain *middleware.EventChain
}

// newSecurityBundle 按环境变量装配安全中间件。
//
// s 与 q 用于构造人工闸门：护栏检测到中危时需要把 Run 与节点一并挂起，
// 复用 runtime.Runtime.Interrupt（已实现且有测试覆盖的 CAS + 事务逻辑），
// 而不是在这里重写一遍状态机 —— 重写意味着两处 CAS 语义要保持同步，迟早会漂移。
// q 用 runtime.Queue 这个窄接口（只要求 Enqueue），*queue.RedisQueue 天然满足；
// 传 nil 也可以：此时只是 Resume 后不主动投递，由 recovery 扫描兜底。
func newSecurityBundle(s *store.MySQL, q runtime.Queue, creds contracts.CredentialProvider) *SecurityBundle {
	b := &SecurityBundle{}
	if !envBool(EnvGuardEnabled, true) && !envBool(EnvRedactEnabled, true) && !envBool(EnvModerationEnabled, true) {
		log.Println("worker: security middleware disabled by configuration")
		return b
	}

	var tools []middleware.Tool
	var models []middleware.Model
	var events []middleware.Event

	if envBool(EnvGuardEnabled, true) {
		guard := newGuard(s, q)
		tools = append(tools, guard)
		// 打印配置原文（工具名单不是敏感信息），便于运维核对策略是否符合预期。
		// 此前误把 csvNames 的返回值（map）传给 %s，vet 抓出：
		// 日志里的类型错误不会导致运行失败，但会打出 %!s(map...) 这类噪音，
		// 在排查安全策略时恰好掩盖真正需要的信息。
		log.Printf("worker: input guard enabled high_risk_tools=%q allowed_tools=%q",
			envString(EnvHighRiskTools, ""), envString(EnvAllowedTools, ""))
	}

	if envBool(EnvRedactEnabled, true) {
		redactor := middleware.NewRedactor(middleware.NewPolicy(parseSensitivity(envString(EnvRedactLevel, "internal")), redactionSaltFrom(creds)))
		// 审计回调只输出身份与命中规则名，**绝不输出原文**：
		// 审计日志本身会进日志系统与可观测平台，把敏感原文抄进去等于二次扩散，
		// 那正是脱敏要防的事 —— 防护层自己成为泄露点是不可接受的。
		redactor.OnRedact = func(_ context.Context, a middleware.AuditContext, st middleware.Stats) {
			log.Printf("redaction applied tenant=%s run=%s rules=%s hits=%d truncated=%v in=%dB out=%dB",
				a.TenantID, a.RunID, strings.Join(st.RuleNames(), ","), st.Total, st.Truncated, st.InputBytes, st.OutputBytes)
		}
		redactor.OnUnsupported = func(typeName string) {
			// fail-open 必须可观测：否则"有负载没被脱敏"会静默长期存在，
			// 没人知道事件流里哪一类负载一直是裸的。
			log.Printf("warn: redactor skipped unsupported event payload type=%s", typeName)
		}
		tools = append(tools, redactor)
		events = append(events, redactor)
		// 模型侧用同一实例的视图：Go 不允许 Redactor 同时具备两套同名 Before/After，
		// 视图是绕开方法名冲突的手段，共享的仍是同一个 Policy 与审计回调。
		models = append(models, &middleware.ModelRedactor{Redactor: redactor})
	}

	if envBool(EnvModerationEnabled, true) {
		guard := newContentGuard(s, q)
		// 顺序是关键：ModelChain.After 是**倒序**执行的（见 middleware.ModelChain）。
		// 护栏必须排在脱敏之后追加，才能在倒序中**先**执行 ——
		// 于是护栏审的是模型的原始输出，脱敏随后才改写它。
		// 顺序反过来，护栏看到的就只有被打码的文本，审核质量被静默降低，
		// 而且这种退化不会报错，只会表现为"护栏几乎从不命中"。
		models = append(models, guard)
		log.Printf("worker: content moderation enabled input=%v output=%v on_error=%s",
			guard.ModerateInput, guard.ModerateOutput, guard.Policy.OnServiceError)
	}

	if len(tools) > 0 {
		b.ToolChain = middleware.NewToolChain(tools...)
	}
	if len(models) > 0 {
		b.ModelChain = middleware.NewModelChain(models...)
	}
	if len(events) > 0 {
		b.EventChain = middleware.NewEventChain(events...)
	}
	return b
}

// newGuard 构造不可信输入防护中间件，接上人工闸门与放行判定。
//
// 两个回调是护栏能真正闭环的关键，缺一个都会退化成"检测到了但没人管"：
//   - Approver：中危命中时把 Run 挂起为 WAITING_HUMAN（持久化，进程重启不丢）。
//     装配了它，护栏的三分流才完整；否则中危会 fail-closed 成拒绝，
//     正常业务（用户说"忽略上面那条建议"）会被误杀。
//   - Bypass：查询该节点是否已获人工放行。**没有它，放行后重新调度会再次命中
//     同一条规则并再次转人工，审批永远收敛不了** —— 这是接上 HITL 后才会出现的死循环。
func newGuard(s *store.MySQL, q runtime.Queue) *middleware.Guard {
	// 复用 runtime.Runtime.Interrupt：它已在同一事务内完成
	// "Run 置 WAITING_HUMAN + 写 run_interrupt + 挂起节点"，且经过测试。
	rt := &runtime.Runtime{Store: s, Queue: q}
	g := middleware.NewGuard(middleware.ApproverFunc(func(ctx context.Context, req middleware.ApprovalRequest) error {
		// NodeID 为空说明调用方没传节点上下文（Executor 未经 worker 的 ctx 注入，
		// 或节点未进入执行路径）。此时挂起会落在错误的锚点上：
		// run_interrupt.node_id 为空，人工放行后 ResumeRun 捞不到待恢复节点，
		// Run 会永远停在 WAITING_HUMAN。显式报错让护栏 fail-closed 成拒绝，
		// 比制造一个无法恢复的挂起态安全得多。
		if req.NodeID == "" {
			return errors.New("worker: cannot request approval without node context")
		}
		return rt.Interrupt(ctx, req.TenantID, req.RunID, req.NodeID, req.Reason)
	}))
	g.Bypass = func(ctx context.Context, ec contracts.ExecutionContext, _ contracts.ToolCallRequest) bool {
		// 按来源收窄到"注入护栏"的审批记录（reason 以 inputGuardReasonPrefix 开头）。
		// 用不限来源的 HasResolvedApproval 会让内容护栏的一次放行
		// 连带豁免掉注入防护 —— 运维点的是一次内容确认，得到的却是两类防护同时失效。
		return hasApproval(ctx, s, ec, inputGuardReasonPrefix)
	}
	g.OnThreat = func(_ context.Context, a middleware.AuditContext, toolName string, action middleware.Action, threats []middleware.Threat) {
		// 只记规则名与位置长度，不记载荷原文，理由同 redactor.OnRedact。
		names := make([]string, 0, len(threats))
		for _, t := range threats {
			names = append(names, t.Rule)
		}
		log.Printf("input guard tenant=%s run=%s tool=%s action=%s threats=%d rules=%s",
			a.TenantID, a.RunID, toolName, action, len(threats), strings.Join(names, ","))
	}
	if hr := csvNames(envString(EnvHighRiskTools, "")); len(hr) > 0 {
		g.Policy.HighRiskTools = hr
	}
	if al := csvNames(envString(EnvAllowedTools, "")); len(al) > 0 {
		g.Policy.AllowedTools = al
	}
	return g
}

// 审批记录的来源前缀。
//
// run_interrupt 只有一张表，而一个节点上可能同时挂着注入护栏与内容护栏两道防护。
// 放行判定必须能区分「人工批准的是哪一类告警」，否则一次确认会连带豁免另一类
// （详见 store.HasResolvedApprovalFor 的注释）。
//
// 这里定义的前缀必须与各护栏构造 ApprovalRequest.Reason 时的开头**逐字一致**：
// Guard 写 "input guard: ..."，ContentGuard 写 "content moderation (...)..."。
// 前缀失配的后果是查不到放行记录、节点被反复转人工（审批收敛不了），
// 属于安全侧的失败而非放行侧，但同样会让业务卡死，因此改动任一侧措辞时必须同步这里。
const (
	inputGuardReasonPrefix   = "input guard"
	contentGuardReasonPrefix = "content moderation"
)

// hasApproval 是两道护栏共用的放行判定，也是本文件唯一的 store 访问点。
//
// 单独抽出来而不各自内联，是为了让一个**曾经真实发生过**的崩溃只有一处可能发生：
// store 为 nil 时（单元测试用 NewFromEnv(nil,nil,nil) 装配、或装配顺序错误），
// 直接调用 s.HasResolvedApprovalFor 会解引用空指针。
// 这类崩溃的表现极具误导性 —— 进程被 SIGKILL 掉、没有 panic 栈、
// goroutine dump 里只看到 "running on other thread"，
// 排查时很容易误判成 OOM 或死循环（本次就误判过一轮）。
//
// 处置方向是 fail-closed：store 不可用时返回 false（视为"未放行"），
// 于是内容会被重新转人工而不是被静默放过。误拦的代价是人工再看一次，
// 误放的代价是违规内容直接出域 —— 两者不对等，必须偏向前者。
func hasApproval(ctx context.Context, s *store.MySQL, ec contracts.ExecutionContext, source string) bool {
	if s == nil || ec.NodeID == "" {
		return false
	}
	ok, err := s.HasResolvedApprovalFor(ctx, ec.TenantID, ec.RunID, ec.NodeID, source)
	if err != nil {
		// 查询失败同样按"未放行"处理，理由同上。
		log.Printf("warn: approval lookup failed tenant=%s run=%s node=%s source=%q: %v (treating as not approved)",
			ec.TenantID, ec.RunID, ec.NodeID, source, err)
		return false
	}
	return ok
}

// newContentGuard 构造内容安全护栏，接上人工闸门与放行判定。
//
// 与 newGuard 的结构刻意对称：两者都是"检测 + 三分流 + 人工闸门 + 放行查询"，
// 装配方式保持一致才能让运维对两道护栏形成稳定预期。
//
// 一个重要的差异：**输出侧不装配 Bypass**。
// ContentGuard.OutputBypass 要求绑定内容指纹，而 run_interrupt 没有可存指纹的列，
// 硬用节点级放行会让「一次豁免」变成「该节点此后所有输出的永久豁免通道」
// （模型输出是非确定性的，重跑生成的是另一段内容）。
// 因此这里保持 nil，让每段输出都重新审核 —— 安全默认，代价只是人工多看几次。
// 需要指纹绑定的部署应当先给 run_interrupt 加列，再装配 OutputBypass。
func newContentGuard(s *store.MySQL, q runtime.Queue) *middleware.ContentGuard {
	rt := &runtime.Runtime{Store: s, Queue: q}
	g := middleware.NewContentGuard(nil, middleware.ApproverFunc(func(ctx context.Context, req middleware.ApprovalRequest) error {
		// 与 newGuard 同理：没有节点上下文就无法把审批落到正确锚点，
		// 宁可 fail-closed 成拒绝，也不要制造一个 ResumeRun 捞不到的挂起态。
		if req.NodeID == "" {
			return errors.New("worker: cannot request content approval without node context")
		}
		return rt.Interrupt(ctx, req.TenantID, req.RunID, req.NodeID, req.Reason)
	}))

	// Moderator 传 nil：仓库暂无外部内容安全服务的适配器，
	// 此时 ContentGuard 只用内置本地规则（能力边界见 middleware.LocalModerator）。
	// 接入真实服务时在此处装配，上层策略与三分流无需改动。
	g.ModerateInput = envBool(EnvModerationInput, true)
	g.ModerateOutput = envBool(EnvModerationOutput, true)
	g.Policy.OnServiceError = parseServiceError(envString(EnvModerationOnError, ""))
	if envBool(EnvModerationStrictInput, false) {
		// 严格模式：输入侧改用与输出侧相同的分流（中危转人工而非放行）。
		// 默认宽松的理由见 middleware.defaultInputActionBySeverity 的注释。
		g.Policy.ActionBySeverityInput = map[middleware.Severity]middleware.Action{
			middleware.SeverityLow:    middleware.ActionAllow,
			middleware.SeverityMedium: middleware.ActionRequireApproval,
			middleware.SeverityHigh:   middleware.ActionBlock,
		}
	}
	// 色情类无论严重度如何都直接拒绝：内置规则里该类别只收明确的露骨请求
	// 与未成年相关内容，不存在"转人工再看一眼"的余地。
	g.Policy.ActionByCategory = map[middleware.ContentCategory]middleware.Action{
		middleware.CategoryPorn: middleware.ActionBlock,
	}

	g.Bypass = func(ctx context.Context, ec contracts.ExecutionContext, stage middleware.Stage) bool {
		// 输出侧一律不跳过：内容非确定，节点级放行会变成永久豁免通道（见函数注释）。
		if stage != middleware.StageInput {
			return false
		}
		return hasApproval(ctx, s, ec, contentGuardReasonPrefix)
	}

	g.OnFinding = func(_ context.Context, a middleware.AuditContext, stage middleware.Stage, action middleware.Action, findings []middleware.ContentFinding) {
		// 只记类别与规则名，**绝不记违规原文**：理由同 redactor.OnRedact ——
		// 审计日志会进日志系统与可观测平台，把违规内容抄进去等于二次传播，
		// 而传播违规内容正是这道护栏要防的事。
		//
		// 输出 user 而不是 node：AuditContext 只承载 TenantID/UserID/RunID 三个身份维度，
		// 节点标识不在其中。节点级定位应当从人工闸门写入的 run_interrupt 记录里查，
		// 那里有权威的 node_id；在这条日志里硬凑一个节点字段只会打出错误的值。
		cats := make([]string, 0, len(findings))
		for _, f := range findings {
			cats = append(cats, string(f.Category)+"/"+f.Rule)
		}
		log.Printf("content moderation tenant=%s run=%s user=%s stage=%s action=%s findings=%d rules=%s",
			a.TenantID, a.RunID, a.UserID, stage, action, len(findings), strings.Join(cats, ","))
	}
	g.OnServiceFailure = func(_ context.Context, a middleware.AuditContext, stage middleware.Stage, err error) {
		// 降级必须可观测：否则"外部审核服务已经挂了几天、期间一直在用本地弱规则"
		// 完全不可见，而降级后的审计输出与正常工作时看起来一模一样。
		log.Printf("warn: content moderation service unavailable tenant=%s run=%s stage=%s: %v (policy=%s)",
			a.TenantID, a.RunID, stage, err, g.Policy.OnServiceError)
	}
	return g
}

// parseServiceError 把配置字符串解析为服务故障处置策略。
// 无法识别时回退 fallback_local（降级但不裸奔）—— 配置写错时向"仍有防护"降级，
// 而不是向 fail_open 降级，否则一个拼写错误会静默关掉整道护栏。
func parseServiceError(s string) middleware.OnServiceError {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "fail_closed", "fail-closed":
		return middleware.ServiceErrorFailClosed
	case "fail_open", "fail-open":
		return middleware.ServiceErrorFailOpen
	default:
		return middleware.ServiceErrorFallbackLocal
	}
}

// parseSensitivity 把配置字符串解析为敏感度门槛。
// 无法识别时回退 internal（几乎全处理）—— 配置写错时向"更严"降级，
// 而不是向"更松"降级，否则一个拼写错误会静默关掉脱敏。
func parseSensitivity(s string) middleware.SensitivityLevel {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "public":
		return middleware.SensitivityPublic
	case "sensitive":
		return middleware.SensitivitySensitive
	case "secret":
		return middleware.SensitivitySecret
	default:
		return middleware.SensitivityInternal
	}
}

// csvNames 把逗号分隔的名单解析为集合，忽略空白项。
func csvNames(s string) map[string]bool {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make(map[string]bool, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out[p] = true
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}
