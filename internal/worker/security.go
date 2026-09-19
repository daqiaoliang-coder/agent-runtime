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
	EnvGuardEnabled  = "SECURITY_GUARD_ENABLED"   // 不可信输入防护开关，默认 true
	EnvRedactEnabled = "SECURITY_REDACT_ENABLED"  // 数据脱敏开关，默认 true
	EnvRedactLevel   = "SECURITY_REDACT_LEVEL"    // 脱敏生效门槛，默认 internal
	EnvHighRiskTools = "SECURITY_HIGH_RISK_TOOLS" // 高危工具名单（逗号分隔），从严处置
	EnvAllowedTools  = "SECURITY_ALLOWED_TOOLS"   // 豁免工具名单（逗号分隔），完全不扫描
)

// SecurityBundle 是装配好的一组安全中间件，供 Worker 与 Dispatcher 分别取用。
//
// Redactor 同时实现 Tool 与 Event 两个接口，因此工具链与事件链**共享同一个实例**：
// 两处必须用同一套规则与门槛，否则会出现"工具结果没脱敏但事件脱敏了"这种
// 只在特定路径上泄露的不一致，排查成本极高。共享实例从结构上排除了这种偏差。
type SecurityBundle struct {
	ToolChain  *middleware.ToolChain
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
	if !envBool(EnvGuardEnabled, true) && !envBool(EnvRedactEnabled, true) {
		log.Println("worker: security middleware disabled by configuration")
		return b
	}

	var tools []middleware.Tool
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
	}

	if len(tools) > 0 {
		b.ToolChain = middleware.NewToolChain(tools...)
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
		if ec.NodeID == "" {
			return false
		}
		ok, err := s.HasResolvedApproval(ctx, ec.TenantID, ec.RunID, ec.NodeID)
		if err != nil {
			// 查询失败按"未放行"处理：放行判定宁可误拦不可误放，
			// 误拦的代价是人工再看一次，误放的代价是攻击载荷直接执行。
			log.Printf("warn: approval lookup failed tenant=%s run=%s node=%s: %v (treating as not approved)",
				ec.TenantID, ec.RunID, ec.NodeID, err)
			return false
		}
		return ok
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
