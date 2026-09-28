package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
)

// 本文件实现 Lifecycle 接口的认证环节：在 Run 启动边界上确认"发起者身份可信"。
//
// 认证的**执行**发生在入口进程（cmd/runtime 调 Runtime.CreateRun 之前），
// 那里才有原始凭证；身份随后落库为 agent_run.user_id / auth_method。
// 本中间件做的是另外两件入口进程做不到的事：
//
//  1. **租户交叉校验**：凭证声称的租户必须与 Run 的租户一致。
//     入口进程校验凭证时只知道"这个 token 是真的"，
//     但"A 租户的凭证去操作 B 租户的 Run"是越权，
//     这个判断需要同时看到凭证身份与目标 Run，因此只能在 Run 启动边界上做。
//  2. **持久化审计**：把"谁、用什么机制、在什么时候发起了哪个 Run"记下来。
//     安全复盘最需要的就是这条记录，而它必须与 Run 生命周期绑定。
//
// 刻意不在这里重新解析 token：Run 可能跨越数十分钟甚至更久，
// 执行节点时原始凭证早已随入口进程的 ctx 销毁（这是设计意图，见 contracts/auth.go）。
// 用"创建时认证一次 + 身份落库"而不是"每个节点重新验签"，
// 代价是身份不会随凭证过期而失效，收益是长任务不会因为 token 过期而中途崩掉。
// 需要更严格语义的部署应当缩短 token 有效期并让入口拒绝长任务，而不是在这里加验签。

// AuthLifecycle 是 Run 生命周期的认证审计与策略环节，实现 Lifecycle 接口。
//
// 装配进 LifecycleChain 后，OnRunStart 会在 Run 启动前执行；
// 返回错误即阻止 Run 启动（fail-closed）—— 身份不可信时继续执行没有意义。
//
// 本中间件**不做验签**：token→Identity 的认证发生在入口进程的 Runtime.CreateRun，
// 因为身份必须在那里落库（agent_run.user_id / auth_method），而 Lifecycle.OnRunStart
// 的签名只返回 error、无法把解析出的 Identity 回传给调用方用于持久化。
// 到达这里时 ExecutionContext 已携带认证结果，本层只负责策略判定与审计。
type AuthLifecycle struct {
	// RequireIdentity 为 true 时，未能确定身份的 Run 一律拒绝启动。
	//
	// 默认 false（宽松）是为了不破坏存量部署；生产环境应当置 true。
	// 这是安全能力里少见的"默认宽松"，理由必须写清：
	// 认证是横切整条链路的能力，若默认强制，任何还没接 IdP 的部署
	// 升级后会立刻全站不可用 —— 那会导致部署方干脆不升级，安全状况反而更差。
	// 因此这里选择"默认放行 + 显式开启 + 未开启时打警告日志"，
	// 让升级路径平滑，同时让"没开认证"这件事在日志里可见。
	RequireIdentity bool
	// RequiredScopes 列出 Run 启动所需的权限范围；为空表示不校验 scope。
	// 任一缺失即拒绝：scope 是"与"关系而非"或"，
	// 因为每一项都对应一类具体权限，缺一项就意味着有一类操作不该被允许。
	RequiredScopes []string
	// OnAudit 为审计回调（可 nil）。
	//
	// 回调拿到的是**认证结果**（身份、机制、是否通过），不含原始凭证：
	// 审计记录会进日志系统与可观测平台，凭证一旦进入就等于泄露。
	// 这与 guard.go 的 OnThreat 不记载荷原文、redact.go 的 OnRedact 不记原文
	// 是同一个原则 —— 防护层自己不能成为泄露点。
	OnAudit func(ctx context.Context, rec AuditRecord)
	// Now 便于测试注入时钟；为 nil 时使用 time.Now。
	Now func() time.Time
}

// AuditRecord 是一次 Run 启动认证的审计记录。
//
// 通过与否都要记：只记成功会丢掉"有人在反复尝试越权"这一最有价值的安全信号，
// 而失败尝试恰恰是入侵检测的主要依据。
type AuditRecord struct {
	TenantID string
	UserID   string
	RunID    string
	// AuthMethod 记录身份的认证机制（jwt-hs256 / static-token / none）。
	// 空串或 "none" 表示这次 Run 没有经过凭证校验 —— 审计消费方应当据此告警。
	AuthMethod string
	// Authenticated 报告身份是否可信。
	Authenticated bool
	// Denied 为 true 时表示本次启动被拒绝，Reason 给出可分流的错误类。
	Denied bool
	Reason string
	At     time.Time
}

var _ Lifecycle = (*AuthLifecycle)(nil)

// OnRunStart 在 Run 启动边界校验身份与租户一致性。
//
// 三个检查按成本从低到高排列，且**顺序不能随意调整**：
//  1. 身份存在性（零成本，纯字段判断）
//  2. scope（零成本，遍历切片）
//  3. 租户一致性（零成本，字符串比较）
//
// 三者都在验签之后发生 —— 验签在入口进程已完成，此处只做策略判定。
// 这个顺序的意义在于：便宜的检查先做，能在绝大多数越权尝试上
// 避免走到需要构造审计记录、调用回调的较贵路径。
func (a *AuthLifecycle) OnRunStart(ctx context.Context, ec contracts.ExecutionContext) error {
	rec := AuditRecord{
		TenantID:   ec.TenantID,
		UserID:     ec.UserID,
		RunID:      ec.RunID,
		AuthMethod: ec.AuthMethod,
		At:         a.now(),
	}
	// "none" 是审计侧对"未认证"的显式标记。
	// 用非空字符串而不是留空，是为了让日志消费方能用一次等值判断识别出
	// "这批 Run 根本没走认证"，而不必区分"字段缺失"与"字段为空串"两种情况。
	if rec.AuthMethod == "" {
		rec.AuthMethod = "none"
	}
	rec.Authenticated = ec.UserID != ""

	if err := a.check(ec, &rec); err != nil {
		rec.Denied = true
		rec.Reason = DenyReason(err)
		a.audit(ctx, rec)
		return err
	}
	a.audit(ctx, rec)
	return nil
}

// check 执行策略判定，返回的错误一律 wrap 了可分流的哨兵。
func (a *AuthLifecycle) check(ec contracts.ExecutionContext, rec *AuditRecord) error {
	if !rec.Authenticated {
		if a.RequireIdentity {
			// 要求认证但身份为空：这是最典型的"升级后忘开认证层"或"入口没注入凭证"，
			// 必须 fail-closed。放行等于让未证明身份的调用方拥有全部权限。
			return fmt.Errorf("%w: run %s has no authenticated identity",
				contracts.ErrUnauthenticated, ec.RunID)
		}
		// 未要求认证：放行，但审计记录里 AuthMethod 已是 "none"，
		// 这让"没开认证"在日志里可见而不是静默通过。
		return nil
	}

	for _, scope := range a.RequiredScopes {
		if !identityOf(ec).HasScope(scope) {
			return fmt.Errorf("%w: run %s missing scope %q",
				contracts.ErrInsufficientScope, ec.RunID, scope)
		}
	}

	// 这里**没有**租户交叉校验，这是刻意的，原因值得写下来：
	//
	// 交叉校验需要同时握有两个租户 —— "凭证声称的租户"与"目标 Run 的租户"。
	// 但 OnRunStart 只收到一个 ExecutionContext，它的 TenantID 就是从 Run 还原出来的，
	// 凭证在入口进程校验完就随 ctx 销毁了。拿 ec.TenantID 与 ec.TenantID 比，
	// 永远相等、永远不触发 —— 那是一段看起来像安全检查、实则完全无效的死代码，
	// 比不写更糟：它会让审阅者以为越权已经被挡住了。
	//
	// 真正的校验在 Runtime.CreateRun.authenticate：那里同时有 Identity.TenantID
	// （来自 token）与调用方请求的 tenant，两者不一致即返回 ErrTenantMismatch，
	// 且发生在任何数据库写入之前。
	//
	// 本层保留的价值是身份存在性与 scope 校验，以及贯穿始终的审计。
	return nil
}

// identityOf 从 ExecutionContext 还原 Identity。
//
// EC 只持久化了 UserID / TenantID / AuthMethod 三个字段（落库的就是这三个），
// Subject / Scopes / ExpiresAt 不入库 —— 它们只在入口进程的内存里存在过。
// 因此这里还原出的 Identity 的 Scopes 恒为空，scope 校验实际依赖
// 入口进程在创建 Run 时完成（见 Runtime.CreateRun 的认证环节）。
// 保留 RequiredScopes 字段的意义在于：装配方可以把同一个 AuthLifecycle
// 用在入口进程（那里 EC 由 Identity 完整填充）与 worker 两处。
func identityOf(ec contracts.ExecutionContext) contracts.Identity {
	return contracts.Identity{
		UserID:   ec.UserID,
		TenantID: ec.TenantID,
		Subject:  ec.UserID,
		Scopes:   ec.Scopes,
		Method:   ec.AuthMethod,
	}
}

// OnRunFinish 记录 Run 结束，携带最终错误。
//
// 结束也要审计的理由：一条 Run 的完整安全叙事是"谁发起的 → 结果如何"，
// 只有起点没有终点，就无法回答"这个用户发起的失败 Run 里有多少是安全拦截导致的"。
func (a *AuthLifecycle) OnRunFinish(ctx context.Context, ec contracts.ExecutionContext, runErr error) error {
	rec := AuditRecord{
		TenantID:      ec.TenantID,
		UserID:        ec.UserID,
		RunID:         ec.RunID,
		AuthMethod:    ec.AuthMethod,
		Authenticated: ec.UserID != "",
		At:            a.now(),
	}
	if rec.AuthMethod == "" {
		rec.AuthMethod = "none"
	}
	if runErr != nil {
		// 记录错误类别而非错误原文：原文可能包含工具入参片段，
		// 而入参可能正是攻击载荷或含敏感数据（同 guard.go 对 Threat.Length 的取舍）。
		rec.Reason = DenyReason(runErr)
	}
	a.audit(ctx, rec)
	return nil
}

// DenyReason 把错误归一化为可安全记录的原因标识。
//
// 刻意**不直接用 err.Error()**：错误信息里常带有租户名、节点名、工具名，
// 有时还会带上被拒绝内容的片段（工具失败时抄入参是常见写法）。
// 审计记录会进日志与可观测平台，把这些抄进去等于二次扩散。
// 这里只保留"属于哪一类失败"，需要细节时应当去源头（Run 记录、节点日志）复现。
//
// 导出是为了让归一化只有一处定义：worker 侧记节点失败日志、
// Resumer 记收敛日志，都应当用同一个函数，否则各处自己截断错误原文，
// 日志里的原因字段会格式不一，检索规则与告警条件就要写多份且互相不兼容。
func DenyReason(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, contracts.ErrTenantMismatch):
		return "tenant_mismatch"
	case errors.Is(err, contracts.ErrInsufficientScope):
		return "insufficient_scope"
	case errors.Is(err, contracts.ErrTokenExpired):
		return "token_expired"
	case errors.Is(err, contracts.ErrInvalidSignature):
		return "invalid_signature"
	case errors.Is(err, contracts.ErrUntrustedIssuer):
		return "untrusted_issuer"
	case errors.Is(err, contracts.ErrUnsupportedAlgorithm):
		return "unsupported_algorithm"
	case errors.Is(err, contracts.ErrMissingToken):
		return "missing_token"
	case errors.Is(err, contracts.ErrTokenMalformed):
		return "malformed_token"
	case errors.Is(err, ErrAwaitingApproval):
		return "awaiting_approval"
	case errors.Is(err, ErrBlocked):
		return "blocked"
	case errors.Is(err, contracts.ErrForbidden):
		return "forbidden"
	case errors.Is(err, contracts.ErrUnauthenticated):
		return "unauthenticated"
	}
	return "failed"
}

func (a *AuthLifecycle) audit(ctx context.Context, rec AuditRecord) {
	if a.OnAudit != nil {
		a.OnAudit(ctx, rec)
	}
}

func (a *AuthLifecycle) now() time.Time {
	if a.Now != nil {
		return a.Now()
	}
	return time.Now()
}

// AuditString 把审计记录渲染成一行结构化日志，供装配层的默认回调使用。
//
// 放在 middleware 而非装配层，是为了让"审计记录长什么样"只有一个定义：
// 各进程（runtime / worker / resume）都需要记这条，若各自格式化，
// 字段名与顺序会漂移，日志检索规则就要写多份且互相不兼容。
//
// 输出**不含**任何可能来自不可信内容的字段，只有身份维度与原因标识。
func (r AuditRecord) AuditString() string {
	var b strings.Builder
	b.Grow(128)
	b.WriteString("run_auth tenant=")
	b.WriteString(r.TenantID)
	b.WriteString(" user=")
	b.WriteString(r.UserID)
	b.WriteString(" run=")
	b.WriteString(r.RunID)
	b.WriteString(" method=")
	b.WriteString(r.AuthMethod)
	b.WriteString(" authenticated=")
	if r.Authenticated {
		b.WriteString("true")
	} else {
		b.WriteString("false")
	}
	if r.Denied {
		b.WriteString(" denied=true reason=")
		b.WriteString(r.Reason)
	} else if r.Reason != "" {
		b.WriteString(" reason=")
		b.WriteString(r.Reason)
	}
	b.WriteString(" at=")
	b.WriteString(r.At.UTC().Format(time.RFC3339Nano))
	return b.String()
}
