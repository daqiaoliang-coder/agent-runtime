package contracts

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// 本文件定义身份认证的稳定契约：Authenticator port 与 Identity 值对象。
//
// 为什么需要这一层。ExecutionContext 自诞生起就携带 TenantID/UserID 两个身份字段，
// 但它们此前只是**透传**：由入口进程（cmd/runtime）从命令行与环境变量读出后逐层传递，
// 全链路没有任何一处校验"这个身份是否有权发起这次 Run"。
// 透传身份不等于已认证身份 —— 谁能在调用处写下 UserID，谁就拥有了那个用户的全部权限。
// 由此产生两个后果：
//
//  1. 审计日志不可信：它记录的是调用方**声称**的身份，而非**证明**的身份；
//  2. 输入护栏的"权限收窄"落不了地：guard.go 三层防御的第 1 层要求
//     "Tool 调用带 Run 发起者身份而非全局服务账号"，可发起者身份既然未经校验，
//     这层防御就只剩第 2、3 层在支撑。
//
// 引入 Authenticator 之后，身份必须先经凭证校验再进入 ExecutionContext，
// worker 注入的 ec.UserID 才有可靠来源（此前只能恒为空，见 worker.go 的注入点注释）。
//
// 与 CredentialProvider 的关系：CredentialProvider 托管的是**本服务对外的凭证**
// （调模型网关、调向量库用的 Key），Authenticator 校验的是**调用方对本服务的凭证**
// （入口收到的 token）。方向相反，但都是"密钥材料"，因此签名密钥同样纳入
// CredentialProvider 托管（见 CredentialPurposeAuthSigning），不另开一条明文配置通道。

// Identity 是一次成功认证得到的调用者身份。
//
// 刻意不携带原始 token：身份是校验的**结果**，凭证是校验的**输入**。
// 让结果结构体持有输入，等于给凭证开了一条通往日志与持久化的路径 ——
// Identity 会进审计记录、会落到 agent_run.user_id，而 token 绝不该去这些地方。
type Identity struct {
	// UserID 是用户标识，落到 ExecutionContext.UserID 与 agent_run.user_id。
	// 对服务账号调用方，这里通常是服务名而非自然人。
	UserID string
	// TenantID 是凭证声称所属的租户。
	//
	// 它必须与业务侧传入的 Run.TenantID 交叉校验：两者不一致意味着
	// "A 租户的凭证正在操作 B 租户的 Run"，是越权而非配置差异。
	// 校验点见 middleware.AuthLifecycle.OnRunStart。
	TenantID string
	// Subject 是凭证的主体标识（JWT 的 sub）。
	// 与 UserID 分开保留，是因为两者可能不同：服务账号代自然人调用时，
	// sub 是服务、UserID 是被代表的用户，审计需要同时知道这两件事。
	Subject string
	// Scopes 是权限范围（JWT 的 scope / scp），已由适配器归一化为切片。
	// 为空表示凭证未声明范围；是否因此拒绝由策略决定，不在本层判断。
	Scopes []string
	// ExpiresAt 是凭证的失效时刻，零值表示不过期。
	// 保留它的意义在于长任务：Run 可能跨越数十分钟，
	// 认证发生在创建时，而工具调用发生在很久之后 —— 有了失效时刻，
	// 下游才能判断"当初的身份现在还成立吗"。
	ExpiresAt time.Time
	// Method 记录身份是怎么被认证的（如 "jwt-hs256" / "static-token"）。
	//
	// 审计必须带上它：一个由弱机制（静态 token 明文比对）认证的身份
	// 与一个由强机制（RS256 非对称签名）认证的身份，可信程度完全不同，
	// 混在一个 UserID 字段里会让事后追溯失去最关键的一环。
	Method string
}

// HasScope 报告身份是否具备指定权限范围。
// scope 为空串时恒返回 false：空范围不构成任何权限，
// 避免"未声明 scope"被误判成"拥有全部权限"这类经典越权。
func (id Identity) HasScope(scope string) bool {
	if scope == "" {
		return false
	}
	for _, s := range id.Scopes {
		if s == scope {
			return true
		}
	}
	return false
}

// Authenticated 报告身份是否可用。
// 判据是 UserID 非空：认证成功的最低要求是知道"是谁"，
// 一个连主体都无法确定的 Identity 不该被下游当作已认证身份使用。
func (id Identity) Authenticated() bool { return id.UserID != "" }

// 认证错误分为两类，调用方必须用 errors.Is 区分，因为处置方式相反。
//
// 第一类是**身份不可信**（ErrUnauthenticated 及其子因）：凭证缺失、格式错误、
// 签名无效、已过期。这类失败可能是配置问题（验签密钥没配好），也可能是攻击。
//
// 第二类是**身份可信但越权**（ErrForbidden 及其子因）：签名验过了、身份是真的，
// 但它无权操作这个租户或这个资源。这类失败几乎一定是配置串号或越权尝试，
// 不该被当作"重试可能就好了"的瞬时故障。
//
// 把两类混成一个错误的代价是具体的：重试策略会对越权请求反复重试
// （每次都同样失败），而运维会把密钥配错导致的全体 401 误判成有人在撞库。
//
// 子因一律用 fmt.Errorf("%w: ...") 挂在父错误上，而不是各自 errors.New：
// 独立定义的"子因"与父错误之间没有任何链接，errors.Is(err, ErrUnauthenticated)
// 会返回 false，于是调用方无法按类分流 —— 契约上承诺的父子关系必须真的成立。
// 这样既能让调用方判断具体原因（是不是过期、是不是签名错），
// 也能让它只关心大类（该重试还是该拒绝）。
var (
	// ErrUnauthenticated 表示调用方未能证明自己是谁，是所有认证失败的父错误。
	ErrUnauthenticated = errors.New("contracts: unauthenticated")
	// ErrMissingToken 表示调用方没有提供凭证。
	//
	// 单独拆出来的理由：它与"提供了但无效"的处置可能不同 ——
	// 部署方可以选择允许匿名访问（本地演示、内网可信调用），
	// 也可以选择一律拒绝。这个决定属于策略，不属于契约，
	// 因此契约只提供足够细的错误让策略层自行判断。
	ErrMissingToken = fmt.Errorf("%w: missing authentication token", ErrUnauthenticated)
	// ErrTokenMalformed 表示凭证无法解析（不是合法的结构）。
	ErrTokenMalformed = fmt.Errorf("%w: malformed token", ErrUnauthenticated)
	// ErrInvalidSignature 表示签名校验失败：内容被篡改，或验签密钥不匹配。
	ErrInvalidSignature = fmt.Errorf("%w: invalid token signature", ErrUnauthenticated)
	// ErrTokenExpired 表示凭证已过期，或尚未生效（nbf 在未来）。
	ErrTokenExpired = fmt.Errorf("%w: token expired or not yet valid", ErrUnauthenticated)
	// ErrUnsupportedAlgorithm 表示凭证声明的签名算法不被本服务接受。
	//
	// 必须显式拒绝而非"尽力校验"：JWT 的 alg=none 攻击与算法混淆攻击
	// 都源于校验方接受了凭证自己声明的算法。这里的约定是白名单 ——
	// 由 Authenticator 实现声明它接受哪些算法，凭证声明的一律不作为依据。
	ErrUnsupportedAlgorithm = fmt.Errorf("%w: unsupported token algorithm", ErrUnauthenticated)
	// ErrUntrustedIssuer 表示凭证的签发方或受众不在本服务的信任范围内。
	//
	// 与 ErrInvalidSignature 分开，是因为两者的故障原因与处置完全不同：
	// 签名无效指向"密钥配错或内容被篡改"，签发方不符指向"凭证来自另一个系统"
	// （常见于多环境共用一套 IdP 而 iss/aud 配串了）。报成签名错误会把排查方向
	// 引向密钥管理，而真正要改的是信任列表配置。
	//
	// 归类为认证失败而非越权：凭证根本不由本服务信任的签发方签发，
	// 身份尚未成立，谈不上"已认证但无权"。
	ErrUntrustedIssuer = fmt.Errorf("%w: untrusted issuer or audience", ErrUnauthenticated)

	// ErrForbidden 表示身份已认证但无权执行本次操作，是越权类失败的父错误。
	ErrForbidden = errors.New("contracts: forbidden")
	// ErrTenantMismatch 表示凭证声称的租户与目标资源的租户不一致。
	ErrTenantMismatch = fmt.Errorf("%w: identity tenant mismatch", ErrForbidden)
	// ErrInsufficientScope 表示凭证缺少本次操作所需的权限范围。
	ErrInsufficientScope = fmt.Errorf("%w: insufficient scope", ErrForbidden)
)

// Authenticator 是身份认证的稳定扩展点（port），与 CredentialProvider 平级。
//
// 实现方负责把"凭证长什么样、用什么密钥验、怎么解析出身份"全部关在适配器内部，
// 使更换认证设施（自建 JWT → 云 IAM → OAuth2 token introspection）只改 adapters 层，
// Runtime 与 worker 的装配代码零改动。
//
// 契约要求：
//   - 实现必须并发安全（同一 Authenticator 会被多个请求协程共享）；
//   - 必须尊重 ctx 的超时与取消 —— 校验可能是一次网络调用（如 introspection）；
//   - 校验失败必须返回 wrap 了 ErrUnauthenticated 或 ErrForbidden 的错误，
//     不可返回裸的 fmt.Errorf，否则调用方无法用 errors.Is 分流处置；
//   - **不得**在错误信息里回显 token 内容或密钥：错误会被日志记录，
//     回显等于把凭证抄进日志系统。
type Authenticator interface {
	// Authenticate 校验凭证并返回身份。
	// token 为去掉鉴权方案前缀（"Bearer "）后的原始凭证串。
	Authenticate(ctx context.Context, token string) (Identity, error)
}

// authenticatorFunc 便于用闭包实现 Authenticator（测试与轻量装配场景）。
type authenticatorFunc func(ctx context.Context, token string) (Identity, error)

func (f authenticatorFunc) Authenticate(ctx context.Context, token string) (Identity, error) {
	return f(ctx, token)
}

// AuthenticatorFunc 把闭包适配成 Authenticator，用法同 middleware.ApproverFunc。
func AuthenticatorFunc(f func(ctx context.Context, token string) (Identity, error)) Authenticator {
	return authenticatorFunc(f)
}

// 凭证在 ctx 中传递，而**不进 ExecutionContext**。
//
// 这是一个必须记录在案的取舍，因为两种做法看起来都可行：
//
// ExecutionContext 是"随调用链下行的环境信息"，token 似乎正属于此类。
// 但 EC 有两个致命属性：它会被写入审计记录（AuditContext 承载 TenantID/UserID/RunID
// 并流入日志与可观测平台），也会随 Run 落库与跨进程传递。
// 把原始凭证放进 EC，等于给它开了一条通往持久化存储与日志系统的路 ——
// 而凭证泄露最常见的途径恰恰就是"被写进了某个日志字段"。
//
// 走 ctx 传递则把凭证的可见范围限制在**单次请求的进程内调用栈**：
// 请求结束即随 ctx 一起丢弃，不落库、不进事件、不出进程。
// 代价与 ExecutionContext 相同（编译期不可见，忘了注入就取不到），
// 但这个代价在安全上是可接受的：取不到凭证的结果是认证失败并拒绝，
// 而不是静默放行。
type authTokenKey struct{}

// WithAuthToken 把原始凭证注入 ctx，返回派生 ctx。
// 注入点应在请求入口（进程边界），此后由认证层读取校验。
func WithAuthToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, authTokenKey{}, token)
}

// AuthTokenFrom 读取 ctx 中的原始凭证。
// 未注入时返回空串与 false，调用方据此决定是拒绝还是按匿名处理。
func AuthTokenFrom(ctx context.Context) (string, bool) {
	tok, ok := ctx.Value(authTokenKey{}).(string)
	return tok, ok && tok != ""
}

// StripBearerScheme 去掉鉴权头的方案前缀，返回纯凭证串。
//
// 单独提供的理由：入口拿到的可能是 "Bearer eyJ..."、"eyJ..."、"bearer eyJ..."
// 或带多余空白的 "  Bearer  eyJ..." 等形态（HTTP 头的方案名大小写不敏感，见 RFC 7235；
// 空白则常来自配置拼接与代理转发）。各处自己 TrimPrefix 会漏掉大小写与空白，
// 导致同一份 token 在入口能验过、在内部转发后验不过 —— 这类不一致极难排查。
//
// 先 trim 再匹配前缀，顺序不能颠倒：若先匹配，带前导空白的头会因为
// 首字符是空格而匹配失败，整个 "Bearer" 会被当成 token 的一部分保留下来，
// 后续验签必然失败，而报错信息会指向"签名无效"而非"头部解析有误"，把人引向错误方向。
func StripBearerScheme(header string) string {
	const prefix = "bearer "
	s := trimSpace(header)
	if len(s) > len(prefix) && asciiEqualFold(s[:len(prefix)], prefix) {
		// 只在前缀完整匹配（含空格）时剥离，避免把 "BearerX..." 这类非法值截成有效值。
		return trimSpace(s[len(prefix):])
	}
	// 只有方案名没有凭证（"Bearer" / "bearer"）：这是"缺凭证"，不是"凭证叫 Bearer"。
	// 返回空串让调用方走 ErrMissingToken 分支，而不是拿着方案名去验签然后报签名错误。
	if asciiEqualFold(s, "bearer") {
		return ""
	}
	return s
}

// asciiEqualFold 对 ASCII 做大小写无关比较。
// 不用 strings.EqualFold 是为了避免 contracts 引入 strings 依赖
// （本包目前只依赖 context/errors/time，保持零第三方与零内部依赖）。
func asciiEqualFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// trimSpace 去掉首尾 ASCII 空白，同样为避免引入 strings。
func trimSpace(s string) string {
	start, end := 0, len(s)
	for start < end && isASCIISpace(s[start]) {
		start++
	}
	for end > start && isASCIISpace(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isASCIISpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '\v' || c == '\f'
}
