package auth

import (
	"agent-runtime/internal/contracts"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// 本文件是认证适配器的**装配入口**，与 credential.FromEnv 同构：
// 各进程（cmd/runtime、cmd/resume）共用这一处按环境变量选择实现，
// 避免每个 main.go 各写一遍配置解析而逐渐漂移。

// 认证相关环境变量。
const (
	// EnvEnabled 是认证总开关，默认 **false**。
	//
	// 这里刻意偏离仓库"安全能力默认开启"的惯例（guard / redact 都默认 true），
	// 理由必须写清：那两项开启后不需要任何外部依赖，而认证需要 IdP 与验签密钥。
	// 若默认开启，所有还没接 IdP 的部署升级后会立刻全站 401 ——
	// 结果往往是干脆不升级，安全状况反而更差。
	// 因此选择"默认关闭 + 显式开启 + 开启失败即启动失败"：
	// 既让升级路径平滑，又不给"以为开了其实没开"留下空间。
	EnvEnabled = "AUTH_ENABLED"
	// EnvAlgorithm 指定验签算法（HS256 / RS256），默认 HS256。
	EnvAlgorithm = "AUTH_JWT_ALGORITHM"
	// EnvIssuer / EnvAudience 为签发方与受众校验，空表示不校验该项。
	EnvIssuer   = "AUTH_JWT_ISSUER"
	EnvAudience = "AUTH_JWT_AUDIENCE"
	// EnvMaxAge 限制 token 最大存活时长（Go duration 格式，如 "30m"），空表示不限制。
	EnvMaxAge = "AUTH_JWT_MAX_AGE"
	// EnvStaticTokens 用于本地开发与测试，格式为 "user:tenant:token"，多条以逗号分隔。
	//
	// 生产环境不应使用：静态 token 无签名、无过期，拿到即等于冒充。
	// 它存在的意义是让本地联调不必先搭一套 IdP。
	EnvStaticTokens = "AUTH_STATIC_TOKENS"
)

// ErrNoSigningKey 表示已开启认证但取不到验签密钥。
//
// 单独定义哨兵错误，是为了让调用方能识别这一种**配置错误**并 fail-fast：
// 此时继续启动会让所有请求验签失败，表现为全体 401，
// 而排查方向会被引向"密钥无效"而非"密钥没配"。
var ErrNoSigningKey = errors.New("auth: signing key not available")

// Config 是一次认证装配的解析结果。
//
// 暴露这个结构体而不只返回 Authenticator，是因为装配方通常还需要知道
// "是否要求身份"（RequireIdentity）以决定 Runtime 的行为 —— 那属于策略而非实现。
type Config struct {
	// Enabled 报告认证是否开启。false 时 Authenticator 为 nil，
	// Runtime.CreateRun 会退化为接入前的行为（身份留空）。
	Enabled bool
	// RequireIdentity 为 true 时，无法确定身份的 Run 一律拒绝创建。
	RequireIdentity bool
	// Authenticator 是装配好的认证器；Enabled 为 false 时为 nil。
	Authenticator contracts.Authenticator
	// Method 描述装配出的实现类型（"jwt-hs256" / "static-token" / "disabled"），
	// 供启动日志输出，让运维确认"这个进程到底在用哪种认证"。
	Method string
}

// FromEnv 按环境变量装配认证器，是各进程共用的唯一入口。
//
// creds 用于取验签密钥（CredentialPurposeAuthSigning），传 nil 表示不用托管层 ——
// 此时只能用静态 token 模式，JWT 模式会因取不到密钥而报错。
//
// 返回错误的情形只有一种：开启了 JWT 认证却拿不到验签密钥。
// 这是配置错误而非运行时故障，应当让进程启动失败而不是静默降级成"不认证"。
func FromEnv(creds contracts.CredentialProvider) (Config, error) {
	cfg := Config{Method: "disabled"}
	if !envBool(EnvEnabled, false) {
		return cfg, nil
	}
	cfg.Enabled = true
	cfg.RequireIdentity = envBool("AUTH_REQUIRE_IDENTITY", false)

	// 静态 token 优先：它让本地开发不必先搭 IdP，也让集成测试有确定行为。
	if raw := strings.TrimSpace(os.Getenv(EnvStaticTokens)); raw != "" {
		ids := parseStaticTokens(raw)
		if len(ids) == 0 {
			return Config{}, fmt.Errorf("auth: %s is set but contains no valid entry (want user:tenant:token)", EnvStaticTokens)
		}
		cfg.Authenticator = &StaticTokenAuthenticator{Identities: ids}
		cfg.Method = "static-token"
		return cfg, nil
	}

	alg := Algorithm(strings.ToUpper(strings.TrimSpace(os.Getenv(EnvAlgorithm))))
	if alg == "" {
		alg = AlgHS256
	}
	if alg != AlgHS256 && alg != AlgRS256 {
		return Config{}, fmt.Errorf("auth: %s=%q unsupported (want HS256 or RS256)", EnvAlgorithm, alg)
	}
	if creds == nil {
		return Config{}, fmt.Errorf("%w: no credential provider configured", ErrNoSigningKey)
	}
	// 验签密钥经托管层获取，不落环境变量、不进本进程的任何字段：
	// 这样换成 Vault / KMS sidecar 时本文件一行都不用改，轮转也无需重启。
	//
	// 这里**只探测可用性，不取出明文**（与 newMemoryOptionsFromEnv 的做法一致）：
	// Verifier 在每次认证时经 KeyProvider 现取，密钥不驻留在装配层的变量里。
	if _, err := creds.Credential(probeContext(), contracts.CredentialPurposeAuthSigning); err != nil {
		return Config{}, fmt.Errorf("%w: %v", ErrNoSigningKey, err)
	}

	v := &Verifier{
		KeyProvider:  creds,
		KeyAlgorithm: alg,
		Issuer:       strings.TrimSpace(os.Getenv(EnvIssuer)),
		Audience:     strings.TrimSpace(os.Getenv(EnvAudience)),
	}
	if s := strings.TrimSpace(os.Getenv(EnvMaxAge)); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return Config{}, fmt.Errorf("auth: %s=%q is not a valid duration: %v", EnvMaxAge, s, err)
		}
		if d <= 0 {
			return Config{}, fmt.Errorf("auth: %s must be positive, got %s", EnvMaxAge, s)
		}
		v.MaxAge = d
	}
	cfg.Authenticator = v
	cfg.Method = "jwt-" + strings.ToLower(string(alg))
	return cfg, nil
}

// parseStaticTokens 解析 "user:tenant:token" 形式的逗号分隔列表。
//
// token 本身可能含冒号（JWT 一定含两个点，但也可能是任意串），
// 因此只按前两个冒号切分：第一段是 user、第二段是 tenant、其余全部属于 token。
func parseStaticTokens(raw string) map[string]contracts.Identity {
	out := make(map[string]contracts.Identity)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		parts := strings.SplitN(entry, ":", 3)
		if len(parts) != 3 || parts[0] == "" || parts[2] == "" {
			// 跳过格式错误的条目而不是整体失败：一条写错不该让整个进程起不来。
			// 但 FromEnv 会在解析结果为空时报错，避免"配了却全部无效"被静默忽略。
			continue
		}
		out[parts[2]] = contracts.Identity{
			UserID:   parts[0],
			TenantID: parts[1],
			Subject:  parts[0],
			Method:   "static-token",
		}
	}
	return out
}

// probeContext 返回一个用于装配期探测的 context。
//
// 装配发生在进程启动阶段，此时还没有请求 context 可用。
// 探测只读本地文件或环境变量（FileProvider / EnvProvider），不涉及网络，
// 因此用 Background 即可；若将来接入需要网络的托管设施，
// 应当把 ctx 从调用方传进来而不是在这里造一个。
func probeContext() context.Context { return context.Background() }

// envBool 解析布尔开关，无法识别时返回默认值。
//
// 与 worker 包的同名函数刻意分开实现：adapters 层不应依赖 worker 层
// （那会造成 adapters → worker → runtime → contracts 的反向依赖）。
// 重复几行解析逻辑的代价，远小于让依赖方向倒置。
func envBool(key string, def bool) bool {
	s := strings.TrimSpace(os.Getenv(key))
	if s == "" {
		return def
	}
	b, err := strconv.ParseBool(s)
	if err != nil {
		return def
	}
	return b
}
