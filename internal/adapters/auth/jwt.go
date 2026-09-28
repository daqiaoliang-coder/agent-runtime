// Package auth 提供 contracts.Authenticator port 的具体适配器实现。
//
// 这一层的职责是：把"凭证长什么样、用什么密钥验、声明字段叫什么名字"这些
// 认证细节全部关在适配器内部，让 Runtime 与 worker 只拿到一个可信的 Identity。
// 更换认证设施（自建 JWT → 云 IAM → OAuth2 introspection）只改这一层，上层零改动。
package auth

import (
	"agent-runtime/internal/contracts"
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Algorithm 是支持的 JWT 签名算法。
type Algorithm string

const (
	AlgHS256 Algorithm = "HS256" // 对称：HMAC-SHA256，密钥为共享密钥
	AlgRS256 Algorithm = "RS256" // 非对称：RSA-SHA256，密钥为 PEM 编码公钥
)

// VerificationKey 是一条"算法 + 密钥材料"的绑定记录。
//
// 为什么必须把算法与密钥绑在一起，而不是让 Verifier 持有一个算法列表加一个密钥列表：
// 这能防住 JWT 最经典的**算法混淆攻击**。设想 Verifier 同时接受 RS256 与 HS256，
// 且密钥集合是共享的 —— 攻击者拿到公开的 RSA 公钥后，
// 可以把 alg 改成 HS256、用公钥字符串作为 HMAC 密钥自行签名，
// 而校验方会拿同一份公钥材料去做 HMAC 比对，签名"验过"，身份被完全伪造。
//
// 绑定之后，每个密钥只能用于它声明的那一种算法：HS256 的密钥材料不会
// 出现在 RS256 的候选集里，反之亦然，混淆攻击在结构上就无法成立。
type VerificationKey struct {
	Algorithm Algorithm
	// Material 对 HS256 是共享密钥原文，对 RS256 是 PEM 编码的公钥。
	// 两种算法的密钥形态不同却共用一个字段，是为了让密钥来源（环境变量 / 文件 /
	// CredentialProvider）保持单一；具体解释由 Algorithm 决定。
	Material string
	// KeyID 对应 JWT header 的 kid，为空表示不参与 kid 匹配（任何 kid 都可用本密钥）。
	// 配置多个密钥时用它避免逐个试签：kid 命中才尝试，减少无谓的密码学运算。
	KeyID string
}

// Verifier 校验 JWT 并产出 Identity，是 Authenticator 的主力实现。
//
// 安全前提（三条都是刻意的设计决定，不是可选优化）：
//  1. **算法白名单**：凭证 header 里声明的 alg 只用于"查是否在白名单内"，
//     绝不用于决定校验方式。alg=none 因不在白名单而被拒绝，
//     这正是 RFC 8725 要求显式拒绝的原因。
//  2. **签名先于声明**：exp/iss/aud 等声明只在签名验证通过后才被读取。
//     反过来的顺序（先看声明再验签）会让攻击者用未签名的载荷影响校验逻辑。
//  3. **常量时间比对**：HMAC 用 hmac.Equal 而非 bytes.Equal，
//     避免通过响应时间差逐字节猜出签名。
type Verifier struct {
	// Keys 为验签密钥候选集。为空时从 KeyProvider 取（见 resolveKeys）。
	Keys []VerificationKey
	// KeyProvider 从托管层取验签密钥，支持外部轮转后进程内自动生效。
	// 与 Keys 同时配置时，两者合并为候选集（轮转过渡期新旧密钥并存）。
	KeyProvider contracts.CredentialProvider
	// KeyAlgorithm 指定 KeyProvider 取到的密钥按哪种算法使用，默认 HS256。
	KeyAlgorithm Algorithm
	// PreviousKeyProvider 用于轮转过渡期的旧密钥；为 nil 时不启用。
	//
	// 存在理由：验签密钥轮转会立刻使所有在途 token 失效，
	// 表现为全体调用方 401。过渡期内同时接受新旧两把密钥，
	// 可以让已签发的 token 自然过期后再彻底移除旧密钥。
	PreviousKeyProvider contracts.CredentialProvider

	// Issuer 非空时校验 iss 声明必须完全相等。
	Issuer string
	// Audience 非空时校验 aud 声明必须包含该值（aud 可为字符串或字符串数组）。
	Audience string
	// ClockSkew 是允许的时钟偏差，<=0 时取 DefaultClockSkew。
	// 设为 0 会因签发方与校验方的时钟微小不同步而随机拒绝有效 token，
	// 这类故障表现为"偶发 401、重试有时成功"，排查成本极高。
	ClockSkew time.Duration
	// MaxAge 限制 token 的最大存活时长（now - iat），<=0 表示不限制。
	//
	// 有了 exp 为什么还要 MaxAge：exp 由签发方决定，一个配置失误的签发方
	// 可能签出有效期十年的 token。MaxAge 是校验方自己设的上界，
	// 保证"即使签发方失控，凭证的可用窗口也不会超过本服务能接受的范围"。
	MaxAge time.Duration

	// Claims 定制声明字段名，为 nil 时用 DefaultClaims。
	Claims *ClaimsMapping
	// Now 便于测试注入时钟；为 nil 时使用 time.Now。
	Now func() time.Time
	// MaxTokenBytes 限制 token 长度，<=0 时取 DefaultMaxTokenBytes。
	// 无界解析会让一个超长 token 消耗大量内存与 CPU（base64 解码 + JSON 解析），
	// 而认证发生在任何限流之前，是廉价的拒绝服务入口。
	MaxTokenBytes int
}

// ClaimsMapping 定制从 JWT 声明到 Identity 字段的映射。
//
// 为什么需要可配置：不同 IdP 的字段名并不统一 ——
// 用户标识可能是 sub、user_id、uid 或 preferred_username；
// 权限范围可能是 scope（空格分隔，OAuth2 惯例）或 scp（数组，Azure AD 惯例）。
// 与其为每个 IdP 写一个适配器，不如把字段名抽成配置。
type ClaimsMapping struct {
	UserID string // 默认 "sub"
	Tenant string // 默认 "tenant_id"
	Scopes string // 默认 "scope"
}

// DefaultClaims 是默认的声明字段映射。
func DefaultClaims() ClaimsMapping {
	return ClaimsMapping{UserID: "sub", Tenant: "tenant_id", Scopes: "scope"}
}

// DefaultClockSkew 是默认时钟偏差容忍。
// 取 30s 的依据：NTP 同步的正常漂移在秒级，30s 足以覆盖绝大多数部署，
// 又短到不会被用来显著延长一个刚过期 token 的可用窗口。
const DefaultClockSkew = 30 * time.Second

// DefaultMaxTokenBytes 是 token 长度上限（64 KiB）。
// 正常 JWT 在数 KiB 量级，64 KiB 已给声明字段留出充裕余量。
const DefaultMaxTokenBytes = 64 << 10

var _ contracts.Authenticator = (*Verifier)(nil)

// jwtHeader 是 JWT 头部中本实现关心的字段。
type jwtHeader struct {
	Alg string `json:"alg"`
	Typ string `json:"typ"`
	Kid string `json:"kid"`
}

// Authenticate 校验 token 并返回身份。
//
// 错误一律 wrap contracts.ErrUnauthenticated（或其子因），使调用方能用
// errors.Is 分流处置。错误信息**刻意不含 token 内容与密钥材料**：
// 这些错误会被日志与追踪系统记录，回显凭证等于把它抄进日志。
func (v *Verifier) Authenticate(ctx context.Context, token string) (contracts.Identity, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		// 不再额外包 ErrUnauthenticated：ErrMissingToken 本身就是它的子因，
		// 重复包装会让错误文本出现两遍 "unauthenticated"，噪音盖住真正的原因。
		return contracts.Identity{}, contracts.ErrMissingToken
	}
	limit := v.MaxTokenBytes
	if limit <= 0 {
		limit = DefaultMaxTokenBytes
	}
	if len(token) > limit {
		return contracts.Identity{}, fmt.Errorf("%w: token exceeds %d bytes", contracts.ErrTokenMalformed, limit)
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return contracts.Identity{}, fmt.Errorf("%w: expected 3 segments, got %d", contracts.ErrTokenMalformed, len(parts))
	}

	// 1) 解析头部，仅用于取 alg 与 kid。此处**不信任** alg 的任何语义，
	//    只用它查白名单；白名单外一律拒绝（含 alg=none）。
	hdrRaw, err := decodeSegment(parts[0])
	if err != nil {
		return contracts.Identity{}, fmt.Errorf("%w: header: %v", contracts.ErrTokenMalformed, err)
	}
	var hdr jwtHeader
	if err := json.Unmarshal(hdrRaw, &hdr); err != nil {
		return contracts.Identity{}, fmt.Errorf("%w: header: %v", contracts.ErrTokenMalformed, err)
	}
	alg := Algorithm(hdr.Alg)
	if !v.accepts(alg) {
		return contracts.Identity{}, fmt.Errorf("%w: %q", contracts.ErrUnsupportedAlgorithm, hdr.Alg)
	}

	// 2) 验签。签名覆盖 header.payload，任一字节被改动都会失败。
	keys, err := v.resolveKeys(ctx, alg)
	if err != nil {
		return contracts.Identity{}, fmt.Errorf("%w: %v", contracts.ErrUnauthenticated, err)
	}
	if len(keys) == 0 {
		return contracts.Identity{}, fmt.Errorf("%w: no verification key configured for %s",
			contracts.ErrUnauthenticated, alg)
	}
	signingInput := parts[0] + "." + parts[1]
	sig, err := decodeSegment(parts[2])
	if err != nil {
		return contracts.Identity{}, fmt.Errorf("%w: signature: %v", contracts.ErrTokenMalformed, err)
	}
	if !v.verify(alg, signingInput, sig, keys, hdr.Kid) {
		// 不区分"密钥不匹配"与"内容被篡改"：两者对调用方的处置相同，
		// 而分开报错会给攻击者一个判断密钥是否接近正确的信号。
		return contracts.Identity{}, fmt.Errorf("%w: %s signature did not verify",
			contracts.ErrInvalidSignature, alg)
	}

	// 3) 签名已验证，此刻读取声明才是安全的。
	payload, err := decodeSegment(parts[1])
	if err != nil {
		return contracts.Identity{}, fmt.Errorf("%w: payload: %v", contracts.ErrTokenMalformed, err)
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return contracts.Identity{}, fmt.Errorf("%w: payload: %v", contracts.ErrTokenMalformed, err)
	}

	id, err := v.toIdentity(claims, alg)
	if err != nil {
		return contracts.Identity{}, err
	}
	return id, nil
}

// accepts 报告算法是否在白名单内。
// 白名单来自配置的 Keys（每条密钥绑定一个算法）与 KeyAlgorithm，
// 而不是一个独立字段 —— 这样"接受某算法"必然意味着"有对应密钥"，
// 不会出现接受 RS256 却没配公钥这种必然失败的组合。
func (v *Verifier) accepts(alg Algorithm) bool {
	switch alg {
	case AlgHS256, AlgRS256:
	default:
		return false
	}
	for _, k := range v.Keys {
		if k.Algorithm == alg {
			return true
		}
	}
	if v.KeyProvider != nil || v.PreviousKeyProvider != nil {
		return v.keyAlgorithm() == alg
	}
	return false
}

func (v *Verifier) keyAlgorithm() Algorithm {
	if v.KeyAlgorithm != "" {
		return v.KeyAlgorithm
	}
	return AlgHS256
}

// resolveKeys 汇总指定算法下的全部候选密钥。
//
// 只返回与目标算法匹配的密钥，这是算法混淆攻击的结构性防线（见 VerificationKey 注释）。
func (v *Verifier) resolveKeys(ctx context.Context, alg Algorithm) ([]VerificationKey, error) {
	var out []VerificationKey
	for _, k := range v.Keys {
		if k.Algorithm == alg && k.Material != "" {
			out = append(out, k)
		}
	}
	if v.KeyProvider != nil && v.keyAlgorithm() == alg {
		if k, err := keyFromProvider(ctx, v.KeyProvider, alg); err != nil {
			return nil, err
		} else if k != nil {
			out = append(out, *k)
		}
	}
	if v.PreviousKeyProvider != nil && v.keyAlgorithm() == alg {
		// 旧密钥取不到不算致命错误：它只在轮转过渡期有意义，
		// 托管设施里没有旧密钥是常态（未轮转或已彻底移除）。
		if k, err := keyFromProvider(ctx, v.PreviousKeyProvider, alg); err == nil && k != nil {
			out = append(out, *k)
		}
	}
	return out, nil
}

// keyFromProvider 从托管层取密钥并绑定算法。
// 每次调用都真实回源，使外部轮转写入后能被下一次认证立即感知
// （与 credential.FileProvider 的取舍一致：本地读取廉价，不缓存换来轮转即时生效）。
func keyFromProvider(ctx context.Context, p contracts.CredentialProvider, alg Algorithm) (*VerificationKey, error) {
	cred, err := p.Credential(ctx, contracts.CredentialPurposeAuthSigning)
	if err != nil {
		return nil, fmt.Errorf("resolve auth signing key: %w", err)
	}
	if cred.Value == "" {
		return nil, nil
	}
	return &VerificationKey{Algorithm: alg, Material: cred.Value}, nil
}

// verify 用候选密钥逐个尝试验签，任一成功即通过。
//
// kid 命中时优先尝试对应密钥：配置了多把密钥时，这能避免对每个请求
// 都做 N 次密码学运算。kid 不匹配任何已配置密钥时仍会回退到全量尝试，
// 因为 kid 由签发方填写，本服务不该把它当成硬性筛选条件。
func (v *Verifier) verify(alg Algorithm, signingInput string, sig []byte, keys []VerificationKey, kid string) bool {
	tried := make(map[int]bool, len(keys))
	for i := range keys {
		if kid != "" && keys[i].KeyID == kid {
			if verifyOne(alg, signingInput, sig, &keys[i]) {
				return true
			}
			tried[i] = true
		}
	}
	for i := range keys {
		if tried[i] {
			continue
		}
		if verifyOne(alg, signingInput, sig, &keys[i]) {
			return true
		}
	}
	return false
}

// verifyOne 按算法执行单次验签。
func verifyOne(alg Algorithm, signingInput string, sig []byte, k *VerificationKey) bool {
	switch alg {
	case AlgHS256:
		mac := hmac.New(sha256.New, []byte(k.Material))
		mac.Write([]byte(signingInput))
		// hmac.Equal 是常量时间比较，防时序侧信道。
		return hmac.Equal(sig, mac.Sum(nil))
	case AlgRS256:
		pub, err := parseRSAPublicKey(k.Material)
		if err != nil {
			return false
		}
		sum := sha256.Sum256([]byte(signingInput))
		return rsa.VerifyPKCS1v15(pub, crypto.SHA256, sum[:], sig) == nil
	}
	return false
}

// parseRSAPublicKey 解析 PEM 编码的 RSA 公钥，兼容 PKIX 与 PKCS#1 两种格式。
func parseRSAPublicKey(material string) (*rsa.PublicKey, error) {
	block, _ := pem.Decode([]byte(material))
	if block == nil {
		return nil, errors.New("auth: PEM block not found")
	}
	switch block.Type {
	case "PUBLIC KEY":
		pub, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("auth: parse PKIX public key: %w", err)
		}
		rsaPub, ok := pub.(*rsa.PublicKey)
		if !ok {
			return nil, errors.New("auth: PKIX key is not RSA")
		}
		return rsaPub, nil
	case "RSA PUBLIC KEY":
		return x509.ParsePKCS1PublicKey(block.Bytes)
	}
	return nil, fmt.Errorf("auth: unsupported PEM type %q", block.Type)
}

// toIdentity 从已验签的声明构造 Identity，并校验时间与签发方约束。
func (v *Verifier) toIdentity(claims map[string]any, alg Algorithm) (contracts.Identity, error) {
	now := time.Now()
	if v.Now != nil {
		now = v.Now()
	}
	skew := v.ClockSkew
	if skew <= 0 {
		skew = DefaultClockSkew
	}

	// 时间声明校验。exp 必须存在：一个永不过期的 token 一旦泄露就无法失效，
	// 而这恰恰是最需要认证的凭证形态。缺 exp 直接拒绝而非"当作不过期"。
	exp, hasExp, err := numericClaim(claims, "exp")
	if err != nil {
		return contracts.Identity{}, err
	}
	if !hasExp {
		return contracts.Identity{}, fmt.Errorf("%w: missing exp claim", contracts.ErrTokenMalformed)
	}
	expAt := time.Unix(int64(exp), 0)
	if !now.Add(-skew).Before(expAt) {
		return contracts.Identity{}, fmt.Errorf("%w: expired at %s", contracts.ErrTokenExpired, expAt.UTC().Format(time.RFC3339))
	}
	if nbf, ok, err := numericClaim(claims, "nbf"); err != nil {
		return contracts.Identity{}, err
	} else if ok && now.Add(skew).Before(time.Unix(int64(nbf), 0)) {
		return contracts.Identity{}, fmt.Errorf("%w: not valid before %s",
			contracts.ErrTokenExpired, time.Unix(int64(nbf), 0).UTC().Format(time.RFC3339))
	}
	if v.MaxAge > 0 {
		iat, ok, err := numericClaim(claims, "iat")
		if err != nil {
			return contracts.Identity{}, err
		}
		// 无 iat 时无法判断 token 年龄，按超限拒绝：
		// MaxAge 的意义正是"即使签发方失控也要有上界"，
		// 而缺失 iat 的 token 恰好无法被这个上界约束，放过它就等于绕过。
		if !ok {
			return contracts.Identity{}, fmt.Errorf("%w: missing iat claim (max age %s enforced)",
				contracts.ErrTokenExpired, v.MaxAge)
		}
		if now.Sub(time.Unix(int64(iat), 0)) > v.MaxAge+skew {
			return contracts.Identity{}, fmt.Errorf("%w: token age exceeds %s", contracts.ErrTokenExpired, v.MaxAge)
		}
	}

	if v.Issuer != "" {
		iss, _ := claims["iss"].(string)
		if iss != v.Issuer {
			// 刻意用 ErrUntrustedIssuer 而非 ErrInvalidSignature：签名是好的，
			// 问题在于凭证来自本服务不信任的签发方（常见于多环境共用 IdP 而 iss 配串）。
			// 报成签名错误会让运维去查密钥轮转，而真正要改的是信任列表。
			return contracts.Identity{}, fmt.Errorf("%w: expected iss %q", contracts.ErrUntrustedIssuer, v.Issuer)
		}
	}
	if v.Audience != "" && !hasAudience(claims["aud"], v.Audience) {
		return contracts.Identity{}, fmt.Errorf("%w: expected aud %q", contracts.ErrUntrustedIssuer, v.Audience)
	}

	m := DefaultClaims()
	if v.Claims != nil {
		if v.Claims.UserID != "" {
			m.UserID = v.Claims.UserID
		}
		if v.Claims.Tenant != "" {
			m.Tenant = v.Claims.Tenant
		}
		if v.Claims.Scopes != "" {
			m.Scopes = v.Claims.Scopes
		}
	}

	subject, _ := claims["sub"].(string)
	userID := stringClaim(claims, m.UserID)
	// UserID 优先取配置字段；该字段缺失时回退到 sub。
	// 回退的理由：sub 是 JWT 唯一强制的主体声明，多数 IdP 下它就是用户标识，
	// 强制要求另配字段会让最常见的部署形态开箱不可用。
	if userID == "" {
		userID = subject
	}
	if userID == "" {
		return contracts.Identity{}, fmt.Errorf("%w: token carries no user identity", contracts.ErrTokenMalformed)
	}

	tenant, _ := claims[m.Tenant].(string)
	id := contracts.Identity{
		UserID:    userID,
		TenantID:  tenant,
		Subject:   subject,
		Scopes:    scopesOf(claims[m.Scopes]),
		ExpiresAt: expAt,
		Method:    "jwt-" + strings.ToLower(string(alg)),
	}
	return id, nil
}

// numericClaim 读取数值型声明。
// 兼容 float64 与 json.Number：encoding/json 默认把数字解成 float64，
// 但若调用方用了 UseNumber，类型就变成 json.Number，只认一种会在特定装配下静默失败。
func numericClaim(claims map[string]any, name string) (float64, bool, error) {
	raw, ok := claims[name]
	if !ok || raw == nil {
		return 0, false, nil
	}
	switch n := raw.(type) {
	case float64:
		return n, true, nil
	case int:
		return float64(n), true, nil
	case int64:
		return float64(n), true, nil
	case json.Number:
		f, err := n.Float64()
		if err != nil {
			return 0, false, fmt.Errorf("%w: claim %q: %v", contracts.ErrTokenMalformed, name, err)
		}
		return f, true, nil
	}
	return 0, false, fmt.Errorf("%w: claim %q is not numeric", contracts.ErrTokenMalformed, name)
}

// stringClaim 读取字符串型声明，非字符串或缺失时返回空串。
func stringClaim(claims map[string]any, name string) string {
	if s, ok := claims[name].(string); ok {
		return s
	}
	return ""
}

// hasAudience 校验 aud 声明。
// aud 按 JWT 规范既可是单个字符串也可是字符串数组，两种都要支持。
func hasAudience(raw any, want string) bool {
	switch a := raw.(type) {
	case string:
		return a == want
	case []any:
		for _, item := range a {
			if s, ok := item.(string); ok && s == want {
				return true
			}
		}
	}
	return false
}

// scopesOf 把权限范围声明归一化为切片。
//
// 兼容三种形态：空格分隔字符串（OAuth2 标准）、字符串数组（Azure AD 的 scp）、
// 单个字符串。不做归一化会让不同 IdP 的 token 在授权判定上表现不一致，
// 而这种不一致通常只在部分调用方身上出现，极难定位。
func scopesOf(raw any) []string {
	switch s := raw.(type) {
	case string:
		return splitScopes(s)
	case []any:
		var out []string
		for _, item := range s {
			if str, ok := item.(string); ok {
				out = append(out, splitScopes(str)...)
			}
		}
		return out
	}
	return nil
}

// splitScopes 按空白切分范围串，忽略空项。
func splitScopes(s string) []string {
	fields := strings.Fields(s)
	if len(fields) == 0 {
		return nil
	}
	return fields
}

// decodeSegment 解码 base64url 段。
//
// 优先按无填充（RawURLEncoding）解码，失败后回退到有填充形式：
// JWT 规范要求无填充，但部分签发方会带上 '='。解码放宽不构成安全风险 ——
// 安全性由签名验证保证，而不是由编码形式保证。
func decodeSegment(s string) ([]byte, error) {
	if b, err := base64.RawURLEncoding.DecodeString(s); err == nil {
		return b, nil
	}
	return base64.URLEncoding.DecodeString(s)
}

// StaticTokenAuthenticator 按"token → 身份"映射做认证，用于测试与本地演示。
//
// 不要在生产中使用：它把凭证与身份的对应关系写进了进程内存，
// 既无签名也无过期，任何拿到映射内容的人都能冒充任意身份。
// 它的价值在于让上层代码统一走 Authenticator port，
// 这样后续换成 Verifier 或云 IAM 适配器时装配代码一行都不用改。
type StaticTokenAuthenticator struct {
	// Identities 是 token 到身份的映射，必填。
	Identities map[string]contracts.Identity
}

var _ contracts.Authenticator = (*StaticTokenAuthenticator)(nil)

// Authenticate 查表返回身份；未命中一律报签名无效。
//
// 不区分"token 不存在"与"token 无效"：前者会泄露"哪些 token 曾经有效"，
// 给攻击者一个枚举方向。
func (a *StaticTokenAuthenticator) Authenticate(_ context.Context, token string) (contracts.Identity, error) {
	if strings.TrimSpace(token) == "" {
		return contracts.Identity{}, contracts.ErrMissingToken
	}
	id, ok := a.Identities[token]
	if !ok || !id.Authenticated() {
		return contracts.Identity{}, fmt.Errorf("%w: token not recognized", contracts.ErrInvalidSignature)
	}
	out := id
	if out.Method == "" {
		out.Method = "static-token"
	}
	return out, nil
}

// AllowAll 返回一个放行一切并给出固定身份的 Authenticator，**仅供本地开发与测试**。
//
// 必须显式命名成 AllowAll 而不是 NewDevAuthenticator 之类：
// 让"这里完全不做认证"在代码里一目了然，避免它被误当成正常配置带到生产。
// 生产装配应当拒绝它（见 worker 侧的装配校验）。
func AllowAll(userID, tenantID string) contracts.Authenticator {
	return contracts.AuthenticatorFunc(func(context.Context, string) (contracts.Identity, error) {
		return contracts.Identity{
			UserID:   userID,
			TenantID: tenantID,
			Subject:  userID,
			Method:   "allow-all-insecure",
		}, nil
	})
}
