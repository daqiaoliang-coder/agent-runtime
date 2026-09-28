package auth

import (
	"agent-runtime/internal/contracts"
	"context"
	"crypto"
	"crypto/hmac"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"strings"
	"testing"
	"time"
)

// 本文件的测试组织原则与 executor/contentchain_test.go 一致：
// 不停留在"函数返回了预期结构体"，而是用**攻击者视角**的可观测结果证明防线成立。
// 密码学代码最危险的失效方式是"看起来在工作、实际什么都放过"，
// 因此每个用例都断言到具体错误类型（errors.Is），而不是只看 err != nil。

// signHS256 构造一个 HS256 JWT，是各用例共用的签发工具。
func signHS256(t *testing.T, secret string, header, claims map[string]any) string {
	t.Helper()
	return signWithAlg(t, header, claims, func(input []byte) []byte {
		mac := hmac.New(sha256.New, []byte(secret))
		mac.Write(input)
		return mac.Sum(nil)
	})
}

func signWithAlg(t *testing.T, header, claims map[string]any, sign func([]byte) []byte) string {
	t.Helper()
	enc := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		return base64.RawURLEncoding.EncodeToString(b)
	}
	input := enc(header) + "." + enc(claims)
	return input + "." + base64.RawURLEncoding.EncodeToString(sign([]byte(input)))
}

const testSecret = "test-signing-secret-not-for-production"

// validClaims 构造一份默认有效的声明集（含 exp/iat，缺一个都过不了 Verifier）。
func validClaims(userID, tenant string) map[string]any {
	now := time.Now()
	return map[string]any{
		"sub":       userID,
		"tenant_id": tenant,
		"scope":     "run:read run:write",
		"iat":       now.Unix(),
		"exp":       now.Add(time.Hour).Unix(),
	}
}

func TestVerifierAcceptsValidToken(t *testing.T) {
	v := &Verifier{Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}}}
	id, err := v.Authenticate(context.Background(), signHS256(t, testSecret,
		map[string]any{"alg": "HS256", "typ": "JWT"}, validClaims("user-42", "tenant-A")))
	if err != nil {
		t.Fatalf("expected valid token to authenticate, got %v", err)
	}
	if id.UserID != "user-42" {
		t.Errorf("UserID = %q, want user-42", id.UserID)
	}
	if id.TenantID != "tenant-A" {
		t.Errorf("TenantID = %q, want tenant-A", id.TenantID)
	}
	if !id.HasScope("run:write") {
		t.Errorf("expected scope run:write, got %v", id.Scopes)
	}
	// HasScope 对空串必须为 false：否则"未声明 scope"会被误判成拥有权限。
	if id.HasScope("") {
		t.Error("HasScope(\"\") must be false")
	}
	if id.Method != "jwt-hs256" {
		t.Errorf("Method = %q, want jwt-hs256", id.Method)
	}
}

func TestVerifierRejectsTamperedPayload(t *testing.T) {
	v := &Verifier{Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}}}
	token := signHS256(t, testSecret, map[string]any{"alg": "HS256"}, validClaims("user-42", "tenant-A"))

	// 把 payload 段换成"提权后的用户"，签名保持不变 —— 这是最基础的篡改攻击。
	parts := strings.Split(token, ".")
	priv := validClaims("admin", "tenant-A")
	raw, _ := json.Marshal(priv)
	parts[1] = base64.RawURLEncoding.EncodeToString(raw)
	tampered := strings.Join(parts, ".")

	_, err := v.Authenticate(context.Background(), tampered)
	if !errors.Is(err, contracts.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature for tampered payload, got %v", err)
	}
}

func TestVerifierRejectsWrongSecret(t *testing.T) {
	v := &Verifier{Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}}}
	// 用另一把密钥签发，结构与声明全部合法。
	token := signHS256(t, "a-completely-different-secret",
		map[string]any{"alg": "HS256"}, validClaims("user-42", "tenant-A"))

	if _, err := v.Authenticate(context.Background(), token); !errors.Is(err, contracts.ErrInvalidSignature) {
		t.Fatalf("expected ErrInvalidSignature, got %v", err)
	}
}

func TestVerifierRejectsExpiredAndNotYetValid(t *testing.T) {
	frozen := time.Now()
	v := &Verifier{
		Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}},
		Now:  func() time.Time { return frozen },
	}

	cases := []struct {
		name   string
		claims map[string]any
	}{
		{
			name: "expired",
			claims: map[string]any{
				"sub": "u", "tenant_id": "t",
				"iat": frozen.Add(-2 * time.Hour).Unix(),
				"exp": frozen.Add(-1 * time.Hour).Unix(),
			},
		},
		{
			name: "not_yet_valid",
			claims: map[string]any{
				"sub": "u", "tenant_id": "t",
				"iat": frozen.Add(time.Hour).Unix(),
				"nbf": frozen.Add(time.Hour).Unix(),
				"exp": frozen.Add(2 * time.Hour).Unix(),
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			token := signHS256(t, testSecret, map[string]any{"alg": "HS256"}, tc.claims)
			if _, err := v.Authenticate(context.Background(), token); !errors.Is(err, contracts.ErrTokenExpired) {
				t.Fatalf("expected ErrTokenExpired, got %v", err)
			}
		})
	}
}

// TestVerifierToleratesClockSkew 证明时钟偏差容忍确实在起作用：
// 一个刚过期 5 秒的 token 在 30s 容忍窗口内应当被接受。
// 没有这个用例，DefaultClockSkew 就是一段"写了但没人验证"的代码。
func TestVerifierToleratesClockSkew(t *testing.T) {
	frozen := time.Now()
	v := &Verifier{
		Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}},
		Now:  func() time.Time { return frozen },
	}
	claims := map[string]any{
		"sub": "u", "tenant_id": "t",
		"iat": frozen.Add(-time.Hour).Unix(),
		"exp": frozen.Add(-5 * time.Second).Unix(), // 刚过期 5s，在 30s 容忍内
	}
	token := signHS256(t, testSecret, map[string]any{"alg": "HS256"}, claims)
	if _, err := v.Authenticate(context.Background(), token); err != nil {
		t.Fatalf("expected token within clock skew to be accepted, got %v", err)
	}
}

func TestVerifierRejectsMissingExp(t *testing.T) {
	v := &Verifier{Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}}}
	// 永不过期的 token 一旦泄露就无法失效，因此缺 exp 必须拒绝而不是"当作不过期"。
	claims := map[string]any{"sub": "u", "tenant_id": "t", "iat": time.Now().Unix()}
	token := signHS256(t, testSecret, map[string]any{"alg": "HS256"}, claims)

	if _, err := v.Authenticate(context.Background(), token); !errors.Is(err, contracts.ErrTokenMalformed) {
		t.Fatalf("expected ErrTokenMalformed for missing exp, got %v", err)
	}
}

// TestVerifierRejectsAlgNone 覆盖 JWT 最经典的绕过手法：alg=none 不签名。
func TestVerifierRejectsAlgNone(t *testing.T) {
	v := &Verifier{Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}}}

	for _, alg := range []string{"none", "None", "NONE"} {
		t.Run(alg, func(t *testing.T) {
			enc := func(val any) string {
				b, _ := json.Marshal(val)
				return base64.RawURLEncoding.EncodeToString(b)
			}
			// alg=none 的"签名"段为空，这是该攻击的标准形态。
			token := enc(map[string]any{"alg": alg, "typ": "JWT"}) + "." +
				enc(validClaims("admin", "tenant-A")) + "."
			if _, err := v.Authenticate(context.Background(), token); !errors.Is(err, contracts.ErrUnsupportedAlgorithm) {
				t.Fatalf("expected ErrUnsupportedAlgorithm for alg=%s, got %v", alg, err)
			}
		})
	}
}

// TestVerifierRejectsAlgorithmConfusion 是本文件最重要的一个用例。
//
// 攻击手法：Verifier 同时接受 RS256 与 HS256 时，攻击者拿到公开的 RSA 公钥，
// 把 alg 改成 HS256、用公钥字符串本身作为 HMAC 密钥签名。
// 若校验方按 alg 从"共享密钥集合"里取材料，就会拿公钥去做 HMAC 比对而验签通过，
// 从而用一把**公开**的密钥伪造出任意身份。
//
// 断言方式：构造这样一个 HS256 token，其中 HMAC 密钥恰好是 RS256 公钥的 PEM 原文。
// 正确的实现必须拒绝它 —— 因为该公钥只绑定给 RS256，不在 HS256 的候选集里。
func TestVerifierRejectsAlgorithmConfusion(t *testing.T) {
	rsaPriv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate rsa key: %v", err)
	}
	pubPEM, err := marshalPublicKeyPEM(&rsaPriv.PublicKey)
	if err != nil {
		t.Fatalf("marshal public key: %v", err)
	}
	hsSecret := "hs-secret-unrelated-to-rsa"

	// 同时接受两种算法，模拟"配置宽松"的部署。
	v := &Verifier{Keys: []VerificationKey{
		{Algorithm: AlgRS256, Material: pubPEM},
		{Algorithm: AlgHS256, Material: hsSecret},
	}}

	// 攻击者用公钥 PEM 作为 HMAC 密钥签发 HS256 token。
	confused := signHS256(t, pubPEM, map[string]any{"alg": "HS256"}, validClaims("admin", "tenant-A"))
	if _, err := v.Authenticate(context.Background(), confused); !errors.Is(err, contracts.ErrInvalidSignature) {
		t.Fatalf("algorithm confusion attack must be rejected with ErrInvalidSignature, got %v", err)
	}

	// 反向也验证一次：用 HS 密钥去签 RS256 声明同样必须失败，
	// 证明"绑定"是双向的，而不是恰好因为某个实现细节挡住了一个方向。
	bogusRS := signWithAlg(t, map[string]any{"alg": "RS256"}, validClaims("admin", "tenant-A"),
		func(input []byte) []byte {
			mac := hmac.New(sha256.New, []byte(hsSecret))
			mac.Write(input)
			return mac.Sum(nil)
		})
	if _, err := v.Authenticate(context.Background(), bogusRS); err == nil {
		t.Fatal("expected RS256 token signed with HMAC secret to be rejected")
	}

	// 正向对照：用真正的 RSA 私钥签 RS256 必须通过。
	// 没有这条对照，上面两个"拒绝"可能只是因为 RS256 整体坏掉了。
	goodRS := signWithAlg(t, map[string]any{"alg": "RS256"}, validClaims("user-7", "tenant-A"),
		func(input []byte) []byte {
			sum := sha256.Sum256(input)
			sig, err := rsa.SignPKCS1v15(rand.Reader, rsaPriv, crypto.SHA256, sum[:])
			if err != nil {
				t.Fatalf("sign: %v", err)
			}
			return sig
		})
	id, err := v.Authenticate(context.Background(), goodRS)
	if err != nil {
		t.Fatalf("expected legitimate RS256 token to authenticate, got %v", err)
	}
	if id.UserID != "user-7" || id.Method != "jwt-rs256" {
		t.Errorf("unexpected identity: %+v", id)
	}
}

func marshalPublicKeyPEM(pub *rsa.PublicKey) (string, error) {
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		return "", err
	}
	block := &pem.Block{Type: "PUBLIC KEY", Bytes: der}
	var sb strings.Builder
	if err := pem.Encode(&sb, block); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func TestVerifierChecksIssuerAndAudience(t *testing.T) {
	v := &Verifier{
		Keys:     []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}},
		Issuer:   "https://idp.example.com",
		Audience: "agent-runtime",
	}
	newToken := func(mutate func(map[string]any)) string {
		c := validClaims("u", "t")
		c["iss"] = "https://idp.example.com"
		c["aud"] = "agent-runtime"
		if mutate != nil {
			mutate(c)
		}
		return signHS256(t, testSecret, map[string]any{"alg": "HS256"}, c)
	}

	if _, err := v.Authenticate(context.Background(), newToken(nil)); err != nil {
		t.Fatalf("expected matching iss/aud to pass, got %v", err)
	}
	// iss/aud 不符应报 ErrUntrustedIssuer 而非签名错误：签名是好的，
	// 问题在信任列表。混淆两者会把排查方向引向密钥管理。
	wrongIss := v2Err(t, v, newToken(func(c map[string]any) { c["iss"] = "https://evil.example.com" }))
	if !errors.Is(wrongIss, contracts.ErrUntrustedIssuer) {
		t.Fatalf("expected ErrUntrustedIssuer for wrong issuer, got %v", wrongIss)
	}
	if errors.Is(wrongIss, contracts.ErrInvalidSignature) {
		t.Error("untrusted issuer must not be reported as a signature failure")
	}
	wrongAud := v2Err(t, v, newToken(func(c map[string]any) { c["aud"] = "some-other-service" }))
	if !errors.Is(wrongAud, contracts.ErrUntrustedIssuer) {
		t.Fatalf("expected ErrUntrustedIssuer for wrong audience, got %v", wrongAud)
	}
	// 两者都必须仍归属"认证失败"大类，调用方才能统一按不可信处置。
	if !errors.Is(wrongIss, contracts.ErrUnauthenticated) || !errors.Is(wrongAud, contracts.ErrUnauthenticated) {
		t.Error("untrusted issuer errors must wrap ErrUnauthenticated")
	}
	// aud 为数组形态时同样要能匹配（Azure AD 等 IdP 用数组）。
	if _, err := v.Authenticate(context.Background(), newToken(func(c map[string]any) {
		c["aud"] = []any{"other", "agent-runtime"}
	})); err != nil {
		t.Fatalf("expected array audience containing target to pass, got %v", err)
	}
}

// v2Err 断言认证必须失败并返回该错误，供上面几个分支复用。
func v2Err(t *testing.T, v *Verifier, token string) error {
	t.Helper()
	_, err := v.Authenticate(context.Background(), token)
	if err == nil {
		t.Fatal("expected authentication to fail")
	}
	return err
}

func TestVerifierEnforcesMaxAge(t *testing.T) {
	frozen := time.Now()
	v := &Verifier{
		Keys:   []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}},
		Now:    func() time.Time { return frozen },
		MaxAge: 10 * time.Minute,
	}
	// iat 在两小时前、exp 仍在未来：签发方给了超长有效期，MaxAge 是本服务自己的上界。
	claims := validClaims("u", "t")
	claims["iat"] = frozen.Add(-2 * time.Hour).Unix()
	claims["exp"] = frozen.Add(24 * time.Hour).Unix()
	token := signHS256(t, testSecret, map[string]any{"alg": "HS256"}, claims)

	if _, err := v.Authenticate(context.Background(), token); !errors.Is(err, contracts.ErrTokenExpired) {
		t.Fatalf("expected token older than MaxAge to be rejected, got %v", err)
	}

	// 缺 iat 时无法约束年龄，必须拒绝而非放过。
	noIat := map[string]any{"sub": "u", "tenant_id": "t", "exp": frozen.Add(time.Hour).Unix()}
	if _, err := v.Authenticate(context.Background(),
		signHS256(t, testSecret, map[string]any{"alg": "HS256"}, noIat)); !errors.Is(err, contracts.ErrTokenExpired) {
		t.Fatalf("expected missing iat to be rejected when MaxAge is set, got %v", err)
	}
}

func TestVerifierReadsKeyFromCredentialProvider(t *testing.T) {
	// 验签密钥走托管层而非明文配置，是本次接入的核心目的：
	// 密钥不进环境变量、不落配置中心，且外部轮转后进程无需重启。
	provider := &rotatingProvider{secret: testSecret}
	v := &Verifier{KeyProvider: provider, KeyAlgorithm: AlgHS256}
	token := signHS256(t, testSecret, map[string]any{"alg": "HS256"}, validClaims("u", "t"))

	if _, err := v.Authenticate(context.Background(), token); err != nil {
		t.Fatalf("expected provider-supplied key to verify token, got %v", err)
	}

	// 轮转：托管层换成新密钥后，旧 token 立刻失效、新 token 立刻可用。
	// 这证明 Verifier 每次都真实回源，没有把密钥缓存在进程里。
	newSecret := "rotated-secret"
	provider.secret = newSecret
	if _, err := v.Authenticate(context.Background(), token); !errors.Is(err, contracts.ErrInvalidSignature) {
		t.Fatalf("expected old token to fail after rotation, got %v", err)
	}
	rotated := signHS256(t, newSecret, map[string]any{"alg": "HS256"}, validClaims("u", "t"))
	if _, err := v.Authenticate(context.Background(), rotated); err != nil {
		t.Fatalf("expected new token to verify after rotation, got %v", err)
	}
}

// TestVerifierRotationWindowAcceptsBothKeys 证明过渡期能同时接受新旧密钥。
// 没有它，轮转就等于一次全量 401。
func TestVerifierRotationWindowAcceptsBothKeys(t *testing.T) {
	oldSecret, newSecret := "old-secret", "new-secret"
	v := &Verifier{
		KeyProvider:         &rotatingProvider{secret: newSecret},
		PreviousKeyProvider: &rotatingProvider{secret: oldSecret},
		KeyAlgorithm:        AlgHS256,
	}
	for _, s := range []string{oldSecret, newSecret} {
		token := signHS256(t, s, map[string]any{"alg": "HS256"}, validClaims("u", "t"))
		if _, err := v.Authenticate(context.Background(), token); err != nil {
			t.Fatalf("expected token signed with %q to verify during rotation window, got %v", s, err)
		}
	}
}

// rotatingProvider 是一个会变的 CredentialProvider，用于模拟外部轮转。
type rotatingProvider struct{ secret string }

func (p *rotatingProvider) Credential(context.Context, contracts.CredentialPurpose) (contracts.Credential, error) {
	if p.secret == "" {
		return contracts.Credential{}, errors.New("no secret")
	}
	return contracts.Credential{Value: p.secret}, nil
}

func TestVerifierRejectsMalformedTokens(t *testing.T) {
	v := &Verifier{Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}}}
	cases := map[string]string{
		"empty":          "",
		"garbage":        "not-a-jwt",
		"two_segments":   "aaa.bbb",
		"four_segments":  "aaa.bbb.ccc.ddd",
		"invalid_base64": "!!!.!!!.!!!",
		"invalid_header": base64.RawURLEncoding.EncodeToString([]byte("nope")) + ".e30.",
	}
	for name, token := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := v.Authenticate(context.Background(), token)
			if err == nil {
				t.Fatalf("expected %q to be rejected", name)
			}
			// 必须是可分流的认证错误，而不是一个裸 fmt.Errorf：
			// 调用方靠 errors.Is 判断该拒绝还是该重试。
			if !errors.Is(err, contracts.ErrUnauthenticated) {
				t.Fatalf("expected error to wrap ErrUnauthenticated, got %v", err)
			}
		})
	}
}

// TestVerifierRejectsOversizeToken 证明认证发生在限流之前时的资源保护。
func TestVerifierRejectsOversizeToken(t *testing.T) {
	v := &Verifier{
		Keys:          []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}},
		MaxTokenBytes: 64,
	}
	big := signHS256(t, testSecret, map[string]any{"alg": "HS256"}, validClaims("u", "t"))
	if len(big) <= 64 {
		t.Fatalf("test token should exceed the tiny limit, got %d bytes", len(big))
	}
	if _, err := v.Authenticate(context.Background(), big); !errors.Is(err, contracts.ErrTokenMalformed) {
		t.Fatalf("expected oversize token to be rejected, got %v", err)
	}
}

// TestVerifierErrorDoesNotLeakSecret 是安全回归测试：
// 认证错误会进日志，任何一条错误里出现 token 或密钥原文都等于把它抄进日志系统。
func TestVerifierErrorDoesNotLeakSecret(t *testing.T) {
	v := &Verifier{Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}}}
	token := signHS256(t, "wrong-secret", map[string]any{"alg": "HS256"}, validClaims("u", "t"))

	_, err := v.Authenticate(context.Background(), token)
	if err == nil {
		t.Fatal("expected failure")
	}
	msg := err.Error()
	for _, leak := range []string{testSecret, "wrong-secret", token} {
		if strings.Contains(msg, leak) {
			t.Errorf("error message leaks secret material: %q", msg)
		}
	}
}

func TestVerifierCustomClaimsMapping(t *testing.T) {
	v := &Verifier{
		Keys:   []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}},
		Claims: &ClaimsMapping{UserID: "user_id", Tenant: "tid", Scopes: "scp"},
	}
	now := time.Now()
	claims := map[string]any{
		"user_id": "u-9", "tid": "tenant-Z", "sub": "subject-only",
		"scp": []any{"read", "write"},
		"iat": now.Unix(), "exp": now.Add(time.Hour).Unix(),
	}
	id, err := v.Authenticate(context.Background(), signHS256(t, testSecret, map[string]any{"alg": "HS256"}, claims))
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if id.UserID != "u-9" {
		t.Errorf("UserID = %q, want u-9 (mapped from user_id)", id.UserID)
	}
	if id.TenantID != "tenant-Z" {
		t.Errorf("TenantID = %q, want tenant-Z (mapped from tid)", id.TenantID)
	}
	if id.Subject != "subject-only" {
		t.Errorf("Subject = %q, want subject-only", id.Subject)
	}
	if !id.HasScope("write") || id.HasScope("delete") {
		t.Errorf("Scopes = %v, want [read write]", id.Scopes)
	}
}

// TestVerifierFallsBackToSubject 证明未配置专用字段时 sub 就是用户标识，
// 让最常见的 IdP 形态开箱可用。
func TestVerifierFallsBackToSubject(t *testing.T) {
	v := &Verifier{Keys: []VerificationKey{{Algorithm: AlgHS256, Material: testSecret}}}
	now := time.Now()
	claims := map[string]any{"sub": "only-sub", "iat": now.Unix(), "exp": now.Add(time.Hour).Unix()}
	id, err := v.Authenticate(context.Background(), signHS256(t, testSecret, map[string]any{"alg": "HS256"}, claims))
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if id.UserID != "only-sub" {
		t.Errorf("UserID = %q, want only-sub", id.UserID)
	}
	if id.TenantID != "" {
		t.Errorf("TenantID = %q, want empty when claim absent", id.TenantID)
	}
}

func TestStaticTokenAuthenticator(t *testing.T) {
	a := &StaticTokenAuthenticator{Identities: map[string]contracts.Identity{
		"tok-1": {UserID: "u-1", TenantID: "t-1"},
		"tok-2": {UserID: ""}, // 无主体的映射项必须视为无效，不能放行
	}}

	id, err := a.Authenticate(context.Background(), "tok-1")
	if err != nil {
		t.Fatalf("authenticate: %v", err)
	}
	if id.UserID != "u-1" || id.Method != "static-token" {
		t.Errorf("unexpected identity: %+v", id)
	}
	// "token 不存在"与"token 无效"必须报同一个错，否则会泄露哪些 token 曾经有效。
	_, errUnknown := a.Authenticate(context.Background(), "tok-unknown")
	_, errEmptySub := a.Authenticate(context.Background(), "tok-2")
	if !errors.Is(errUnknown, contracts.ErrInvalidSignature) || !errors.Is(errEmptySub, contracts.ErrInvalidSignature) {
		t.Fatalf("expected both to report ErrInvalidSignature, got %v / %v", errUnknown, errEmptySub)
	}
	if _, err := a.Authenticate(context.Background(), ""); !errors.Is(err, contracts.ErrMissingToken) {
		t.Fatalf("expected ErrMissingToken for empty token, got %v", err)
	}
}

func TestStripBearerScheme(t *testing.T) {
	cases := []struct{ in, want string }{
		{"Bearer abc.def.ghi", "abc.def.ghi"},
		{"bearer abc", "abc"},
		{"BeArEr abc", "abc"},
		{"abc", "abc"},
		{"  Bearer   abc  ", "abc"},
		{"", ""},
		// 前缀相似但不匹配时不得截断，否则会把非法值裁成看似有效的值。
		{"BearerX abc", "BearerX abc"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			if got := contracts.StripBearerScheme(tc.in); got != tc.want {
				t.Errorf("StripBearerScheme(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

// TestAuthTokenContextRoundTrip 证明凭证走 ctx 而非 ExecutionContext：
// 取不到凭证时应当是"认证失败"，而不是"静默按匿名放行"。
func TestAuthTokenContextRoundTrip(t *testing.T) {
	ctx := contracts.WithAuthToken(context.Background(), "tok-xyz")
	got, ok := contracts.AuthTokenFrom(ctx)
	if !ok || got != "tok-xyz" {
		t.Fatalf("round trip failed: %q, %v", got, ok)
	}
	if _, ok := contracts.AuthTokenFrom(context.Background()); ok {
		t.Error("expected no token in bare context")
	}
	// 空串注入后应当报告"没有凭证"，而不是"有一个空凭证"。
	if _, ok := contracts.AuthTokenFrom(contracts.WithAuthToken(context.Background(), "")); ok {
		t.Error("expected empty token to report absent")
	}
}

// AllowAll 必须在代码里一眼看出"这里完全不做认证"，因此名字刻意刺眼。
func TestAllowAllIsExplicitlyInsecure(t *testing.T) {
	a := AllowAll("dev-user", "dev-tenant")
	id, err := a.Authenticate(context.Background(), "")
	if err != nil {
		t.Fatalf("AllowAll should never fail: %v", err)
	}
	if id.Method != "allow-all-insecure" {
		t.Errorf("Method = %q, want allow-all-insecure so audit trails stay honest", id.Method)
	}
}
