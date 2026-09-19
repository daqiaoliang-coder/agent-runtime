package llm

import (
	"agent-runtime/internal/contracts"
	"context"
	"errors"
	"fmt"
)

// credentialInvalidator 是 CachingProvider 暴露的可选失效能力。
//
// 用局部接口 + 类型断言而不是给 CredentialProvider 加方法，是为了保持 port 稳定：
// 静态密钥实现没有缓存可失效，强制它实现 Invalidate 只是噪音。
type credentialInvalidator interface {
	Invalidate(purpose contracts.CredentialPurpose)
}

// resolveAuthorization 统一 llm 包内各客户端的密钥来源，返回可直接写入
// Authorization 头的值。
//
// 优先级：托管层（creds）> 明文（staticKey）。两条路径都不可用时返回明确错误，
// 让调用方在发起请求前失败——匿名请求换来的是网关 401，会把排查方向
// 从"密钥没配"误导到"密钥无效"。
//
// purpose 为空时回退到 fallbackPurpose，便于调用方省略该字段。
func resolveAuthorization(ctx context.Context, creds contracts.CredentialProvider, purpose, fallbackPurpose contracts.CredentialPurpose, staticKey string) (string, error) {
	if purpose == "" {
		purpose = fallbackPurpose
	}
	if creds != nil {
		cred, err := creds.Credential(ctx, purpose)
		if err != nil {
			return "", fmt.Errorf("llm: resolve credential for %q: %w", purpose, err)
		}
		if cred.Value == "" {
			return "", fmt.Errorf("llm: credential for purpose %q is empty", purpose)
		}
		scheme := cred.Scheme
		if scheme == "" {
			scheme = "Bearer"
		}
		return scheme + " " + cred.Value, nil
	}
	if staticKey == "" {
		return "", errors.New("llm: openai api key not configured")
	}
	return "Bearer " + staticKey, nil
}

// invalidateCredential 在凭证被服务端拒绝后主动失效托管层缓存。
//
// 尽力而为：provider 为 nil 或不支持失效时静默跳过，不能因为清理缓存失败
// 而把一个本来只是鉴权错误的响应升级成另一种错误。
func invalidateCredential(creds contracts.CredentialProvider, purpose contracts.CredentialPurpose) {
	if inv, ok := creds.(credentialInvalidator); ok {
		inv.Invalidate(purpose)
	}
}
