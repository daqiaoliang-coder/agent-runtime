package worker

import (
	"agent-runtime/internal/adapters/credential"
	"agent-runtime/internal/contracts"
	"context"
	"log"
)

// 本文件把"密钥从哪来"收敛到一处，使 worker / 记忆检索 / 向量库共用同一个托管 port。
//
// 改造前的事实是：多处各自 os.Getenv("OPENAI_API_KEY") 直读，密钥以明文散落在
// 进程环境变量里，没有轮转、没有失效处理。改造后所有取用都经 CredentialProvider，
// 明文只在构造鉴权头的瞬间存在（见 credential.applyBearer 的 setHeader 回调），
// 上层代码不再持有密钥字符串 —— 这就是"透明替换"的落点：
// 换托管设施只改 adapter 层，Runtime / Agent / 模型客户端零改动。
//
// 装配实现本体在 credential.FromEnv（cmd/runtime 也用它），此处只保留 worker
// 特有的降级策略：取不到就退化，绝不 Fatal。

// newCredentialsFromEnv 返回 worker 使用的凭证托管实现。
//
// 支持两种形态，由 CREDENTIAL_FILE_DIR 决定（详见 credential.FromEnv 的注释）：
// 文件托管（Vault/KMS sidecar 周期性覆写，轮转不重启）优先，环境变量兜底。
func newCredentialsFromEnv() contracts.CredentialProvider {
	return credential.FromEnv()
}

// redactionSaltFrom 从托管层取脱敏伪标识的盐值。
//
// 取不到时返回空串，此时 Redactor 会降级为纯掩码而不生成伪标识
// （见 Policy.replacement 的 fail-safe）：敏感值仍然被遮住，只失去跨请求关联能力。
//
// 为什么盐值要走托管层而不是新加一个环境变量：它同样是密钥材料，
// 泄露后攻击者可对候选值穷举比对，使伪标识退化为可逆。复用同一套托管设施，
// 就继承了文件挂载与轮转，不必维护一个平行的配置通道。
//
// 刻意不在此处 log.Fatal —— 盐值缺失不该让整个 worker 起不来，降级运行优于停机。
func redactionSaltFrom(creds contracts.CredentialProvider) string {
	salt := credential.Value(context.Background(), creds, contracts.CredentialPurposeRedaction)
	if salt == "" {
		log.Println("worker: no redaction salt configured; tokenized redaction degrades to mask-only")
	}
	return salt
}
