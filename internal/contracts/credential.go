package contracts

import (
	"context"
	"time"
)

// CredentialPurpose 标识凭证的使用场景。
//
// 为什么要区分用途：托管层可以为不同场景签发不同权限范围、不同时效的凭证。
// 例如向量化只需要 embedding 额度，不该拿到与推理同一把高权限 Key；
// 向量库的写入凭证又与网关凭证完全无关。用途维度是"最小权限"落地的前提。
type CredentialPurpose string

const (
	CredentialPurposeChat      CredentialPurpose = "chat"      // 对话补全网关
	CredentialPurposeEmbedding CredentialPurpose = "embedding" // 向量化网关
	CredentialPurposeVectorDB  CredentialPurpose = "vector_db" // 向量库读写
	CredentialPurposeRedaction CredentialPurpose = "redaction" // 脱敏伪标识派生盐值
)

// 关于 CredentialPurposeRedaction 的两个必须说清的边界：
//
// 1. **它不是鉴权凭证**，不会出现在任何请求头里，只用于派生脱敏伪标识
//    sha256(salt|value)。放进 CredentialProvider 而非另建机制的理由是：
//    它同样是"必须保密、不该硬编码、不该散落在环境变量里"的密钥材料，
//    复用托管层即可继承同一套文件挂载与轮转设施，避免多一个平行配置通道。
//
// 2. **它的轮转语义与其他用途相反**。网关密钥轮转是为了缩短暴露窗口，越勤越好；
//    而盐值一旦轮转，同一个手机号在轮转前后会派生出不同伪标识，
//    跨请求的关联能力（同一实体在多条日志里可对上）就此断裂。
//    因此盐值应当长期稳定，只在疑似泄露时才轮转，并接受关联断链的代价。

// Credential 是一次外部调用所需的短时效凭证句柄。
//
// 刻意不带 KeyID / 明文之外的元信息字段：调用方只需要能构造出鉴权头，
// 凭证的生命周期管理（获取、缓存、轮转、吊销）全部收敛在 Provider 实现里。
type Credential struct {
	Value string // 凭证明文，仅在构造鉴权头的瞬间使用，调用方不应长期持有或落日志
	// Scheme 为鉴权方案，空值按 "Bearer" 处理。
	// 兼容非 Bearer 网关（如 Basic、自定义 X-Api-Key 场景由 Applier 承担）。
	Scheme string
	// ExpiresAt 为该凭证的失效时刻；零值表示不过期（如长期静态 Key）。
	ExpiresAt time.Time
}

// Expired 报告凭证在 now 时刻是否已失效。
// 缓存型 Provider 用它决定是否需要回源取新凭证。
func (c Credential) Expired(now time.Time) bool {
	return !c.ExpiresAt.IsZero() && !now.Before(c.ExpiresAt)
}

// CredentialProvider 是密钥托管的稳定扩展点（port），与 ModelProvider / ToolProvider 平级。
//
// 设计意图是让 Runtime 与 Agent 代码看不到明文密钥：适配器负责对接 KMS / Vault /
// 文件挂载等托管设施并返回短时效凭证，轮转因此变成适配器内部的事，上层无感。
//
// 契约要求：实现必须是并发安全的（同一 Provider 会被多个 worker 协程共享），
// 且应尊重 ctx 的超时与取消——回源取凭证可能是一次网络调用。
type CredentialProvider interface {
	Credential(ctx context.Context, purpose CredentialPurpose) (Credential, error)
}

// CredentialApplier 是**可选**的零暴露扩展：实现方直接把鉴权信息写入请求头，
// 明文不返回给调用方。
//
// 为什么单独拆一个可选接口而不是把它塞进 CredentialProvider：
// 后者已有多个实现与测试替身，加方法会强制所有实现都具备写头能力，破坏面过大。
// 这与 MemorySearcher 相对于 MemoryProvider 的处理方式一致——
// 用类型断言探测可选能力，以保持基础 port 的稳定。
//
// 需要"密钥明文绝不出现在调用方内存"的部署（如 Vault 动态凭证）应实现本接口。
type CredentialApplier interface {
	// Apply 通过 setHeader 回调写入鉴权头。回调形式而非传入 http.Header，
	// 是为了让本接口不依赖 net/http，保持 contracts 包零外部依赖。
	Apply(ctx context.Context, setHeader func(key, value string), purpose CredentialPurpose) error
}
