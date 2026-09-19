// Package credential 提供 CredentialProvider port 的具体适配器实现。
//
// 这一层的职责是：把"密钥从哪来、多久换一次、怎么并发安全地取"这些托管细节
// 全部关在适配器内部，让 Runtime / Agent / 模型客户端只拿到一个能用的凭证句柄。
// 更换托管设施（环境变量 → 文件挂载 → Vault → 云 KMS）只改这一层，上层零改动。
package credential

import (
	"agent-runtime/internal/contracts"
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"
)

// ErrNotFound 表示托管设施中不存在该用途的凭证。
//
// 单独定义哨兵错误是为了让调用方能区分"没配密钥"（配置问题，应降级或拒绝启动）
// 与"取密钥失败"（网络/权限问题，可重试）。前者不该重试，重试只会拖慢失败。
var ErrNotFound = errors.New("credential: not found")

// EnvCredentialDir 是文件托管模式的目录环境变量。
// 设置后，各用途凭证按约定文件名从该目录读取
// （chat.key / embedding.key / vector-db.key / redaction.salt）。
//
// 对应生产中最常见的托管形态：Vault Agent、云 KMS 客户端或 sidecar 把短时效凭证
// 落到约定路径并周期性覆写，业务进程只管读文件。收益是密钥不进环境变量、
// 不落配置中心明文，轮转由外部设施驱动且**进程无需重启**。
const EnvCredentialDir = "CREDENTIAL_FILE_DIR"

// FromEnv 按部署形态选择凭证托管实现，是各进程（worker / runtime / indexer）
// 共用的唯一装配入口。
//
// 优先级：文件托管（可轮转）> 环境变量（兼容默认）。
//
// **刻意不包 CachingProvider**，这是一个必须记录在案的取舍：
// FileProvider 与 EnvProvider 都不返回 ExpiresAt，而 CachingProvider.stale 对
// 无失效时刻的凭证一律判定为"长期有效、不刷新"。一旦包上缓存，
// 外部设施轮转了磁盘上的密钥，进程内仍会无限期复用旧值 ——
// 恰好摧毁了文件托管"轮转不重启"这一核心价值，且故障表现为
// 轮转后一段时间集体 401，极难归因。
//
// 缓存层留给会自带 ExpiresAt 的短时效实现（Vault / KMS 直连适配器），
// 那时它有明确失效时刻，提前刷新与单飞回源才有意义。
// 而文件读取本身是廉价的（微秒级本地 IO），不缓存没有性能代价。
func FromEnv() contracts.CredentialProvider {
	if dir := strings.TrimSpace(os.Getenv(EnvCredentialDir)); dir != "" {
		return &FileProvider{Paths: pathsForDir(dir)}
	}
	return &EnvProvider{}
}

// pathsForDir 把目录展开为各用途的凭证文件路径，与 defaultFilePaths 保持同名约定。
func pathsForDir(dir string) map[contracts.CredentialPurpose]string {
	join := func(name string) string {
		return strings.TrimSuffix(dir, "/") + "/" + name
	}
	return map[contracts.CredentialPurpose]string{
		contracts.CredentialPurposeChat:      join("chat.key"),
		contracts.CredentialPurposeEmbedding: join("embedding.key"),
		contracts.CredentialPurposeVectorDB:  join("vector-db.key"),
		contracts.CredentialPurposeRedaction: join("redaction.salt"),
	}
}

// Value 取指定用途的凭证明文，取不到时返回空串而非错误。
//
// 供**无法接受 port 的第三方客户端**使用（如向量库 SDK 只收 string 型 apiKey）。
// 这是托管层的降级出口：明文短暂出现在局部变量里，但不再被存进结构体长期持有。
// 新增代码应优先走 port，只有对接不可改造的外部 SDK 时才用本函数。
func Value(ctx context.Context, p contracts.CredentialProvider, purpose contracts.CredentialPurpose) string {
	if p == nil {
		return ""
	}
	cred, err := p.Credential(ctx, purpose)
	if err != nil {
		return ""
	}
	return cred.Value
}

// EnvProvider 从进程环境变量读取凭证。
//
// 这是**兼容性默认实现**，不是托管方案：密钥以明文存在于进程环境变量中，
// 无轮转、无失效、无审计。它的价值在于让调用方代码统一走 port，
// 这样后续换成 FileProvider 或 Vault 适配器时上层一行都不用改。
//
// 刻意不缓存：环境变量在进程生命周期内基本不变，每次读取的成本远低于缓存的一致性负担。
type EnvProvider struct {
	// Vars 覆盖用途到环境变量名的映射；为 nil 时使用 defaultEnvVars。
	Vars map[contracts.CredentialPurpose][]string
}

// defaultEnvVars 是用途到环境变量名的默认映射。
// 每个用途给多个候选名，按顺序取第一个非空值：
// 专用变量优先（便于按用途最小授权），通用变量兜底（保持既有部署零改动）。
var defaultEnvVars = map[contracts.CredentialPurpose][]string{
	contracts.CredentialPurposeChat:      {"CHAT_API_KEY", "OPENAI_API_KEY"},
	contracts.CredentialPurposeEmbedding: {"EMBEDDING_API_KEY", "OPENAI_API_KEY"},
	contracts.CredentialPurposeVectorDB:  {"QDRANT_API_KEY"},
	contracts.CredentialPurposeRedaction: {"REDACTION_SALT"},
}

var (
	_ contracts.CredentialProvider = (*EnvProvider)(nil)
	_ contracts.CredentialApplier  = (*EnvProvider)(nil)
)

// Credential 按用途查找第一个非空的环境变量并返回。
// 全部缺失时返回 ErrNotFound（而非空字符串凭证），让调用方显式处理"未配置"。
func (p *EnvProvider) Credential(_ context.Context, purpose contracts.CredentialPurpose) (contracts.Credential, error) {
	for _, name := range p.names(purpose) {
		if v := os.Getenv(name); v != "" {
			return contracts.Credential{Value: v}, nil
		}
	}
	return contracts.Credential{}, fmt.Errorf("%w: purpose %q has no env var set", ErrNotFound, purpose)
}

// Apply 直接把 Authorization 头写入回调，调用方不接触明文。
func (p *EnvProvider) Apply(ctx context.Context, setHeader func(key, value string), purpose contracts.CredentialPurpose) error {
	return applyBearer(ctx, p, setHeader, purpose)
}

func (p *EnvProvider) names(purpose contracts.CredentialPurpose) []string {
	if p.Vars != nil {
		if names, ok := p.Vars[purpose]; ok && len(names) > 0 {
			return names
		}
	}
	return defaultEnvVars[purpose]
}

// FileProvider 从文件读取凭证，支持外部轮转后进程内自动生效。
//
// 对应生产中最常见的托管形态：Vault Agent / 云 KMS 客户端 / Sidecar 把短时效凭证
// 落到约定路径并周期性覆写，业务进程只管读文件。这样密钥不进环境变量、
// 不落配置中心明文，轮转由外部设施驱动，业务侧无需重启——即"透明替换"。
//
// 安全边界（必须诚实标注）：本实现读取的是**磁盘上的明文文件**，
// 文件权限与目录可见性由部署方保证（建议 0400 + 专用用户）。
// 它解决的是"轮转不重启"和"密钥不出现在进程环境变量/代码仓库"，
// 不解决"磁盘上无明文"——后者需要 KMS 信封加密，本层未实现。
type FileProvider struct {
	// Paths 覆盖用途到文件路径的映射；为 nil 时使用 defaultFilePaths。
	Paths map[contracts.CredentialPurpose]string
	// ReadFile 便于测试注入内存文件系统；为 nil 时使用 os.ReadFile。
	ReadFile func(name string) ([]byte, error)
}

var defaultFilePaths = map[contracts.CredentialPurpose]string{
	contracts.CredentialPurposeChat:      "/etc/agent-runtime/credentials/chat.key",
	contracts.CredentialPurposeEmbedding: "/etc/agent-runtime/credentials/embedding.key",
	contracts.CredentialPurposeVectorDB:  "/etc/agent-runtime/credentials/vector-db.key",
	contracts.CredentialPurposeRedaction: "/etc/agent-runtime/credentials/redaction.salt",
}

var _ contracts.CredentialProvider = (*FileProvider)(nil)

// Credential 读取对应文件内容作为凭证。
// 每次调用都真实读盘，以便外部轮转写入后立即被下一次取用感知到；
// 若需摊薄 IO，应在外层包一个 CachingProvider，而不是在这里做缓存——
// 缓存与轮转感知是互相拉扯的两个目标，分开实现才能各自调整。
func (p *FileProvider) Credential(_ context.Context, purpose contracts.CredentialPurpose) (contracts.Credential, error) {
	path := p.path(purpose)
	if path == "" {
		return contracts.Credential{}, fmt.Errorf("%w: purpose %q has no file path configured", ErrNotFound, purpose)
	}
	read := p.ReadFile
	if read == nil {
		read = os.ReadFile
	}
	raw, err := read(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return contracts.Credential{}, fmt.Errorf("%w: purpose %q path %s does not exist", ErrNotFound, purpose, path)
		}
		return contracts.Credential{}, fmt.Errorf("credential: read %s: %w", path, err)
	}
	// 去掉尾部换行与首尾空白：凭证文件常被 echo / 编辑器写入时带上 \n，
	// 不去掉会让 Authorization 头尾部多一个字符导致 401，且这种错误极难排查。
	value := strings.TrimSpace(string(raw))
	if value == "" {
		return contracts.Credential{}, fmt.Errorf("%w: purpose %q path %s is empty", ErrNotFound, purpose, path)
	}
	return contracts.Credential{Value: value, Scheme: bearerFromPath(path)}, nil
}

func (p *FileProvider) path(purpose contracts.CredentialPurpose) string {
	if p.Paths != nil {
		if v, ok := p.Paths[purpose]; ok && v != "" {
			return v
		}
	}
	return defaultFilePaths[purpose]
}

// bearerFromPath 允许在文件名上以 .basic 后缀声明非 Bearer 方案，
// 便于对接只接受 Basic 认证的自建网关，而不必为此新增配置项。
func bearerFromPath(path string) string {
	if strings.HasSuffix(strings.ToLower(path), ".basic") {
		return "Basic"
	}
	return "Bearer"
}

// StaticProvider 返回固定凭证，用于测试与本地演示。
//
// 不要在生产中使用：它把密钥写进了进程内存与（通常）代码或配置文件，
// 既无轮转也无审计，等同于当前 os.Getenv 直读的处境。
type StaticProvider struct {
	Value   string
	Scheme  string
	TTL     time.Duration // >0 时为每次取用生成 ExpiresAt = now+TTL 的凭证，用于测试缓存过期路径
	Expires time.Time     // 非零时直接作为固定失效时刻（测试用）
}

var _ contracts.CredentialProvider = (*StaticProvider)(nil)

func (p *StaticProvider) Credential(context.Context, contracts.CredentialPurpose) (contracts.Credential, error) {
	if p.Value == "" {
		return contracts.Credential{}, fmt.Errorf("%w: static provider has empty value", ErrNotFound)
	}
	c := contracts.Credential{Value: p.Value, Scheme: p.Scheme}
	switch {
	case !p.Expires.IsZero():
		c.ExpiresAt = p.Expires
	case p.TTL > 0:
		c.ExpiresAt = time.Now().Add(p.TTL)
	}
	return c, nil
}

// CachingProvider 包装任意 CredentialProvider，在失效前复用同一份凭证。
//
// 回源取凭证（Vault / KMS 网络调用）通常比一次模型推理慢，且托管设施有 QPS 限制；
// 每次请求都回源既慢又可能触发限流。本层做三件事：
//
//  1. **提前刷新**：在 ExpiresAt 之前留出 RefreshMargin 就视为过期，
//     避免"凭证恰好在请求途中失效"这一最难排查的偶发 401。
//  2. **单飞（single-flight）**：并发请求同一用途时只放一个协程回源，
//     其余等待复用结果。轮转瞬间最容易出现惊群——上百个 worker 同时发现过期、
//     同时打向 KMS，把托管设施打挂，然后全体失败。
//  3. **失败降级到旧值**：回源出错但仍有未过期旧凭证时，返回旧值而不是直接失败。
//     托管设施短暂抖动不该让整个 Runtime 停摆。
type CachingProvider struct {
	// Inner 为被包装的回源实现，必填。
	Inner contracts.CredentialProvider
	// RefreshMargin 为提前刷新余量，<=0 时取 DefaultRefreshMargin。
	RefreshMargin time.Duration
	// MaxStale 为回源失败后仍可继续使用旧凭证的最长时间；<=0 表示不允许陈旧兜底。
	MaxStale time.Duration
	// Now 便于测试注入时钟；为 nil 时使用 time.Now。
	Now func() time.Time

	mu     sync.Mutex
	cache  map[contracts.CredentialPurpose]cacheEntry
	flight map[contracts.CredentialPurpose]*flightCall
}

type cacheEntry struct {
	cred      contracts.Credential
	fetchedAt time.Time
}

// flightCall 表示一次进行中的回源，done 在结果写入后关闭。
type flightCall struct {
	done chan struct{}
	cred contracts.Credential
	err  error
}

// DefaultRefreshMargin 是默认的提前刷新余量。
// 取 60s 的依据：一次 LLM 请求的常见超时是 60s，留出等量的余量才能保证
// "取到的凭证足以撑完本次请求"，否则会在长请求中途失效。
const DefaultRefreshMargin = 60 * time.Second

var _ contracts.CredentialProvider = (*CachingProvider)(nil)

func (p *CachingProvider) now() time.Time {
	if p.Now != nil {
		return p.Now()
	}
	return time.Now()
}

func (p *CachingProvider) margin() time.Duration {
	if p.RefreshMargin > 0 {
		return p.RefreshMargin
	}
	return DefaultRefreshMargin
}

// Credential 返回未过期的缓存凭证，必要时单飞回源刷新。
func (p *CachingProvider) Credential(ctx context.Context, purpose contracts.CredentialPurpose) (contracts.Credential, error) {
	if p.Inner == nil {
		return contracts.Credential{}, errors.New("credential: caching provider has no inner provider")
	}
	now := p.now()

	p.mu.Lock()
	if e, ok := p.cache[purpose]; ok && !p.stale(e.cred, now) {
		p.mu.Unlock()
		return e.cred, nil
	}
	// 已过期或即将过期：尝试加入进行中的回源，否则自己发起。
	if f, ok := p.flight[purpose]; ok {
		p.mu.Unlock()
		select {
		case <-f.done:
		case <-ctx.Done():
			return contracts.Credential{}, ctx.Err()
		}
		if f.err != nil {
			// 领头的回源失败了。此处不直接返回错误，而是走下面的陈旧兜底判断：
			// 并发等待者可能手里还有可用旧值，让它们复用比全体失败更合理。
			if c, ok := p.staleFallback(purpose, now); ok {
				return c, nil
			}
			return contracts.Credential{}, f.err
		}
		return f.cred, nil
	}
	f := &flightCall{done: make(chan struct{})}
	if p.flight == nil {
		p.flight = make(map[contracts.CredentialPurpose]*flightCall)
	}
	p.flight[purpose] = f
	p.mu.Unlock()

	cred, err := p.Inner.Credential(ctx, purpose)

	p.mu.Lock()
	delete(p.flight, purpose)
	if err == nil {
		if p.cache == nil {
			p.cache = make(map[contracts.CredentialPurpose]cacheEntry)
		}
		p.cache[purpose] = cacheEntry{cred: cred, fetchedAt: now}
	}
	p.mu.Unlock()

	f.cred, f.err = cred, err
	close(f.done)

	if err != nil {
		if c, ok := p.staleFallback(purpose, now); ok {
			return c, nil
		}
		return contracts.Credential{}, err
	}
	return cred, nil
}

// stale 报告凭证是否需要刷新：显式失效时刻已到（含提前余量），
// 或凭证本身无失效时刻但已超过缓存条目的可复用窗口。
func (p *CachingProvider) stale(c contracts.Credential, now time.Time) bool {
	if !c.ExpiresAt.IsZero() {
		return !now.Add(p.margin()).Before(c.ExpiresAt)
	}
	// 回源没给失效时刻（如 EnvProvider / FileProvider）：视为长期有效，不主动刷新。
	// 这类实现本身读取就是廉价的，且外部轮转的感知由调用方决定是否包缓存。
	return false
}

// staleFallback 在回源失败时尝试返回仍在 MaxStale 窗口内的旧凭证。
func (p *CachingProvider) staleFallback(purpose contracts.CredentialPurpose, now time.Time) (contracts.Credential, bool) {
	if p.MaxStale <= 0 {
		return contracts.Credential{}, false
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	e, ok := p.cache[purpose]
	if !ok {
		return contracts.Credential{}, false
	}
	if now.Sub(e.fetchedAt) > p.MaxStale {
		return contracts.Credential{}, false
	}
	return e.cred, true
}

// Invalidate 主动丢弃指定用途的缓存凭证（purpose 为空时清空全部）。
//
// 用于收到 401 后的强制刷新：调用方发现凭证被服务端拒绝时，
// 与其等缓存自然过期，不如立刻失效并回源，把故障恢复时间从分钟级降到一次调用。
func (p *CachingProvider) Invalidate(purpose contracts.CredentialPurpose) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if purpose == "" {
		p.cache = nil
		return
	}
	delete(p.cache, purpose)
}

// applyBearer 取凭证并构造鉴权头，是各 Applier 的公共实现。
//
// 明文的可见范围被限制在本函数内：调用方只提供一个 setHeader 回调，
// 拿到的是已经拼好的头值，不需要自己持有 Credential。
func applyBearer(ctx context.Context, p contracts.CredentialProvider, setHeader func(key, value string), purpose contracts.CredentialPurpose) error {
	if setHeader == nil {
		return errors.New("credential: setHeader callback is nil")
	}
	cred, err := p.Credential(ctx, purpose)
	if err != nil {
		return err
	}
	if cred.Value == "" {
		return fmt.Errorf("%w: purpose %q returned empty credential", ErrNotFound, purpose)
	}
	scheme := cred.Scheme
	if scheme == "" {
		scheme = "Bearer"
	}
	setHeader("Authorization", scheme+" "+cred.Value)
	return nil
}

// BearerApplier 把任意 CredentialProvider 适配成 CredentialApplier。
//
// 存在意义：EnvProvider / FileProvider / CachingProvider 都只实现了基础 port，
// 但模型客户端希望走"不接触明文"的 Apply 路径。用它包一层即可获得该能力，
// 无需给每个 Provider 都重复实现 Apply。
type BearerApplier struct {
	Provider contracts.CredentialProvider
	Purpose  contracts.CredentialPurpose
}

var _ contracts.CredentialApplier = (*BearerApplier)(nil)

func (a *BearerApplier) Apply(ctx context.Context, setHeader func(key, value string), _ contracts.CredentialPurpose) error {
	if a.Provider == nil {
		return errors.New("credential: bearer applier has no provider")
	}
	return applyBearer(ctx, a.Provider, setHeader, a.Purpose)
}

// HeaderFor 取凭证并返回 (key, value) 头对，供无法接受回调的调用方使用。
//
// 这是 Apply 的降级形态：明文会出现在返回值中，可见范围比回调大。
// 仅在实现方无法持有回调（如需要把头存进结构体）时使用。
func HeaderFor(ctx context.Context, p contracts.CredentialProvider, purpose contracts.CredentialPurpose) (string, string, error) {
	var k, v string
	if err := applyBearer(ctx, p, func(key, value string) { k, v = key, value }, purpose); err != nil {
		return "", "", err
	}
	return k, v, nil
}
