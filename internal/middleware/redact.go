package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
)

// SensitivityLevel 是字段的敏感度分级，四级从公开到机密。
//
// 分级而非布尔开关的原因：脱敏策略必须能按业务场景差异化。同一段文本，
// 在内部审计事件里可以保留手机号后四位便于对账，在面向前端的 SSE 里必须全遮。
// 有了序数级别，策略只需声明"处理 >= 某级"，而不必为每个场景重写规则集。
//
// 这套分级骨架与上下文治理里的"字段按重要性四级分类"（必须完整/可摘要/只传链接/可丢弃）
// 是同一个工程模式：字段级元数据声明 → 执行面统一裁剪点 → 有上界保证 → 降级为引用。
// 差别只在判定维度是敏感度而不是 token 预算。
type SensitivityLevel int

const (
	SensitivityPublic    SensitivityLevel = 0 // 公开信息，不处理
	SensitivityInternal  SensitivityLevel = 1 // 内部信息，仅出域时处理
	SensitivitySensitive SensitivityLevel = 2 // 敏感个人信息（手机号/邮箱/身份证）
	SensitivitySecret    SensitivityLevel = 3 // 机密（密钥/令牌/私钥/银行卡）
)

func (l SensitivityLevel) String() string {
	switch l {
	case SensitivityPublic:
		return "public"
	case SensitivityInternal:
		return "internal"
	case SensitivitySensitive:
		return "sensitive"
	case SensitivitySecret:
		return "secret"
	}
	return fmt.Sprintf("level-%d", int(l))
}

// RedactAction 决定命中敏感模式后如何处置原文。
//
// 命名为 RedactAction 而非 Action：同包的 Guard 已有 Action 表达处置决策
// （放行/转人工/拒绝），两者语义不同，共用一个名字会互相遮蔽。
type RedactAction int

const (
	// RedactMask 保留首尾若干字符，中间以固定长度的掩码替换。
	// 用于需要人工肉眼核对同一性的场景（如手机号后四位对账）。
	RedactMask RedactAction = iota
	// RedactTokenize 用确定性伪标识替换整个命中值。
	// 同一原值在任意位置、任意事件中恒得到同一标识，因此下游仍能做实体关联与去重，
	// 但无法从标识反推原值。这是"既不可读又可用"的折中，也是凭证类命中的默认动作。
	RedactTokenize
	// RedactDrop 整体删除命中值，不保留任何痕迹。
	// 用于机密级别：连"这里曾经有一个密钥"都不该暴露给日志检索方。
	RedactDrop
)

// Rule 是一条敏感模式规则：匹配什么、多敏感、怎么处置。
type Rule struct {
	// Name 为规则标识，会出现在占位符中以便排查"是谁遮的"。
	// 必须只用 [a-z0-9_]，因为它会被拼进占位符，含特殊字符会破坏 JSON 与幂等性。
	Name string
	// Pattern 为已编译的匹配式。**第一个捕获组**界定被替换的实际值；
	// 无捕获组时替换整个匹配。用捕获组是为了把前缀（如 "Bearer "）留在组外，
	// 使输出仍保留结构可读性。
	Pattern *regexp.Regexp
	Level   SensitivityLevel
	Action  RedactAction
	// KeepPrefix / KeepSuffix 仅在 RedactMask 下生效，指定保留的首尾字符数（按 rune 计）。
	KeepPrefix int
	KeepSuffix int
}

// DefaultRules 是覆盖常见强模式的内置规则集。
//
// 只收确定性高、误报低的模式。刻意**不**收"姓名/地址"这类需要 NER 判定的软模式——
// 正则做软模式的误报率会把正常业务文本打得千疮百孔，那类需求应由外部 DLP 服务承担，
// 本层只负责有明确格式契约的强模式，以及字段元数据声明路径。
//
// 顺序有意义：更具体的规则（如带前缀的密钥）必须排在更宽泛的规则（如长数字串）之前，
// 否则先命中的宽规则会把具体规则该处理的值吃掉。
var DefaultRules = []Rule{
	// 机密级：凭证与私钥。这些是最不该出现在日志与事件里的内容。
	{
		Name: "private_key_block", Level: SensitivitySecret, Action: RedactDrop,
		// 覆盖 PEM 块的 BEGIN/END 边界与中间 base64 体；(?s) 让 . 匹配换行。
		Pattern: regexp.MustCompile(`(?s)-----BEGIN [A-Z ]*PRIVATE KEY-----.*?-----END [A-Z ]*PRIVATE KEY-----`),
	},
	{
		Name: "openai_key", Level: SensitivitySecret, Action: RedactTokenize,
		Pattern: regexp.MustCompile(`\b(sk-[A-Za-z0-9_\-]{16,})\b`),
	},
	{
		Name: "aws_access_key", Level: SensitivitySecret, Action: RedactTokenize,
		// AKIA/ASIA 开头的 20 位访问密钥 ID 是固定格式，误报极低。
		Pattern: regexp.MustCompile(`\b((?:AKIA|ASIA)[0-9A-Z]{16})\b`),
	},
	{
		Name: "jwt", Level: SensitivitySecret, Action: RedactTokenize,
		// 三段 base64url 且各段足够长才认定为 JWT，避免误伤普通的点号分隔标识符。
		Pattern: regexp.MustCompile(`\b(eyJ[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,}\.[A-Za-z0-9_\-]{8,})\b`),
	},
	{
		Name: "bearer_token", Level: SensitivitySecret, Action: RedactTokenize,
		// 捕获组只含令牌本体，"Bearer " 前缀留在组外以保持结构可读。
		Pattern: regexp.MustCompile(`(?i)\bBearer\s+([A-Za-z0-9._\-]{16,})\b`),
	},
	{
		Name: "basic_auth", Level: SensitivitySecret, Action: RedactTokenize,
		Pattern: regexp.MustCompile(`(?i)\bBasic\s+([A-Za-z0-9+/=]{16,})\b`),
	},

	// 敏感级：个人身份信息。
	{
		Name: "cn_id_card", Level: SensitivitySensitive, Action: RedactMask, KeepPrefix: 4,
		// 18 位大陆身份证：6 位地址码 + 8 位出生日期 + 3 位顺序 + 1 位校验。
		// 用出生日期段做结构约束，比裸匹配 18 位数字的误报率低得多。
		Pattern: regexp.MustCompile(`\b([1-9]\d{5}(?:19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[\dXx])\b`),
	},
	{
		Name: "email", Level: SensitivitySensitive, Action: RedactMask, KeepPrefix: 2,
		// 捕获组只含本地部分：域名通常是业务可见信息（企业邮箱后缀），
		// 遮掉会损失可用性而无安全收益，故留在组外原样保留。
		Pattern: regexp.MustCompile(`\b([A-Za-z0-9._%+\-]+)@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}\b`),
	},
	{
		Name: "cn_mobile", Level: SensitivitySensitive, Action: RedactMask, KeepPrefix: 3, KeepSuffix: 4,
		// 大陆手机号：1 开头、第二位 3-9、共 11 位。
		// 用 \b 而非环视断言：Go 的 RE2 不支持 (?<!) / (?!)，编译期即失败。
		// \b 是 word/non-word 边界，数字属 word 字符，故 \b 天然要求前后不紧邻数字，
		// 能把订单号等长数字串的中段排除掉；顺带也排除了字母紧邻（如 "ID13800138000"），
		// 这类紧邻数字字母的串本就不是手机号，排除属于收益而非漏报。
		Pattern: regexp.MustCompile(`\b(1[3-9][0-9]{9})\b`),
	},
	{
		Name: "bank_card", Level: SensitivitySensitive, Action: RedactMask, KeepSuffix: 4,
		// 13-19 位卡号，同样以 \b 压误报。
		// 注意 20 位以上的连续数字串整体不匹配：{12,18} 最长只能覆盖 19 位，
		// 余下数字使尾部 \b 不成立，RE2 不做回溯，因此整串被跳过——
		// 这是期望行为，超长数字串（订单号/流水号）不该被当作卡号切碎。
		Pattern: regexp.MustCompile(`\b([3-6][0-9]{12,18})\b`),
	},
}

// Policy 是一次脱敏的完整策略：用哪些规则、从哪一级开始处理、输出上界多少。
//
// 零值 Policy 不可直接使用（无规则）；用 NewPolicy 构造。
type Policy struct {
	Rules []Rule
	// MinLevel 为生效门槛：低于此级别的规则跳过。
	// 面向前端的链路设 SensitivityInternal（几乎全处理），内部审计链路可设
	// SensitivitySecret（只遮机密），从而在不换规则集的前提下按场景收紧或放宽。
	MinLevel SensitivityLevel
	// Salt 用于确定性伪标识的派生。必须按租户或部署维度隔离且保密：
	// 一旦泄露，攻击者可对候选值做穷举比对（把字典里的手机号逐个算标识再对照），
	// 使 RedactTokenize 退化为可逆。生产应从密钥托管层取，不可硬编码。
	Salt string
	// MaxOutputBytes 为脱敏后文本的硬上界，<=0 时取 DefaultMaxOutputBytes。
	MaxOutputBytes int
}

// DefaultMaxOutputBytes 是脱敏输出的默认硬上界（64 KiB）。
//
// 为什么必须有上界：占位符本身占字节。一段含上千个手机号的文本，
// 每个 11 字节被替换成约 30 字节的占位符后，输出会比输入膨胀近三倍。
// 下游的 token 预算与事件体积限制是按"脱敏不会显著变大"假设的，
// 无界膨胀会反过来撑爆预算——标记文本自己变成了新的问题。
// 因此超限必须降级，而不是任其增长。
const DefaultMaxOutputBytes = 64 * 1024

// NewPolicy 用给定规则与门槛构造策略。rules 为空时使用 DefaultRules。
func NewPolicy(minLevel SensitivityLevel, salt string, rules ...Rule) *Policy {
	if len(rules) == 0 {
		rules = DefaultRules
	}
	return &Policy{Rules: rules, MinLevel: minLevel, Salt: salt}
}

// Stats 是一次脱敏的观测数据，用于审计与告警。
//
// 刻意与返回值分离而不是塞进文本：脱敏是否发生、命中哪些规则，
// 本身就是需要进审计链路的事实，但绝不能污染业务输出。
type Stats struct {
	// Hits 为各规则命中次数，按规则名索引。
	Hits map[string]int
	// Total 为命中总次数。
	Total int
	// Truncated 为 true 表示输出触及 MaxOutputBytes 上界并已降级。
	Truncated bool
	// InputBytes / OutputBytes 便于观测膨胀比。
	InputBytes, OutputBytes int
}

func (s Stats) HitAny() bool { return s.Total > 0 }

// RuleNames 返回命中过的规则名（升序），用于日志与事件标注。
func (s Stats) RuleNames() []string {
	names := make([]string, 0, len(s.Hits))
	for name := range s.Hits {
		names = append(names, name)
	}
	// 插入排序即可：规则数量级只有十来个，不值得引入 sort 包。
	for i := 1; i < len(names); i++ {
		for j := i; j > 0 && names[j] < names[j-1]; j-- {
			names[j], names[j-1] = names[j-1], names[j]
		}
	}
	return names
}

// Redact 对文本执行脱敏，返回处理后的文本与命中统计。
//
// 幂等性：对已脱敏的文本再次调用不产生二次变化。这不是巧合而是设计约束——
// Tool.After 与 Event.Transform 可能都对同一段输出生效（工具结果先被脱敏，
// 再随事件出域被二次处理），若非幂等，占位符会被逐层套娃，最终输出全是噪音。
// 实现上依靠：占位符与掩码只含 '*'、'['、']'、':' 与十六进制字符，
// 不满足任何规则的结构约束（手机号要求 11 位纯数字、JWT 要求三段 base64url、
// Bearer 要求 16 位以上令牌字符集等），因此不会被二次命中。
func (p *Policy) Redact(text string) (string, Stats) {
	stats := Stats{InputBytes: len(text)}
	if p == nil || text == "" || len(p.Rules) == 0 {
		stats.OutputBytes = len(text)
		return text, stats
	}
	limit := p.MaxOutputBytes
	if limit <= 0 {
		limit = DefaultMaxOutputBytes
	}

	out := text
	for i := range p.Rules {
		r := &p.Rules[i]
		if r.Pattern == nil || r.Level < p.MinLevel {
			continue
		}
		// 已超出上界才停止替换：结果马上要被丢弃，继续替换纯属浪费。
		// 判据必须是**严格大于**——若写成 >=，输入长度恰好等于上限时会在此处
		// 直接跳出，导致该文本一条规则都不过、原样返回，形成静默泄露。
		if len(out) > limit {
			break
		}
		out = p.applyRule(out, r, &stats)
	}

	// 上界保证：超出硬上限时不做大段截断（截断会切碎占位符，破坏可解析性），
	// 而是整体降级为一个摘要占位符——即"不可传递时降级为引用而非内容"。
	// 原值已被丢弃，下游只拿到"这里有过敏感内容、被哪些规则命中"的引用。
	if len(out) > limit {
		stats.Truncated = true
		// 审计统计改为对**原文**做纯检测重算。
		//
		// 为什么不能沿用替换过程中累计的计数：降级模式下内容已被丢弃，
		// 唯一还有价值的事实是"原文里出现过哪些类别的敏感数据"。
		// 而替换循环可能在上界处提前 break（输入本身就超限时更是首次即 break），
		// 沿用旧计数会让审计漏掉未执行的规则，甚至出现 rules 为空——
		// 那时运维只能看到"有个大文本被降级了"，完全无法判断泄露的是什么类型的数据。
		stats.Hits = nil
		stats.Total = 0
		for i := range p.Rules {
			r := &p.Rules[i]
			if r.Pattern == nil || r.Level < p.MinLevel {
				continue
			}
			p.countRule(text, r, &stats)
		}
		out = fmt.Sprintf("[REDACTED:oversize:input=%d:limit=%d:rules=%s]",
			stats.InputBytes, limit, strings.Join(stats.RuleNames(), ","))
	}

	stats.OutputBytes = len(out)
	return out, stats
}

// countRule 只统计命中、不生成替换文本，用于降级路径的审计重算。
//
// 手动推进而非 FindAllStringIndex 一次取全：超大文本上后者会分配与命中数等长的
// 切片（万级命中即万级分配），而降级路径只需要计数。
//
// 口径说明：降级模式下统计的是"原文中该规则命中多少次"，与正常模式下
// "实际执行了多少次替换"略有差异（前者不受前序规则替换的影响）。
// 这是有意的——降级时替换并未发生，报"原文检出量"才是可解释的事实。
func (p *Policy) countRule(text string, r *Rule, stats *Stats) {
	if stats.Hits == nil {
		stats.Hits = make(map[string]int)
	}
	for pos := 0; pos < len(text); {
		m := r.Pattern.FindStringIndex(text[pos:])
		if m == nil {
			return
		}
		stats.Hits[r.Name]++
		stats.Total++
		// 零长匹配必须强制前进，否则死循环。内置规则均要求至少一个字符，
		// 此分支只防自定义规则写出 (?i)a* 这类可空匹配。
		advance := m[1]
		if advance <= 0 {
			advance = 1
		}
		pos += advance
	}
}

// applyRule 执行单条规则的替换。
//
// 用 FindAllStringSubmatchIndex + 手写拼接而不是 ReplaceAllStringFunc：
// 后者无法区分"整个匹配"与"捕获组"，而本层需要只替换捕获组、保留组外前缀。
func (p *Policy) applyRule(text string, r *Rule, stats *Stats) string {
	if stats.Hits == nil {
		stats.Hits = make(map[string]int)
	}
	var b strings.Builder
	b.Grow(len(text))
	last := 0
	for _, m := range r.Pattern.FindAllStringSubmatchIndex(text, -1) {
		start, end := m[0], m[1]
		// 捕获组优先：规则用括号精确界定"该被替换的那一段"。
		gStart, gEnd := start, end
		if len(m) >= 4 && m[2] >= 0 && m[3] >= 0 {
			gStart, gEnd = m[2], m[3]
		}
		b.WriteString(text[last:start])
		if gStart != start {
			b.WriteString(text[start:gStart]) // 组外前缀，如 "Bearer "
		}
		b.WriteString(p.replacement(r, text[gStart:gEnd]))
		if gEnd != end {
			b.WriteString(text[gEnd:end]) // 组外后缀，如邮箱域名
		}
		last = end
		stats.Hits[r.Name]++
		stats.Total++
	}
	b.WriteString(text[last:])
	return b.String()
}

// replacement 按动作生成替换文本。
func (p *Policy) replacement(r *Rule, value string) string {
	switch r.Action {
	case RedactDrop:
		return ""
	case RedactMask:
		return maskValue(value, r)
	case RedactTokenize:
		// 无盐值时不生成伪标识，降级为纯占位符。
		//
		// 这是一道 fail-safe，不是可选优化：token 由 sha256(salt|value) 派生，
		// salt 为空时退化成 sha256("|"+value) —— 攻击者拿手机号字典逐个算一遍
		// 就能把伪标识全部反解，脱敏形同未做，而且**看起来像做了**，
		// 比完全不脱敏更危险（运维会因为看到占位符而误以为安全）。
		//
		// 而"忘记配置盐值"恰恰是最常见的部署状态。降级后敏感值仍然被遮住，
		// 只失去跨请求关联能力 —— 可用性损失换安全性，方向正确。
		if p.Salt == "" {
			return fmt.Sprintf("[REDACTED:%s]", r.Name)
		}
		return fmt.Sprintf("[REDACTED:%s:%s]", r.Name, p.token(value))
	}
	return fmt.Sprintf("[REDACTED:%s]", r.Name)
}

// token 由 sha256(salt|value) 派生 6 字节十六进制作为确定性伪标识。
//
// 取 12 个 hex 字符（48 bit）的依据：在单条链路的实体规模（万级）下碰撞概率可忽略，
// 同时占位符总长控制在 30 字节内以抑制膨胀。不取全长摘要，因为占位符会大量重复出现，
// 长度直接进入 token 预算。
func (p *Policy) token(value string) string {
	h := sha256.Sum256([]byte(p.Salt + "|" + value))
	return hex.EncodeToString(h[:6])
}

// maskValue 保留首尾、遮盖中间。
//
// 掩码长度固定为 4 个 '*'，**不**等于被遮字符数：
// 按原长打星会把值的长度泄露出去（身份证 18 位、手机号 11 位一眼可辨），
// 而长度本身就是可用于穷举收窄的旁路信息。定长掩码切断这条泄露。
func maskValue(value string, r *Rule) string {
	runes := []rune(value)
	n := len(runes)
	if r.KeepPrefix+r.KeepSuffix >= n {
		// 保留位已覆盖全值，等于没脱敏。此时整体遮盖，宁可少给信息也不能漏。
		return "****"
	}
	var b strings.Builder
	b.Grow(r.KeepPrefix + r.KeepSuffix + 4)
	b.WriteString(string(runes[:r.KeepPrefix]))
	b.WriteString("****")
	if r.KeepSuffix > 0 {
		b.WriteString(string(runes[n-r.KeepSuffix:]))
	}
	return b.String()
}

// AuditContext 是脱敏回调所需的最小身份字段。
//
// 单独定义而不直接透传 contracts.ExecutionContext：审计只需要"哪个租户、哪次运行"，
// 收窄字段可避免审计实现误用其余身份数据，也让回调签名不随 ExecutionContext 演进而变。
type AuditContext struct {
	TenantID string
	UserID   string
	RunID    string
}

func auditContextOf(ec contracts.ExecutionContext) AuditContext {
	return AuditContext{TenantID: ec.TenantID, UserID: ec.UserID, RunID: ec.RunID}
}

// Redactor 是把 Policy 接到 Tool.After 与 Event.Transform 两个挂载点的中间件。
//
// 为什么两处都要挂：脱敏有两个方向，很多方案只做出向。
//   - Tool.After：工具结果回传给 LLM 之前。拦住的是"外部系统的数据带着敏感信息进入模型上下文"。
//   - Event.Transform：事件离开 Runtime 之前。**这一层的扩散面最大**——
//     同一份事件会进 SSE 推给前端、进 MQ 给下游消费者、进日志、进可观测系统，
//     任何一处泄露都是跨边界的。工具结果即便在 After 漏了，这里还有一道。
//
// 两层叠加要求 Redact 幂等，见该方法注释。
type Redactor struct {
	Policy *Policy
	// ToolEnabled / EventEnabled 分别控制两个挂载点是否生效，便于按链路差异配置。
	ToolEnabled  bool
	EventEnabled bool
	// OnRedact 为命中回调（可 nil），用于把审计事实送进日志或审计流。
	// 回调拿到的是身份与统计，**不含原文**——审计本身不该成为二次泄露点。
	OnRedact func(ctx context.Context, audit AuditContext, stats Stats)
	// OnUnsupported 为遇到未覆盖负载类型时的回调（可 nil）。
	// fail-open 必须可观测，否则"有负载没被脱敏"会静默长期存在。
	OnUnsupported func(typeName string)
}

// NewRedactor 构造默认两个挂载点都开启的脱敏中间件。policy 为 nil 时使用内置规则。
func NewRedactor(policy *Policy) *Redactor {
	if policy == nil {
		policy = NewPolicy(SensitivityInternal, "")
	}
	return &Redactor{Policy: policy, ToolEnabled: true, EventEnabled: true}
}

var _ Tool = (*Redactor)(nil)
var _ Event = (*Redactor)(nil)

// Before 不改写请求，原样透传。
//
// 刻意不做入向脱敏：工具入参是 LLM 的决策结果，改写它会让工具收到与模型意图
// 不符的参数，产生静默的错误行为——这比敏感信息泄露更难排查。
// 入向的保护由注入检测（收窄权限）与工具自身的入参校验承担。
func (r *Redactor) Before(_ context.Context, _ contracts.ExecutionContext, req contracts.ToolCallRequest) (contracts.ToolCallRequest, error) {
	return req, nil
}

// After 对工具输出脱敏。
//
// 只处理 Output，不动 CallID 与 IsError：CallID 是幂等键的一部分，
// 改动它会让同一逻辑调用在重试时被算成不同调用，破坏幂等语义。
func (r *Redactor) After(ctx context.Context, ec contracts.ExecutionContext, _ contracts.ToolCallRequest, result contracts.ToolResult) (contracts.ToolResult, error) {
	if !r.ToolEnabled || result.Output == "" {
		return result, nil
	}
	out, stats := r.Policy.Redact(result.Output)
	if stats.HitAny() {
		result.Output = out
		if r.OnRedact != nil {
			r.OnRedact(ctx, auditContextOf(ec), stats)
		}
	}
	return result, nil
}

// Transform 对事件负载脱敏。
//
// Data 是 any，实际形状为 map[string]any / []any / string / contracts.ToolResult /
// contracts.ToolCallRequest / contracts.Message 这几种（见 worker 与 react 的发射点）。
// 用类型 switch + 受限递归而不是 reflect 遍历：reflect 会碰到不可导出字段与循环引用，
// 且对未知结构体的处理语义无法保证安全，宁可漏也不要把事件流搞崩。
//
// 未知类型采取 fail-open（原样透传）：事件流是 Run 的可观测主干，
// 因为一个没预料到的负载形状就丢弃全部事件，代价远大于潜在的单点泄露。
func (r *Redactor) Transform(_ context.Context, ev contracts.RuntimeEvent) (contracts.RuntimeEvent, error) {
	if !r.EventEnabled || ev.Data == nil {
		return ev, nil
	}
	b := &payloadBudget{}
	if redacted := r.redactValue(ev.Data, 0, b); redacted != nil {
		ev.Data = redacted
	}
	return ev, nil
}

// maxPayloadDepth 限制事件负载的递归深度。
// 事件负载实际嵌套不超过 3 层，给到 8 层已很宽裕；设限是为了防自引用结构
// 把递归打成栈溢出——脱敏组件自己成为可用性故障点是不可接受的。
const maxPayloadDepth = 8

// maxPayloadNodes 限制单次 Transform 访问的节点总数，防止超大负载拖垮事件发射。
const maxPayloadNodes = 4096

// payloadBudget 在一次 Transform 内跨递归层共享，累计已访问节点数。
type payloadBudget struct{ nodes int }

// redactValue 递归脱敏负载。返回 nil 表示"无变化"，调用方沿用原值——
// 这样未命中的负载不会被无谓地复制一遍，对高频事件流是可观的分配节省。
func (r *Redactor) redactValue(v any, depth int, b *payloadBudget) any {
	if depth > maxPayloadDepth || b.nodes > maxPayloadNodes {
		return nil
	}
	switch val := v.(type) {
	case string:
		b.nodes++
		out, stats := r.Policy.Redact(val)
		if stats.HitAny() {
			return out
		}
		return nil
	case map[string]any:
		changed := false
		out := make(map[string]any, len(val))
		for k, item := range val {
			b.nodes++
			// 只脱敏值、不动 key：key 是结构契约，改动会让消费者解析失败。
			// key 本身含敏感信息属于生产方设计缺陷，不在本层兜。
			if rv := r.redactValue(item, depth+1, b); rv != nil {
				out[k] = rv
				changed = true
			} else {
				out[k] = item
			}
		}
		if changed {
			return out
		}
		return nil
	case []any:
		changed := false
		out := make([]any, len(val))
		for i, item := range val {
			b.nodes++
			if rv := r.redactValue(item, depth+1, b); rv != nil {
				out[i] = rv
				changed = true
			} else {
				out[i] = item
			}
		}
		if changed {
			return out
		}
		return nil
	case contracts.ToolResult:
		if val.Output == "" {
			return nil
		}
		out, stats := r.Policy.Redact(val.Output)
		if !stats.HitAny() {
			return nil
		}
		val.Output = out
		return val
	case contracts.ToolCallRequest:
		if val.Arguments == "" {
			return nil
		}
		// 入参里的敏感值通常是 LLM 从上下文抄来的（如把用户手机号填进查询工具）。
		// 此处只改事件副本、不影响真正送给工具的请求，故不存在
		// "改写参数导致工具行为异常"的问题——那正是 Before 刻意不做脱敏的原因。
		out, stats := r.Policy.Redact(val.Arguments)
		if !stats.HitAny() {
			return nil
		}
		val.Arguments = out
		return val
	case contracts.Message:
		if val.Content == "" {
			return nil
		}
		out, stats := r.Policy.Redact(val.Content)
		if !stats.HitAny() {
			return nil
		}
		val.Content = out
		return val
	default:
		if r.OnUnsupported != nil {
			r.OnUnsupported(fmt.Sprintf("%T", v))
		}
		return nil
	}
}
