package middleware

import (
	"agent-runtime/internal/contracts"
	"context"
	"strings"
	"testing"
)

// TestPolicy_Redact_StrongPatterns 强模式必须被命中，且各动作的输出形态符合约定。
//
// 逐项断言而非只看"变了"：脱敏最容易出的错是"看起来遮了、其实留了尾巴"，
// 例如只遮了手机号前段、或把域名一起遮掉导致不可用。
func TestPolicy_Redact_StrongPatterns(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "test-salt")

	cases := []struct {
		name        string
		in          string
		mustContain []string
		mustLack    []string
	}{
		{
			name:     "手机号保留前三后四",
			in:       "联系人电话 13800138000 已登记",
			mustLack: []string{"13800138000"},
			// 前 3 后 4 保留，中间定长 4 星。
			mustContain: []string{"138****8000"},
		},
		{
			name:        "身份证保留前四位",
			in:          "身份证号 110101199003074210",
			mustLack:    []string{"110101199003074210"},
			mustContain: []string{"1101****"},
		},
		{
			name:        "邮箱只遮本地部分、保留域名",
			in:          "邮箱 zhang.san@example.com 收信",
			mustLack:    []string{"zhang.san@"},
			mustContain: []string{"@example.com"},
		},
		{
			name:        "银行卡保留后四位",
			in:          "卡号 6222021234567890123",
			mustLack:    []string{"6222021234567890123"},
			mustContain: []string{"****0123"},
		},
		{
			name:     "OpenAI 密钥令牌化",
			in:       "key=sk-abcdefghij1234567890",
			mustLack: []string{"sk-abcdefghij1234567890"},
			// 令牌化占位符带规则名与伪标识。
			mustContain: []string{"[REDACTED:openai_key:"},
		},
		{
			name:        "Bearer 令牌只遮本体、保留前缀",
			in:          "Authorization: Bearer abcdefghijklmnop1234567890",
			mustLack:    []string{"abcdefghijklmnop1234567890"},
			mustContain: []string{"Bearer [REDACTED:bearer_token:"},
		},
		{
			name:     "PEM 私钥整块删除",
			in:       "-----BEGIN RSA PRIVATE KEY-----\nMIIEvQIBADAN\n-----END RSA PRIVATE KEY-----\n后续内容",
			mustLack: []string{"MIIEvQIBADAN", "BEGIN RSA PRIVATE KEY"},
			// 块被删掉但周边内容保留，说明 Drop 只作用于命中区间。
			mustContain: []string{"后续内容"},
		},
		{
			name:        "AWS 访问密钥 ID",
			in:          "akid=AKIAIOSFODNN7EXAMPLE",
			mustLack:    []string{"AKIAIOSFODNN7EXAMPLE"},
			mustContain: []string{"[REDACTED:aws_access_key:"},
		},
		{
			name:     "JWT 三段式",
			in:       "token=eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.abcdef1234567890xyz",
			mustLack: []string{"eyJhbGciOiJIUzI1NiJ9"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			out, stats := p.Redact(tc.in)
			if !stats.HitAny() {
				t.Fatalf("expected at least one hit, got none; out=%q", out)
			}
			for _, s := range tc.mustLack {
				if strings.Contains(out, s) {
					t.Errorf("output still contains %q: %s", s, out)
				}
			}
			for _, s := range tc.mustContain {
				if !strings.Contains(out, s) {
					t.Errorf("output missing expected %q: %s", s, out)
				}
			}
		})
	}
}

// TestPolicy_Redact_Idempotent 二次脱敏不得产生变化。
//
// 这是 Tool.After 与 Event.Transform 双层挂载的硬前提：同一段工具输出会先被
// After 脱敏、再随事件出域被 Transform 处理一次。若非幂等，占位符会被逐层套娃。
func TestPolicy_Redact_Idempotent(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "salt")
	inputs := []string{
		"电话 13800138000，邮箱 a.b@example.com，key sk-abcdefghij1234567890",
		"Authorization: Bearer abcdefghijklmnop1234567890 卡号 6222021234567890123",
		"-----BEGIN PRIVATE KEY-----\nQUJDREVG\n-----END PRIVATE KEY-----",
		"无敏感内容的普通业务文本",
	}
	for _, in := range inputs {
		once, _ := p.Redact(in)
		twice, stats2 := p.Redact(once)
		if twice != once {
			t.Errorf("not idempotent:\n once=%q\ntwice=%q", once, twice)
		}
		if stats2.Total != 0 {
			t.Errorf("second pass reported %d hits on already-redacted text: %q", stats2.Total, once)
		}
	}
}

// TestPolicy_Redact_TokenizeDeterministic 同一原值必须恒得同一伪标识（下游才能做实体关联），
// 不同原值必须得不同标识（否则等于把不同人合并成一个）。
func TestPolicy_Redact_TokenizeDeterministic(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "salt")
	a1, _ := p.Redact("key sk-abcdefghij1234567890")
	a2, _ := p.Redact("key sk-abcdefghij1234567890")
	b1, _ := p.Redact("key sk-zzzzzzzzzz9999999999")
	if a1 != a2 {
		t.Errorf("same value produced different tokens:\n%q\n%q", a1, a2)
	}
	if a1 == b1 {
		t.Errorf("different values produced identical tokens: %q", a1)
	}
}

// TestPolicy_Redact_SaltIsolation 不同盐必须产出不同标识。
//
// 这条守住多租户隔离：若盐与租户无关，A 租户能用自己已知的原值算出标识，
// 再去 B 租户的数据里比对，从而反推 B 的敏感值——脱敏反而成了关联攻击的桥梁。
func TestPolicy_Redact_SaltIsolation(t *testing.T) {
	in := "key sk-abcdefghij1234567890"
	outA, _ := NewPolicy(SensitivityInternal, "tenant-a").Redact(in)
	outB, _ := NewPolicy(SensitivityInternal, "tenant-b").Redact(in)
	if outA == outB {
		t.Errorf("different salts produced identical tokens: %q", outA)
	}
}

// TestPolicy_Redact_MaskDoesNotLeakLength 掩码长度必须固定，不得泄露原值长度。
//
// 身份证 18 位、手机号 11 位若按原长打星，等于把值的格式暴露出来，
// 而格式本身可用于收窄穷举空间。
func TestPolicy_Redact_MaskDoesNotLeakLength(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "salt")
	out11, _ := p.Redact("13800138000")        // 11 位手机号
	out18, _ := p.Redact("110101199003074210") // 18 位身份证
	// 两者的掩码段都应是定长 4 星。
	if strings.Count(out11, "*") != 4 {
		t.Errorf("mobile mask not fixed-length 4: %q", out11)
	}
	if strings.Count(out18, "*") != 4 {
		t.Errorf("id-card mask not fixed-length 4: %q", out18)
	}
}

// TestPolicy_Redact_MinLevel MinLevel 必须能按场景收紧处理范围。
//
// 内部审计链路只需遮机密级，若把手机号也遮掉就失去对账可用性；
// 面向前端链路则相反。同一个规则集靠 MinLevel 切换，而不必维护两套规则。
func TestPolicy_Redact_MinLevel(t *testing.T) {
	in := "电话 13800138000 与 key sk-abcdefghij1234567890"

	secretOnly := NewPolicy(SensitivitySecret, "salt")
	out, stats := secretOnly.Redact(in)
	if strings.Contains(out, "sk-abcdefghij1234567890") {
		t.Errorf("secret-level policy failed to redact api key: %q", out)
	}
	if !strings.Contains(out, "13800138000") {
		t.Errorf("secret-level policy should leave phone untouched: %q", out)
	}
	if stats.Hits["cn_mobile"] != 0 {
		t.Errorf("secret-level policy must not hit cn_mobile: %v", stats.Hits)
	}

	sensitive := NewPolicy(SensitivitySensitive, "salt")
	out2, _ := sensitive.Redact(in)
	if strings.Contains(out2, "13800138000") {
		t.Errorf("sensitive-level policy failed to redact phone: %q", out2)
	}
}

// TestPolicy_Redact_OutputBound 输出必须有硬上界，超限时整体降级为摘要引用。
//
// 占位符比原值长，海量命中的文本脱敏后会膨胀数倍，反过来撑爆下游 token 预算
// ——标记文本自己成了新问题。上界是"有界保证"，必须显式验证。
func TestPolicy_Redact_OutputBound(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "salt")
	// 构造远超上界的输入：4000 个手机号，每个 11 字节。
	var b strings.Builder
	for i := 0; i < 4000; i++ {
		b.WriteString("13800138000 ")
	}
	p.MaxOutputBytes = 4096
	out, stats := p.Redact(b.String())
	if len(out) > p.MaxOutputBytes {
		t.Errorf("output %d bytes exceeds limit %d", len(out), p.MaxOutputBytes)
	}
	if !stats.Truncated {
		t.Error("expected Truncated=true when output hit the bound")
	}
	// 降级后必须是摘要引用而不是被切碎的大段占位符。
	if !strings.HasPrefix(out, "[REDACTED:oversize:") {
		t.Errorf("expected oversize summary placeholder, got prefix %q", out[:min(60, len(out))])
	}
	// 原值不得残留在摘要里。
	if strings.Contains(out, "13800138000") {
		t.Errorf("original value leaked into summary: %q", out[:min(200, len(out))])
	}
	if stats.Total == 0 {
		t.Error("expected hits to be counted before truncation")
	}
}

func TestPolicy_Redact_InputExactlyAtLimit(t *testing.T) {
	// 构造长度恰好等于 limit 的输入。
	// 填充必须以非 word 字符（空格）结尾：cn_mobile 规则用 \b 界定边界，
	// 若填充的 'x' 与手机号首位相邻，边界不成立、规则不命中——
	// 那是规则"排除 ID13800138000 这类紧邻串"的既定行为，不是本用例要验证的东西。
	p := NewPolicy(SensitivityInternal, "salt")
	p.MaxOutputBytes = 200
	phone := "13800138000"
	in := strings.Repeat("x", p.MaxOutputBytes-len(phone)-1) + " " + phone
	if len(in) != p.MaxOutputBytes {
		t.Fatalf("test setup: input length %d != limit %d", len(in), p.MaxOutputBytes)
	}
	out, stats := p.Redact(in)
	// 恰好等于上限：不触发降级，但必须正常脱敏。
	if stats.Truncated {
		t.Errorf("input at exact limit should not be treated as oversize: %q", out)
	}
	if strings.Contains(out, phone) {
		t.Errorf("phone leaked when input length == limit: %q", out)
	}
	if stats.Hits["cn_mobile"] == 0 {
		t.Errorf("expected cn_mobile hit, got %v", stats.Hits)
	}
	// 遮盖后长度不变（11 位 → 11 位），因此仍恰好等于上限、不该被降级。
	if len(out) > p.MaxOutputBytes {
		t.Errorf("redacted output %d exceeds limit %d", len(out), p.MaxOutputBytes)
	}
}

// TestPolicy_Redact_OversizeAuditReportsRules 降级摘要必须报告原文命中的规则名。
//
// 这是运维能判断"泄露的是什么类型数据"的唯一线索：
// 内容已被丢弃，只剩 rules= 列表可用。若为空，告警就退化成"有个大文本被拦了"。
func TestPolicy_Redact_OversizeAuditReportsRules(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "salt")
	p.MaxOutputBytes = 256
	// 混合多种敏感类型，且总量远超上限。
	var b strings.Builder
	for i := 0; i < 200; i++ {
		b.WriteString("电话 13800138000 邮箱 a.b@example.com 密钥 sk-abcdefghij1234567890\n")
	}
	out, stats := p.Redact(b.String())
	if !stats.Truncated {
		t.Fatal("expected truncation")
	}
	for _, want := range []string{"cn_mobile", "email", "openai_key"} {
		if !strings.Contains(out, want) {
			t.Errorf("oversize summary missing rule %q: %s", want, out)
		}
		if stats.Hits[want] == 0 {
			t.Errorf("stats missing hit count for %q: %v", want, stats.Hits)
		}
	}
	// 摘要本身必须仍在上界内，且不含任何原值。
	if len(out) > p.MaxOutputBytes {
		t.Errorf("summary %d bytes exceeds limit %d", len(out), p.MaxOutputBytes)
	}
	for _, leak := range []string{"13800138000", "a.b@example.com", "sk-abcdefghij1234567890"} {
		if strings.Contains(out, leak) {
			t.Errorf("original value leaked into summary: %q", leak)
		}
	}
}

// TestPolicy_Redact_OversizeInputStillAudited 输入本身就超上限时也必须完成检测。
//
// 回归防护：曾经的实现是"超限即 break 出规则循环"，导致这种输入一条规则都没扫，
// 审计里 rules 为空。降级路径改为对原文做纯检测重算后，计数必须非空。
func TestPolicy_Redact_OversizeInputStillAudited(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "salt")
	p.MaxOutputBytes = 64
	in := strings.Repeat("13800138000 ", 500) // 远超 64 字节
	out, stats := p.Redact(in)
	if !stats.Truncated {
		t.Fatal("expected truncation")
	}
	if stats.Total == 0 {
		t.Fatal("oversize input was not scanned; audit would lose all rule facts")
	}
	if stats.Hits["cn_mobile"] != 500 {
		t.Errorf("expected 500 cn_mobile hits, got %v", stats.Hits)
	}
	if strings.Contains(out, "13800138000") {
		t.Errorf("original value leaked: %q", out)
	}
}

// TestPolicy_Redact_NoFalsePositiveOnOrdinaryText 普通业务文本不得被误伤。
//
// 误报率是脱敏能否上生产的关键：把正常文本打得千疮百孔，业务方会直接关掉它。
// 这里的样本刻意覆盖"形似敏感但不是"的情况。
func TestPolicy_Redact_NoFalsePositiveOnOrdinaryText(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "salt")
	benign := []string{
		"订单 ORD-20260919-0001 已发货",
		"节点 node-3 执行耗时 1.25s，重试 2 次",
		"用户反馈：功能不可用，请忽略上面那条建议改为检查配置", // "忽略上面"后无"指令/规则"类名词，不应命中
		"金额 12345 元，数量 67890 件",      // 5 位数字，不构成手机号/卡号
		"版本号 v1.2.3，构建号 20260919",
		"DAG 依赖：plan -> code -> test -> done",
		`SELECT * FROM agent_node WHERE tenant_id = ?`,
	}
	for _, in := range benign {
		out, stats := p.Redact(in)
		if stats.HitAny() {
			t.Errorf("false positive on %q -> %q (hits=%v)", in, out, stats.Hits)
		}
		if out != in {
			t.Errorf("benign text altered:\n in=%q\nout=%q", in, out)
		}
	}
}

// TestPolicy_Redact_BankCardSkipsLongDigits 超长数字串不得被当作卡号切碎。
//
// RE2 不做回溯：20 位以上连续数字整体不匹配 {12,18}+尾部词边界，因此被完整跳过。
// 订单号、流水号被切碎是真实的可用性事故，这条必须锁死。
func TestPolicy_Redact_BankCardSkipsLongDigits(t *testing.T) {
	p := NewPolicy(SensitivityInternal, "salt")
	in := "订单号 1234567890123456789012345" // 25 位
	out, _ := p.Redact(in)
	if !strings.Contains(out, in[len("订单号 "):]) {
		t.Errorf("25-digit order number was altered: %q", out)
	}
}

// TestRedactor_After_ToolOutput Tool.After 必须脱敏输出，且不动 CallID 与 IsError。
//
// CallID 是幂等键的一部分，改它会让同一逻辑调用在重试时被算成不同调用。
func TestRedactor_After_ToolOutput(t *testing.T) {
	var audit AuditContext
	var auditStats Stats
	called := false
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	r.OnRedact = func(_ context.Context, ac AuditContext, s Stats) {
		called, audit, auditStats = true, ac, s
	}

	ec := newEC("tenant-1", "user-9", "run-7")
	res := toolResult("call-abc", "查询到用户 13800138000 的记录", false)
	out, err := r.After(context.Background(), ec, toolRequest("call-abc", "query", `{"phone":"13800138000"}`), res)
	if err != nil {
		t.Fatalf("After: %v", err)
	}
	if strings.Contains(out.Output, "13800138000") {
		t.Errorf("phone leaked in tool output: %q", out.Output)
	}
	if out.CallID != "call-abc" {
		t.Errorf("CallID changed from call-abc to %q", out.CallID)
	}
	if out.IsError != false {
		t.Errorf("IsError changed to %v", out.IsError)
	}
	if !called {
		t.Fatal("OnRedact was not invoked")
	}
	if audit.TenantID != "tenant-1" || audit.UserID != "user-9" || audit.RunID != "run-7" {
		t.Errorf("audit context mismatch: %+v", audit)
	}
	if auditStats.Total != 1 {
		t.Errorf("expected 1 hit, got %d", auditStats.Total)
	}
}

// TestRedactor_After_PreservesCleanOutput 无命中时输出必须逐字节不变。
//
// 脱敏不能引入任何副作用：若对干净文本也做了改写（哪怕是空格规范化），
// 工具结果的幂等复用与断言比较都会失效。
func TestRedactor_After_PreservesCleanOutput(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	clean := "节点执行成功，耗时 1.2s"
	out, err := r.After(context.Background(), newEC("t", "u", "r"), toolRequest("c1", "echo", "{}"), toolResult("c1", clean, false))
	if err != nil {
		t.Fatalf("After: %v", err)
	}
	if out.Output != clean {
		t.Errorf("clean output altered:\n got=%q\nwant=%q", out.Output, clean)
	}
}

// TestRedactor_After_Disabled ToolEnabled=false 时不得处理（按链路差异关闭的能力）。
func TestRedactor_After_Disabled(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	r.ToolEnabled = false
	out, _ := r.After(context.Background(), newEC("t", "u", "r"), toolRequest("c1", "q", "{}"), toolResult("c1", "13800138000", false))
	if out.Output != "13800138000" {
		t.Errorf("expected untouched output when disabled, got %q", out.Output)
	}
}

// TestRedactor_Before_Passthrough Before 不得改写入参。
//
// 入参是 LLM 的决策结果，改写它会让工具收到与模型意图不符的参数，
// 产生静默的错误行为——比泄露更难排查。
func TestRedactor_Before_Passthrough(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	req := toolRequest("c1", "lookup", `{"phone":"13800138000"}`)
	out, err := r.Before(context.Background(), newEC("t", "u", "r"), req)
	if err != nil {
		t.Fatalf("Before: %v", err)
	}
	if out != req {
		t.Errorf("Before altered request:\n got=%+v\nwant=%+v", out, req)
	}
}

// TestModelRedactor_After_ModelOutput 模型输出必须脱敏，且视图与核心共享同一 Policy 与审计回调。
//
// 断言"共享"而不只是"遮住了"：视图若各自持一份 Policy，规则与门槛就会分叉，
// 而 OnRedact 挂在核心上仍在触发，表面看不出偏差——只有回调真的从视图路径触发才能证明是同一实例。
func TestModelRedactor_After_ModelOutput(t *testing.T) {
	var audit AuditContext
	var auditStats Stats
	called := false
	core := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	core.OnRedact = func(_ context.Context, ac AuditContext, s Stats) {
		called, audit, auditStats = true, ac, s
	}
	m := &ModelRedactor{Redactor: core}

	resp := contracts.GenerateResponse{
		Message: contracts.Message{Role: contracts.RoleAssistant, Content: "已登记 13800138000，密钥 sk-abcdefghij1234567890"},
		Model:   "gpt-4o",
		Usage:   contracts.Usage{PromptTokens: 11, CompletionTokens: 22, TotalTokens: 33},
	}
	out, err := m.After(context.Background(), newEC("tenant-1", "user-9", "run-7"), contracts.GenerateRequest{Model: "gpt-4o"}, resp)
	if err != nil {
		t.Fatalf("After: %v", err)
	}
	for _, leak := range []string{"13800138000", "sk-abcdefghij1234567890"} {
		if strings.Contains(out.Message.Content, leak) {
			t.Errorf("sensitive value %q leaked in model output: %q", leak, out.Message.Content)
		}
	}
	if !strings.Contains(out.Message.Content, "138****8000") {
		t.Errorf("expected masked phone, got %q", out.Message.Content)
	}
	// 角色、模型名与用量不是脱敏对象：Usage 参与成本归集，改了就错账。
	if out.Message.Role != contracts.RoleAssistant || out.Model != "gpt-4o" || out.Usage.TotalTokens != 33 {
		t.Errorf("non-content fields altered: %+v", out)
	}
	if !called {
		t.Fatal("OnRedact was not invoked through the model view")
	}
	if audit.TenantID != "tenant-1" || audit.UserID != "user-9" || audit.RunID != "run-7" {
		t.Errorf("audit context mismatch: %+v", audit)
	}
	if auditStats.Hits["cn_mobile"] != 1 || auditStats.Hits["openai_key"] != 1 {
		t.Errorf("audit stats missing rule hits: %v", auditStats.Hits)
	}
}

// TestModelRedactor_After_CleanAndEmptyUntouched 干净内容逐字节不变，空内容不触发审计。
//
// 与工具侧同一条纪律：脱敏不得引入任何副作用，否则模型输出的幂等复用与断言比较都会失效。
func TestModelRedactor_After_CleanAndEmptyUntouched(t *testing.T) {
	fired := 0
	m := &ModelRedactor{Redactor: NewRedactor(NewPolicy(SensitivityInternal, "salt"))}
	m.OnRedact = func(context.Context, AuditContext, Stats) { fired++ }

	clean := "构建成功，12 个用例通过"
	out, err := m.After(context.Background(), newEC("t", "u", "r"), contracts.GenerateRequest{},
		contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant, Content: clean}})
	if err != nil {
		t.Fatalf("After: %v", err)
	}
	if out.Message.Content != clean {
		t.Errorf("clean content altered:\n got=%q\nwant=%q", out.Message.Content, clean)
	}

	empty, err := m.After(context.Background(), newEC("t", "u", "r"), contracts.GenerateRequest{},
		contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant}})
	if err != nil {
		t.Fatalf("After on empty content: %v", err)
	}
	if empty.Message.Content != "" {
		t.Errorf("empty content became %q", empty.Message.Content)
	}
	if fired != 0 {
		t.Errorf("OnRedact fired %d times on clean/empty content", fired)
	}
}

// TestModelRedactor_After_IdempotentOverToolOutput 已经脱敏过的内容二次处理不得套娃。
//
// 真实链路上同一段内容会依次经过 Tool.After →（作为模型上下文）→ 模型输出 → Model.After
// → Event.Transform，多层叠加的硬前提是幂等：非幂等时占位符会被逐层套上新的占位符。
func TestModelRedactor_After_IdempotentOverToolOutput(t *testing.T) {
	core := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	m := &ModelRedactor{Redactor: core}
	ec := newEC("t", "u", "r")

	res, err := core.After(context.Background(), ec, toolRequest("c1", "query", "{}"), toolResult("c1", "用户 13800138000", false))
	if err != nil {
		t.Fatalf("tool After: %v", err)
	}
	once := res.Output
	if strings.Contains(once, "13800138000") {
		t.Fatalf("test setup: tool output not redacted: %q", once)
	}

	second, err := m.After(context.Background(), ec, contracts.GenerateRequest{},
		contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant, Content: once}})
	if err != nil {
		t.Fatalf("model After: %v", err)
	}
	if second.Message.Content != once {
		t.Errorf("model pass re-wrapped already-redacted text:\n in=%q\nout=%q", once, second.Message.Content)
	}
}

// TestModelRedactor_After_Disabled 模型挂载点的开关必须独立生效，且不影响工具挂载点。
func TestModelRedactor_After_Disabled(t *testing.T) {
	core := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	core.ModelEnabled = false
	m := &ModelRedactor{Redactor: core}

	resp, err := m.After(context.Background(), newEC("t", "u", "r"), contracts.GenerateRequest{},
		contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant, Content: "13800138000"}})
	if err != nil {
		t.Fatalf("After: %v", err)
	}
	if resp.Message.Content != "13800138000" {
		t.Errorf("expected untouched model output when disabled, got %q", resp.Message.Content)
	}
	// 关掉模型挂载点不得连带关掉工具挂载点：两者是独立开关。
	tr, _ := core.After(context.Background(), newEC("t", "u", "r"), toolRequest("c1", "q", "{}"), toolResult("c1", "13800138000", false))
	if strings.Contains(tr.Output, "13800138000") {
		t.Errorf("tool mount unexpectedly disabled: %q", tr.Output)
	}
}

// TestModelRedactor_Before_Passthrough Before 不得改写发往模型的请求。
//
// 改写 prompt 会让模型基于与调用方意图不符的输入作答，且过程静默、错误一路传导到输出。
func TestModelRedactor_Before_Passthrough(t *testing.T) {
	m := &ModelRedactor{Redactor: NewRedactor(NewPolicy(SensitivityInternal, "salt"))}
	req := contracts.GenerateRequest{
		Model:    "gpt-4o",
		Messages: []contracts.Message{{Role: contracts.RoleUser, Content: "电话 13800138000"}},
	}
	out, err := m.Before(context.Background(), newEC("t", "u", "r"), req)
	if err != nil {
		t.Fatalf("Before: %v", err)
	}
	if out.Model != req.Model || len(out.Messages) != 1 || out.Messages[0] != req.Messages[0] {
		t.Errorf("Before altered the outgoing request:\n got=%+v\nwant=%+v", out, req)
	}
}

// TestModelRedactor_NilSafe 零值视图不得 panic，原样透传。
//
// 链由部署侧拼装，一个零值元素不该让整个 Run 崩掉——
// 脱敏组件自己成为可用性故障点是不可接受的。
func TestModelRedactor_NilSafe(t *testing.T) {
	var m *ModelRedactor
	resp := contracts.GenerateResponse{Message: contracts.Message{Role: contracts.RoleAssistant, Content: "13800138000"}}
	out, err := m.After(context.Background(), newEC("t", "u", "r"), contracts.GenerateRequest{}, resp)
	if err != nil || out.Message.Content != "13800138000" {
		t.Fatalf("nil view must pass through: out=%+v err=%v", out, err)
	}
	out2, err := (&ModelRedactor{}).After(context.Background(), newEC("t", "u", "r"), contracts.GenerateRequest{}, resp)
	if err != nil || out2.Message.Content != "13800138000" {
		t.Fatalf("zero-value view must pass through: out=%+v err=%v", out2, err)
	}
}

// TestRedactor_Transform_NestedPayload 事件负载的嵌套结构必须被递归脱敏。
//
// worker 与 react 发射的事件负载是 map[string]any，工具结果与入参常嵌在其中；
// 只处理顶层字符串会漏掉绝大部分实际泄露点。
func TestRedactor_Transform_NestedPayload(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	ev := runtimeEvent("run-1", "node-1", map[string]any{
		"output":  "用户 13800138000 已认证",
		"attempt": 2,
		"nested": map[string]any{
			"email": "a.b@example.com",
			"safe":  "普通文本",
		},
		"list": []any{"卡号 6222021234567890123", 42},
	})

	out, err := r.Transform(context.Background(), ev)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	m, ok := out.Data.(map[string]any)
	if !ok {
		t.Fatalf("payload type changed to %T", out.Data)
	}
	if strings.Contains(m["output"].(string), "13800138000") {
		t.Errorf("top-level string not redacted: %v", m["output"])
	}
	if m["attempt"] != 2 {
		t.Errorf("non-string field altered: %v", m["attempt"])
	}
	nested := m["nested"].(map[string]any)
	if strings.Contains(nested["email"].(string), "a.b@") {
		t.Errorf("nested email not redacted: %v", nested["email"])
	}
	if nested["safe"] != "普通文本" {
		t.Errorf("nested safe value altered: %v", nested["safe"])
	}
	list := m["list"].([]any)
	if strings.Contains(list[0].(string), "6222021234567890123") {
		t.Errorf("list item not redacted: %v", list[0])
	}
	if list[1] != 42 {
		t.Errorf("list non-string item altered: %v", list[1])
	}
}

// TestRedactor_Transform_TypedPayloads 结构体形态的负载也必须被覆盖。
//
// react 引擎直接把 contracts.ToolCallRequest / ToolResult 作为 Data 发射，
// 若只处理 map 与 string，ReAct 链路的事件会完全绕过脱敏。
func TestRedactor_Transform_TypedPayloads(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))

	evResult := runtimeEvent("run-1", "n1", toolResult("c1", "查到 13800138000", false))
	out, err := r.Transform(context.Background(), evResult)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	tr, ok := out.Data.(contracts.ToolResult)
	if !ok {
		t.Fatalf("payload type changed to %T", out.Data)
	}
	if strings.Contains(tr.Output, "13800138000") {
		t.Errorf("ToolResult.Output not redacted: %q", tr.Output)
	}
	if tr.CallID != "c1" {
		t.Errorf("ToolResult.CallID changed to %q", tr.CallID)
	}

	evReq := runtimeEvent("run-1", "n1", toolRequest("c2", "lookup", `{"phone":"13800138000"}`))
	out2, _ := r.Transform(context.Background(), evReq)
	tcr := out2.Data.(contracts.ToolCallRequest)
	if strings.Contains(tcr.Arguments, "13800138000") {
		t.Errorf("ToolCallRequest.Arguments not redacted: %q", tcr.Arguments)
	}
	if tcr.CallID != "c2" || tcr.Name != "lookup" {
		t.Errorf("request identity fields changed: %+v", tcr)
	}
}

// TestRedactor_Transform_FailOpenOnUnknown 未知负载类型必须原样透传并上报。
//
// 事件流是 Run 的可观测主干，因为一个没预料到的负载形状就丢弃全部事件，
// 代价远大于潜在的单点泄露。但 fail-open 必须可观测，否则会静默长期存在。
func TestRedactor_Transform_FailOpenOnUnknown(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	var reported string
	r.OnUnsupported = func(typeName string) { reported = typeName }

	type customPayload struct{ Secret string }
	ev := runtimeEvent("run-1", "n1", customPayload{Secret: "13800138000"})
	out, err := r.Transform(context.Background(), ev)
	if err != nil {
		t.Fatalf("Transform returned error on unknown type: %v", err)
	}
	cp, ok := out.Data.(customPayload)
	if !ok {
		t.Fatalf("unknown payload dropped or altered: %T", out.Data)
	}
	if cp.Secret != "13800138000" {
		t.Errorf("unknown payload mutated: %+v", cp)
	}
	if reported == "" {
		t.Error("OnUnsupported was not called; fail-open would be silent")
	}
}

// TestRedactor_Transform_NilDataPreserved Data 为 nil 的事件不得被改动。
func TestRedactor_Transform_NilDataPreserved(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	ev := runtimeEvent("run-1", "n1", nil)
	out, err := r.Transform(context.Background(), ev)
	if err != nil {
		t.Fatalf("Transform: %v", err)
	}
	if out.Data != nil {
		t.Errorf("nil payload became %v", out.Data)
	}
	if out.RunID != "run-1" || out.NodeID != "n1" {
		t.Errorf("event identity changed: %+v", out)
	}
}

// TestRedactor_Transform_Disabled EventEnabled=false 时不得处理。
func TestRedactor_Transform_Disabled(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	r.EventEnabled = false
	ev := runtimeEvent("run-1", "n1", map[string]any{"output": "13800138000"})
	out, _ := r.Transform(context.Background(), ev)
	m := out.Data.(map[string]any)
	if m["output"] != "13800138000" {
		t.Errorf("expected untouched payload when disabled, got %v", m["output"])
	}
}

// TestRedactor_DepthLimit 超深嵌套必须被截断而不是栈溢出。
//
// 自引用或恶意构造的深层结构会把递归打爆——脱敏组件自己成为可用性故障点
// 是不可接受的，宁可少处理也不能让进程崩。
func TestRedactor_DepthLimit(t *testing.T) {
	r := NewRedactor(NewPolicy(SensitivityInternal, "salt"))
	// 构造 50 层嵌套（远超 maxPayloadDepth=8）。
	var payload any = "13800138000"
	for i := 0; i < 50; i++ {
		payload = map[string]any{"n": payload}
	}
	ev := runtimeEvent("run-1", "n1", payload)
	// 只要不 panic 且能返回，就算达到目的；深层内容未被脱敏是预期行为。
	if _, err := r.Transform(context.Background(), ev); err != nil {
		t.Fatalf("Transform on deep nesting: %v", err)
	}
}

// TestRedactor_ChainOrder 脱敏与注入检测串在一条链上时，顺序必须可预期：
// Guard.Before 先判、Redactor.After 后处理，两者互不干扰。
func TestRedactor_ChainOrder(t *testing.T) {
	chain := NewToolChain(NewGuard(nil), NewRedactor(NewPolicy(SensitivityInternal, "salt")))
	req, err := chain.Before(context.Background(), newEC("t", "u", "r"), toolRequest("c1", "lookup", `{"phone":"13800138000"}`))
	if err != nil {
		t.Fatalf("chain.Before: %v", err)
	}
	res, err := chain.After(context.Background(), newEC("t", "u", "r"), req, toolResult("c1", "用户 13800138000", false))
	if err != nil {
		t.Fatalf("chain.After: %v", err)
	}
	if strings.Contains(res.Output, "13800138000") {
		t.Errorf("chain did not redact output: %q", res.Output)
	}
}

// TestStats_RuleNames_Stable 审计用的规则名列表必须稳定有序。
//
// map 遍历无序，若不排序，同一输入会产出不同顺序的日志行，
// 让"同一告警的重复出现"无法在日志系统中聚合。
func TestStats_RuleNames_Stable(t *testing.T) {
	s := Stats{Hits: map[string]int{"cn_mobile": 1, "email": 2, "aws_access_key": 1}}
	got := s.RuleNames()
	want := []string{"aws_access_key", "cn_mobile", "email"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("RuleNames=%v want %v", got, want)
	}
}
