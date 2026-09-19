package worker

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/middleware"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// 本文件验证 worker 侧的**接线**：事件出域是否真的经过脱敏链，
// 以及安全装配的策略解析是否按预期降级。
//
// 与 executor/toolchain_test.go 同理，中间件能力本身已有充分单测，
// 这里只证明"生产路径上真的会经过它们"。此前 EventChain 在整个仓库
// 从未被调用过 —— 挂载点存在但链路是断的。

// recordingSink 记录发射出去的事件，用于断言出域内容。
type recordingSink struct {
	events []contracts.RuntimeEvent
	err    error
}

func (s *recordingSink) Emit(_ context.Context, ev contracts.RuntimeEvent) error {
	s.events = append(s.events, ev)
	return s.err
}

var _ eventSinkForTest = (*recordingSink)(nil)

// eventSinkForTest 是为了让本文件不依赖 event 包的具体类型而声明的局部接口。
// Worker.RuntimeEvents 字段本身就是 event.Sink（单方法接口），此处同形。
type eventSinkForTest interface {
	Emit(context.Context, contracts.RuntimeEvent) error
}

func sensitiveEvent(data any) contracts.RuntimeEvent {
	return contracts.RuntimeEvent{
		ID: "ev-1", RunID: "r1", NodeID: "n1", TenantID: "t1",
		Type: contracts.EventNodeFinished, Timestamp: time.Now(), Data: data,
	}
}

// TestEmitRuntimeEvent_RedactsPayload 事件出域前必须脱敏。
//
// 这是扩散面最大的一道出口：同一份事件会进 SSE 推给前端、进日志、
// 进可观测系统。工具结果即便在 Tool.After 漏了，这里必须兜住。
func TestEmitRuntimeEvent_RedactsPayload(t *testing.T) {
	sink := &recordingSink{}
	w := &Worker{
		RuntimeEvents: sink,
		EventChain:    middleware.NewEventChain(middleware.NewRedactor(middleware.NewPolicy(middleware.SensitivityInternal, "test-salt"))),
	}

	w.emitRuntimeEvent(context.Background(), sensitiveEvent(map[string]any{"output": "contact 13800138000 now"}))

	if len(sink.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(sink.events))
	}
	payload, ok := sink.events[0].Data.(map[string]any)
	if !ok {
		t.Fatalf("payload type changed: %T", sink.events[0].Data)
	}
	got, _ := payload["output"].(string)
	if strings.Contains(got, "13800138000") {
		t.Errorf("event payload leaked raw phone number: %q", got)
	}
	if got == "" {
		t.Error("event payload was dropped entirely instead of being masked")
	}
}

// TestEmitRuntimeEvent_TransformFailureStillEmits 变换失败不得吞掉事件。
//
// 事件流是 Run 的可观测主干。因为脱敏环节出错就丢弃全部事件，
// 会让整个 Run 变成黑盒 —— 排障能力丢失的代价远大于单点泄露风险。
func TestEmitRuntimeEvent_TransformFailureStillEmits(t *testing.T) {
	sink := &recordingSink{}
	w := &Worker{RuntimeEvents: sink, EventChain: middleware.NewEventChain(failingEvent{})}

	w.emitRuntimeEvent(context.Background(), sensitiveEvent("payload"))

	if len(sink.events) != 1 {
		t.Fatalf("event was dropped when transform failed: got %d event(s)", len(sink.events))
	}
	if sink.events[0].Data != "payload" {
		t.Errorf("expected original payload on transform failure, got %v", sink.events[0].Data)
	}
}

// failingEvent 是一个总是失败的变换器，用于验证降级路径。
type failingEvent struct{}

func (failingEvent) Transform(context.Context, contracts.RuntimeEvent) (contracts.RuntimeEvent, error) {
	return contracts.RuntimeEvent{}, errors.New("transform boom")
}

var _ middleware.Event = failingEvent{}

// TestEmitRuntimeEvent_NilChainIsNoop 未装配事件链时原样发射。
//
// 保证既有部署与测试零感知：接入这套能力不应是一次 flag day 迁移。
func TestEmitRuntimeEvent_NilChainIsNoop(t *testing.T) {
	sink := &recordingSink{}
	w := &Worker{RuntimeEvents: sink}

	w.emitRuntimeEvent(context.Background(), sensitiveEvent("contact 13800138000"))

	if len(sink.events) != 1 {
		t.Fatalf("expected 1 emitted event, got %d", len(sink.events))
	}
	if sink.events[0].Data != "contact 13800138000" {
		t.Errorf("payload altered without EventChain: %v", sink.events[0].Data)
	}
}

// TestEmitRuntimeEvent_NilSinkNoPanic 未配置发射器时不得 panic。
func TestEmitRuntimeEvent_NilSinkNoPanic(t *testing.T) {
	w := &Worker{EventChain: middleware.NewEventChain(middleware.NewRedactor(nil))}
	w.emitRuntimeEvent(context.Background(), sensitiveEvent("x")) // 不应 panic
}

// TestParseSensitivity_UnknownFallsBackStricter 无法识别的配置必须向"更严"降级。
//
// 一个拼写错误若导致脱敏门槛退到 public（几乎不处理），
// 就会静默关掉脱敏 —— 且没有任何报错，泄露要等到出事才发现。
func TestParseSensitivity_UnknownFallsBackStricter(t *testing.T) {
	for _, in := range []string{"", "typo", "PUBLIK", "  "} {
		if got := parseSensitivity(in); got != middleware.SensitivityInternal {
			t.Errorf("parseSensitivity(%q) = %v, want internal (stricter fallback)", in, got)
		}
	}
	cases := map[string]middleware.SensitivityLevel{
		"public":     middleware.SensitivityPublic,
		"internal":   middleware.SensitivityInternal,
		"sensitive":  middleware.SensitivitySensitive,
		"secret":     middleware.SensitivitySecret,
		"  SECRET  ": middleware.SensitivitySecret, // 大小写与空白都要容错
	}
	for in, want := range cases {
		if got := parseSensitivity(in); got != want {
			t.Errorf("parseSensitivity(%q) = %v, want %v", in, got, want)
		}
	}
}

// TestCsvNames 名单解析需忽略空白与空项。
func TestCsvNames(t *testing.T) {
	if got := csvNames(""); got != nil {
		t.Errorf("empty input should yield nil, got %v", got)
	}
	if got := csvNames("  ,  ,"); got != nil {
		t.Errorf("blank-only input should yield nil, got %v", got)
	}
	got := csvNames(" shell_exec, db_write ,, ")
	if len(got) != 2 || !got["shell_exec"] || !got["db_write"] {
		t.Errorf("csvNames parsed wrong: %v", got)
	}
}

// TestSecurityBundle_DisabledByEnv 两个开关都关闭时不得装配任何链。
func TestSecurityBundle_DisabledByEnv(t *testing.T) {
	t.Setenv(EnvGuardEnabled, "false")
	t.Setenv(EnvRedactEnabled, "false")

	b := newSecurityBundle(nil, nil, nil)
	if b.ToolChain != nil || b.EventChain != nil {
		t.Errorf("expected no chains when both disabled, got tool=%v event=%v", b.ToolChain, b.EventChain)
	}
}

// TestSecurityBundle_RedactOnlySkipsToolGuard 只关护栏时脱敏仍须在两处生效。
//
// Redactor 同时挂在工具链与事件链上，共用同一实例。
// 只关护栏不应影响脱敏 —— 否则一次策略调整会连带关掉另一个独立能力。
func TestSecurityBundle_RedactOnlySkipsToolGuard(t *testing.T) {
	t.Setenv(EnvGuardEnabled, "false")
	t.Setenv(EnvRedactEnabled, "true")
	t.Setenv("REDACTION_SALT", "unit-salt")

	b := newSecurityBundle(nil, nil, newCredentialsFromEnv())
	if b.ToolChain == nil {
		t.Fatal("expected tool chain with redactor when guard disabled")
	}
	if b.EventChain == nil {
		t.Fatal("expected event chain with redactor when guard disabled")
	}

	// 事件链确实能脱敏（不只是非 nil）。
	sink := &recordingSink{}
	w := &Worker{RuntimeEvents: sink, EventChain: b.EventChain}
	w.emitRuntimeEvent(context.Background(), sensitiveEvent(map[string]any{"output": "phone 13900139000"}))
	if len(sink.events) != 1 {
		t.Fatalf("expected 1 event, got %d", len(sink.events))
	}
	payload, _ := sink.events[0].Data.(map[string]any)
	if got, _ := payload["output"].(string); strings.Contains(got, "13900139000") {
		t.Errorf("assembled event chain did not redact: %q", got)
	}
}

// TestRedactionSaltFrom_FallsBackEmpty 未配置盐值时返回空串，由 Redactor 降级为纯掩码。
//
// 空盐值绝不能用于派生伪标识：token 会退化成 sha256("|"+value)，
// 攻击者拿字典逐个算一遍即可反解，脱敏看起来做了实则没做。
func TestRedactionSaltFrom_FallsBackEmpty(t *testing.T) {
	t.Setenv("REDACTION_SALT", "")
	if got := redactionSaltFrom(&credentialEnvForTest{}); got != "" {
		t.Errorf("expected empty salt when unset, got %q", got)
	}
	if got := redactionSaltFrom(nil); got != "" {
		t.Errorf("nil provider must yield empty salt, got %q", got)
	}
}

// credentialEnvForTest 是一个读不到任何凭证的 provider。
type credentialEnvForTest struct{}

func (credentialEnvForTest) Credential(context.Context, contracts.CredentialPurpose) (contracts.Credential, error) {
	return contracts.Credential{}, errors.New("no credential")
}

var _ contracts.CredentialProvider = credentialEnvForTest{}

// TestEmptySaltDisablesToken 空盐值下不得生成可穷举反解的伪标识。
//
// 这条守住的是"脱敏看起来生效、实则可逆"这一最危险的失效形态：
// 日志里满屏 [REDACTED:phone:a1b2c3d4] 会让运维确信安全，
// 而攻击者只需对手机号字典做一轮 sha256 就能全部还原。
func TestEmptySaltDisablesToken(t *testing.T) {
	policy := middleware.NewPolicy(middleware.SensitivityInternal, "")
	out, stats := policy.Redact("contact 13800138000")
	if !stats.HitAny() {
		t.Fatal("expected the phone rule to hit")
	}
	if strings.Contains(out, "13800138000") {
		t.Errorf("value not masked: %q", out)
	}
	// 空盐值时应降级为纯占位符，不带冒号分隔的伪标识段。
	if strings.Count(out, ":") > 1 {
		t.Errorf("empty salt still produced a tokenized placeholder: %q", out)
	}
}
