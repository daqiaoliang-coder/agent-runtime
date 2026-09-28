package worker

import (
	"testing"
)

// 接线 smoke：瀑布关闭时 gate 必须维持休眠（Policy 为 nil——此前
// NewFromEnv 从未装配 Policy，默认挂 CommandPolicy 会激活休眠的 gate
// 改变默认部署行为）；开启时 Policy 与 Approval 必须同时就位——
// Approval 缺席的话，L5 默认送审（瀑布最常见的终局）会在 Handle 里
// 报错并按基础设施失败重试直至 DLQ，而不是转人工。
func TestNewFromEnvPolicyWiring(t *testing.T) {
	t.Run("waterfall disabled keeps gate dormant", func(t *testing.T) {
		w := NewFromEnv(nil, nil, nil)
		if w.Policy != nil {
			t.Fatalf("Policy must stay nil when waterfall disabled (dormant gate), got %T", w.Policy)
		}
		if w.Approval != nil {
			t.Fatalf("Approval must stay nil when waterfall disabled, got %T", w.Approval)
		}
	})

	t.Run("waterfall enabled wires policy and approval together", func(t *testing.T) {
		t.Setenv("PERMISSION_WATERFALL_ENABLED", "true")
		w := NewFromEnv(nil, nil, nil)
		if w.Policy == nil {
			t.Fatal("Policy must be wired when waterfall enabled")
		}
		if w.Approval == nil {
			t.Fatal("Approval must be wired when waterfall enabled; otherwise require_approval dead-ends into error-retry")
		}
	})
}
