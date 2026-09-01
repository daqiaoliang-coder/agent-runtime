package worker

import (
	"testing"
)

// TestDefaultPricer_KnownModel 已登记模型按 prompt/completion 单价计费。
func TestDefaultPricer_KnownModel(t *testing.T) {
	// gpt-4o: prompt $2.5/M, completion $10/M
	// 200 prompt + 100 completion = 200/1e6*2.5 + 100/1e6*10 = 0.0005 + 0.001 = 0.0015
	cost := DefaultPricer("gpt-4o", 200, 100)
	if cost < 0.00149 || cost > 0.00151 {
		t.Errorf("gpt-4o cost = %v, want ~0.0015", cost)
	}
}

// TestDefaultPricer_UnknownModel 未登记模型 cost 记 0（仅追踪 token）。
func TestDefaultPricer_UnknownModel(t *testing.T) {
	cost := DefaultPricer("unknown-model", 500, 200)
	if cost != 0 {
		t.Errorf("unknown model cost = %v, want 0", cost)
	}
}

// TestDefaultPricer_GPT4oMini gpt-4o-mini 价格更低，验证不同模型单价正确。
func TestDefaultPricer_GPT4oMini(t *testing.T) {
	// gpt-4o-mini: prompt $0.15/M, completion $0.6/M
	// 1M prompt + 0 completion = 0.15
	cost := DefaultPricer("gpt-4o-mini", 1_000_000, 0)
	if cost < 0.149 || cost > 0.151 {
		t.Errorf("gpt-4o-mini cost = %v, want ~0.15", cost)
	}
}
