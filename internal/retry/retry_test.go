package retry

import (
	"testing"
	"time"
)

// TestBackoff_ExponentialGrowth 验证无抖动时退避按 Factor 指数增长，并封顶 Max。
func TestBackoff_ExponentialGrowth(t *testing.T) {
	p := Policy{MaxAttempts: 5, Initial: 1 * time.Second, Factor: 2, Max: 10 * time.Second, Jitter: 0}
	want := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 10 * time.Second}
	for i, w := range want {
		got := p.Backoff(i + 1)
		if got != w {
			t.Errorf("attempt %d: want %v, got %v", i+1, w, got)
		}
	}
}

// TestBackoff_RespectsMinAttempt 小于 1 的 attempt 按 1 处理。
func TestBackoff_RespectsMinAttempt(t *testing.T) {
	p := Policy{MaxAttempts: 1, Initial: 500 * time.Millisecond, Factor: 2, Max: 10 * time.Second, Jitter: 0}
	if p.Backoff(0) != 500*time.Millisecond {
		t.Errorf("attempt 0 should be treated as 1")
	}
	if p.Backoff(-3) != 500*time.Millisecond {
		t.Errorf("negative attempt should be treated as 1")
	}
}

// TestBackoff_JitterStaysBounded 抖动后时长仍落在 [base - j/2*base, base + j/2*base] 且不超 Max。
func TestBackoff_JitterStaysBounded(t *testing.T) {
	p := Policy{MaxAttempts: 4, Initial: 1 * time.Second, Factor: 2, Max: 10 * time.Second, Jitter: 0.2}
	base := 2 * time.Second
	lo := time.Duration(float64(base) * (1 - 0.1))
	hi := time.Duration(float64(base) * (1 + 0.1))
	for i := 0; i < 200; i++ {
		got := p.Backoff(2)
		if got < lo || got > hi {
			t.Errorf("jitter out of bounds: %v (want %v~%v)", got, lo, hi)
		}
	}
}

// TestShouldRetry_Boundary 验证 MaxAttempts 边界：第 MaxAttempts 次仍可重试，+1 次耗尽。
func TestShouldRetry_Boundary(t *testing.T) {
	p := Policy{MaxAttempts: 3}
	if !p.ShouldRetry(1) || !p.ShouldRetry(2) || !p.ShouldRetry(3) {
		t.Errorf("attempts 1..3 should be retryable")
	}
	if p.ShouldRetry(4) {
		t.Errorf("attempt 4 should be exhausted (DLQ)")
	}
}

// TestDefault_ProducesSensibleValues Default 策略的基本合理性。
func TestDefault_ProducesSensibleValues(t *testing.T) {
	d := Default()
	if d.MaxAttempts < 1 || d.Initial <= 0 || d.Factor < 1 || d.Max <= 0 {
		t.Errorf("default policy invalid: %+v", d)
	}
}
