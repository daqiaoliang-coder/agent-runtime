package trace

import (
	"context"
	"testing"
)

// TestStartSpan_NoopWithoutInit 未调用 Init 时 StartSpan 返回 no-op span，不 panic。
func TestStartSpan_NoopWithoutInit(t *testing.T) {
	// 确保 Tracer 为 nil（测试环境未 Init）
	Tracer = nil
	_, span := StartSpan(context.Background(), "test.span")
	if span == nil {
		t.Fatal("span should not be nil even without Init")
	}
	span.End()
}

// TestInit_DisabledWithEnvVar OTEL_DISABLED=1 时跳过 exporter 初始化，返回 no-op。
func TestInit_DisabledWithEnvVar(t *testing.T) {
	t.Setenv("OTEL_DISABLED", "1")
	shutdown, err := Init("test-service")
	if err != nil {
		t.Fatalf("Init with OTEL_DISABLED should not error: %v", err)
	}
	if shutdown == nil {
		t.Fatal("shutdown should not be nil")
	}
	if err := shutdown(context.Background()); err != nil {
		t.Errorf("no-op shutdown should not error: %v", err)
	}
	// Init 后 Tracer 应非 nil（otel no-op tracer）
	if Tracer == nil {
		t.Error("Tracer should be set after Init")
	}
}
