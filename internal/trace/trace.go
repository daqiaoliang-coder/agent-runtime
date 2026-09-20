// Package trace 保留为可观测性入口的兼容封装，实际实现已下沉到 internal/obs。
//
// 迁移说明：
//   - 历史上 trace 包只封装了 OTel TracerProvider；
//   - P0 阶段将 Tracer / Meter / Logger 统一到 internal/obs 初始化（共享 Resource、
//     OTLP 或 stdout exporter），本包改为薄封装，保持 11 处现有调用零改动。
//
// 新代码应直接使用 internal/obs（obs.From(ctx) 记录日志、obs.Meter 创建指标），
// trace 包仅保留 StartSpan / Tracer / Init 三个符号用于向后兼容。
package trace

import (
	"agent-runtime/internal/obs"
	"context"

	"go.opentelemetry.io/otel/trace"
)

// Tracer 与 obs.Tracer 保持同步，供直接引用 trace.Tracer 的调用方使用。
var Tracer trace.Tracer

// Init 委托给 obs.Init，后者同时初始化 Tracer / Meter / Logger 三个 Provider。
// 返回的 shutdown 函数会 flush 全部三类遥测数据。
func Init(serviceName string) (func(context.Context) error, error) {
	shutdown, err := obs.Init(serviceName)
	if err != nil {
		return nil, err
	}
	Tracer = obs.Tracer
	return shutdown, nil
}

// StartSpan 从 ctx 创建子 span。未初始化时返回 ctx 中现有 span（no-op）。
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if obs.Tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return obs.Tracer.Start(ctx, name, opts...)
}
