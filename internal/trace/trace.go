// Package trace 提供基于 OpenTelemetry 的轨迹追踪能力。
//
// 设计要点：
//   - Init 初始化全局 TracerProvider，默认使用 stdout exporter（演示），
//     生产中可替换为 OTLP / Jaeger exporter 将 span 发往远端 Collector；
//   - Tracer 为各组件共享的 tracer 句柄，用于在 worker / executor / llm /
//     store 各层创建 span，串联 Agent 执行的全链路轨迹；
//   - Span 属性携带 run_id / node_id / tenant_id / model / token 等维度，
//     便于按租户与 Run 维度检索完整执行轨迹。
package trace

import (
	"context"
	"fmt"
	"os"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// Tracer 是全局共享的 tracer 句柄，各组件通过 StartSpan 创建子 span。
// 未调用 Init 时为 nil，StartSpan 会退化为 no-op（不产生 span）。
var Tracer trace.Tracer

// Init 初始化 TracerProvider，设置 service.name 资源与 stdout batch exporter。
// 返回 shutdown 函数，调用方应在进程退出前调用以 flush 缓冲的 span。
// 当环境变量 OTEL_DISABLED=1 时跳过初始化（如单元测试），返回 no-op。
func Init(serviceName string) (func(context.Context) error, error) {
	if os.Getenv("OTEL_DISABLED") == "1" {
		Tracer = otel.GetTracerProvider().Tracer("agent-runtime")
		return func(context.Context) error { return nil }, nil
	}
	exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, fmt.Errorf("trace: stdout exporter: %w", err)
	}
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(serviceName),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("trace: resource: %w", err)
	}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	Tracer = tp.Tracer("agent-runtime")
	return tp.Shutdown, nil
}

// StartSpan 从 ctx 创建一个名为 name 的子 span，返回派生 ctx 与 span。
// 调用方通过 defer span.End() 结束 span，并通过 opts（如 trace.WithAttributes）写入维度属性。
// Tracer 未初始化时返回 no-op span（trace.SpanFromContext），保证非追踪场景零开销。
func StartSpan(ctx context.Context, name string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if Tracer == nil {
		return ctx, trace.SpanFromContext(ctx)
	}
	return Tracer.Start(ctx, name, opts...)
}
