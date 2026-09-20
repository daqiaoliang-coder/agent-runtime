// Package obs 是 OpenTelemetry 三支柱（Traces / Metrics / Logs）的统一初始化入口。
//
// 设计目标：
//   - 三个 Provider 共享同一个 Resource（service.name / service.version /
//     deployment.environment），保证 trace、metric、log 在后端可按同一组维度关联；
//   - 初始化失败或未配置 OTLP 端点时降级为 stdout，不阻断进程启动（可观测性是增强项，
//     不能让它成为启动前置依赖）；
//   - OTEL_DISABLED=1 时全部退化为 no-op，供单元测试使用。
//
// Exporter 选择策略：
//   - 设置 OTEL_EXPORTER_OTLP_ENDPOINT 时，trace/metric/log 均走 OTLP gRPC
//     （端口遵循 OTel 约定：endpoint 中的端口即 gRPC 端口，4317）；
//   - 未设置时，三者均降级为 stdout exporter，保持本地开发零配置可见。
//
// 调用方在进程退出前必须调用返回的 ShutdownFunc，以 flush 缓冲的 span/metric/log。
package obs

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	// semconv 版本必须与 otel/sdk 对齐：当前 otel/sdk v1.46.0 对应 semconv v1.43.0。
	// 二者 Schema URL 冲突会导致 resource.Merge 失败并让全链路静默退化，
	// 升级 otel/sdk 时必须同步核对此导入（参考 internal/trace/trace.go 的注释）。
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
	"go.opentelemetry.io/otel/trace"
)

// 全局共享句柄，各组件通过它们创建 span / meter instrument / 记录日志。
// 未调用 Init 时为 nil，对应使用方需做空值降级（见 trace.StartSpan / obs.From）。
var (
	Tracer trace.Tracer
	Meter  metric.Meter
)

// ShutdownFunc 关闭全部 Provider 并 flush 缓冲数据，返回聚合的关闭错误。
type ShutdownFunc func(context.Context) error

// otlpEndpoint 返回 OTLP gRPC 端点。优先读取 OTEL_EXPORTER_OTLP_ENDPOINT，
// 缺省回退到 OTEL_EXPORTER_OTLP_TRACES_ENDPOINT（trace 专用），
// 再缺省回退到 localhost:4317（本地 docker-compose 默认）。
// 空字符串表示未配置 OTLP，调用方据此降级为 stdout。
func otlpEndpoint() string {
	if ep := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); ep != "" {
		return ep
	}
	if ep := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"); ep != "" {
		return ep
	}
	return "localhost:4317"
}

// useOTLP 判断是否启用 OTLP exporter。
// 仅当显式设置 OTEL_EXPORTER_OTLP_ENDPOINT（或 traces 专用端点）时启用，
// 避免本地开发因 Collector 未启动而产生大量连接重试日志。
func useOTLP() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" ||
		os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != ""
}

// buildResource 构造跨三支柱共享的 Resource。
// service.name 由调用方传入；service.version / deployment.environment 从环境变量读取，
// 缺省分别为 "unknown" 和 "development"。这些字段是后端按版本/环境分组关联的关键。
func buildResource(serviceName string) (*sdkresource.Resource, error) {
	version := os.Getenv("OTEL_SERVICE_VERSION")
	if version == "" {
		version = "unknown"
	}
	env := os.Getenv("OTEL_ENVIRONMENT")
	if env == "" {
		env = os.Getenv("DEPLOYMENT_ENVIRONMENT")
	}
	if env == "" {
		env = "development"
	}
	return sdkresource.Merge(
		sdkresource.Default(),
		sdkresource.NewWithAttributes(semconv.SchemaURL,
			semconv.ServiceName(serviceName),
			semconv.ServiceVersion(version),
			attribute.String("deployment.environment", env),
		),
	)
}

// Init 初始化并注册全局 TracerProvider / MeterProvider / LoggerProvider。
// 三者共享同一 Resource，保证遥测数据可按 service/version/env 关联。
// 返回的 ShutdownFunc 应在进程退出前调用以 flush 缓冲数据。
func Init(serviceName string) (ShutdownFunc, error) {
	if os.Getenv("OTEL_DISABLED") == "1" {
		Tracer = otel.GetTracerProvider().Tracer("agent-runtime")
		Meter = otel.GetMeterProvider().Meter("agent-runtime")
		return func(context.Context) error { return nil }, nil
	}

	res, err := buildResource(serviceName)
	if err != nil {
		return nil, fmt.Errorf("obs: build resource: %w", err)
	}

	tp, err := newTracerProvider(res)
	if err != nil {
		return nil, fmt.Errorf("obs: tracer provider: %w", err)
	}
	mp, err := newMeterProvider(res)
	if err != nil {
		_ = tp.Shutdown(context.Background())
		return nil, fmt.Errorf("obs: meter provider: %w", err)
	}
	lp, err := newLoggerProvider(res)
	if err != nil {
		_ = tp.Shutdown(context.Background())
		_ = mp.Shutdown(context.Background())
		return nil, fmt.Errorf("obs: logger provider: %w", err)
	}

	otel.SetTracerProvider(tp)
	otel.SetMeterProvider(mp)
	global.SetLoggerProvider(lp)
	// 注册 W3C TraceContext + Baggage 传播器，使 traceparent 能随消息跨进程传递。
	// 未注册时全局 propagator 为 no-op，队列/消息边界的 trace 会断链。
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{},
	))

	Tracer = tp.Tracer("agent-runtime")
	Meter = mp.Meter("agent-runtime")
	// 初始化 slog，使其同时输出到控制台（带 trace_id）并经 OTLP 发往 Collector。
	// 必须在 LoggerProvider 注册后调用，otelslog bridge 才能取到 provider。
	initSlog(lp)

	return func(ctx context.Context) error {
		var errs []error
		// ForceFlush 确保 shutdown 前把批次中的 span/metric/log 全部推出去，
		// 避免短生命周期进程（runtime/resume/cancel）的遥测丢失。
		if err := tp.ForceFlush(ctx); err != nil {
			errs = append(errs, fmt.Errorf("trace flush: %w", err))
		}
		if err := tp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("trace shutdown: %w", err))
		}
		if err := mp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("metric shutdown: %w", err))
		}
		if err := lp.Shutdown(ctx); err != nil {
			errs = append(errs, fmt.Errorf("log shutdown: %w", err))
		}
		return errors.Join(errs...)
	}, nil
}

// newTracerProvider 构造 TracerProvider。
// OTLP 模式用 batch processor + gRPC exporter；stdout 模式用 pretty print 便于本地观察。
func newTracerProvider(res *sdkresource.Resource) (*sdktrace.TracerProvider, error) {
	opts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}
	if useOTLP() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		exp, err := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(otlpEndpoint()),
			otlptracegrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("otlp trace exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	} else {
		exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
		if err != nil {
			return nil, fmt.Errorf("stdout trace exporter: %w", err)
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	return sdktrace.NewTracerProvider(opts...), nil
}

// newMeterProvider 构造 MeterProvider，使用周期导出（默认 60s）。
func newMeterProvider(res *sdkresource.Resource) (*sdkmetric.MeterProvider, error) {
	opts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	if useOTLP() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		exp, err := otlpmetricgrpc.New(ctx,
			otlpmetricgrpc.WithEndpoint(otlpEndpoint()),
			otlpmetricgrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("otlp metric exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	} else {
		exp, err := stdoutmetric.New()
		if err != nil {
			return nil, fmt.Errorf("stdout metric exporter: %w", err)
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)))
	}
	return sdkmetric.NewMeterProvider(opts...), nil
}

// newLoggerProvider 构造 LoggerProvider，使用 batch processor。
func newLoggerProvider(res *sdkresource.Resource) (*sdklog.LoggerProvider, error) {
	opts := []sdklog.LoggerProviderOption{sdklog.WithResource(res)}
	if useOTLP() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		exp, err := otlploggrpc.New(ctx,
			otlploggrpc.WithEndpoint(otlpEndpoint()),
			otlploggrpc.WithInsecure(),
		)
		if err != nil {
			return nil, fmt.Errorf("otlp log exporter: %w", err)
		}
		opts = append(opts, sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	} else {
		exp, err := stdoutlog.New()
		if err != nil {
			return nil, fmt.Errorf("stdout log exporter: %w", err)
		}
		opts = append(opts, sdklog.WithProcessor(sdklog.NewBatchProcessor(exp)))
	}
	return sdklog.NewLoggerProvider(opts...), nil
}
