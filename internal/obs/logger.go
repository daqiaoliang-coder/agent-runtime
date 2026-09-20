package obs

import (
	"context"
	"log/slog"
	"os"

	"go.opentelemetry.io/contrib/bridges/otelslog"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/trace"
)

// defaultLogger 是进程内默认 slog logger，默认输出到 stderr 的 JSON，
// 并通过 fanout 同时把日志经 otelslog bridge 发往 OTel LoggerProvider。
var defaultLogger *slog.Logger

// initSlog 构造默认 slog logger：
//   - 一路写到 stderr（JSON，携带 trace_id/span_id），供本地开发与容器标准输出采集；
//   - 一路经 otelslog bridge 发往 OTel LoggerProvider，供 Collector → Loki 管道采集。
//
// 两条路径都需要 trace 关联：traceHandler 负责从 ctx 提取 trace_id/span_id 注入 JSON；
// otelslog bridge 则会自动把 ctx 中的 span context 转成 OTel log record 的 TraceID/SpanID。
func initSlog(lp *sdklog.LoggerProvider) {
	consoleHandler := newTraceHandler(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
		// 把 slog 内置的 level/msg/time 字段统一命名，避免与业务字段冲突。
	}))
	otelHandler := otelslog.NewHandler("agent-runtime", otelslog.WithLoggerProvider(lp))
	defaultLogger = slog.New(&fanoutHandler{handlers: []slog.Handler{consoleHandler, otelHandler}})
	slog.SetDefault(defaultLogger)
}

// From 返回携带 ctx 中 trace 上下文的 logger。
// 业务代码统一用 obs.From(ctx).InfoContext(ctx, ...) 或 obs.From(ctx).Info(...)，
// 保证每条日志都带 trace_id/span_id，可在 Loki 中按 trace_id 下钻到 Tempo。
func From(ctx context.Context) *slog.Logger {
	if defaultLogger == nil {
		return slog.Default()
	}
	return defaultLogger
}

// fanoutHandler 将一条 slog 记录扇出到多个 handler，
// 用于同时输出到控制台 JSON 与 OTel 日志管道。
// 若任一 handler 返回错误，记录错误但不阻断其它 handler（可观测性降级不丢数据）。
type fanoutHandler struct {
	handlers []slog.Handler
}

func (h *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	for _, hh := range h.handlers {
		if hh.Enabled(ctx, level) {
			return true
		}
	}
	return false
}

func (h *fanoutHandler) Handle(ctx context.Context, r slog.Record) error {
	var firstErr error
	for _, hh := range h.handlers {
		if !hh.Enabled(ctx, r.Level) {
			continue
		}
		if err := hh.Handle(ctx, r); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (h *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, hh := range h.handlers {
		handlers[i] = hh.WithAttrs(attrs)
	}
	return &fanoutHandler{handlers: handlers}
}

func (h *fanoutHandler) WithGroup(name string) slog.Handler {
	handlers := make([]slog.Handler, len(h.handlers))
	for i, hh := range h.handlers {
		handlers[i] = hh.WithGroup(name)
	}
	return &fanoutHandler{handlers: handlers}
}

// traceHandler 在 JSON handler 之上注入 trace_id/span_id。
// 仅当 ctx 中存在有效 span 时才注入，避免普通日志多出空字段。
type traceHandler struct {
	inner slog.Handler
}

func newTraceHandler(inner slog.Handler) *traceHandler { return &traceHandler{inner: inner} }

func (h *traceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *traceHandler) Handle(ctx context.Context, r slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		r = r.Clone()
		r.AddAttrs(
			slog.String("trace_id", sc.TraceID().String()),
			slog.String("span_id", sc.SpanID().String()),
		)
	}
	return h.inner.Handle(ctx, r)
}

func (h *traceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &traceHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *traceHandler) WithGroup(name string) slog.Handler {
	return &traceHandler{inner: h.inner.WithGroup(name)}
}
