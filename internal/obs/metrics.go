// 本文件定义 Runtime 的指标层。
//
// 为什么单独一层而不是在各包里直接 metric.Int64Counter：
//  1. **收敛指标名与 label 集**。散落各处的 Int64Counter 很容易出现
//     "同一个语义两个名字"（node.duration / node_duration / duration），
//     PromQL 写完才发现要对两个序列求和；更难发现的是同一个名字两种 unit。
//  2. **守住基数边界**。run_id / node_id 是每个人第一个想到要加的 label，
//     但它们随时间无上界增长 —— Prometheus 里每对标签组合都是独立时间序列，
//     一次长跑就能撑爆内存。因为字段私有、只能走方法，
//     "哪些维度允许进 label"就只剩这一个文件能改。
//     允许的集合：tenant / node.type / tool / model / status / reason / outcome，
//     均为有限集合（tenant 以租户数为上界）。
//  3. **单测友好**。未调用 obs.Init 时全部退化为 no-op 空操作，
//     被测代码里的埋点不需要任何条件判断或开关。
package obs

import (
	"context"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
)

const instrumentationName = "agent-runtime"

// noopMeter 提供 no-op instrument，仅在 instrument 构造失败时用于降级。
var noopMeter = noop.NewMeterProvider().Meter(instrumentationName)

// meter 返回一个**永不为 nil** 的 Meter。
//
// obs.Init 未被调用时（单元测试、一次性 CLI）Meter 为零值 nil，
// 此时退回全局 MeterProvider —— 未注册过 provider 时它是 no-op 实现，
// 所有 Add / Record 都是安全的空操作。
// 这样调用方不必到处写 `if obs.Meter != nil`：那种判断漏写一处就是 nil panic，
// 而 panic 恰恰会发生在最不该失败的地方（埋点代码把业务打挂）。
func meter() metric.Meter {
	if Meter != nil {
		return Meter
	}
	return otel.GetMeterProvider().Meter(instrumentationName)
}

// RuntimeMetrics 是 Runtime 全部业务指标。
// 所有字段私有，外部一律通过方法上报 —— 防止有人绕过方法直接 Add 而丢掉维度约定。
type RuntimeMetrics struct {
	runsStarted   metric.Int64Counter
	runsSettled   metric.Int64Counter
	runErrors     metric.Int64Counter
	nodesClaimed  metric.Int64Counter
	nodesSettled  metric.Int64Counter
	nodeRetries   metric.Int64Counter
	nodeDuration  metric.Float64Histogram
	dlqEnqueued   metric.Int64Counter
	toolCalls     metric.Int64Counter
	toolRefused   metric.Int64Counter
	toolDuration  metric.Float64Histogram
	llmTokens     metric.Int64Counter
	llmRequests   metric.Int64Counter
	llmDuration   metric.Float64Histogram
	hitlRequested metric.Int64Counter
	eventsHandled metric.Int64Counter
}

var (
	runtimeOnce sync.Once
	runtimeM    *RuntimeMetrics
)

// Runtime 返回全局指标集（懒构造、并发安全）。
func Runtime() *RuntimeMetrics {
	runtimeOnce.Do(func() { runtimeM = newRuntimeMetrics(meter()) })
	return runtimeM
}

// newRuntimeMetrics 构造全部 instrument。
// 构造错误（如指标名非法）一律换成 no-op 而不是中断启动：
// 指标是增强项，不该因为命名问题让 worker 起不来 ——
// 那会把可观测性变成可用性依赖项，本末倒置。错误经日志暴露，肉眼可见。
func newRuntimeMetrics(m metric.Meter) *RuntimeMetrics {
	return &RuntimeMetrics{
		runsStarted:   counter(m, "runs.started", "Created runs", "1"),
		runsSettled:   counter(m, "runs.settled", "Runs converged to a terminal state", "1"),
		runErrors:     counter(m, "run.errors", "Run creation failures by reason", "1"),
		nodesClaimed:  counter(m, "nodes.claimed", "Nodes claimed by workers", "1"),
		nodesSettled:  counter(m, "nodes.settled", "Nodes reached a terminal state", "1"),
		nodeRetries:   counter(m, "node.retries", "Node retries scheduled", "1"),
		nodeDuration:  histogram(m, "node.duration", "Node execution duration", "s"),
		dlqEnqueued:   counter(m, "dlq.enqueued", "Nodes sent to the dead-letter queue", "1"),
		toolCalls:     counter(m, "tool.calls", "Tool invocations", "1"),
		toolRefused:   counter(m, "tool.calls.refused", "Tool calls refused re-execution", "1"),
		toolDuration:  histogram(m, "tool.duration", "Tool execution duration", "s"),
		llmTokens:     counter(m, "llm.tokens", "LLM token consumption", "{token}"),
		llmRequests:   counter(m, "llm.requests", "LLM requests by outcome", "1"),
		llmDuration:   histogram(m, "llm.duration", "LLM request duration", "s"),
		hitlRequested: counter(m, "hitl.requested", "Human approval requests", "1"),
		eventsHandled: counter(m, "events.handled", "Domain events handled by the resumer", "1"),
	}
}

func counter(m metric.Meter, name, desc, unit string) metric.Int64Counter {
	c, err := m.Int64Counter(name, metric.WithDescription(desc), metric.WithUnit(unit))
	if err != nil {
		warnNoop(name, err)
		c, _ = noopMeter.Int64Counter(name)
		return c
	}
	return c
}

func histogram(m metric.Meter, name, desc, unit string) metric.Float64Histogram {
	h, err := m.Float64Histogram(name, metric.WithDescription(desc), metric.WithUnit(unit))
	if err != nil {
		warnNoop(name, err)
		h, _ = noopMeter.Float64Histogram(name)
		return h
	}
	return h
}

// warnNoop 记录一次"指标名非法 → 降级为 no-op"。
func warnNoop(name string, err error) {
	From(context.Background()).Warn("metrics: invalid instrument name; falling back to no-op",
		"name", name, "error", err)
}

func attrOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

// RunStarted 记录一次 Run 创建成功。
func (r *RuntimeMetrics) RunStarted(ctx context.Context, t string) {
	r.runsStarted.Add(ctx, 1, metric.WithAttributes(attribute.String("tenant", attrOr(t, "unknown"))))
}

// RunSettled 记录 Run 收敛到终态（SUCCESS / FAILED / CANCELLED）。
func (r *RuntimeMetrics) RunSettled(ctx context.Context, t, status string) {
	r.runsSettled.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("status", attrOr(status, "unknown")),
	))
}

// RunError 记录 Run 创建失败，reason 为有限类别：auth / plan / version_conflict。
func (r *RuntimeMetrics) RunError(ctx context.Context, t, reason string) {
	r.runErrors.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("reason", attrOr(reason, "unknown")),
	))
}

// NodeClaimed 记录 worker 成功认领节点。
func (r *RuntimeMetrics) NodeClaimed(ctx context.Context, t, nodeType string) {
	r.nodesClaimed.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("node.type", attrOr(nodeType, "unknown")),
	))
}

// NodeSettled 记录节点到达终态。
func (r *RuntimeMetrics) NodeSettled(ctx context.Context, t, nodeType, status string) {
	r.nodesSettled.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("node.type", attrOr(nodeType, "unknown")),
		attribute.String("status", attrOr(status, "unknown")),
	))
}

// NodeDuration 记录节点执行耗时（秒）。
func (r *RuntimeMetrics) NodeDuration(ctx context.Context, t, nodeType string, seconds float64) {
	r.nodeDuration.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("node.type", attrOr(nodeType, "unknown")),
	))
}

// NodeRetry 记录一次重试编排。
func (r *RuntimeMetrics) NodeRetry(ctx context.Context, t, nodeType string) {
	r.nodeRetries.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("node.type", attrOr(nodeType, "unknown")),
	))
}

// DLQ 记录节点进入死信队列，reason 为有限类别。
// 这是最该配告警的指标之一：正常流量下它应当恒为 0，任何增量都需要人看。
func (r *RuntimeMetrics) DLQ(ctx context.Context, t, reason string) {
	r.dlqEnqueued.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("reason", attrOr(reason, "unknown")),
	))
}

// ToolCall 记录一次工具调用及其结果。
func (r *RuntimeMetrics) ToolCall(ctx context.Context, t, tool, outcome string) {
	r.toolCalls.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("tool", attrOr(tool, "unknown")),
		attribute.String("outcome", attrOr(outcome, "unknown")),
	))
}

// ToolRefused 记录工具调用被拒绝重执行（RUNNING / UNKNOWN）。
//
// 这是本次埋点里运维价值最高的一个：它上升意味着有节点的副作用状态卡在未知，
// 而且**不会自愈** —— 这些调用只能靠人工确认或下游幂等查询解决，
// 没有别的机制会替你处理，也没有任何日志比它更能提前暴露这个问题。
func (r *RuntimeMetrics) ToolRefused(ctx context.Context, t, status string) {
	r.toolRefused.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("status", attrOr(status, "unknown")),
	))
}

// ToolDuration 记录工具执行耗时（秒）。
func (r *RuntimeMetrics) ToolDuration(ctx context.Context, t, tool string, seconds float64) {
	r.toolDuration.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("tool", attrOr(tool, "unknown")),
	))
}

// LLMTokens 记录 token 消耗，按方向分别累计。
func (r *RuntimeMetrics) LLMTokens(ctx context.Context, t, model, direction string, tokens int64) {
	if tokens <= 0 {
		return
	}
	r.llmTokens.Add(ctx, tokens, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("model", attrOr(model, "unknown")),
		attribute.String("direction", attrOr(direction, "unknown")),
	))
}

// LLMRequest 记录一次 LLM 请求结果（outcome: ok / error）。
func (r *RuntimeMetrics) LLMRequest(ctx context.Context, t, model, outcome string) {
	r.llmRequests.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("model", attrOr(model, "unknown")),
		attribute.String("outcome", attrOr(outcome, "unknown")),
	))
}

// LLMDuration 记录 LLM 请求耗时（秒）。
func (r *RuntimeMetrics) LLMDuration(ctx context.Context, t, model string, seconds float64) {
	r.llmDuration.Record(ctx, seconds, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("model", attrOr(model, "unknown")),
	))
}

// HITL 记录一次转人工请求，source 区分来源（policy / guard）。
func (r *RuntimeMetrics) HITL(ctx context.Context, t, source string) {
	r.hitlRequested.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("source", attrOr(source, "unknown")),
	))
}

// EventHandled 记录 resumer 处理的领域事件。
func (r *RuntimeMetrics) EventHandled(ctx context.Context, t, eventType, outcome string) {
	r.eventsHandled.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tenant", attrOr(t, "unknown")),
		attribute.String("event.type", attrOr(eventType, "unknown")),
		attribute.String("outcome", attrOr(outcome, "unknown")),
	))
}
