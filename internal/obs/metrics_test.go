package obs

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// collectFrom 构造一个独立的指标集，上报后返回采集结果。
// 刻意不用全局 Runtime()：那是 sync.Once 构造的，一旦被别的用例先初始化，
// 这里就拿不到干净实例。埋点逻辑值得有自己的回归测试，不该依赖全局初始化顺序。
func collectFrom(t *testing.T, report func(*RuntimeMetrics, context.Context)) metricdata.ResourceMetrics {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m := newRuntimeMetrics(mp.Meter("agent-runtime-test"))
	report(m, context.Background())

	var out metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &out); err != nil {
		t.Fatalf("collect metrics: %v", err)
	}
	return out
}

// findMetric 按名字取出单个指标，未找到即失败。
func findMetric(t *testing.T, rm metricdata.ResourceMetrics, name string) metricdata.Metrics {
	t.Helper()
	for _, scope := range rm.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == name {
				return m
			}
		}
	}
	t.Fatalf("metric %q not found; got %d metrics total", name, countMetrics(rm))
	return metricdata.Metrics{}
}

func countMetrics(rm metricdata.ResourceMetrics) int {
	n := 0
	for _, scope := range rm.ScopeMetrics {
		n += len(scope.Metrics)
	}
	return n
}

// dataPointsOf 抹平 Sum / Histogram 的类型差异，便于统一遍历 attribute 集合。
func dataPointsOf(m metricdata.Metrics) []attribute.Set {
	switch d := m.Data.(type) {
	case metricdata.Sum[int64]:
		out := make([]attribute.Set, 0, len(d.DataPoints))
		for _, dp := range d.DataPoints {
			out = append(out, dp.Attributes)
		}
		return out
	case metricdata.Histogram[float64]:
		out := make([]attribute.Set, 0, len(d.DataPoints))
		for _, dp := range d.DataPoints {
			out = append(out, dp.Attributes)
		}
		return out
	}
	return nil
}

func TestRuntimeMetrics_CountersRecord(t *testing.T) {
	rm := collectFrom(t, func(m *RuntimeMetrics, ctx context.Context) {
		m.RunStarted(ctx, "tenant-a")
		m.RunStarted(ctx, "tenant-a")
		m.ToolRefused(ctx, "tenant-b", "UNKNOWN")
	})

	started := findMetric(t, rm, "runs.started")
	sum, ok := started.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("runs.started is %T, want Sum[int64]", started.Data)
	}
	if !sum.IsMonotonic {
		t.Error("runs.started must be monotonic")
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("expected 1 data point (same attributes aggregate), got %d", len(sum.DataPoints))
	}
	if got := sum.DataPoints[0].Value; got != 2 {
		t.Errorf("runs.started value = %d, want 2", got)
	}
	if v, ok := sum.DataPoints[0].Attributes.Value("tenant"); !ok || v.AsString() != "tenant-a" {
		t.Errorf("runs.started missing tenant attribute, got %v", sum.DataPoints[0].Attributes)
	}

	refused := findMetric(t, rm, "tool.calls.refused")
	rsum, ok := refused.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("tool.calls.refused is %T, want Sum[int64]", refused.Data)
	}
	if len(rsum.DataPoints) != 1 || rsum.DataPoints[0].Value != 1 {
		t.Errorf("tool.calls.refused = %+v, want single point of 1", rsum.DataPoints)
	}
}

func TestRuntimeMetrics_HistogramRecords(t *testing.T) {
	rm := collectFrom(t, func(m *RuntimeMetrics, ctx context.Context) {
		m.NodeDuration(ctx, "tenant-a", "LLM", 1.25)
		m.NodeDuration(ctx, "tenant-a", "LLM", 3.75)
	})
	h, ok := findMetric(t, rm, "node.duration").Data.(metricdata.Histogram[float64])
	if !ok {
		t.Fatalf("node.duration is %T, want Histogram[float64]", findMetric(t, rm, "node.duration").Data)
	}
	if len(h.DataPoints) != 1 {
		t.Fatalf("expected 1 histogram point, got %d", len(h.DataPoints))
	}
	if got := h.DataPoints[0].Count; got != 2 {
		t.Errorf("node.duration count = %d, want 2", got)
	}
	if got := h.DataPoints[0].Sum; got != 5.0 {
		t.Errorf("node.duration sum = %v, want 5.0", got)
	}
}

// TestRuntimeMetrics_NoHighCardinalityLabels 守护基数边界。
//
// run_id / node_id 一旦作为 label 进入 Prometheus，时间序列数量会随时间失去上界，
// 一次长跑就能撑爆收割方。这个用例的价值不在于证明"当前没写错"，
// 而在于让未来任何"方便起见加个 run_id"的改动在 CI 里就失败。
func TestRuntimeMetrics_NoHighCardinalityLabels(t *testing.T) {
	allowed := map[string]bool{
		"tenant": true, "node.type": true, "tool": true, "model": true,
		"status": true, "reason": true, "outcome": true, "direction": true,
		"event.type": true, "source": true,
	}
	forbidden := []string{"run_id", "run.id", "node_id", "node.id", "call_id", "trace_id"}

	rm := collectFrom(t, func(m *RuntimeMetrics, ctx context.Context) {
		m.RunStarted(ctx, "t")
		m.RunSettled(ctx, "t", "SUCCESS")
		m.RunError(ctx, "t", "plan")
		m.NodeClaimed(ctx, "t", "LLM")
		m.NodeSettled(ctx, "t", "TOOL", "success")
		m.NodeRetry(ctx, "t", "TOOL")
		m.NodeDuration(ctx, "t", "LLM", 0.5)
		m.DLQ(ctx, "t", "retries_exhausted")
		m.ToolCall(ctx, "t", "search", "success")
		m.ToolRefused(ctx, "t", "RUNNING")
		m.ToolDuration(ctx, "t", "search", 0.25)
		m.LLMTokens(ctx, "t", "gpt-4o-mini", "prompt", 128)
		m.LLMRequest(ctx, "t", "gpt-4o-mini", "ok")
		m.LLMDuration(ctx, "t", "gpt-4o-mini", 1.0)
		m.HITL(ctx, "t", "policy")
		m.EventHandled(ctx, "t", "AgentStepCompleted", "ok")
	})

	if countMetrics(rm) == 0 {
		t.Fatal("no metrics collected; reporting is silently broken")
	}
	for _, scope := range rm.ScopeMetrics {
		for _, metric := range scope.Metrics {
			for _, set := range dataPointsOf(metric) {
				for _, k := range forbidden {
					if _, ok := set.Value(attribute.Key(k)); ok {
						t.Errorf("metric %q carries forbidden high-cardinality label %q", metric.Name, k)
					}
				}
				for _, kv := range set.ToSlice() {
					if !allowed[string(kv.Key)] {
						t.Errorf("metric %q has unexpected label %q; only add to allowlist if bounded", metric.Name, kv.Key)
					}
				}
			}
		}
	}
}

// TestRuntimeMetrics_EmptyTenantLabeled 空租户必须落成 "unknown" 而非空字符串：
// 空 label 在 Prometheus 里同样占一个 series，只是对人不可读，
// 写 PromQL 时必须记得用 == "" 过滤，等于给自己埋坑。
func TestRuntimeMetrics_EmptyTenantLabeled(t *testing.T) {
	rm := collectFrom(t, func(m *RuntimeMetrics, ctx context.Context) { m.RunStarted(ctx, "") })
	sum, ok := findMetric(t, rm, "runs.started").Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("runs.started is not Sum[int64]")
	}
	v, ok := sum.DataPoints[0].Attributes.Value("tenant")
	if !ok || v.AsString() != "unknown" {
		t.Errorf("empty tenant should be labeled 'unknown', got %v", sum.DataPoints[0].Attributes)
	}
}

// TestMeter_NeverNil 覆盖降级路径：obs.Init 未调用（Meter 为 nil）时，
// 埋点必须安静地变成空操作而不是 panic。
// 这条保护的是最不该发生的事故形态 —— 埋点代码把业务流程打挂。
func TestMeter_NeverNil(t *testing.T) {
	saved := Meter
	Meter = nil
	t.Cleanup(func() { Meter = saved })

	if m := meter(); m == nil {
		t.Fatal("meter() must never return nil even without obs.Init")
	}
	if m := newRuntimeMetrics(meter()); m != nil {
		m.RunStarted(context.Background(), "t")
		m.NodeDuration(context.Background(), "t", "LLM", 1)
	}
}

func TestRuntime_LazyInit(t *testing.T) {
	a, b := Runtime(), Runtime()
	if a != b {
		t.Fatal("Runtime() must return the same instance")
	}
	a.RunStarted(context.Background(), "t")
}

// TestNewRuntimeMetrics_AllInstrumentsPresent 防止新增方法漏配 instrument：
// 漏配的字段是 nil，调用它的方法会 panic，而且只在那个分支首次被执行时 panic，
// 可能上线很久才暴露。
func TestNewRuntimeMetrics_AllInstrumentsPresent(t *testing.T) {
	rm := newRuntimeMetrics(sdkmetric.NewMeterProvider().Meter("test"))
	if rm.runsStarted == nil || rm.runsSettled == nil || rm.runErrors == nil ||
		rm.nodesClaimed == nil || rm.nodesSettled == nil || rm.nodeRetries == nil ||
		rm.nodeDuration == nil || rm.dlqEnqueued == nil || rm.toolCalls == nil ||
		rm.toolRefused == nil || rm.toolDuration == nil || rm.llmTokens == nil ||
		rm.llmRequests == nil || rm.llmDuration == nil || rm.hitlRequested == nil ||
		rm.eventsHandled == nil {
		t.Fatal("one or more instruments are nil")
	}
}
