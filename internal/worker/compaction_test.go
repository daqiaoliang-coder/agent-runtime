// Package worker 上下文压缩管线单测（docs/context-compaction.md）。
//
// 覆盖口径分三类：
//  1. 纯函数：档位收缩/水位线过滤/阈值计算/摘要解析/硬截断/前缀选择/回填塑形；
//  2. 降级链：Model 缺省时 L3/L4 逐层退到 L5，且任何一层失败都不阻断组装；
//  3. 端到端 Apply：fake store + fake model 走 none → L2 → L3 → L4 → L5 各出口，
//     验证水位线持久化、基座注入、事件口径与缓存换代。
//
// 端到端用例统一用 ModelWindow=8192（阈值被 minCompactionThreshold 钳到 1024
// token = 4096 rune），使"多少内容触发哪一层"可以用字符串长度精确构造。
package worker

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"context"
	"errors"
	"strings"
	"testing"

	oteltrace "go.opentelemetry.io/otel/trace"
)

// ==== 测试替身 ====

// fakeCompactionStore 是 compactionStore 的内存实现：记录插入与计数供断言。
type fakeCompactionStore struct {
	usage     *model.LLMUsage
	full      *model.RunCompaction
	micro     *model.RunCompaction
	inserted  []*model.RunCompaction
	fullCount int
	// conflict=true 时 InsertCompaction 模拟并发方先插入成功（返回 false）。
	conflict bool
}

func (f *fakeCompactionStore) LastLLMPromptUsage(context.Context, string, string) (model.LLMUsage, bool, error) {
	if f.usage == nil {
		return model.LLMUsage{}, false, nil
	}
	return *f.usage, true, nil
}

func (f *fakeCompactionStore) LatestCompaction(_ context.Context, _, _, kind string) (*model.RunCompaction, error) {
	if kind == CompactionKindFull {
		return f.full, nil
	}
	return f.micro, nil
}

func (f *fakeCompactionStore) InsertCompaction(_ context.Context, rec *model.RunCompaction) (bool, error) {
	if f.conflict {
		return false, nil
	}
	f.inserted = append(f.inserted, rec)
	if rec.Kind == CompactionKindFull {
		f.fullCount++
		f.full = rec
	} else {
		f.micro = rec
	}
	return true, nil
}

func (f *fakeCompactionStore) CountCompactions(context.Context, string, string, string) (int, error) {
	return f.fullCount, nil
}

// fakeSummaryModel 按最后一条 user 消息（摘要 prompt）回调响应内容。
type fakeSummaryModel struct {
	respond func(prompt string) string
}

func (f fakeSummaryModel) Generate(_ context.Context, req contracts.GenerateRequest) (contracts.GenerateResponse, error) {
	if len(req.Messages) == 0 || f.respond == nil {
		return contracts.GenerateResponse{}, errors.New("fake model: no prompt")
	}
	return contracts.GenerateResponse{
		Message: contracts.Message{Role: contracts.RoleAssistant, Content: f.respond(req.Messages[len(req.Messages)-1].Content)},
		Model:   "fake-summary",
		Usage:   contracts.Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
	}, nil
}

func (f fakeSummaryModel) Stream(context.Context, contracts.GenerateRequest) (<-chan contracts.ModelEvent, error) {
	return nil, errors.New("fake model: stream not implemented")
}

// fakeEventSink 收集压缩事件，供断言事件口径（kind/水位线，绝无摘要原文）。
type fakeEventSink struct{ events []contracts.RuntimeEvent }

func (f *fakeEventSink) Emit(_ context.Context, ev contracts.RuntimeEvent) error {
	f.events = append(f.events, ev)
	return nil
}

// tightOpt 返回阈值钳到 1024 token 的配置：tMicro == tAuto == 1024。
func tightOpt() CompactionOptions { return CompactionOptions{ModelWindow: 8192} }

func testEC() contracts.ExecutionContext {
	return contracts.ExecutionContext{TenantID: "tenant-t", RunID: "run-1", NodeID: "node-cur"}
}

// eventData 取回事件 Data（map 形态），断言失败即终止。
func eventData(t *testing.T, ev contracts.RuntimeEvent) map[string]any {
	t.Helper()
	m, ok := ev.Data.(map[string]any)
	if !ok {
		t.Fatalf("event data must be a map, got %T", ev.Data)
	}
	return m
}

// ==== 纯函数 ====

// TestShrinkKeep 锁定 L2 收缩档位：稀疏档位是刻意设计（收益来自遮蔽整段工具结果）。
func TestShrinkKeep(t *testing.T) {
	for in, want := range map[int]int{6: 4, 5: 4, 4: 2, 3: 2, 2: 0, 1: 0, 0: 0} {
		if got := shrinkKeep(in); got != want {
			t.Errorf("shrinkKeep(%d) = %d, want %d", in, got, want)
		}
	}
}

// TestAfterWaterline 水位线过滤：命中取后缀；未命中/空水位线 fail-open 全量原文。
func TestAfterWaterline(t *testing.T) {
	nodes := []model.Node{llmNode("n1", "i1", "o1"), llmNode("n2", "i2", "o2"), llmNode("n3", "i3", "o3")}
	if got := afterWaterline(nodes, ""); len(got) != 3 {
		t.Errorf("empty waterline must keep all, got %d", len(got))
	}
	got := afterWaterline(nodes, "n1")
	if len(got) != 2 || got[0].ID != "n2" || got[1].ID != "n3" {
		t.Errorf("waterline n1 must yield [n2 n3], got %+v", idsOf(got))
	}
	if got := afterWaterline(nodes, "missing"); len(got) != 3 {
		t.Errorf("unknown waterline must fail open to full history, got %d", len(got))
	}
}

func idsOf(nodes []model.Node) []string {
	out := make([]string, 0, len(nodes))
	for _, n := range nodes {
		out = append(out, n.ID)
	}
	return out
}

// TestCompactionThresholds 阈值公式 T = floor(window×ratio) − reserve 的关键不变量：
// 默认值、按模型覆盖、micro 不得高于 auto（写反时钳位）、最小下限。
func TestCompactionThresholds(t *testing.T) {
	c := &Compactor{Opt: CompactionOptions{}}
	tMicro, tAuto := c.thresholds("")
	// 0.8×32768−8192 = 18022；0.92×32768−8192 = 21954
	if tMicro != 18022 || tAuto != 21954 {
		t.Errorf("default thresholds = (%d,%d), want (18022,21954)", tMicro, tAuto)
	}
	c.Opt.ModelWindows = map[string]int{"GPT4O": 128000}
	tMicro, tAuto = c.thresholds("gpt-4o")
	if tMicro != 94208 || tAuto != 109568 {
		t.Errorf("gpt-4o thresholds = (%d,%d), want (94208,109568)", tMicro, tAuto)
	}
	// 配置写反（micro 比例高于 auto）：钳到危险线，宁可早进入全量压缩。
	c2 := &Compactor{Opt: CompactionOptions{MicroRatio: 0.99, AutoRatio: 0.5, ModelWindow: 100000}}
	tMicro, tAuto = c2.thresholds("")
	if tMicro != tAuto {
		t.Errorf("micro must be clamped to auto when misconfigured: (%d,%d)", tMicro, tAuto)
	}
	// 窗口过小：下限防止负/零预算让管线空转。
	c3 := &Compactor{Opt: CompactionOptions{ModelWindow: 2048}}
	tMicro, tAuto = c3.thresholds("")
	if tMicro < minCompactionThreshold || tAuto < minCompactionThreshold {
		t.Errorf("thresholds below floor: (%d,%d)", tMicro, tAuto)
	}
}

// TestModelWindowsFromEnv_Roundtrip 环境变量键与 modelWindowKey 必须互逆：
// CONTEXT_MODEL_WINDOW_<NAME> 能被 windowFor("<name>") 命中（分隔符删除后大写）。
func TestModelWindowsFromEnv_Roundtrip(t *testing.T) {
	t.Setenv("CONTEXT_MODEL_WINDOW_GPT4O", "128000")
	t.Setenv("CONTEXT_MODEL_WINDOW_CLAUDE35SONNET", "200000")
	c := &Compactor{Opt: CompactionOptions{ModelWindows: modelWindowsFromEnv()}}
	if got := c.Opt.windowFor("gpt-4o"); got != 128000 {
		t.Errorf("windowFor(gpt-4o) = %d, want 128000", got)
	}
	if got := c.Opt.windowFor("claude-3.5-sonnet"); got != 200000 {
		t.Errorf("windowFor(claude-3.5-sonnet) = %d, want 200000", got)
	}
	if got := c.Opt.windowFor("unknown-model"); got != DefaultCompactionModelWindow {
		t.Errorf("unknown model must fall back to default window, got %d", got)
	}
}

// TestHardTruncate L5 兜底语义：保尾部、system 永不丢、丢弃处插指针；预算内原样返回。
func TestHardTruncate(t *testing.T) {
	// 预算内：不得插入指针、不得改写消息。
	small := []llm.Message{{Role: llm.RoleUser, Content: "hi"}}
	got := hardTruncate(small, 1024, 4, "run-1")
	if len(got) != 1 || got[0].Content != "hi" {
		t.Errorf("within-budget input must pass through, got %+v", got)
	}
	// 超预算：最老消息被丢、system 保留、指针在保留段头部。
	msgs := []llm.Message{
		{Role: llm.RoleSystem, Content: strings.Repeat("s", 100)},          // 25 token，永不丢
		{Role: llm.RoleUser, Content: strings.Repeat("a", 40000)},          // 10000 token，必丢
		{Role: llm.RoleAssistant, Content: strings.Repeat("b", 200)},       // 50 token，保留
	}
	got = hardTruncate(msgs, 200, 4, "run-9")
	if len(got) != 3 {
		t.Fatalf("expected [ptr system kept], got %+v", got)
	}
	if !strings.Contains(got[0].Content, "[Dropped 1 old message(s)") || !strings.Contains(got[0].Content, "run-9") {
		t.Errorf("drop pointer missing/malformed: %q", got[0].Content)
	}
	if got[1].Role != llm.RoleSystem || got[1].Content != strings.Repeat("s", 100) {
		t.Errorf("system message must never be dropped: %+v", got[1])
	}
	if got[2].Content != strings.Repeat("b", 200) {
		t.Errorf("recent message must be kept verbatim: %+v", got[2])
	}
}

// TestParseSummaryMap L3 输出解析的容错口径：围栏/前后噪声可解，垃圾与空对象拒绝。
func TestParseSummaryMap(t *testing.T) {
	m, err := parseSummaryMap(`{"n1":"did a","n2":"did b"}`)
	if err != nil || m["n1"] != "did a" {
		t.Fatalf("plain json must parse: %v %+v", err, m)
	}
	m, err = parseSummaryMap("noise before ```json\n{\"n1\":\"x\"}\n``` after")
	if err != nil || m["n1"] != "x" {
		t.Fatalf("fenced json must parse: %v %+v", err, m)
	}
	if _, err = parseSummaryMap("not json at all"); err == nil {
		t.Error("garbage must be rejected")
	}
	if _, err = parseSummaryMap("{}"); err == nil {
		t.Error("empty map must be rejected (nothing backfilled)")
	}
}

// TestMicroPrefix 前缀选择：保留区以最后 keep 个 TOOL 中最早者为界；
// keep=0 时全部可摘要；已回填节点跳过；工具数不足窗口时无前缀。
func TestMicroPrefix(t *testing.T) {
	active := []model.Node{
		toolNode("t1", "search", "o1"),
		llmNode("l1", "i1", "o1"),
		toolNode("t2", "search", "o2"),
		toolNode("t3", "search", "o3"),
	}
	if got := microPrefix(active, nil, 2); len(got) != 2 || got[0].ID != "t1" || got[1].ID != "l1" {
		t.Errorf("keep=2 must summarize [t1 l1], got %+v", idsOf(got))
	}
	if got := microPrefix(active, nil, 1); len(got) != 3 || got[2].ID != "t2" {
		t.Errorf("keep=1 must summarize up to t2, got %+v", idsOf(got))
	}
	if got := microPrefix(active, nil, 0); len(got) != 4 {
		t.Errorf("keep=0 (L2 terminal) must allow summarizing all, got %+v", idsOf(got))
	}
	if got := microPrefix(active, nil, 5); len(got) != 0 {
		t.Errorf("keep beyond tool count must yield empty prefix, got %+v", idsOf(got))
	}
	if got := microPrefix(active, map[string]string{"t1": "done"}, 2); len(got) != 1 || got[0].ID != "l1" {
		t.Errorf("already-sectioned nodes must be skipped, got %+v", idsOf(got))
	}
}

// TestNodesToMessages_SectionsBackfill 回填是水位线语义，不随塑形开关退化：
// 塑形关闭时也必须以摘要替换原文，否则微压缩在关闭塑形的部署里白做。
func TestNodesToMessages_SectionsBackfill(t *testing.T) {
	nodes := []model.Node{toolNode("t1", "search", strings.Repeat("x", 5000)), llmNode("l1", "i1", "o1")}
	secs := map[string]string{"t1": "t1 did a search"}
	want := "[Summary of node t1 (TOOL search): t1 did a search]"

	got := nodesToMessages(nodes, ContextOptions{ToolMasking: false}, DefaultToolMaskWindow, secs)
	if got[0].Content != want {
		t.Errorf("masking off: section must still backfill, got %q", got[0].Content)
	}
	if got[1].Content != "i1" || got[2].Content != "o1" {
		t.Errorf("unsectioned node must stay verbatim: %+v", got[1:])
	}

	got = nodesToMessages(nodes, ContextOptions{ToolMasking: true}, DefaultToolMaskWindow, secs)
	if got[0].Content != want {
		t.Errorf("masking on: section must backfill, got %q", got[0].Content)
	}
	if strings.Contains(got[0].Content, strings.Repeat("x", 100)) {
		t.Error("original tool output must not leak alongside the summary")
	}
}

// TestCompactionID 同一 (run,kind,waterline) 派生稳定 ID、不同输入互异：
// 这是"先算后插、冲突丢弃"并发策略的幂等落点。
func TestCompactionID(t *testing.T) {
	a := compactionID("r1", "micro", "n5")
	if a != compactionID("r1", "micro", "n5") {
		t.Error("compaction id must be deterministic")
	}
	if a == compactionID("r1", "full", "n5") || a == compactionID("r2", "micro", "n5") {
		t.Error("compaction id must differ across kind/run")
	}
}

// ==== Apply 端到端 ====

// TestCompactor_Apply_NoneWhenSmall 小上下文零干预：消息与无压缩路径完全一致。
func TestCompactor_Apply_NoneWhenSmall(t *testing.T) {
	store := &fakeCompactionStore{}
	c := &Compactor{Store: store, Opt: tightOpt()}
	nodes := []model.Node{llmNode("n1", "i1", "o1"), toolNode("t1", "search", "small result")}
	opt := ContextOptions{ToolMasking: true, ToolMaskWindow: 6}
	got := c.Apply(context.Background(), testEC(), nodes, nil, opt)
	want := nodesToMessages(nodes, opt, 6, nil)
	if len(got) != len(want) {
		t.Fatalf("small context must pass through untouched: got %d want %d msgs", len(got), len(want))
	}
	for i := range got {
		if got[i] != want[i] {
			t.Errorf("msg[%d] drifted: got %q want %q", i, got[i].Content, want[i].Content)
		}
	}
	if len(store.inserted) != 0 {
		t.Errorf("no compaction record must be written, got %d", len(store.inserted))
	}
}

// TestCompactor_Apply_L2Shrink 纯确定性收缩就能回到预算内时，不得动用 LLM：
// 老工具结果全部遮蔽、无摘要、无截断指针、无落库。
func TestCompactor_Apply_L2Shrink(t *testing.T) {
	store := &fakeCompactionStore{}
	fm := fakeSummaryModel{respond: func(string) string {
		t.Error("L2 exit must not invoke the summary model")
		return ""
	}}
	c := &Compactor{Store: store, Model: fm, Opt: tightOpt()}
	big := strings.Repeat("r", 2500)
	nodes := []model.Node{
		toolNode("t1", "search", big), toolNode("t2", "search", big),
		toolNode("t3", "search", big), toolNode("t4", "search", big),
	}
	got := c.Apply(context.Background(), testEC(), nodes, nil, ContextOptions{ToolMasking: true, ToolMaskWindow: 4})
	if len(got) != 8 {
		t.Fatalf("4 tool nodes must yield 8 messages, got %d", len(got))
	}
	for _, m := range got {
		if strings.Contains(m.Content, "[Summary of node") {
			t.Errorf("L2 exit must not emit summaries: %q", m.Content)
		}
		if strings.Contains(m.Content, "[Dropped") {
			t.Errorf("L2 exit must not truncate: %q", m.Content)
		}
	}
	// 全部工具结果已遮蔽（收缩到终点档 keep=0），原文指针保留。
	masked := 0
	for _, m := range got {
		if strings.Contains(m.Content, "Compacted tool result") {
			masked++
		}
	}
	if masked != 4 {
		t.Errorf("all 4 tool outputs must be masked at keep=0, got %d", masked)
	}
	if len(store.inserted) != 0 {
		t.Errorf("L2 must not persist compaction records, got %d", len(store.inserted))
	}
}

// TestCompactor_Apply_Micro L3 微压缩：老节点整段换转述、记录落库、事件只带量级。
func TestCompactor_Apply_Micro(t *testing.T) {
	store := &fakeCompactionStore{}
	sink := &fakeEventSink{}
	fm := fakeSummaryModel{respond: func(string) string {
		return `{"n1":"n1 done","l1":"l1 thought hard"}`
	}}
	c := &Compactor{Store: store, Model: fm, Events: sink, Opt: tightOpt()}
	// 无工具可遮蔽、LLM 原文 6000 rune（1500 token > 1024）：L2 无从收缩，必须走 L3。
	nodes := []model.Node{llmNode("n1", "i1", strings.Repeat("x", 6000)), llmNode("l1", "i2", "o2")}
	got := c.Apply(context.Background(), testEC(), nodes, nil, ContextOptions{ToolMasking: true})
	for _, m := range got {
		if !strings.Contains(m.Content, "[Summary of node") {
			t.Errorf("all messages must be summary backfills, got %q", m.Content)
		}
	}
	if store.micro == nil {
		t.Fatal("micro compaction record must be persisted")
	}
	if store.micro.WaterlineNodeID != "l1" {
		t.Errorf("waterline must cover through the last active node, got %q", store.micro.WaterlineNodeID)
	}
	if len(sink.events) != 1 || sink.events[0].Type != contracts.EventContextCompacted {
		t.Fatalf("exactly one CONTEXT_COMPACTED event expected, got %+v", sink.events)
	}
	data := eventData(t, sink.events[0])
	if data["kind"] != CompactionKindMicro || data["waterline_node_id"] != "l1" {
		t.Errorf("event must carry kind/waterline only: %+v", data)
	}
	for _, m := range got {
		if !strings.Contains(m.Content, "node ") {
			t.Errorf("summary backfill must carry the node_id pointer: %q", m.Content)
		}
	}
}

// TestCompactor_Apply_AnchorSkipsCompaction 实测锚定的价值：增量极小时即便全量
// 估算超阈值也不压缩——对比无锚点时同样的节点会触发 L3。
func TestCompactor_Apply_AnchorSkipsCompaction(t *testing.T) {
	nodes := []model.Node{llmNode("n1", "i1", strings.Repeat("x", 6000)), llmNode("n2", "i2", "o2")}
	ec := testEC()
	ctx := context.Background()

	// 有锚点：上次实测 100 token，本次只新增了 n2 的几十字符 → 不压缩。
	anchored := &fakeCompactionStore{usage: &model.LLMUsage{ID: "u1", RunID: ec.RunID, NodeID: "n1", TenantID: ec.TenantID, Model: "m1", PromptTokens: 100}}
	var called bool
	fm := fakeSummaryModel{respond: func(string) string { called = true; return "{}" }}
	c := &Compactor{Store: anchored, Model: fm, Opt: tightOpt()}
	got := c.Apply(ctx, ec, nodes, nil, ContextOptions{ToolMasking: true})
	if called {
		t.Error("anchored estimate must stay under threshold; summary model must not be called")
	}
	if !strings.Contains(got[1].Content, strings.Repeat("x", 100)) {
		t.Errorf("original history must be kept verbatim under anchor estimate: %q", got[1].Content)
	}

	// 无锚点：同样的节点按全量估算 ≈1500 token → 触发 L3 微压缩。
	plain := &fakeCompactionStore{}
	fm2 := fakeSummaryModel{respond: func(string) string { return `{"n1":"s1","n2":"s2"}` }}
	c2 := &Compactor{Store: plain, Model: fm2, Opt: tightOpt()}
	got2 := c2.Apply(ctx, ec, nodes, nil, ContextOptions{ToolMasking: true})
	if plain.micro == nil {
		t.Fatal("without anchor the same nodes must trigger micro compaction")
	}
	if !strings.Contains(got2[0].Content, "[Summary of node n1") {
		t.Errorf("expected summary backfill, got %q", got2[0].Content)
	}
}

// TestCompactor_Apply_FullCompaction L4 全量压缩：八段摘要成为 system 基座、
// 水位线推进到最新节点、缓存键换代 g1、事件 kind=full。
func TestCompactor_Apply_FullCompaction(t *testing.T) {
	store := &fakeCompactionStore{}
	sink := &fakeEventSink{}
	fm := fakeSummaryModel{respond: func(prompt string) string {
		if strings.Contains(prompt, "Summarize each completed agent step") {
			return "```not-json```" // L3 解析失败 → 降级，不停摆
		}
		return "1 任务目标与约束: demo goal\n8 当前工作与下一步: finishing"
	}}
	c := &Compactor{Store: store, Model: fm, Events: sink, Opt: tightOpt()}
	nodes := []model.Node{
		toolNode("t1", "search", strings.Repeat("r", 2500)),
		llmNode("l1", "i1", strings.Repeat("x", 20000)),
		toolNode("t2", "search", strings.Repeat("r", 2500)),
	}
	got := c.Apply(context.Background(), testEC(), nodes, nil, ContextOptions{ToolMasking: true, ToolMaskWindow: 2})
	// 水位线推进到最后一个节点：active 清空，上下文只剩摘要基座（当前指令由 executor 追加）。
	if len(got) != 1 {
		t.Fatalf("full compaction must reduce context to the summary base, got %d msgs: %+v", len(got), got)
	}
	if got[0].Role != llm.RoleSystem || !strings.HasPrefix(got[0].Content, compactedSummaryPrefix) {
		t.Errorf("base must be a system message with the compaction prefix: %+v", got[0])
	}
	if !strings.Contains(got[0].Content, "demo goal") {
		t.Errorf("base must carry the structured summary: %q", got[0].Content)
	}
	if store.full == nil || store.full.WaterlineNodeID != "t2" {
		t.Fatalf("full record must be persisted with waterline at latest node, got %+v", store.full)
	}
	if gen := c.CacheGen(context.Background(), testEC().TenantID, testEC().RunID); gen != ":g1" {
		t.Errorf("cache gen must advance to :g1 after full compaction, got %q", gen)
	}
	if len(sink.events) == 0 || eventData(t, sink.events[len(sink.events)-1])["kind"] != CompactionKindFull {
		t.Errorf("full compaction event expected, got %+v", sink.events)
	}
}

// TestCompactor_Apply_HardTruncateFallback L5 兜底：Model 完全缺省（L3/L4 均降级）
// 时仍必须产出预算内的消息——压缩失败绝不挂 Run。
func TestCompactor_Apply_HardTruncateFallback(t *testing.T) {
	store := &fakeCompactionStore{}
	c := &Compactor{Store: store, Opt: tightOpt()} // Model=nil：L3/L4 直接降级
	nodes := []model.Node{llmNode("n1", "i1", strings.Repeat("x", 20000))}
	got := c.Apply(context.Background(), testEC(), nodes, nil, ContextOptions{ToolMasking: true})
	if len(got) < 2 {
		t.Fatalf("hard truncate must keep at least the pointer and recent messages, got %+v", got)
	}
	if !strings.Contains(got[0].Content, "[Dropped 1 old message(s)") || !strings.Contains(got[0].Content, testEC().RunID) {
		t.Errorf("drop pointer with run id expected, got %q", got[0].Content)
	}
	if total := messagesRunes(got); total > minCompactionThreshold*c.Opt.charsPerToken()+512 {
		t.Errorf("truncated context must fit the budget, got %d runes", total)
	}
	if len(store.inserted) != 0 {
		t.Errorf("fallback path must not persist records, got %d", len(store.inserted))
	}
}

// TestCompactor_Apply_PreexistingFullBase 既有 full 记录：基座注入 system、
// 水位线之前的节点不再展开，水位线之后的增量原样保留。
func TestCompactor_Apply_PreexistingFullBase(t *testing.T) {
	store := &fakeCompactionStore{full: &model.RunCompaction{
		ID: compactionID("run-1", CompactionKindFull, "n2"), TenantID: "tenant-t", RunID: "run-1",
		Kind: CompactionKindFull, WaterlineNodeID: "n2", Summary: "BASE SUMMARY",
	}}
	c := &Compactor{Store: store, Opt: tightOpt()}
	nodes := []model.Node{llmNode("n1", "i1", "o1"), llmNode("n2", "i2", "o2"), llmNode("n3", "i3", "o3")}
	got := c.Apply(context.Background(), testEC(), nodes, nil, ContextOptions{ToolMasking: true})
	if len(got) != 3 {
		t.Fatalf("base + one post-waterline pair expected, got %d: %+v", len(got), got)
	}
	if got[0].Role != llm.RoleSystem || !strings.Contains(got[0].Content, "BASE SUMMARY") {
		t.Errorf("summary base must lead the context: %+v", got[0])
	}
	if got[1].Content != "i3" || got[2].Content != "o3" {
		t.Errorf("only post-waterline node must expand, got %+v", got[1:])
	}
}

// TestTryMicro_ConflictReusesExisting 并发压缩"先算后插、冲突丢弃"：
// 插入冲突时复用已存在记录，本地结果丢弃无信息损失。
func TestTryMicro_ConflictReusesExisting(t *testing.T) {
	existing := &model.RunCompaction{
		ID: "existing", TenantID: "tenant-t", RunID: "run-1", Kind: CompactionKindMicro,
		WaterlineNodeID: "t0", Sections: map[string]string{"t0": "existing summary"},
	}
	store := &fakeCompactionStore{conflict: true, micro: existing}
	sink := &fakeEventSink{}
	fm := fakeSummaryModel{respond: func(string) string { return `{"t0":"fresh summary"}` }}
	c := &Compactor{Store: store, Model: fm, Events: sink, Opt: tightOpt()}
	ctx := context.Background()
	secs, ok := c.tryMicro(ctx, oteltrace.SpanFromContext(ctx), testEC(),
		[]model.Node{toolNode("t0", "search", "o")}, nil, 0, 5000)
	if !ok {
		t.Fatal("conflict must reuse the existing record, not fail the layer")
	}
	if secs["t0"] != "existing summary" {
		t.Errorf("conflicting insert must drop local result and reuse existing, got %q", secs["t0"])
	}
	if len(sink.events) != 1 || eventData(t, sink.events[0])["waterline_node_id"] != "t0" {
		t.Errorf("event must report the reused record's waterline, got %+v", sink.events)
	}
}
