package worker

import (
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"agent-runtime/internal/providers"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// legacyLoader 构造关闭祖先作用域与工具遮蔽的 loader（全量节点 + 无差别配对），
// 供只验证跨 Run 记忆路径的既有测试使用。
func legacyLoader(s contextStore, mem providers.MemoryProvider) func(context.Context, string, string, string) ([]llm.Message, error) {
	return newContextLoader(s, ContextOptions{AncestorScope: false, ToolMasking: false, Memory: MemoryOptions{Memory: mem}})
}

// ==== 测试替身 ====

// fakeContextStore 实现 contextStore，可分别注入全量/祖先节点查询与 GetRun 的行为。
type fakeContextStore struct {
	nodes        []model.Node
	nodesErr     error
	ancestor     []model.Node
	ancestorErr  error
	ancestorWith string // 记录最近一次祖先查询的目标 nodeID
	ancestorN    int
	run          *model.Run
	runErr       error
	getRunCalls  int
}

func (f *fakeContextStore) CompletedNodes(context.Context, string, string) ([]model.Node, error) {
	return f.nodes, f.nodesErr
}

func (f *fakeContextStore) CompletedAncestorNodes(_ context.Context, _, _, nodeID string) ([]model.Node, error) {
	f.ancestorN++
	f.ancestorWith = nodeID
	return f.ancestor, f.ancestorErr
}

func (f *fakeContextStore) GetRun(context.Context, string, string) (*model.Run, error) {
	f.getRunCalls++
	return f.run, f.runErr
}

// fakeSearcher 同时实现 MemoryProvider 与 MemorySearcher，记录调用入参。
type fakeSearcher struct {
	msgs     []contracts.Message
	err      error
	block    time.Duration // 模拟慢响应，用于验证超时降级
	calls    int
	lastEC   contracts.ExecutionContext
	lastQ    string
	lastTopK int
}

func (f *fakeSearcher) Load(context.Context, contracts.ExecutionContext) ([]contracts.Message, error) {
	return nil, nil
}

func (f *fakeSearcher) Save(context.Context, contracts.ExecutionContext, []contracts.Message) error {
	return nil
}

func (f *fakeSearcher) Search(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) ([]contracts.Message, error) {
	f.calls++
	f.lastEC, f.lastQ, f.lastTopK = ec, query, topK
	if f.block > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(f.block):
		}
	}
	return f.msgs, f.err
}

// plainMemory 只实现 MemoryProvider，**不**实现 MemorySearcher，
// 用于验证能力探测：没有语义检索能力的记忆应当被跳过而非报错。
type plainMemory struct{}

func (plainMemory) Load(context.Context, contracts.ExecutionContext) ([]contracts.Message, error) {
	return nil, nil
}
func (plainMemory) Save(context.Context, contracts.ExecutionContext, []contracts.Message) error {
	return nil
}

func llmNode(id, in, out string) model.Node {
	return model.Node{ID: id, Type: model.NodeLLM, Status: model.NodeSuccess, Input: in, Output: out}
}

// ==== mergeMessages：拼接与截断 ====

// TestMergeMessages_NoMemory 无记忆时应原样返回当前 Run 历史。
func TestMergeMessages_NoMemory(t *testing.T) {
	cur := []llm.Message{{Role: llm.RoleUser, Content: "a"}, {Role: llm.RoleAssistant, Content: "b"}}
	got := mergeMessages(nil, cur, 20)
	if len(got) != 2 || got[0].Content != "a" || got[1].Content != "b" {
		t.Fatalf("unexpected: %+v", got)
	}
}

// TestMergeMessages_MemoryFirst 记忆必须前置、当前历史后置。
//
// 顺序影响推理质量：跨 Run 记忆在前、当前对话紧邻本轮 prompt，
// 符合时间顺序直觉，也让模型优先关注最近上下文。
func TestMergeMessages_MemoryFirst(t *testing.T) {
	mem := []llm.Message{{Role: llm.RoleAssistant, Content: "旧记忆"}}
	cur := []llm.Message{{Role: llm.RoleUser, Content: "新问题"}}
	got := mergeMessages(mem, cur, 20)
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}
	if got[0].Content != "旧记忆" || got[1].Content != "新问题" {
		t.Errorf("memory should be prepended: %+v", got)
	}
}

// TestMergeMessages_TrimsOldestMemory 超限时必须从**最老**的跨 Run 记忆开始丢弃，
// 优先保住当前 Run 历史的完整性——它直接决定本轮推理的连贯性。
func TestMergeMessages_TrimsOldestMemory(t *testing.T) {
	mem := []llm.Message{
		{Content: "老记忆1"}, {Content: "老记忆2"}, {Content: "新记忆3"},
	}
	cur := []llm.Message{{Content: "当前1"}, {Content: "当前2"}}

	got := mergeMessages(mem, cur, 4)
	if len(got) != 4 {
		t.Fatalf("expected 4 messages, got %d", len(got))
	}
	// 当前历史必须完整保留且在末尾。
	if got[2].Content != "当前1" || got[3].Content != "当前2" {
		t.Errorf("current run history should be kept intact at the tail: %+v", got)
	}
	// 记忆只留最近的 2 条（预算 = 4 - 2）。
	if got[0].Content != "老记忆2" || got[1].Content != "新记忆3" {
		t.Errorf("expected oldest memory dropped, got %+v", got)
	}
}

// TestMergeMessages_CurrentExceedsMax 当前历史本身就超限时，保留**最新**的一段。
func TestMergeMessages_CurrentExceedsMax(t *testing.T) {
	cur := []llm.Message{{Content: "1"}, {Content: "2"}, {Content: "3"}, {Content: "4"}}
	got := mergeMessages([]llm.Message{{Content: "记忆"}}, cur, 2)
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}
	if got[0].Content != "3" || got[1].Content != "4" {
		t.Errorf("expected newest current history kept, got %+v", got)
	}
}

// TestMergeMessages_NoAliasing 返回值必须是独立分配。
//
// executor 会对 ContextLoader 的返回值做 append（msgs = append(hist, msgs...)），
// 若返回带富余容量的内部子切片，append 会写入共享底层数组，
// 造成调用方数据被静默串改——这类 bug 极难定位。
func TestMergeMessages_NoAliasing(t *testing.T) {
	mem := []llm.Message{{Content: "m1"}, {Content: "m2"}}
	cur := []llm.Message{{Content: "c1"}}

	got := mergeMessages(mem, cur, 20)
	_ = append(got, llm.Message{Content: "追加"})

	if mem[0].Content != "m1" || mem[1].Content != "m2" {
		t.Errorf("input memory slice was mutated: %+v", mem)
	}
	if cur[0].Content != "c1" {
		t.Errorf("input current slice was mutated: %+v", cur)
	}
	// 截断分支同样不得与入参共享底层数组。
	cur2 := []llm.Message{{Content: "a"}, {Content: "b"}, {Content: "c"}}
	got2 := mergeMessages(nil, cur2, 2)
	_ = append(got2, llm.Message{Content: "追加"})
	if cur2[0].Content != "a" || cur2[1].Content != "b" || cur2[2].Content != "c" {
		t.Errorf("truncation branch aliased input: %+v", cur2)
	}
}

// TestMergeMessages_NonPositiveMax max<=0 时回退默认值，而非返回空。
func TestMergeMessages_NonPositiveMax(t *testing.T) {
	cur := []llm.Message{{Content: "a"}}
	if got := mergeMessages(nil, cur, 0); len(got) != 1 {
		t.Errorf("expected fallback to default max, got %d messages", len(got))
	}
}

// ==== MemoryOptions 默认值 ====

func TestMemoryOptions_Defaults(t *testing.T) {
	var zero MemoryOptions
	if got := zero.searchTimeout(); got != DefaultMemorySearchTimeout {
		t.Errorf("searchTimeout default: %v", got)
	}
	if got := zero.maxMessages(); got != DefaultMaxMemoryMessages {
		t.Errorf("maxMessages default: %d", got)
	}
	opt := MemoryOptions{SearchTimeout: time.Second, MaxMessages: 5}
	if opt.searchTimeout() != time.Second || opt.maxMessages() != 5 {
		t.Error("explicit values should be honored")
	}
	neg := MemoryOptions{SearchTimeout: -1, MaxMessages: -3}
	if neg.searchTimeout() != DefaultMemorySearchTimeout || neg.maxMessages() != DefaultMaxMemoryMessages {
		t.Error("negative values should fall back to defaults")
	}
}

// ==== ContextLoader：未启用记忆时必须与接入前行为一致 ====

// TestContextLoader_MemoryDisabled_UnchangedBehavior 记忆未启用时应返回全部当前 Run 历史，
// **且不施加 MaxMessages 截断**——否则长 Run 的上下文会被悄悄砍掉，
// 违背"现有部署零感知"的前提。
func TestContextLoader_MemoryDisabled_UnchangedBehavior(t *testing.T) {
	var nodes []model.Node
	for i := 0; i < 30; i++ {
		nodes = append(nodes, llmNode(string(rune('a'+i%26)), "in", "out"))
	}
	s := &fakeContextStore{nodes: nodes}
	// MaxMessages 故意设得很小：若被误用，结果会被截断。
	// 显式关闭塑形：本用例锁定的是"无截断"旧行为，与祖先/遮蔽策略正交。
	loader := newContextLoader(s, ContextOptions{AncestorScope: false, ToolMasking: false, Memory: MemoryOptions{Memory: nil, MaxMessages: 4}})

	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	if len(got) != 60 {
		t.Errorf("memory disabled should return all 60 messages untruncated, got %d", len(got))
	}
	if s.getRunCalls != 0 {
		t.Errorf("memory disabled should not query the run at all, got %d calls", s.getRunCalls)
	}
}

// TestContextLoader_PrependsMemory 启用记忆后，召回内容应前置到当前 Run 历史之前。
func TestContextLoader_PrependsMemory(t *testing.T) {
	s := &fakeContextStore{
		nodes: []model.Node{llmNode("n1", "当前问题", "当前回答")},
		run:   &model.Run{ID: "run-2", TenantID: "tenant-A", ThreadID: "thread-1", Input: "为什么延期"},
	}
	fs := &fakeSearcher{msgs: []contracts.Message{
		{Role: contracts.RoleAssistant, Content: "上次说过依赖未就绪"},
	}}
	loader := newContextLoader(s, ContextOptions{AncestorScope: false, ToolMasking: false, Memory: MemoryOptions{Memory: fs}})

	got, err := loader(context.Background(), "tenant-A", "run-2", "cur")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 1 memory + 2 current = 3, got %d: %+v", len(got), got)
	}
	if got[0].Content != "上次说过依赖未就绪" {
		t.Errorf("memory should come first, got %q", got[0].Content)
	}
	if got[1].Content != "当前问题" || got[2].Content != "当前回答" {
		t.Errorf("current run history should follow: %+v", got)
	}
}

// TestContextLoader_QueryAndIsolation 查询文本必须取 Run.Input（而非节点 Input），
// 且 ExecutionContext 要带上 ThreadID 与 RunID：
// ThreadID 决定会话隔离范围，RunID 让 provider 能在源头排除当前 Run。
func TestContextLoader_QueryAndIsolation(t *testing.T) {
	s := &fakeContextStore{
		nodes: []model.Node{llmNode("n1", "节点级子任务输入", "out")},
		run:   &model.Run{ID: "run-9", TenantID: "tenant-A", ThreadID: "thread-7", Input: "整个会话的原始诉求"},
	}
	fs := &fakeSearcher{}
	loader := newContextLoader(s, ContextOptions{AncestorScope: false, ToolMasking: false, Memory: MemoryOptions{Memory: fs, MaxMessages: 7}})

	if _, err := loader(context.Background(), "tenant-A", "run-9", "cur"); err != nil {
		t.Fatalf("loader: %v", err)
	}
	if fs.calls != 1 {
		t.Fatalf("expected exactly 1 search, got %d", fs.calls)
	}
	if fs.lastQ != "整个会话的原始诉求" {
		t.Errorf("query should be Run.Input, got %q", fs.lastQ)
	}
	if fs.lastEC.ThreadID != "thread-7" {
		t.Errorf("ThreadID not propagated: %+v", fs.lastEC)
	}
	if fs.lastEC.RunID != "run-9" {
		t.Errorf("RunID not propagated (provider needs it to exclude the current run): %+v", fs.lastEC)
	}
	if fs.lastEC.TenantID != "tenant-A" {
		t.Errorf("TenantID not propagated: %+v", fs.lastEC)
	}
	if fs.lastTopK != 7 {
		t.Errorf("topK should be MaxMessages, got %d", fs.lastTopK)
	}
}

// TestContextLoader_SkipsWhenNoSearcher 记忆未实现 MemorySearcher 时应跳过召回，
// 不报错——保持 provider port 稳定，旧实现无需改动即可继续工作。
func TestContextLoader_SkipsWhenNoSearcher(t *testing.T) {
	s := &fakeContextStore{
		nodes: []model.Node{llmNode("n1", "in", "out")},
		run:   &model.Run{ID: "run-1", TenantID: "tenant-A", ThreadID: "t", Input: "q"},
	}
	loader := newContextLoader(s, ContextOptions{AncestorScope: false, ToolMasking: false, Memory: MemoryOptions{Memory: plainMemory{}}})

	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected only current run history, got %d", len(got))
	}
	if s.getRunCalls != 0 {
		t.Errorf("should not query run when searcher is unavailable, got %d calls", s.getRunCalls)
	}
}

// ==== 降级：记忆故障绝不能让节点执行失败 ====

// TestContextLoader_DegradesOnSearchError Search 返回错误时只降级，不上抛。
//
// 这是读取路径最关键的不变量：向量库宕机或网关限流若让 ContextLoader 报错，
// executor 会直接让 LLM 节点失败（见 executor_test 中 ContextLoader 错误传播的用例），
// 于是"记忆故障"升级成"整个 Run 失败"。
func TestContextLoader_DegradesOnSearchError(t *testing.T) {
	s := &fakeContextStore{
		nodes: []model.Node{llmNode("n1", "in", "out")},
		run:   &model.Run{ID: "run-1", TenantID: "tenant-A", ThreadID: "t", Input: "q"},
	}
	fs := &fakeSearcher{err: errors.New("qdrant unavailable")}
	loader := legacyLoader(s, fs)

	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	if err != nil {
		t.Fatalf("search failure must degrade, got err=%v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected current run history only, got %d", len(got))
	}
}

// TestContextLoader_DegradesOnTimeout 召回超时必须在预算内返回，且不上抛错误。
func TestContextLoader_DegradesOnTimeout(t *testing.T) {
	s := &fakeContextStore{
		nodes: []model.Node{llmNode("n1", "in", "out")},
		run:   &model.Run{ID: "run-1", TenantID: "tenant-A", ThreadID: "t", Input: "q"},
	}
	fs := &fakeSearcher{block: 500 * time.Millisecond} // 远超下面的超时预算
	loader := newContextLoader(s, ContextOptions{AncestorScope: false, ToolMasking: false, Memory: MemoryOptions{Memory: fs, SearchTimeout: 10 * time.Millisecond}})

	start := time.Now()
	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("timeout must degrade, got err=%v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected current run history only, got %d", len(got))
	}
	// 必须在预算量级内返回，而不是等满 500ms。
	if elapsed > 200*time.Millisecond {
		t.Errorf("timeout budget not enforced: took %v", elapsed)
	}
}

// TestContextLoader_DegradesOnGetRunError 取不到 Run 时无法确定会话维度，
// 应放弃召回但正常返回当前 Run 历史。
func TestContextLoader_DegradesOnGetRunError(t *testing.T) {
	s := &fakeContextStore{
		nodes:  []model.Node{llmNode("n1", "in", "out")},
		runErr: errors.New("db unreachable"),
	}
	fs := &fakeSearcher{msgs: []contracts.Message{{Content: "不该出现"}}}
	loader := legacyLoader(s, fs)

	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	if err != nil {
		t.Fatalf("GetRun failure must degrade, got err=%v", err)
	}
	if len(got) != 2 {
		t.Errorf("expected current run history only, got %d", len(got))
	}
	if fs.calls != 0 {
		t.Errorf("should not search without run context, got %d calls", fs.calls)
	}
}

// TestContextLoader_SkipsWhenRunInputEmpty Run.Input 为空时跳过召回。
//
// 没有查询文本就没有召回依据；此时若仍发起检索，会退化成"无约束全量召回"，
// 把该会话无关的内容混进上下文。
func TestContextLoader_SkipsWhenRunInputEmpty(t *testing.T) {
	s := &fakeContextStore{
		nodes: []model.Node{llmNode("n1", "in", "out")},
		run:   &model.Run{ID: "run-1", TenantID: "tenant-A", ThreadID: "t", Input: ""},
	}
	fs := &fakeSearcher{msgs: []contracts.Message{{Content: "不该出现"}}}
	loader := legacyLoader(s, fs)

	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	if fs.calls != 0 {
		t.Errorf("should not search with empty query, got %d calls", fs.calls)
	}
	if len(got) != 2 {
		t.Errorf("expected current run history only, got %d", len(got))
	}
}

// TestContextLoader_SkipsEmptyMemoryContent 召回结果中的空内容应被丢弃，
// 空消息只会浪费 token 并可能干扰模型。
func TestContextLoader_SkipsEmptyMemoryContent(t *testing.T) {
	s := &fakeContextStore{
		nodes: []model.Node{llmNode("n1", "in", "out")},
		run:   &model.Run{ID: "run-1", TenantID: "tenant-A", ThreadID: "t", Input: "q"},
	}
	fs := &fakeSearcher{msgs: []contracts.Message{
		{Role: contracts.RoleAssistant, Content: ""},
		{Role: contracts.RoleAssistant, Content: "有效记忆"},
	}}
	loader := legacyLoader(s, fs)

	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 1 valid memory + 2 current, got %d: %+v", len(got), got)
	}
	if got[0].Content != "有效记忆" {
		t.Errorf("empty memory should be skipped, got %q first", got[0].Content)
	}
}

// TestContextLoader_RoleConversion 召回消息的角色必须正确转换为 llm.Role。
func TestContextLoader_RoleConversion(t *testing.T) {
	s := &fakeContextStore{
		nodes: nil,
		run:   &model.Run{ID: "run-1", TenantID: "tenant-A", ThreadID: "t", Input: "q"},
	}
	fs := &fakeSearcher{msgs: []contracts.Message{
		{Role: contracts.RoleSystem, Content: "sys"},
		{Role: contracts.RoleUser, Content: "usr"},
		{Role: contracts.RoleAssistant, Content: "ast"},
	}}
	loader := legacyLoader(s, fs)

	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	want := []llm.Role{llm.RoleSystem, llm.RoleUser, llm.RoleAssistant}
	if len(got) != len(want) {
		t.Fatalf("expected %d messages, got %d", len(want), len(got))
	}
	for i, w := range want {
		if got[i].Role != w {
			t.Errorf("message %d: role = %q, want %q", i, got[i].Role, w)
		}
	}
}

// ==== 主链路数据不可降级 ====

// TestContextLoader_CompletedNodesErrorPropagates CompletedNodes 失败必须上抛。
//
// 与记忆召回相反：当前 Run 历史是主链路**必需**数据，
// 静默吞掉会让 Agent 在缺失历史的情况下推理，产生错误结论。
// executor_test.go 已锁定"ContextLoader 报错应传播"的行为，此处保持一致。
func TestContextLoader_CompletedNodesErrorPropagates(t *testing.T) {
	s := &fakeContextStore{nodesErr: errors.New("db unreachable")}
	loader := newContextLoader(s, ContextOptions{AncestorScope: false, ToolMasking: false})

	if _, err := loader(context.Background(), "tenant-A", "run-1", "cur"); err == nil {
		t.Fatal("CompletedNodes failure must propagate")
	}
}

// TestContextLoader_NoRowsIsNotAnError sql.ErrNoRows 表示"还没有已完成节点"，
// 属于正常状态而非故障，不应上抛。
func TestContextLoader_NoRowsIsNotAnError(t *testing.T) {
	s := &fakeContextStore{nodesErr: sql.ErrNoRows}
	loader := newContextLoader(s, ContextOptions{AncestorScope: false, ToolMasking: false})

	got, err := loader(context.Background(), "tenant-A", "run-1", "cur")
	if err != nil {
		t.Fatalf("sql.ErrNoRows should not propagate, got %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected no messages, got %d", len(got))
	}
}

// ==== 环境变量解析辅助 ====

func TestEnvHelpers(t *testing.T) {
	t.Setenv("MEM_T_STR", "custom")
	if got := envString("MEM_T_STR", "def"); got != "custom" {
		t.Errorf("envString: %q", got)
	}
	if got := envString("MEM_T_UNSET", "def"); got != "def" {
		t.Errorf("envString default: %q", got)
	}

	t.Setenv("MEM_T_BOOL", "true")
	if !envBool("MEM_T_BOOL", false) {
		t.Error("envBool should parse true")
	}
	t.Setenv("MEM_T_BOOL", "not-a-bool")
	if !envBool("MEM_T_BOOL", true) {
		t.Error("envBool should fall back to default on invalid input")
	}
	if envBool("MEM_T_UNSET", true) != true {
		t.Error("envBool default")
	}

	t.Setenv("MEM_T_INT", "42")
	if got := envInt("MEM_T_INT", 1); got != 42 {
		t.Errorf("envInt: %d", got)
	}
	t.Setenv("MEM_T_INT", "xyz")
	if got := envInt("MEM_T_INT", 7); got != 7 {
		t.Errorf("envInt should fall back to default on invalid input, got %d", got)
	}

	t.Setenv("MEM_T_F", "0.75")
	if got := envFloat32("MEM_T_F", 0.1); got != 0.75 {
		t.Errorf("envFloat32: %v", got)
	}
	t.Setenv("MEM_T_F", "abc")
	if got := envFloat32("MEM_T_F", 0.25); got != 0.25 {
		t.Errorf("envFloat32 should fall back to default, got %v", got)
	}

	t.Setenv("MEM_T_D", "250ms")
	if got := envDuration("MEM_T_D", time.Second); got != 250*time.Millisecond {
		t.Errorf("envDuration: %v", got)
	}
	t.Setenv("MEM_T_D", "nope")
	if got := envDuration("MEM_T_D", 3*time.Second); got != 3*time.Second {
		t.Errorf("envDuration should fall back to default, got %v", got)
	}
}

// TestNewMemoryOptionsFromEnv_DisabledByDefault 默认必须不启用记忆，
// 保证未配置向量库的现有部署与测试零感知。
func TestNewMemoryOptionsFromEnv_DisabledByDefault(t *testing.T) {
	// 不设 MEMORY_ENABLED，即使其余配置齐备也不应启用。
	t.Setenv("OPENAI_API_KEY", "k")
	t.Setenv("OPENAI_BASE_URL", "http://localhost:1/v1")
	if opt := newMemoryOptionsFromEnv(newCredentialsFromEnv()); opt.Memory != nil {
		t.Error("memory should stay disabled unless MEMORY_ENABLED is set")
	}
}

// TestNewMemoryOptionsFromEnv_DisabledWithoutKey 显式开启但缺 API key 时应降级为不启用，
// 而不是装配出一个必然失败的记忆实现。
func TestNewMemoryOptionsFromEnv_DisabledWithoutKey(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("OPENAI_API_KEY", "")
	if opt := newMemoryOptionsFromEnv(newCredentialsFromEnv()); opt.Memory != nil {
		t.Error("memory should be disabled without an embedding key")
	}
}

// ==== P0：DAG 祖先作用域 ====

// TestContextLoader_AncestorScope_RoutesQuery 开启祖先作用域后必须改走
// CompletedAncestorNodes 且把当前 nodeID 透传，全量查询不应被调用。
func TestContextLoader_AncestorScope_RoutesQuery(t *testing.T) {
	s := &fakeContextStore{ancestor: []model.Node{llmNode("a1", "in", "out")}}
	loader := newContextLoader(s, ContextOptions{AncestorScope: true, ToolMasking: false})

	got, err := loader(context.Background(), "tenant-A", "run-1", "target-node")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	if s.ancestorN != 1 || s.ancestorWith != "target-node" {
		t.Errorf("ancestor query must run once with target nodeID, got n=%d node=%q", s.ancestorN, s.ancestorWith)
	}
	if len(got) != 2 {
		t.Errorf("expected 2 messages from ancestors, got %d", len(got))
	}
}

// TestContextLoader_AncestorScope_ErrorPropagates 祖先查询失败属于主链路必需数据失败，
// 必须与 CompletedNodes 失败一样上抛。
func TestContextLoader_AncestorScope_ErrorPropagates(t *testing.T) {
	s := &fakeContextStore{ancestorErr: errors.New("cte failed")}
	loader := newContextLoader(s, ContextOptions{AncestorScope: true, ToolMasking: false})

	if _, err := loader(context.Background(), "t", "r", "n"); err == nil {
		t.Fatal("ancestor query failure must propagate")
	}
}

// ==== P0：工具结果遮蔽 / REFLECT 排除 ====

func toolNode(id, name, out string) model.Node {
	return model.Node{ID: id, Type: model.NodeTool, Name: name, Status: model.NodeSuccess, Input: "q-" + id, Output: out}
}

func reflectNode(id string) model.Node {
	return model.Node{ID: id, Type: model.NodeReflect, Status: model.NodeSuccess,
		Input: "evaluate", Output: `{"action":"replan","reason":"x"}`}
}

// TestNodesToMessages_LegacyMapping 塑形关闭时，TOOL/REFLECT 一律无差别配对，
// 不做前缀、遮蔽与排除——锁定旧行为。
func TestNodesToMessages_LegacyMapping(t *testing.T) {
	nodes := []model.Node{
		toolNode("t1", "search", "big-result"),
		reflectNode("r1"),
	}
	got := nodesToMessages(nodes, ContextOptions{ToolMasking: false})
	if len(got) != 4 {
		t.Fatalf("legacy mapping must emit 2 messages per node, got %d: %+v", len(got), got)
	}
	if got[0].Content != "q-t1" || got[1].Content != "big-result" {
		t.Errorf("tool node must pass through verbatim, got %+v", got[:2])
	}
	if got[2].Content != "evaluate" || got[3].Content == "" {
		t.Errorf("reflect node must pass through verbatim, got %+v", got[2:])
	}
}

// TestNodesToMessages_MaskOldToolResults 窗口外工具结果替换为占位符（含 node_id 指针与
// 规模），窗口内保留原文；工具调用消息必须带 [tool_call:<name>] 前缀；REFLECT 整节点排除。
func TestNodesToMessages_MaskOldToolResults(t *testing.T) {
	nodes := []model.Node{
		toolNode("old1", "search", "result-1"),
		toolNode("old2", "search", "result-2"),
		llmNode("l1", "think", "answer"),
		toolNode("new1", "search", "result-3"),
		reflectNode("rf1"),
	}
	// 窗口只保留最近 1 个工具节点（new1）。
	got := nodesToMessages(nodes, ContextOptions{ToolMasking: true, ToolMaskWindow: 1, ToolOutputMaxRunes: 100000})

	// 4 个非 REFLECT 节点；其中 3 个工具各 2 条 + 1 个 LLM 2 条 = 8 条。
	if len(got) != 8 {
		t.Fatalf("expected 8 messages (reflect excluded), got %d: %+v", len(got), got)
	}
	if got[0].Content != "[tool_call:search] q-old1" {
		t.Errorf("tool call must be prefixed, got %q", got[0].Content)
	}
	if !strings.Contains(got[1].Content, "Compacted tool result") || !strings.Contains(got[1].Content, `node_id="old1"`) {
		t.Errorf("old result must be a placeholder with node_id pointer, got %q", got[1].Content)
	}
	if strings.Contains(got[1].Content, "result-1") {
		t.Errorf("masked result must not contain raw content, got %q", got[1].Content)
	}
	if got[2].Content != "[tool_call:search] q-old2" || !strings.Contains(got[3].Content, `node_id="old2"`) {
		t.Errorf("second-oldest tool result must also be masked, got %+v", got[2:4])
	}
	// LLM 节点维持原样。
	if got[4].Content != "think" || got[5].Content != "answer" {
		t.Errorf("llm node must pass through, got %+v", got[4:6])
	}
	// 窗口内的 new1 保留原文。
	if got[7].Content != "result-3" {
		t.Errorf("in-window tool result must be kept verbatim, got %q", got[7].Content)
	}
}

// TestNodesToMessages_TruncatesLongToolOutput 窗口内但超长的结果按 rune 截断，
// 附规模与 node_id 指针，且不得切断多字节字符。
func TestNodesToMessages_TruncatesLongToolOutput(t *testing.T) {
	long := strings.Repeat("结", 50) // 50 个 rune / 150 字节
	nodes := []model.Node{toolNode("t1", "search", long)}
	got := nodesToMessages(nodes, ContextOptions{ToolMasking: true, ToolMaskWindow: 1, ToolOutputMaxRunes: 10})
	out := got[1].Content
	if !utf8.ValidString(out) {
		t.Errorf("truncation produced invalid UTF-8: %q", out)
	}
	if !strings.HasPrefix(out, strings.Repeat("结", 10)) {
		t.Errorf("expected first 10 runes kept, got %q", out)
	}
	if !strings.Contains(out, "node_id=\"t1\"") || !strings.Contains(out, "50 characters") {
		t.Errorf("truncation suffix must carry pointer and original size, got %q", out)
	}
}

// TestContextOptions_Defaults 零值选项必须回退默认窗口/上限。
func TestContextOptions_Defaults(t *testing.T) {
	var zero ContextOptions
	if zero.toolMaskWindow() != DefaultToolMaskWindow {
		t.Errorf("window default = %d", zero.toolMaskWindow())
	}
	if zero.toolOutputMaxRunes() != DefaultToolOutputMaxRunes {
		t.Errorf("max runes default = %d", zero.toolOutputMaxRunes())
	}
	explicit := ContextOptions{ToolMaskWindow: 2, ToolOutputMaxRunes: 100}
	if explicit.toolMaskWindow() != 2 || explicit.toolOutputMaxRunes() != 100 {
		t.Error("explicit values should be honored")
	}
}
