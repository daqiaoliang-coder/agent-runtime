package providers

import (
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"context"
	"errors"
	"testing"
	"time"
)

// failingEmbedder 总是失败的向量化实现，用于验证 embedding 网关故障时的降级。
type failingEmbedder struct{ err error }

func (f failingEmbedder) Embed(context.Context, []string) ([][]float32, error) {
	return nil, f.err
}

func ec(tenant, thread, run string) contracts.ExecutionContext {
	return contracts.ExecutionContext{TenantID: tenant, ThreadID: thread, RunID: run}
}

// newTestMemory 构造一个基于 fake 向量库 + StubEmbedder 的 VectorMemory。
// MinScore 设 0（不过滤），使召回结果只由向量相似度决定，便于断言。
func newTestMemory(t *testing.T, collection string) (*VectorMemory, *vector.Fake) {
	t.Helper()
	f := vector.NewFake()
	if err := f.EnsureCollection(context.Background(), collection, 8); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	return &VectorMemory{
		Embedder:   llm.StubEmbedder{Dim: 8},
		Store:      f,
		Collection: collection,
		MinScore:   0,
	}, f
}

// seed 写入一条记忆点，向量由文本经 StubEmbedder 派生（与查询用同一 embedder，
// 因此同文本必然相似度 1.0，可稳定命中）。
func seed(t *testing.T, m *VectorMemory, tenant, thread, nodeID, role, text string, at time.Time) {
	t.Helper()
	vecs, err := m.Embedder.Embed(context.Background(), []string{text})
	if err != nil {
		t.Fatalf("embed seed: %v", err)
	}
	p := vector.Point{
		ID:        vector.PointID(tenant, thread, nodeID, role),
		Vector:    vecs[0],
		TenantID:  tenant,
		ThreadID:  thread,
		RunID:     "run-" + nodeID,
		NodeID:    nodeID,
		Role:      role,
		Text:      text,
		CreatedAt: at,
	}
	if err := m.Store.Upsert(context.Background(), m.Collection, []vector.Point{p}); err != nil {
		t.Fatalf("seed upsert: %v", err)
	}
}

// TestVectorMemory_SearchRoundTrip 写入的记忆必须能被语义召回，且内容完整。
func TestVectorMemory_SearchRoundTrip(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	seed(t, m, "tenant-A", "thread-1", "node-1", "assistant", "项目延期因为上游依赖未就绪", time.Now())

	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-2"), "项目延期因为上游依赖未就绪", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 message, got %d", len(got))
	}
	if got[0].Content != "项目延期因为上游依赖未就绪" {
		t.Errorf("content mismatch: %q", got[0].Content)
	}
	if got[0].Role != contracts.RoleAssistant {
		t.Errorf("role should be preserved as assistant, got %q", got[0].Role)
	}
}

// TestVectorMemory_SearchOrdersByTimeAscending 召回结果必须按时间正序。
//
// 向量库返回的是 score 降序，但对话历史喂给 LLM 必须还原真实先后顺序，
// 否则模型会看到"先回答后提问"的错乱上下文。这里刻意让**较旧**的记忆
// 相似度更高（更容易被排到前面），以验证确实做了时间重排而非沿用分数顺序。
func TestVectorMemory_SearchOrdersByTimeAscending(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	old := time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	// query 与 oldText 完全一致（相似度 1.0），与 newerText 不同（相似度较低）。
	const query = "完全相同的查询文本"
	seed(t, m, "tenant-A", "thread-1", "node-old", "assistant", query, old)
	seed(t, m, "tenant-A", "thread-1", "node-new", "assistant", "毫不相干的另一段内容", newer)

	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-x"), query, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(got))
	}
	// 时间正序：旧的在前。若沿用 score 降序，query 本身会排第一（碰巧也是旧的），
	// 因此再断言第二条确实是较新那条，确保顺序不是巧合。
	if got[0].Content != query {
		t.Errorf("expected oldest first, got %q", got[0].Content)
	}
	if got[1].Content != "毫不相干的另一段内容" {
		t.Errorf("expected newer second, got %q", got[1].Content)
	}
}

// TestVectorMemory_MinScoreFilters 低于阈值的结果必须被丢弃。
// 无阈值过滤会把不相关的旧对话塞进上下文，既浪费 token 又干扰模型。
func TestVectorMemory_MinScoreFilters(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	seed(t, m, "tenant-A", "thread-1", "node-1", "assistant", "完全匹配的查询", time.Now())
	seed(t, m, "tenant-A", "thread-1", "node-2", "assistant", "另一段完全无关的内容", time.Now())

	// 阈值 0.99：只有与查询完全一致的那条能过。
	m.MinScore = 0.99
	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-x"), "完全匹配的查询", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected only the exact match above 0.99, got %d", len(got))
	}
	if got[0].Content != "完全匹配的查询" {
		t.Errorf("unexpected survivor: %q", got[0].Content)
	}
}

// TestVectorMemory_TopKRespected topK 必须生效，防止召回过多撑爆上下文窗口。
func TestVectorMemory_TopKRespected(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	for i := 0; i < 5; i++ {
		// 五条都用同一文本，保证相似度都足够高、都会进入结果集。
		seed(t, m, "tenant-A", "thread-1", string(rune('a'+i)), "assistant", "相同文本", base.Add(time.Duration(i)*time.Second))
	}
	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-x"), "相同文本", 2)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("topK=2 not respected, got %d", len(got))
	}
}

// TestVectorMemory_TopKFallbackToDefault 调用方传 0 时应回退到配置的 TopK，
// 再回退到 DefaultMemoryTopK，避免"传 0 就召回 0 条"的静默失效。
func TestVectorMemory_TopKFallbackToDefault(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	base := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	for i := 0; i < DefaultMemoryTopK+5; i++ {
		seed(t, m, "tenant-A", "thread-1", string(rune('A'+i)), "assistant", "相同文本", base.Add(time.Duration(i)*time.Second))
	}
	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-x"), "相同文本", 0)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != DefaultMemoryTopK {
		t.Errorf("expected fallback to DefaultMemoryTopK=%d, got %d", DefaultMemoryTopK, len(got))
	}
}

// TestVectorMemory_TenantIsolation 租户隔离是越权防线，必须传递到检索条件。
func TestVectorMemory_TenantIsolation(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	const secret = "租户A的机密内容"
	seed(t, m, "tenant-A", "thread-1", "node-1", "assistant", secret, time.Now())

	got, err := m.Search(ctx, ec("tenant-B", "thread-1", "run-x"), secret, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("cross-tenant leak: tenant-B saw %d messages from tenant-A", len(got))
	}
}

// TestVectorMemory_ThreadIsolation 会话隔离：同租户不同会话不得互相召回。
func TestVectorMemory_ThreadIsolation(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	const text = "两个会话都有的相同文本"
	seed(t, m, "tenant-A", "thread-1", "node-1", "assistant", text, time.Now())
	seed(t, m, "tenant-A", "thread-2", "node-2", "assistant", text, time.Now())

	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-x"), text, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("cross-thread leak: got %d messages, want 1", len(got))
	}
}

// TestVectorMemory_EmptyThreadIDSeesLegacy 查询侧 threadID 为空时不按会话过滤。
//
// 这是存量数据兼容的关键：迁移前所有 Run 的 thread_id 都是空串，
// 若空串也参与严格匹配，升级后新会话将查不到任何旧记忆（静默失效，极难排查）。
func TestVectorMemory_EmptyThreadIDSeesLegacy(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	seed(t, m, "tenant-A", "", "node-legacy", "assistant", "存量记忆内容", time.Now())
	seed(t, m, "tenant-A", "thread-1", "node-new", "assistant", "存量记忆内容", time.Now())

	got, err := m.Search(ctx, ec("tenant-A", "", "run-x"), "存量记忆内容", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("empty threadID should skip thread filter, got %d want 2", len(got))
	}
}

// TestVectorMemory_EmptyTenantIDReturnsNothing 租户为空必须返回空而非全量。
// fail-closed：缺少租户身份时绝不能返回任何数据。
func TestVectorMemory_EmptyTenantIDReturnsNothing(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	seed(t, m, "tenant-A", "thread-1", "node-1", "assistant", "内容", time.Now())

	got, err := m.Search(ctx, ec("", "thread-1", "run-x"), "内容", 10)
	if err != nil {
		t.Fatalf("search should not error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("empty tenant must be fail-closed, got %d messages", len(got))
	}
}

// TestVectorMemory_EmptyQueryReturnsNothing 空查询无法向量化，应返回空而非报错。
func TestVectorMemory_EmptyQueryReturnsNothing(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	seed(t, m, "tenant-A", "thread-1", "node-1", "assistant", "内容", time.Now())

	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-x"), "", 10)
	if err != nil {
		t.Fatalf("empty query should not error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected no messages for empty query, got %d", len(got))
	}
}

// TestVectorMemory_ExcludesCurrentRun 召回必须排除当前 Run 自己的记忆。
//
// 当前 Run 的节点完成后会被索引器投影进向量库，其历史又由 worker 从
// 已提交节点单独提供；若不排除，同一段对话会在上下文中出现两次。
// 由于 Search 返回的 Message 不含 node_id，调用方无法在下游去重，
// 因此这条不变量必须在 provider 层锁死。
func TestVectorMemory_ExcludesCurrentRun(t *testing.T) {
	m, f := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	now := time.Now()

	const text = "同一段会被重复注入的内容"
	// 两条记忆文本相同（相似度都为 1.0），只有所属 Run 不同。
	seedWithRun(t, m, "tenant-A", "thread-1", "run-current", "node-cur", "assistant", text, now)
	seedWithRun(t, m, "tenant-A", "thread-1", "run-previous", "node-prev", "assistant", text, now)
	if n := f.Count("agent_memory"); n != 2 {
		t.Fatalf("expected 2 seeded points, got %d", n)
	}

	// 以 run-current 的身份检索：只应看到 run-previous 的那条。
	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-current"), text, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("current run should be excluded, got %d messages want 1", len(got))
	}

	// 换一个 RunID 检索：两条都应可见（排除只针对当前 Run）。
	got, err = m.Search(ctx, ec("tenant-A", "thread-1", "run-other"), text, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("unrelated run should see both, got %d want 2", len(got))
	}
}

// seedWithRun 与 seed 相同，但允许显式指定所属 Run（seed 由 nodeID 派生 RunID）。
func seedWithRun(t *testing.T, m *VectorMemory, tenant, thread, runID, nodeID, role, text string, at time.Time) {
	t.Helper()
	vecs, err := m.Embedder.Embed(context.Background(), []string{text})
	if err != nil {
		t.Fatalf("embed seed: %v", err)
	}
	p := vector.Point{
		ID:        vector.PointID(tenant, thread, nodeID, role),
		Vector:    vecs[0],
		TenantID:  tenant,
		ThreadID:  thread,
		RunID:     runID,
		NodeID:    nodeID,
		Role:      role,
		Text:      text,
		CreatedAt: at,
	}
	if err := m.Store.Upsert(context.Background(), m.Collection, []vector.Point{p}); err != nil {
		t.Fatalf("seed upsert: %v", err)
	}
}

// ==== 降级契约：记忆故障绝不能让 Run 失败 ====

// TestVectorMemory_DegradesOnEmbedderFailure embedding 网关故障必须静默降级。
// 这是最关键的不变量：网关限流/超时若向上抛错，会导致所有 LLM 节点失败。
func TestVectorMemory_DegradesOnEmbedderFailure(t *testing.T) {
	f := vector.NewFake()
	_ = f.EnsureCollection(context.Background(), "agent_memory", 8)
	m := &VectorMemory{
		Embedder:   failingEmbedder{err: errors.New("rate limited")},
		Store:      f,
		Collection: "agent_memory",
	}
	got, err := m.Search(context.Background(), ec("tenant-A", "thread-1", "run-x"), "查询", 10)
	if err != nil {
		t.Fatalf("embedder failure must degrade to (nil,nil), got err=%v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected no messages on degradation, got %d", len(got))
	}
}

// TestVectorMemory_DegradesOnStoreFailure 向量库不可用必须静默降级。
func TestVectorMemory_DegradesOnStoreFailure(t *testing.T) {
	f := vector.NewFake()
	_ = f.EnsureCollection(context.Background(), "agent_memory", 8)
	m := &VectorMemory{
		Embedder:   llm.StubEmbedder{Dim: 8},
		Store:      f,
		Collection: "agent_memory",
	}
	f.FailNext = true // 下一次 Search 返回错误
	got, err := m.Search(context.Background(), ec("tenant-A", "thread-1", "run-x"), "查询", 10)
	if err != nil {
		t.Fatalf("store failure must degrade to (nil,nil), got err=%v", err)
	}
	if len(got) != 0 {
		t.Errorf("expected no messages on degradation, got %d", len(got))
	}
}

// TestVectorMemory_DegradesOnMissingDependencies 依赖未装配时视为"记忆未启用"，
// 不报错——这使未配置向量库的部署可以安全地把 Memory 字段留空。
func TestVectorMemory_DegradesOnMissingDependencies(t *testing.T) {
	ctx := context.Background()
	cases := map[string]*VectorMemory{
		"nil embedder": {Store: vector.NewFake(), Collection: "c"},
		"nil store":    {Embedder: llm.StubEmbedder{Dim: 8}, Collection: "c"},
		"both nil":     {Collection: "c"},
	}
	for name, m := range cases {
		got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-x"), "查询", 10)
		if err != nil {
			t.Errorf("%s: expected (nil,nil), got err=%v", name, err)
		}
		if len(got) != 0 {
			t.Errorf("%s: expected no messages, got %d", name, len(got))
		}
	}
	// 完全未初始化的 nil receiver 也不能 panic（防御性：装配遗漏时的兜底）。
	var nilMem *VectorMemory
	if got, err := nilMem.Search(ctx, ec("tenant-A", "thread-1", "run-x"), "查询", 10); err != nil || len(got) != 0 {
		t.Errorf("nil receiver: expected (nil,nil), got (%v,%v)", got, err)
	}
}

// TestVectorMemory_DegradesOnMalformedEmbedding 返回空向量/数量不符也应降级，
// 而非带着空向量去查向量库（会得到无意义结果或触发底层错误）。
func TestVectorMemory_DegradesOnMalformedEmbedding(t *testing.T) {
	f := vector.NewFake()
	_ = f.EnsureCollection(context.Background(), "agent_memory", 8)

	cases := map[string]llm.Embedder{
		"returns no vector": fixedEmbedder{vecs: [][]float32{}},
		"returns empty vec": fixedEmbedder{vecs: [][]float32{{}}},
		"returns too many":  fixedEmbedder{vecs: [][]float32{{1}, {2}}},
	}
	for name, emb := range cases {
		m := &VectorMemory{Embedder: emb, Store: f, Collection: "agent_memory"}
		got, err := m.Search(context.Background(), ec("tenant-A", "thread-1", "run-x"), "查询", 10)
		if err != nil {
			t.Errorf("%s: expected (nil,nil), got err=%v", name, err)
		}
		if len(got) != 0 {
			t.Errorf("%s: expected no messages, got %d", name, len(got))
		}
	}
}

// fixedEmbedder 返回预置向量，用于构造畸形响应场景。
type fixedEmbedder struct{ vecs [][]float32 }

func (f fixedEmbedder) Embed(context.Context, []string) ([][]float32, error) { return f.vecs, nil }

// ==== Load / Save 语义 ====

// TestVectorMemory_LoadReturnsEmpty Load 无查询参数，无法表达语义召回，
// 必须返回空记忆而非 error，使调用方自然回退到"仅用当前 Run 历史"。
func TestVectorMemory_LoadReturnsEmpty(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	seed(t, m, "tenant-A", "thread-1", "node-1", "assistant", "内容", time.Now())

	got, err := m.Load(context.Background(), ec("tenant-A", "thread-1", "run-x"))
	if err != nil {
		t.Fatalf("Load must not error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("Load should return empty (no query available), got %d messages", len(got))
	}
}

// TestVectorMemory_SaveReturnsError Save 必须显式报错而非静默 no-op。
// 返回 nil 会让调用方误以为已持久化，从而静默丢数据。
func TestVectorMemory_SaveReturnsError(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	err := m.Save(context.Background(), ec("tenant-A", "thread-1", "run-x"), []contracts.Message{
		{Role: contracts.RoleUser, Content: "x"},
	})
	if err == nil {
		t.Fatal("Save should error: VectorMemory is read-only")
	}
}

// ==== 角色还原与集合名 ====

// TestRoleOrUser 未知或空角色必须归为 user（安全默认）。
func TestRoleOrUser(t *testing.T) {
	cases := map[string]contracts.Role{
		"system":      contracts.RoleSystem,
		"user":        contracts.RoleUser,
		"assistant":   contracts.RoleAssistant,
		"tool":        contracts.RoleTool,
		"":            contracts.RoleUser,
		"weird-value": contracts.RoleUser,
	}
	for in, want := range cases {
		if got := roleOrUser(in); got != want {
			t.Errorf("roleOrUser(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestVectorMemory_CollectionFallback 未配置集合名时使用默认值。
func TestVectorMemory_CollectionFallback(t *testing.T) {
	m := &VectorMemory{}
	if got := m.collection(); got != DefaultMemoryCollection {
		t.Errorf("expected %q, got %q", DefaultMemoryCollection, got)
	}
	m.Collection = "custom"
	if got := m.collection(); got != "custom" {
		t.Errorf("expected custom, got %q", got)
	}
	var nilMem *VectorMemory
	if got := nilMem.collection(); got != DefaultMemoryCollection {
		t.Errorf("nil receiver should fall back to default, got %q", got)
	}
}

// TestVectorMemory_EmptyTextSkipped 空文本的记忆应被跳过：
// 喂空消息给 LLM 只会浪费 token 并可能干扰模型。
func TestVectorMemory_EmptyTextSkipped(t *testing.T) {
	m, _ := newTestMemory(t, "agent_memory")
	ctx := context.Background()
	seed(t, m, "tenant-A", "thread-1", "node-empty", "assistant", "", time.Now())
	seed(t, m, "tenant-A", "thread-1", "node-real", "assistant", "真实内容", time.Now())

	got, err := m.Search(ctx, ec("tenant-A", "thread-1", "run-x"), "真实内容", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 1 || got[0].Content != "真实内容" {
		t.Errorf("expected only the non-empty memory, got %+v", got)
	}
}
