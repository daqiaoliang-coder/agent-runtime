//go:build integration

// 记忆向量检索的集成测试：验证真实 MySQL + 真实向量库下的端到端行为。
//
// 运行方式（需先启动依赖服务并应用迁移）：
//
//	docker compose -f deploy/docker-compose.yml up -d
//	mysql ... < migrations/012_run_thread.sql
//	mysql ... < migrations/013_memory_indexed.sql
//	OPENAI_BASE_URL=... OPENAI_API_KEY=... \
//	  go test -tags=integration ./internal/providers/ -run TestMemory -v
//
// 单测（memory_vector_test.go）已用 fake 向量库覆盖了召回、隔离与降级的逻辑分支；
// 本文件的价值在于验证**单测覆盖不到的部分**：
//   - 真实 SQL（JOIN agent_run 取 thread_id、LEFT JOIN memory_indexed 取增量）是否正确；
//   - Qdrant 的 payload filter / must_not / point ID 覆盖语义是否与 fake 一致；
//   - 写入路径与读取路径跨进程协作后，会话隔离与幂等是否真的成立。
package providers

import (
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/memory"
	"agent-runtime/internal/model"
	"agent-runtime/internal/store"
	"context"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// testDSN 返回集成测试用的 DSN，与 internal/store/integration_test.go 保持一致。
func testDSN() string {
	if v := os.Getenv("DATABASE_DSN"); v != "" {
		return v
	}
	return "agent:agent@tcp(localhost:3308)/agent_runtime?parseTime=true"
}

// collection 为本次测试使用独立的集合名，避免污染默认的 agent_memory，
// 并在结束后清理。
const itCollection = "agent_memory_it"

// memoryFixture 打包一次记忆链路测试所需的全部真实依赖。
type memoryFixture struct {
	store    *store.MySQL
	vectors  *vector.Qdrant
	indexer  *memory.Indexer
	search   *VectorMemory
	embedder llm.Embedder
	dim      int
}

// newMemoryFixture 连接真实 MySQL 与向量库，建集合、清理数据，返回夹具与清理函数。
//
// embedding 优先使用真实网关（EMBEDDING_BASE_URL/KEY 或 OPENAI_*）；
// 未配置时回退到 StubEmbedder——这使"验证 SQL 与 Qdrant 交互"这类结构性问题
// 无需付费网关也能测，但要注意 StubEmbedder 不具备语义相似性，
// 涉及"语义相关性"的断言只在真实网关下有意义（见 TestMemory_SemanticRecall）。
func newMemoryFixture(t *testing.T) *memoryFixture {
	t.Helper()
	ctx := context.Background()

	s, err := store.New(ctx, testDSN())
	if err != nil {
		t.Skipf("MySQL 不可用，跳过集成测试: %v", err)
	}

	vs, err := vector.NewQdrant(
		envOr("QDRANT_HOST", "localhost"),
		envIntOr("QDRANT_PORT", 6334),
		os.Getenv("QDRANT_API_KEY"),
	)
	if err != nil {
		s.Close()
		t.Skipf("向量库不可用，跳过集成测试: %v", err)
	}

	f := &memoryFixture{store: s, vectors: vs}
	f.embedder = newEmbedder(t)

	// 集合维度必须取自 embedding 的**实际输出**，而不是读环境变量：
	// 若二者不符（例如换了模型却忘了改 EMBEDDING_DIM），Qdrant 会拒绝写入，
	// 而报错信息只说向量长度不对，极难定位到配置层面。这里主动探测一次。
	dim, err := probeEmbeddingDim(ctx, f.embedder)
	if err != nil {
		_ = vs.Close()
		s.Close()
		t.Skipf("embedding 探测失败，跳过集成测试: %v", err)
	}
	f.dim = dim

	// 探测向量库是否真的可达：NewQdrant 只建立配置，不发请求，
	// 必须实际调用一次才能确认服务在线，否则后续测试会报难以理解的错误。
	if err := vs.EnsureCollection(ctx, itCollection, dim); err != nil {
		_ = vs.Close()
		s.Close()
		t.Skipf("向量库连接失败，跳过集成测试: %v", err)
	}

	f.indexer = memory.New(f.embedder, s, vs, memory.Config{
		Collection:   itCollection,
		Dim:          dim,
		ScanLimit:    50,
		BatchSize:    8,
		BatchDelay:   0, // 集成测试不需要限速
		PollInterval: time.Second,
	})
	f.search = &VectorMemory{
		Embedder:   f.embedder,
		Store:      vs,
		Collection: itCollection,
		TopK:       10,
		MinScore:   0, // 集成测试关注"能否召回/是否隔离"，不用阈值筛掉结果
	}
	cleanupMemoryTables(t, s)
	return f
}

// probeEmbeddingDim 通过实际调用一次 embedding 来确定向量维度。
func probeEmbeddingDim(ctx context.Context, e llm.Embedder) (int, error) {
	vecs, err := e.Embed(ctx, []string{"dim-probe"})
	if err != nil {
		return 0, err
	}
	if len(vecs) != 1 || len(vecs[0]) == 0 {
		return 0, fmt.Errorf("embedding 返回了 %d 个向量", len(vecs))
	}
	return len(vecs[0]), nil
}

func (f *memoryFixture) Close() {
	if f.vectors != nil {
		// 关闭 gRPC 连接。测试集合保留在库中以便排查，
		// 每个用例开头都会 TRUNCATE 业务表，因此不会相互污染。
		_ = f.vectors.Close()
	}
	if f.store != nil {
		f.store.Close()
	}
}

// newEmbedder 按环境选择真实网关或确定性 Stub。
// 不再接收 dim 参数：集合维度由 probeEmbeddingDim 从实际输出探测得出，
// StubEmbedder 使用其自身默认维度（8）。
func newEmbedder(t *testing.T) llm.Embedder {
	t.Helper()
	base := envOr("EMBEDDING_BASE_URL", os.Getenv("OPENAI_BASE_URL"))
	key := os.Getenv("EMBEDDING_API_KEY")
	if key == "" {
		key = os.Getenv("OPENAI_API_KEY")
	}
	if base != "" && key != "" {
		t.Logf("使用真实 embedding 网关: %s (model=%s)", base, envOr("EMBEDDING_MODEL", "text-embedding-3-small"))
		return llm.NewOpenAIEmbedder(base, key, envOr("EMBEDDING_MODEL", "text-embedding-3-small"))
	}
	t.Logf("未配置 embedding 网关，使用 StubEmbedder——语义相关性断言将被跳过")
	return llm.StubEmbedder{}
}

// usingStubEmbedder 判断当前是否在 Stub 模式下运行。
func usingStubEmbedder(e llm.Embedder) bool {
	_, ok := e.(llm.StubEmbedder)
	return ok
}

// cleanupMemoryTables 清空本次测试涉及的表，保证用例互不污染。
// 与 internal/store/integration_test.go 同样的做法：先关外键再 TRUNCATE。
func cleanupMemoryTables(t *testing.T, s *store.MySQL) {
	t.Helper()
	ctx := context.Background()
	if _, err := s.DB.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=0"); err != nil {
		t.Fatalf("disable FK: %v", err)
	}
	for _, tbl := range []string{"memory_indexed", "agent_node", "agent_edge", "agent_run"} {
		if _, err := s.DB.ExecContext(ctx, fmt.Sprintf("TRUNCATE TABLE %s", tbl)); err != nil {
			t.Logf("truncate %s: %v", tbl, err)
		}
	}
	if _, err := s.DB.ExecContext(ctx, "SET FOREIGN_KEY_CHECKS=1"); err != nil {
		t.Fatalf("enable FK: %v", err)
	}
}

// seedRun 创建一个 Run 并写入一个已完成的 LLM 节点，模拟"上一轮对话已发生"。
// 走真实的 InsertPlan/ClaimNode/CompleteNode 路径，确保 finished_at 等字段被正确填充
// （索引器依赖 finished_at 作为记忆的 created_at）。
func seedRun(t *testing.T, s *store.MySQL, tenant, threadID, runID, question, answer string) {
	t.Helper()
	ctx := context.Background()

	run := &model.Run{
		ID: runID, TenantID: tenant, ThreadID: threadID, AgentID: "agent-it",
		Status: model.RunRunning, Version: 0, Input: question, MaxSteps: 10,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun(%s): %v", runID, err)
	}
	nodeID := runID + ":n1"
	plan := model.Plan{Nodes: []model.PlanNode{
		{ID: nodeID, Type: model.NodeLLM, Name: "reason", Input: question},
	}}
	if err := s.InsertPlan(ctx, runID, tenant, plan); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	if err := s.MarkReady(ctx, tenant, nodeID); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}
	n, err := s.GetNode(ctx, tenant, nodeID)
	if err != nil {
		t.Fatalf("GetNode: %v", err)
	}
	ok, err := s.ClaimNode(ctx, tenant, nodeID, n.Version, "worker-it", 60*time.Second)
	if err != nil || !ok {
		t.Fatalf("ClaimNode: ok=%v err=%v", ok, err)
	}
	n2, _ := s.GetNode(ctx, tenant, nodeID)
	if _, err := s.CompleteNode(ctx, tenant, nodeID, n2.Version, answer); err != nil {
		t.Fatalf("CompleteNode: %v", err)
	}
}

// ==== 端到端链路 ====

// TestMemory_EndToEnd_ThreadIsolation 验证完整链路与会话隔离：
// 写入一个会话的记忆 → 索引器投影 → 同会话可召回、异会话不可召回、异租户不可召回。
//
// 这是本方案最核心的不变量。三条隔离断言分别对应三个真实故障模式：
// 会话串味（跨 thread 泄漏）、租户越权（跨 tenant 泄漏）、以及召回完全失效。
func TestMemory_EndToEnd_ThreadIsolation(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	ctx := context.Background()

	const tenant = "tenant-it"
	const secret = "支付网关的密钥轮换周期是 90 天"
	seedRun(t, f.store, tenant, "thread-A", "it-run-a", "支付密钥多久轮换一次", secret)

	// 索引器投影
	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("indexer RunOnce: %v", err)
	}

	// 同会话应能召回
	got, err := f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: tenant, ThreadID: "thread-A", RunID: "it-run-b",
	}, secret, 10)
	if err != nil {
		t.Fatalf("search same thread: %v", err)
	}
	if len(got) == 0 {
		t.Fatal("同会话应召回到记忆，但结果为空——写入或召回链路断了")
	}
	if !containsText(got, secret) {
		t.Errorf("召回内容不含预期文本: %+v", got)
	}

	// 异会话不可召回（会话隔离）
	got, err = f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: tenant, ThreadID: "thread-B", RunID: "it-run-b",
	}, secret, 10)
	if err != nil {
		t.Fatalf("search other thread: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("跨会话泄漏: thread-B 召回到 %d 条属于 thread-A 的记忆", len(got))
	}

	// 异租户不可召回（越权防线）
	got, err = f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: "tenant-other", ThreadID: "thread-A", RunID: "it-run-b",
	}, secret, 10)
	if err != nil {
		t.Fatalf("search other tenant: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("跨租户越权: tenant-other 召回到 %d 条记忆", len(got))
	}
}

// TestMemory_ExcludesCurrentRun 验证召回会排除当前 Run 自身的记忆。
//
// 当前 Run 的节点完成后也会被投影进向量库，而其历史已由 worker 从
// 已提交节点单独提供；若不排除，同一段对话会在上下文里出现两次。
// 由于 Search 返回的 Message 不含 node_id，调用方无法在下游去重，
// 因此必须在向量层源头排除。
func TestMemory_ExcludesCurrentRun(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	ctx := context.Background()

	const tenant, thread = "tenant-it", "thread-x"
	const text = "这段内容同时存在于两个 Run"
	seedRun(t, f.store, tenant, thread, "it-run-cur", text, text)
	seedRun(t, f.store, tenant, thread, "it-run-old", text, text)

	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("indexer RunOnce: %v", err)
	}

	// 以 it-run-cur 的身份召回：只应看到 it-run-old 的内容。
	got, err := f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: tenant, ThreadID: thread, RunID: "it-run-cur",
	}, text, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	// it-run-old 贡献 input+output 两条（文本相同），it-run-cur 的两条必须被排除。
	if len(got) != 2 {
		t.Errorf("当前 Run 未被排除: 期望 2 条（来自 it-run-old），实际 %d 条", len(got))
	}

	// 换一个无关 RunID：两个 Run 的记忆都应可见。
	got, err = f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: tenant, ThreadID: thread, RunID: "it-run-unrelated",
	}, text, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("期望 4 条（两个 Run 各 2 条），实际 %d 条", len(got))
	}
}

// ==== 幂等与崩溃安全 ====

// TestMemory_ReplayIsIdempotent 验证重复投影不产生重复记忆。
//
// 这是崩溃恢复的基础：索引器在"向量已写入、进度未标记"之间被 kill 后，
// 重启会重扫同一批节点。若 point ID 不确定，就会累积重复向量，
// 召回时同一段对话反复出现，污染上下文并浪费 token。
func TestMemory_ReplayIsIdempotent(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	ctx := context.Background()

	const tenant, thread = "tenant-it", "thread-idem"
	const text = "幂等性验证内容"
	seedRun(t, f.store, tenant, thread, "it-run-idem", text, text)

	// 第一轮正常投影。
	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("first RunOnce: %v", err)
	}
	got1, err := f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: tenant, ThreadID: thread, RunID: "other",
	}, text, 10)
	if err != nil {
		t.Fatalf("search after first round: %v", err)
	}

	// 模拟崩溃恢复：删掉进度记录，让同一批节点重新变为"未索引"，
	// 相当于索引器在标记进度前被 kill、重启后重扫。
	if _, err := f.store.DB.ExecContext(ctx, "DELETE FROM memory_indexed"); err != nil {
		t.Fatalf("delete progress rows: %v", err)
	}
	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	got2, err := f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: tenant, ThreadID: thread, RunID: "other",
	}, text, 10)
	if err != nil {
		t.Fatalf("search after replay: %v", err)
	}

	if len(got1) != len(got2) {
		t.Errorf("重放后召回条数变化: %d -> %d（说明产生了重复记忆）", len(got1), len(got2))
	}
	if len(got2) != 2 {
		t.Errorf("期望 2 条（input+output），实际 %d 条", len(got2))
	}
}

// TestMemory_ProgressTableTracksIndexed 验证进度表与实际投影一致，
// 且已投影的节点不会被重复扫描（增量扫描生效）。
func TestMemory_ProgressTableTracksIndexed(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	ctx := context.Background()

	const tenant, thread = "tenant-it", "thread-prog"
	seedRun(t, f.store, tenant, thread, "it-run-p1", "问题一", "回答一")
	seedRun(t, f.store, tenant, thread, "it-run-p2", "问题二", "回答二")

	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	n, err := f.store.CountMemoryIndexed(ctx)
	if err != nil {
		t.Fatalf("CountMemoryIndexed: %v", err)
	}
	if n != 2 {
		t.Errorf("期望进度表记录 2 个节点，实际 %d", n)
	}

	// 再跑一轮：无新增节点，不应重复投影。
	before := n
	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("second RunOnce: %v", err)
	}
	after, _ := f.store.CountMemoryIndexed(ctx)
	if after != before {
		t.Errorf("进度表行数从 %d 变为 %d，说明增量扫描失效、重复处理了已投影节点", before, after)
	}
}

// TestMemory_SkipsToolAndReflectNodes 验证只投影对话型节点。
//
// TOOL 节点的 output 是工具原始返回（体积大、噪声多），
// REFLECT 节点的 output 是机器决策 JSON（{"action":"replan",...}），
// 二者向量化后都会在召回时污染对话上下文。
func TestMemory_SkipsToolAndReflectNodes(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	ctx := context.Background()

	const tenant = "tenant-it"
	runID := "it-run-types"
	run := &model.Run{
		ID: runID, TenantID: tenant, ThreadID: "thread-types", AgentID: "agent-it",
		Status: model.RunRunning, Version: 0, Input: "q", MaxSteps: 10,
	}
	if err := f.store.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	plan := model.Plan{Nodes: []model.PlanNode{
		{ID: runID + ":llm", Type: model.NodeLLM, Name: "reason", Input: "对话问题"},
		{ID: runID + ":tool", Type: model.NodeTool, Name: "search", Input: "搜索词"},
		{ID: runID + ":reflect", Type: model.NodeReflect, Name: "reflect", Input: "反思输入"},
	}}
	if err := f.store.InsertPlan(ctx, runID, tenant, plan); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	// 把三个节点都推进到 SUCCESS。
	for _, suffix := range []struct{ id, out string }{
		{":llm", "这是对话回答"},
		{":tool", `{"results":["工具原始返回"]}`},
		{":reflect", `{"action":"finish","reason":"done"}`},
	} {
		nodeID := runID + suffix.id
		if err := f.store.MarkReady(ctx, tenant, nodeID); err != nil {
			t.Fatalf("MarkReady(%s): %v", nodeID, err)
		}
		n, _ := f.store.GetNode(ctx, tenant, nodeID)
		if _, err := f.store.ClaimNode(ctx, tenant, nodeID, n.Version, "w-it", 60*time.Second); err != nil {
			t.Fatalf("ClaimNode(%s): %v", nodeID, err)
		}
		n2, _ := f.store.GetNode(ctx, tenant, nodeID)
		if _, err := f.store.CompleteNode(ctx, tenant, nodeID, n2.Version, suffix.out); err != nil {
			t.Fatalf("CompleteNode(%s): %v", nodeID, err)
		}
	}

	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// 只有 LLM 节点被投影（input+output 共 2 条）。
	got, err := f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: tenant, ThreadID: "thread-types", RunID: "other",
	}, "对话问题", 20)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("期望只投影 LLM 节点（2 条），实际 %d 条", len(got))
	}
	for _, m := range got {
		if strings.Contains(m.Content, "results") || strings.Contains(m.Content, "action") {
			t.Errorf("TOOL/REFLECT 节点内容被错误投影进记忆: %q", m.Content)
		}
	}
}

// ==== thread_id 落库 ====

// TestMemory_ThreadIDPersisted 验证 thread_id 真正落库并可读回，
// 且索引器能通过 JOIN 正确取到它（这是会话隔离的数据来源）。
func TestMemory_ThreadIDPersisted(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	ctx := context.Background()

	seedRun(t, f.store, "tenant-it", "thread-persist", "it-run-persist", "q", "a")

	got, err := f.store.GetRun(ctx, "tenant-it", "it-run-persist")
	if err != nil {
		t.Fatalf("GetRun: %v", err)
	}
	if got.ThreadID != "thread-persist" {
		t.Errorf("thread_id 未正确落库或读回: %q", got.ThreadID)
	}

	// 索引器扫描时应能从 JOIN 取到 thread_id，并写进进度表。
	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	var threadInIndex string
	err = f.store.DB.QueryRowContext(ctx,
		`SELECT thread_id FROM memory_indexed WHERE run_id=?`, "it-run-persist").Scan(&threadInIndex)
	if err != nil {
		if err == sql.ErrNoRows {
			t.Fatal("进度表无记录，索引器未投影该节点")
		}
		t.Fatalf("query memory_indexed: %v", err)
	}
	if threadInIndex != "thread-persist" {
		t.Errorf("索引器取到的 thread_id 不正确: %q", threadInIndex)
	}
}

// TestMemory_LegacyEmptyThreadID 验证存量数据兼容：thread_id 为空串的 Run，
// 其记忆在"查询侧 threadID 也为空"时仍可召回。
//
// 迁移前所有 Run 的 thread_id 都是空串。若空串也参与严格匹配，
// 升级后新会话将查不到任何旧记忆——这是静默失效，极难排查。
func TestMemory_LegacyEmptyThreadID(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	ctx := context.Background()

	const text = "存量会话的记忆内容"
	// threadID 传空串，模拟迁移前的存量 Run。
	seedRun(t, f.store, "tenant-it", "", "it-run-legacy", text, text)

	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// 查询侧 threadID 也为空 → 不按会话过滤 → 应能召回。
	got, err := f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: "tenant-it", ThreadID: "", RunID: "other",
	}, text, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) == 0 {
		t.Error("存量数据（thread_id 为空）应可被空 threadID 的查询召回，实际为空")
	}

	// 但指定了具体会话的查询不应看到存量数据（避免串味）。
	got, err = f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: "tenant-it", ThreadID: "thread-new", RunID: "other",
	}, text, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("指定会话的查询不应召回存量无会话数据，实际 %d 条", len(got))
	}
}

// ==== 真实故障下的降级（本文件最关键的一条）====

// TestMemory_DegradesWhenQdrantUnreachable 验证向量库**真实不可达**时检索静默降级。
//
// 为什么必须用真实连接而非 fake：单测（memory_vector_test.go）里的降级用例注入的是
// vector.Fake 返回的错误，而真实故障是 gRPC 层的——连接被拒、DNS 解析失败、
// 协议不匹配（例如把 REST 的 6333 填成 QDRANT_PORT）。这些错误的传播路径与
// fake 完全不同：gRPC 是**惰性连接**，NewQdrant 不会立刻失败，
// 错误要到首次调用时才浮现。只有真连一次才能证明降级链路是通的。
//
// 断言的是"绝不阻断主链路"这条硬约束：记忆检索失败必须返回 (nil, nil)，
// 一旦向上抛错，executor 会让 LLM 节点失败（executor_test 已锁定该传播行为），
// 于是"向量库宕机"会升级成"所有 Run 失败"。
func TestMemory_DegradesWhenQdrantUnreachable(t *testing.T) {
	ctx := context.Background()

	// 指向一个确定不可达的地址（本机未开放端口）。
	// 刻意不复用 newMemoryFixture：夹具连不上会直接 Skip，测不到降级行为。
	vs, err := vector.NewQdrant("127.0.0.1", 1, "")
	if err != nil {
		// gRPC 惰性连接，通常这里不会失败；若失败则说明连构造都过不去。
		t.Fatalf("NewQdrant 构造失败（预期惰性连接、不应在此报错）: %v", err)
	}
	defer vs.Close()

	m := &VectorMemory{
		Embedder:   llm.StubEmbedder{},
		Store:      vs,
		Collection: itCollection,
		MinScore:   0,
	}

	// 用带超时的 ctx，避免真实 gRPC 拨号长时间阻塞测试。
	sctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	got, err := m.Search(sctx, contracts.ExecutionContext{
		TenantID: "tenant-it", ThreadID: "thread-A", RunID: "it-run-x",
	}, "任意查询", 10)
	if err != nil {
		t.Fatalf("向量库不可达时必须降级为 (nil,nil)，实际返回 error: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("降级时不应返回任何记忆，实际 %d 条", len(got))
	}
}

// TestMemory_EnsureCollectionDimensionMismatch 验证集合维度与 embedding 输出不符时
// 必须**显式报错**，而不是静默写入。
//
// 换 embedding 模型（如 1536 → 3072 维）是最容易踩的运维坑：若不校验，
// 向量库会返回只说"向量长度不对"的底层错误，很难定位到配置层面。
func TestMemory_EnsureCollectionDimensionMismatch(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	ctx := context.Background()

	// 夹具已用 f.dim 建好集合，再以一个不同维度请求，应当报错。
	wrongDim := f.dim + 1
	if err := f.vectors.EnsureCollection(ctx, itCollection, wrongDim); err == nil {
		t.Errorf("维度不符（集合为 %d，请求 %d）应报错而非静默通过", f.dim, wrongDim)
	}
	// 相同维度应幂等通过。
	if err := f.vectors.EnsureCollection(ctx, itCollection, f.dim); err != nil {
		t.Errorf("相同维度应幂等通过，实际报错: %v", err)
	}
}

// ==== 语义相关性（仅真实网关下有意义）====

// TestMemory_SemanticRecall 验证语义召回：用**不同措辞**查询，仍能召回相关记忆。
//
// 这条测试只在真实 embedding 网关下有意义。StubEmbedder 是哈希伪向量，
// "项目延期"与"进度推迟"在向量空间里毫无关系，因此 Stub 模式下自动跳过。
func TestMemory_SemanticRecall(t *testing.T) {
	f := newMemoryFixture(t)
	defer f.Close()
	if usingStubEmbedder(f.embedder) {
		t.Skip("StubEmbedder 不具备语义相似性，跳过语义召回验证")
	}
	ctx := context.Background()

	const tenant, thread = "tenant-it", "thread-semantic"
	seedRun(t, f.store, tenant, thread, "it-run-sem",
		"项目进度如何", "由于上游依赖延迟，项目整体推迟两周交付")

	if _, err := f.indexer.RunOnce(ctx); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// 用与原文**措辞不同**但语义相关的查询召回。
	got, err := f.search.Search(ctx, contracts.ExecutionContext{
		TenantID: tenant, ThreadID: thread, RunID: "other",
	}, "交付时间有没有变化", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) == 0 {
		t.Error("语义相关的查询未能召回记忆")
	}
}

// ==== 辅助 ====

func containsText(msgs []contracts.Message, want string) bool {
	for _, m := range msgs {
		if strings.Contains(m.Content, want) {
			return true
		}
	}
	return false
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func envIntOr(k string, d int) int {
	if v := os.Getenv(k); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return d
}
