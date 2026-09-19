//go:build integration

// worker 记忆读取路径的集成测试：验证真实 MySQL + 真实向量库下的上下文构建与降级。
//
// 运行方式：
//
//	docker compose -f deploy/docker-compose.yml up -d
//	go test -tags=integration ./internal/worker/ -run TestMemory -v
//
// 单测（memory_test.go）已用 fake 覆盖了拼接、截断与降级分支；
// 本文件的价值在于把 **provider 层与 worker 层串起来**在真实依赖上验证：
// 真实 SQL（CompletedNodes / GetRun）与真实 gRPC 故障的组合行为，
// 这正是单测的任何一层都无法单独覆盖的部分。
package worker

import (
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/memory"
	"agent-runtime/internal/model"
	"agent-runtime/internal/providers"
	"agent-runtime/internal/store"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// newItIndexer 构造集成测试用的索引器，与 cmd/memory-indexer 的装配保持一致，
// 但关掉批间限速（BatchDelay=0）以免拖慢测试。
func newItIndexer(s *store.MySQL, vs vector.VectorStore, emb llm.Embedder) *memory.Indexer {
	return memory.New(emb, s, vs, memory.Config{
		Collection:   itCollection,
		Dim:          8,
		ScanLimit:    50,
		BatchSize:    8,
		BatchDelay:   0,
		PollInterval: time.Second,
	})
}

func testDSN() string {
	if v := os.Getenv("DATABASE_DSN"); v != "" {
		return v
	}
	return "agent:agent@tcp(localhost:3308)/agent_runtime?parseTime=true"
}

const itCollection = "agent_memory_worker_it"

// seedCompletedRun 创建一个 Run 并把一个 LLM 节点推进到 SUCCESS，
// 使 CompletedNodes 能返回真实数据。走与生产相同的 InsertPlan/Claim/Complete 路径，
// 确保 finished_at 等字段被正确填充。
func seedCompletedRun(t *testing.T, s *store.MySQL, tenant, threadID, runID, question, answer string) {
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
	if err := s.InsertPlan(ctx, runID, tenant, model.Plan{Nodes: []model.PlanNode{
		{ID: nodeID, Type: model.NodeLLM, Name: "reason", Input: question},
	}}); err != nil {
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

func cleanupWorkerTables(t *testing.T, s *store.MySQL) {
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

func newItStore(t *testing.T) *store.MySQL {
	t.Helper()
	s, err := store.New(context.Background(), testDSN())
	if err != nil {
		t.Skipf("MySQL 不可用，跳过集成测试: %v", err)
	}
	cleanupWorkerTables(t, s)
	return s
}

// TestMemoryContextLoader_RealStore_CurrentRunHistory 验证接入记忆后，
// 真实 SQL 路径仍能正确重建当前 Run 的对话历史（回归保护）。
//
// 这是最容易在重构中被破坏的部分：ContextLoader 从内联闭包改为 newContextLoader 后，
// 必须保证"未启用记忆"时的输出与改动前完全一致。
func TestMemoryContextLoader_RealStore_CurrentRunHistory(t *testing.T) {
	s := newItStore(t)
	defer s.Close()
	ctx := context.Background()

	const tenant, thread = "tenant-it", "thread-w1"
	seedCompletedRun(t, s, tenant, thread, "it-w-run1", "项目为什么延期", "上游依赖未就绪")

	// 记忆未启用（Memory=nil）：应与接入前行为一致。
	loader := newContextLoader(s, MemoryOptions{})
	got, err := loader(ctx, tenant, "it-w-run1")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("期望 2 条（input+output），实际 %d 条: %+v", len(got), got)
	}
	if got[0].Role != llm.RoleUser || got[0].Content != "项目为什么延期" {
		t.Errorf("第一条应为 user 提问: %+v", got[0])
	}
	if got[1].Role != llm.RoleAssistant || got[1].Content != "上游依赖未就绪" {
		t.Errorf("第二条应为 assistant 回答: %+v", got[1])
	}
}

// TestMemoryContextLoader_RealStore_DegradesWhenQdrantDown 是本文件**最关键**的一条：
// 真实 MySQL 可用、真实向量库不可达时，ContextLoader 必须仍返回当前 Run 历史且不报错。
//
// 它把 provider 层与 worker 层串起来验证完整的降级链路：
// 单测只能证明"fake 报错时会降级"，而这里证明的是真实 gRPC 连接失败
// （惰性连接、首次调用才暴露错误）时，节点执行不会被记忆故障拖垮。
//
// 一旦这条失败，后果是向量库宕机会让所有 LLM 节点失败——
// 记忆从"增强项"退化成"单点故障"，正是设计时要杜绝的情况。
func TestMemoryContextLoader_RealStore_DegradesWhenQdrantDown(t *testing.T) {
	s := newItStore(t)
	defer s.Close()
	ctx := context.Background()

	const tenant, thread = "tenant-it", "thread-w2"
	seedCompletedRun(t, s, tenant, thread, "it-w-run2", "查询问题", "查询回答")

	// 指向确定不可达的向量库地址（本机未开放端口）。
	vs, err := vector.NewQdrant("127.0.0.1", 1, "")
	if err != nil {
		t.Fatalf("NewQdrant 构造失败（预期惰性连接、不应在此报错）: %v", err)
	}
	defer vs.Close()

	mem := &providers.VectorMemory{
		Embedder:   llm.StubEmbedder{},
		Store:      vs,
		Collection: itCollection,
		MinScore:   0,
	}
	loader := newContextLoader(s, MemoryOptions{
		Memory:        mem,
		SearchTimeout: 3 * time.Second,
		MaxMessages:   20,
	})

	got, err := loader(ctx, tenant, "it-w-run2")
	if err != nil {
		t.Fatalf("向量库不可达时 ContextLoader 必须降级而非报错，实际: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("降级后应仍返回当前 Run 的 2 条历史，实际 %d 条", len(got))
	}
	if got[1].Content != "查询回答" {
		t.Errorf("当前 Run 历史内容不正确: %+v", got)
	}
}

// TestMemoryContextLoader_RealStore_EndToEndRecall 验证真实端到端召回：
// 先用索引器把上一个 Run 投影进向量库，再让下一个 Run 的 ContextLoader 召回它。
//
// 这是跨进程协作的验证——写入路径（cmd/memory-indexer 的核心逻辑）与
// 读取路径（worker 的 ContextLoader）通过真实向量库衔接。
func TestMemoryContextLoader_RealStore_EndToEndRecall(t *testing.T) {
	s := newItStore(t)
	defer s.Close()
	ctx := context.Background()

	vs, err := vector.NewQdrant(
		envOr("QDRANT_HOST", "localhost"),
		envIntOr("QDRANT_PORT", 6334),
		os.Getenv("QDRANT_API_KEY"),
	)
	if err != nil {
		s.Close()
		t.Skipf("向量库不可用，跳过: %v", err)
	}
	defer vs.Close()

	embedder := llm.StubEmbedder{Dim: 8}
	if err := vs.EnsureCollection(ctx, itCollection, 8); err != nil {
		s.Close()
		t.Skipf("向量库连接失败，跳过: %v", err)
	}

	const tenant, thread = "tenant-it", "thread-w3"
	const secret = "支付网关密钥每 90 天轮换一次"

	// 上一个 Run：包含需要被跨 Run 记住的事实。
	seedCompletedRun(t, s, tenant, thread, "it-w-prev", "密钥多久轮换", secret)

	// 写入路径：投影到向量库（这里直接调用索引器核心，等价于 cmd/memory-indexer 的一轮）。
	indexer := newItIndexer(s, vs, embedder)
	if _, err := indexer.RunOnce(ctx); err != nil {
		t.Fatalf("索引器投影失败: %v", err)
	}

	// 当前 Run：尚未有任何完成节点，因此 CompletedNodes 为空。
	run := &model.Run{
		ID: "it-w-cur", TenantID: tenant, ThreadID: thread, AgentID: "agent-it",
		Status: model.RunRunning, Version: 0, Input: secret, MaxSteps: 10,
	}
	if err := s.CreateRun(ctx, run); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}

	mem := &providers.VectorMemory{
		Embedder: embedder, Store: vs, Collection: itCollection, MinScore: 0, TopK: 10,
	}
	loader := newContextLoader(s, MemoryOptions{Memory: mem, SearchTimeout: 5 * time.Second})

	got, err := loader(ctx, tenant, "it-w-cur")
	if err != nil {
		t.Fatalf("loader: %v", err)
	}
	// 当前 Run 无已完成节点 → 上下文应全部来自跨 Run 记忆。
	if len(got) == 0 {
		t.Fatal("未能召回上一个 Run 的记忆——写入路径与读取路径未正确衔接")
	}
	found := false
	for _, m := range got {
		if m.Content == secret {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("召回内容不含预期事实，实际: %+v", got)
	}
}

// TestNewFromEnv_MemoryEnabledButQdrantDown 验证 worker 装配在向量库不可达时不失败。
//
// NewQdrant 是惰性连接，因此装配通常"成功"，真正的故障要到检索时才暴露——
// 这恰好是期望的行为：worker 必须能启动并处理节点，
// 而不是因为一个可选的记忆依赖不可用就整体起不来。
func TestNewFromEnv_MemoryEnabledButQdrantDown(t *testing.T) {
	t.Setenv("MEMORY_ENABLED", "true")
	t.Setenv("OPENAI_API_KEY", "sk-integration-fake")
	t.Setenv("OPENAI_BASE_URL", "http://127.0.0.1:9/v1")
	t.Setenv("QDRANT_HOST", "127.0.0.1")
	t.Setenv("QDRANT_PORT", "1") // 不可达

	opt := newMemoryOptionsFromEnv(newCredentialsFromEnv())
	// 惰性连接下装配会"成功"拿到 Memory；关键是它绝不能 panic 或让 worker 起不来。
	if opt.Memory == nil {
		t.Log("装配阶段即降级为记忆关闭（NewQdrant 返回错误）——同样可接受")
		return
	}
	// 装配成功的情况下，必须能在检索时降级：用真实不可达地址验证一次。
	s := newItStore(t)
	defer s.Close()
	seedCompletedRun(t, s, "tenant-it", "thread-env", "it-w-env", "q", "a")

	loader := newContextLoader(s, opt)
	got, err := loader(context.Background(), "tenant-it", "it-w-env")
	if err != nil {
		t.Fatalf("向量库不可达时装配出的 loader 仍须降级，实际报错: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("期望降级后返回当前 Run 的 2 条历史，实际 %d 条", len(got))
	}
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
