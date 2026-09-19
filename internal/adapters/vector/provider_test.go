package vector

import (
	"context"
	"testing"
	"time"
)

// TestPointID_Deterministic 锁定 PointID 的输出：同一输入永远得到同一 ID。
//
// 这是**契约级**测试。索引器崩溃重启、节点重试、全量重建都依赖它——
// 一旦生成规则变更，存量向量会变成无法去重的孤儿数据，只能清空 collection 重建。
// 因此这里断言具体的数值输出，而不只是"两次调用相等"，
// 使得任何对该函数的修改都会显式失败、迫使实现者意识到数据兼容后果。
func TestPointID_Deterministic(t *testing.T) {
	const tenant, thread, nodeID, role = "tenant-A", "thread-1", "node-42", "assistant"

	first := PointID(tenant, thread, nodeID, role)
	for i := 0; i < 100; i++ {
		if got := PointID(tenant, thread, nodeID, role); got != first {
			t.Fatalf("PointID not deterministic: %d != %d", got, first)
		}
	}
	// 锁定的期望值：SHA256("tenant-A|thread-1|node-42|assistant") 前 8 字节 big-endian。
	// 该值已用独立脚本复算核对（sha256=32c8e07e60b51e7515390fc68a34d9e88c9afc2053625cc60b5d66aa195f1d6b）。
	// 修改 PointID 实现会使本测试失败，这是刻意的。
	const want uint64 = 3659421530631511669
	if first != want {
		t.Errorf("PointID changed from the locked value: got %d want %d\n"+
			"⚠️ 变更 PointID 会使存量向量无法去重，需清空 collection 并全量重建索引。", first, want)
	}
}

// TestPointID_DistinctOnEveryDimension 任一维度不同都必须产生不同 ID。
// 否则不同租户/会话/节点的记忆会互相覆盖，造成越权可见或数据丢失。
func TestPointID_DistinctOnEveryDimension(t *testing.T) {
	base := PointID("tenant-A", "thread-1", "node-1", "assistant")
	cases := map[string]uint64{
		"different tenant": PointID("tenant-B", "thread-1", "node-1", "assistant"),
		"different thread": PointID("tenant-A", "thread-2", "node-1", "assistant"),
		"different node":   PointID("tenant-A", "thread-1", "node-2", "assistant"),
		"different role":   PointID("tenant-A", "thread-1", "node-1", "user"),
	}
	for name, got := range cases {
		if got == base {
			t.Errorf("%s produced the same PointID (%d) — memories would collide", name, got)
		}
	}
}

// TestPointID_SeparatorInjection 字段拼接必须有分隔符保护：
// 否则 ("ab","c") 与 ("a","bc") 会得到相同 ID，导致不同记忆相互覆盖。
func TestPointID_SeparatorInjection(t *testing.T) {
	a := PointID("tenant", "ab", "c", "role")
	b := PointID("tenant", "a", "bc", "role")
	if a == b {
		t.Error("field boundary collision: distinct (thread,node) pairs produced the same PointID")
	}
}

func testPoint(tenant, thread, nodeID, role, text string, at time.Time) Point {
	return Point{
		ID:        PointID(tenant, thread, nodeID, role),
		Vector:    []float32{1, 0, 0},
		TenantID:  tenant,
		ThreadID:  thread,
		RunID:     "run-" + nodeID,
		NodeID:    nodeID,
		Role:      role,
		Text:      text,
		CreatedAt: at,
	}
}

// TestFake_UpsertSearchRoundTrip 写入后必须能召回，且 payload 字段完整还原。
func TestFake_UpsertSearchRoundTrip(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	if err := f.EnsureCollection(ctx, "agent_memory", 3); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	now := time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)
	p := testPoint("tenant-A", "thread-1", "node-1", "assistant", "项目延期因为依赖未就绪", now)
	if err := f.Upsert(ctx, "agent_memory", []Point{p}); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	hits, err := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}
	got := hits[0].Point
	if got.NodeID != "node-1" || got.Role != "assistant" || got.Text != p.Text {
		t.Errorf("payload not round-tripped: %+v", got)
	}
	if !got.CreatedAt.Equal(now) {
		t.Errorf("created_at lost precision: got %v want %v", got.CreatedAt, now)
	}
	if got.TenantID != "tenant-A" || got.ThreadID != "thread-1" {
		t.Errorf("isolation keys not round-tripped: %+v", got)
	}
}

// TestFake_UpsertIsIdempotent 相同 ID 重复写入不得增加点数。
// 这是"索引器重放安全"的核心不变量——崩溃重启后重扫同一批节点，
// 若点数增长说明产生了重复记忆，召回时会出现重复内容污染上下文。
func TestFake_UpsertIsIdempotent(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	p := testPoint("tenant-A", "thread-1", "node-1", "assistant", "text", time.Now())

	for i := 0; i < 5; i++ {
		if err := f.Upsert(ctx, "agent_memory", []Point{p}); err != nil {
			t.Fatalf("upsert #%d: %v", i, err)
		}
		if n := f.Count("agent_memory"); n != 1 {
			t.Fatalf("replay #%d produced %d points, want 1 (duplicate memory)", i, n)
		}
	}
}

// TestFake_UpsertOverwritesSameID 相同 ID 但内容变化时应覆盖，而非保留旧值。
func TestFake_UpsertOverwritesSameID(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	now := time.Now()
	_ = f.Upsert(ctx, "agent_memory", []Point{testPoint("tenant-A", "thread-1", "node-1", "assistant", "old", now)})
	_ = f.Upsert(ctx, "agent_memory", []Point{testPoint("tenant-A", "thread-1", "node-1", "assistant", "new", now)})
	hits, _ := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 10)
	if len(hits) != 1 {
		t.Fatalf("expected 1 hit, got %d", len(hits))
	}
	if hits[0].Text != "new" {
		t.Errorf("expected overwrite to 'new', got %q", hits[0].Text)
	}
}

// TestFake_TenantIsolation 租户隔离是越权防线：其他租户的数据绝不能被召回。
func TestFake_TenantIsolation(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	now := time.Now()
	_ = f.Upsert(ctx, "agent_memory", []Point{
		testPoint("tenant-A", "thread-1", "node-1", "assistant", "A 的机密数据", now),
		testPoint("tenant-B", "thread-1", "node-2", "assistant", "B 的机密数据", now),
	})
	hits, err := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 || hits[0].TenantID != "tenant-A" {
		t.Fatalf("cross-tenant leak: got %d hits %+v", len(hits), hits)
	}
}

// TestFake_ThreadIsolation 会话隔离：同租户不同会话的数据不得互相召回。
func TestFake_ThreadIsolation(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	now := time.Now()
	_ = f.Upsert(ctx, "agent_memory", []Point{
		testPoint("tenant-A", "thread-1", "node-1", "assistant", "会话一的内容", now),
		testPoint("tenant-A", "thread-2", "node-2", "assistant", "会话二的内容", now),
	})
	hits, _ := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 10)
	if len(hits) != 1 || hits[0].ThreadID != "thread-1" {
		t.Fatalf("cross-thread leak: got %+v", hits)
	}
}

// TestFake_EmptyThreadIDMeansNoThreadFilter 空 threadID 必须**不加**会话过滤。
//
// 这是"存量数据兼容"的关键语义：迁移前所有 Run 的 thread_id 都是空串，
// 若空串也参与严格匹配，升级后新会话将查不到任何旧记忆（静默失效，极难排查）。
func TestFake_EmptyThreadIDMeansNoThreadFilter(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	now := time.Now()
	_ = f.Upsert(ctx, "agent_memory", []Point{
		testPoint("tenant-A", "", "node-1", "assistant", "存量数据（无会话）", now),
		testPoint("tenant-A", "thread-1", "node-2", "assistant", "新数据（有会话）", now),
	})
	// 查询侧 threadID 为空：两条都应召回（不做会话隔离）。
	hits, _ := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: ""}, 10)
	if len(hits) != 2 {
		t.Errorf("empty threadID should skip thread filter, got %d hits want 2", len(hits))
	}
	// 查询侧 threadID 非空：只召回该会话的数据，不得串味。
	hits, _ = f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 10)
	if len(hits) != 1 || hits[0].NodeID != "node-2" {
		t.Errorf("non-empty threadID should strictly match, got %+v", hits)
	}
}

// TestFake_TopKTruncation topK 必须生效，否则召回过多会撑爆上下文窗口与 token 预算。
func TestFake_TopKTruncation(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	now := time.Now()
	for i := 0; i < 10; i++ {
		_ = f.Upsert(ctx, "agent_memory", []Point{
			testPoint("tenant-A", "thread-1", string(rune('a'+i)), "assistant", "text", now),
		})
	}
	hits, _ := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 3)
	if len(hits) != 3 {
		t.Errorf("topK not applied: got %d hits want 3", len(hits))
	}
}

// TestFake_ScoreDescending 结果必须按相似度降序，调用方截断 topK 才取到最相关的。
func TestFake_ScoreDescending(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	now := time.Now()
	pts := []Point{
		testPoint("tenant-A", "thread-1", "far", "assistant", "far", now),
		testPoint("tenant-A", "thread-1", "near", "assistant", "near", now),
	}
	pts[0].Vector = []float32{0, 1, 0} // 与查询正交，相似度 0
	pts[1].Vector = []float32{1, 0, 0} // 与查询同向，相似度 1
	_ = f.Upsert(ctx, "agent_memory", pts)

	hits, _ := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 10)
	if len(hits) != 2 {
		t.Fatalf("expected 2 hits, got %d", len(hits))
	}
	if hits[0].NodeID != "near" {
		t.Errorf("most similar should rank first, got %q", hits[0].NodeID)
	}
	if hits[0].Score < hits[1].Score {
		t.Errorf("scores not descending: %f then %f", hits[0].Score, hits[1].Score)
	}
}

// TestFake_FailureInjection 故障注入必须返回 error（而非空结果），
// 这样上层才能区分"没有相关记忆"与"检索失败"，进而走降级而非误判。
func TestFake_FailureInjection(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	_ = f.Upsert(ctx, "agent_memory", []Point{testPoint("tenant-A", "thread-1", "node-1", "assistant", "t", time.Now())})

	f.FailNext = true
	if _, err := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A"}, 10); err == nil {
		t.Error("expected injected search failure to surface as error")
	}
	f.FailNext = true
	if err := f.Upsert(ctx, "agent_memory", []Point{testPoint("tenant-A", "thread-1", "node-2", "assistant", "t", time.Now())}); err == nil {
		t.Error("expected injected upsert failure to surface as error")
	}
	// FailNext 是一次性的：下一次调用应恢复正常。
	if _, err := f.Search(ctx, "agent_memory", []float32{1, 0, 0}, Filter{TenantID: "tenant-A"}, 10); err != nil {
		t.Errorf("expected recovery after one-shot failure, got %v", err)
	}
}

// TestFake_ExcludeRunID ExcludeRunID 必须在存储层生效。
//
// 这是"当前 Run 历史不被重复注入"的第一道防线：provider 层依赖它排除自身 Run，
// 而 MemorySearcher.Search 返回的 Message 不含 node_id，调用方无法在下游补救。
func TestFake_ExcludeRunID(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "agent_memory", 3)
	now := time.Now()

	mk := func(runID, nodeID, text string) Point {
		p := testPoint("tenant-A", "thread-1", nodeID, "assistant", text, now)
		p.RunID = runID
		return p
	}
	_ = f.Upsert(ctx, "agent_memory", []Point{
		mk("run-current", "node-cur", "内容"),
		mk("run-previous", "node-prev", "内容"),
	})

	// 排除当前 Run：只剩上一个 Run 的记忆。
	hits, err := f.Search(ctx, "agent_memory", []float32{1, 0, 0},
		Filter{TenantID: "tenant-A", ThreadID: "thread-1", ExcludeRunID: "run-current"}, 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(hits) != 1 || hits[0].RunID != "run-previous" {
		t.Fatalf("ExcludeRunID not applied: got %+v", hits)
	}

	// 不排除：两条都在。
	hits, _ = f.Search(ctx, "agent_memory", []float32{1, 0, 0},
		Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 10)
	if len(hits) != 2 {
		t.Errorf("without exclusion expected 2 hits, got %d", len(hits))
	}

	// 排除一个不存在的 Run：不应误伤任何数据。
	hits, _ = f.Search(ctx, "agent_memory", []float32{1, 0, 0},
		Filter{TenantID: "tenant-A", ThreadID: "thread-1", ExcludeRunID: "run-nonexistent"}, 10)
	if len(hits) != 2 {
		t.Errorf("excluding unknown run should keep all, got %d", len(hits))
	}
}

// TestFake_CollectionIsolation 不同集合的数据必须互相隔离，避免测试之间串数据。
func TestFake_CollectionIsolation(t *testing.T) {
	ctx := context.Background()
	f := NewFake()
	_ = f.EnsureCollection(ctx, "coll-a", 3)
	_ = f.EnsureCollection(ctx, "coll-b", 3)
	_ = f.Upsert(ctx, "coll-a", []Point{testPoint("tenant-A", "thread-1", "node-1", "assistant", "t", time.Now())})

	hits, _ := f.Search(ctx, "coll-b", []float32{1, 0, 0}, Filter{TenantID: "tenant-A", ThreadID: "thread-1"}, 10)
	if len(hits) != 0 {
		t.Errorf("collection leak: got %d hits from coll-b", len(hits))
	}
	if f.Dimension("coll-a") != 3 {
		t.Errorf("EnsureCollection did not record dimension: %d", f.Dimension("coll-a"))
	}
}

// TestPayloadFields_CreatedAtRoundTrip created_at 以 Unix 纳秒存取，必须无损还原。
func TestPayloadFields_CreatedAtRoundTrip(t *testing.T) {
	at := time.Date(2026, 9, 19, 12, 34, 56, 123456789, time.UTC)
	p := testPoint("tenant-A", "thread-1", "node-1", "assistant", "text", at)
	fields := p.PayloadFields()

	raw, ok := fields[PayloadCreatedAt].(int64)
	if !ok {
		t.Fatalf("created_at should be stored as int64, got %T", fields[PayloadCreatedAt])
	}
	if got := CreatedAtFromPayload(raw); !got.Equal(at) {
		t.Errorf("created_at lost precision: got %v want %v", got, at)
	}
	// 缺失或类型不符时应返回零值而非 panic（Qdrant 老数据可能无该字段）。
	if !CreatedAtFromPayload(nil).IsZero() {
		t.Error("expected zero time for nil payload")
	}
	if !CreatedAtFromPayload("not-a-time").IsZero() {
		t.Error("expected zero time for unexpected payload type")
	}
}
