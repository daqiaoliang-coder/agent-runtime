package memory

import (
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

// ==== 测试替身 ====

// fakeStore 记录调用顺序与内容，用于验证"先写向量、后标记进度"这一关键不变量。
type fakeStore struct {
	nodes   []model.MemoryNode
	scanErr error

	marked []string
	// ops 按调用顺序记录事件，用于断言 Upsert 发生在 MarkMemoryIndexed 之前。
	ops       []string
	markErr   error
	scanCalls int
}

func (f *fakeStore) UnindexedSuccessNodes(context.Context, int) ([]model.MemoryNode, error) {
	f.scanCalls++
	f.ops = append(f.ops, "scan")
	if f.scanErr != nil {
		return nil, f.scanErr
	}
	return f.nodes, nil
}

func (f *fakeStore) MarkMemoryIndexed(_ context.Context, _, nodeID, _, _ string) error {
	f.ops = append(f.ops, "mark:"+nodeID)
	if f.markErr != nil {
		return f.markErr
	}
	f.marked = append(f.marked, nodeID)
	return nil
}

func (f *fakeStore) hasMarked(nodeID string) bool {
	for _, id := range f.marked {
		if id == nodeID {
			return true
		}
	}
	return false
}

// recordingVectors 包装 fake 向量库并记录写入的 point，供断言。
type recordingVectors struct {
	*vector.Fake
	points     []vector.Point
	upsertErr  error
	ensureErr  error
	upsertOps  *[]string // 与 fakeStore.ops 共享，用于验证跨组件顺序
	collection string
}

func (r *recordingVectors) EnsureCollection(ctx context.Context, c string, dim int) error {
	if r.ensureErr != nil {
		return r.ensureErr
	}
	return r.Fake.EnsureCollection(ctx, c, dim)
}

func (r *recordingVectors) Upsert(ctx context.Context, c string, points []vector.Point) error {
	if r.upsertOps != nil {
		*r.upsertOps = append(*r.upsertOps, "upsert")
	}
	if r.upsertErr != nil {
		return r.upsertErr
	}
	r.points = append(r.points, points...)
	return r.Fake.Upsert(ctx, c, points)
}

// errEmbedder 总是失败的向量化实现。
type errEmbedder struct{ err error }

func (e errEmbedder) Embed(context.Context, []string) ([][]float32, error) { return nil, e.err }

// countingEmbedder 记录调用批次大小，用于验证批处理确实生效。
type countingEmbedder struct {
	inner   llm.Embedder
	batches [][]int // 每次调用收到的文本条数
}

func (c *countingEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	c.batches = append(c.batches, []int{len(texts)})
	return c.inner.Embed(ctx, texts)
}

func node(id, tenant, thread, runID, input, output string) model.MemoryNode {
	return model.MemoryNode{
		NodeID:     id,
		TenantID:   tenant,
		ThreadID:   thread,
		RunID:      runID,
		Type:       model.NodeLLM,
		Name:       "reason",
		Input:      input,
		Output:     output,
		FinishedAt: time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC),
	}
}

// newTestIndexer 构造基于内存替身的索引器（local 后端：embed+直连 fake 向量库）。
// BatchDelay 设为极小正值：既避开默认 500ms 限速拖慢测试，又仍走批次间隔分支。
func newTestIndexer(st *fakeStore, vs *recordingVectors, emb llm.Embedder) *Indexer {
	return New(&LocalWriter{Embedder: emb, Vectors: vs}, st, Config{
		Collection: "agent_memory",
		Dim:        8,
		BatchSize:  16,
		BatchDelay: time.Nanosecond,
		// PollInterval 参与退避计算，测试里设短以加速。
		PollInterval: time.Millisecond,
	})
}

func newRecordingFake(t *testing.T) *recordingVectors {
	t.Helper()
	f := vector.NewFake()
	if err := f.EnsureCollection(context.Background(), "agent_memory", 8); err != nil {
		t.Fatalf("ensure collection: %v", err)
	}
	return &recordingVectors{Fake: f}
}

// ==== 正常投影 ====

// TestIndexer_ProjectsInputAndOutput 每个节点应产出两条记忆（提问 + 回答），
// 且 point ID 因 role 维度不同而互不覆盖。
func TestIndexer_ProjectsInputAndOutput(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "为什么延期", "因为依赖未就绪")}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	n, err := ix.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 1 {
		t.Errorf("expected 1 indexed node, got %d", n)
	}
	if len(vs.points) != 2 {
		t.Fatalf("expected 2 points (input+output), got %d", len(vs.points))
	}
	roles := map[string]string{}
	for _, p := range vs.points {
		roles[string(p.Role)] = p.Text
		if p.TenantID != "tenant-A" || p.ThreadID != "thread-1" || p.RunID != "run-1" || p.NodeID != "n1" {
			t.Errorf("payload not propagated: %+v", p)
		}
	}
	if roles[string(contracts.RoleUser)] != "为什么延期" {
		t.Errorf("input not indexed as user message: %v", roles)
	}
	if roles[string(contracts.RoleAssistant)] != "因为依赖未就绪" {
		t.Errorf("output not indexed as assistant message: %v", roles)
	}
	if !st.hasMarked("n1") {
		t.Error("node not marked as indexed")
	}
}

// TestIndexer_CreatedAtFromFinishedAt 记忆时间必须取节点完成时间，
// 否则召回时按 created_at 排序会得到错误的对话顺序。
func TestIndexer_CreatedAtFromFinishedAt(t *testing.T) {
	finished := time.Date(2026, 3, 4, 5, 6, 7, 0, time.UTC)
	n1 := node("n1", "tenant-A", "thread-1", "run-1", "in", "out")
	n1.FinishedAt = finished
	st := &fakeStore{nodes: []model.MemoryNode{n1}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	if _, err := ix.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	for _, p := range vs.points {
		if !p.CreatedAt.Equal(finished) {
			t.Errorf("created_at should be node FinishedAt %v, got %v", finished, p.CreatedAt)
		}
	}
}

// ==== 顺序不变量：先写向量，后标记进度 ====

// TestIndexer_UpsertBeforeMark 必须先确认向量落库，才能标记进度。
//
// 这是写入路径最关键的不变量：顺序颠倒时，若进程在两步之间崩溃，
// 进度表会领先于实际数据，该节点此后永远不被重扫，记忆静默丢失且无法察觉。
func TestIndexer_UpsertBeforeMark(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "in", "out")}}
	vs := newRecordingFake(t)
	vs.upsertOps = &st.ops // 共享同一条操作时间线
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	if _, err := ix.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	upsertIdx, markIdx := -1, -1
	for i, op := range st.ops {
		if op == "upsert" && upsertIdx == -1 {
			upsertIdx = i
		}
		if op == "mark:n1" && markIdx == -1 {
			markIdx = i
		}
	}
	if upsertIdx < 0 || markIdx < 0 {
		t.Fatalf("missing ops: %v", st.ops)
	}
	if upsertIdx > markIdx {
		t.Errorf("Upsert must happen before MarkMemoryIndexed, got ops=%v", st.ops)
	}
}

// TestIndexer_UpsertFailureDoesNotMark 向量写入失败时绝不能标记进度，
// 否则该节点会被永久跳过、记忆丢失。
func TestIndexer_UpsertFailureDoesNotMark(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "in", "out")}}
	vs := newRecordingFake(t)
	vs.upsertErr = errors.New("qdrant unavailable")
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	if _, err := ix.RunOnce(context.Background()); err == nil {
		t.Fatal("expected error to surface from RunOnce when upsert fails")
	}
	if st.hasMarked("n1") {
		t.Error("node must NOT be marked when upsert failed (would lose memory permanently)")
	}
	if ix.FailureCount("n1") == 0 {
		t.Error("expected failure recorded for backoff")
	}
}

// TestIndexer_EmbedFailureDoesNotUpsertOrMark embedding 失败时既不写向量也不标记，
// 让下一轮自然重试。
func TestIndexer_EmbedFailureDoesNotUpsertOrMark(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "in", "out")}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, errEmbedder{err: errors.New("rate limited")})

	if _, err := ix.RunOnce(context.Background()); err == nil {
		t.Fatal("expected error when embedding fails")
	}
	if len(vs.points) != 0 {
		t.Errorf("no points should be written when embedding fails, got %d", len(vs.points))
	}
	if st.hasMarked("n1") {
		t.Error("node must NOT be marked when embedding failed")
	}
}

// TestIndexer_MarkFailureStillKeepsVector 标记失败时向量已写入，不回滚；
// 下一轮重扫会重新 Upsert，因 point ID 确定性而幂等。
func TestIndexer_MarkFailureStillKeepsVector(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "in", "out")}, markErr: errors.New("db down")}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	n, _ := ix.RunOnce(context.Background())
	if n != 0 {
		t.Errorf("expected 0 successfully indexed when mark fails, got %d", n)
	}
	if len(vs.points) != 2 {
		t.Errorf("vectors should still be written before the mark attempt, got %d", len(vs.points))
	}
}

// ==== 幂等与重放 ====

// TestIndexer_ReplayIsIdempotent 重复投影同一批节点不得产生重复记忆。
// 这是崩溃重启安全的基础：确定性 point ID 使重放变成覆盖而非追加。
func TestIndexer_ReplayIsIdempotent(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "in", "out")}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if _, err := ix.RunOnce(ctx); err != nil {
			t.Fatalf("round %d: %v", i, err)
		}
	}
	if got := vs.Count("agent_memory"); got != 2 {
		t.Errorf("replay produced %d points, want 2 (duplicate memories)", got)
	}
	if len(vs.points) != 6 {
		t.Errorf("expected 6 Upsert calls' worth of points recorded (3 rounds x 2), got %d", len(vs.points))
	}
	// 所有重放的 point ID 必须一致，否则无法覆盖去重。
	first := vs.points[0].ID
	for _, p := range vs.points {
		if p.Role == vs.points[0].Role && p.ID != first {
			t.Errorf("point ID not stable across replay: %d != %d", p.ID, first)
		}
	}
}

// ==== 空节点 ====

// TestIndexer_EmptyNodeMarkedWithoutPoints Input/Output 都为空的节点应被标记但不产生向量。
// 不标记会让空节点永远滞留待扫描队列，白白占用每轮扫描配额。
func TestIndexer_EmptyNodeMarkedWithoutPoints(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n-empty", "tenant-A", "thread-1", "run-1", "", "")}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	n, err := ix.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(vs.points) != 0 {
		t.Errorf("empty node should not produce points, got %d", len(vs.points))
	}
	if !st.hasMarked("n-empty") {
		t.Error("empty node should still be marked to leave the scan queue")
	}
	if n != 0 {
		t.Errorf("empty node should not count as indexed, got %d", n)
	}
}

// TestIndexer_PartialEmpty 只有 Output 有内容时，应只投影那一条。
func TestIndexer_PartialEmpty(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "", "只有回答")}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	if _, err := ix.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if len(vs.points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(vs.points))
	}
	if vs.points[0].Role != string(contracts.RoleAssistant) || vs.points[0].Text != "只有回答" {
		t.Errorf("unexpected point: %+v", vs.points[0])
	}
}

// ==== 截断 ====

// TestTruncate_ByRunesNotBytes 必须按字符截断：中文是多字节，
// 按字节截断会在字符中间切断产生非法 UTF-8，导致网关报错或向量库拒写。
func TestTruncate_ByRunesNotBytes(t *testing.T) {
	s := strings.Repeat("中", 10) // 每个"中"占 3 字节，共 30 字节
	got := truncate(s, 4)
	if runeLen := len([]rune(got)); runeLen != 4 {
		t.Errorf("expected 4 runes, got %d", runeLen)
	}
	if !utf8.ValidString(got) {
		t.Errorf("truncation produced invalid UTF-8: %q", got)
	}
	// 未超长时原样返回。
	if truncate("abc", 10) != "abc" {
		t.Error("short string should be returned unchanged")
	}
	// maxLen<=0 表示不限制。
	if truncate("abc", 0) != "abc" {
		t.Error("maxLen 0 should disable truncation")
	}
}

// TestIndexer_TruncatesLongOutput 超长节点输出必须被截断后才向量化。
func TestIndexer_TruncatesLongOutput(t *testing.T) {
	long := strings.Repeat("长", 5000)
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "q", long)}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})
	ix.Config.MaxTextLen = 100

	if _, err := ix.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	for _, p := range vs.points {
		if p.Role == string(contracts.RoleAssistant) {
			if n := len([]rune(p.Text)); n != 100 {
				t.Errorf("expected truncation to 100 runes, got %d", n)
			}
			if !utf8.ValidString(p.Text) {
				t.Error("truncated text is invalid UTF-8")
			}
		}
	}
}

// ==== 队头阻塞防护 ====

// TestIndexer_FailingNodeDoesNotBlockOthers 持续失败的节点必须退避让位，
// 否则它会和好节点待在**同一批**里（扫描按 finished_at 正序、批内整批 embed），
// 一个坏文本会让整批 embed 失败，好节点因此永远得不到处理。
//
// 测试刻意让两者同批（BatchSize 足够大），并给足退避窗口，
// 以验证"退避过滤"确实在分批之前把坏节点摘掉。
func TestIndexer_FailingNodeDoesNotBlockOthers(t *testing.T) {
	// bad 节点完成时间更早，因此在扫描结果中排在前面。
	bad := node("bad", "tenant-A", "thread-1", "run-1", "q", "a")
	bad.FinishedAt = time.Date(2026, 9, 1, 10, 0, 0, 0, time.UTC)
	good := node("good", "tenant-A", "thread-1", "run-1", "q", "b")
	good.FinishedAt = time.Date(2026, 9, 19, 10, 0, 0, 0, time.UTC)

	st := &fakeStore{nodes: []model.MemoryNode{bad, good}}
	vs := newRecordingFake(t)

	// 只对 bad 节点的文本失败的 embedder；整批 embed 会因此失败。
	emb := &selectiveEmbedder{failOnText: "a", inner: llm.StubEmbedder{Dim: 8}}
	ix := newTestIndexer(st, vs, emb)
	// BatchSize 足够大 → bad 与 good 落入同一批；
	// PollInterval 设 1 小时 → 退避窗口足够长，第二轮必然跳过 bad。
	ix.Config.BatchSize = 16
	ix.Config.PollInterval = time.Hour
	ctx := context.Background()

	// 第一轮：整批失败，两个节点都进入退避。
	if _, err := ix.RunOnce(ctx); err == nil {
		t.Fatal("expected failure when batch contains a rejected text")
	}
	if ix.FailureCount("bad") == 0 {
		t.Fatal("expected backoff recorded for bad node")
	}
	if st.hasMarked("good") {
		t.Fatal("good node should not be marked while its batch failed")
	}

	// 第二轮：手动让 good 脱离退避（模拟它的失败原因已消失，例如网关恢复），
	// 而 bad 仍在退避窗口内。此时 bad 必须在分批前被过滤掉，good 才能成功投影。
	delete(ix.failures, "good")
	n, err := ix.RunOnce(ctx)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if !st.hasMarked("good") {
		t.Errorf("good node was blocked by the failing node (head-of-line blocking); indexed=%d", n)
	}
	if n != 1 {
		t.Errorf("expected exactly 1 node indexed, got %d", n)
	}
	if ix.FailureCount("bad") == 0 {
		t.Error("bad node should still be tracked as failing")
	}
}

// selectiveEmbedder 对指定文本返回错误，其余委托给 inner。
type selectiveEmbedder struct {
	failOnText string
	inner      llm.Embedder
}

func (s *selectiveEmbedder) Embed(ctx context.Context, texts []string) ([][]float32, error) {
	for _, t := range texts {
		if t == s.failOnText {
			return nil, errors.New("content rejected by gateway")
		}
	}
	return s.inner.Embed(ctx, texts)
}

// ==== 批处理与配置 ====

// TestIndexer_Batching 应按 BatchSize（节点数）分批调用 embedding，摊薄网关往返。
// 每个节点最多产出 2 条文本，因此每批文本数 = 节点数 × 2。
func TestIndexer_Batching(t *testing.T) {
	var nodes []model.MemoryNode
	for i := 0; i < 5; i++ {
		nodes = append(nodes, node(string(rune('a'+i)), "tenant-A", "thread-1", "run-1", "q", "a"))
	}
	st := &fakeStore{nodes: nodes}
	vs := newRecordingFake(t)
	emb := &countingEmbedder{inner: llm.StubEmbedder{Dim: 8}}
	ix := newTestIndexer(st, vs, emb)
	ix.Config.BatchSize = 2

	if _, err := ix.RunOnce(context.Background()); err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	// 5 个节点按 batchSize=2 切分 → 3 批（2+2+1），每节点 2 条文本。
	wantBatches := [][]int{{4}, {4}, {2}}
	if len(emb.batches) != len(wantBatches) {
		t.Fatalf("expected %d embed batches, got %d: %v", len(wantBatches), len(emb.batches), emb.batches)
	}
	for i, want := range wantBatches {
		if emb.batches[i][0] != want[0] {
			t.Errorf("batch %d: expected %d texts, got %d", i, want[0], emb.batches[i][0])
		}
	}
	if len(vs.points) != 10 {
		t.Errorf("expected 10 points, got %d", len(vs.points))
	}
}

// TestNew_AppliesDefaults 零值配置必须回退到安全默认值。
func TestNew_AppliesDefaults(t *testing.T) {
	ix := New(&LocalWriter{Embedder: llm.StubEmbedder{}, Vectors: vector.NewFake()}, &fakeStore{}, Config{})
	if ix.Config.Collection != DefaultCollection {
		t.Errorf("collection default: %q", ix.Config.Collection)
	}
	if ix.Config.BatchSize != DefaultBatchSize {
		t.Errorf("batch size default: %d", ix.Config.BatchSize)
	}
	if ix.Config.ScanLimit != DefaultScanLimit {
		t.Errorf("scan limit default: %d", ix.Config.ScanLimit)
	}
	// ScanLimit 必须大于 BatchSize，否则每轮只有一批、批间限速永不生效。
	if ix.Config.ScanLimit <= ix.Config.BatchSize {
		t.Errorf("ScanLimit(%d) must exceed BatchSize(%d) for batch throttling to take effect",
			ix.Config.ScanLimit, ix.Config.BatchSize)
	}
	if ix.Config.MaxTextLen != DefaultMaxTextLen {
		t.Errorf("max text len default: %d", ix.Config.MaxTextLen)
	}
	if ix.Config.PollInterval != DefaultPollInterval {
		t.Errorf("poll interval default: %v", ix.Config.PollInterval)
	}
	// BatchDelay 零值也必须回退到默认限速：它是与主链路共用网关的限流保护，
	// 装配遗漏时宁可慢，也不能高频挤占配额。
	if ix.Config.BatchDelay != DefaultBatchDelay {
		t.Errorf("batch delay should default to %v, got %v", DefaultBatchDelay, ix.Config.BatchDelay)
	}
	if ix.failures == nil {
		t.Error("failures map must be initialized")
	}
}

// TestIndexer_NilDependencies RunOnce 必须报配置错误而非静默跳过。
// 索引器是独立进程，装配遗漏要在启动时立刻暴露。
// Writer 内部依赖（embedder/vectors）的缺失在写入时暴露，
// 因此这两个 case 必须带节点数据让流程真正走到 Write。
func TestIndexer_NilDependencies(t *testing.T) {
	ctx := context.Background()
	cfg := Config{Collection: "c", PollInterval: time.Second}
	pending := &fakeStore{nodes: []model.MemoryNode{node("n1", "t", "th", "r", "in", "out")}}
	cases := map[string]*Indexer{
		"nil writer":   {Store: &fakeStore{}, Config: cfg},
		"nil store":    {Writer: &LocalWriter{Embedder: llm.StubEmbedder{}, Vectors: newRecordingFake(t)}, Config: cfg},
		"nil embedder": {Writer: &LocalWriter{Vectors: newRecordingFake(t)}, Store: pending, Config: cfg},
		"nil vectors":  {Writer: &LocalWriter{Embedder: llm.StubEmbedder{}}, Store: pending, Config: cfg},
	}
	for name, ix := range cases {
		if ix.failures == nil {
			ix.failures = make(map[string]failure)
		}
		if _, err := ix.RunOnce(ctx); err == nil {
			t.Errorf("%s: expected configuration error", name)
		}
	}
}

// TestIndexer_ScanError 扫描失败应向上返回 error（区别于单节点失败只记日志）。
func TestIndexer_ScanError(t *testing.T) {
	st := &fakeStore{scanErr: errors.New("db unreachable")}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	if _, err := ix.RunOnce(context.Background()); err == nil {
		t.Fatal("expected scan error to propagate")
	}
}

// TestIndexer_NoNodes 无待处理节点时应安静返回 0，不报错。
func TestIndexer_NoNodes(t *testing.T) {
	st := &fakeStore{nodes: nil}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	n, err := ix.RunOnce(context.Background())
	if err != nil || n != 0 {
		t.Fatalf("expected (0,nil), got (%d,%v)", n, err)
	}
}

// TestIndexer_ContextCancelled ctx 取消时应尽快返回，不再写入。
func TestIndexer_ContextCancelled(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "q", "a")}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ix.RunOnce(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// ==== 集合初始化 ====

// TestIndexer_EnsureCollectionRequiresDim 维度未配置必须报错。
// 否则会以 0 维建集合，后续写入全部失败且错误信息难以定位。
func TestIndexer_EnsureCollectionRequiresDim(t *testing.T) {
	ix := New(&LocalWriter{Embedder: llm.StubEmbedder{}, Vectors: newRecordingFake(t)}, &fakeStore{}, Config{Collection: "c"})
	if err := ix.EnsureCollection(context.Background()); err == nil {
		t.Fatal("expected error when dimension is not configured")
	}
}

// TestIndexer_EnsureCollectionIdempotent 重复调用不应报错（每次启动都会调）。
func TestIndexer_EnsureCollectionIdempotent(t *testing.T) {
	ix := New(&LocalWriter{Embedder: llm.StubEmbedder{}, Vectors: newRecordingFake(t)}, &fakeStore{}, Config{Collection: "agent_memory", Dim: 8})
	for i := 0; i < 3; i++ {
		if err := ix.EnsureCollection(context.Background()); err != nil {
			t.Fatalf("call #%d: %v", i, err)
		}
	}
}

// TestIndexer_EnsureCollectionNilVectors 向量库未装配时报明确错误。
func TestIndexer_EnsureCollectionNilVectors(t *testing.T) {
	ix := New(&LocalWriter{Embedder: llm.StubEmbedder{}}, &fakeStore{}, Config{Collection: "c", Dim: 8})
	if err := ix.EnsureCollection(context.Background()); err == nil {
		t.Fatal("expected error when vector store is nil")
	}
}

// ==== 退避行为 ====

// TestIndexer_BackoffGrows 退避应随失败次数指数增长、封顶后不再增长。
//
// 断言用公式计算期望值而非依赖真实时钟差值：recordFailure 内部取 time.Now()，
// 直接测量墙钟差会引入纳秒级抖动，在退避进入平台期后会被误判为"退避变小"。
func TestIndexer_BackoffGrows(t *testing.T) {
	poll := time.Second
	ix := New(&LocalWriter{Embedder: llm.StubEmbedder{}, Vectors: newRecordingFake(t)}, &fakeStore{}, Config{PollInterval: poll})

	var prev time.Duration
	for i := 1; i <= 10; i++ {
		before := time.Now()
		ix.recordFailure("n1")
		got := ix.failures["n1"].nextTry.Sub(before)

		// 期望值：PollInterval × 2^min(count-1,6)，且不超过 maxBackoff。
		want := poll * time.Duration(1<<min(i-1, 6))
		if want > maxBackoff {
			want = maxBackoff
		}
		// 容忍 1ms 测量抖动（时钟读取与减法之间的耗时）。
		if diff := got - want; diff > time.Millisecond || diff < -time.Millisecond {
			t.Errorf("failure #%d: backoff %v, want ~%v", i, got, want)
		}
		if got < prev-time.Millisecond {
			t.Errorf("backoff should never shrink: %v then %v", prev, got)
		}
		prev = got
	}
	if ix.FailureCount("n1") != 10 {
		t.Errorf("expected 10 failures recorded, got %d", ix.FailureCount("n1"))
	}
	// 指数封顶：第 7 次之后退避不再增长（2^6 = 64s）。
	if got := ix.failures["n1"].nextTry; got.IsZero() {
		t.Error("nextTry should be set")
	}
}

// TestIndexer_BackoffClearedOnSuccess 节点成功投影后应清除失败计数，
// 否则历史失败会持续影响其后的退避计算。
//
// 注意：必须构造一个**已过期**的退避记录。若 nextTry 在未来，
// RunOnce 会在分批前就把该节点过滤掉（这正是队头阻塞防护的行为），
// 于是投影根本不会发生，也就谈不上"成功后清除"。
func TestIndexer_BackoffClearedOnSuccess(t *testing.T) {
	st := &fakeStore{nodes: []model.MemoryNode{node("n1", "tenant-A", "thread-1", "run-1", "q", "a")}}
	vs := newRecordingFake(t)
	ix := newTestIndexer(st, vs, llm.StubEmbedder{Dim: 8})

	// 模拟"曾失败 3 次、退避已到期"：计数非 0，但 nextTry 在过去。
	ix.failures["n1"] = failure{count: 3, nextTry: time.Now().Add(-time.Hour)}
	if ix.FailureCount("n1") != 3 {
		t.Fatalf("setup failed: expected count 3, got %d", ix.FailureCount("n1"))
	}

	n, err := ix.RunOnce(context.Background())
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if n != 1 {
		t.Fatalf("expected 1 node indexed, got %d", n)
	}
	if got := ix.FailureCount("n1"); got != 0 {
		t.Errorf("expected failure count cleared after success, got %d", got)
	}
}
