package ingest

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/chunk"
	"github.com/daqiaoliang-coder/rag-service/internal/sparse"
	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

// fakeWriter 模拟向量库：按集合内存储点，统计 UpsertDefault/UpsertHybrid 调用。
type fakeWriter struct {
	collections map[string]map[uint64]vector.DocPoint

	defaultCalls       int
	hybridCalls        int
	deleteByDocIDCalls int
	retrieveErr        error
	upsertErr          error
	deleteErr          error
}

func newFakeWriter() *fakeWriter {
	return &fakeWriter{collections: map[string]map[uint64]vector.DocPoint{}}
}

func (f *fakeWriter) EnsureMemoryCollection(context.Context, string, int) error { return nil }
func (f *fakeWriter) EnsureDocsCollection(context.Context, string, int) error   { return nil }

func (f *fakeWriter) UpsertDefault(_ context.Context, c string, pts []vector.DocPoint) error {
	f.defaultCalls++
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.store(c, pts)
	return nil
}

func (f *fakeWriter) UpsertHybrid(_ context.Context, c string, pts []vector.DocPoint) error {
	f.hybridCalls++
	if f.upsertErr != nil {
		return f.upsertErr
	}
	f.store(c, pts)
	return nil
}

func (f *fakeWriter) store(c string, pts []vector.DocPoint) {
	if f.collections[c] == nil {
		f.collections[c] = map[uint64]vector.DocPoint{}
	}
	for _, p := range pts {
		f.collections[c][p.ID] = p
	}
}

func (f *fakeWriter) Delete(_ context.Context, c string, ids ...uint64) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	for _, id := range ids {
		delete(f.collections[c], id)
	}
	return nil
}

// deleteByDocIDCalls 统计过滤删除调用（docs 路径删除走这里而非按 ID 删）。
func (f *fakeWriter) DeleteByDocID(_ context.Context, c, tenantID, docID string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleteByDocIDCalls++
	for id, p := range f.collections[c] {
		if p.TenantID == tenantID && p.DocID == docID {
			delete(f.collections[c], id)
		}
	}
	return nil
}

func (f *fakeWriter) Retrieve(_ context.Context, c string, ids []uint64) (map[uint64]vector.RetrievedDoc, error) {
	if f.retrieveErr != nil {
		return nil, f.retrieveErr
	}
	out := map[uint64]vector.RetrievedDoc{}
	for _, id := range ids {
		if p, ok := f.collections[c][id]; ok {
			out[id] = vector.RetrievedDoc{
				ID: p.ID, TenantID: p.TenantID, ThreadID: p.ThreadID, RunID: p.RunID,
				NodeID: p.NodeID, Role: p.Role, Text: p.Text, CreatedAt: p.CreatedAt,
				ContentHash: p.ContentHash, DocID: p.DocID, ChunkSeq: p.ChunkSeq,
				ParentText: p.ParentText,
			}
		}
	}
	return out, nil
}

// countingEmbedder 返回固定维度向量并统计调用次数与批量大小。
type countingEmbedder struct {
	calls int
	lastN int
	sizes []int // 每次 Embed 调用的文本数（子批断言用）
	err   error
	empty bool // 返回与入参不等长的向量，模拟网关异常
}

func (e *countingEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.calls++
	e.lastN = len(texts)
	e.sizes = append(e.sizes, len(texts))
	if e.err != nil {
		return nil, e.err
	}
	if e.empty {
		return [][]float32{{1}}, nil
	}
	out := make([][]float32, len(texts))
	for i := range out {
		out[i] = []float32{float32(i)}
	}
	return out, nil
}

func newTestService(w *fakeWriter, e *countingEmbedder) *Service {
	return &Service{
		Embedder:         e,
		Sparse:           sparse.NewTF(),
		Store:            w,
		MemoryCollection: "agent_memory",
		DocsCollection:   "rag_documents",
	}
}

func doc(id uint64, content string) Document {
	return Document{
		ID:      strconv.FormatUint(id, 10),
		Content: content,
		Metadata: DocumentMetadata{
			TenantID: "t1", ThreadID: "th1", RunID: "r1",
			NodeID: "n1", Role: "user", CreatedAt: time.Unix(100, 0),
		},
	}
}

func TestUpsert_IdempotentSkip(t *testing.T) {
	w := newFakeWriter()
	e := &countingEmbedder{}
	svc := newTestService(w, e)

	if _, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(1, "为什么延期")}); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if e.calls != 1 || e.lastN != 1 {
		t.Fatalf("want 1 embed call for 1 doc, got calls=%d n=%d", e.calls, e.lastN)
	}
	// 同 ID 同内容重放：不得再次调用 embedding（崩溃恢复后重扫的典型场景）。
	resp, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(1, "为什么延期")})
	if err != nil {
		t.Fatalf("replay upsert: %v", err)
	}
	if e.calls != 1 {
		t.Fatalf("replay must skip embedding, got %d calls", e.calls)
	}
	if resp.Skipped != 1 || resp.Indexed != 0 {
		t.Fatalf("want skipped=1 indexed=0, got %+v", resp)
	}
}

func TestUpsert_ContentChangeReembeds(t *testing.T) {
	w := newFakeWriter()
	e := &countingEmbedder{}
	svc := newTestService(w, e)

	if _, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(1, "v1")}); err != nil {
		t.Fatal(err)
	}
	// 同 ID 不同内容：hash 不同，必须重新嵌入并覆盖。
	if _, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(1, "v2")}); err != nil {
		t.Fatal(err)
	}
	if e.calls != 2 {
		t.Fatalf("content change must re-embed, got %d calls", e.calls)
	}
	if got := w.collections["agent_memory"][1].Text; got != "v2" {
		t.Fatalf("point must be overwritten, got text %q", got)
	}
}

func TestUpsert_MemoryLayoutIsDenseOnly(t *testing.T) {
	w := newFakeWriter()
	e := &countingEmbedder{}
	svc := newTestService(w, e)

	if _, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(7, "记忆文本")}); err != nil {
		t.Fatal(err)
	}
	if w.defaultCalls != 1 || w.hybridCalls != 0 {
		t.Fatalf("memory collection must use default layout, got default=%d hybrid=%d", w.defaultCalls, w.hybridCalls)
	}
	p := w.collections["agent_memory"][7]
	if p.Sparse != nil {
		t.Fatal("memory layout must NOT carry a sparse vector (runtime 兼容前提)")
	}
	if p.ContentHash == "" {
		t.Fatal("content_hash must be stored for idempotency")
	}
}

func TestUpsert_DocsLayoutIsHybrid(t *testing.T) {
	w := newFakeWriter()
	e := &countingEmbedder{}
	svc := newTestService(w, e)

	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(9, "部署文档 数据库")}); err != nil {
		t.Fatal(err)
	}
	if w.hybridCalls != 1 || w.defaultCalls != 0 {
		t.Fatalf("docs collection must use hybrid layout, got default=%d hybrid=%d", w.defaultCalls, w.hybridCalls)
	}
	// Phase 3：docs 集合是 parent-child 分块，点 ID 由 (tenant|docID|seq)
	// 派生，不再是文档自身的 uint64 ID。
	p, ok := w.collections["rag_documents"][childPointID("t1", "9", 0)]
	if !ok {
		t.Fatalf("child0 point not found at derived id %d", childPointID("t1", "9", 0))
	}
	if p.Sparse == nil || len(p.Sparse.Indices) == 0 {
		t.Fatal("docs layout must carry sparse vector")
	}
	if p.DocID != "9" || p.ChunkSeq != 0 || p.ParentText != "部署文档 数据库" {
		t.Fatalf("chunk payload wrong: doc_id=%q chunk_seq=%d parent=%q", p.DocID, p.ChunkSeq, p.ParentText)
	}
	if p.ContentHash == "" {
		t.Fatal("doc-level content_hash must be stored")
	}
}

func TestUpsert_UnknownCollectionRejected(t *testing.T) {
	w := newFakeWriter()
	svc := newTestService(w, &countingEmbedder{})
	_, err := svc.Upsert(context.Background(), "random_coll", []Document{doc(1, "x")})
	if !errors.Is(err, ErrUnknownCollection) {
		t.Fatalf("want ErrUnknownCollection, got %v", err)
	}
}

func TestUpsert_Validation(t *testing.T) {
	cases := []struct {
		name string
		docs []Document
		want error
	}{
		{"empty batch", nil, ErrInvalidDocument},
		{"empty content", []Document{{ID: "1", Content: "", Metadata: DocumentMetadata{TenantID: "t"}}}, ErrInvalidDocument},
		{"empty tenant", []Document{{ID: "1", Content: "c", Metadata: DocumentMetadata{}}}, ErrInvalidDocument},
		{"bad id", []Document{{ID: "not-a-number", Content: "c", Metadata: DocumentMetadata{TenantID: "t"}}}, ErrInvalidDocument},
	}
	for _, tc := range cases {
		svc := newTestService(newFakeWriter(), &countingEmbedder{})
		if _, err := svc.Upsert(context.Background(), "agent_memory", tc.docs); !errors.Is(err, tc.want) {
			t.Errorf("%s: want %v, got %v", tc.name, tc.want, err)
		}
	}
}

func TestUpsert_TooManyDocsRejected(t *testing.T) {
	svc := newTestService(newFakeWriter(), &countingEmbedder{})
	svc.MaxDocsPerRequest = 2
	docs := []Document{doc(1, "a"), doc(2, "b"), doc(3, "c")}
	if _, err := svc.Upsert(context.Background(), "agent_memory", docs); !errors.Is(err, ErrTooManyDocs) {
		t.Fatalf("want ErrTooManyDocs, got %v", err)
	}
}

func TestUpsert_BackendErrorsPropagate(t *testing.T) {
	// 写路径与读路径降级契约相反：错误必须上抛，供调用方重试与进度控制。
	w := newFakeWriter()
	w.retrieveErr = errors.New("qdrant down")
	svc := newTestService(w, &countingEmbedder{})
	if _, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(1, "c")}); err == nil {
		t.Fatal("retrieve failure must propagate")
	}

	w2 := newFakeWriter()
	e := &countingEmbedder{err: errors.New("gateway 429")}
	svc2 := newTestService(w2, e)
	if _, err := svc2.Upsert(context.Background(), "agent_memory", []Document{doc(1, "c")}); err == nil {
		t.Fatal("embed failure must propagate")
	}

	w3 := newFakeWriter()
	w3.upsertErr = errors.New("qdrant write timeout")
	svc3 := newTestService(w3, &countingEmbedder{})
	if _, err := svc3.Upsert(context.Background(), "agent_memory", []Document{doc(1, "c")}); err == nil {
		t.Fatal("upsert failure must propagate")
	}

	w4 := newFakeWriter()
	e4 := &countingEmbedder{empty: true} // 向量数与文本数不符
	svc4 := newTestService(w4, e4)
	if _, err := svc4.Upsert(context.Background(), "agent_memory", []Document{doc(1, "c"), doc(2, "d")}); err == nil {
		t.Fatal("length mismatch must propagate")
	}
}

func TestDelete_Idempotent(t *testing.T) {
	w := newFakeWriter()
	svc := newTestService(w, &countingEmbedder{})
	if _, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(5, "c")}); err != nil {
		t.Fatal(err)
	}
	deleted, err := svc.Delete(context.Background(), "agent_memory", 5)
	if err != nil || !deleted {
		t.Fatalf("want (true,nil), got (%v,%v)", deleted, err)
	}
	// 删除不存在的 ID：成功但 deleted=false。
	deleted, err = svc.Delete(context.Background(), "agent_memory", 5)
	if err != nil || deleted {
		t.Fatalf("idempotent delete: want (false,nil), got (%v,%v)", deleted, err)
	}
	if _, err = svc.Delete(context.Background(), "nope", 5); !errors.Is(err, ErrUnknownCollection) {
		t.Fatalf("unknown collection must be rejected, got %v", err)
	}
}

func TestGet(t *testing.T) {
	w := newFakeWriter()
	svc := newTestService(w, &countingEmbedder{})
	if _, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(5, "文档内容")}); err != nil {
		t.Fatal(err)
	}
	got, found, err := svc.Get(context.Background(), "agent_memory", 5)
	if err != nil || !found {
		t.Fatalf("want found, got (%v,%v)", found, err)
	}
	if got.Text != "文档内容" || got.TenantID != "t1" || got.ContentHash == "" {
		t.Fatalf("unexpected doc: %+v", got)
	}
	if _, found, err := svc.Get(context.Background(), "agent_memory", 999); err != nil || found {
		t.Fatalf("missing doc: want (false,nil), got (%v,%v)", found, err)
	}
}

func TestGetDoc(t *testing.T) {
	w := newFakeWriter()
	svc := newTestService(w, &countingEmbedder{})
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(5, "文档内容")}); err != nil {
		t.Fatal(err)
	}
	got, found, err := svc.GetDoc(context.Background(), "rag_documents", "t1", "5")
	if err != nil || !found {
		t.Fatalf("want found, got (%v,%v)", found, err)
	}
	if got.Text != "文档内容" || got.TenantID != "t1" || got.DocID != "5" || got.ChunkSeq != 0 {
		t.Fatalf("unexpected doc: %+v", got)
	}
	// 缺 tenant：无法派生 child ID → ErrTenantRequired（httpapi 映射 400）。
	if _, _, err := svc.GetDoc(context.Background(), "rag_documents", "", "5"); !errors.Is(err, ErrTenantRequired) {
		t.Fatalf("want ErrTenantRequired, got %v", err)
	}
	// 租户隔离：别的租户派生出不同 ID → 查不到。
	if _, found, err := svc.GetDoc(context.Background(), "rag_documents", "other", "5"); err != nil || found {
		t.Fatalf("other tenant: want (false,nil), got (%v,%v)", found, err)
	}
	// 未写入的文档 → found=false。
	if _, found, err := svc.GetDoc(context.Background(), "rag_documents", "t1", "999"); err != nil || found {
		t.Fatalf("missing doc: want (false,nil), got (%v,%v)", found, err)
	}
}

func TestUpsert_PartialSkip(t *testing.T) {
	// 混合批次：1 个已存在（同 hash）+ 1 个新文档 → 只嵌入新文档。
	w := newFakeWriter()
	e := &countingEmbedder{}
	svc := newTestService(w, e)
	if _, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(1, "old")}); err != nil {
		t.Fatal(err)
	}
	resp, err := svc.Upsert(context.Background(), "agent_memory", []Document{doc(1, "old"), doc(2, "new")})
	if err != nil {
		t.Fatal(err)
	}
	if e.lastN != 1 {
		t.Fatalf("only the new doc should be embedded, got %d", e.lastN)
	}
	if resp.Indexed != 1 || resp.Skipped != 1 {
		t.Fatalf("want indexed=1 skipped=1, got %+v", resp)
	}
}

// ==== docs 路径（Phase 3：分块 + 上下文增强） ====

// spyEnhancer 记录调用并可注入错误。
type spyEnhancer struct {
	calls int
	err   error
}

func (s *spyEnhancer) Enhance(_ context.Context, _ Document, _ chunk.Chunk) (string, error) {
	s.calls++
	if s.err != nil {
		return "", s.err
	}
	return "PREFIX-" + strconv.Itoa(s.calls), nil
}

func TestUpsert_DocsChunking(t *testing.T) {
	w := newFakeWriter()
	svc := newTestService(w, &countingEmbedder{})
	// 多父块多子块：60 句 ≈ 1300+ rune → 2 个 parent、多个 child。
	var sb strings.Builder
	for i := 0; i < 60; i++ {
		fmt.Fprintf(&sb, "这是第 %d 句话，用来把文档撑到跨父块与多子块的规模。", i)
	}
	content := sb.String()
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, content)}); err != nil {
		t.Fatal(err)
	}
	points := w.collections["rag_documents"]
	if len(points) < 3 {
		t.Fatalf("want multi-chunk doc, got %d chunks", len(points))
	}
	// 全部子块共享文档级 hash（child0 是幂等锚）；seq 从 0 连续编号；
	// 点 ID 与派生规则一致；parent_text 非空。
	var firstHash string
	seqs := map[int]bool{}
	parents := map[string]bool{}
	for id, p := range points {
		if p.DocID != "1" {
			t.Fatalf("unexpected doc_id %q", p.DocID)
		}
		if firstHash == "" {
			firstHash = p.ContentHash
		} else if p.ContentHash != firstHash {
			t.Fatalf("all chunks must share doc-level hash, got %q vs %q", p.ContentHash, firstHash)
		}
		if want := childPointID("t1", "1", p.ChunkSeq); id != want {
			t.Fatalf("point id %d != derived %d for seq %d", id, want, p.ChunkSeq)
		}
		if p.ParentText == "" {
			t.Fatal("parent_text must be stored on every child")
		}
		parents[p.ParentText] = true
		seqs[p.ChunkSeq] = true
	}
	for i := 0; i < len(seqs); i++ {
		if !seqs[i] {
			t.Fatalf("chunk seq %d missing, got %v", i, seqs)
		}
	}
	if len(parents) < 2 {
		t.Fatalf("want multi-parent doc, got %d parents", len(parents))
	}
}

func TestUpsert_DocsIdempotentSkip(t *testing.T) {
	w := newFakeWriter()
	e := &countingEmbedder{}
	svc := newTestService(w, e)
	d := doc(1, "值得记住的一句话。")
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{d}); err != nil {
		t.Fatal(err)
	}
	// 同文档重放：不重嵌、不触发删除。
	resp, err := svc.Upsert(context.Background(), "rag_documents", []Document{d})
	if err != nil {
		t.Fatal(err)
	}
	if e.calls != 1 {
		t.Fatalf("replay must skip embedding, got %d calls", e.calls)
	}
	if w.deleteByDocIDCalls != 0 {
		t.Fatalf("replay must not delete, got %d calls", w.deleteByDocIDCalls)
	}
	if resp.Skipped != 1 || resp.Indexed != 0 {
		t.Fatalf("want skipped=1 indexed=0, got %+v", resp)
	}
}

func TestUpsert_DocsContentChangeCleansOrphans(t *testing.T) {
	w := newFakeWriter()
	svc := newTestService(w, &countingEmbedder{})
	// v1 多子块 → v2 单子块：新子块数少于旧子块，必须整删后重写。
	long := strings.Repeat("一句用于撑长文档的话。", 60)
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, long)}); err != nil {
		t.Fatal(err)
	}
	if before := len(w.collections["rag_documents"]); before < 2 {
		t.Fatalf("want multi-chunk v1, got %d", before)
	}
	resp, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, "短文本。")})
	if err != nil {
		t.Fatal(err)
	}
	if w.deleteByDocIDCalls != 1 {
		t.Fatalf("stale doc must be deleted by doc_id, got %d calls", w.deleteByDocIDCalls)
	}
	if after := len(w.collections["rag_documents"]); after != 1 {
		t.Fatalf("orphan chunks must be cleaned, want 1 point, got %d", after)
	}
	if resp.Indexed != 1 || resp.Chunks != 1 {
		t.Fatalf("want indexed=1 chunks=1, got %+v", resp)
	}
}

func TestUpsert_DocsCtxVersionForcesReembed(t *testing.T) {
	w := newFakeWriter()
	e := &countingEmbedder{}
	svc := newTestService(w, e)
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, "同样的内容。")}); err != nil {
		t.Fatal(err)
	}
	// 改分块参数：ctxVersion 变 → 文档级 hash 变 → 同内容强制重嵌。
	svc.ChunkConfig = chunk.Config{ParentSize: 600, ChildSize: 200}
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, "同样的内容。")}); err != nil {
		t.Fatal(err)
	}
	if e.calls != 2 {
		t.Fatalf("ctx version change must force re-embed, got %d calls", e.calls)
	}
}

func TestUpsert_DocsEnhancement(t *testing.T) {
	w := newFakeWriter()
	e := &countingEmbedder{}
	en := &spyEnhancer{}
	svc := newTestService(w, e)
	svc.Enhancer = en
	svc.CtxVersion = "test-model"

	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, "需要上下文定位的内容。")}); err != nil {
		t.Fatal(err)
	}
	if en.calls == 0 {
		t.Fatal("enhancer must be called for every chunk")
	}
	for _, p := range w.collections["rag_documents"] {
		// 点文本 = 摘要前缀 + 子块正文（嵌入文本与存储文本同构）。
		if !strings.HasPrefix(p.Text, "PREFIX-") || !strings.HasSuffix(p.Text, "需要上下文定位的内容。") {
			t.Fatalf("point text must carry enhance prefix, got %q", p.Text)
		}
	}
}

func TestUpsert_DocsEnhanceFailureFails(t *testing.T) {
	w := newFakeWriter()
	svc := newTestService(w, &countingEmbedder{})
	svc.Enhancer = &spyEnhancer{err: errors.New("llm down")}
	// 增强失败 = 整批失败（写路径契约），不得写入任何点。
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, "内容。")}); err == nil {
		t.Fatal("enhance failure must fail the batch")
	}
	if len(w.collections["rag_documents"]) != 0 {
		t.Fatalf("no points must be written on failure, got %d", len(w.collections["rag_documents"]))
	}
}

func TestUpsert_DocsTooManyChunks(t *testing.T) {
	svc := newTestService(newFakeWriter(), &countingEmbedder{})
	svc.MaxChunksPerRequest = 2
	long := strings.Repeat("填充句子。", 300) // 1500 rune → 4 子块 > 上限 2
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, long)}); !errors.Is(err, ErrTooManyChunks) {
		t.Fatalf("want ErrTooManyChunks, got %v", err)
	}
}

func TestUpsert_DocsBlankContent(t *testing.T) {
	svc := newTestService(newFakeWriter(), &countingEmbedder{})
	d := Document{ID: "1", Content: "  \n  ", Metadata: DocumentMetadata{TenantID: "t1"}}
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{d}); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("blank content must be invalid, got %v", err)
	}
}

func TestUpsert_DocsValidation(t *testing.T) {
	svc := newTestService(newFakeWriter(), &countingEmbedder{})
	// 空 ID：docs 的 ID 是逻辑标识，但不可为空。
	d := Document{ID: "", Content: "c", Metadata: DocumentMetadata{TenantID: "t1"}}
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{d}); !errors.Is(err, ErrInvalidDocument) {
		t.Fatalf("empty id: want ErrInvalidDocument, got %v", err)
	}
	// 非数字 ID 合法（与 memory 路径的关键差异）：点 ID 由内部派生。
	d2 := Document{ID: "faq-deploy", Content: "c", Metadata: DocumentMetadata{TenantID: "t1"}}
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{d2}); err != nil {
		t.Fatalf("logical doc id must be accepted, got %v", err)
	}
}

func TestUpsert_DocsSubBatchEmbed(t *testing.T) {
	w := newFakeWriter()
	e := &countingEmbedder{}
	svc := newTestService(w, e)
	// ~91 子块（>64 子批、<256 上限）：嵌入必须拆分为 64+27 两批。
	long := strings.Repeat("word. ", 6000) // 36000 rune → 91 子块
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(1, long)}); err != nil {
		t.Fatal(err)
	}
	wantCalls := (len(w.collections["rag_documents"]) + defaultChunkEmbedBatch - 1) / defaultChunkEmbedBatch
	if e.calls != wantCalls || wantCalls < 2 {
		t.Fatalf("want %d embed calls, got %d (sizes=%v)", wantCalls, e.calls, e.sizes)
	}
	if e.sizes[0] != defaultChunkEmbedBatch {
		t.Fatalf("first sub-batch must be %d, got %v", defaultChunkEmbedBatch, e.sizes)
	}
}

func TestDeleteDoc(t *testing.T) {
	w := newFakeWriter()
	svc := newTestService(w, &countingEmbedder{})
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(7, strings.Repeat("一句话。", 150))}); err != nil {
		t.Fatal(err)
	}
	if n := len(w.collections["rag_documents"]); n < 2 {
		t.Fatalf("want multi-chunk doc, got %d", n)
	}
	deleted, err := svc.DeleteDoc(context.Background(), "rag_documents", "t1", "7")
	if err != nil || !deleted {
		t.Fatalf("want (true,nil), got (%v,%v)", deleted, err)
	}
	if n := len(w.collections["rag_documents"]); n != 0 {
		t.Fatalf("all chunks must be deleted, got %d", n)
	}
	// 幂等：重复删除 deleted=false。
	if deleted, err = svc.DeleteDoc(context.Background(), "rag_documents", "t1", "7"); err != nil || deleted {
		t.Fatalf("idempotent delete: want (false,nil), got (%v,%v)", deleted, err)
	}
	// 缺 tenant → ErrTenantRequired。
	if _, err := svc.DeleteDoc(context.Background(), "rag_documents", "", "7"); !errors.Is(err, ErrTenantRequired) {
		t.Fatalf("want ErrTenantRequired, got %v", err)
	}
}
