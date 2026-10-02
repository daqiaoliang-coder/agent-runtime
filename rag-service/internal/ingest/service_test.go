package ingest

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/daqiaoliang-coder/rag-service/internal/sparse"
	"github.com/daqiaoliang-coder/rag-service/internal/vector"
)

// fakeWriter 模拟向量库：按集合内存储点，统计 UpsertDefault/UpsertHybrid 调用。
type fakeWriter struct {
	collections map[string]map[uint64]vector.DocPoint

	defaultCalls int
	hybridCalls  int
	retrieveErr  error
	upsertErr    error
	deleteErr    error
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
				ContentHash: p.ContentHash,
			}
		}
	}
	return out, nil
}

// countingEmbedder 返回固定维度向量并统计调用次数与批量大小。
type countingEmbedder struct {
	calls int
	lastN int
	err   error
	empty bool // 返回与入参不等长的向量，模拟网关异常
}

func (e *countingEmbedder) Embed(_ context.Context, texts []string) ([][]float32, error) {
	e.calls++
	e.lastN = len(texts)
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
	p := w.collections["rag_documents"][9]
	if p.Sparse == nil || len(p.Sparse.Indices) == 0 {
		t.Fatal("docs layout must carry sparse vector")
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
	if _, err := svc.Upsert(context.Background(), "rag_documents", []Document{doc(5, "文档内容")}); err != nil {
		t.Fatal(err)
	}
	got, found, err := svc.Get(context.Background(), "rag_documents", 5)
	if err != nil || !found {
		t.Fatalf("want found, got (%v,%v)", found, err)
	}
	if got.Text != "文档内容" || got.TenantID != "t1" || got.ContentHash == "" {
		t.Fatalf("unexpected doc: %+v", got)
	}
	if _, found, err := svc.Get(context.Background(), "rag_documents", 999); err != nil || found {
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
