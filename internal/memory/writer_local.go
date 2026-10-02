// LocalWriter 是 Writer 的进程内实现：批量 embed + 直连 Qdrant upsert。
// 它承接原 projectBatch 的内联步骤（embed → 组 Point → Upsert），
// 行为逐行等价，是 local 后端与影子对比的基准路径。
package memory

import (
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/llm"
	"context"
	"fmt"
)

// LocalWriter 依赖注入：Embedder 与 Vectors 任一为 nil 时方法返回配置错误
// 而非 panic，装配遗漏在索引器启动时立刻暴露。
type LocalWriter struct {
	Embedder llm.Embedder
	Vectors  vector.VectorStore
}

var _ Writer = (*LocalWriter)(nil)

// EnsureCollection 委托向量库建集合（幂等，与原 Indexer.EnsureCollection 等价）。
func (w *LocalWriter) EnsureCollection(ctx context.Context, collection string, dim int) error {
	if w == nil || w.Vectors == nil {
		return fmt.Errorf("memory: local writer not configured (vectors required)")
	}
	if dim <= 0 {
		return fmt.Errorf("memory: embedding dimension must be set (EMBEDDING_DIM)")
	}
	return w.Vectors.EnsureCollection(ctx, collection, dim)
}

// Write 批量嵌入并写入。texts[i] 与 docs[i] 严格按下标对应，
// 任何一步失败整批上抛（调用方对整批退避重试，幂等 ID 保证重试安全）。
func (w *LocalWriter) Write(ctx context.Context, collection string, docs []Document) error {
	if w == nil || w.Embedder == nil || w.Vectors == nil {
		return fmt.Errorf("memory: local writer not configured (embedder/vectors required)")
	}
	if len(docs) == 0 {
		return nil
	}
	texts := make([]string, 0, len(docs))
	for _, d := range docs {
		texts = append(texts, d.Text)
	}
	vecs, err := w.Embedder.Embed(ctx, texts)
	if err != nil {
		return fmt.Errorf("embed %d texts: %w", len(texts), err)
	}
	if len(vecs) != len(texts) {
		return fmt.Errorf("embed returned %d vectors for %d texts", len(vecs), len(texts))
	}
	points := make([]vector.Point, 0, len(docs))
	for i, d := range docs {
		if len(vecs[i]) == 0 {
			return fmt.Errorf("embed returned empty vector for node %s role %s", d.NodeID, d.Role)
		}
		points = append(points, vector.Point{
			ID:        d.ID,
			Vector:    vecs[i],
			TenantID:  d.TenantID,
			ThreadID:  d.ThreadID,
			RunID:     d.RunID,
			NodeID:    d.NodeID,
			Role:      d.Role,
			Text:      d.Text,
			CreatedAt: d.CreatedAt,
		})
	}
	if err := w.Vectors.Upsert(ctx, collection, points); err != nil {
		return fmt.Errorf("upsert %d points: %w", len(points), err)
	}
	return nil
}
