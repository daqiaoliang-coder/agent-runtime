// 本文件是 VectorStore 的内存实现，**仅供测试与本地演示使用**。
// 它使 providers / worker / memory-indexer 的单测无需真跑 Qdrant 即可验证
// 写入、召回、隔离与降级逻辑。生产装配请使用 qdrant.go 中的实现。
package vector

import (
	"context"
	"math"
	"sort"
	"sync"
)

// Fake 是内存版 VectorStore：暴力遍历算余弦相似度，行为与 Qdrant 对齐
// （相同 ID 覆盖、tenant/thread 过滤、按 Score 降序、topK 截断）。
type Fake struct {
	mu          sync.RWMutex
	collections map[string]int // collection -> 维度，记录 EnsureCollection 的调用
	points      map[string]map[uint64]Point
	// FailNext 让测试注入故障，验证上层降级：为 true 时下一次操作返回错误。
	FailNext    bool
	UpsertCalls int
	SearchCalls int
}

// NewFake 创建一个空的内存向量库。
func NewFake() *Fake {
	return &Fake{
		collections: make(map[string]int),
		points:      make(map[string]map[uint64]Point),
	}
}

var errFakeFailure = fakeError("fake vector store failure")

type fakeError string

func (e fakeError) Error() string { return string(e) }

func (f *Fake) EnsureCollection(_ context.Context, collection string, dim int) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.FailNext {
		f.FailNext = false
		return errFakeFailure
	}
	f.collections[collection] = dim
	if f.points[collection] == nil {
		f.points[collection] = make(map[uint64]Point)
	}
	return nil
}

func (f *Fake) Upsert(_ context.Context, collection string, points []Point) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.UpsertCalls++
	if f.FailNext {
		f.FailNext = false
		return errFakeFailure
	}
	if f.points[collection] == nil {
		f.points[collection] = make(map[uint64]Point)
	}
	for _, p := range points {
		// 相同 ID 覆盖而非追加——这正是 Qdrant Upsert 的语义，
		// 也是索引器重放不产生重复向量所依赖的行为。
		f.points[collection][p.ID] = p
	}
	return nil
}

// Search 返回 topK 条最相似的点。
//
// 用写锁而非读锁：本方法会修改 FailNext / SearchCalls 等计数与故障状态，
// 在读锁下修改属于数据竞争（并发 Search 会触发 race detector 告警）。
// fake 只用于测试，此处优先保证并发正确性而非读并发度。
func (f *Fake) Search(_ context.Context, collection string, vec []float32, flt Filter, topK int) ([]Hit, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.SearchCalls++
	if f.FailNext {
		f.FailNext = false
		return nil, errFakeFailure
	}
	var out []Hit
	for _, p := range f.points[collection] {
		if p.TenantID != flt.TenantID {
			continue
		}
		// ThreadID 为空表示不按会话过滤（存量数据 thread_id 为空串）。
		if flt.ThreadID != "" && p.ThreadID != flt.ThreadID {
			continue
		}
		// 在源头排除指定 Run：当前正在执行的 Run 的历史由 CompletedNodes 提供，
		// 若不排除会在上下文中重复出现同一段对话。
		if flt.ExcludeRunID != "" && p.RunID == flt.ExcludeRunID {
			continue
		}
		out = append(out, Hit{Point: p, Score: cosine(vec, p.Vector)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	if topK > 0 && len(out) > topK {
		out = out[:topK]
	}
	return out, nil
}

// Count 返回集合中的点数，供幂等性断言使用（重复 Upsert 不应增加点数）。
func (f *Fake) Count(collection string) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return len(f.points[collection])
}

// Dimension 返回 EnsureCollection 记录的维度，0 表示未建过集合。
func (f *Fake) Dimension(collection string) int {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.collections[collection]
}

// cosine 计算余弦相似度，任一向量模长为 0 时返回 0（视为不相似）。
func cosine(a, b []float32) float32 {
	if len(a) == 0 || len(a) != len(b) {
		return 0
	}
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return float32(dot / (math.Sqrt(na) * math.Sqrt(nb)))
}
