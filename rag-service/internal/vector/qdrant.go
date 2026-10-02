// Qdrant 的 gRPC 实现。检索逻辑与 runtime internal/adapters/vector 的
// Qdrant.Search 逐行等价（相同的过滤构建与 payload 还原），这是 Phase 1
// 影子对比验收的前提：任何非刻意的语义差异都会被对比测试放大暴露。
// Phase 2 增补写路径（Upsert/Delete/Retrieve）与混合检索（prefetch+RRF）。
package vector

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/qdrant/go-client/qdrant"
)

// Qdrant 是 Searcher 的实现，同时提供 Health 供就绪探针使用。
type Qdrant struct {
	client *qdrant.Client
}

// NewQdrant 连接 Qdrant 服务端。apiKey 为空表示服务端未开启鉴权（本地部署常见）。
func NewQdrant(host string, port int, apiKey string) (*Qdrant, error) {
	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = 6334 // gRPC 端口，不是 REST/Web UI 的 6333
	}
	c, err := qdrant.NewClient(&qdrant.Config{Host: host, Port: port, APIKey: apiKey})
	if err != nil {
		return nil, fmt.Errorf("vector: connect qdrant %s:%d: %w", host, port, err)
	}
	return &Qdrant{client: c}, nil
}

// Close 释放底层 gRPC 连接，由装配方在进程退出时调用。
func (q *Qdrant) Close() error {
	if q.client == nil {
		return nil
	}
	return q.client.Close()
}

// Health 供 /readyz 探针：确认 Qdrant 可达。
func (q *Qdrant) Health(ctx context.Context) error {
	if _, err := q.client.HealthCheck(ctx); err != nil {
		return fmt.Errorf("vector: qdrant health: %w", err)
	}
	return nil
}

// Search 按余弦相似度检索，返回 topK 条结果（Score 降序）。
//
// 过滤构建与 runtime 完全一致：
//   - TenantID 无条件 must（fail-closed：空租户匹配不到任何点，绝不能退化为全量）；
//   - ThreadID 非空才追加该条件；
//   - ExcludeRunID 非空时用 must_not 源头排除。
//
// MinScore 不在此层处理，由 search.Service 统一过滤，使本实现与测试 fake
// 行为一致（与 runtime 的分层决策相同），测试结论才能代表生产行为。
func (q *Qdrant) Search(ctx context.Context, collection string, vec []float32, f Filter, topK int) ([]Hit, error) {
	if len(vec) == 0 {
		return nil, fmt.Errorf("vector: search with empty query vector")
	}
	if topK <= 0 {
		topK = 10
	}
	must := []*qdrant.Condition{
		qdrant.NewMatchKeyword(PayloadTenantID, f.TenantID),
	}
	if f.ThreadID != "" {
		must = append(must, qdrant.NewMatchKeyword(PayloadThreadID, f.ThreadID))
	}
	flt := &qdrant.Filter{Must: must}
	if f.ExcludeRunID != "" {
		flt.MustNot = []*qdrant.Condition{
			qdrant.NewMatchKeyword(PayloadRunID, f.ExcludeRunID),
		}
	}
	hits, err := q.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: collection,
		Query:          qdrant.NewQuery(vec...),
		Filter:         flt,
		Limit:          uint64Ptr(uint64(topK)),
		// 取回 payload 才能还原 role/text/created_at，否则只剩分数与 ID。
		WithPayload: qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("vector: search %q: %w", collection, err)
	}
	out := make([]Hit, 0, len(hits))
	for _, h := range hits {
		if h == nil {
			continue
		}
		p := h.GetPayload()
		out = append(out, Hit{
			Score:    h.GetScore(),
			PointID:  pointIDFromPB(h.GetId()),
			TenantID: p[PayloadTenantID].GetStringValue(),
			ThreadID: p[PayloadThreadID].GetStringValue(),
			RunID:    p[PayloadRunID].GetStringValue(),
			NodeID:   p[PayloadNodeID].GetStringValue(),
			Role:     p[PayloadRole].GetStringValue(),
			Text:     p[PayloadText].GetStringValue(),
			// 与 runtime 的 CreatedAtFromPayload 等价：payload 存 Unix 纳秒，
			// 非法/缺失值退化为零值时间。
			CreatedAt: time.Unix(0, p[PayloadCreatedAt].GetIntegerValue()),
		})
	}
	return out, nil
}

// pointIDFromPB 从 protobuf point ID 还原数值 ID；非数值型（UUID）返回 0。
func pointIDFromPB(id *qdrant.PointId) uint64 {
	if id == nil {
		return 0
	}
	if n, ok := id.GetPointIdOptions().(*qdrant.PointId_Num); ok {
		return n.Num
	}
	return 0
}

func uint64Ptr(v uint64) *uint64 { return &v }

// ==== 写路径（Phase 2） ====

// buildFilter 与 Search 相同的 fail-closed 过滤构建，写路径的 Retrieve
// 与混合检索共用。
func buildFilter(f Filter) *qdrant.Filter {
	must := []*qdrant.Condition{
		qdrant.NewMatchKeyword(PayloadTenantID, f.TenantID),
	}
	if f.ThreadID != "" {
		must = append(must, qdrant.NewMatchKeyword(PayloadThreadID, f.ThreadID))
	}
	flt := &qdrant.Filter{Must: must}
	if f.ExcludeRunID != "" {
		flt.MustNot = []*qdrant.Condition{
			qdrant.NewMatchKeyword(PayloadRunID, f.ExcludeRunID),
		}
	}
	return flt
}

// EnsureMemoryCollection 与 runtime memory-indexer 的 EnsureCollection 逐行等价：
// 未命名 dense 向量（Cosine + dim）+ tenant/thread/run/node payload 索引。
// 维度不匹配时显式报错而非静默重建——换 embedding 模型必须人工介入。
func (q *Qdrant) EnsureMemoryCollection(ctx context.Context, collection string, dim int) error {
	exists, err := q.client.CollectionExists(ctx, collection)
	if err != nil {
		return fmt.Errorf("vector: check collection %q: %w", collection, err)
	}
	if exists {
		info, err := q.client.GetCollectionInfo(ctx, collection)
		if err != nil {
			return fmt.Errorf("vector: inspect collection %q: %w", collection, err)
		}
		if existing := info.GetConfig().GetParams().GetVectorsConfig().GetParams().GetSize(); int(existing) != dim {
			return fmt.Errorf("vector: collection %q has dim %d but RAG_EMBED_DIM is %d; align config or rebuild the index", collection, existing, dim)
		}
		return q.ensurePayloadIndexes(ctx, collection)
	}
	if err := q.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collection,
		VectorsConfig:  qdrant.NewVectorsConfig(&qdrant.VectorParams{Size: uint64(dim), Distance: qdrant.Distance_Cosine}),
	}); err != nil {
		return fmt.Errorf("vector: create collection %q: %w", collection, err)
	}
	return q.ensurePayloadIndexes(ctx, collection)
}

// EnsureDocsCollection 创建 docs 布局集合：命名向量 dense（Cosine + dim）
// 与 sparse（Qdrant 内置稀疏索引）。既有集合校验 dense 维度。
func (q *Qdrant) EnsureDocsCollection(ctx context.Context, collection string, dim int) error {
	exists, err := q.client.CollectionExists(ctx, collection)
	if err != nil {
		return fmt.Errorf("vector: check collection %q: %w", collection, err)
	}
	if exists {
		info, err := q.client.GetCollectionInfo(ctx, collection)
		if err != nil {
			return fmt.Errorf("vector: inspect collection %q: %w", collection, err)
		}
		params := info.GetConfig().GetParams().GetVectorsConfig().GetParamsMap().GetMap()
		if p, ok := params[VectorNameDense]; ok && int(p.GetSize()) != dim {
			return fmt.Errorf("vector: collection %q dense dim %d != RAG_EMBED_DIM %d; align config or rebuild the index", collection, p.GetSize(), dim)
		}
		return q.ensurePayloadIndexes(ctx, collection)
	}
	if err := q.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collection,
		VectorsConfig: qdrant.NewVectorsConfigMap(map[string]*qdrant.VectorParams{
			VectorNameDense: {Size: uint64(dim), Distance: qdrant.Distance_Cosine},
		}),
		SparseVectorsConfig: &qdrant.SparseVectorConfig{
			Map: map[string]*qdrant.SparseVectorParams{VectorNameSparse: {}},
		},
	}); err != nil {
		return fmt.Errorf("vector: create docs collection %q: %w", collection, err)
	}
	return q.ensurePayloadIndexes(ctx, collection)
}

// ensurePayloadIndexes 为隔离键建 keyword 索引；"已存在"错误被吞掉（预期状态）。
func (q *Qdrant) ensurePayloadIndexes(ctx context.Context, collection string) error {
	for _, field := range []string{PayloadTenantID, PayloadThreadID, PayloadRunID, PayloadNodeID} {
		ft := qdrant.FieldType_FieldTypeKeyword
		if _, err := q.client.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
			CollectionName: collection,
			Wait:           boolPtr(true),
			FieldName:      field,
			FieldType:      &ft,
		}); err != nil && !isAlreadyIndexed(err) {
			return fmt.Errorf("vector: create payload index %q on %q: %w", field, collection, err)
		}
	}
	return nil
}

func isAlreadyIndexed(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "already indexed"))
}

func boolPtr(b bool) *bool { return &b }

// docPayload 展开 DocPoint 的 payload：runtime 契约键 + content_hash。
func docPayload(p DocPoint) map[string]any {
	return map[string]any{
		PayloadTenantID:    p.TenantID,
		PayloadThreadID:    p.ThreadID,
		PayloadRunID:       p.RunID,
		PayloadNodeID:      p.NodeID,
		PayloadRole:        p.Role,
		PayloadText:        p.Text,
		PayloadCreatedAt:   p.CreatedAt.UnixNano(),
		PayloadContentHash: p.ContentHash,
	}
}

// UpsertDefault 以未命名向量写入（memory 布局），wait=true 保证
// "Upsert 成功 → 调用方标记进度"的顺序不变量（与 runtime indexer 一致）。
func (q *Qdrant) UpsertDefault(ctx context.Context, collection string, points []DocPoint) error {
	if len(points) == 0 {
		return nil
	}
	structs := make([]*qdrant.PointStruct, 0, len(points))
	for _, p := range points {
		if len(p.Dense) == 0 {
			return fmt.Errorf("vector: point %d has empty dense vector", p.ID)
		}
		structs = append(structs, &qdrant.PointStruct{
			Id:      qdrant.NewIDNum(p.ID),
			Payload: qdrant.NewValueMap(docPayload(p)),
			Vectors: qdrant.NewVectors(p.Dense...),
		})
	}
	if _, err := q.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection, Wait: boolPtr(true), Points: structs,
	}); err != nil {
		return fmt.Errorf("vector: upsert %d points into %q: %w", len(structs), collection, err)
	}
	return nil
}

// UpsertHybrid 以命名向量写入 dense+sparse（docs 布局）。
func (q *Qdrant) UpsertHybrid(ctx context.Context, collection string, points []DocPoint) error {
	if len(points) == 0 {
		return nil
	}
	structs := make([]*qdrant.PointStruct, 0, len(points))
	for _, p := range points {
		if len(p.Dense) == 0 {
			return fmt.Errorf("vector: point %d has empty dense vector", p.ID)
		}
		vecs := map[string]*qdrant.Vector{VectorNameDense: qdrant.NewVectorDense(p.Dense)}
		if p.Sparse != nil && len(p.Sparse.Indices) > 0 {
			vecs[VectorNameSparse] = qdrant.NewVectorSparse(p.Sparse.Indices, p.Sparse.Values)
		}
		structs = append(structs, &qdrant.PointStruct{
			Id:      qdrant.NewIDNum(p.ID),
			Payload: qdrant.NewValueMap(docPayload(p)),
			Vectors: qdrant.NewVectorsMap(vecs),
		})
	}
	if _, err := q.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection, Wait: boolPtr(true), Points: structs,
	}); err != nil {
		return fmt.Errorf("vector: upsert %d hybrid points into %q: %w", len(structs), collection, err)
	}
	return nil
}

// Delete 按 ID 删除（幂等：不存在的 ID 也成功），wait=true 保证删除可见。
func (q *Qdrant) Delete(ctx context.Context, collection string, ids ...uint64) error {
	if len(ids) == 0 {
		return nil
	}
	pbIDs := make([]*qdrant.PointId, 0, len(ids))
	for _, id := range ids {
		pbIDs = append(pbIDs, qdrant.NewIDNum(id))
	}
	if _, err := q.client.Delete(ctx, &qdrant.DeletePoints{
		CollectionName: collection,
		Wait:           boolPtr(true),
		Points:         qdrant.NewPointsSelectorIDs(pbIDs),
	}); err != nil {
		return fmt.Errorf("vector: delete %d points from %q: %w", len(ids), collection, err)
	}
	return nil
}

// Retrieve 按 ID 批量取回点（含 payload），供幂等检查与 GET status 复用。
// 返回 map 而非切片：调用方按 ID 查找，且天然表达"部分存在"。
func (q *Qdrant) Retrieve(ctx context.Context, collection string, ids []uint64) (map[uint64]RetrievedDoc, error) {
	if len(ids) == 0 {
		return map[uint64]RetrievedDoc{}, nil
	}
	pbIDs := make([]*qdrant.PointId, 0, len(ids))
	for _, id := range ids {
		pbIDs = append(pbIDs, qdrant.NewIDNum(id))
	}
	pts, err := q.client.Get(ctx, &qdrant.GetPoints{
		CollectionName: collection,
		Ids:            pbIDs,
		WithPayload:    qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("vector: retrieve %d points from %q: %w", len(ids), collection, err)
	}
	out := make(map[uint64]RetrievedDoc, len(pts))
	for _, pt := range pts {
		if pt == nil {
			continue
		}
		id := pointIDFromPB(pt.GetId())
		p := pt.GetPayload()
		out[id] = RetrievedDoc{
			ID:          id,
			TenantID:    p[PayloadTenantID].GetStringValue(),
			ThreadID:    p[PayloadThreadID].GetStringValue(),
			RunID:       p[PayloadRunID].GetStringValue(),
			NodeID:      p[PayloadNodeID].GetStringValue(),
			Role:        p[PayloadRole].GetStringValue(),
			Text:        p[PayloadText].GetStringValue(),
			CreatedAt:   time.Unix(0, p[PayloadCreatedAt].GetIntegerValue()),
			ContentHash: p[PayloadContentHash].GetStringValue(),
		}
	}
	return out, nil
}

// HybridSearch 混合检索：dense 与 sparse 各自 prefetch，服务端 RRF 融合。
// 相比客户端 RRF（两次查询+本地融合），服务端融合只需一次往返，
// 且过滤语义在 Qdrant 内一致执行；SDK v1.19+ 与 Qdrant 服务端均原生支持。
//
// prefetch 深度取 topK*4（下限 20）：RRF 的质量取决于两路召回的覆盖面，
// 只 prefetch topK 会把融合前的候选截得太狠。
func (q *Qdrant) HybridSearch(ctx context.Context, collection string, dense []float32, sparse SparseVector, f Filter, topK int) ([]Hit, error) {
	if len(dense) == 0 {
		return nil, fmt.Errorf("vector: hybrid search with empty dense vector")
	}
	if topK <= 0 {
		topK = 10
	}
	prefetchLimit := topK * 4
	if prefetchLimit < 20 {
		prefetchLimit = 20
	}
	flt := buildFilter(f)
	hits, err := q.client.Query(ctx, &qdrant.QueryPoints{
		CollectionName: collection,
		Prefetch: []*qdrant.PrefetchQuery{
			{
				Query:  qdrant.NewQueryDense(dense),
				Using:  strPtr(VectorNameDense),
				Filter: flt,
				Limit:  uint64Ptr(uint64(prefetchLimit)),
			},
			{
				Query:  qdrant.NewQuerySparse(sparse.Indices, sparse.Values),
				Using:  strPtr(VectorNameSparse),
				Filter: flt,
				Limit:  uint64Ptr(uint64(prefetchLimit)),
			},
		},
		// RRF 融合：score = Σ 1/(60+rank_i)，两路均命中的点得分最高。
		// Filter 同时设置在顶层与每个 prefetch：prefetch 未显式带过滤时，
		// 两路召回会先取出无过滤候选占满名额，租户隔离虽在融合结果仍成立，
		// 但召回质量会显著劣化（名额被其他租户的点占走）。
		Query:       qdrant.NewQueryFusion(qdrant.Fusion_RRF),
		Filter:      flt,
		Limit:       uint64Ptr(uint64(topK)),
		WithPayload: qdrant.NewWithPayload(true),
	})
	if err != nil {
		return nil, fmt.Errorf("vector: hybrid search %q: %w", collection, err)
	}
	out := make([]Hit, 0, len(hits))
	for _, h := range hits {
		if h == nil {
			continue
		}
		p := h.GetPayload()
		out = append(out, Hit{
			Score:     h.GetScore(),
			PointID:   pointIDFromPB(h.GetId()),
			TenantID:  p[PayloadTenantID].GetStringValue(),
			ThreadID:  p[PayloadThreadID].GetStringValue(),
			RunID:     p[PayloadRunID].GetStringValue(),
			NodeID:    p[PayloadNodeID].GetStringValue(),
			Role:      p[PayloadRole].GetStringValue(),
			Text:      p[PayloadText].GetStringValue(),
			CreatedAt: time.Unix(0, p[PayloadCreatedAt].GetIntegerValue()),
		})
	}
	return out, nil
}

func strPtr(s string) *string { return &s }

// 编译期断言：Qdrant 同时满足读（Search/HybridSearch）与写（Writer）契约。
var (
	_ Searcher       = (*Qdrant)(nil)
	_ HybridSearcher = (*Qdrant)(nil)
	_ Writer         = (*Qdrant)(nil)
)
