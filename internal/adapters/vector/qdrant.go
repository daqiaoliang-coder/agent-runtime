// 本文件是 VectorStore 的 Qdrant 实现，通过 gRPC 通信（默认端口 6334，
// 注意 6333 是 REST/Web UI，用错端口会得到难以理解的连接错误）。
//
// SDK 细节被完全封闭在本文件内：上层只见 VectorStore 接口，
// 因此未来替换为 Milvus / Weaviate 时，providers 与 worker 无需改动。
package vector

import (
	"context"
	"fmt"
	"strings"

	"github.com/qdrant/go-client/qdrant"
)

// Qdrant 是基于 Qdrant 官方 Go 客户端的 VectorStore 实现。
type Qdrant struct {
	client *qdrant.Client
	// waitUpsert 控制写入是否等待落盘确认。
	// 必须为 true：索引器依赖"Upsert 成功 → MarkMemoryIndexed"的顺序保证，
	// 异步写入会让进度表领先于实际数据，崩溃后该节点被永久跳过、向量丢失。
	waitUpsert bool
}

// NewQdrant 连接到 Qdrant 服务端。apiKey 为空表示服务端未开启鉴权（本地部署常见）。
func NewQdrant(host string, port int, apiKey string) (*Qdrant, error) {
	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = 6334 // gRPC 端口，不是 REST 的 6333
	}
	c, err := qdrant.NewClient(&qdrant.Config{Host: host, Port: port, APIKey: apiKey})
	if err != nil {
		return nil, fmt.Errorf("vector: connect qdrant %s:%d: %w", host, port, err)
	}
	return &Qdrant{client: c, waitUpsert: true}, nil
}

// Close 释放底层 gRPC 连接，由装配方在进程退出时调用。
func (q *Qdrant) Close() error {
	if q.client == nil {
		return nil
	}
	return q.client.Close()
}

// EnsureCollection 幂等地确保集合存在：Cosine 距离 + 指定维度，并为隔离键建 payload 索引。
//
// 维度校验是刻意保留的：换 embedding 模型（如 1536 → 3072 维）时，若不校验就写入，
// Qdrant 会返回难以定位的向量长度错误。这里显式报错，提示需要新建 collection 或重建索引。
func (q *Qdrant) EnsureCollection(ctx context.Context, collection string, dim int) error {
	if dim <= 0 {
		return fmt.Errorf("vector: invalid embedding dimension %d", dim)
	}
	exists, err := q.client.CollectionExists(ctx, collection)
	if err != nil {
		return fmt.Errorf("vector: check collection %q: %w", collection, err)
	}
	if exists {
		info, err := q.client.GetCollectionInfo(ctx, collection)
		if err != nil {
			return fmt.Errorf("vector: inspect collection %q: %w", collection, err)
		}
		existing := info.GetConfig().GetParams().GetVectorsConfig().GetParams().GetSize()
		if int(existing) != dim {
			return fmt.Errorf("vector: collection %q has dim %d but embedder produces %d; create a new collection or rebuild the index", collection, existing, dim)
		}
		// 集合已存在仍尝试补建 payload 索引（历史集合可能缺索引），失败不阻断。
		q.ensurePayloadIndexes(ctx, collection)
		return nil
	}
	err = q.client.CreateCollection(ctx, &qdrant.CreateCollection{
		CollectionName: collection,
		VectorsConfig: qdrant.NewVectorsConfig(&qdrant.VectorParams{
			Size:     uint64(dim),
			Distance: qdrant.Distance_Cosine,
		}),
	})
	if err != nil {
		return fmt.Errorf("vector: create collection %q: %w", collection, err)
	}
	if err := q.ensurePayloadIndexes(ctx, collection); err != nil {
		return err
	}
	return nil
}

// ensurePayloadIndexes 为 tenant_id / thread_id 建 keyword payload 索引。
//
// 这两个字段是每次检索的过滤条件，无索引时 Qdrant 退化为全量扫描 payload，
// 数据量上来后检索延迟会显著恶化。重复创建时 Qdrant 返回错误，此处吞掉——
// 索引已存在是预期状态，不是故障。
func (q *Qdrant) ensurePayloadIndexes(ctx context.Context, collection string) error {
	for _, field := range []string{PayloadTenantID, PayloadThreadID, PayloadRunID, PayloadNodeID} {
		ft := qdrant.FieldType_FieldTypeKeyword
		_, err := q.client.CreateFieldIndex(ctx, &qdrant.CreateFieldIndexCollection{
			CollectionName: collection,
			Wait:           boolPtr(true),
			FieldName:      field,
			FieldType:      &ft,
		})
		if err != nil && !isAlreadyIndexed(err) {
			return fmt.Errorf("vector: create payload index %q on %q: %w", field, collection, err)
		}
	}
	return nil
}

// isAlreadyIndexed 判断错误是否为"索引已存在"。Qdrant 对此返回带提示文本的错误，
// 依赖消息文本匹配（SDK 未暴露专用错误类型），故做宽松匹配。
func isAlreadyIndexed(err error) bool {
	return err != nil && strings.Contains(err.Error(), "already exists")
}

// Upsert 批量写入或覆盖点。相同 ID 重复写入不增加点数——这是索引器重放幂等的基础，
// 而幂等性完全依赖 PointID 的确定性（见 provider.go 中的契约说明）。
func (q *Qdrant) Upsert(ctx context.Context, collection string, points []Point) error {
	if len(points) == 0 {
		return nil
	}
	structs := make([]*qdrant.PointStruct, 0, len(points))
	for _, p := range points {
		if len(p.Vector) == 0 {
			return fmt.Errorf("vector: point for node %q has empty vector", p.NodeID)
		}
		structs = append(structs, &qdrant.PointStruct{
			Id:      &qdrant.PointId{PointIdOptions: &qdrant.PointId_Num{Num: p.ID}},
			Payload: qdrant.NewValueMap(p.PayloadFields()),
			Vectors: qdrant.NewVectors(p.Vector...),
		})
	}
	_, err := q.client.Upsert(ctx, &qdrant.UpsertPoints{
		CollectionName: collection,
		Wait:           boolPtr(q.waitUpsert),
		Points:         structs,
	})
	if err != nil {
		return fmt.Errorf("vector: upsert %d points into %q: %w", len(structs), collection, err)
	}
	return nil
}

// Search 按余弦相似度检索，返回 topK 条结果（Score 降序）。
//
// 租户过滤是**无条件**加上的：TenantID 为空时不会跳过，因为空租户查询
// 应当查不到任何数据，而不是查到全部数据——这是越权防线，语义上必须 fail-closed。
// ThreadID 为空则不加该条件（存量 Run 的 thread_id 全是空串，
// 无条件过滤会导致新会话查不到任何旧数据）。
// ExcludeRunID 非空时用 must_not 排除该 Run（通常是当前正在执行的 Run，
// 其历史由调用方另行提供，重复注入会污染上下文）。
//
// score 阈值不在此层处理：由 providers.VectorMemory 统一过滤，
// 使 Qdrant 实现与 fake 行为完全一致，测试结论才可信。
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
		out = append(out, Hit{
			Score: h.GetScore(),
			Point: Point{
				ID:        pointIDFromPB(h.GetId()),
				TenantID:  h.GetPayload()[PayloadTenantID].GetStringValue(),
				ThreadID:  h.GetPayload()[PayloadThreadID].GetStringValue(),
				RunID:     h.GetPayload()[PayloadRunID].GetStringValue(),
				NodeID:    h.GetPayload()[PayloadNodeID].GetStringValue(),
				Role:      h.GetPayload()[PayloadRole].GetStringValue(),
				Text:      h.GetPayload()[PayloadText].GetStringValue(),
				CreatedAt: CreatedAtFromPayload(h.GetPayload()[PayloadCreatedAt].GetIntegerValue()),
			},
		})
	}
	return out, nil
}

// pointIDFromPB 从 protobuf point ID 还原数值 ID；非数值型（UUID）返回 0。
// 本实现只用数值型 ID，UUID 分支仅为防御性处理。
func pointIDFromPB(id *qdrant.PointId) uint64 {
	if id == nil {
		return 0
	}
	if n, ok := id.GetPointIdOptions().(*qdrant.PointId_Num); ok {
		return n.Num
	}
	return 0
}

func boolPtr(b bool) *bool       { return &b }
func uint64Ptr(v uint64) *uint64 { return &v }
