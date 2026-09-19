// Package vector 抽象向量存储能力，把具体向量数据库 SDK 藏在 VectorStore 接口之后。
//
// 设计遵循 internal/adapters/mcp 的薄适配范式：Runtime 只依赖本地接口，
// 因此 Qdrant 可替换为 Milvus / Weaviate 而不影响上层；单测可注入 fake 而无需真跑向量库。
//
// 关键约束：向量库是**派生索引**，不是数据源。权威内容在 agent_node.output，
// 索引器（cmd/memory-indexer）可随时从 MySQL 全量重建，因此写入路径无需事务保证，
// 只需幂等——这由确定性 Point ID 提供（见 PointID）。
package vector

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"time"
)

// Point 是一条待入库的记忆向量及其元数据。
// Payload 字段集是刻意精简的：只保留隔离键（tenant/thread）、溯源键（run/node）
// 与召回后需要还原成对话消息的内容（role/text/created_at）。
type Point struct {
	ID        uint64 // 必须由 PointID 确定性生成，重放时保证幂等
	Vector    []float32
	TenantID  string
	ThreadID  string
	RunID     string
	NodeID    string
	Role      string
	Text      string
	CreatedAt time.Time
}

// Filter 是检索时的强制隔离条件。
//
// TenantID 是**必填**的越权防线：任何检索都必须带租户过滤，
// 与 store 层"所有按 ID 查询强制带 tenant_id"的铁律一致。
//
// ThreadID 为空表示不按会话过滤（存量 Run 的 thread_id 全是空串，
// 若无条件按 thread_id 过滤会导致新会话查不到任何旧数据）。
// 非空时严格匹配，防止跨会话串味。
//
// ExcludeRunID 用于在**源头**排除某个 Run 的记忆，避免召回当前正在执行的 Run 自己的节点。
//
//	为什么必须在源头排除：当前 Run 的节点完成后会被索引器投影进向量库，
//	于是"本 Run 的历史"会同时出现在向量召回结果和 CompletedNodes 结果里，
//	造成同一段对话在上下文中重复。而 MemorySearcher.Search 返回的是
//	contracts.Message（只有 Role/Content，不含 node_id），调用方**无法**在下游去重，
//	因此必须在检索阶段就排除。传空串表示不排除。
type Filter struct {
	TenantID     string
	ThreadID     string
	ExcludeRunID string
}

// Hit 是带相似度分数的检索结果，Score 越大越相似（Cosine 距离下取值 [-1,1]）。
//
// 命名为 Hit 而非 ScoredPoint：Qdrant SDK 已有 protobuf 类型 qdrant.ScoredPoint，
// 同名会在适配层内造成遮蔽与误用，故本地类型刻意避开该名字。
type Hit struct {
	Point
	Score float32
}

// VectorStore 是向量存储的最小能力集。
// 三个方法都显式接收 collection 名，便于同一进程服务多个集合、也便于测试隔离。
type VectorStore interface {
	// EnsureCollection 幂等地确保集合存在（Cosine 距离 + 指定维度），
	// 并为隔离键建 payload 索引。由索引器启动时调用一次。
	EnsureCollection(ctx context.Context, collection string, dim int) error
	// Upsert 批量写入或覆盖点。相同 ID 重复写入不增加点数（幂等）。
	Upsert(ctx context.Context, collection string, points []Point) error
	// Search 返回 topK 条最相似的点，按 Score 降序。
	Search(ctx context.Context, collection string, vec []float32, f Filter, topK int) ([]Hit, error)
}

// PointID 由 (tenant, thread, nodeID, role) 确定性派生 point ID。
//
// ⚠️ 这是重放不产生重复向量的**唯一屏障**：索引器崩溃重启、节点重试、全量重建
// 都依赖"同一记忆条目 → 同一 ID"。规则一旦变更，存量向量会变成无法去重的孤儿数据，
// 只能清空 collection 重建。因此本函数被视为不可变更的契约，并由单测锁定其输出。
//
// 取 SHA-256 前 8 字节转 uint64，符合 Qdrant 对数值型 point ID 的要求。
// 碰撞概率在记忆条目量级（百万级）下可忽略。
func PointID(tenant, thread, nodeID, role string) uint64 {
	h := sha256.Sum256([]byte(tenant + "|" + thread + "|" + nodeID + "|" + role))
	return binary.BigEndian.Uint64(h[:8])
}

// Payload 键名常量：Qdrant 实现与 fake 共用，避免字符串散落导致过滤条件写错。
const (
	PayloadTenantID  = "tenant_id"
	PayloadThreadID  = "thread_id"
	PayloadRunID     = "run_id"
	PayloadNodeID    = "node_id"
	PayloadRole      = "role"
	PayloadText      = "text"
	PayloadCreatedAt = "created_at"
)

// PayloadFields 把 Point 的元数据展开为 payload 键值对，供各实现复用。
// created_at 以 Unix 纳秒存储：Qdrant payload 不支持原生时间类型，
// 用整数既能排序又能在 Go 侧无损还原 time.Time。
func (p Point) PayloadFields() map[string]any {
	return map[string]any{
		PayloadTenantID:  p.TenantID,
		PayloadThreadID:  p.ThreadID,
		PayloadRunID:     p.RunID,
		PayloadNodeID:    p.NodeID,
		PayloadRole:      p.Role,
		PayloadText:      p.Text,
		PayloadCreatedAt: p.CreatedAt.UnixNano(),
	}
}

// CreatedAtFromPayload 从 payload 中的 Unix 纳秒还原时间，越界或类型不符时返回零值。
func CreatedAtFromPayload(v any) time.Time {
	switch n := v.(type) {
	case int64:
		return time.Unix(0, n)
	case float64:
		return time.Unix(0, int64(n))
	case uint64:
		return time.Unix(0, int64(n))
	default:
		return time.Time{}
	}
}
