// 本文件实现基于向量检索的记忆提供方。
//
// 职责边界：VectorMemory 只负责**读**（语义召回）。
// 写入由 cmd/memory-indexer 独立完成——它从 MySQL 的 SUCCESS 节点派生记忆并投影到向量库。
// 这样拆分是因为向量库只是**派生索引**：权威内容在 agent_node.output，
// 索引器可随时全量重建，因此写入路径无需事务、也不需要在执行链路内同步做 embedding。
package providers

import (
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"context"
	"fmt"
	"log"
	"sort"
)

// 默认检索参数。可被 VectorMemory 字段覆盖，装配方从 env 读取（见 cmd/worker）。
const (
	DefaultMemoryTopK     = 10
	DefaultMemoryMinScore = float32(0.7)
)

// VectorMemory 是 MemoryProvider + MemorySearcher 的向量检索实现。
//
// 它组合两个可替换的依赖：llm.Embedder（文本→向量）与 vector.VectorStore（向量库）。
// 两者都是接口，因此单测可注入 llm.StubEmbedder 与 vector.Fake，无需真实网关与 Qdrant。
type VectorMemory struct {
	Embedder   llm.Embedder
	Store      vector.VectorStore
	Collection string
	// TopK 单次召回的最大条数，<=0 时取 DefaultMemoryTopK。
	// 上限同时是上下文膨胀与 token 预算的闸门。
	TopK int
	// MinScore 相似度阈值，低于该分数的结果被丢弃。
	// 设 0 表示不过滤（召回全部 topK，含不相关内容）。
	MinScore float32
}

// 编译期断言：VectorMemory 必须同时满足稳定 port 与可选扩展接口。
var (
	_ MemoryProvider = (*VectorMemory)(nil)
	_ MemorySearcher = (*VectorMemory)(nil)
)

// Search 以 query 为语义查询召回同租户（且同会话）的历史记忆。
//
// 实现顺序是刻意的：
//  1. 参数与依赖校验（缺依赖视为"未启用记忆"，不是错误）；
//  2. embedding 查询文本；
//  3. 带 tenant/thread 过滤检索；
//  4. 按 MinScore 丢弃不相关结果；
//  5. 按 created_at **正序**重排（向量库返回的是 score 降序，
//     但对话历史必须按时间顺序喂给 LLM，否则语义会错乱）；
//  6. 转成 contracts.Message。
//
// **降级契约**：任何内部错误（embedding 网关超时/限流、向量库不可用）都转成
// (nil, nil) + warn 日志，绝不向上抛。记忆是主执行链路的增强项，
// 向量库故障必须表现为"这次没有召回到历史"，而不是"Run 失败"。
func (m *VectorMemory) Search(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) ([]contracts.Message, error) {
	if m == nil || m.Embedder == nil || m.Store == nil {
		return nil, nil
	}
	// 空查询无法向量化：返回空记忆而非报错，等价于"本次不召回"。
	if query == "" || ec.TenantID == "" {
		return nil, nil
	}
	if topK <= 0 {
		topK = m.TopK
	}
	if topK <= 0 {
		topK = DefaultMemoryTopK
	}

	vecs, err := m.Embedder.Embed(ctx, []string{query})
	if err != nil {
		m.degrade("embed query", err, ec)
		return nil, nil
	}
	if len(vecs) != 1 || len(vecs[0]) == 0 {
		m.degrade("embed query returned no vector", fmt.Errorf("got %d vectors", len(vecs)), ec)
		return nil, nil
	}

	collection := m.collection()
	hits, err := m.Store.Search(ctx, collection, vecs[0], vector.Filter{
		TenantID: ec.TenantID,
		ThreadID: ec.ThreadID,
		// 排除当前 Run：它的历史由调用方从已提交节点另行提供（见 worker.ContextLoader）。
		// 当前 Run 的节点一旦完成就会被索引器投影进向量库，若不排除，
		// 同一段对话会在上下文中出现两次。返回的 contracts.Message 不含 node_id，
		// 调用方无法在下游去重，因此必须在此处源头排除。
		ExcludeRunID: ec.RunID,
	}, topK)
	if err != nil {
		m.degrade("vector search", err, ec)
		return nil, nil
	}

	msgs := make([]contracts.Message, 0, len(hits))
	kept := make([]vector.Hit, 0, len(hits))
	for _, h := range hits {
		// 阈值过滤在 provider 层统一做，使 Qdrant 实现与测试用 fake 行为一致，
		// 否则单测结论无法代表生产行为。
		if m.MinScore > 0 && h.Score < m.MinScore {
			continue
		}
		if h.Text == "" {
			continue
		}
		kept = append(kept, h)
	}
	// 按时间正序：对话历史必须还原真实先后顺序。
	// 同一时刻的并列项按 NodeID 排序，保证结果稳定可复现（便于测试与问题复现）。
	sort.SliceStable(kept, func(i, j int) bool {
		if kept[i].CreatedAt.Equal(kept[j].CreatedAt) {
			return kept[i].NodeID < kept[j].NodeID
		}
		return kept[i].CreatedAt.Before(kept[j].CreatedAt)
	})
	for _, h := range kept {
		msgs = append(msgs, contracts.Message{
			Role:    roleOrUser(h.Role),
			Content: h.Text,
		})
	}
	return msgs, nil
}

// Load 返回空记忆。
//
// 这是刻意的：MemoryProvider.Load 的签名不接受查询文本，而语义召回必须有查询，
// 因此向量记忆**无法**通过 Load 表达。保持 Load 的签名稳定是
// docs/architecture-v3.md 对 provider port 的硬要求，二者不可兼得，
// 故本实现把语义检索收敛到 MemorySearcher.Search，Load 退化为"无历史"。
//
// 返回 (nil, nil) 而非 error：调用方据此自然回退到"仅用当前 Run 的对话历史"，
// 这正是记忆不可用时期望的安全行为。
// 需要语义召回的调用方请用类型断言探测 MemorySearcher（见 internal/worker/worker.go）。
func (m *VectorMemory) Load(context.Context, contracts.ExecutionContext) ([]contracts.Message, error) {
	return nil, nil
}

// Save 返回明确错误。
//
// VectorMemory 是只读视图：记忆的写入由 cmd/memory-indexer 从 MySQL 的 SUCCESS 节点
// 投影而来（幂等、可全量重建）。这里**不做静默 no-op**——若返回 nil，
// 调用方会误以为已持久化，从而静默丢数据；显式报错能让接线错误立刻暴露。
//
// 当前代码库无任何调用方；本方法存在只为满足 MemoryProvider 接口。
func (m *VectorMemory) Save(_ context.Context, _ contracts.ExecutionContext, _ []contracts.Message) error {
	return fmt.Errorf("providers: VectorMemory is read-only; memory writes are projected by cmd/memory-indexer from committed nodes")
}

// degrade 统一记录降级：记忆检索失败只打 warn，不中断执行链路。
// 日志带上租户/会话/Run 维度，便于运维定位是哪个租户的向量库或网关出了问题。
func (m *VectorMemory) degrade(stage string, err error, ec contracts.ExecutionContext) {
	log.Printf("memory: degraded (%s) tenant=%s thread=%s run=%s: %v", stage, ec.TenantID, ec.ThreadID, ec.RunID, err)
}

func (m *VectorMemory) collection() string {
	if m == nil || m.Collection == "" {
		return DefaultMemoryCollection
	}
	return m.Collection
}

// DefaultMemoryCollection 是未显式配置时使用的集合名。
const DefaultMemoryCollection = "agent_memory"

// roleOrUser 把 payload 中存储的角色字符串还原为 contracts.Role。
// 空值或未知值一律归为 user：记忆内容作为上下文喂给 LLM 时，
// 标记为 user 比标记为空角色更安全（不会与系统指令混淆，也不会被模型忽略）。
func roleOrUser(s string) contracts.Role {
	switch contracts.Role(s) {
	case contracts.RoleSystem, contracts.RoleUser, contracts.RoleAssistant, contracts.RoleTool:
		return contracts.Role(s)
	default:
		return contracts.RoleUser
	}
}
