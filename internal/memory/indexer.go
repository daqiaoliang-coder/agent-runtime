// Package memory 实现记忆的**写入路径**：把已完成节点的内容投影为向量。
//
// 为什么是独立的索引器而非在 worker 主流程内同步写入：
//  1. 向量库只是**派生索引**，权威内容在 agent_node.output，随时可全量重建，
//     因此不需要与节点提交保持事务一致（这正是它可以异步的理由）；
//  2. embedding 调用慢且可能限流，放在主流程会直接拖慢节点执行；
//  3. 与 cmd/outbox 同构：一个独立的轮询投影进程，故障域与主链路隔离。
//
// 幂等保证是双重的：确定性 point ID（vector.PointID）+ memory_indexed 的 INSERT IGNORE。
// 因此崩溃重启、重复扫描、全量重建都不会产生重复记忆。
package memory

import (
	"agent-runtime/internal/adapters/vector"
	"agent-runtime/internal/contracts"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"context"
	"errors"
	"fmt"
	"log"
	"time"
)

// Store 是索引器所需的持久化能力（*store.MySQL 天然实现）。
// 抽象为接口便于用 fake 做单测，不依赖真实 MySQL。
type Store interface {
	UnindexedSuccessNodes(ctx context.Context, limit int) ([]model.MemoryNode, error)
	MarkMemoryIndexed(ctx context.Context, tenant, nodeID, threadID, runID string) error
}

// 默认配置。可被 Config 字段覆盖，装配方从 env 读取（见 cmd/memory-indexer）。
const (
	DefaultCollection = "agent_memory"
	// DefaultScanLimit 每轮扫描的候选节点数上限。
	//
	// 刻意大于 DefaultBatchSize：一次扫描的结果会被切成多个 embedding 批次，
	// 批次之间才插得进限速间隔。若两者相等，每轮只有一批，批间限速永远不生效，
	// 索引器会以最快速度连续打满网关。
	DefaultScanLimit = 100
	// DefaultBatchSize 单批处理的**节点**数（不是文本条数）。
	//
	// 注意：一个节点最多产出 2 条文本（Input + Output），因此单次 embedding 调用
	// 实际最多携带 2×BatchSize 条文本。调大该值前需确认网关的请求体与批量上限。
	DefaultBatchSize = 16
	// DefaultMaxTextLen 单条记忆文本的最大字符数，超出部分截断。
	// 节点 output 可能是超长报告，全量向量化既浪费 token 又会被 embedding 模型截断，
	// 不如在源头截断以获得确定性行为。
	DefaultMaxTextLen = 4000
	// DefaultBatchDelay 批次之间的间隔。
	//
	// ⚠️ 这是**限流保护**而非性能调优：索引器与主链路推理共用同一个 embedding 网关与 API key，
	// 索引器若持续高频调用会挤占配额，导致正常推理被限流。
	// 宁可索引滞后，不可拖累在线推理。
	DefaultBatchDelay = 500 * time.Millisecond
	// DefaultPollInterval 无待处理节点时的轮询间隔。
	DefaultPollInterval = 5 * time.Second
	// maxBackoff 单节点重试退避的上限。
	maxBackoff = 5 * time.Minute
)

// Config 是索引器配置。零值字段会回退到上面的默认值。
type Config struct {
	Collection string
	Dim        int
	// ScanLimit 每轮从数据库扫描的候选节点数上限。
	ScanLimit int
	// BatchSize 单批处理的节点数（每节点最多产出 2 条文本）。
	BatchSize int
	// MaxTextLen 单条记忆文本的最大字符数。
	MaxTextLen int
	// BatchDelay embedding 批次之间的限速间隔。
	BatchDelay time.Duration
	// PollInterval 轮询间隔。
	PollInterval time.Duration
}

// Indexer 把已完成节点投影为向量记忆。
//
// Embedder 与 Store 是必需依赖；两者任一为 nil 时 Run 会返回配置错误而非静默跳过，
// 因为索引器是独立进程，装配遗漏必须在启动时立刻暴露。
type Indexer struct {
	Embedder llm.Embedder
	Store    Store
	Vectors  vector.VectorStore
	Config   Config

	// failures 记录各节点的连续失败次数与下次可重试时间，用于指数退避。
	//
	// 为什么需要它：UnindexedSuccessNodes 按 finished_at 正序返回，
	// 若某个节点永久失败（如文本触发网关内容审核）而不做退避，
	// 它会一直占据批次首位，导致后续节点**永远得不到处理**（队头阻塞）。
	failures map[string]failure
}

type failure struct {
	count   int
	nextTry time.Time
}

// New 构造索引器并填充默认配置。
func New(embedder llm.Embedder, st Store, vs vector.VectorStore, cfg Config) *Indexer {
	if cfg.Collection == "" {
		cfg.Collection = DefaultCollection
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.ScanLimit <= 0 {
		cfg.ScanLimit = DefaultScanLimit
	}
	if cfg.MaxTextLen <= 0 {
		cfg.MaxTextLen = DefaultMaxTextLen
	}
	// BatchDelay 零值也回退到默认值：它是与主链路共用 embedding 网关的**限流保护**，
	// 装配遗漏时宁可慢也不能高频挤占配额、拖累在线推理。
	// 需要更快时显式传一个小的正值（如 time.Millisecond）。
	if cfg.BatchDelay <= 0 {
		cfg.BatchDelay = DefaultBatchDelay
	}
	if cfg.PollInterval <= 0 {
		cfg.PollInterval = DefaultPollInterval
	}
	return &Indexer{Embedder: embedder, Store: st, Vectors: vs, Config: cfg, failures: make(map[string]failure)}
}

// EnsureCollection 在开始投影前确保向量集合存在（Cosine + 指定维度 + payload 索引）。
// 幂等，可每次启动调用。
func (ix *Indexer) EnsureCollection(ctx context.Context) error {
	if ix.Vectors == nil {
		return fmt.Errorf("memory: vector store not configured")
	}
	if ix.Config.Dim <= 0 {
		return fmt.Errorf("memory: embedding dimension must be set (EMBEDDING_DIM)")
	}
	return ix.Vectors.EnsureCollection(ctx, ix.Config.Collection, ix.Config.Dim)
}

// RunOnce 执行一轮投影，返回本轮成功投影的节点数。
//
// 流程：扫描未索引节点 → 过滤退避中的节点 → 分批 embed → Upsert → MarkMemoryIndexed。
//
// 错误语义是刻意的分层：
//   - 扫描失败 → 立即返回 error（无法继续，本轮作废）；
//   - 单个批次失败 → **不中断整轮**，继续处理后续批次，最后用 errors.Join 汇总上报。
//
// 之所以继续处理后续批次：失败通常是节点内容层面的（如文本触发网关审核），
// 中断整轮会让一个坏节点阻塞所有好节点。之所以仍要汇总上报：
// 调用方（Run 循环）需要知道本轮出过错，否则故障只会沉在日志里、指标上看不到。
func (ix *Indexer) RunOnce(ctx context.Context) (int, error) {
	if ix.Embedder == nil || ix.Store == nil || ix.Vectors == nil {
		return 0, fmt.Errorf("memory: indexer not fully configured (embedder/store/vectors required)")
	}
	// 扫描上限与 embedding 批大小是两个独立参数：一次扫描的结果会被切成多批，
	// 批次之间才插得进限速间隔。
	nodes, err := ix.Store.UnindexedSuccessNodes(ctx, ix.Config.ScanLimit)
	if err != nil {
		return 0, fmt.Errorf("memory: scan unindexed nodes: %w", err)
	}
	if len(nodes) == 0 {
		return 0, nil
	}

	// 过滤掉仍在退避窗口内的节点，避免队头阻塞。
	now := time.Now()
	candidates := make([]model.MemoryNode, 0, len(nodes))
	for _, n := range nodes {
		if f, ok := ix.failures[n.NodeID]; ok && now.Before(f.nextTry) {
			continue
		}
		candidates = append(candidates, n)
	}
	if len(candidates) == 0 {
		return 0, nil
	}

	indexed := 0
	var batchErrs []error
	for start := 0; start < len(candidates); start += ix.Config.BatchSize {
		if ctx.Err() != nil {
			return indexed, errors.Join(append(batchErrs, ctx.Err())...)
		}
		end := start + ix.Config.BatchSize
		if end > len(candidates) {
			end = len(candidates)
		}
		n, err := ix.projectBatch(ctx, candidates[start:end])
		indexed += n
		if err != nil {
			// 关键日志：批次失败但继续，失败节点已计入退避，下一轮自动跳过直到退避到期。
			log.Printf("memory: batch projection failed (%d node(s)): %v", end-start, err)
			batchErrs = append(batchErrs, err)
		}
		if ix.Config.BatchDelay > 0 && end < len(candidates) {
			// 批次间限速：保护与主链路共用的 embedding 网关配额。
			select {
			case <-ctx.Done():
				return indexed, errors.Join(append(batchErrs, ctx.Err())...)
			case <-time.After(ix.Config.BatchDelay):
			}
		}
	}
	return indexed, errors.Join(batchErrs...)
}

// Run 持续轮询投影，直到 ctx 取消。结构与 cmd/outbox 的发布循环同构。
func (ix *Indexer) Run(ctx context.Context) error {
	if err := ix.EnsureCollection(ctx); err != nil {
		return err
	}
	ticker := time.NewTicker(ix.Config.PollInterval)
	defer ticker.Stop()
	for {
		n, err := ix.RunOnce(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			// 关键日志：投影失败但进程继续，下一轮重试。
			// 索引器故障绝不能影响主执行链路——这是它独立成进程的意义。
			log.Printf("memory: indexing round failed: %v", err)
		} else if n > 0 {
			log.Printf("memory: indexed %d node(s) into collection %q", n, ix.Config.Collection)
		}
		select {
		case <-ctx.Done():
			log.Println("memory: indexer stopped")
			return nil
		case <-ticker.C:
		}
	}
}

// pendingItem 是一段待向量化的文本及其归属（哪个节点、哪种角色）。
// 提到包级是为了让 hasText 等辅助函数可以引用它，而不必重复匿名结构体字面量。
type pendingItem struct {
	node model.MemoryNode
	role contracts.Role
	text string
}

// projectBatch 投影一批节点，返回成功投影的节点数。
//
// 顺序至关重要：**先 Upsert 成功，再 MarkMemoryIndexed**。
// 若顺序颠倒，崩溃会让进度表领先于实际向量数据，
// 该节点此后永远不会被重新扫描，记忆静默丢失且无法察觉。
// 反之，Upsert 成功但标记失败时，下一轮会重扫并重新 Upsert——
// 因 point ID 确定性而幂等，只是多花一次 embedding 调用。
func (ix *Indexer) projectBatch(ctx context.Context, nodes []model.MemoryNode) (int, error) {
	// 1. 收集待向量化的文本，并记录每段文本对应哪个节点/角色。
	var items []pendingItem
	texts := make([]string, 0, len(nodes)*2)
	for _, n := range nodes {
		// 一个节点最多产出两条记忆：用户提问（Input）与助手回答（Output）。
		//
		// 同时索引提问是有意的：未来的查询通常更像"问题"而非"答案"，
		// 只索引 Output 会显著降低召回率。point ID 含 role 维度，
		// 因此同节点的两条记忆天然不会互相覆盖。
		for _, cand := range []struct {
			role contracts.Role
			text string
		}{
			{contracts.RoleUser, n.Input},
			{contracts.RoleAssistant, n.Output},
		} {
			text := truncate(cand.text, ix.Config.MaxTextLen)
			if text == "" {
				continue
			}
			items = append(items, pendingItem{node: n, role: cand.role, text: text})
			texts = append(texts, text)
		}
	}

	// 2. 无任何可索引文本的节点：直接标记为已处理。
	//    否则空节点会永远留在待扫描队列里，白白占用每轮的扫描配额。
	for _, n := range nodes {
		if !hasText(items, n.NodeID) {
			if err := ix.mark(ctx, n); err != nil {
				log.Printf("memory: mark empty node %s: %v", n.NodeID, err)
			}
		}
	}

	if len(texts) == 0 {
		return 0, nil
	}

	// 3. 批量 embedding。一次调用覆盖整批，摊薄网关往返。
	vecs, err := ix.Embedder.Embed(ctx, texts)
	if err != nil {
		// 整批退避：embedding 失败通常是网关级问题（限流/超时/鉴权），
		// 逐个重试只会加剧限流。
		ix.backoffAll(nodes)
		return 0, fmt.Errorf("embed %d texts: %w", len(texts), err)
	}
	if len(vecs) != len(texts) {
		ix.backoffAll(nodes)
		return 0, fmt.Errorf("embed returned %d vectors for %d texts", len(vecs), len(texts))
	}

	// 4. 组装 points。texts[i] 与 vecs[i] 严格对应，与 items[i] 同源，故按下标对齐。
	points := make([]vector.Point, 0, len(items))
	for i, it := range items {
		if len(vecs[i]) == 0 {
			return 0, fmt.Errorf("embed returned empty vector for node %s role %s", it.node.NodeID, it.role)
		}
		points = append(points, vector.Point{
			ID: vector.PointID(it.node.TenantID, it.node.ThreadID, it.node.NodeID, string(it.role)),
			// 向量本身不存进 payload：Qdrant 已持有，重复存储只会放大内存占用。
			Vector:    vecs[i],
			TenantID:  it.node.TenantID,
			ThreadID:  it.node.ThreadID,
			RunID:     it.node.RunID,
			NodeID:    it.node.NodeID,
			Role:      string(it.role),
			Text:      it.text,
			CreatedAt: it.node.FinishedAt,
		})
	}

	// 5. 先写入向量库。
	if err := ix.Vectors.Upsert(ctx, ix.Config.Collection, points); err != nil {
		ix.backoffAll(nodes)
		return 0, fmt.Errorf("upsert %d points: %w", len(points), err)
	}

	// 6. 向量确认落地后才标记进度。
	indexed := 0
	for _, n := range nodes {
		if !hasText(items, n.NodeID) {
			continue // 上面已标记
		}
		if err := ix.mark(ctx, n); err != nil {
			// 标记失败不回滚向量：下一轮重扫会重新 Upsert（幂等），只是多一次 embedding。
			log.Printf("memory: mark node %s after upsert: %v", n.NodeID, err)
			ix.recordFailure(n.NodeID)
			continue
		}
		delete(ix.failures, n.NodeID)
		indexed++
	}
	return indexed, nil
}

func (ix *Indexer) mark(ctx context.Context, n model.MemoryNode) error {
	return ix.Store.MarkMemoryIndexed(ctx, n.TenantID, n.NodeID, n.ThreadID, n.RunID)
}

// hasText 判断某节点是否有待投影的文本。
func hasText(items []pendingItem, nodeID string) bool {
	for _, it := range items {
		if it.node.NodeID == nodeID {
			return true
		}
	}
	return false
}

// backoffAll 对整批节点施加指数退避。
func (ix *Indexer) backoffAll(nodes []model.MemoryNode) {
	for _, n := range nodes {
		ix.recordFailure(n.NodeID)
	}
}

// recordFailure 递增失败计数并按指数退避安排下次重试时间。
// 退避序列约 5s → 10s → 20s → ... 上限 5min，
// 使持续失败的节点逐渐让出扫描配额，而不会永久阻塞队列。
func (ix *Indexer) recordFailure(nodeID string) {
	if ix.failures == nil {
		ix.failures = make(map[string]failure)
	}
	f := ix.failures[nodeID]
	f.count++
	backoff := ix.Config.PollInterval * time.Duration(1<<min(f.count-1, 6))
	if backoff > maxBackoff {
		backoff = maxBackoff
	}
	f.nextTry = time.Now().Add(backoff)
	ix.failures[nodeID] = f
}

// FailureCount 暴露某节点的连续失败次数，供测试与运维观测使用。
func (ix *Indexer) FailureCount(nodeID string) int {
	return ix.failures[nodeID].count
}

// truncate 按**字符**（rune）截断而非字节，避免在多字节字符中间切断产生非法 UTF-8，
// 进而导致 embedding 网关报错或向量库拒绝写入。
func truncate(s string, maxLen int) string {
	if maxLen <= 0 {
		return s
	}
	r := []rune(s)
	if len(r) <= maxLen {
		return s
	}
	return string(r[:maxLen])
}
