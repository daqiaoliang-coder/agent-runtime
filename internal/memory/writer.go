// 本文件定义记忆写入路径的 Writer port，服务于写路径外移到 rag-api 的迁移
// （Phase 2，与读路径的 MEMORY_BACKEND 开关对应）：
//   - local：进程内 embed + 直连 Qdrant upsert（LocalWriter，原 projectBatch 内联逻辑）；
//   - remote：经 rag-api 文档 API 写入（RemoteWriter），索引器不再需要
//     embedding 凭证与 Qdrant 连接；
//   - shadow：双写对比（ShadowWriter），直连为准、远程失败仅告警——
//     切换 remote 前的等价性验收手段。
//
// 写路径错误语义与读路径刻意相反：读路径任何故障降级为 (nil, nil) 不拖垮主链路；
// 写路径的错误必须显式上抛——索引器据此重试与退避，静默吞错会让"未写入"
// 被误当"已写入"，那是记忆静默丢失的唯一通道（rag-api 文档端点同样承诺 4xx/5xx）。
package memory

import (
	"context"
	"time"
)

// Document 是一段待写入的记忆文本及其归属，由节点投影产出。
// ID 即向量库 point ID（vector.PointID 派生），是重放幂等的唯一屏障：
// 同一记忆条目永远映射到同一 ID，重复写入表现为覆盖而非追加。
type Document struct {
	ID        uint64
	Text      string
	Role      string
	TenantID  string
	ThreadID  string
	RunID     string
	NodeID    string
	CreatedAt time.Time
}

// Writer 是记忆写入的能力集：确保集合就绪 + 批量写入。
// Write 返回 nil 表示整批已确认落库（Qdrant wait=true / rag-api 同步 ingest），
// 调用方（Indexer）此后才标记进度——"先写后标"不变量跨越任何后端实现不变。
type Writer interface {
	// EnsureCollection 幂等确保集合就绪。local 语义是建集合（Cosine+维度+
	// payload 索引）；remote 语义是确认 rag-api 可用（它在启动时已 fail-fast
	// 建集合并校验维度）。dim 在 remote 下仅用于本地校验日志，不参与远端建集合。
	EnsureCollection(ctx context.Context, collection string, dim int) error
	// Write 批量写入文档。空批次是合法输入（直接返回 nil），
	// 与 Indexer "空节点直接标记"的流程兼容。
	Write(ctx context.Context, collection string, docs []Document) error
}
