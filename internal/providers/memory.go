package providers

import (
	"agent-runtime/internal/contracts"
	"context"
)

// MemoryProvider 是记忆的稳定扩展点（port）。
//
// Load/Save 的签名刻意保持不变：docs/architecture-v3.md 要求 provider 是稳定契约，
// 上层只依赖接口，具体实现（内存 / MySQL / 向量库 / 企业内部记忆服务）可自由替换。
//
// ⚠️ Load 不接受查询语句，因此**无法表达相似度召回**。需要语义检索的实现
// 应额外满足 MemorySearcher；调用方用类型断言探测，探测不到则退回 Load。
type MemoryProvider interface {
	Load(context.Context, contracts.ExecutionContext) ([]contracts.Message, error)
	Save(context.Context, contracts.ExecutionContext, []contracts.Message) error
}

// MemorySearcher 是**可选**的语义检索扩展，与 MemoryProvider 分离以保持后者稳定。
//
// 采用可选接口而非扩展现有接口，是为了零破坏：已存在的实现无需改动即可继续编译，
// 这与 store 层 CancelStore / HITLStore 的 `s.(CancelStore)` 断言范式一致
// （见 internal/runtime/runtime.go）。
//
// 契约要点：
//   - ec.TenantID 是必填的越权防线，实现必须按其过滤，不得返回其他租户的数据；
//   - ec.ThreadID 为空表示不按会话过滤（存量 Run 的 thread_id 均为空串），非空则严格匹配；
//   - query 为空或无任何依赖时，实现应返回空结果而非 error；
//   - **失败必须静默降级**：记忆是主执行链路的增强项而非依赖项，
//     实现应把内部错误转成 (nil, nil) + 日志，不得让 Run 因记忆故障而失败。
type MemorySearcher interface {
	// Search 以 query 为语义查询，召回最多 topK 条最相关的历史消息，
	// 按时间正序返回（便于直接作为对话历史前置到 LLM 请求中）。
	Search(ctx context.Context, ec contracts.ExecutionContext, query string, topK int) ([]contracts.Message, error)
}
