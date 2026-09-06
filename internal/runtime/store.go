package runtime

import (
	"agent-runtime/internal/model"
	"context"
)

// Store 抽象 Runtime/Resumer 所需的持久化操作。
// 引入接口而非直接依赖 *store.MySQL，便于在单元测试中注入 fake，
// 也为未来替换存储实现（如 PostgreSQL）留好扩展点。
// *store.MySQL 天然满足该接口。
type Store interface {
	CreateRun(ctx context.Context, r *model.Run) error
	GetRun(ctx context.Context, tenant, id string) (*model.Run, error)
	UpdateRunCAS(ctx context.Context, tenant, id string, version int64, status model.RunStatus, currentNode, output string) (bool, error)
	InsertPlan(ctx context.Context, runID, tenant string, p model.Plan) error
	MarkReady(ctx context.Context, tenant, nodeID string) error
	Children(ctx context.Context, tenant, nodeID string) ([]model.Task, error)
	DependenciesReady(ctx context.Context, tenant, nodeID string) (bool, error)
	RunComplete(ctx context.Context, tenant, runID string) (bool, error)
	RunHasFailure(ctx context.Context, tenant, runID string) (bool, error)
	CompletedNodes(ctx context.Context, tenant, runID string) ([]model.Node, error)
	// 消费端幂等 Inbox：InboxSeen 处理前查表，MarkInbox 处理后写表，去重 RocketMQ 至少一次投递。
	InboxSeen(ctx context.Context, tenant, eventID string) (bool, error)
	MarkInbox(ctx context.Context, tenant, eventID string) error
}

// Queue 抽象任务投递所需的最小操作。*queue.RedisQueue 满足该接口。
type Queue interface {
	Enqueue(ctx context.Context, t model.Task) error
}
