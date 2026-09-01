// Package worker 实现任务执行单元。
// Worker 从 Redis 队列消费任务，认领节点、执行并通过 Outbox 事务式记录完成事件。
package worker

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/model"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Worker 是单个任务执行器实例。
// ID 用于租约归属标识；Events 字段预留给直接发布场景（当前完成事件经 Outbox 投递）。
type Worker struct {
	Store  *store.MySQL
	Queue  *queue.RedisQueue
	Events *event.RocketMQ
	ID     string
}

// Handle 处理单个任务，流程：
//  1. 以任务携带的租户身份读取节点并以租约方式 Claim（CAS），竞争失败则直接返回；
//  2. 启动心跳协程定期续租，防止长耗时 LLM/工具调用因租约过期被恢复；
//  3. 执行节点（此处为占位输出），构造带租户身份的完成事件；
//  4. 通过 CompleteNodeWithOutbox 在单事务中更新节点状态并写入 Outbox 事件。
//
// 租户隔离：所有节点操作都带 t.TenantID，跨租户的 node_id 会被 WHERE tenant_id=? 拦截。
func (w *Worker) Handle(ctx context.Context, t model.Task) error {
	n, err := w.Store.GetNode(ctx, t.TenantID, t.NodeID)
	if err != nil {
		return err
	}
	ok, err := w.Store.ClaimNode(ctx, n.TenantID, n.ID, n.Version, w.ID, 30*time.Second)
	if err != nil {
		return err
	}
	if !ok {
		return nil
	}
	n, _ = w.Store.GetNode(ctx, n.TenantID, n.ID)
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	// 执行期间持续续租，避免长任务租约过期被抢占恢复。
	heartbeat := time.NewTicker(10 * time.Second)
	defer heartbeat.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-heartbeat.C:
				_, _ = w.Store.RenewLease(ctx, n.TenantID, n.ID, w.ID, n.Version, 30*time.Second)
			}
		}
	}()

	output := fmt.Sprintf("executed %s/%s input=%s", n.Type, n.Name, n.Input)
	e := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: "AgentStepCompleted", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Output: output, Timestamp: time.Now()}
	payload, _ := json.Marshal(e)
	// 节点完成 + Outbox 事件在同一事务内提交，保证状态与事件一致。
	// 事件携带 TenantID，Resume Controller 据此继续做租户隔离。
	_, err = w.Store.CompleteNodeWithOutbox(ctx, n, output, model.OutboxMessage{ID: e.ID, EventType: e.Type, AggregateID: n.RunID, Payload: string(payload)})
	if err != nil {
		return err
	}
	return nil
}

// NewFromEnv 从环境变量 WORKER_ID 读取标识，缺省时按时间戳生成。
func NewFromEnv(s *store.MySQL, q *queue.RedisQueue, r *event.RocketMQ) *Worker {
	id := os.Getenv("WORKER_ID")
	if id == "" {
		id = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	return &Worker{Store: s, Queue: q, Events: r, ID: id}
}
