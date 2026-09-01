// Package worker 实现任务执行单元。
// Worker 从 Redis 队列消费任务，认领节点、通过 Executor 执行，并经 Outbox 事务式记录完成/失败事件。
package worker

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/executor"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/store"
	"agent-runtime/internal/tool"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Worker 是单个任务执行器实例。
// ID 用于租约归属标识；Exec 执行节点（默认为带 Stub LLM + 演示工具的 Dispatcher）。
// Events 字段预留给直接发布场景（当前完成/失败事件经 Outbox 投递）。
type Worker struct {
	Store  *store.MySQL
	Queue  *queue.RedisQueue
	Events *event.RocketMQ
	Exec   executor.Executor
	ID     string
}

// Handle 处理单个任务，流程：
//  1. 以任务携带的租户身份读取节点并以租约方式 Claim（CAS），竞争失败则直接返回；
//  2. 启动心跳协程定期续租，防止长耗时 LLM/工具调用因租约过期被恢复；
//  3. 通过 Executor 执行节点（LLM 推理或工具调用）；
//  4. 成功：CompleteNodeWithOutbox 写 AgentStepCompleted；失败：FailNodeWithOutbox 写 AgentStepFailed。
//
// 租户隔离：所有节点操作都带 t.TenantID，跨租户的 node_id 会被 WHERE tenant_id=? 拦截。
// 事件携带 TenantID，Resume Controller 据此继续做租户隔离。
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

	// 执行节点：替换原先的占位字符串拼接，真正发起 LLM 推理或工具调用。
	output, execErr := w.Exec.Execute(ctx, n)
	if execErr != nil {
		// 执行失败：经 Outbox 写 AgentStepFailed 事件，触发 Resume 将 Run 标记为 FAILED。
		fe := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: "AgentStepFailed", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Error: execErr.Error(), Timestamp: time.Now()}
		payload, _ := json.Marshal(fe)
		if _, ferr := w.Store.FailNodeWithOutbox(ctx, n, model.OutboxMessage{ID: fe.ID, EventType: fe.Type, AggregateID: n.RunID, Payload: string(payload)}); ferr != nil {
			// DB 写失败：不 ack，任务重投递，节点靠租约过期恢复后重试。
			return fmt.Errorf("persist failure: %w (exec err: %v)", ferr, execErr)
		}
		// 执行失败已持久化为事件，ack 任务，由 Resume 收敛 Run。
		return nil
	}

	e := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: "AgentStepCompleted", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Output: output, Timestamp: time.Now()}
	payload, _ := json.Marshal(e)
	// 节点完成 + Outbox 事件在同一事务内提交，保证状态与事件一致。
	_, err = w.Store.CompleteNodeWithOutbox(ctx, n, output, model.OutboxMessage{ID: e.ID, EventType: e.Type, AggregateID: n.RunID, Payload: string(payload)})
	if err != nil {
		return err
	}
	return nil
}

// NewFromEnv 从环境变量 WORKER_ID 读取标识，缺省时按时间戳生成。
// 默认使用 Echo（Stub）LLM + Search/Calculator 工具，并注入真实 store 启用 tool_call 幂等。
// 配置 OPENAI_BASE_URL + OPENAI_API_KEY 时切换为真实 OpenAI 兼容 HTTP 客户端。
func NewFromEnv(s *store.MySQL, q *queue.RedisQueue, r *event.RocketMQ) *Worker {
	id := os.Getenv("WORKER_ID")
	if id == "" {
		id = fmt.Sprintf("worker-%d", time.Now().UnixNano())
	}
	tools := tool.NewRegistry()
	tools.Register(tool.Search{})
	tools.Register(tool.Calculator{})
	var client llm.Client = llm.Echo()
	if base := os.Getenv("OPENAI_BASE_URL"); base != "" {
		if key := os.Getenv("OPENAI_API_KEY"); key != "" {
			client = llm.NewOpenAIClient(base, key)
		}
	}
	// ToolStore=s 使 TOOL 节点经 tool_call 表保证幂等（SUCCESS 复用、崩溃在途拒绝重执行）。
	return &Worker{Store: s, Queue: q, Events: r, ID: id,
		Exec: &executor.Dispatcher{LLM: client, Tools: tools, ToolStore: s}}
}
