// Package worker 实现任务执行单元。
// Worker 从 Redis 队列消费任务，认领节点、通过 Executor 执行，并经 Outbox 事务式记录完成/失败事件。
package worker

import (
	"agent-runtime/internal/event"
	"agent-runtime/internal/executor"
	"agent-runtime/internal/llm"
	"agent-runtime/internal/model"
	"agent-runtime/internal/queue"
	"agent-runtime/internal/retry"
	"agent-runtime/internal/store"
	"agent-runtime/internal/tool"
	"agent-runtime/internal/trace"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

// Worker 是单个任务执行器实例。
// ID 用于租约归属标识；Exec 执行节点（默认为带 Stub LLM + 演示工具的 Dispatcher）。
// Events 字段预留给直接发布场景（当前完成/失败事件经 Outbox 投递）。
type Worker struct {
	Store  *store.MySQL
	Queue  *queue.RedisQueue
	Events *event.RocketMQ
	Exec   executor.Executor
	Retry  retry.Policy
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
	// 轨迹根 span：串联从认领节点到执行完成的全流程，携带 run/node/tenant 维度。
	ctx, span := trace.StartSpan(ctx, "worker.handle")
	defer span.End()
	span.SetAttributes(
		attribute.String("run.id", t.RunID),
		attribute.String("node.id", t.NodeID),
		attribute.String("tenant.id", t.TenantID),
		attribute.Int("attempt", t.Attempt),
	)
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
		// 在 span 上记录执行错误，便于在追踪系统中按错误维度检索失败轨迹。
		span.RecordError(execErr)
		span.SetStatus(codes.Error, execErr.Error())
		next := n.Attempt + 1
		if w.Retry.ShouldRetry(next) {
			// 仍可重试：指数退避，置回 READY 并安排 ready_at，ack 任务。
			// recovery 的 ReadyTasks 扫描会在 ready_at 到期后补投递，实现真正的退避重试。
			readyAt := time.Now().Add(w.Retry.Backoff(next))
			ok, rerr := w.Store.RetryNode(ctx, n.TenantID, n.ID, n.Version, readyAt)
			if rerr != nil {
				return fmt.Errorf("retry node: %w (exec err: %v)", rerr, execErr)
			}
			if !ok {
				// 版本/状态已变（可能被恢复抢占），不 ack，任务重投递由恢复机制收敛。
				return fmt.Errorf("retry cas conflict for %s (exec err: %v)", n.ID, execErr)
			}
			return nil
		}
		// 重试耗尽：入死信队列 + 失败事件，由 Resume 收敛 Run 为 FAILED。
		_ = w.Store.EnqueueDLQ(ctx, n.TenantID, n.RunID, n.ID, execErr.Error(), next, output)
		fe := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: "AgentStepFailed", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Error: execErr.Error(), Timestamp: time.Now()}
		payload, _ := json.Marshal(fe)
		if _, ferr := w.Store.FailNodeWithOutbox(ctx, n, model.OutboxMessage{ID: fe.ID, EventType: fe.Type, AggregateID: n.RunID, Payload: string(payload)}); ferr != nil {
			return fmt.Errorf("persist failure: %w (exec err: %v)", ferr, execErr)
		}
		return nil
	}

	e := model.Event{ID: fmt.Sprintf("event-%s-%d", n.ID, time.Now().UnixNano()), Type: "AgentStepCompleted", RunID: n.RunID, NodeID: n.ID, TenantID: n.TenantID, Attempt: n.Attempt, Output: output, Timestamp: time.Now()}
	payload, _ := json.Marshal(e)
	// 累积检查点上下文（对话历史 + 节点输出），在节点完成前落盘，供崩溃恢复重建 Agent 上下文。
	w.saveCheckpoint(ctx, n, output)
	// 节点完成 + Outbox 事件在同一事务内提交，保证状态与事件一致。
	_, err = w.Store.CompleteNodeWithOutbox(ctx, n, output, model.OutboxMessage{ID: e.ID, EventType: e.Type, AggregateID: n.RunID, Payload: string(payload)})
	if err != nil {
		return err
	}
	return nil
}

// saveCheckpoint 累积更新 Run 检查点：追加本节点的输入/输出到对话历史与 NodeOutputs。
// 最佳努力落盘（忽略错误）：节点状态由 CompleteNodeWithOutbox 事务保证，检查点为上下文缓存。
func (w *Worker) saveCheckpoint(ctx context.Context, n *model.Node, output string) {
	_, _, stateJSON, err := w.Store.LoadCheckpoint(ctx, n.RunID)
	var rc model.RunContext
	if err == nil && len(stateJSON) > 0 {
		_ = json.Unmarshal(stateJSON, &rc)
	}
	if rc.NodeOutputs == nil {
		rc.NodeOutputs = map[string]string{}
	}
	rc.NodeOutputs[n.Name] = output
	rc.Messages = append(rc.Messages,
		model.ChatTurn{Role: string(llm.RoleUser), Content: n.Input},
		model.ChatTurn{Role: string(llm.RoleAssistant), Content: output})
	_ = w.Store.SaveCheckpoint(ctx, n.RunID, n.Version, n.ID, rc)
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
	// Retry=指数退避策略，失败可重试节点置回 READY 并按 ready_at 补投递，耗尽入 DLQ。
	// UsageRecorder=s 使 LLM 节点的 token 用量与成本落 llm_usage 表，支撑成本分析。
	disp := &executor.Dispatcher{LLM: client, Tools: tools, ToolStore: s, UsageRecorder: s, Pricer: DefaultPricer}
	// ContextLoader 从 checkpoint 重建对话历史，使崩溃恢复后的 LLM 节点保持上下文连续。
	disp.ContextLoader = func(ctx context.Context, runID string) ([]llm.Message, error) {
		_, _, stateJSON, err := s.LoadCheckpoint(ctx, runID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return nil, nil // 无历史检查点（根节点首次执行），非错误
			}
			return nil, err
		}
		var rc model.RunContext
		if err := json.Unmarshal(stateJSON, &rc); err != nil {
			return nil, err
		}
		msgs := make([]llm.Message, 0, len(rc.Messages))
		for _, m := range rc.Messages {
			msgs = append(msgs, llm.Message{Role: llm.Role(m.Role), Content: m.Content})
		}
		return msgs, nil
	}
	return &Worker{Store: s, Queue: q, Events: r, ID: id, Retry: retry.Default(), Exec: disp}
}

// modelPrices 按模型名记录每百万 token 的单价（美元），prompt 在前、completion 在后。
// 仅含常见模型作为演示；生产中应从配置中心动态加载。未命中的模型 cost 记 0。
var modelPrices = map[string][2]float64{
	"gpt-4o":        {2.5, 10},
	"gpt-4o-mini":   {0.15, 0.6},
	"gpt-4-turbo":   {10, 30},
	"gpt-3.5-turbo": {0.5, 1.5},
}

// DefaultPricer 根据 model 与 token 数估算单次调用成本（美元）。
// 价格按每百万 token 计；未在 modelPrices 中登记的模型返回 0（仅追踪 token）。
func DefaultPricer(model string, prompt, completion int) float64 {
	prices, ok := modelPrices[model]
	if !ok {
		return 0
	}
	return float64(prompt)/1e6*prices[0] + float64(completion)/1e6*prices[1]
}
