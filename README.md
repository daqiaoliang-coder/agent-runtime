# Agent Runtime v2

一个用 Go 编写的 Agent Runtime，从单进程 v1 演进为分布式、事件驱动的执行引擎。

## 技术栈

- Go
- MySQL 8
- Redis Streams
- RocketMQ 5
- OpenTelemetry（轨迹追踪）

## 核心能力

- 动态 DAG 规划（静态 DemoPlanner / 基于 LLM 的 LLMPlanner）
- 并行独立节点
- 事件驱动的 Run 恢复
- MySQL 持久化状态
- 乐观锁 / CAS
- 步骤租约与崩溃恢复
- 至少一次投递
- MySQL -> RocketMQ 的 Outbox 可靠投递模式
- Redis Streams 工作队列
- 工具 / LLM 执行抽象
- 工具调用幂等（`tool_call` 表 + 幂等键）
- 节点执行重试策略（指数退避 + 抖动 + 死信队列）
- RocketMQ 消费端幂等（Inbox 模式）
- Checkpoint / Context 恢复（对话历史 + 节点输出）
- LLM token / cost 追踪（用量落库 + 成本估算）
- OpenTelemetry 全链路轨迹追踪

## 组件

```text
cmd/runtime    创建 Run + DAG（可选 LLM 动态规划）
cmd/worker     执行就绪的 DAG 节点（LLM 推理 / 工具调用）
cmd/resume     消费 RocketMQ 完成事件并推进 DAG
cmd/recovery   恢复过期租约 + 修复 READY 投递缺口
cmd/outbox     将 MySQL Outbox 事件发布到 RocketMQ
```

内部包：

```text
internal/llm       LLM 客户端抽象（Stub / OpenAI 兼容 HTTP，含 token usage 解析）
internal/tool      工具抽象 + Registry + 演示工具（Search / Calculator）
internal/executor  节点执行器：按 node.Type 分发 LLM/Tool/SubAgent
internal/runtime   Run 生命周期、DAG 规划、Resume
internal/store      MySQL 持久化（CAS / 租约 / Outbox / tool_call 幂等 / LLM usage / Inbox）
internal/queue      Redis Streams 工作队列
internal/event      RocketMQ 事件层
internal/worker     任务执行单元
internal/retry      重试策略（指数退避 + 抖动 + 最大次数）
internal/trace      OpenTelemetry 轨迹追踪（span 初始化 + 辅助函数）
```

## 启动基础设施

```bash
docker compose -f deploy/docker-compose.yml up -d
```

初始化数据库：

```bash
mysql -h127.0.0.1 -uagent -pagent < migrations/001_init.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/002_node_tenant.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/003_tool_call_tenant.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/004_retry_dlq.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/005_inbox.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/006_llm_usage.sql
```

安装依赖：

```bash
go mod tidy
```

在各自终端启动服务：

```bash
go run ./cmd/resume
go run ./cmd/worker
go run ./cmd/recovery
go run ./cmd/runtime
```

演示规划器生成的 DAG 如下：

```text
Search A ──┐
           ├──> Reason ──> Report
Search B ──┘
```

## 执行抽象与动态规划

Agent Runtime 的"执行"语义由三层抽象承载：

| 包 | 职责 |
| --- | --- |
| `internal/llm` | LLM 客户端接口 `Client.Complete(ctx, Request) (Response, error)`，屏蔽 provider 差异。内置 `Stub`（确定性，可定制响应）与 `OpenAIClient`（真实 HTTP，兼容 OpenAI / Azure / vLLM / ollama 的 `/chat/completions`）。 |
| `internal/tool` | 工具接口 `Tool`（`Name` + `Execute`）与 `Registry`。内置 `Search`、`Calculator` 演示工具。 |
| `internal/executor` | `Dispatcher` 按 `node.Type` 路由：`LLM` 节点调 LLM、`TOOL` 节点查 Registry 执行、`SUB_AGENT` 委托子执行器，未知类型显式报错。 |

Worker 认领节点后调用 `Executor.Execute`，真正发起 LLM 推理或工具调用：

- 成功 → `CompleteNodeWithOutbox` 写 `AgentStepCompleted`；
- 失败 → `FailNodeWithOutbox` 写 `AgentStepFailed`，Resume 据此将 Run 标记 FAILED。

### 动态规划

- `DemoPlanner`：静态固定 DAG（默认，无需 LLM）；
- `LLMPlanner`：提示 LLM 返回 JSON 计划并解析为 DAG（ReAct / plan-and-execute），支持 Markdown 代码围栏剥离、缺省 ID 兜底、非法 JSON 报错。配置 `OPENAI_API_KEY` 时 `cmd/runtime` 自动切换。

### 工具调用幂等

工具调用经 `tool_call` 表落库，幂等键 = `sha256(run_id|node_id|tool_name|input)`（跨重试稳定，同时作 `call_id` 与 `idempotency_key`）。状态机 `RUNNING → SUCCESS/FAILED`：

- **新建调用**（`ClaimToolCall` INSERT IGNORE 抢占 RUNNING）→ 执行工具 → 成功落 `SUCCESS`、失败落 `FAILED`；
- **命中 SUCCESS**：复用已持久化输出，**不重复执行副作用**；
- **命中 FAILED**：回收为 `RUNNING` 重试一次（失败通常发生在副作用之前）；
- **命中 RUNNING（崩溃在途）**：副作用状态未知，**拒绝盲目重执行**（非幂等工具安全优先）。

> 诚实说明：这是"SUCCESS 缓存 + 崩溃在途拒绝"的幂等策略，不是严格的 exactly-once。工具副作用发生到 `SUCCESS` 落库之间仍存在窗口；该窗口内的崩溃会留下停滞 `RUNNING` 记录，需人工或租约机制清理后才能重试。对真正非幂等的副作用（发邮件 / 扣款），这正是期望的保守行为——宁可卡住也不重复。

### 重试策略与死信队列

节点执行失败时，Worker 按指数退避 + 抖动策略重试（`internal/retry`）：

- **退避公式**：`Initial * Factor^(attempt-1)`，封顶 `Max`，叠加 ±Jitter/2 抖动，避免惊群；
- **重试流程**：失败可重试 → `RetryNode` 将节点置回 `READY` 并写入 `ready_at`（退避到期时间），`attempt+1`；
- **投递闸门**：`ReadyTasks` 扫描仅选取 `ready_at` 已到期的 `READY` 节点，未到期的重试节点不会被提前投递；
- **死信队列**：重试耗尽（达到 `MaxAttempts`）后写入 `agent_dlq` 表，供人工排查或补偿，同时发 `AgentStepFailed` 事件由 Resume 收敛 Run 为 `FAILED`。

### RocketMQ 消费端幂等（Inbox）

Resume Controller 消费 RocketMQ 事件时采用 Inbox 模式去重至少一次投递的重复消息：

- **处理前查表**：`InboxSeen(tenant, event_id)` 检查事件是否已处理过；
- **处理后写表**：事件成功推进后 `MarkInbox` 写入 `agent_inbox`（`INSERT IGNORE` 保证幂等）；
- **标记后模式**：先处理事件、成功后再标记 Inbox，崩溃在处理中途不会误标完成，重投递可幂等重放（底层 CAS / 状态机本身幂等）。

### Checkpoint / Context 恢复

Worker 在节点完成后将对话历史与节点输出累积写入 `checkpoint` 表，供崩溃恢复后重建 Agent 上下文：

- **累积写入**：`saveCheckpoint` 追加当前节点的输入/输出到 `RunContext.Messages` 与 `RunContext.NodeOutputs`；
- **上下文重建**：`ContextLoader` 从 checkpoint 读取历史消息，`executeLLM` 将历史消息前置到当前 prompt，保证 Agent 对话连续性；
- **崩溃安全**：检查点为"最佳努力"落盘（忽略错误），节点状态由 `CompleteNodeWithOutbox` 事务保证；检查点仅为上下文缓存，丢失不影响状态正确性。

### LLM token / cost 追踪

LLM 调用的 token 消耗与成本被持久化到 `llm_usage` 表，支撑成本分析与配额管控：

- **用量采集**：`llm.Response` 携带 `Usage`（prompt/completion/total tokens）与 `Model`，OpenAI 客户端解析 API 响应中的 `usage` 字段；
- **成本估算**：`Pricer` 函数根据模型名与 token 数估算成本（`DefaultPricer` 内置 gpt-4o/gpt-4o-mini 等常见模型价格表，未登记模型 cost 记 0，仅追踪 token）；
- **落库**：`executor.executeLLM` 在 LLM 调用成功后，通过 `UsageRecorder` 接口将用量写入 `llm_usage` 表，按 run/tenant/model 维度聚合统计；
- **最佳努力**：落库失败不影响主流程（忽略错误），token 用量同时写入 OTel span 属性，可在追踪系统中观测。

### OpenTelemetry 轨迹追踪

全链路 span 串联 Agent 执行轨迹，便于在追踪系统中按租户与 Run 维度检索失败用例：

| Span | 位置 | 属性 |
| --- | --- | --- |
| `worker.handle` | Worker 任务处理入口 | run.id, node.id, tenant.id, attempt |
| `executor.execute` | 节点分发 | node.type, node.id, run.id, tenant.id |
| `executor.llm` | LLM 节点执行 | llm.model, prompt/completion/total tokens |
| `executor.tool` | 工具节点执行 | tool.name |
| `llm.complete` | OpenAI HTTP 调用 | llm.base_url, request_model, message_count |
| `resumer.handle` | DAG 推进事件处理 | event.type, event.id, run.id, tenant.id |

- **初始化**：`trace.Init(serviceName)` 创建 TracerProvider，默认 stdout exporter（演示），生产中替换为 OTLP / Jaeger exporter；
- **降级**：`OTEL_DISABLED=1` 跳过 exporter 初始化，返回 no-op tracer，不阻断启动（单元测试默认走此路径）；
- **零开销**：Tracer 未初始化时 `StartSpan` 返回 no-op span，非追踪场景零性能损耗；
- **错误标记**：执行失败的 span 自动 `RecordError` + `SetStatus(Error)`，在追踪系统中以红色高亮失败轨迹。

## 环境变量

```text
DATABASE_DSN=agent:agent@tcp(localhost:3306)/agent_runtime?parseTime=true
REDIS_ADDR=localhost:6379
REDIS_STREAM=agent.tasks
REDIS_GROUP=agent-workers
ROCKETMQ_NAMESRV=localhost:9876
ROCKETMQ_TOPIC=agent.events
ROCKETMQ_CONSUMER_GROUP=agent-resumer
WORKER_ID=worker-1
# 可选：配置后 worker 与 runtime 切换为真实 OpenAI 兼容 LLM，缺省时使用 Stub（本地无副作用）
OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_API_KEY=sk-...
# 设为 1 时禁用 OpenTelemetry exporter，返回 no-op tracer（单元测试 / 无 Collector 场景）
OTEL_DISABLED=1
```

## 可靠性模型

MySQL 是唯一真相源，Redis 负责任务投递，RocketMQ 负责领域事件投递。

节点完成时，会在同一个 MySQL 事务中同时写入节点状态和一条 Outbox 记录。发布器会持续重试 Outbox 直到 RocketMQ 接收成功。因此 Resume Controller 可以安全重试——底层 DAG 状态已持久化，且节点状态流转受 CAS / 租约保护。

### 启动 Outbox 发布器

```bash
go run ./cmd/outbox
```

完整的 v2 流程需要运行以下四个常驻进程：

```text
runtime -> Redis -> worker -> MySQL + Outbox -> RocketMQ -> resume -> Redis
                                               ^
                                               |
                                            outbox

recovery -> MySQL 租约扫描 -> Redis
```

recovery 进程还会扫描 READY 节点。这用于关闭一个投递缺口：进程在提交 `READY` 后、Redis 入队成功前崩溃的场景。

## 测试

```bash
go test ./...
```

覆盖 50+ 个用例：

- `internal/store`：多租户过滤、tool_call 幂等存储、RetryNode/DLQ、Inbox、Checkpoint、LLM usage 落库（sqlmock）
- `internal/runtime`：Resumer 事件推进与错误传播（fake store）、Inbox 幂等、LLMPlanner JSON 解析
- `internal/executor`：节点分发、工具调用幂等、LLM token/cost 记录
- `internal/retry`：退避策略与抖动
- `internal/worker`：成本估算（DefaultPricer）
- `internal/trace`：OTel 初始化与 no-op 降级

## v3 Agent Framework Layer

The runtime keeps its durable execution kernel and adds provider/adapter contracts for
Model, Tool, Memory, Prompt, MCP, Skill and Sandbox. `RuntimeEvent` normalizes run/node/
text/tool signals for streaming consumers, middleware provides lifecycle/tool/event hooks,
and `internal/agent/react` provides a provider-agnostic ReAct loop. HITL interruptions are
persisted as `WAITING_HUMAN` state in `run_interrupt`, so human approval survives process
restarts. See `docs/architecture-v3.md`.
