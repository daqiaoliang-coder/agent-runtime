# Agent Runtime v2

一个用 Go 编写的 Agent Runtime，从单进程 v1 演进为分布式、事件驱动的执行引擎。

## 技术栈

- Go
- MySQL 8
- Redis Streams
- RocketMQ 5

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
internal/llm       LLM 客户端抽象（Stub / OpenAI 兼容 HTTP）
internal/tool      工具抽象 + Registry + 演示工具（Search / Calculator）
internal/executor  节点执行器：按 node.Type 分发 LLM/Tool/SubAgent
internal/runtime   Run 生命周期、DAG 规划、Resume
internal/store      MySQL 持久化（CAS / 租约 / Outbox / tool_call 幂等）
internal/queue      Redis Streams 工作队列
internal/event      RocketMQ 事件层
internal/worker     任务执行单元
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

覆盖 40 个用例：

- `internal/store`：多租户过滤、tool_call 幂等存储（sqlmock）
- `internal/runtime`：Resumer 事件推进与错误传播（fake store）、LLMPlanner JSON 解析
- `internal/executor`：节点分发、工具调用幂等（SUCCESS 缓存 / FAILED 重试 / RUNNING 拒绝）
