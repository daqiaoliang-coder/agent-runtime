# Agent Runtime

> 面向 Agent 应用的通用 Runtime：以 **Provider / Adapter** 解耦能力，以 **RuntimeEvent** 统一事件语义，以 **Run / Node / Checkpoint** 承载可恢复执行，并通过 **Middleware / ReAct / HITL / DAG** 构建上层 Agent 能力。

一个用 Go 编写、面向生产级 Agent Infra 演进的执行运行时。

***

## 1. 设计目标

Agent Runtime 不负责重新实现每一个模型、工具或协议，而是负责把这些能力组织成一个**可执行、可恢复、可观测、可扩展**的 Agent Execution Runtime。

核心设计原则：

```text
能力解耦        Provider / Adapter
      ↓
事件归一        RuntimeEvent
      ↓
执行编排        Run / Node / ReAct / DAG
      ↓
可靠执行        CAS / Lease / Retry / Idempotency
      ↓
状态恢复        Checkpoint / Resume / Recovery
      ↓
横切治理        Middleware / Trace / HITL
```

Runtime 重点解决的问题：

- Agent Run 生命周期管理
- 动态任务 / DAG 执行
- LLM / Tool / MCP 等异构能力接入
- 长任务与进程崩溃后的恢复
- Tool Call 幂等与副作用保护
- Retry / DLQ / Inbox / Outbox
- Human-in-the-loop 暂停与恢复
- 统一 Runtime Event 与流式输出
- Middleware 横切能力
- LLM Token / Cost / OpenTelemetry 追踪

***

## 2. 整体架构

```text
                         Client / API / SSE
                                │
                                ▼
                         ┌──────────────┐
                         │    Runner    │
                         └──────┬───────┘
                                │
              ┌─────────────────┼─────────────────┐
              │                 │                 │
              ▼                 ▼                 ▼
        ┌──────────┐      ┌──────────┐      ┌──────────┐
        │  ReAct   │      │   DAG    │      │   HITL   │
        │  Agent   │      │ Workflow │      │  Resume  │
        └────┬─────┘      └────┬─────┘      └────┬─────┘
             │                 │                 │
             └─────────────────┼─────────────────┘
                               ▼
                    ┌────────────────────┐
                    │   Runtime Kernel   │
                    │                    │
                    │ Run / Node         │
                    │ Checkpoint         │
                    │ Event / Resume     │
                    │ Cancel / Recovery  │
                    └─────────┬──────────┘
                              │
              ┌───────────────┼────────────────┐
              │               │                │
              ▼               ▼                ▼
        ┌──────────┐    ┌────────────┐   ┌────────────┐
        │Middleware│    │ Providers  │   │ Durability │
        │          │    │            │   │            │
        │Lifecycle │    │ Model      │   │ MySQL      │
        │Tool      │    │ Tool       │   │ Redis      │
        │Event     │    │ MCP        │   │ RocketMQ   │
        │HITL      │    │ Memory     │   │ Outbox     │
        └──────────┘    │ Prompt     │   │ Inbox      │
                        │ Skill      │   │ CAS/Lease  │
                        │ Sandbox    │   └────────────┘
                        └─────┬──────┘
                              │
                              ▼
                    ┌───────────────────┐
                    │     Adapters      │
                    │ Eino / OpenAI /   │
                    │ MCP / SDK / ...   │
                    └───────────────────┘
```

### 三层核心边界

Runtime 将 Agent 系统拆成三个相互独立的关注点：

| 层次          | 解决什么问题        | 代表能力                                                          |
| :---------- | :------------ | :------------------------------------------------------------ |
| Capability  | Agent 能调用什么能力 | Model / Tool / MCP / Memory / Prompt / Skill / Sandbox        |
| Execution   | Agent 如何运行    | Run / Node / ReAct / DAG / Checkpoint / Resume                |
| Reliability | Agent 如何稳定运行  | CAS / Lease / Retry / Idempotency / Outbox / Inbox / Recovery |

这样可以避免把某一个 LLM SDK、Agent Framework 或协议直接耦合进 Runtime Kernel。

***

## 3. 分层与目录结构

```text
agent-runtime/
│
├── cmd/
│   ├── runtime/          # 创建 Run / DAG
│   ├── worker/           # 执行节点
│   ├── resume/           # 消费完成事件并推进 Run
│   ├── recovery/         # 租约恢复 / READY 补投递
│   └── outbox/           # Outbox → RocketMQ
│
├── internal/
│   ├── contracts/        # Runtime 稳定语义契约
│   ├── providers/        # Model / Tool / MCP / Memory 等能力接口
│   ├── adapters/         # 第三方 SDK → Provider
│   │   ├── llm/
│   │   ├── tool/
│   │   └── mcp/
│   ├── agent/
│   │   └── react/        # Provider-agnostic ReAct
│   ├── runtime/          # Run 生命周期 / DAG / Resume / HITL
│   ├── executor/         # Node → LLM / Tool 执行分发
│   ├── worker/           # 分布式节点执行单元
│   ├── middleware/       # Lifecycle / Tool / Event 横切链
│   ├── event/            # RuntimeEvent / Event Sink / RocketMQ
│   ├── hitl/             # Human-in-the-loop API
│   ├── store/            # MySQL 持久化 / CAS / Outbox / Inbox
│   ├── queue/            # Redis Streams
│   ├── retry/            # Retry / Backoff / DLQ
│   └── trace/            # OpenTelemetry
│
├── migrations/
│   ├── 001_init.sql
│   ├── 002_node_tenant.sql
│   ├── 003_tool_call_tenant.sql
│   ├── 004_retry_dlq.sql
│   ├── 005_inbox.sql
│   ├── 006_llm_usage.sql
│   └── 007_hitl.sql
│
└── docs/
    ├── design.md
    ├── architecture-v2.md
    └── architecture-v3.md
```

***

## 4. 核心概念

### 4.1 Run

`Run` 是一次 Agent 执行的顶层生命周期。

```text
CREATE
  │
  ▼
RUNNING
  │
  ├───────────────┐
  │               │
  ▼               ▼
WAITING_HUMAN   FAILED
  │
  ▼
RUNNING
  │
  ▼
COMPLETED
```

一个 Run 携带：

- `RunID`
- `TenantID`
- `ExecutionContext`
- Messages / Node Outputs
- 当前执行状态
- DAG / Node 状态
- Checkpoint
- Trace / Token / Cost 信息

### 4.2 Node

Run 内部的最小持久化执行单元。

```text
Run
 ├── Node A : TOOL
 ├── Node B : TOOL
 ├── Node C : LLM
 └── Node D : SUB_AGENT
```

Node 是 Runtime 进行：

- 调度
- Lease
- CAS
- Retry
- Idempotency
- Checkpoint
- Recovery

的基本粒度。

### 4.3 ExecutionContext

Context 是跨 Provider / Executor / Middleware / Tool 的统一执行上下文：

```text
ExecutionContext
├── UserID
├── TenantID
├── ThreadID
├── RunID
├── JWT
└── Trace
```

避免不同 SDK 各自定义一套上下文，导致租户、用户、Trace 信息在调用链中丢失。

***

## 5. Capability Provider

Runtime 对外暴露稳定的 Provider Port，具体 SDK 通过 Adapter 接入。

```text
                    Runtime
                       │
             ┌─────────┴─────────┐
             │     Providers     │
             └─────────┬─────────┘
                       │
      ┌────────────────┼────────────────┐
      │                │                │
      ▼                ▼                ▼
    Model             Tool             MCP
      │                │                │
      ├──────────────┐ │ ┌──────────────┤
      ▼              ▼ ▼ ▼              ▼
   OpenAI           Eino SDK         MCP SDK
```

当前 Provider：

| Provider          | 职责                        |
| :---------------- | :------------------------ |
| `ModelProvider`   | LLM Generate / Stream     |
| `ToolProvider`    | Tool Discovery / Call     |
| `MCPProvider`     | MCP Tool 接入               |
| `MemoryProvider`  | Memory Load / Save        |
| `PromptProvider`  | Prompt Resolve            |
| `SkillProvider`   | Skill Discovery / Session |
| `SandboxProvider` | Sandbox Session / Execute |

### 为什么要 Provider + Adapter？

Runtime Kernel 只依赖接口：

```text
Runtime → Provider → Adapter → Concrete SDK
```

因此可以替换：

- OpenAI / Azure / vLLM / Ollama
- Eino
- MCP SDK
- 内部 Tool SDK
- 企业内部 Memory / Sandbox

而无需修改 Runtime 核心执行逻辑。

***

## 6. Runtime Contract 与 RuntimeEvent

Runtime 不直接向上层暴露某一个 SDK 的 Event，而是定义自己的事件中间表示：`RuntimeEvent`。

```text
LLM / Tool / Worker / Runtime
            │
            ▼
      RuntimeEvent IR
            │
      ┌─────┴─────┐
      ▼           ▼
     SSE        Protocol
                Adapter
              /         \
            AG-UI       A2A
```

典型事件：

```text
RUN_STARTED
NODE_STARTED
TEXT_DELTA
TOOL_CALL
TOOL_RESULT
NODE_FINISHED
NODE_FAILED
RUN_COMPLETED
RUN_FAILED
```

这样可以实现：

- Runtime 内部事件语义稳定
- 上层协议独立演进
- SSE / AG-UI / A2A 不反向污染 Runtime
- 统一事件追踪与 Middleware

***

## 7. 事件流转

一次节点执行的典型事件链：

```text
                 MySQL
                   │
              Node = READY
                   │
                   ▼
             Redis Streams
                   │
                   ▼
                Worker
                   │
             Node = RUNNING
                   │
        ┌──────────┴──────────┐
        │                     │
        ▼                     ▼
      LLM                   Tool
        │                     │
        └──────────┬──────────┘
                   ▼
             Node Completed
                   │
                   ▼
               Outbox
                   │
                   ▼
               RocketMQ
                   │
                   ▼
                Resume
                   │
                   ▼
             DAG State Update
                   │
          ┌────────┴────────┐
          │                 │
       New READY          DONE
          │
          ▼
        Redis
```

### 可靠性关键点

**MySQL 是状态真相源。**

Redis 负责任务投递，RocketMQ 负责领域事件传播。

节点完成时：

```text
MySQL Transaction
├── UPDATE node
└── INSERT outbox_event
```

因此不存在：

```text
Node 已完成
      ↓
进程 Crash
      ↓
事件永远丢失
```

Outbox Publisher 会持续将事件投递到 RocketMQ。

***

## 8. Middleware 横切机制

Middleware 不进入具体 Agent / Tool / Model 实现，而是围绕 Runtime 执行链提供横切能力。

当前支持的 Hook Chain：

```text
Lifecycle
Tool
Event
Memory
HITL
FrontendTool
```

执行模型：

```text
Request
  │
  ▼
┌──────────────────────┐
│ Lifecycle Middleware │
└──────────┬───────────┘
           ▼
      Agent / Node
           │
     ┌─────┴─────┐
     ▼           ▼
    Tool        Event
     │           │
     └─────┬─────┘
           ▼
        Response
```

Middleware 可以承载：

- 日志
- Metrics
- Trace
- 权限校验
- Tool 审计
- Event 转换
- 限流 / 熔断
- HITL 拦截
- Frontend Tool

### 生命周期语义

Lifecycle Chain：

```text
Start:  M1 → M2 → M3 → Handler
Finish: M3 → M2 → M1
```

Tool / Event Transform Chain 按注册顺序处理，Tool After 阶段反向执行。

***

## 9. ReAct Agent

`internal/agent/react` 提供与具体 LLM SDK 无关的 ReAct Engine。

核心循环：

```text
                 ┌─────────────┐
                 │    Think    │
                 │  LLM Decide │
                 └──────┬──────┘
                        │
              ┌─────────┴─────────┐
              │                   │
           Final Answer         Tool Call
              │                   │
              ▼                   ▼
             DONE              Execute Tool
                                  │
                                  ▼
                              Observation
                                  │
                                  └───────┐
                                          │
                                          ▼
                                        Think
```

ReAct Engine 只依赖：

```go
StepRunner {
    RunLLM(ctx, input)
    RunTool(ctx, request)
}
```

因此 ReAct 与具体 Provider 解耦。

### 当前边界

ReAct 当前已经具备独立的 Provider-agnostic 执行能力，但下一阶段会进一步将：

```text
ReAct Decision
      ↓
Durable Node
      ↓
Worker
      ↓
Checkpoint
      ↓
Resume
      ↓
Next ReAct Decision
```

彻底打通，使长时间 ReAct 可以跨进程恢复。

***

## 10. Durable Execution

Runtime 的核心价值不是简单地调用 LLM，而是让一个 Agent 即使面对：

- Worker Crash
- 网络超时
- MQ 重投
- Redis 重投
- LLM 失败
- Tool 失败
- 进程重启
- 长时间等待人工确认

仍然可以从正确状态继续执行。

核心机制：

```text
              Durable Execution
                     │
      ┌──────────────┼──────────────┐
      ▼              ▼              ▼
     CAS           Lease          Checkpoint
      │              │              │
      ▼              ▼              ▼
  状态一致性      Crash Recovery   Context Resume

      ┌──────────────┼──────────────┐
      ▼              ▼              ▼
    Retry         Idempotency     Outbox/Inbox
      │              │              │
      ▼              ▼              ▼
    DLQ          Tool Safety      Event Delivery
```

***

## 11. Tool Call 幂等

Tool 调用通过 `tool_call` 表进行持久化。

幂等键：

```text
sha256(run_id | node_id | tool_name | input)
```

状态机：

```text
        ┌─────────────┐
        │   RUNNING   │
        └──────┬──────┘
               │
        ┌──────┴──────┐
        ▼             ▼
     SUCCESS        FAILED
```

策略：

- `SUCCESS`：直接复用持久化结果
- `FAILED`：允许重新执行
- `RUNNING`：副作用状态未知时拒绝盲目重执行

> 这是 **SUCCESS Cache + Crash-safe Conservative Retry**，不是严格意义上的 exactly-once。对于扣款、发邮件等非幂等副作用，Runtime 优先避免重复执行。

***

## 12. Retry / DLQ

节点失败后采用指数退避 + Jitter：

```text
backoff = min(Initial × Factor^(attempt-1), Max)
```

执行流程：

```text
Node Failed
    │
    ▼
Retryable ? ── No ──> DLQ
    │
   Yes
    │
    ▼
READY + ready_at
    │
    ▼
Recovery / Scheduler
    │
    ▼
Redis Streams
    │
    ▼
Worker Retry
```

通过 `ready_at` 作为投递闸门，避免重试节点在退避时间到达前被提前消费。

***

## 13. Checkpoint / Context Resume

Checkpoint 保存 Agent 执行过程中的：

- 对话历史
- 节点输入
- 节点输出

```text
Run
 │
 ├── Node A output
 ├── Node B output
 ├── Message history
 └── Current context
          │
          ▼
      Checkpoint
          │
          ▼
      Process Crash
          │
          ▼
      ContextLoader
          │
          ▼
   Rebuild Agent Context
```

Checkpoint 是上下文恢复缓存；Run / Node 状态仍以 MySQL 状态机为准。

因此即使 Checkpoint 丢失，也不会破坏核心状态正确性。

***

## 14. Human-in-the-loop

HITL 将人工审批作为 Runtime 的一种持久化状态，而不是进程内 Channel。

```text
RUNNING
   │
   │ interrupt
   ▼
WAITING_HUMAN
   │
   │ human decision
   ▼
RUNNING
   │
   ▼
continue execution
```

`run_interrupt` 持久化：

- interrupt ID
- run ID
- tenant ID
- node ID
- reason
- decision
- status

`Interrupt` 与 `Resume` 使用事务 + CAS 保证状态切换的一致性。

因此：

```text
Worker Crash
     ↓
Process Restart
     ↓
WAITING_HUMAN remains
     ↓
Human Resume
```

不会因为进程重启导致审批状态丢失。

***

## 15. 动态 DAG

Runtime 支持通过 Planner 生成动态执行计划。

当前提供：

- `DemoPlanner`：固定 DAG
- `LLMPlanner`：LLM 输出 JSON Plan → DAG

示例：

```text
Search A ─────┐
              ├──> Reason ──> Report
Search B ─────┘
```

对应执行语义：

```text
Plan
 │
 ▼
Validate DAG
 │
 ▼
Create Nodes
 │
 ▼
Execute READY nodes
 │
 ▼
Event Resume
 │
 ▼
Unlock dependent nodes
 │
 └───────────────> next nodes
```

DAG Engine 与 Runtime Kernel 的边界：

```text
DAG / Planner
    │
    │ decides WHAT to execute
    ▼
Runtime Kernel
    │
    │ guarantees HOW it executes reliably
    ▼
Worker / Executor
```

***

## 16. LLM Token / Cost Tracking

LLM 调用记录：

```text
run_id
tenant_id
model
prompt_tokens
completion_tokens
total_tokens
estimated_cost
```

成本由 `Pricer` 根据模型和 Token 数进行估算。

```text
LLM Response
     │
     ▼
Usage
     │
 ┌───┴────┐
 ▼        ▼
MySQL    OTel
 │        │
 ▼        ▼
Cost     Trace
```

Usage 落库采用最佳努力策略，不影响 Agent 主执行链。

***

## 17. OpenTelemetry

Runtime 在 Run → Worker → Executor → LLM / Tool → Resume 链路上建立 Trace。

典型 Span：

```text
worker.handle
   │
   ▼
executor.execute
   │
   ├──> executor.llm
   │       └──> llm.complete
   │
   └──> executor.tool
   │
   ▼
resumer.handle
```

核心属性：

- `run.id`
- `node.id`
- `tenant.id`
- `attempt`
- `node.type`
- `llm.model`
- `Token Usage`
- `Tool Name`
- `Event Type`

支持 `OTEL_DISABLED=1` 降级到 no-op tracer，不阻断业务启动。

***

## 18. 技术栈

```text
Go
 │
 ├── Agent Runtime
 ├── Provider / Adapter
 ├── ReAct / DAG
 └── Worker / Resume

MySQL 8
 ├── Run / Node
 ├── Checkpoint
 ├── Tool Call
 ├── LLM Usage
 ├── Outbox / Inbox
 └── HITL Interrupt

Redis Streams
 └── Task Queue

RocketMQ 5
 └── Domain Event Bus

OpenTelemetry
 └── Distributed Trace
```

***

## 19. 快速启动

启动基础设施：

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
mysql -h127.0.0.1 -uagent -pagent < migrations/007_hitl.sql
```

安装依赖：

```bash
go mod tidy
go test ./...
```

启动常驻组件：

```bash
go run ./cmd/outbox
go run ./cmd/worker
go run ./cmd/resume
go run ./cmd/recovery
go run ./cmd/runtime
```

完整数据流：

```text
runtime
   │
   ▼
Redis Streams
   │
   ▼
worker
   │
   ├──────────> MySQL
   │               │
   │               └──> Outbox
   │                       │
   │                       ▼
   │                    RocketMQ
   │                       │
   │                       ▼
   └──────────────────> resume
                           │
                           ▼
                         Redis

recovery ───────────────> MySQL Lease / READY Scan
```

***

## 20. 配置

```text
DATABASE_DSN=agent:agent@tcp(localhost:3306)/agent_runtime?parseTime=true

REDIS_ADDR=localhost:6379
REDIS_STREAM=agent.tasks
REDIS_GROUP=agent-workers

ROCKETMQ_NAMESRV=localhost:9876
ROCKETMQ_TOPIC=agent.events
ROCKETMQ_CONSUMER_GROUP=agent-resumer

WORKER_ID=worker-1

OPENAI_BASE_URL=https://api.openai.com/v1
OPENAI_API_KEY=sk-...

OTEL_DISABLED=1
```

缺省情况下可以使用 Stub LLM 进行本地测试；配置 OpenAI-compatible Endpoint 后可以切换真实模型。

***

## 21. 可靠性模型

Runtime 的核心可靠性模型：

```text
                 MySQL
              Source of Truth
                    │
        ┌───────────┼───────────┐
        ▼           ▼           ▼
       CAS         Lease      Outbox
        │           │           │
        ▼           ▼           ▼
   State Safety  Crash Safe  Event Safe
                    │
        ┌───────────┴───────────┐
        ▼                       ▼
      Redis                   RocketMQ
   Task Delivery            Event Delivery
        │                       │
        └───────────┬───────────┘
                    ▼
                 Worker
                    │
                    ▼
                Checkpoint
```

### 关键保证

| 问题           | 机制                          |
| :----------- | :-------------------------- |
| 并发更新         | MySQL CAS / Optimistic Lock |
| Worker Crash | Node Lease + Recovery       |
| Redis 重投     | Node 状态机 + 幂等               |
| MQ 重投        | Inbox                       |
| 事件丢失         | Outbox                      |
| Tool 重复执行    | Tool Call Idempotency       |
| 临时失败         | Retry + Backoff             |
| 重试耗尽         | DLQ                         |
| 上下文丢失        | Checkpoint                  |
| 人工审批         | Durable HITL                |
| 分布式排障        | OpenTelemetry               |

> Runtime 的目标不是宣称 exactly-once，而是通过持久化状态机、幂等、Outbox/Inbox、Lease 和恢复机制，将分布式执行的不确定性收敛到可控状态。

***

## 22. 测试

```bash
go test ./...
```

主要覆盖：

- Provider / Contract
- Event Emitter
- Middleware Chain
- ReAct Engine
- MCP Adapter
- HITL Interrupt / Resume
- Runtime / Resume
- Store / CAS / Outbox / Inbox
- Tool Call Idempotency
- Retry / DLQ
- Checkpoint
- LLM Usage / Cost
- OpenTelemetry
- Worker Execution

***

## 23. Roadmap

当前 Runtime 已完成从 **Durable Execution Kernel → Agent Framework Layer** 的第一阶段演进。

```text
Phase 1  Durable Runtime
        Run / Node / CAS / Lease / Retry
                    │
                    ▼
Phase 2  Event-driven Runtime
        Redis / RocketMQ / Outbox / Inbox
                    │
                    ▼
Phase 3  Agent Framework Layer   ← 当前
        Provider / Adapter
        RuntimeEvent
        Middleware
        ReAct
        MCP
        HITL
                    │
                    ▼
Phase 4  Durable Agent
        ReAct Decision
             ↓
        Durable Node
             ↓
        Worker
             ↓
        Checkpoint
             ↓
        Resume
                    │
                    ▼
Phase 5  Agent Platform
        DeepAgent / Supervisor
        AG-UI / A2A
        Declarative DAG
        Session / Memory
        Streaming Protocol
```

下一阶段最重要的目标是把 **ReAct 决策循环与 Durable Runtime Kernel 完整融合**，形成：

```text
User Request
     │
     ▼
    Run
     │
     ▼
 ReAct Decision
     │
 ┌───┴──────────────┐
 ▼                  ▼
LLM Final          Tool Call
                     │
                     ▼
               Durable Node
                     │
                     ▼
                   Worker
                     │
                     ▼
                Checkpoint
                     │
                     ▼
                  Resume
                     │
                     ▼
              Next ReAct Loop
```

这也是 Runtime 从“Agent 执行框架”进一步演进为“可恢复 Agent Infra”的关键一步。

***

## 24. 文档

- `docs/design.md`：基础设计
- `docs/architecture-v2.md`：Durable Execution 架构
- `docs/architecture-v3.md`：Provider / Event / Middleware / ReAct / HITL 架构

***

## License

仅用于技术研究、架构验证与 Agent Runtime 实验。
