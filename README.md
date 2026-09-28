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
│   ├── outbox/           # Outbox → RocketMQ
│   └── memory-indexer/   # 已完成节点 → 向量记忆（写入路径）
│
├── internal/
│   ├── contracts/        # Runtime 稳定语义契约
│   ├── providers/        # Model / Tool / MCP / Memory 等能力接口
│   ├── adapters/         # 第三方 SDK → Provider
│   │   ├── llm/
│   │   ├── tool/
│   │   ├── mcp/
│   │   └── vector/       # 向量库（Qdrant）适配器 + 测试用内存实现
│   ├── memory/           # 记忆写入路径：扫描 → embedding → 投影 → 进度标记
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
│   ├── 007_hitl.sql
│   ├── 008_cancel_state.sql
│   ├── 009_replan.sql
│   ├── 010_run_limits.sql
│   ├── 011_planner_decision.sql
│   ├── 012_run_thread.sql
│   └── 013_memory_indexed.sql
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
├── UserID       # 已认证的发起者（来自 agent_run.user_id）
├── AuthMethod   # 身份是被怎么认证的（jwt-hs256 / static-token / none）
├── Scopes       # 权限范围（仅入口进程内有效，不落库）
├── TenantID
├── ThreadID
├── RunID
├── NodeID       # 人工闸门的审批锚点
└── Trace
```

避免不同 SDK 各自定义一套上下文，导致租户、用户、Trace 信息在调用链中丢失。

> **JWT 刻意不在这里。** 原始凭证只在进程内的 `context.Context` 中传递
> （见 `contracts.WithAuthToken` / `AuthTokenFrom`），因为 `ExecutionContext`
> 会进审计记录、随 Run 落库、跨进程传递——凭证一旦进入这些通道就等于泄露，
> 而凭证泄露最常见的途径恰恰是"被写进了某个日志字段"。
> 走 ctx 传递把凭证的可见范围限制在单次请求的进程内调用栈，请求结束即丢弃。
>
> 身份认证的完整链路：入口进程用 `Authenticator` 校验凭证得到 `Identity`
> → 身份写入 `agent_run.user_id` / `auth_method` → worker 在另一进程读库还原
> → 注入 `ExecutionContext.UserID` → 工具调用据此做权限判定。
> 身份必须落库，因为创建 Run 与执行节点隔着 MySQL 与 Redis，不落库就传不过去。

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
| `MemorySearcher`  | 跨 Run 语义召回（可选扩展）     |
| `PromptProvider`  | Prompt Resolve            |
| `SkillProvider`   | Skill Discovery / Session |
| `SandboxProvider` | Sandbox Session / Execute |

### 长期记忆（向量检索）

`MemoryProvider` 的 `Load/Save` 签名稳定但**无法表达相似度召回**（`Load` 不接受查询文本）。
语义检索因此收敛到可选接口 `MemorySearcher`，调用方用类型断言探测
（范式同 store 层的 `s.(CancelStore)`）：

```text
写入路径（cmd/memory-indexer，独立进程）
  agent_node(SUCCESS) → Embedder → Qdrant
        └─ memory_indexed 表记录投影进度（INSERT IGNORE 幂等）

读取路径（worker 的 ContextLoader）
  当前 Run 提问 → Embedder → Qdrant(按 tenant+thread 过滤) → 前置到对话历史
```

设计要点：

- **向量库是派生索引，不是数据源**：权威内容在 `agent_node.output`，
  清空 `qdrant_data` 卷 + `memory_indexed` 表即可全量重建。
- **会话隔离维度是 `agent_run.thread_id`**：ThreadID 为空表示不隔离（存量数据兼容），
  非空则严格匹配；召回时按 `run_id` 在源头排除当前 Run，避免上下文重复注入。
- **记忆故障绝不阻断主链路**：embedding 网关或向量库不可用时，
  读取路径降级为"仅用当前 Run 历史"，写入路径只记日志并退避重试。
- **重放幂等**：point ID 由 `SHA256(tenant|thread|node|role)` 确定性派生，
  崩溃重启、节点重试、全量重建都不会产生重复记忆。

相关环境变量（未设置时记忆能力整体关闭，现有部署零感知）：

| 变量                   | 默认值                  | 说明                                    |
| :------------------- | :------------------- | :------------------------------------ |
| `MEMORY_ENABLED`     | `false`              | 显式开启后才装配长期记忆                          |
| `QDRANT_HOST`        | `localhost`          | 向量库地址                                 |
| `QDRANT_PORT`        | `6334`               | gRPC 端口（**不是** REST 的 6333）           |
| `QDRANT_COLLECTION`  | `agent_memory`       | 集合名                                   |
| `QDRANT_API_KEY`     | 空（不鉴权）              | 向量库 API key，服务端开启鉴权时必填                |
| `EMBEDDING_MODEL`    | `text-embedding-3-small` | 向量化模型，复用 `OPENAI_BASE_URL`/`API_KEY` |
| `EMBEDDING_DIM`      | `1536`               | 向量维度，须与集合一致                           |
| `MEMORY_TOP_K`       | `10`                 | 单次召回条数上限                              |
| `MEMORY_MIN_SCORE`   | `0.7`                | 相似度阈值，0 表示不过滤                         |
| `MEMORY_SEARCH_TIMEOUT` | `800ms`           | 读取路径单次召回预算                            |
| `MEMORY_MAX_MESSAGES`   | `20`              | 拼接后交给 LLM 的消息条数上限                     |

索引器额外支持 `MEMORY_SCAN_LIMIT`（默认 `100`，每轮扫描节点数）/ `MEMORY_BATCH_SIZE`
（默认 `16`，每批节点数，每节点最多产出 2 条文本）/ `MEMORY_BATCH_DELAY`（默认 `500ms`，
批间限速，保护与主链路共用的 embedding 网关配额）/ `MEMORY_POLL_INTERVAL`（默认 `5s`）/
`MEMORY_MAX_TEXT_LEN`（默认 `4000`，单条记忆按**字符**截断，避免超长 output 浪费 token）
（见 `cmd/memory-indexer`）。

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

## Policy Gateway

Tool execution is guarded by an extensible Policy Gateway:

```text
Tool Call
   |
   v
Policy Chain
   |
   +--> ALLOW ------------> Claim -> Execute
   |
   +--> REQUIRE_APPROVAL -> WAITING_HUMAN
   |                         |
   |                         v
   |                      Approve
   |                         |
   |                         v
   |                      Resume -> READY -> Execute
   |
   +--> DENY -------------> Fail
```

The policy layer is deliberately separated from executors and persistence. Policies can be composed with `DENY > REQUIRE_APPROVAL > ALLOW` precedence.

See [docs/policy-gateway.md](docs/policy-gateway.md) for the decision model, extension points and limitations.

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
mysql -h127.0.0.1 -uagent -pagent < migrations/008_cancel_state.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/009_replan.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/010_run_limits.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/011_planner_decision.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/012_run_thread.sql
mysql -h127.0.0.1 -uagent -pagent < migrations/013_memory_indexed.sql
```

> 012 为 `agent_run` 增加 `thread_id`（长期记忆的会话隔离维度），
> 013 建立 `memory_indexed`（向量投影进度表）。二者缺失时记忆功能会静默失效。

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

启用长期记忆时，额外启动记忆索引器（写入路径），并为 worker 设置 `MEMORY_ENABLED=true`：

```bash
export OPENAI_BASE_URL=https://api.openai.com/v1   # embedding 复用同一网关
export OPENAI_API_KEY=sk-...
export MEMORY_ENABLED=true                          # worker 侧读取路径开关
go run ./cmd/memory-indexer                         # 向量写入路径（独立进程）
go run ./cmd/worker
```

未设置 `MEMORY_ENABLED` 时，worker 与索引器都不会触达向量库，行为与接入记忆前一致。

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

### 单元测试（默认，不依赖外部服务）

```bash
go test ./...
```

### 集成测试（需要真实 MySQL / Redis）

```bash
docker compose -f deploy/docker-compose.yml up -d
go test -tags=integration ./internal/store/ ./internal/queue/ -v
```

集成测试使用 `//go:build integration` 标签隔离，不影响默认 `go test ./...`。覆盖关键可靠性不变量：

| 测试 | 验证的不变量 |
| :--- | :--- |
| CreateRun + 租户隔离 | 列/参数匹配，跨租户不可读 |
| UpdateRunCAS | 乐观锁：过期版本 CAS 失败 |
| InsertPlan + DAG 查询 | 节点写入、依赖就绪检查、Children 查询 |
| ClaimNode + Lease + RecoverExpired | 租约过期 → 恢复扫描 → ReadyTasks 闭环 |
| CompleteNodeWithOutbox | 事务 Outbox 原子性（节点 + 事件同事务） |
| CompleteNodeWithOutbox CAS 失败 | 过期版本提交时 Outbox 回滚 |
| ToolCall 幂等 | 相同幂等键第二次认领返回 false，结果复用 |
| ToolCall UNKNOWN | 模糊失败标记 UNKNOWN，不盲目重试 |
| Inbox 去重 | 消费端幂等 |
| Decision 持久化与复用 | ReplanRequested 重投不重调 LLM |
| Run 收敛 | 所有节点终态时正确收敛 |
| CancelRun 事务原子性 | Run + 节点同事务取消 |
| Redis Enqueue/Consume | 至少一次投递 + XAck |
| Redis PEL Reclaim | 失败不 Ack → XAutoClaim 回收 → 重新入队 |

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
