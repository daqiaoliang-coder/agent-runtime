# Agent Runtime

一个用 Go 写的 Agent Runtime，主要用于面试练习和系统设计验证。

## 架构

```text
Client
  |
  v
Agent Runtime
  |
  +--> Planner ---------> 动态 Step
  |
  +--> Scheduler -------> Worker Pool
                              |
                              +--> LLM
                              +--> Tool
  |
  +--> State Store
          |
          +--> Run / Step
          +--> Checkpoint
          +--> Tool 幂等
          +--> Event / Trace
```

## v1 已实现

- Run / Step / Checkpoint / ToolCall / Event 模型
- Planner / Scheduler / Executor 三层分离
- 动态 Step 生成
- 状态机
- 乐观锁 / CAS
- At-least-once 执行模型
- Tool 幂等
- 指数退避 + 抖动重试
- 超时
- 取消请求
- 最大步数限制
- Event / 轨迹追踪
- 内存版 StateStore 与 Worker Queue

## 运行

```bash
go run .
```

预期流程：

```text
LLM 规划
  -> 调用 search 工具
  -> LLM 推理
  -> 结束
```

## 设计说明

v1 故意做成单进程的 MVP。各接口已经按生产形态留好扩展点，下一版可以直接替换：

- MemoryStore -> PostgreSQL
- 内存队列 -> Kafka / Redis Streams
- MockLLM -> 真实 LLM 提供方
- MockTool -> HTTP/gRPC/MCP 工具
- 同步等待 Step -> 事件驱动恢复

生产形态的演进路线见 `docs/design.md`。
