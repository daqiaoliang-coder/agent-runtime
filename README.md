# Agent Runtime v2

一个用 Go 编写的 Agent Runtime，从单进程 v1 演进为分布式、事件驱动的执行引擎。

## 技术栈

- Go
- MySQL 8
- Redis Streams
- RocketMQ 5

## 核心能力

- 动态 DAG 规划
- 并行独立节点
- 事件驱动的 Run 恢复
- MySQL 持久化状态
- 乐观锁 / CAS
- 步骤租约与崩溃恢复
- 至少一次投递
- MySQL -> RocketMQ 的 Outbox 可靠投递模式
- Redis Streams 工作队列
- 工具 / LLM 执行抽象

## 组件

```text
cmd/runtime    创建 Run + DAG
cmd/worker     执行就绪的 DAG 节点
cmd/resume     消费 RocketMQ 完成事件并推进 DAG
cmd/recovery   恢复过期租约 + 修复 READY 投递缺口
cmd/outbox     将 MySQL Outbox 事件发布到 RocketMQ
```

## 启动基础设施

```bash
docker compose -f deploy/docker-compose.yml up -d
```

初始化数据库：

```bash
mysql -h127.0.0.1 -uagent -pagent < migrations/001_init.sql
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
