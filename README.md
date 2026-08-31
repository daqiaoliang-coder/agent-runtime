# Agent Runtime v2

A Go Agent Runtime evolved from a single-process v1 into a distributed, event-driven execution engine.

## Stack

- Go
- MySQL 8
- Redis Streams
- RocketMQ 5

## Key capabilities

- Dynamic DAG planning
- Parallel independent nodes
- Event-driven Run resume
- MySQL durable state
- Optimistic locking / CAS
- Step leases and crash recovery
- At-least-once delivery
- Outbox pattern for MySQL -> RocketMQ reliability
- Redis Streams worker queue
- Tool/LLM execution abstraction

## Components

```text
cmd/runtime    create Run + DAG
cmd/worker     execute ready DAG nodes
cmd/resume     consume RocketMQ completion events and advance DAG
cmd/recovery   recover expired leases + repair READY delivery gaps
cmd/outbox     publish MySQL Outbox events to RocketMQ
```

## Start infrastructure

```bash
docker compose -f deploy/docker-compose.yml up -d
```

Apply schema:

```bash
mysql -h127.0.0.1 -uagent -pagent < migrations/001_init.sql
```

Install dependencies:

```bash
go mod tidy
```

Start services in separate terminals:

```bash
go run ./cmd/resume
go run ./cmd/worker
go run ./cmd/recovery
go run ./cmd/runtime
```

The demo planner creates:

```text
Search A ──┐
           ├──> Reason ──> Report
Search B ──┘
```

## Environment

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

## Reliability model

MySQL is the source of truth. Redis is task delivery. RocketMQ is domain-event delivery.

A node completion writes both the node state and an Outbox row in one MySQL transaction. A publisher retries the Outbox until RocketMQ accepts it. The Resume Controller is therefore safe to retry because the underlying DAG state is persisted and node transitions are CAS/lease protected.

### Start the Outbox publisher

```bash
go run ./cmd/outbox
```

Run these four long-lived processes for the full v2 flow:

```text
runtime -> Redis -> worker -> MySQL + Outbox -> RocketMQ -> resume -> Redis
                                               ^
                                               |
                                            outbox

recovery -> MySQL lease scan -> Redis
```

The recovery process also scans READY nodes. This closes the delivery gap where a process can commit `READY` and crash before its Redis enqueue succeeds.
