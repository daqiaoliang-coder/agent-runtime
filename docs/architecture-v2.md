# Agent Runtime v2 Architecture

## Design goal

Turn the v1 single-process Agent loop into an event-driven distributed runtime.

## Source of truth

MySQL owns durable Run/Node/DAG/Checkpoint/ToolCall/Outbox state. Redis and RocketMQ are delivery infrastructure.

## Execution

1. Run Service creates a Run and asks Planner for a dynamic DAG.
2. Planner can create independent nodes.
3. Nodes with no dependencies become READY and are pushed to Redis Streams.
4. Worker claims a node with MySQL optimistic locking + a 30-second lease.
5. Worker executes the LLM/Tool operation.
6. Node SUCCESS and an Outbox event are committed in the same MySQL transaction.
7. Outbox Publisher sends the event to RocketMQ.
8. Resume Controller consumes StepCompleted and checks DAG dependencies.
9. Newly ready nodes are pushed to Redis.
10. When no unfinished nodes remain, Run becomes SUCCESS.

## Why Outbox?

Without Outbox:

```text
MySQL COMMIT -> SUCCESS
RocketMQ SEND -> failure
```

The Run can become permanently stuck because the resume event was lost.

With Outbox:

```text
BEGIN
  Node SUCCESS
  Outbox PENDING
COMMIT

Outbox Publisher -> RocketMQ
```

The state and event intent are durable together.

## Crash recovery

A worker claim creates a lease. If a worker crashes, the recovery process finds expired leases and returns nodes to PENDING before re-enqueueing them.

This deliberately gives at-least-once execution. Idempotency must therefore be enforced at side-effect boundaries.

## Exactly-once discussion

The runtime does not claim global exactly-once execution. It provides durable state transitions plus idempotency keys, which gives effectively-once semantics for correctly implemented tools.
