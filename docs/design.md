# Agent Runtime v1 Design

## Core abstraction

A `Run` represents one Agent execution. A Run contains dynamically generated `Step`s.

```text
Run
 |
 +-- Step: LLM Planning
 +-- Step: Search Tool
 +-- Step: LLM Reasoning
 +-- Step: Finish
```

Unlike a traditional workflow engine, the execution graph can be expanded by the Planner at runtime.

## Reliability

The runtime uses an at-least-once execution model.

A Worker may crash after a Tool succeeds but before the Step/Checkpoint is persisted. Retrying the logical Tool operation therefore requires an idempotency key.

The v1 implementation uses:

```text
run_id + step_id
```

as the logical operation key for a Tool Step.

State transitions use optimistic locking:

```text
expected_version -> CAS update -> version + 1
```

This prevents two Workers from simultaneously claiming the same Step.

## State separation

The production direction is:

```text
Run metadata        -> PostgreSQL
Hot execution state -> Redis
Large artifacts     -> Object Storage
Events / traces     -> Kafka + analytical storage
```

The Runtime should pass references rather than embedding very large context payloads in control-plane records.

## Production evolution

### v2

- PostgreSQL persistence
- Redis/Kafka queue
- delayed retry queue
- event-driven Run resumption
- multiple Runtime instances
- lease/heartbeat for long-running Steps

### v3

- dynamic DAG / parallel Steps
- SubAgent
- per-tenant quota
- fair scheduling
- cost/token budgets
- stronger cancellation
- OpenTelemetry tracing
- persistent tool-call records

## Important limitation of v1

`Run()` waits for each generated Step to finish. This is intentional for readability. A production Agent Runtime should not hold a goroutine for a long-running Run. It should persist Run state and resume planning from Step-completion events.
