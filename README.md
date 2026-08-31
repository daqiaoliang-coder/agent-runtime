# Agent Runtime

A first-version Go implementation of an Agent Runtime for interview/system-design practice.

## Architecture

```text
Client
  |
  v
Agent Runtime
  |
  +--> Planner ---------> dynamic Step
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
          +--> Tool Idempotency
          +--> Event / Trace
```

## Implemented in v1

- Run / Step / Checkpoint / ToolCall / Event models
- Planner / Scheduler / Executor separation
- Dynamic step generation
- State machine
- Optimistic locking / CAS
- At-least-once execution model
- Tool idempotency
- Retry with exponential backoff + jitter
- Timeout
- Cancellation request
- Max-step guardrail
- Event / trajectory tracing
- In-memory StateStore and Worker Queue

## Run

```bash
go run .
```

Expected flow:

```text
LLM planning
  -> search tool
  -> LLM reasoning
  -> finish
```

## Design note

This is intentionally a single-process MVP. The interfaces are designed so that the next version can replace:

- MemoryStore -> PostgreSQL
- in-memory queue -> Kafka / Redis Streams
- MockLLM -> real LLM provider
- MockTool -> HTTP/gRPC/MCP tools
- synchronous step waiting -> event-driven resume

See `docs/design.md` for the intended production evolution.
