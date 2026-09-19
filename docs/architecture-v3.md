# Agent Runtime v3 Architecture

v3 does not replace the durable execution kernel. It adds a framework layer above it so
Eino/OpenAI/MCP/other implementations can be plugged in without coupling the kernel to a
specific SDK.

```text
Client / SSE
    |
    v
Runner
    |
    +--> ReAct Engine ----+
    |                     |
    +--> DAG Engine ------+--> Runtime Kernel
                               |
                               +--> Middleware
                               |
                               +--> Providers
                               |      |
                               |      +--> Adapters (Eino/OpenAI/MCP/...)
                               |      |
                               |      +--> Memory --> Vector adapter --> Qdrant
                               |                              (derived index, rebuildable)
                               |
                               +--> Durable Execution
                                      |
                                      +--> MySQL   (authoritative: runs/nodes/memory_indexed)
                                      +--> Redis
                                      +--> RocketMQ
```

Qdrant sits outside the durability boundary on purpose: it holds a **derived index** over
content whose authoritative copy is `agent_node.output` in MySQL. It can be dropped and
fully rebuilt from MySQL plus `memory_indexed`, so a vector-store outage degrades memory
recall but never fails a run.

## Three boundaries

1. **Agent decision** — ReAct/DAG decides what should happen next.
2. **Runtime execution** — Run/Node/Checkpoint/Event/Cancel/HITL owns execution semantics.
3. **Durability** — CAS/Lease/Retry/Idempotency/Outbox/Inbox/Recovery guarantees recovery.

## Provider contracts

`ModelProvider`, `ToolProvider`, `MemoryProvider`, `PromptProvider`, `MCPProvider`,
`SkillProvider`, and `SandboxProvider` are stable ports. Concrete SDKs are adapters.

`MemoryProvider` (Load/Save) is complemented by an optional `MemorySearcher` port for
semantic recall. Callers probe it with a type assertion (`m.(providers.MemorySearcher)`),
the same pattern used for `CancelStore`/`HITLStore` in the runtime, so the stable
`MemoryProvider` signature never changes. Vector-backed memory lives in
`internal/adapters/vector` (Qdrant via `github.com/qdrant/go-client`), with writes
projected by the standalone `cmd/memory-indexer` process.

## RuntimeEvent

LLM/tool/agent lifecycle signals are normalized into `RuntimeEvent`. Token streaming is
kept ephemeral; durable state is checkpointed to MySQL. This allows the same event stream
to feed SSE, MQ, logs, or observability without persisting every token.

## ReAct

`internal/agent/react` owns the Think -> Tool -> Observation loop. It depends on a
`StepRunner`, not a concrete tool or model SDK. A durable StepRunner can therefore map
these actions onto the existing Run/Node/Worker kernel.

## HITL

Human approval is modeled as durable `WAITING_HUMAN` state plus `run_interrupt`. The
process must not depend on an in-memory channel surviving a worker restart.
