# Agent Policy & Approval Gateway — P0

This change adds a policy decision point between Agent scheduling and tool execution.

## Decision model

`ALLOW` → execute immediately

`REQUIRE_APPROVAL` → persist the existing durable HITL interrupt and stop before `ClaimNode`

`DENY` → deterministic policy failure, never retry as an infrastructure failure

Policy precedence:

`DENY > REQUIRE_APPROVAL > ALLOW`

## Why the gate is before ClaimNode

The worker evaluates policy before acquiring the node lease. Therefore an approval wait never holds a worker lease and never needs an in-process goroutine/channel.

The existing durable HITL implementation moves the Run to `WAITING_HUMAN`. After approval, `Runtime.Resume` moves the Run back to `RUNNING` and requeues `CurrentNodeID`.

This gives:

Tool Call
→ Policy
→ Require Approval
→ Durable WAITING_HUMAN
→ Human decision
→ Resume
→ READY
→ Worker Claim
→ Execute

## Extension points

Implement `policy.Policy` for:

- command / shell rules
- filesystem scope rules
- network/domain rules
- production-resource rules
- tenant/project policies
- enterprise deny lists
- capability-token checks

Do not put these checks inside individual executors.

## Important limitation

The default `CommandPolicy` is intentionally conservative but is NOT a sandbox. It is a policy layer only. A production-grade execution boundary still needs OS/container/remote-sandbox isolation.

## Existing repository integration

The repository already has durable HITL and `run_interrupt` persistence. The supplied patch reuses that mechanism rather than creating another approval store.
