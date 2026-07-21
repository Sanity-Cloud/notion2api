# Notion2API MCP Concurrency and Multi-Tool Evaluation

**Date:** July 20, 2026  
**Repository:** `X:\Code\notion2api`  
**Evaluation base:** `2ca7f819075135ec8cd661a4339d2bea777d4af7`  
**Implementation branch:** `feat/mcp-concurrency-admission`  
**Scope:** MCP chat requests, persistent sessions, conversation storage, account/client sharing, Notion-internal tool execution, multi-source requests, and future fan-out/fan-in orchestration.

## Executive status

Notion2API supports **bounded parallel chat work across different conversations**. The MCP wrapper creates one pollable asynchronous task per `request_id`, while the backend stream transport uses an asynchronous HTTP client. SQLite conversation storage uses WAL mode and short per-operation connections, so different conversations are not intentionally serialized by persistence.

The pre-change same-conversation rule was not atomic. It checked for an active request, then separately wrote the new job, then separately registered the in-memory task. Two simultaneous submissions could both pass the check. Session records also used unguarded JSON read-modify-write sequences, so concurrent updates could overwrite unrelated sessions.

The P0 implementation in this branch closes those races **inside one MCP process** while preserving parallelism across different conversations. It does not claim cross-process safety, provider-account scheduling, or durable per-tool-call orchestration.

**Current verdict:**

- **Suitable after P0:** bounded single-process concurrency with one unresolved turn per conversation and parallel turns across independent conversations.
- **Not yet suitable:** multiple MCP worker processes sharing the JSON ledgers, high-volume queues, provider-account leasing, or guaranteed multi-tool fan-out/fan-in execution.
- **Multi-source status:** source scopes and web-access controls are supported request inputs, subject to Notion/provider authorization and availability.
- **Multi-tool status:** Notion-internal tools are enabled and unsafe-URL continuation can verify upstream `tool_step_ids`, but the MCP wrapper does not yet maintain a durable execution ledger for every internal tool call.

## Evidence snapshot

The live main checkout was inspected without modifying its runtime state.

| Item | Observed state |
|---|---:|
| Persisted MCP chat jobs | 403 |
| Completed | 302 |
| Stale | 22 |
| Error | 51 |
| Cancelled | 27 |
| Pending | 1 |
| Duplicate active conversation IDs in snapshot | 0 |
| Recoverable/orphan temp job ledgers | 24 |
| Persistent MCP sessions | 176 |
| Duplicate session conversation bindings in snapshot | 0 |
| Configured active Notion accounts | 1 |

The absence of a collision in one state snapshot does not prove atomic admission. The previous code path was structurally check-then-write and the previous tests did not submit competing claims simultaneously.

## Capability status by layer

| Layer | Current status | Safety assessment | Recommended direction |
|---|---|---|---|
| Top-level MCP requests | One `asyncio.Task` per request ID; different conversations may execute concurrently | Functional for one process | Preserve independent-conversation parallelism |
| Same-conversation turns | P0 atomically claims the job and registers its task under one process-local lock | Safe in one process; not fenced across processes | Add durable conversation leases and fencing tokens |
| Request identity | Caller-supplied or generated `request_id`; terminal responses are reused | Useful idempotency boundary | Bind request digest, payload digest, and immutable receipts |
| Polling and recovery | Durable job JSON, bounded response previews, SQLite checkpoint recovery | Functional at current scale; global JSON rewrite is a bottleneck | Move to SQLite WAL or sharded append-only job records |
| Session registry | P0 serializes read-modify-write mutation with an `RLock` | Prevents single-process lost updates | Add cross-process transaction/fencing support |
| Conversation storage | SQLite WAL, separate connections, transactional writes | Different-conversation concurrency supported | Preserve; add explicit same-conversation sequence constraint |
| Account selection | Round-robin plus cooldown | No lease, capacity reservation, or in-flight cap | Add account leases, per-account semaphore, and backpressure |
| Account HTTP client | Shared `requests`/cloudscraper session; `_scraper_lock` exists but is unused | Thread-safety and cookie mutation behavior are unproven | Prefer request-isolated clients or explicitly guard/prove shared client use |
| Source selection | `sources`, mode, task, and web-access controls are passed through | Supported input, not a delivery guarantee | Record resolved capabilities and unavailable-source reasons |
| Notion-internal tools | Upstream transcript can invoke tools; unsafe URL continuation validates selected `tool_step_ids` | Best-effort and upstream-opaque | Project structured tool events and receipts locally |
| Multi-tool lineage | No durable per-tool-call DAG or parent/child receipt chain | Insufficient for autonomous orchestration | Add parent request, tool call, result, and fan-in records |
| Diagnostics | Bounded public progress and persisted visible responses | Appropriate; raw hidden reasoning must not become workflow state | Integrate with RepoAI protected diagnostic traces, not duplicate hidden reasoning |

## P0 implemented in this branch

### Atomic conversation admission

`_claim_chat_job_task()` now holds the existing job-state lock while it:

1. reloads the durable ledger;
2. checks the exact request ID;
3. checks unresolved work for the same conversation;
4. writes a durable `pending` claim;
5. creates the asynchronous task;
6. records the claim as `running`; and
7. registers the in-memory task.

The job is therefore durable before it can become runnable. A scheduling failure records a terminal error and releases the conversation for a separately identified retry. A failure while writing the initial claim creates no task.

This closes the previous admission race. The lock covers only the admission transaction; provider work proceeds outside it, so different conversations are not globally serialized during inference.

### Idempotent duplicate request handling

Two simultaneous submissions with the same request ID produce one task and one durable job record. The second caller observes the existing request rather than launching duplicate work.

### Fail-closed orphan handling

A persisted active turn with no local task is marked stale, but a replacement is **not** started in the same call. The response requires durable reconciliation first. A missing local task may indicate a restart or a different process; treating it as immediate retry authority could overlap provider work.

### Session update serialization

Session creation, continuation, reset, rename, and metadata updates now share a process-local reentrant lock around their JSON read-modify-write sequence. Concurrent updates to different sessions no longer discard one another inside one process.

### Terminal checkpoint recovery

A completed job without an embedded response is reconstructed from the local SQLite conversation checkpoint when possible. The old path passed an unsupported argument to the pending-output function and could fail while reading an already completed job.

### Temp-ledger recovery hygiene

Valid temporary ledgers are merged into the canonical job state and removed only after the canonical write succeeds **and** the canonical file is reloaded and verified to contain every recovered job at an equal or newer timestamp. Invalid, unreadable, or unverifiable temporary files remain available for diagnosis.

### Transactional round allocation

`ConversationManager.persist_round()` now begins an immediate SQLite writer transaction before reading `next_round_index`. Competing connections therefore cannot both reserve the same index. The complete user/assistant turn, archive records, sliding-window row, and counter increment commit in one transaction.

## Concurrency invariants

The following invariants are now required for the supported single-process runtime:

1. At most one unresolved active request may own a conversation.
2. Independent conversations may execute concurrently after a brief ledger transaction.
3. The same request ID must not create more than one task or durable job.
4. A durable-write failure must cancel the newly created task and must not register it as active.
5. An orphaned active request must be reconciled before replacement.
6. Session updates to unrelated names must not overwrite each other.
7. Terminal job state must be monotonic; polling must not relaunch terminal work.
8. A response persisted in SQLite may authoritatively complete a job whose stream closure was lost.
9. Temporary recovery artifacts may be removed only after successful canonical promotion.
10. Raw private model reasoning must not become the execution ledger or retry authority.

Future multi-process and multi-tool work must add:

11. Every conversation claim has an owner, lease expiry, heartbeat, and fencing token.
12. Every tool invocation has a stable tool-call ID, argument digest, dispatch boundary, terminal status, and receipt digest.
13. No tool retry is authorized merely because the caller timed out.
14. Fan-out children preserve parent request and evidence lineage; fan-in accepts partial and minority results explicitly.
15. Account capacity and unsettled usage remain reserved until authoritative reconciliation.

## Test matrix

### Implemented in P0

- simultaneous same-conversation claims admit exactly one request;
- simultaneous different-conversation claims admit both requests;
- simultaneous same-request-ID claims create one task and one record;
- durable claim-write failure creates no task;
- task-scheduling failure records a terminal error and permits a separately identified retry;
- orphaned active work requires reconciliation and creates no replacement;
- concurrent unrelated session updates preserve both records;
- a terminal job without an embedded response recovers from the SQLite checkpoint;
- valid temporary ledgers are promoted, reloaded, verified, and removed;
- concurrent SQLite connections allocate unique round indices;
- existing MCP session, polling, attachment, and job tests remain green.

### Required before multi-process deployment

- two OS processes competing for one conversation lease;
- owner crash after dispatch but before terminal persistence;
- fencing-token rejection of a late prior owner;
- database lock contention and forced restart during admission;
- process restart with active, completed, indeterminate, and cancelled jobs;
- high-cardinality load with bounded ledger latency and memory use.

### Required before durable multi-tool orchestration

- duplicate tool-call ID across reconnects;
- timeout before dispatch versus timeout after dispatch;
- one failed child among parallel tool calls;
- cancellation propagation through a fan-out tree;
- fan-in with partial results and explicit minority reports;
- capability/source denial without silently widening scope;
- account cooldown, Retry-After, budget exhaustion, and unsettled usage reconciliation;
- tool result tampering and receipt-digest mismatch.

## Prioritized recommended updates

### P1 — durable cross-process request control

Replace the single global job JSON rewrite path with SQLite WAL or sharded append-only per-job records. Add:

- conversation lease owner;
- fencing token;
- lease expiry and heartbeat;
- immutable admission/request digest;
- monotonic terminal transition constraints;
- explicit reconciliation records;
- indexed queries by conversation, request, status, and update time.

The current JSON files can remain migration inputs and human-readable recovery artifacts, but should not remain the sole multi-process concurrency authority.

### P1 — account capacity and client isolation

Add an account lease layer above round-robin selection:

- configurable `max_in_flight` per account;
- bounded queue and fairness policy;
- cooldown and Retry-After integration;
- lease release only on authoritative terminal observation;
- usage/capacity reconciliation after timeouts;
- metrics for queue time, execution time, error class, and account saturation.

Begin with a conservative limit of one in-flight request for the current single account, then benchmark two before raising it. Do not infer safe concurrency from HTTP transport capability alone.

The shared cloud/requests client must either be protected by a documented critical section for stateful cookie operations or replaced with request-isolated client instances. The existing unused `_scraper_lock` is not evidence that shared use is safe.

### P1 — structured multi-tool execution projection

Project observable Notion-internal tool activity into a protected local ledger compatible with RepoAI’s execution guard and diagnostic trace. Minimum event fields:

- `request_id` and `conversation_id`;
- `tool_call_id` and parent tool-call ID;
- tool/capability/source name;
- canonical argument digest, not unrestricted raw secrets;
- dispatch state;
- started, completed, rejected, cancelled, or indeterminate status;
- result or receipt digest;
- resource/account lock identity;
- timestamps and duration;
- retry/reconciliation decision;
- visible bounded diagnostic summary.

Do not persist hidden chain-of-thought. Record observable tool inputs, outputs, status transitions, and sanitized diagnostics only.

### P2 — explicit fan-out/fan-in orchestration

Add a durable request DAG for specialist delegation:

- independent conversation branches for parallel specialists;
- declared authority and budgets per branch;
- parent/child lineage;
- cancellation propagation;
- synthesis only after declared completion policy is met;
- support for partial results and minority reports;
- content-addressed evidence attached to final synthesis;
- anti-loop and no-value termination signals.

This should interoperate with RepoAI’s execution guard rather than duplicating its failure taxonomy or protected diagnostic model.

### P2 — capability and source budgets

Before dispatch, declare allowed sources, web access, tool families, maximum fan-out, maximum tool calls, timeout, and output budget. The resolver should return what was actually available and used. Missing capabilities must fail explicitly or degrade according to a declared policy; they must not silently widen to unrelated sources.

## Relationship to concurrent development

- The separate attachment-type worktree was not modified.
- The Sanity Cloud portal’s active YouTube publisher branch was not modified.
- RepoAI’s current execution guard and protected diagnostic trace are treated as reusable contracts; this branch does not recreate them inside Notion2API.
- Internal API usage remains an approved architectural choice and is preserved.

## Delegated adversarial review status

A separate persistent Notion2API advisory request was submitted under:

- session: `notion2api-concurrency-review-20260720`
- request ID: `n2-concurrency-architecture-review-20260720-a`

The connector poll timed out repeatedly while the durable job continued independently. The request was not resubmitted and no duplicate conversation was created. The durable job later completed with a 9,510-character adversarial assessment.

The review agreed with the single-process boundary and required four corrections before treating P0 as complete:

1. persist the conversation claim before task creation;
2. record task-scheduling failure so it cannot strand an active claim;
3. verify canonical temp-ledger promotion before deleting recovery files; and
4. allocate round indices under an independent SQLite writer transaction.

All four corrections are implemented and regression-tested in this branch. The review also reaffirmed that account leases, cross-process fencing, and per-tool receipts remain P1/P2 work rather than capabilities this branch should claim.

## Validation receipt

- focused concurrency and recovery tests: `41 passed in 1.19s`;
- complete repository suite: `196 passed in 78.70s`;
- full-suite process exit code: `0`;
- full-suite stderr: empty;
- Ruff on all modified Python and test files: passed;
- Python compilation on all modified Python and test files: passed;
- `git diff --check`: passed with line-ending conversion warnings only;
- live secrets were not copied into the worktree; tests used synthetic account and API-key values.

## KISS summary

- Different conversations: parallel.
- Same conversation: one unresolved turn at a time.
- Same request ID: one task.
- Lost task: reconcile before retry.
- Current locks: one-process protection only.
- Multi-process and durable multi-tool execution: next architecture phase, not yet complete.
