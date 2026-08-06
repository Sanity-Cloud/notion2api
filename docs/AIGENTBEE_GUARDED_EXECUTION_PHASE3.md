# AIgentBee Guarded Execution Dispatcher - Phase 3

## Lifecycle and authority

Phase 3 implements the `Validate -> Build -> Pilot -> Adopt` gate between a Phase 2 `READY` dispatch receipt and bounded execution.

The dispatcher does not grant credentials, expand worker authority, or expose arbitrary execution. Every execution is constrained by the existing materialization plan, worker appointment, lease, adapter contract, and human-governance rules.

## Fail-closed execution flow

```text
MATERIALIZED plan + READY receipt + ACTIVE, non-stale lease
  -> enabled compiled adapter
  -> capability/domain/authority/payload/timeout verification
  -> CLAIMED
  -> ACKNOWLEDGED Phase 2 receipt
  -> RUNNING
  -> COMPLETED | REVIEW_REQUIRED | FAILED | TIMED_OUT | CANCELLED
  -> Phase 2 receipt synchronization
  -> lease release and human fan-in
```

A denied request is recorded as `DENIED` without consuming the Phase 2 lane or releasing its lease.

## Compiled adapter boundary

Only implementation identifiers compiled into `app/hive_dispatcher.py` can be registered. Unknown implementation identifiers are rejected.

Built-in implementations:

- `builtin.noop.v1`: deterministic no-op receipt generation.
- `builtin.evidence_digest.v1`: deterministic digest of bounded JSON evidence; independent review is mandatory.
- `builtin.bounded_delay.v1`: cooperative timeout and cancellation validation; a governed authorization receipt is mandatory for every execution.

All built-ins seed as `DISABLED`. Enabling an adapter requires an adopted-plan authorization receipt within the configured authority ceiling. Compiled capability, writable-domain, timeout, payload-size, review, and approval limits cannot be weakened through the registry.

No built-in adapter provides shell, process, browser, network, filesystem, credential, environment, URL, or arbitrary-code access.

## Execution preconditions

A new execution requires all of the following:

1. The plan is `MATERIALIZED`.
2. The lane receipt is `READY`.
3. The worker lease is `ACTIVE`.
4. Lease liveness is not `STALE`, `OFFLINE`, or `EXPIRED`.
5. The assigned worker is `APPOINTED`.
6. The adapter is `ENABLED`.
7. The requested capability is explicitly allowed.
8. Requested writable domains are allowed by both the adapter and lease.
9. Lease authority meets the adapter requirement.
10. The payload is JSON-serializable and within the byte limit.
11. Blocked execution keys such as `command`, `shell`, `path`, `url`, `token`, and `credential` are absent.
12. Any required per-execution governed authorization receipt is present.

Failure at a policy precondition produces a durable `DENIED` execution and leaves the lane available for a corrected request.

## Independent review

An adapter or materialization plan can require independent review. The execution then stops at `REVIEW_REQUIRED`; the Phase 2 receipt remains `ACKNOWLEDGED`.

The reviewer must be:

- different from the executing worker;
- `APPOINTED`;
- classified as `GOVERNANCE_REVIEWER`; and
- authorized at or above the adapter requirement.

Approval moves the execution and Phase 2 lane to `COMPLETED`. Rejection moves them to `FAILED` and preserves the findings receipt.

## Recovery, timeout, and cancellation

Execution claims and state transitions are durable and idempotent. A human-approved recovery can resume a stale `CLAIMED` or `RUNNING` execution using the persisted bounded request. Recovery increments the attempt ledger. A repeated recovery key resumes a prior claimed recovery after a second interruption rather than creating another execution.

Timeout requests are capped by the compiled adapter limit. A timeout requests cooperative cancellation, records `TIMED_OUT`, and fails the Phase 2 lane. A cancellation request is cooperatively observed by running built-ins; claimed or review-pending executions can be cancelled immediately.

## Durable tables

Phase 3 adds:

- `hive_execution_adapters`
- `hive_execution_adapter_events`
- `hive_dispatch_executions`
- `hive_execution_reviews`
- `hive_execution_events`

The migration is additive and does not change the Hive runtime schema version.

## MCP operations

- `hive_upsert_execution_adapter`
- `hive_list_execution_adapters`
- `hive_execute_dispatch`
- `hive_get_execution`
- `hive_cancel_execution`
- `hive_recover_execution`
- `hive_review_execution`

The dedicated AIgentBee profile exposes the same operations with the `aigentbee_` prefix.

## Explicit exclusions

Phase 3 does not authorize arbitrary external adapters, local command execution, browser automation, remote provider invocation, credential use, secret access, or filesystem mutation. Adding any external-effect adapter requires a separate lifecycle gate, threat model, explicit authority contract, and live rollback validation.
