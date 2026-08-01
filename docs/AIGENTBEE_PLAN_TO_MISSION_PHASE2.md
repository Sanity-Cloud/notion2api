# AIgentBee Plan-to-Mission Materialization â€” Phase 2

## Purpose

Phase 2 converts a Phase 1 invocation plan into a durable, governed Hive mission. It does not create workers, grant credentials, elevate authority, or execute external actions by itself.

The materialization boundary is fail-closed. A mission is created only when:

- all required competencies are covered;
- all requested writable domains are covered;
- enough workers exist for the planned lane count;
- every selected worker is `APPOINTED`;
- an appointed `GOVERNANCE_REVIEWER` is selected when independent review is required;
- every required human gate has been approved.

## Durable records

Phase 2 adds four additive tables to the existing Hive SQLite ledger:

- `hive_invocation_materializations`
- `hive_materialization_events`
- `hive_worker_leases`
- `hive_dispatch_receipts`

The existing Hive runtime schema version remains unchanged. Mission, work-unit, event, action, decision, worker, and worker-event records are not rewritten.

## Materialization states

```text
BLOCKED
AWAITING_APPROVAL
PREPARING
MATERIALIZED
READY_FOR_FAN_IN
CLOSED_WITH_FAILURE
CANCELLED
```

`PREPARING` is recoverable. An idempotent retry resumes mission creation and binding after an interrupted process.

## Worker leases

Each materialized lane receives a lease containing:

- the selected appointed worker;
- the mission and work-unit identifiers;
- the bounded authority ceiling;
- the intersected writable domains;
- the worker's source boundary;
- an active, released, or revoked state.

A lane authority is never higher than either the requested mission authority or the worker's appointment ceiling.

## Conversation and dispatch bindings

Every work unit receives a deterministic conversation binding:

```text
aigentbee:<plan_id>:<worker_id>
```

Every lane begins with a `READY` dispatch receipt. Execution systems may advance it through:

```text
READY â†’ ACKNOWLEDGED â†’ COMPLETED
                     â†’ FAILED
                     â†’ CANCELLED
```

Terminal receipts cannot be reopened. Each transition is written to the append-only materialization event ledger with idempotency protection.

## Lease release and fan-in

When all receipts become terminal:

- all completed lanes produce `READY_FOR_FAN_IN`;
- any failed or cancelled lane produces `CLOSED_WITH_FAILURE`;
- active leases are released automatically;
- the underlying Hive mission remains available for the governed `hive_fan_in` decision with evidence, authority, and dissent receipts.

Leases may also be explicitly released or revoked without deleting the mission, bindings, or evidence.

## MCP operations

Dedicated AIgentBee profile:

```text
aigentbee_hive_materialize_invocation
aigentbee_hive_approve_materialization
aigentbee_hive_get_materialization
aigentbee_hive_record_dispatch_receipt
aigentbee_hive_release_materialization_leases
```

Primary Notion2API exposes the same operations without the `aigentbee_` prefix.

## Explicit exclusions

Phase 2 does not:

- provision or rotate credentials;
- automatically appoint workers;
- invoke arbitrary tools from a lease;
- bypass connector authorization;
- publish externally;
- spend funds;
- close the Hive mission without human fan-in;
- push or rewrite remote Git history.

## Next gate

Phase 3 may add a guarded dispatcher that consumes `READY` receipts, calls an explicitly authorized execution adapter, records acknowledgements and terminal evidence, and preserves provider-specific cancellation and timeout semantics.
