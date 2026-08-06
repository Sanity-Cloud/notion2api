# AIgentBee Workforce Lifecycle Gap Closure

## Lifecycle gate

**Stage:** Build -> automated Validate

**Scope:** recruitment gaps, execution liveness, lease expiry/reconciliation, and workforce audit/offboarding.

This change closes four workforce-control gaps without granting credentials, expanding authority, deploying services, or publishing externally.

## Gap-driven recruitment

`hive_materialize_invocation` now accepts a `recruitment_mode`:

- `disabled`: backward-compatible behavior. Missing coverage leaves the plan `BLOCKED`.
- `requisition_only`: creates deterministic, bounded worker requisitions and returns the plan as `RECRUITING`. No appointment authority is granted.
- `auto_appoint`: creates a requisition and advances it through `SHADOW`, `PROBATION`, and `APPOINTED` only when the existing governed authorization gate passes. The planner is rerun after appointment and materialization proceeds only if competency, domain, lane-count, reviewer, and authority checks all pass.

Automatically generated workers:

- use deterministic `auto-recruit-<digest>` identifiers;
- are capped at `A2` even when the mission requests higher authority;
- receive only the missing competencies and writable domains;
- inherit a bounded source and appointment scope from the originating plan;
- receive no credentials, routing authority, deployment authority, or external-effect authority.

Existing callers do not recruit workers unless they select a recruitment mode explicitly.

## Lease lifecycle and execution liveness

Every new worker lease now records:

- `issued_at`;
- `expires_at`;
- `last_heartbeat_at`;
- `heartbeat_status`;
- `renewal_count`;
- bounded liveness evidence.

Registry state and execution state are separate:

- `ACTIVE` means the assignment has not been released, revoked, or reconciled as expired.
- `execution_live=true` requires a fresh `READY`, `RUNNING`, or `IDLE` heartbeat.
- a new lease is `ACTIVE` with `UNKNOWN` liveness until execution acknowledges or sends a heartbeat.
- `DEGRADED` is fresh but impaired.
- `STALE`, `OFFLINE`, and `EXPIRED` fail the guarded-dispatch precondition even when the stored lease status has not yet been reconciled.

`ACKNOWLEDGED` dispatch receipts automatically record a `RUNNING` heartbeat and extend the lease. Explicit heartbeats can also renew a lease without changing its authority or writable domains.

## Stale-lease reconciliation

`hive_reconcile_stale_leases` is dry-run by default. It identifies an `ACTIVE` lease when any of the following is true:

- the expiry timestamp elapsed;
- the worker reported `OFFLINE`;
- the last heartbeat exceeded the freshness threshold;
- no heartbeat was received within the initial grace period.

Applying reconciliation changes stale leases to `EXPIRED`. Optional revocation requires governed `A2` authorization. Every heartbeat and reconciliation operation produces an append-only lease event with idempotency protection.

Materialization performs local stale-lease reconciliation before selecting workers, preventing old assignments from indefinitely remaining authoritative.

## Workforce audit and offboarding

`hive_audit_workforce` is dry-run by default. It reports:

- placeholder, ignore, and do-not-use identities;
- abandoned requisitions;
- unresolved stale suspensions;
- chronically inactive appointments;
- duplicate appointment profiles.

Applying recommendations requires governed authorization. Non-protected workers with an `OFFBOARD` recommendation can be transitioned through the existing append-only workforce event ledger.

`HIVE_LEADER` and `GOVERNANCE_REVIEWER` roles are protected:

- normal audits recommend review instead of automatically offboarding inactive protected roles;
- protected-role offboarding requires explicit inclusion and governed `A3` authorization;
- active assignments are never classified as chronically inactive.

Audit results and applied actions are stored in `hive_workforce_audits` and can be replayed safely by idempotency key.

## Additive migration

The migration is additive and backward compatible:

- existing lease rows are backfilled with `issued_at=created_at`;
- existing lease rows receive the default 24-hour expiry from creation;
- existing materialization records default to recruitment `disabled` and no recruited workers;
- new tables are `hive_lease_events` and `hive_workforce_audits`;
- no credential-bearing values are persisted.

## MCP operations

Primary Notion2API exposes:

```text
hive_heartbeat_worker_lease
hive_reconcile_stale_leases
hive_audit_workforce
```

The AIgentBee profile exposes the same names with the `aigentbee_` prefix.

The guarded dispatcher rejects `STALE`, `OFFLINE`, or `EXPIRED` liveness before invoking an adapter.

## Decision boundary

Automated validation can establish schema compatibility, state transitions, idempotency, concurrency behavior, and fail-closed dispatch rules. Manual Pilot validation remains required before production restart or adoption to confirm:

- heartbeat cadence under real worker execution;
- appropriate lease TTL and grace thresholds;
- quality of recruitment competency labels;
- audit false-positive rate on the existing workforce registry;
- operator recovery and protected-role review procedures.

## Portal control-plane contract

The SanityCloud portal is an observability and governed control surface. It reads the backend projection exposed under `/v1/hive/workforce` and may submit explicit operator actions, but it is not the workforce scheduler.

Portal-visible areas are:

- workforce registry: identity, role, competencies, model, account profile, appointment state, runtime state, quarantine state, and current assignment;
- requisition queue: originating plan, urgency, missing competencies/domains, matching attempts, candidate evaluations, and appointment outcome;
- lease monitor: issuance, expiry, renewal, heartbeat age, liveness, stale classification, and projected cleanup action;
- recruitment policy: automatic-hiring mode, worker ceiling, allowed models, evaluation threshold, budget declarations, quarantine rules, and service intervals;
- audit and offboarding: findings, applied actions, revocation history, and retained lease/dispatch artifacts;
- operational metrics: time to fill, gap-blocked plans, stale leases removed, appointments, utilization, and failed evaluations.

## Portal control-plane contract

The SanityCloud portal is an observability and governed control surface. It reads the backend projection exposed under `/v1/hive/workforce` and may submit explicit operator actions, but it is not the workforce scheduler.

Portal-visible areas are:

- workforce registry: identity, role, competencies, model, account profile, appointment state, runtime state, quarantine state, and current assignment;
- requisition queue: originating plan, urgency, missing competencies/domains, matching attempts, candidate evaluations, and appointment outcome;
- lease monitor: issuance, expiry, renewal, heartbeat age, liveness, stale classification, and projected cleanup action;
- recruitment policy: automatic-hiring mode, worker ceiling, allowed models, evaluation threshold, budget declarations, quarantine rules, and service intervals;
- audit and offboarding: findings, applied actions, revocation history, and retained lease/dispatch artifacts;
- operational metrics: time to fill, gap-blocked plans, stale leases removed, appointments, utilization, and failed evaluations.

The portal must not poll worker heartbeats, expire leases, create or evaluate workers, restart worker processes, or clean stale assignments. Those operations remain in `app.hive_workforce_governor`, worker runtimes, and the guarded backend APIs so closing or restarting the portal cannot disable governance.

The backend exposes:

```text
GET  /v1/hive/workforce/overview
GET  /v1/hive/workforce/registry
GET  /v1/hive/workforce/requisitions
GET  /v1/hive/workforce/leases
GET  /v1/hive/workforce/policy
PUT  /v1/hive/workforce/policy
POST /v1/hive/workforce/lease/heartbeat
POST /v1/hive/workforce/leases/reconcile
POST /v1/hive/workforce/recruitment/process
POST /v1/hive/workforce/audits
```

`app.hive_workforce_governor` is independently executable and isolates lease reconciliation, recruitment, and audit lanes so one lane failure does not suppress the others. Its latest durable run state is included in the overview projection.

## Policy enforcement boundary

The worker ceiling, allowed models, minimum evaluation score, and quarantine rules are enforced by the backend. Budget fields are persisted and surfaced as governance declarations, but billing-grade budget enforcement remains blocked until actual provider token and cost telemetry is available; estimated stream-byte token values are not used for spending decisions.
