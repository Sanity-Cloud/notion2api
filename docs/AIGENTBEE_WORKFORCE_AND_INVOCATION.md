# AIgentBee Workforce and Invocation Contract

## Purpose

This contract defines the first governed workforce-management and invocation-routing slice for AIgentBee. It distinguishes durable worker identity from temporary mission work and keeps execution authority human-controlled.

The implementation is intentionally limited to:

- recording worker requisitions and lifecycle transitions;
- listing the current workforce registry;
- planning whether a request should use one agent or a multi-lane Hive;
- identifying capability, writable-domain, and authority gaps.

It does not automatically spawn workers, grant credentials, change access, publish content, spend funds, or execute external effects.

## Worker classes

- `TEMPORARY_WORKER`: bounded assignment with no assumption of continued membership.
- `PERSISTENT_MEMBER`: durable Hive member with a maintained role charter.
- `SPECIALIST_CONTRACTOR`: specialist available for defined scopes or missions.
- `ROAMING_SCOUT`: read-oriented discovery and issue-identification role.
- `HIVE_LEADER`: coordination and fan-in role; not an unrestricted administrator.
- `GOVERNANCE_REVIEWER`: independent review, dissent, and gate-validation role.

## Hiring and onboarding lifecycle

Every durable worker begins in `REQUISITIONED`. Registration records the role, accountable owner, worker class, competencies, writable domains, authority ceiling, source boundary, and appointment scope.

Allowed transitions are:

```text
REQUISITIONED -> SHADOW | REJECTED | OFFBOARDED
SHADOW        -> PROBATION | SUSPENDED | OFFBOARDED
PROBATION     -> APPOINTED | SUSPENDED | OFFBOARDED
APPOINTED     -> SUSPENDED | OFFBOARDED
SUSPENDED     -> PROBATION | APPOINTED | OFFBOARDED
```

`REJECTED` and `OFFBOARDED` are terminal. Moving into `PROBATION` or `APPOINTED` requires `human_approval=true`. Revision checks and idempotency keys prevent stale or duplicated lifecycle changes.

A worker record is not a credential grant. Credentialing and account access remain separate governed actions.

## Invocation planning

`hive_plan_invocation` is a read-only planner. It considers:

- objective;
- required competencies;
- writable domains;
- dependency count;
- number of parallelizable workstreams;
- risk level;
- requested authority ceiling;
- independent-review requirement;
- external effects;
- optional preferred workers.

The planner selects `single_agent` when one appointed or probationary worker covers the competencies, domains, and requested authority and no orchestration trigger applies.

It selects `hive` when any of the following applies:

- multiple parallel workstreams;
- task dependencies;
- mandatory independent review;
- high or critical risk;
- external effects;
- no single worker covers the full task.

A Hive plan may include lower-authority workers in bounded sub-lanes. That condition forces a human gate instead of silently expanding worker authority.

## Human-gate conditions

The plan sets `human_gate_required=true` when:

- the task has external effects;
- risk is high or critical;
- requested authority is `A3` or above;
- a selected worker is in probation;
- a selected worker has a lower ceiling than the overall task;
- required competencies or writable domains are missing.

## MCP operations

Primary Notion2API exposes bare names. The dedicated AIgentBee profile exposes the same operations with the configured `aigentbee_` prefix.

- `hive_register_worker`
- `hive_transition_worker`
- `hive_list_workers`
- `hive_plan_invocation`

Existing mission operations remain unchanged:

- `hive_create_mission`
- `hive_status`
- `hive_append_event`
- `hive_cancel`
- `hive_fan_in`

## Phase 1 limitations

This phase does not yet:

- automatically create a mission from an invocation plan;
- bind selected workers to new conversations;
- maintain performance scores or promotion evidence summaries;
- enforce worker selection inside every connector;
- automate suspension after policy violations;
- provision or revoke credentials;
- publish remote repository changes.

Those capabilities require a separate Pilot-to-Adopt gate with live mission simulations and explicit authority review.