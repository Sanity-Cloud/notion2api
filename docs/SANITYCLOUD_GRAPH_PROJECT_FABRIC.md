# SanityCloud Graph Project Fabric

## Purpose

The Graph Project Fabric turns a SanityCloud coding, business, creative, or hybrid
initiative into one governed AIgentBee mission. It uses the existing SQLite event
ledger, workforce registry, leases, dispatcher, review, dissent, and fan-in controls.
It does not require a graph database.

Canonical lineage:

```text
Founder / accountable human
  -> governance record
  -> project / Hive mission
  -> management branch / work unit
  -> task and execution events
  -> evidence, review, dissent, decision
  -> outcome and closure receipt
```

## System ownership

| System | Responsibility |
|---|---|
| Notion | Canonical governance, project, task, decision, risk, and outcome records |
| AIgentBee | Project admission, identity binding, graph validation, routing, leases, and fan-in |
| SanityCloud Session Broker | Opaque authenticated-session capability leases, generation invalidation, provider adapters, and redacted receipts |
| Notion2API | Persistent advisory conversations and durable execution receipts |
| NotebookLM | Bounded source-grounded research; advisory until promoted |
| RepoAI | Approved repository implementation, testing, packaging, and review |
| Accountable human | Authority, consent, consequential approvals, and final acceptance |

## Project contract

### Human-readable authority levels

| Human name | Compatibility code | Meaning |
|---|---|---|
| Observe only | `A0` | Inspect and monitor without changing project content or state |
| Prepare and recommend | `A1` | Research, analyze, draft, summarize, and propose within approved sources |
| Execute bounded work | `A2` | Perform scoped, reversible project work within declared writable domains |
| Manage high-impact work | `A3` | Coordinate high-risk governed operations with required evidence and review |
| Critical authority | `A4` | Highest system-recognized ceiling; reserved human gates still cannot be bypassed |

Human-facing views show the name first, such as `Execute bounded work (A2)`.
Existing records and API inputs retain `A0`–`A4` as stable compatibility identifiers.

Pass `project_contract` to `hive_create_mission`. A governed project also requires
`parent_context_id`, `workspace_id`, and `user_id`.
New Hive missions default to `Execute bounded work (A2)`; A3/A4 remain explicit
compatibility levels rather than implicit defaults.

```json
{
  "title": "Launch the SanityCloud client story system",
  "objective": "Create and validate a reusable client-story workflow",
  "lifecycle_stage": "Plan",
  "parent_context_id": "notion-governance-or-project-parent-id",
  "workspace_id": "canonical-workspace-id",
  "user_id": "bound-notion-user-id",
  "authority_ceiling": "A2",
  "project_contract": {
    "project_kind": "hybrid",
    "scope": "Research, plan, build, review, and package the workflow.",
    "exclusions": [
      "Publishing without human approval",
      "Using sources outside the approved project set"
    ],
    "accountable_human": "SanityCloud Founder",
    "source_boundary": [
      "Approved Notion project records",
      "Approved repository files",
      "Approved NotebookLM sources"
    ],
    "risks": [
      {
        "risk": "Cross-branch edits",
        "mitigation": "Separate writable domains and independent review"
      }
    ],
    "acceptance_criteria": [
      "Every artifact traces to evidence and responsible activity",
      "Independent review is recorded",
      "Material dissent remains attached to the final decision"
    ],
    "decision_gates": [
      "Human approval before publication or external delivery"
    ],
    "fan_in_owner": "AIgentBee project leader",
    "closure_condition": "Accepted outcome and terminal receipts are recorded"
  },
  "work_units": [
    {
      "work_unit_id": "research",
      "title": "Research source material",
      "role": "researcher",
      "writable_domain": "notebooklm:project-sources",
      "dependencies": [],
      "authority_ceiling": "A1"
    },
    {
      "work_unit_id": "build",
      "title": "Build the reusable workflow",
      "role": "developer",
      "writable_domain": "repo:client-story-system",
      "dependencies": ["research"],
      "authority_ceiling": "A2"
    },
    {
      "work_unit_id": "review",
      "title": "Review evidence and outcome",
      "role": "independent reviewer",
      "writable_domain": "notion:project-review",
      "dependencies": ["build"],
      "authority_ceiling": "A2"
    }
  ]
}
```

`project_kind` accepts `coding`, `business`, `creative`, or `hybrid`. The control
plane is otherwise domain-neutral; behavior comes from the project contract and
work-unit graph, not a separate scheduler for each kind.

## Creation-time invariants

Mission creation fails before durable mutation when any of these checks fail:

- workspace/user identity is missing or mismatched;
- mission or work-unit authority is unknown;
- child authority exceeds the mission ceiling;
- a dependency is missing, duplicated, or self-referential;
- the dependency graph contains a cycle;
- work-unit count, dependency count, graph depth, or fan-out exceeds a finite cap.

The existing `MISSION_OPENED` event carries a `graph_receipt` containing:

- dependency edge count;
- mutation-conflict pairs derived from writable domains;
- dependency-ready waves;
- conflict-independent execution waves;
- graph depth and maximum safe parallel width;
- validated authority ceiling.

The receipt is also exposed on `HiveMissionSnapshot.graph_receipt`. The complete
project contract is exposed on `HiveMissionSnapshot.project_contract` and projected
into the workspace organizational library.

## Delegated task layer

A work unit is an execution lane. `hive_delegate_tasks` creates a bounded child DAG
beneath one or more lanes. Each `HiveDelegatedTaskSpec` records the parent lane,
objective, scope, exclusions, required context, source boundary, writable domains,
authority level, dependencies, acceptance criteria, deliverables, evidence
requirements, checkpoint, fan-in owner, closure condition, and optional worker
binding.

The runtime rejects delegation before mutation when a task exceeds its lane or
mission authority, expands beyond the mission source boundary, writes outside the
parent lane domain, references an unknown or cross-lane dependency, creates a cycle,
or exceeds graph bounds. Writable-domain comparison is hierarchical, so
`repo:project/api` conflicts with `repo:project/api/routes`.

`hive_transition_task` provides the durable lifecycle:

```text
TASK_DELEGATED -> TASK_ACCEPTED -> TASK_ACTIVE
                                     |      |
                              TASK_BLOCKED  HANDOFF_READY
                                                |
                                        HANDOFF_ACCEPTED
                                                |
                                      LANE_FAN_IN_READY
```

Acceptance and active execution use finite AIgentBee execution leases. These are
worker/scheduler locks, not authenticated-session capabilities. When a task needs a
provider session, it uses the existing `X:\Code\sanitycloud-session-broker` MCP
surface (`session_broker_acquire`, `session_broker_execute`,
`session_broker_refresh`, `session_broker_health`, `session_broker_revoke`, and
`session_broker_receipts`). Hive records only redacted broker receipts as evidence;
it does not persist broker lease IDs, cookies, tokens, or provider credentials.

A task cannot start until
all dependencies complete, and two active tasks in the same lane cannot hold
overlapping writable domains. A `BLOCKED` task is excluded from `ready_task_ids`;
it can resume only through an explicit allowed transition after its blocker is resolved.
`HANDOFF_READY` requires evidence and a typed handoff
receipt. The snapshot exposes every task plus a lane-local graph receipt with safe
execution waves, current ready tasks, conflicts, and automatic fan-in readiness.

Task events remain in the existing Hive event ledger; task current state is stored
in `hive_delegated_tasks`. Workspace projection preserves the full lineage:

```text
governance -> project/mission -> lane/work unit -> delegated task -> event/evidence
```

## Reusable delivery lifecycle

1. **Frame** — record parent, purpose, scope, exclusions, authority, sources, risks,
   acceptance criteria, decision gates, fan-in owner, and closure condition.
2. **Decompose** — create work units with stable IDs, writable domains, authority
   ceilings, and dependencies.
3. **Validate** — reject invalid identity, authority, dependency, and concurrency
   graphs before mutation.
4. **Plan** — use the receipt's dependency and execution waves as the bounded
   scheduling topology.
5. **Execute** — materialize appointed workers and finite leases through the existing
   guarded dispatcher.
6. **Review** — preserve evidence, independent findings, risks, and dissent.
7. **Fan in** — the named owner records the integrated decision without erasing
   minority findings.
8. **Close or transition** — terminalize leases and work while preserving identity,
   provenance, and history.

## Domain patterns

### Coding

Typical branches: discovery, architecture, implementation, security review, tests,
documentation, release decision. Writable domains should be repository paths or
explicit artifacts. Commit, merge, deployment, and publication remain separate gates.

### Business

Typical branches: customer evidence, offer design, financial analysis, operating
workflow, legal/risk review, decision memo. Spending, contracting, account changes,
and external commitments require accountable-human authority.

### Creative

Typical branches: source/story research, concept, draft, production, accessibility
and quality review, rights/consent review, release decision. The project contract must
name source, consent, likeness, and publication boundaries where relevant.

### Hybrid

Use one mission when the branches share one outcome and fan-in owner. Use separate
missions when they have different accountable humans, authority ceilings, source
boundaries, or closure conditions.

## Current implementation boundary

Implemented now:

- cross-domain governed project contract;
- bounded dependency and authority validation;
- mutation-conflict detection and safe wave derivation;
- durable project/graph receipts in the existing event ledger;
- workspace-library projection for canonical operating records;
- durable delegated tasks beneath lanes with child-DAG validation;
- authority, source, and hierarchical writable-domain inheritance enforcement;
- finite task leases, typed handoff receipts, and lane-local conflict locks;
- automatic lane fan-in readiness and delegated-task workspace projection;
- regression tests for valid hybrid projects and fail-closed invalid graphs.

Deferred until measured need:

- dedicated graph database;
- graph query API beyond mission snapshots and workspace projection;
- cross-mission portfolio optimization;
- automatic external publication or high-authority actions.
