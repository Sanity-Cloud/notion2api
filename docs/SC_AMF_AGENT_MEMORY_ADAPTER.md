# SC-AMF Agent Memory Adapter — R1 implementation receipt

Status: bounded implementation branch; not deployed.

## Source boundary

- SanityCloud repository: canonical `notion2api`, isolated worktree branch `feature/sc-amf-001-memory-adapter`.
- Worktree: `X:\Code\worktrees\sc-amf-001-memory-adapter`.
- Base commit: `22a83a6` (`fix(aigentbee): validate leader chat as leader identity`).
- Upstream repository: `TencentCloud/TencentDB-Agent-Memory`.
- Upstream default branch pinned for this pilot: `feat/server_team`.
- Upstream commit: `fe3230f176f1bf5832fee79d12494bbc2d19a8aa`.
- Upstream Python SDK observed version: `0.2.0`.

The upstream `main` branch is a different line (`3c6fc6425f22d24c4917dc3b7791a175d2d13545`) and is not the team-memory server baseline used by this R1 adapter.

## Implemented boundary

`app.agent_memory.SanityCloudMemoryAdapter` implements the contract-first derived-memory layer with:

- complete `IdentityEnvelope` admission fields;
- mandatory injected authoritative lease-validation hook (fail closed when absent);
- candidate-only writes;
- fail-closed project/principal/lease scoping;
- durable SQLite operation/idempotency receipts;
- durable receipt identity provenance and replay blocking for `RUNNING` / `OUTCOME_UNKNOWN` operations;
- secret detection and quarantine without raw secret payload persistence;
- bounded retrieval defaults (12 assets / 12,000 chars / 3 seconds / 2 graph hops);
- selection manifests;
- evidence-gap and dissent preservation;
- bidirectional contradiction preservation with conflicted records excluded from ordinary retrieval;
- reviewer-gated test-only transition to retrieval eligibility;
- non-destructive supersession;
- cooperative cancellation before local candidate commit;
- explicit `OUTCOME_UNKNOWN` freeze + evidence-bearing reconciliation without semantic replay;
- bounded, injected upstream read client boundary;
- no worker-callable `promote`, canonical-write, credential, delete, or core-write route.

## Upstream API observations used

At the pinned revision, the Python v3 client requires `team_id`, `agent_id`, and `user_id`; conversation writes also require `session_id`, and `task_id` is optional. Read methods include `/v3/conversation/*`, `/v3/atomic/*`, `/v3/scenario/*`, and `/v3/core/read`.

The pilot explicitly denies upstream delete routes and `/v3/core/write`. Native ACL/deletion/migration behavior and safe multi-principal L3 semantics remain validation blockers.

## Deliberately not claimed

- No local TencentDB Agent Memory service has been deployed by this branch.
- No live/private history has been ingested.
- No reusable credential is present in the adapter API.
- No canonical Notion/Graph state is modified by the adapter.
- No candidate memory or Skill can self-promote.
- No production/public routing is enabled.
