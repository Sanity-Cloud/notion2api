# Notion-Governed Branch Alignment — 2026-08-10

Status: Validate / local alignment only. This record does not authorize or claim deployment, publication, runtime promotion, or deletion of dirty worktrees.

## Governing sources

This alignment uses the latest canonical Notion records available on 2026-08-10:

- [Notion2API + AIgentBee consolidated engineering history](https://app.notion.com/p/3b6b13aa3ad28111a281f5538ecf6f7b)
- [Notion AI chat-history storage and persistence architecture](https://app.notion.com/p/3b6b13aa3ad2811dacd7f11e109d5ec1)
- [AIgentBee canonical operating instructions and governance specification](https://app.notion.com/p/5112ce7971b44ee2a511b7ec0f80d0a6)
- [AIgentBee, NotebookLM, and Notion2API cross-talk protocol](https://app.notion.com/p/3acb13aa3ad2810aac42c2a953972303)
- [SanityCloud Graph Project Fabric](https://app.notion.com/p/3b5b13aa3ad281b3aedff0d190336f49)
- [P0 Notion2API reliability and data-integrity program charter](https://app.notion.com/p/3acb13aa3ad2817aa9b2fa9f7a687299)
- [Sanity Management four Master Hives runtime charter](https://app.notion.com/p/3b2b13aa3ad2816fb693ed41685adca7)
- [N2API iteration write-through protocol and authority grant](https://app.notion.com/p/3acb13aa3ad281c68259ec6ba670446a)

## Accepted invariants

1. Existing conversations retain immutable workspace, account, profile, and remote-thread affinity. Account rotation applies only to new unbound work.
2. Server transcript RPCs are the canonical history source. The Notion2API durable archive is an independent raw and normalized archive; Electron-local stores are supplemental recovery evidence.
3. Raw transcript records are immutable or versioned, normalized projections are regenerable, and archival identities include account, workspace, user, thread, message, and version dimensions.
4. A persistent remote thread must not receive replayed historical dialogue. Remote-native persistence and stateless reconstruction are explicit, mutually exclusive modes.
5. Ambiguous remote outcomes reconcile against the original operation and binding before retry. Infrastructure failure must not silently mutate prompt, mode, sources, attachments, session intent, or parent operation.
6. Delegated authority is monotonic: child authority and writable scope are subsets of the lane and mission. An explicit empty writable domain remains empty.
7. Conversation serialization and resource-write serialization are separate controls. Consequential overlapping writes require a single writer, normalized resource identity, fencing, or escalation.
8. Independent review and dissent remain structurally separate from implementation and fan-in.
9. Notion records govern decisions and lifecycle, but Git, tests, runtime health, and external receipts remain required implementation evidence.
10. Local completion, merge, deployment, publication, and external delivery are distinct states.

## Branch disposition

| Branch or stack | Governing disposition | Reason and next gate |
|---|---|---|
| `feature/aigentbee-control-correctives` | Port one accepted invariant; hold the rest | Port the explicit-empty writable-domain correction. Re-evaluate heuristic prerequisite logic against the structured mission compiler before any further integration. |
| `feature/notion2api-notebook-updates-20260805` | Hold in Validate | The consolidated record classifies the grouped eight-action history surface as review work without unified implementation or smoke proof. Preserve it as a proposal until account-scoped validation, bounded pagination, privacy, partial-result, and idempotency gates are accepted. |
| `feature/model-catalog-reasoning-governance-20260806` | Hold as an independent feature lane | Useful but not a P0 integrity prerequisite. Port only after request identity, thread isolation, and archive controls remain intact. |
| `feature/model-evaluation-benchmark-20260806` | Preserve dirty evaluation work separately | Same committed model-catalog base plus uncommitted evaluation changes. Do not fold the dirty changes into catalog integration. |
| `feature/adaptive-account-scheduler` | Split and constrain | Health-aware selection may apply only to new unbound work in an eligible pool. It must not rotate an existing binding or override a pinned Master Hive account. Review its dirty CI change separately. |
| `deploy/notion2api-hive` and backup | Preserve as historical/separate runtime evidence | The four-Master-Hive charter supersedes a shared rotating runtime as the target operating topology. No silent migration of legacy sessions or shared stores. |
| R2 streaming and terminal-state branches | Supersession candidates | Preserve missing regression assertions, but prefer current typed stream-integrity and terminalization behavior. Delete only after an equivalence receipt and dirty-worktree review. |
| request-control admission/retry/fan-in/observability branches | Supersession or redesign candidates | Current admission/telemetry covers part of the need, but the P0 lifecycle still requires explicit logical-operation/attempt identity and indeterminate-outcome reconciliation. Do not merge the old stack as a substitute for that contract. |
| `fix/terra-alias-provenance-candidate` | Hold pending explicit disposition | Notion records it as a validated local candidate whose merge/live application requires a separate decision. Equivalent current behavior may supersede it, but that must be proven by assertion comparison. |
| already merged dirty worktrees | Preserve until evidence is captured | Commit ancestry does not include uncommitted files. Archive, commit, or explicitly discard those files before worktree removal. |

## Accepted local correction

`HiveMaterializationStore.materialize_invocation` previously expanded an explicitly empty requested writable-domain set to every writable domain held by the selected worker. That converted no requested mutation authority into broader worker authority.

The governed correction retains the set intersection as-is:

- empty request → empty lease;
- authorized subset → that subset;
- disjoint request → blocked materialization;
- reviewer lanes no longer inherit their worker's full writable domain merely because the mission request is empty for that lane.

This is a least-privilege correction derived from the adopted Graph Project Fabric and AIgentBee authority rules. It does not promote the rest of the source branch.

## Known implementation gaps retained as gates

The latest canonical records describe the following as unresolved or design/review work rather than completed implementation:

- complete request lifecycle with logical-operation and attempt identity;
- explicit unknown-remote-outcome and reconciliation-required states;
- durable Bee mailbox with cycle, duplicate, TTL, and hop controls;
- normalized resource write intents and fencing tokens;
- high-level `compile_and_dispatch_task` mission compilation surface;
- complete four-account fault-injection acceptance;
- transactional rollback of failed mission creation;
- real-time lossless raw server-record ingestion while automatic ingestion remains paused;
- safe treatment or quarantined migration of ambiguous legacy conversation bindings.

Branches must not be marked complete merely because they merge cleanly or contain adjacent functionality. Each gap requires its own bounded implementation lane, tests, evidence, independent review where material, and an explicit lifecycle transition.

## Publication and remote boundary

For this repository lineage, `Sanity-Cloud/notion2api` is the matching integration remote. `Sanity-Cloud/Notion2API-CLI` has disconnected history and is not a merge source for this `main`. Remote naming may be normalized separately, but no push or remote change is part of this alignment branch.
