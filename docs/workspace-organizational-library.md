# Workspace Organizational Library Contract

## Purpose

Provide a deterministic control-plane projection for SanityCloud work without coupling workspace organization to provider stream parsing.

## Canonical lineage

`Accountable human -> governance root -> department/context -> program/project -> branch/work unit -> task/risk/decision/receipt/outcome`

The runtime Hive database remains the technical event store. Notion remains the organizational system of record after a verified projection is promoted by an authorized workflow.

## Separation of concerns

- `stream_parser.py` validates provider wire events and emits normalized completion/content events.
- `api/chat.py` prevents unvalidated streamed output from reaching clients.
- `workspace_library.py` projects verified Hive snapshots into organizational records.
- The projection does not mutate Notion, infer missing governance facts, or convert model prose into authoritative records.

## Required project/branch fields

Every project or branch should record: parent record, purpose, scope, exclusions, accountable human, authority ceiling, source boundary, dependencies, risks, acceptance criteria, decision gates, fan-in owner, and closure/transition condition.

Missing fields are returned in `evidence_gaps`; they are never fabricated.

## Promotion rule

Only validated, non-quarantined terminal outcomes may be promoted into the workspace library. A parser completion signal alone is insufficient authority for a Notion write.
