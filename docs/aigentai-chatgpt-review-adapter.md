# AIgentBee: compiled AiGentAI ChatGPT review adapter (source candidate)

This is a **disabled-by-default source implementation**, not approval to run
ChatGPT jobs or a deployed production integration. The adapter is part of
AIgentBee's bounded Hive dispatcher, not a second swarm coordinator.

## Compile-time admission

- Implementation: `builtin.aigentai_chatgpt_review.v1`
- Capability: `chatgpt_review`; authority floor **A1**
- Writable domains: **none**; independent review and human approval required
- Limits: 30,000 ms and 8,192-byte payload/result maximum
- Registry state after seeding: **DISABLED**; existing enabled state is not reset
- Payload permits exactly `lane` and lower-case `evidence_sha256`
- Compiled lane IDs: `AG002`, `AG003`, `AG005`, `V5`; no free-form prompts

This adapter does not contain network code or credentials. AIgentBee must
inject a separately qualified **owner-scoped** transport at runtime.
If no transport is injected, execution fails closed. No direct ChatGPT API,
CDP automation, HTTP URL, token, browser profile or account selector is
accepted through the dispatch payload.

## Identity and uncertain submission

The active Hive mission, worker, lease and READY/ACKNOWLEDGED dispatch receipt
are validated using the existing Agent Memory context check. The adapter
binds the original workspace, user, profile, worker, work unit, execution ID
and evidence hash to a deterministic provider idempotency key.

The trusted transport must submit exactly once and read back the same durable
job. Completion requires a matching provider `chatgpt`, account profile,
verified native conversation ID, and SHA-256 of the bounded response text.
Any missing proof, readback timeout, post-submit cancellation or scope mismatch
becomes `OUTCOME_UNKNOWN` without a semantic retry.

Hive execution deadline expiration is also `OUTCOME_UNKNOWN` for this
adapter rather than generic retryable `TIMED_OUT`. Generic stale
`CLAIMED` or `RUNNING` recovery is explicitly denied for this
adapter, even with A2 recovery authorization. Only read-only reconciliation
of the original provider job is permitted.

## Gates before any activation

1. Review and merge this source against **Sanity-Cloud/notion2api**, not the
   unrelated `Notion2API-CLI` remote. Protect all dirty primary worktrees.
2. Adopt an evidence-backed governance-plan decision for A1 appointments
   of the four requisitioned read-only specialist roles and a distinct
   independent governance reviewer. Requisition is not appointment.
3. Review and implement a fixed-loopback, owner-bound transport with
   server-held credentials and separate ChatGPT CDP/turn scope. Do not use
   arbitrary shell/network execution or transfer Copilot/Arena credentials.
4. Verify an original-gateway job ID, native user/assistant turn identity,
   result hash, idempotency and unknown-outcome nonreplay across restart.
5. Independently approve/enable the adapter, read its persisted enabled
   revision, materialize an appointed Hive lease and test **one** new bounded
   smoke. The earlier platform-rejected direct dispatch must not be retried
   or silently rerouted as a workaround.
6. Keep Arena HTTP writes at 423, AG-005 OAuth and v5 live WAL migration held
   until their separate acceptance records independently pass.

Source tests use injected fakes, a private test SQLite database and no
browser/network traffic. Passing them does not establish authenticated
production access, user consent, agent execution or v5 readiness.
