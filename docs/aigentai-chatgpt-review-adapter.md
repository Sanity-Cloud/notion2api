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

The trusted transport must resolve a bounded source excerpt and manifest
from the supplied evidence SHA-256. **Expected** repository, PR number, exact
head/base commits, Git tree label, source mode, paths, excerpt ranges and hashes
come from the previously governed worker-lease `source_boundary` JSON, not
from the resolver that returns source bytes. A mismatched but syntactically
valid resolver manifest fails before reservation or submission. The source
excerpt is included as JSON-encoded **untrusted evidence**, separated from
review instructions; source comments cannot grant permissions or supply
test/approval claims.

The allowlisted governed JSON currently fits only within a 1,280-character
source-boundary field; larger full-diff/file manifests require a separately
approved immutable evidence-reference scheme. This is **claim consistency**,
not independent Git blob/tree verification: a real source-fetch qualification
must prove completeness for `full_diff`, `full_files`, or exact `excerpt`
path/ranges, and must independently bind the claimed commit/tree IDs. No
unit test or self-reported manifest substitutes for that proof.

The transport must durably reserve the original operation ID and prompt/manifest
hashes **before** any provider effect, submit at most once, and support
`lookup_by_idempotency_key` without submitting. This is an interface
requirement, not yet an accepted real transport implementation. Completion
requires a matching original job and operation ID, provider, immutable owner/
workspace/user/profile/lease/receipt binding, native conversation ID, compiled
prompt hash, exact source manifest hash, and bounded response-text SHA-256.
Missing proof or uncertain results remain `OUTCOME_UNKNOWN` without replay.
The trusted transport receives only an immutable, allowlisted identity snapshot,
not the unrestricted mission object or browser credentials.

Hive execution deadline expiration is also `OUTCOME_UNKNOWN` for this
adapter rather than generic retryable `TIMED_OUT`. Generic stale
`CLAIMED` or `RUNNING` recovery is explicitly denied for this
adapter. A separately A2-authorized
`inspect_chatgpt_review_reconciliation` path can inspect even an expired
original worker lease using its persisted identity and the original idempotency
key, **read-only**, never submitting or automatically marking the task complete.
A distinct governed reconciliation decision and independent review remain
necessary before accepting any outcome. Concurrency and late worker finalization
remain release-qualification tests; a fake transport does not establish them.

## Gates before any activation

1. Review and merge this source against **Sanity-Cloud/notion2api**, not the
   unrelated `Notion2API-CLI` remote. Protect all dirty primary worktrees.
2. Adopt an evidence-backed governance-plan decision for A1 appointments
   of the four requisitioned read-only specialist roles and a distinct
   independent governance reviewer. Requisition is not appointment.
3. Review and implement a fixed-loopback, owner-bound transport with
   server-held credentials and separate ChatGPT CDP/turn scope. It must resolve
   a trusted source manifest/excerpt, durably reserve the original operation
   before submission, and locate original jobs by idempotency key after crash.
   Do not use arbitrary shell/network execution or transfer credentials.
4. Prove real durable reservation, native user/assistant turn identity,
   result hash, account/worker/source binding, no duplicate turns under
   concurrent requests, late-completion race safety, and read-only recovery
   after losing a returned job ID. Existing tests are mock-only.
5. Independently approve/enable the adapter, read its persisted enabled
   revision, materialize an appointed Hive lease and test **one** new bounded
   smoke. The earlier platform-rejected direct dispatch must not be retried
   or silently rerouted as a workaround.
6. Keep Arena HTTP writes at 423, AG-005 OAuth and v5 live WAL migration held
   until their separate acceptance records independently pass.

Source tests use injected fakes, a private test SQLite database and no
browser/network traffic. Passing them does not establish authenticated
production access, user consent, agent execution or v5 readiness.
