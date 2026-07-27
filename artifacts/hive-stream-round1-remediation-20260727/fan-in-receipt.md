# Hive fan-in receipt - hive-stream-round1-remediation-20260727

## Scope and isolation
- Worktree: `X:\Code\.worktrees\notion2api-stream-round1-20260727`
- Branch: `fix/stream-round1-protocol-20260727`
- Baseline reviewed: `8cb2dddd6910a340057b372faa7173bf7e6c5b23`
- No shared-checkout modifications, merge, deploy, service restart, secret access, or evidence deletion.

## Remediation disposition
- **Accepted - coherent non-success closure:** the frontend streaming consumer, embed consumer, and MCP client treat `[DONE]` as an unqualified normal close and do not visibly classify `finish_reason`. The guard now omits `[DONE]` for every non-success outcome: ordinary invalid/interrupted streams, forced character/chunk limits, and quarantined/content-filter streams. Clean validated streams alone preserve the provider terminal and `[DONE]` unchanged.
- **Accepted - bounded object overhead:** the character budget did not bound the number of buffered Python string/list objects. The guard now has a 10,000-chunk cap in addition to the 500,000-character cap, clears its primary buffer on either breach, and stops retaining later metadata/hygiene chunks after the breach. The fail-closed content-filter receipt identifies chunk-limit activation.
- **Accepted - bounded limit cancellation (decision B):** the final advisory's stated `break` was factually wrong: the prior guard continued draining after clearing its buffer, so an infinite upstream could consume forever. The guard now breaks on the first limit breach and runs a small best-effort `close()` helper in `finally`. The standard and lite stream adapters close their upstream generator in their own `finally`, so guard closure reaches `NotionClient.stream_response`; that generator's existing `finally` closes the active `requests.Response` whose parser consumes `response.iter_lines()`.
- **Preserved dissent / no terminal bookkeeping - GeneratorExit:** `GeneratorExit` still bypasses tracker finalization by design. The guard catches only ordinary `Exception` after explicitly re-raising `asyncio.CancelledError`; it does not catch `BaseException`, `GeneratorExit`, or `KeyboardInterrupt`. Its `finally` closes upstream without emitting a receipt, preserving client-disconnect teardown.
- **Rejected as unsupported by current evidence:** no change was made to infer, repair, or reassemble arbitrary transport-fragmented SSE bytes. The guard's source contract is already logical SSE chunks, while the frontend splits logical event boundaries and the MCP client uses `aiter_lines()`. The regression fixture covers multiple small logical frames; byte-level reassembly remains a Round 2 adapter concern.

## Test coverage added
- Every non-success guard path omits `[DONE]`; content-filter/limit tests assert this alongside the existing ordinary-error regression.
- A naive DONE-only completion model stays incomplete on non-success.
- `GeneratorExit` and `asyncio.CancelledError` propagate unchanged.
- A deliberately infinite counting source proves one-pull limit handling and close invocation; a standard-generator fixture proves close cascades to its upstream source.
- Multiple small logical SSE frames preserve clean-stream behavior.

## Validation
- Final focused stream/MCP union: **129 passed in 17.60s**, including deterministic limit cancellation, outer-to-inner close propagation, and cancellation/GeneratorExit cleanup assertions.
- Ruff passed, compileall passed, and `git diff --check` passed.
- `mypy` and `pyright` are not installed in this environment; no type checker was available.

## Remaining gate risks
- Buffered validation still delays delivery until a clean upstream terminal sequence; limit breaches instead close upstream immediately, so upstream cancellation behavior remains dependent on the provider transport.
- The frontend/MCP consumers still do not render custom stream-error receipts or visibly classify `finish_reason`; omission of `[DONE]` prevents the success sentinel but consumer-visible error UX is a separate change.
- Byte-level SSE reassembly and provider-specific terminal conventions require recorded Round 2 fixtures before broader adapter rollout.
- The requested 125-test union has three unrelated attachment-route failures under the dummy-account environment (401/403 versus the fixture's accepted 200/503); reproduce with an isolated mocked-account configuration before merge.
- Independent review tooling was unavailable in the prior lane and remains an external merge-gate risk.

## Recommendation
**Conditional isolated-branch approval only. Do not deploy.** Require independent review and Round 2 fixture coverage before merge.
