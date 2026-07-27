# Terminal-contract remediation evidence

## Scope and isolation
- Mission: `hive-stream-controlled-pilot-deploy-20260727`.
- Base: `a15def1`.
- Isolated branch/worktree: `fix/hive-stream-terminal-remediation-20260727` at `X:\Code\.worktrees\notion2api-hive-stream-terminal-remediation-20260727`.
- Production ports and the shared checkout were not modified, merged, or deployed.

## Root cause
The candidate guard buffered and replayed a non-quarantined upstream stream verbatim. It did not require exactly one accepted `finish_reason` followed by exactly one `[DONE]`. Therefore a truncated upstream could still receive HTTP 200 and reach the browser; a strict browser consumer then correctly failed it as `incomplete_terminal_state`.

The direct `orchid-muffin` result proves only the valid path. The durable pilot result for browser `terra` proves the missing-terminal path occurred. The operations drawer is injected by `app/server.py`, and its wrapper calls `original.call(this, chat, model, aiWrapper, attachments)`; it neither changes the request payload nor consumes SSE frames.

## Minimal change
`_guard_stream_until_integrity` now validates the buffered clean path before replaying it:
1. exactly one of `stop`, `length`, `tool_calls`, or `function_call`;
2. exactly one `[DONE]` after that finish frame; and
3. no frames after `[DONE]`.

An invalid terminal sequence suppresses all buffered content and emits a structured SSE error (`incomplete_terminal_state`), an `error` finish frame, and `[DONE]`. Quarantine and buffer-limit fail-closed paths are unchanged.

## Regression coverage
`tests/test_stream_integrity_guard.py` checks:
- clean stop-plus-DONE passthrough;
- quarantine and buffer-limit suppression;
- the deployed browser POST shape including headers, attachments, and `metadata.persist_remote_chat`;
- operations-drawer injection and the four-argument delegating wrapper;
- a 200 body with no terminal receipt becoming an explicit terminal error with no leaked content; and
- reordered and duplicate terminal frames being rejected.

## Validation
Executed in the isolated worktree with a non-secret synthetic account configuration:

```text
pytest -q tests/test_stream_integrity_guard.py
5 passed in 0.32s
python -m compileall -q app
 git diff --check
```

No live provider, browser, production, merge, or deployment action was performed.
