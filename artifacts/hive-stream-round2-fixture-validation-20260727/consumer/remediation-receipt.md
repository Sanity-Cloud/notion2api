# Round 2 consumer terminal-state remediation receipt

## Accepted contract
Success requires exactly one of `stop`, `length`, `tool_calls`, or `function_call`, followed by `[DONE]`. Structured `stream_error`, `object=error`, `finish_reason=error`, `finish_reason=content_filter`, malformed/duplicate terminals, `DONE` without a successful finish, and EOF before `[DONE]` are non-successes. Consumers stop at `[DONE]` and never append post-DONE bytes.

## Changed files
- `app/mcp_server.py`: tracks finish/DONE/error state, preserves model and thread metadata, returns structured non-success results with bounded partial metadata, and emits final progress only for validated success.
- `frontend/js/chat/streaming.js`: validates terminal state, clears quarantined partial UI, and rejects failed/incomplete streams.
- `frontend/index.html`, `frontend/embed.html`: equivalent minimal terminal tracking; terminal failure leaves a Failed/error presentation rather than Ready.
- `tests/stream_round2/consumer_contracts/test_mcp_line_parser_contract.py`: replaces the strict xfail and covers successful finish variants, failure terminals, EOF, metadata, post-DONE, and final progress.
- `tests/stream_round2/consumer_contracts/test_browser_terminal_contract_source.py`: source-faithful browser contract assertions, not browser smoke.

## Residual dissent
Browser checks are deterministic source-faithful tests only. No service or browser smoke was performed. The inline UIs intentionally retain their own presentation styles.

## Gate status
Consumer terminal blocker is cleared for a subsequent isolated HTTP smoke gate, subject to that gate's own approval and execution. No server-side guard output changed in this lane.
