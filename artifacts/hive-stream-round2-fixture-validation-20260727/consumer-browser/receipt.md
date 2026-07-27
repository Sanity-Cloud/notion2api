# Browser consumer terminal-state receipt

- Base: `154e417` on `fix/r2-consumer-browser-terminal-20260727`.
- Product commit: `7a8049b`; reconciliation/test commit: `ac4a96c`.
- Scope: `frontend/js/chat/streaming.js`, bundled `frontend/index.html`, and `frontend/embed.html`; no MCP or server-guard files changed.
- Success contract: exactly one accepted finish reason (`stop`, `length`, `tool_calls`, or `function_call`) followed by `[DONE]`.
- Failure contract: structured `stream_error` / `object:error` / top-level `error`, `finish_reason=error`, `finish_reason=content_filter`, duplicate finish, or EOF before the complete terminal receipt fails closed.
- Partial-content handling: modular and bundled main UI clear partial rendering before raising to the existing error-card path; embed replaces partial rendering with the failure message and does not append an assistant history entry.
- Post-terminal handling: consumers stop reading/processing after `[DONE]`; subsequent frames do not mutate visible content or history.
- Validation: executable Node tests against the real modular parser passed; inline index/embed scripts passed `node --check` after extraction; one Python source-contract test passed; Ruff, compileall, and `git diff --check` passed.
- Runtime dissent: no real browser or HTTP smoke was run in this lane. The browser blocker is cleared for a separately authorized isolated smoke, not for deployment.
