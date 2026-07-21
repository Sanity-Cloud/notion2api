# Polling hang debug report — 2026-07-16

## Status

**DONE** — completed-job polling is bounded by default and the full suite passes.

## Symptom

`notion2api_get_chat_job` appeared to hang when a request completed, especially for long responses.

## Root cause

The status tool described its output as bounded, but returned the completed answer three times: `response_text`, `response`, and `raw_job.response`. A persisted 801,784-character answer produced a 4,873,760-byte poll response (6.08× amplification), forcing the MCP client to serialize and ingest several megabytes just to learn that the request completed.

## Fix

- Status polls now return at most a 4,000-character response preview by default.
- `response_chars` reports the full answer size.
- `response_truncated` explicitly identifies a preview.
- `response` is omitted by default.
- `raw_job` no longer duplicates `response` or `response_text`.
- Call `notion2api_get_chat_job(..., include_response=true)` only when the full persisted response is required.

## Evidence

- The same 801,784-character completed request now returns a 6,153-byte status payload and still reports `status="completed"`.
- `tests/test_mcp_server.py` covers bounded and explicit-full-response behavior.
- MCP tests: **29 passed**.
- Full suite: **186 passed**.
- Ruff: pass.
- Compile check: pass.

## Activation

The Notion2API MCP server was restarted and is responding on port 8130.
