# WU-R2-FRAMING analysis

## Boundary and adapter trace

`StreamProtocolTracker.observe` calls `classify_sse_frame` once per supplied item (`app/stream_protocol.py:188-196`). The classifier states its contract explicitly: one SSE line or one complete `data: ...\n\n` chunk (`:50-54`); it does not retain a pending byte buffer.

* `app.stream_parser.parse_stream` uses `requests.Response.iter_lines(decode_unicode=True)` at `app/stream_parser.py:1106`; this is a logical-line adapter.
* `NotionMcpClient.post_chat_stream` uses `httpx.Response.aiter_lines()` at `app/mcp_server.py:737`, then independently parses each `data:` line at `:738-746`; this is a logical-line adapter.
* `_guard_stream_until_integrity` iterates an already-produced provider iterable at `app/api/chat.py:661-663`, stringifies each yielded application chunk, and sends it directly to the tracker. `StreamingResponse(_guard_stream_until_integrity(...))` is constructed at `app/api/chat.py:1869-1871`, `2192-2194`, and `3154-3156`; it controls downstream response delivery, not raw upstream TCP segmentation.

Therefore TCP boundaries must not be assumed to reach the tracker through those traced adapters. Multiline SSE and byte-fragment fixtures are deliberately preserved as unsupported framing evidence, not normalized into product success.

## Terminal precedence transition table

| prior state | input | observe result | final precedence |
|---|---|---|---|
| no terminal | finish | records `first_finish_sequence` | normal path |
| no terminal | DONE | records `first_done_sequence` | missing-finish if finalized |
| finish recorded | DONE | records done | success if visible output |
| DONE recorded | finish/metadata/content | early return at `observe:200-205`; increments `post_terminal_count`; finish is not recorded | `finalize:252-256` reports `stream_post_terminal_content` before terminal-order check |

The terminal-order branch at `finalize:286-294` requires `first_finish_sequence > first_done_sequence`. That state cannot be produced by normal `observe` transitions because every non-DONE semantic input after DONE returns before assigning a finish sequence. Thus post-terminal content outranks terminal-order invalid by construction. This is safe as a conservative quarantine classification, but contradictory if the intended diagnostic distinction is specifically "DONE before finish". No product patch was made.

## Test evidence

`tests/stream_round2/framing/test_tracker_framing.py` adds 14 deterministic tests for LF/CRLF, boundaries, comments, keepalives, malformed/non-data frames, SSE fields, multi-data events, whitespace, packed events, JSON/UTF-8 fragmentation, connection close, and terminal precedence. The focused run passed: `14 passed in 0.05s`.

## Risks and dissent

* A future adapter that uses `iter_raw`, `aiter_raw`, or arbitrary socket chunks would require a lower incremental SSE/UTF-8 assembler before the tracker.
* Dissent: treating a complete multi-line SSE event as a single tracker input is valid SSE, so it may merit support if the tracker boundary is intentionally widened. The traced current adapters do not establish that requirement.
* No defect is confirmed at the current boundary; no failing product test or application patch recommendation is included.

## Recommendation

Keep the current tracker contract and evidence classifications. If lower-level bytes are ever passed to the guard, introduce a tested transport assembler below the tracker rather than changing receipt semantics opportunistically.
