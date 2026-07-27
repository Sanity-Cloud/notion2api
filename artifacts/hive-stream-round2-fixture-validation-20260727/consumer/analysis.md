# WU-R2-CONSUMERS analysis

## Scope and method

Base and checkout were verified at `b897d2b4145b21f7aacfeca32af7fef478746d46` on `validate/r2-consumers-20260727`. All replay used offline strings, `httpx.MockTransport`, and local source inspection; no service, network, provider, smoke, merge, or deploy action occurred.

| Consumer | stream_error | finish_reason | content_filter / quarantine | EOF without DONE | DONE | Final result | Partial visible |
|---|---|---|---|---|---|---|---|
| Web UI (`streaming.js`) | ignored | ignored | ignored | resolves stream, ambiguous success | ignored payload | success-like result | yes |
| Embed UI (`embed.html`) | ignored | ignored | ignored | `Ready`; empty says `No visible response received.` | ignored payload | success-like/ambiguous | yes |
| MCP (`post_chat_stream`) | ignored | ignored then force-emits `stop` | ignored; retains delta | `ok=True`, forced `stop` | ignored; parsing continues | false success | yes (progress/final) |

## MCP parser evidence

`Notion2APIClient.post_chat_stream` iterates `response.aiter_lines()` and processes only lines beginning `data:`. It trims each line, ignores blank data and `[DONE]`, skips malformed JSON and non-dict events, accepts only `choices[0].delta`, and never reads `finish_reason`. At end-of-iterator it unconditionally calls final progress and returns `ok=True` with synthesized `finish_reason="stop"`. A multiline SSE JSON event is therefore not reassembled. Offline `httpx.MockTransport` tests cover clean, structured error, content filter, malformed/missing DONE, socket EOF, non-data lines, multiline fragments, and post-DONE content.

## Risks

1. **False success (confirmed, blocking):** MCP synthesizes `stop` for stream error, content filter, malformed terminal, and EOF without DONE.
2. **Silent failure:** both browser consumers discard structured error/hygiene payloads and terminal fields.
3. **Partial-content contamination:** web/embed render deltas before final classification; MCP retains partial output and progresses it before returning success.
4. **UI ambiguity:** web UI resolves on EOF; embed reports `Ready` on nonempty partial EOF and a generic no-visible-response string for empty EOF. Neither waits for a valid terminal. No reproducible infinite wait or stale loading state was observed in these bounded offline replays; the stale-state risk is downstream of an unreadable/hung stream, not a terminal-classification branch.

## Dissent

The browser results are source-faithful data-line replays, not browser smoke tests. The web UI lives in modular code, while embed has independent inline code; they intentionally retain distinct EOF/empty-response presentation. The exact guard/harness terminal signals are represented in the lane fixtures without normalizing their semantics into success.

## Minimal separate patch recommendation

Create a consumer-only patch that tracks a terminal state per response. Recognize `type=stream_error`, `finish_reason=error`, and `finish_reason=content_filter`/hygiene quarantine; do not classify EOF as success unless a valid finish plus DONE terminal is observed. On terminal error, clear/quarantine already rendered or accumulated partial content and present the structured error. In MCP, stop processing after DONE and return an error/indeterminate result instead of synthesizing `stop`.
