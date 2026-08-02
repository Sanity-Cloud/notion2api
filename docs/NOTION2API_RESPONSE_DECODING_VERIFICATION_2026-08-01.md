# Notion2API Response Parsing and Decoding Verification

- **Date:** 2026-08-01
- **Lifecycle stage:** Validate
- **Branch:** `fix/mcp-response-decoding-validation`
- **Scope:** ChatGPT-facing Notion2API MCP transport, SSE parsing, output integrity, and durable conversation reconstruction.

## Decision

The normal JSON and UTF-8 response path is correct, but two edge-case defects were found and corrected on the validation branch:

1. The MCP stream reader ignored backend `output_hygiene`, `content_replace`, `thinking_replace`, and terminal `content_filter` events.
2. When streamed and authoritative final content differed only by an erroneous whitespace split, the delivered response could be correct while the durable conversation record stored the malformed final body.

## Live evidence

Three completed Sanity Management refresh jobs were compared across:

- MCP job projection
- Raw backend `choices[0].message.content`
- Output-integrity character count and SHA-256
- Durable `conversations.db` assistant message

| Channel | Projection = raw | Integrity receipt valid | Durable substantive body |
|---|---:|---:|---:|
| `sanity-management-account-3` | Yes | Yes | Exact |
| `sanity-management-personaltouch` | Yes | Yes | Exact |
| `sanity-management-math` | Yes | Yes | One historical whitespace mismatch |

The math job delivered `their tool definitions`; the durable row stored `the ir tool definitions`. The characters are ordinary ASCII and prove a final-content selection/persistence divergence rather than a Unicode decoding error.

The generated search-source preamble appears in the MCP projection but is intentionally omitted from the durable conversation body. That difference is structural and not corruption.

## Corrections

- Parse and apply `content_replace` and `thinking_replace` events.
- Preserve backend `output_hygiene` and integrity receipts.
- Fail closed when the backend emits `content_filter` or requires quarantine.
- Do not expose quarantined content as a successful empty response.
- Prefer the exact streamed body when streamed and final text have identical non-whitespace character sequences.
- Preserve the upstream quarantine receipt through terminal job normalization.

## Validation

- Focused parser, stream, hygiene, and integrity suite: **102 passed**.
- Complete repository suite: **489 passed, 6 subtests passed**.
- `py_compile`: passed.
- `ruff check`: passed.
- `git diff --check`: passed.

## Preserved evidence and dissent

The existing malformed math conversation row remains unchanged as incident evidence. Automatically rewriting historical messages would obscure provenance. A separate governed reconciliation should use the verified MCP job payload and record an explicit repair receipt if historical correction is authorized.

## Next gate

Merge, push, restart, and run a controlled Unicode/replacement/quarantine smoke test. Verify that the delivered response, integrity receipt, and newly persisted conversation body are identical after removal of intentionally projected metadata.
