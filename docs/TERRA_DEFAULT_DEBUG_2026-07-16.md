# Terra default debug report — 2026-07-16

## Status

**DONE** — Terra is the visible UI default and the explicit, agent-facing default for every MCP chat tool.

## Why Opus 4.8 was used

The agent explanation that the request omitted a model and the backend fell back to Opus 4.8 was incorrect.

The persisted job record for the affected request contains:

- `model`: `claude-opus4.8`
- `requested_model`: `claude-opus4.8`
- `actual_model`: `ambrosia-tart-high` (the Opus 4.8 backend route)

That proves the caller explicitly supplied Opus 4.8. An omitted model defaults to `terra` in both the FastAPI request schema and the live MCP tool schema.

The likely cause was a cached connector schema or an agent choosing the optional model argument instead of omitting it.

## Fix

- All four MCP agent-call tools retain `default: "terra"`.
- Their model parameter now says: “Omit this argument to use Terra. Only pass another model when the user explicitly requests that model.”
- Tool descriptions repeat the Terra-default rule.
- The served UI visibly initializes as `OpenAI · GPT-5.6 Terra`.
- Regression tests enforce all four MCP schemas and the visible UI default.
- The MCP server was restarted so clients can refresh the schema.

## Verification

- Live UI: `provider=OpenAI model=GPT-5.6 Terra default=terra`.
- MCP schemas: all four agent-call tools report `default=terra` and the omit-model instruction.
- Full suite: **186 passed**.
- Ruff: pass.
- MCP endpoint: responding on port 8130.

Existing clients or agents that cached the old tool schema must reconnect/start a new task before testing again.
