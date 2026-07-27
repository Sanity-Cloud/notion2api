# Browser consumer terminal-state receipt

- Base: `154e417` on `fix/r2-consumer-browser-terminal-20260727`.
- Scope: browser consumers only; no MCP/server files changed.
- Contract: successful completion requires an accepted finish reason and `[DONE]`; structured errors, malformed terminal payloads, and incomplete EOF fail closed.
- Validation: Node source/contract suite passed (15 assertions); JS syntax check passed; Python compileall and Ruff passed; diff check passed; UTF-8 no-BOM check passed.
- Runtime dissent: no browser smoke was run by instruction. The Node suite is source/contract coverage, not a browser-runtime claim.
