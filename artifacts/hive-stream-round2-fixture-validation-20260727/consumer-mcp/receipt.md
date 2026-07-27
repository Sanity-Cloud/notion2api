# MCP consumer terminal-state receipt

- Worktree: `X:\Code\.worktrees\notion2api-r2-consumer-mcp-20260727`
- Branch: `fix/r2-consumer-mcp-terminal-20260727`
- Scope: MCP `Notion2APIClient.post_chat_stream` only; no browser consumers changed.
- Contract: success requires exactly one accepted finish reason (`stop`, `length`, `tool_calls`, or `function_call`) followed by `[DONE]`; errors and incomplete streams return structured non-success results.
- Tests: `python -m pytest -q tests\stream_round2\consumer_contracts\test_mcp_line_parser_contract.py tests\test_mcp_server.py` — 67 passed in 1.88s.
- Static checks: Ruff passed; compileall passed; `git diff --check` passed; changed Python files verified UTF-8 without BOM.