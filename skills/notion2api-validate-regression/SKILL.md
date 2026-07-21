---
name: notion2api-validate-regression
description: Validate Notion2API fixes and integration changes with focused automated and live checks. Use after changes to HTTP/MCP tools, models, streaming, polling, sessions, persistence, attachments, security, or frontend behavior.
---

# Validate Notion2API Regressions

Read `prompts/notion2api-mcp/n2api-regression-validation.prompt.md` before validation.

## Select checks

1. Run the smallest relevant test file first: `tests/test_mcp_server.py`, `tests/test_notion_protocol_contract.py`, or `tests/test_model_registry.py`.
2. Run `ruff check` on changed Python files.
3. For MCP changes, verify handshake, tool listing, advertised schema, structured bad-input errors, and the exact broken behavior.
4. For sessions/jobs, verify continuation, polling, terminalization, cancellation, and persisted recovery without resubmitting prompts.
5. For streaming/timeouts, verify partial progress and final delivery separately.
6. For attachments, verify manifest redaction and test only with scratch content.
7. For UI changes, test keyboard use, focus, light/dark themes, narrow layout, and the connected API path.
8. Never delete user content or expose secrets during validation.

Return a check matrix with expected, actual, pass/fail, blocking failures, and follow-up patches.
