---
name: notion2api-write-output-schema
description: Create or repair predictable structured output models and JSON schemas for Notion2API MCP tools. Use when adding a tool, changing a response model, fixing client compatibility, or reviewing success and error shapes.
---

# Write Notion2API Output Schemas

Read `prompts/notion2api-mcp/n2api-mcp-output-schema-writer.prompt.md` before designing a schema.

## Workflow

1. Inspect the endpoint response and its callers before changing `app/mcp_server.py`.
2. Reuse existing Pydantic output models and shared fields; add a new model only when no existing shape fits.
3. Keep identifiers needed for follow-up actions, but exclude credentials, raw authorization data, private reasoning, and unbounded payloads.
4. Preserve structured success and error information, retryability, diagnostics, and pagination where applicable.
5. Register tools with `structured_output=True` and a concrete return annotation.
6. Add or update the smallest schema assertion in `tests/test_mcp_server.py`.

Run `pytest tests/test_mcp_server.py` and `ruff check app/mcp_server.py tests/test_mcp_server.py`.
