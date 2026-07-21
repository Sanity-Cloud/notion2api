---
name: notion2api-route-tools
description: Classify Notion2API requests and choose the minimum safe HTTP endpoint, MCP tool, or prompt. Use when a request could be read, chat, write, update, debug, schema, admin, export, session, attachment, or continuation work.
---

# Route Notion2API Tools

Read `prompts/notion2api-mcp/n2api-mcp-tool-router.prompt.md` for the canonical intent classes.

## Route

1. Prefer a read-only action when it answers the request.
2. For normal inference, choose `notion2api_chat`; use the specialized chat, Responses, attachment, or upload tool only when its extra behavior is required.
3. For polling or recovery, choose `notion2api_get_chat_job`, `notion2api_get_messages`, or `notion2api_get_last_response`; never send a duplicate prompt.
4. For session administration, use list, rename, reset, cancel, or unsafe-URL continuation only when the user intent requires it.
5. Route code changes to the narrow skill: `$notion2api-debug-provider`, `$notion2api-write-output-schema`, `$notion2api-validate-regression`, `$notion2api-redact-secrets`, or `$notion2api-build-ui`.

State the chosen intent, minimum action set, required inputs, and any destructive action needing confirmation.
