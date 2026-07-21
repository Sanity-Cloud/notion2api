---
name: notion2api-operate
description: Operate the Notion2API HTTP API and MCP server safely. Use for health checks, model discovery, chat and responses calls, durable sessions, polling, continuation, cancellation, file attachments, page uploads, and reading persisted responses.
---

# Operate Notion2API

Read `prompts/notion2api-mcp/n2api-mcp-operator.prompt.md` before acting; it is the canonical safety policy.

## Workflow

1. Check `notion2api_health` before a live workflow and use `notion2api_list_models` only when model choice matters.
2. Use `notion2api_chat` for normal prompts, `notion2api_chat_with_file` for one client-uploaded file, `notion2api_chat_completion` for explicit message arrays, and `notion2api_responses` only for Responses API compatibility.
3. Omit `model` unless the user requests one; Terra is the server default.
4. Reuse `session_name`, `conversation_id`, or `continue_from_request_id` for follow-ups. Never invent a remote thread ID.
5. Treat `request_id` as one retry-safe operation. Poll it with `notion2api_get_chat_job`; do not resubmit the prompt to check progress.
6. Confirm completion only when `ok=true` and response text/content is present. Use `notion2api_get_last_response` after client timeouts.
7. Cancel only obsolete or stuck jobs. Reset, rename, upload, or unsafe-URL continuation only when explicitly needed.

Return the result, safe object/session identifiers, validation, and any caveat. Never expose credentials, raw private reasoning, or unredacted payloads.
