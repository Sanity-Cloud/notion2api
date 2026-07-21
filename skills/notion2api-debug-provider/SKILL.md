---
name: notion2api-debug-provider
description: Diagnose Notion2API HTTP, MCP, Notion provider, model-routing, timeout, polling, streaming, persistence, authentication, 502/503, empty-content, and stalled-job failures. Use for incident investigation and root-cause analysis.
---

# Debug Notion2API Provider Failures

Read `prompts/notion2api-mcp/n2api-provider-debugger.prompt.md` for failure classes and reporting format.

## Diagnose

1. Reproduce the failing surface and record the path/tool, status, timestamp, requested model, `request_id`, and whether streaming was used.
2. Check `/health`, then separate backend failure from MCP wrapper, frontend, or client timeout failure.
3. Trace every caller before editing shared chat, stream, session, attachment, or persistence code.
4. Inspect bounded job state with `notion2api_get_chat_job`; use persisted messages/last response to distinguish slow work from lost delivery.
5. Verify model metadata, remote thread binding, timeout settings, and terminal job state before changing concurrency or retry behavior.
6. Redact secrets before showing logs. Preserve conversations and job files unless deletion is explicitly authorized.
7. Fix the root cause at the shared boundary and leave one focused regression check.

Report finding, evidence, minimal fix, validation, and unsafe workarounds to avoid.
