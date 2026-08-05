# Notion2API MCP server

This repo includes a thin MCP wrapper around the existing Notion2API HTTP API. The wrapper does not replace the OpenAI-compatible `/v1` API; it runs as a separate MCP server and forwards tool calls to a local Notion2API backend.

## Local run

Start Notion2API first, then run:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-notion2api-mcp.ps1 -BaseUrl http://127.0.0.1:8120
```

Default MCP endpoint:

```text
http://127.0.0.1:8130/mcp
```

For the repo's default Notion2API port, use:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\start-notion2api-mcp.ps1 -BaseUrl http://127.0.0.1:8000
```

## ChatGPT connection

ChatGPT custom connectors expect an MCP endpoint. For local development, expose the local endpoint through OpenAI Secure MCP Tunnel or another HTTPS tunnel, then create a ChatGPT connector pointing at the tunnel-backed `/mcp` endpoint.

## Environment variables

| Variable | Purpose | Default |
| --- | --- | --- |
| `MCP_NOTION2API_BASE_URL` | Backend Notion2API base URL | `http://127.0.0.1:8000` |
| `MCP_NOTION2API_API_KEY` | Bearer token sent to Notion2API | falls back to `NOTION2API_API_KEY` or `API_KEY` |
| `MCP_NOTION2API_TIMEOUT` | Backend request timeout in seconds | `180` |
| `MCP_HOST` | MCP listen host | `127.0.0.1` |
| `MCP_PORT` | MCP listen port | `8130` |
| `MCP_PATH` | Streamable HTTP MCP path | `/mcp` |
| `MCP_TRANSPORT` | `streamable-http`, `stdio`, or `sse` | `streamable-http` |
| `MCP_NOTION2API_CALL_WAIT_SECONDS` | Deprecated compatibility setting; chat submissions return immediately | Ignored |
| `MCP_NOTION2API_MAX_CALL_WAIT_SECONDS` | Deprecated compatibility setting; chat submissions return immediately | Ignored |
| `MCP_NOTION2API_STALL_SECONDS` | No-progress interval before dead-loop suspicion | `180` |
| `MCP_NOTION2API_SESSION_STATE` | Durable named-session state file | `.notion2api_mcp_sessions.json` |
| `MCP_NOTION2API_CHAT_JOB_STATE` | Durable request/job state file | `.notion2api_mcp_chat_jobs.json` |

## Tools

- `notion2api_health`
- `notion2api_list_models`
- `notion2api_chat_history`
- `notion2api_chat`
- `notion2api_chat_completion`
- `notion2api_responses`
- `notion2api_list_sessions`
- `notion2api_get_messages`
- `notion2api_get_last_response`
- `notion2api_get_chat_job`
- `notion2api_cancel_chat_job`
- `notion2api_reset_session`
- `notion2api_rename_session`

### Grouped chat-history tool

`notion2api_chat_history` exposes eight governed actions through one typed tool: `status`, `list_threads`, `get_thread`, `search`, `export_markdown`, `model_stats`, `sync_from_notion`, and `hydrate_thread`.

List and search actions use bounded `limit` and `offset` pagination. Thread reads cap returned messages and process steps with `message_limit`; Markdown export is capped with `content_limit`. Every successful dispatch includes whitelisted account, workspace, teamspace, and governance provenance. Account emails, user IDs, raw credentials, destructive deletion, cleanup, raw-debug export, and arbitrary local database mutation are not exposed.

`sync_from_notion` and `hydrate_thread` are non-destructive, idempotent archive operations. They require an explicit zero-based `account_index` and return partial-result receipts when the backend reports failed sub-operations.


## Client-visible contract snapshots

The reviewed MCP schemas are stored separately for the two runtime profiles:

- `contracts/mcp/notion2api.json` ? plain Notion2API profile, currently 48 tools.
- `contracts/mcp/aigentbee.json` ? AIgentBee-prefixed profile, currently 51 tools, including the three conditional swarm-workbench tools.

The snapshots freeze tool names, descriptions, input and output schemas, required fields, defaults, enum values, annotations, profile-specific metadata, and server instructions. Regenerate them only after reviewing an intentional client-visible change:

```powershell
python scripts/generate_mcp_contract_snapshots.py
python -m pytest -q tests/test_mcp_contract_snapshots.py
```

A process restart is required before a running MCP server advertises changed schemas. Existing ChatGPT or other connector sessions may also retain a stale tool cache and require reconnection or refresh; snapshot validation does not prove that a live client has refreshed.

Current annotation coverage is incomplete for legacy tools. The snapshots preserve that fact as reviewed baseline evidence rather than assigning unverified read-only, destructive, or idempotency semantics. New or changed tools should declare explicit annotations, and a separate governance audit should classify legacy operations before clients rely on annotation-driven authorization.

## Models, sessions, and continuation

The default requested model is the consumer-facing `terra` alias, which resolves to the current Terra backend route. `sol`, `terra`, and `luna` are accepted alongside the longer public model names and canonical Notion route IDs.

Omitting `session_name` creates a descriptive generated session name for new work. The legacy literal `op` is normalized the same way, preventing stale clients from adding new work to the old shared session.

Continue an existing chat with any authoritative identifier:

- reuse its `session_name`;
- pass its local `conversation_id`; or
- pass `continue_from_request_id` from an earlier job.

The model may change on a later turn without changing the local conversation or remote Notion chat. Results expose the local `conversation_id`, the durable `remote_chat_id`/`notion_thread_id`, and the pollable `request_id`.

Chat requests return `pending` immediately. Poll `notion2api_get_chat_job` for the visible response, bounded activity summary, checklist/task state, poll count, and stall indicators. Raw private reasoning is not persisted. A stalled job reports `dead_loop_suspected` and `cancel_recommended`; cancel it explicitly with `notion2api_cancel_chat_job` or set `cancel_if_stalled=true` while polling.

## Notion AI modes, tasks, sources, and personalization

`notion2api_chat`, `notion2api_chat_with_file`, and `notion2api_chat_completion` expose the Notion AI home controls as optional arguments:

| Argument | Values | Behavior |
| --- | --- | --- |
| `mode` | `default`, `ask`, `research` | `default` can search and edit; `ask` is read-only; `research` enables deeper research and web search by default. |
| `task` | `visualize`, `create_slides`, `spreadsheet`, `deep_research` | Selects the matching Notion task preset and enables its artifact capabilities. |
| `sources` | list of source-scope strings | Restricts retrieval to selected sources. Common values are `all`, `notion`, `web`, `notion-help-center`, `github`, `gmail`, `google-calendar`, and `google-drive`. |
| `web_access` | `true`, `false`, or omitted | Explicitly enables/disables web search; omitted uses the selected mode/source default. |
| `persona` | `sidekick`, `minimalist`, `analyst` | Applies Notion's warm, concise, or structured response style for the request. |
| `notion_instructions` | string | Adds per-request operating instructions alongside the task/persona. |

The four task cards contain example prompts, not additional protocol modes. Send those examples as the normal `prompt` while selecting the appropriate `task`.

Example MCP arguments for web-backed legal research:

```json
{
  "prompt": "Compare the current Minnesota statutes governing service of process.",
  "mode": "research",
  "task": "deep_research",
  "sources": ["web"],
  "web_access": true,
  "persona": "analyst"
}
```

Example MCP arguments for a slide deck:

```json
{
  "prompt": "Turn these meeting notes into an executive update deck.",
  "task": "create_slides",
  "sources": ["notion"],
  "notion_instructions": "Use a concise six-slide structure."
}
```

## File attachments and ZIP uploads

Notion2API distinguishes **service-host paths** from **ChatGPT-uploaded files**. These are different trust and transport boundaries.

### Service-host local paths

The `attachments` argument accepts only paths that already exist on the machine running Notion2API:

```json
{
  "prompt": "Review this source package.",
  "attachments": ["X:\\Code\\.ai-runs\\<run-id>\\source.zip"],
  "require_attachments": true
}
```

Do not place ChatGPT `/mnt/data/...` paths in `attachments`. Those paths exist in ChatGPT's sandbox, not on the Windows service host.

### ChatGPT-uploaded files

For one file, call `notion2api_chat_with_file`. This route automatically requires a verified attachment and fails closed when no file bytes arrive.

For multiple files, use the staged workflow:

1. Call `notion2api_stage_file(file=...)` once for each uploaded file.
2. Collect the returned opaque `staged_file_id` values.
3. Call `notion2api_chat(..., staged_file_ids=[...], require_attachments=true)` or `notion2api_chat_completion(...)`.

Staged ids expire after 24 hours by default. Configure `MCP_NOTION2API_STAGED_FILE_TTL_SECONDS` to change the lifetime within the enforced 60-second to 7-day range.

### Attachment provenance and fail-closed behavior

Every chat job and poll result exposes:

- `attachment_required`
- `attachment_count`
- `attachment_transfer_status`: `verified`, `missing`, or `not_requested`
- `attachment_manifest`: redacted names, MIME types, sizes, and sources

A document-grounded workflow must set `require_attachments=true`. The wrapper returns HTTP-style status `422` with `required_attachments_missing` instead of submitting a text-only request when no attachment was verified. A model response must not be described as document-grounded unless `attachment_transfer_status` is `verified` and `attachment_count` is greater than zero.

### Backend policy

The backend must be started with attachment support enabled and a restricted local root:

```powershell
$env:ENABLE_ATTACHMENTS = 'true'
$env:ALLOW_LOCAL_ATTACHMENT_PATHS = 'true'
$env:ATTACHMENT_LOCAL_ROOT = 'X:\Code\.ai-runs'
$env:ATTACHMENT_ALLOWED_MIME_TYPES = 'application/pdf,application/zip,application/x-zip-compressed,text/csv,image/png,image/jpeg,image/gif,image/webp,image/heic'
```

Each validated file is converted into the OpenAI-compatible Notion2API attachment shape and forwarded to `/v1/chat/completions` or `/v1/responses`. ZIP descriptors are normalized to `application/x-zip-compressed` and include `allowUnsupportedTypes: true`.

Successful local staging does not prove successful Notion ingestion. The upstream sequence is descriptor creation, multipart upload, processing, signed URL resolution, and `runInferenceTranscript`. If Notion rejects the final inference after staging, Notion2API reports the stage as `runInferenceTranscript_after_attachment_staging` and classifies deterministic HTTP 400 responses as `UPSTREAM_PROTOCOL_REJECTED`; it does not report document analysis as successful.

## Security note

Do not publish this server directly to the public internet unless you add proper MCP-side authentication. Prefer Secure MCP Tunnel for private local use.


## Concurrency and multi-tool status

Notion2API permits parallel MCP chat jobs across different conversations while serializing unresolved turns within one conversation. The current implementation is designed for one MCP process; cross-process leases, account capacity scheduling, and durable per-tool-call fan-out/fan-in remain separate architecture work.

See [MCP Concurrency and Multi-Tool Evaluation — July 20, 2026](MCP_CONCURRENCY_AND_MULTITOOL_EVALUATION_2026-07-20.md) for the evidence snapshot, supported invariants, test matrix, and prioritized updates.
