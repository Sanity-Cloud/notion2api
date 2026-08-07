# accounts.json Usage

Use the local login helper to create or refresh account configuration:

```bash
python login.py
```

The helper is intended for local use only. It creates the account fields required by the API service and writes them to local configuration files that must not be committed.

## Security notes

- Do not commit `accounts.json`, `.env`, browser cookies, or session tokens.
- Keep account configuration files in `.gitignore`.
- If a login session expires, rerun the local login helper and restart the service.
- Prefer separate low-privilege accounts for automation.

## Multiple account format

```json
[
  { "profile_name": "default", "token_v2": "...", "space_id": "...", "user_id": "...", "space_view_id": "...", "user_name": "...", "user_email": "..." },
  { "profile_name": "backup", "token_v2": "...", "space_id": "...", "user_id": "...", "space_view_id": "...", "user_name": "...", "user_email": "..." }
]
```

The first healthy account is treated as primary. Failed accounts rotate to the next account after a short cooldown.

## SanityCloud governance alignment

At startup, every configured account is bound to the same non-secret governance contract.
The service fails closed when an account points to another teamspace, authority context, or
documented-output root. The sole default authority is the **Sanity Management** workspace
and its **Sanity-Cloud-InScene** teamspace:

- Workspace: `fe8b13aa-3ad2-811e-8292-0003b78a02f9`
- Teamspace: `3acb13aa-3ad2-8176-9ff3-004220d4868f`
- Ultimate governance authority: `3acb13aa-3ad2-816c-b6ec-d4930a87e12e`
- Documented-output root: `3acb13aa-3ad2-8161-b851-f9bb30c31ecc`
- Procedural-feedback root: `3acb13aa-3ad2-813f-81c9-c659c579c093`

The former SanityCloud-HQ teamspace is named **Sanity-Cloud-InScene-Deprecated** and is
not enabled by default. Historical access requires explicit inclusion of `sanitycloud-hq`
in `SANITYCLOUD_ENABLED_WORKSPACES`; it cannot be selected as the default workspace.

Environment variables in `.env.example` may override these values as one atomic contract.
Per-request context overrides may not replace the canonical authority page. Project-specific
sources should be supplied as supporting sources, not as a replacement source of truth.
`/health` and `/v1/notion/account_info` expose the active governance receipt.

## Explicit account selection

When more than one account is configured, the backend supports two selection modes:

- `auto`: health/load/quota-aware selection among capacity roles (Alpha/Beta production peers, bounded Canary fraction, Dev excluded unless a development/test workload requests it).
- `pinned`: all new requests use one named profile or capacity alias (Alpha/Beta/Canary/Dev) until changed.

Use `GET /v1/notion/accounts` to list safe account metadata (including alias, role, health score/reason, inflight/queue pressure, and routing evidence), `POST /v1/notion/accounts/switch` to change modes, and `POST /v1/notion/accounts/rollback` to restore the immediately preceding selection. MCP profiles expose equivalent `list_accounts`, `switch_account`, and `rollback_account_switch` tools using their configured tool prefix.

Set `NOTION_ACCOUNT_SELECTION_STATE` to persist the Auto/Pinned choice across restarts. The state file contains only the mode and profile name; credentials remain in the protected account configuration.

Optional `NOTION_CANARY_ROUTE_FRACTION` (default `0.1`) controls the bounded fraction of ordinary eligible new work that may land on Canary.

Account selection affects new requests. Persistent Notion chats remain sticky to the original `workspace_id + user_id` binding and are not migrated when selection changes.

## Capacity roles

Operational aliases (not credential renames):

- Account 1 → Alpha
- Account 2 → Beta
- Account 3 → Canary
- Account 4 → Dev

City, NotebookLM, governance-domain, and task-domain labels are workload metadata only and never permanently own an account.

## Cursor custom agents

Cursor agents are scoped by `(account_key, workspace_id)` and store Bitwarden secret references only (`cursor_api_key_secret_id`). Raw Cursor API keys must never appear in `accounts.json`, registry rows, logs, or MCP outputs.
