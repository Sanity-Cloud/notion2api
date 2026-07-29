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
documented-output root. The canonical defaults are:

- Teamspace: `3aabf4af-15b3-810f-a1e8-004254c8eb80`
- Ultimate governance authority: `3a8bf4af-15b3-811e-aca0-d011efea6b50`
- Documented-output root: `1f2e3064-f1f9-424d-9892-ca82f88238d7`
- Procedural-feedback root: `3a8bf4af-15b3-81f1-a9bf-ebf67111b1ab`

Environment variables in `.env.example` may override these values as one atomic contract.
Per-request context overrides may not replace the canonical authority page. Project-specific
sources should be supplied as supporting sources, not as a replacement source of truth.
`/health` and `/v1/notion/account_info` expose the active governance receipt.

## Explicit account selection

When more than one account is configured, the backend supports two selection modes:

- `auto`: round-robin rotation with cooldown failover.
- `pinned`: all new requests use one named profile until changed.

Use `GET /v1/notion/accounts` to list safe account metadata, `POST /v1/notion/accounts/switch` to change modes, and `POST /v1/notion/accounts/rollback` to restore the immediately preceding selection. MCP profiles expose equivalent `list_accounts`, `switch_account`, and `rollback_account_switch` tools using their configured tool prefix.

Set `NOTION_ACCOUNT_SELECTION_STATE` to persist the Auto/Pinned choice across restarts. The state file contains only the mode and profile name; credentials remain in the protected account configuration.

Account selection affects new requests. Start a new persistent Notion chat after switching profiles because an existing remote thread remains bound to the account that created it.
