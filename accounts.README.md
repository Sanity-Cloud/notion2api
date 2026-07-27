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

- Teamspace: `fe8b13aa-3ad2-811e-8292-0003b78a02f9`
- Ultimate governance authority: `3a8bf4af-15b3-811e-aca0-d011efea6b50`
- Documented-output root: `1f2e3064-f1f9-424d-9892-ca82f88238d7`
- Procedural-feedback root: `3a8bf4af-15b3-81f1-a9bf-ebf67111b1ab`

Environment variables in `.env.example` may override these values as one atomic contract.
Per-request context overrides may not replace the canonical authority page. Project-specific
sources should be supplied as supporting sources, not as a replacement source of truth.
`/health` and `/v1/notion/account_info` expose the active governance receipt.
