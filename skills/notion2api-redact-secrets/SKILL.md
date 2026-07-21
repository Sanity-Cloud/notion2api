---
name: notion2api-redact-secrets
description: Sanitize Notion2API logs, configuration, diagnostics, reports, examples, and MCP outputs. Use before sharing material containing cookies, tokens, API keys, OAuth data, signed URLs, credentials, session identifiers, or private workspace content.
---

# Redact Notion2API Secrets

Read `prompts/notion2api-mcp/n2api-security-redaction.prompt.md` before handling sensitive text.

## Redact

1. Work on a copy; never overwrite the only diagnostic artifact.
2. Remove authorization headers, cookies, tokens, keys, OAuth codes, credentials, private keys, signed URLs, temporary media tokens, and private request/response bodies.
3. Keep operation names, methods, status/error codes, timings, safe paths, and non-secret model names when useful.
4. Use `[REDACTED]` by default. Use `[REDACTED sha256:12hex]` only when stable cross-entry correlation is necessary.
5. Inspect nested JSON, query strings, exception text, attachment manifests, and copied shell output for secondary leaks.
6. Never invent absent lines or claim a secret is safe because it is expired.

Return the redacted report, fields removed, debug value preserved, and residual risk.
