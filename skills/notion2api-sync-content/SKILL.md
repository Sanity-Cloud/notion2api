---
name: notion2api-sync-content
description: Create, append, update, or synchronize Notion content through Notion2API with idempotent and non-destructive behavior. Use for pages, databases, blocks, notes, reports, uploads, and repeatable content syncs.
---

# Sync Notion Content

Read `prompts/notion2api-mcp/n2api-notion-content-sync.prompt.md` before writing.

## Workflow

1. Resolve the target page, database, parent, or workspace section. Do not guess an ambiguous destination.
2. Read the target before updating. Search by stable title, source URL, project key, or external ID before creating.
3. Append to logs and runbooks; preserve existing blocks and database properties unless replacement is explicit.
4. Include a compact source note with project, date, and reason when generating content.
5. Use `notion2api_upload_file_to_page` only for a local file intended for a known page; use chat attachment tools for model input.
6. Stop on partial or empty upstream responses instead of creating duplicates.
7. Re-read the changed object or use returned identifiers to validate success.

Return created, updated, skipped, conflicts, and validation. Do not dump raw Notion JSON unless debugging was requested.
