# Notion AI allowance capture implementation plan

1. Prove the current Notion provider contract without exposing credentials.
2. Add failing tests for the exact provider endpoint and allowance persistence.
3. Add `NotionOpusAPI.get_ai_allowance_status()` and strict normalization of rolling/monthly percentages.
4. Add `POST /v1/usage/allowance/refresh?profile_name=...` that persists the observation under the selected profile's canonical account key.
5. Update the Portal Notion AI Usage refresh path to invoke the local capture endpoint and retain stored-observation fallback.
6. Run focused tests, full regression suites, Ruff, compile checks, and `git diff --check` in isolated worktrees.
7. Commit each repository, fan in only to unchanged canonical parents, restart Notion2API and Portal, and verify all governed accounts show current percentages.
8. Push the resolved canonical branches/remotes and verify the resulting remote commits. Treat local merge, runtime restart, remote push, and publication as separate receipts.
