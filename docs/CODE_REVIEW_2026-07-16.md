# Code review — 2026-07-16

## Applied change

Terra is now the default across every active interface:

- `app/model_registry.py:402` — backend registry default was already `terra`.
- `app/schemas.py:33` — OpenAI-compatible request default was already `terra`.
- `app/mcp_server.py:45` — MCP tool default was already `terra`.
- `frontend/index.html:817` — changed the served browser UI default to `terra` and added its label/provider/selector entry.
- `frontend/js/core/constants.js:107` — changed the modular UI constants default to `terra` and added its label/icon/selector entry.

`terra` resolves to Notion's `orchid-muffin` route and is displayed as **GPT-5.6 Terra**.

## Fixed issues

1. **MCP terminal-job resume failure.** Existing jobs now load their persisted `baseline_message_id` before the no-response fallback uses it. `tests/test_mcp_server.py` covers the previously reproducible `UnboundLocalError` path.

2. **Pytest collection failure.** `pyproject.toml` limits collection to `tests/`, so executable `scripts/test_*.py` files are no longer imported as tests.

3. **Static-analysis failures.** The undefined name, two unused imports, and two unused variables are removed. Ruff is configured for the deployed Python 3.11 runtime.

4. **Pydantic v2 deprecation.** The request path now uses `model_dump()` instead of deprecated `dict()`.

5. **Unreproducible installs.** Runtime dependencies are pinned in `requirements.txt`; `requirements-dev.txt` pins pytest and Ruff.

6. **Frontend default drift.** Both frontend representations select Terra, and `tests/test_model_registry.py` prevents the defaults and selector entries from drifting again.

## Complexity review

`frontend/js/:L1: delete: 22 modular JavaScript files (5,963 lines) are not referenced by the served index; frontend/index.html carries 1,437 lines of inline JavaScript instead. Keep one source of truth—either delete the unused modules or generate the bundled index from them.`

This duplication caused the Terra default to drift between backend, modular frontend, and the served page. It remains an architectural cleanup rather than a safe mechanical deletion: the modular files may be maintenance sources even though the served page is self-contained.

`net: -5,963 lines possible.`

## Verification

- `python -m pytest -q` — **186 passed**.
- `python -m ruff check app main.py login.py tests/test_mcp_server.py tests/test_model_registry.py` — pass.
- `python -m compileall -q app main.py login.py tests` — pass.
- Runtime and development dependency dry-runs — pass.
- `git diff --check` — pass.

## Recommended order

1. Add the existing Ruff and pytest commands to CI.
2. Choose one frontend source of truth when the deployment/bundling contract is explicit.
3. Audit broad exception handlers incrementally alongside tests for each affected integration path; a mass replacement would change resilience behavior without proving safety.
