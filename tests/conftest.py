from __future__ import annotations

import os

import pytest


# Test modules import app.config during collection, before fixtures execute.
# Seed security/tool-namespace variables with explicit empty values so the live
# repository .env cannot alter auth or MCP tool names during test imports.
os.environ["API_KEY"] = ""
os.environ["MCP_TOOL_PREFIX"] = ""


@pytest.fixture(autouse=True)
def isolate_notion_runtime_state(tmp_path, monkeypatch):
    """Prevent tests from writing admission/cache telemetry into live state."""
    from app import model_registry, notion_admission
    from app.model_restriction_cache import ModelRestrictionCache
    from app.notion_admission import NotionAdmissionController
    from app.notion_admission_store import SharedAdmissionStore
    from app.notion_request_telemetry import NotionRequestTelemetryStore

    database = tmp_path / "notion-runtime-state.sqlite3"
    # Keep the import-time isolation in force for code that consults the
    # environment dynamically after collection.
    monkeypatch.setenv("API_KEY", "")
    monkeypatch.setenv("MCP_TOOL_PREFIX", "")
    monkeypatch.setenv("NOTION_MODEL_CATALOG_ALLOW_STATIC_SELECTION", "true")
    monkeypatch.setattr(
        notion_admission,
        "_REQUEST_TELEMETRY",
        NotionRequestTelemetryStore(database),
    )
    monkeypatch.setattr(
        notion_admission,
        "_GLOBAL_CONTROLLER",
        NotionAdmissionController(shared_store=SharedAdmissionStore(database)),
    )
    monkeypatch.setattr(
        model_registry,
        "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(database),
    )
    model_registry._RESTRICTED_MODELS_CACHE.clear()
    model_registry._RESTRICTED_MODELS_LOCKS.clear()
