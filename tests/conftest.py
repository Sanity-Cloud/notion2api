from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def isolate_notion_runtime_state(tmp_path, monkeypatch):
    """Prevent tests from writing admission/cache telemetry into live state."""
    from app import model_registry, notion_admission
    from app.model_restriction_cache import ModelRestrictionCache
    from app.notion_admission import NotionAdmissionController
    from app.notion_admission_store import SharedAdmissionStore
    from app.notion_request_telemetry import NotionRequestTelemetryStore

    database = tmp_path / "notion-runtime-state.sqlite3"
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
