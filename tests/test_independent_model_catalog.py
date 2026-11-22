"""UI-evidence catalog is independent of Notion model discovery and fail-closed."""

from datetime import date
from types import SimpleNamespace

import pytest

from app.independent_model_catalog import curated_ui_envelope
from app.model_catalog import ModelSelectionError
from app import model_registry
from app.workspace_routing import SANITY_MONKEY, SANITY_MANAGEMENT


def test_curated_catalog_transcribes_workspace_evidence():
    snapshot = curated_ui_envelope(
        model_registry._static_catalog_records(), today=date(2026, 11, 22)
    )
    models = {m["display_name"]: m for m in snapshot.snapshot["models"]}
    assert snapshot.source == "curated_ui"
    assert snapshot.stale_policy_exceeded is False
    assert len(models) == 35
    assert models["GPT-6.2 Sol"]["canonical_id"] == "unverified:gpt-6-2-sol"
    assert models["GPT-6.2 Sol"]["previously_observed_route_id"] == ""
    assert models["GPT-6 Astra"]["is_disabled"] is True
    assert models["GPT-6 Astra"]["permission_status"] == "restricted"
    assert models["GPT-6.1 Sol"]["canonical_id"] == "unverified:gpt-6-1-sol"
    assert models["GPT-6.1 Sol"]["previously_observed_route_id"] == ""
    assert models["GPT-6.1 Sol"]["routes"] == {}
    assert models["GPT-6.1 Sol"]["model_card_attributes"] is None
    assert models["Kimi K3"]["permission_status"] == "unverified"
    assert models["Grok 4.3"]["selected_in_picker"] is True
    assert all(m["route_verified"] is False for m in models.values())
    assert all(m["selectable"] is False for m in models.values())
    assert all(
        not r["supported"]
        for m in models.values()
        for r in m["routes"].values()
    )


def test_curated_mode_does_not_contact_notions_model_picker(monkeypatch):
    monkeypatch.setenv("NOTION_MODEL_CATALOG_SOURCE", "curated_ui")
    monkeypatch.setattr(
        model_registry,
        "curated_ui_envelope",
        lambda records: curated_ui_envelope(records, today=date(2026, 11, 22)),
    )
    class NoPicker:
        space_id = SANITY_MONKEY.contract.workspace_id

        def get_ai_model_picker_config(self):
            raise AssertionError("The Notion model-picker API was called")

    client = NoPicker()
    result = model_registry.get_model_catalog_for_client(client)
    assert result.source == "curated_ui"
    assert result.snapshot["catalog_evidence"] == (
        "user_confirmed_current_workspace_picker_screenshots"
    )
    with pytest.raises(ModelSelectionError) as caught:
        model_registry.resolve_model_selection(client, "GPT-6.2 Sol")
    assert caught.value.code == "model_route_unverified"

    with pytest.raises(ModelSelectionError) as caught:
        model_registry.resolve_model_selection(client, "GPT-6.1 Sol")
    assert caught.value.code == "model_route_unverified"

    with pytest.raises(ModelSelectionError) as caught:
        model_registry.resolve_model_selection(client, "GPT-6 Astra")
    assert caught.value.code == "model_disabled"

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                account_pool=SimpleNamespace(
                    get_metadata_client=lambda: client,
                    get_client=lambda **_: (_ for _ in ()).throw(
                        AssertionError("UI discovery must not rotate accounts")
                    ),
                )
            )
        )
    )
    entries, receipt = model_registry.list_model_metadata_for_request(request)
    assert receipt["catalog_source"] == "curated_ui"
    assert receipt["catalog_authority"] == "workspace_ui_display_evidence_only"
    assert receipt["catalog_routes_verified"] is False
    assert receipt["catalog_visible_model_count"] == 35
    assert receipt["catalog_unverified_route_count"] == 35
    assert receipt["catalog_model_selection_restricted"] is None
    assert receipt["alias_reconciliation"]["active_alias_count"] == 0
    assert len(entries) == 35


def test_curated_mode_rejects_other_workspace(monkeypatch):
    monkeypatch.setenv("NOTION_MODEL_CATALOG_SOURCE", "curated_ui")
    foreign = SimpleNamespace(space_id=SANITY_MANAGEMENT.contract.workspace_id)
    with pytest.raises(model_registry.ModelCatalogUnavailable, match="not verified"):
        model_registry.get_model_catalog_for_client(foreign)
    with pytest.raises(model_registry.ModelCatalogUnavailable, match="not verified"):
        model_registry.get_model_catalog_for_client(SimpleNamespace())


def test_discovery_mode_is_independent_from_selection(monkeypatch):
    monkeypatch.setenv("NOTION_MODEL_CATALOG_SOURCE", "notion_picker")
    monkeypatch.setenv("NOTION_MODEL_CATALOG_DISCOVERY_SOURCE", "curated_ui")
    monkeypatch.setattr(
        model_registry, "curated_ui_envelope",
        lambda records: curated_ui_envelope(records, today=date(2026, 11, 22)),
    )

    class Client:
        space_id = SANITY_MONKEY.contract.workspace_id

        def get_ai_model_picker_config(self):
            raise AssertionError("Notion picker called during UI discovery")

    client = Client()
    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                account_pool=SimpleNamespace(
                    get_metadata_client=lambda: client,
                    get_client=lambda **_: (_ for _ in ()).throw(
                        AssertionError("health-routed client should not be read")
                    ),
                )
            )
        )
    )
    entries, receipt = model_registry.list_model_metadata_for_request(request)
    assert len(entries) == 35
    assert receipt["catalog_source"] == "curated_ui"
    assert all(entry["canonical_id"].startswith("unverified:") for entry in entries)

    class PickerService:
        def __init__(self, _cache):
            pass

        def get(self, *_args, **_kwargs):
            raise AssertionError("selection still uses the separately configured source")

    monkeypatch.setattr(model_registry, "ModelCatalogService", PickerService)
    with pytest.raises(AssertionError, match="selection still uses"):
        model_registry.resolve_model_selection(client, "GPT-6.1 Sol")


def test_ui_observation_expiration_is_explicit():
    envelope = curated_ui_envelope(
        model_registry._static_catalog_records(), today=date(2026, 12, 1)
    )
    assert envelope.source == "curated_ui"
    assert envelope.stale is True
    assert envelope.stale_policy_exceeded is True
    assert envelope.snapshot["catalog_observed_on"] == "2026-11-22"


def test_unrecognized_catalog_source_is_rejected(monkeypatch):
    monkeypatch.setenv("NOTION_MODEL_CATALOG_SOURCE", "invalid-source")
    with pytest.raises(model_registry.ModelCatalogUnavailable):
        model_registry.get_model_catalog_for_client(SimpleNamespace())
