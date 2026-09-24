from __future__ import annotations

import asyncio
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import model_registry
from app.api.chat import _validate_request_model_selection
from app.api.models import list_models
from app.conversation import apply_notion_ai_options, build_lite_transcript
from app.model_catalog import (
    ModelCatalogService,
    ModelCatalogUnavailable,
    ModelCatalogValidationError,
    ModelSelectionError,
    parse_picker_catalog,
    resolve_reasoning_effort,
)
from app.model_restriction_cache import ModelRestrictionCache
from app.schemas import ChatCompletionRequest, ChatMessage


def _picker_payload() -> dict:
    return {
        "restrictedGeoPolicyApplied": False,
        "restrictedAccessModelsInPickerConfig": [
            {
                "codename": "acai-budino",
                "modelMessage": "Fable 5",
                "modelFamily": "anthropic",
                "disabledReason": "business_or_enterprise_plan_required",
            }
        ],
        "models": [
            {
                "model": "orchid-muffin",
                "modelMessage": "GPT-5.6 Terra",
                "modelFamily": "openai",
                "modelProvider": "openai",
                "displayGroup": "intelligent",
                "modelConfiguration": {
                    "supportedReasoningEfforts": [
                        "none",
                        "low",
                        "medium",
                        "high",
                        "xhigh",
                        "max",
                    ],
                    "defaultReasoningEffort": "medium",
                },
                "isDisabled": False,
                "isApproachingRateLimit": False,
                "modelCardAttributes": {
                    "speed": 4,
                    "intelligence": 4,
                    "cost": 4,
                },
                "workflow": {"finalModelName": "orchid-muffin", "beta": True},
                "customAgent": {"finalModelName": "orchid-muffin", "beta": True},
                "agentService": {"finalModelName": "orchid-muffin", "beta": True},
            },
            {
                "model": "olive-jellyroll",
                "modelMessage": "GPT-5.6 Luna",
                "modelFamily": "openai",
                "modelProvider": "openai",
                "displayGroup": "fast",
                "modelConfiguration": {
                    "supportedReasoningEfforts": ["none", "low", "medium", "high"],
                    "defaultReasoningEffort": "medium",
                },
                "isDisabled": False,
                "modelCardAttributes": {
                    "speed": 5,
                    "intelligence": 3,
                    "cost": 2,
                },
                "agentService": {"finalModelName": "olive-jellyroll", "beta": True},
            },
            {
                "model": "acai-budino-high",
                "modelMessage": "Fable 5",
                "modelFamily": "anthropic",
                "modelProvider": "anthropic",
                "displayGroup": "intelligent",
                "modelConfiguration": {
                    "supportedReasoningEfforts": ["low", "medium", "high", "max"],
                    "defaultReasoningEffort": "high",
                },
                "isDisabled": False,
                "restrictedAccessModelCodename": "acai-budino",
                "modelCardAttributes": {
                    "speed": 2,
                    "intelligence": 5,
                    "cost": 5,
                },
                "workflow": {"finalModelName": "acai-budino-high", "beta": True},
                "customAgent": {"finalModelName": "acai-budino-high", "beta": True},
                "agentService": {"finalModelName": "acai-budino-high", "beta": True},
            },
        ],
    }


class PickerClient:
    def __init__(self, payload: dict | None = None, *, space_id: str = "space-global") -> None:
        self.space_id = space_id
        self.payload = payload if payload is not None else _picker_payload()
        self.calls = 0
        self.error: Exception | None = None

    def get_ai_model_picker_config(self) -> dict:
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.payload


def test_parse_picker_catalog_preserves_effort_routes_ratings_and_restrictions() -> None:
    catalog = parse_picker_catalog(_picker_payload())
    terra = next(model for model in catalog["models"] if model["canonical_id"] == "orchid-muffin")
    fable = next(model for model in catalog["models"] if model["canonical_id"] == "acai-budino-high")

    assert terra["supported_reasoning_efforts"] == [
        "none",
        "low",
        "medium",
        "high",
        "xhigh",
        "max",
    ]
    assert terra["default_reasoning_effort"] == "medium"
    assert terra["routes"]["workflow"] == {
        "final_model_name": "orchid-muffin",
        "beta": True,
        "supported": True,
    }
    assert terra["model_card_attributes"] == {
        "speed": 4,
        "intelligence": 4,
        "cost": 4,
    }
    assert fable["restricted_access_model_codename"] == "acai-budino"
    assert fable["is_disabled"] is True
    assert fable["disabled_reason"] == "business_or_enterprise_plan_required"
    assert len(catalog["snapshot_sha256"]) == 64


def test_parse_picker_catalog_isolates_invalid_model_entries() -> None:
    payload = _picker_payload()
    payload["models"][0]["modelConfiguration"]["defaultReasoningEffort"] = "ultra"
    payload["models"].append("not-a-model-record")
    payload["models"].append(dict(payload["models"][1]))

    catalog = parse_picker_catalog(payload)

    assert [model["canonical_id"] for model in catalog["models"]] == [
        "olive-jellyroll",
        "acai-budino-high",
    ]
    assert catalog["upstream_model_count"] == 5
    assert catalog["rejected_model_count"] == 3
    assert catalog["rejected_models"][0]["canonical_id"] == "orchid-muffin"
    assert "default effort" in catalog["rejected_models"][0]["error"]
    assert catalog["rejected_models"][1]["canonical_id"] == ""
    assert "must be an object" in catalog["rejected_models"][1]["error"]
    assert catalog["rejected_models"][2]["canonical_id"] == "olive-jellyroll"
    assert "Duplicate" in catalog["rejected_models"][2]["error"]


def test_authoritative_catalog_cache_is_global_to_workspace(tmp_path: Path) -> None:
    cache = ModelRestrictionCache(tmp_path / "catalog.sqlite3")
    service = ModelCatalogService(cache)
    first = PickerClient(space_id="shared-workspace")
    second = PickerClient(space_id="shared-workspace")

    live = service.get(first)
    cached = service.get(second)

    assert live.source == "authoritative_live"
    assert cached.source == "authoritative_cache"
    assert first.calls == 1
    assert second.calls == 0
    assert live.snapshot["snapshot_sha256"] == cached.snapshot["snapshot_sha256"]


def test_catalog_uses_last_known_good_after_stale_policy_threshold(
    monkeypatch, tmp_path: Path
) -> None:
    now = [100.0]
    monkeypatch.setenv("NOTION_MODEL_CATALOG_CACHE_TTL_SECONDS", "5")
    monkeypatch.setenv("NOTION_MODEL_CATALOG_MAX_STALE_SECONDS", "20")
    cache = ModelRestrictionCache(tmp_path / "catalog.sqlite3", clock=lambda: now[0])
    service = ModelCatalogService(cache, clock=lambda: now[0])
    client = PickerClient()

    live = service.get(client)
    original_sha = live.snapshot["snapshot_sha256"]
    assert live.source == "authoritative_live"

    now[0] = 106.0
    client.error = RuntimeError("upstream unavailable")
    stale = service.get(client)
    assert stale.source == "last_known_good"
    assert stale.stale is True
    assert stale.stale_policy_exceeded is False
    assert stale.age_seconds == 6.0
    assert stale.snapshot["snapshot_sha256"] == original_sha
    assert "upstream unavailable" in stale.upstream_error

    now[0] = 121.0
    expired = service.get(client)
    assert expired.source == "last_known_good"
    assert expired.stale is True
    assert expired.stale_policy_exceeded is True
    assert expired.snapshot["snapshot_sha256"] == original_sha
    receipt = expired.receipt()
    assert receipt["catalog_fallback"] is True
    assert receipt["catalog_stale_policy_exceeded"] is True
    assert "upstream unavailable" in receipt["catalog_fallback_reason"]


def test_empty_restricted_refresh_preserves_last_known_good(
    monkeypatch, tmp_path: Path
) -> None:
    now = [100.0]
    monkeypatch.setenv("NOTION_MODEL_CATALOG_CACHE_TTL_SECONDS", "1")
    monkeypatch.setenv("NOTION_MODEL_CATALOG_MAX_STALE_SECONDS", "2")
    cache = ModelRestrictionCache(tmp_path / "restricted-empty.sqlite3", clock=lambda: now[0])
    service = ModelCatalogService(cache, clock=lambda: now[0])
    client = PickerClient()

    live = service.get(client)
    original_sha = live.snapshot["snapshot_sha256"]

    now[0] = 105.0
    client.payload = {
        "modelSelectionRestricted": True,
        "models": [],
        "restrictedAccessModelsInPickerConfig": [],
        "restrictedGeoPolicyApplied": False,
    }
    fallback = service.get(client)

    assert fallback.source == "last_known_good"
    assert fallback.stale is True
    assert fallback.stale_policy_exceeded is True
    assert fallback.snapshot["snapshot_sha256"] == original_sha
    assert "no usable models" in fallback.upstream_error
    assert "restricted" in fallback.upstream_error.lower()


def test_catalog_refresh_atomically_replaces_removed_and_new_models(
    monkeypatch, tmp_path: Path
) -> None:
    now = [100.0]
    monkeypatch.setenv("NOTION_MODEL_CATALOG_CACHE_TTL_SECONDS", "1")
    cache = ModelRestrictionCache(tmp_path / "refresh.sqlite3", clock=lambda: now[0])
    service = ModelCatalogService(cache, clock=lambda: now[0])
    client = PickerClient()

    first = service.get(client)
    assert {model["canonical_id"] for model in first.snapshot["models"]} == {
        "orchid-muffin",
        "olive-jellyroll",
        "acai-budino-high",
    }

    updated = _picker_payload()
    terra = updated["models"][0]
    updated["models"] = [
        terra,
        {
            "model": "future-reasoner-v1",
            "modelMessage": "Future Reasoner 1",
            "modelFamily": "future-family",
            "modelProvider": "future-provider",
            "modelConfiguration": {
                "supportedReasoningEfforts": ["low", "high"],
                "defaultReasoningEffort": "low",
            },
            "workflow": {"finalModelName": "future-reasoner-v1", "beta": True},
        },
    ]
    now[0] = 102.0
    client.payload = updated

    refreshed = service.get(client)
    assert refreshed.source == "authoritative_live"
    assert {model["canonical_id"] for model in refreshed.snapshot["models"]} == {
        "orchid-muffin",
        "future-reasoner-v1",
    }
    persisted = cache.get(service.cache_key(client.space_id))
    assert persisted is not None
    assert persisted["payload"]["snapshot_sha256"] == refreshed.snapshot["snapshot_sha256"]
    assert persisted["payload"]["snapshot_sha256"] != first.snapshot["snapshot_sha256"]


def test_catalog_without_live_or_lkg_fails_closed(tmp_path: Path) -> None:
    service = ModelCatalogService(ModelRestrictionCache(tmp_path / "catalog.sqlite3"))
    client = PickerClient()
    client.error = RuntimeError("offline")

    with pytest.raises(ModelCatalogUnavailable, match="no valid last-known-good"):
        service.get(client)


def test_reasoning_effort_validation_is_exact_and_model_specific() -> None:
    terra = parse_picker_catalog(_picker_payload())["models"][0]

    explicit = resolve_reasoning_effort(terra, "high")
    assert explicit["resolved_reasoning_effort"] == "high"
    assert explicit["reasoning_effort_source"] == "explicit"

    defaulted = resolve_reasoning_effort(terra, None)
    assert defaulted["resolved_reasoning_effort"] == "medium"
    assert defaulted["reasoning_effort_source"] == "catalog_default"

    with pytest.raises(ModelSelectionError) as exc_info:
        resolve_reasoning_effort(terra, "HIGH")
    assert exc_info.value.code == "reasoning_effort_not_supported"
    assert exc_info.value.param == "reasoning_effort"


def test_registry_selection_enforces_disabled_surface_and_effort(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("NOTION_MODEL_CATALOG_ALLOW_STATIC_SELECTION", raising=False)
    monkeypatch.setattr(
        model_registry,
        "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(tmp_path / "selection.sqlite3"),
    )
    client = PickerClient()

    selected = model_registry.resolve_model_selection(client, "terra", "max")
    assert selected["canonical_id"] == "orchid-muffin"
    assert selected["resolved_reasoning_effort"] == "max"
    assert selected["catalog_source"] == "authoritative_live"

    with pytest.raises(ModelSelectionError) as surface_error:
        model_registry.resolve_model_selection(client, "luna", "medium", surface="workflow")
    assert surface_error.value.code == "model_surface_not_supported"

    with pytest.raises(ModelSelectionError) as disabled_error:
        model_registry.resolve_model_selection(client, "fable-5", "high")
    assert disabled_error.value.code == "model_disabled"

    with pytest.raises(ModelSelectionError) as effort_error:
        model_registry.resolve_model_selection(client, "terra", "minimal")
    assert effort_error.value.code == "reasoning_effort_not_supported"


def test_transcript_config_carries_validated_reasoning_effort() -> None:
    transcript = build_lite_transcript("hello", "orchid-muffin")
    configured = apply_notion_ai_options(transcript, reasoning_effort="high")
    config = next(block["value"] for block in configured if block["type"] == "config")

    assert config["reasoningEffort"] == "high"


def test_http_request_binding_records_resolved_effort_and_catalog_receipt(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("NOTION_MODEL_CATALOG_ALLOW_STATIC_SELECTION", raising=False)
    monkeypatch.setattr(
        model_registry,
        "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(tmp_path / "http-selection.sqlite3"),
    )
    request = ChatCompletionRequest(
        model="terra",
        reasoning_effort="xhigh",
        messages=[ChatMessage(role="user", content="test")],
        metadata={},
    )

    selection = _validate_request_model_selection(request, PickerClient())

    assert request.model == "orchid-muffin"
    assert request.reasoning_effort == "xhigh"
    assert selection["canonical_id"] == "orchid-muffin"
    receipt = request.metadata["model_selection"]
    assert receipt["resolved_reasoning_effort"] == "xhigh"
    assert receipt["reasoning_effort_source"] == "explicit"
    assert receipt["catalog_source"] == "authoritative_live"
    assert len(receipt["catalog_snapshot_sha256"]) == 64


def test_models_endpoint_exposes_catalog_efforts_ratings_routes_and_restrictions(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        model_registry,
        "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(tmp_path / "models-endpoint.sqlite3"),
    )
    client = PickerClient()

    class Pool:
        def get_client(self, *, wait_if_cooling: bool):
            assert wait_if_cooling is False
            return client

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(account_pool=Pool()))
    )
    response = asyncio.run(list_models(request))

    assert response["object"] == "list"
    assert response["catalog"]["catalog_source"] == "authoritative_live"
    assert len(response["data"]) == 3
    terra = next(row for row in response["data"] if row["id"] == "orchid-muffin")
    fable = next(row for row in response["data"] if row["id"] == "acai-budino-high")
    assert terra["supported_reasoning_efforts"][-2:] == ["xhigh", "max"]
    assert terra["default_reasoning_effort"] == "medium"
    assert terra["model_card_attributes"]["cost"] == 4
    assert terra["routes"]["workflow"]["supported"] is True
    assert fable["is_disabled"] is True
    assert fable["disabled_reason"] == "business_or_enterprise_plan_required"
    aliases = response["catalog"]["alias_reconciliation"]
    assert aliases["active_alias_count"] > 0
    assert "terra" not in aliases["unavailable_aliases"]


def test_models_endpoint_uses_explicit_static_fallback_for_empty_live_catalog(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        model_registry,
        "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(tmp_path / "models-static-fallback.sqlite3"),
    )
    client = PickerClient(
        {
            "modelSelectionRestricted": True,
            "models": [],
            "restrictedAccessModelsInPickerConfig": [],
            "restrictedGeoPolicyApplied": False,
        }
    )

    class Pool:
        def get_client(self, *, wait_if_cooling: bool):
            assert wait_if_cooling is False
            return client

    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(account_pool=Pool()))
    )
    response = asyncio.run(list_models(request))

    assert response["object"] == "list"
    assert response["data"]
    assert response["catalog"]["catalog_source"] == "static_fallback"
    assert response["catalog"]["catalog_fallback"] is True
    assert response["catalog"]["catalog_stale"] is True
    assert "no usable models" in response["catalog"]["catalog_fallback_reason"]


def test_removed_alias_is_not_silently_substituted(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("NOTION_MODEL_CATALOG_ALLOW_STATIC_SELECTION", raising=False)
    monkeypatch.setattr(
        model_registry,
        "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(tmp_path / "removed-alias.sqlite3"),
    )
    payload = _picker_payload()
    payload["models"] = [
        model
        for model in payload["models"]
        if model["model"] != "olive-jellyroll"
    ]
    client = PickerClient(payload)

    terra = model_registry.resolve_model_selection(client, "terra", "high")
    assert terra["canonical_id"] == "orchid-muffin"

    envelope = model_registry.get_model_catalog_for_client(client)
    alias_state = envelope.snapshot["alias_reconciliation"]
    assert "luna" not in alias_state["active_aliases"]
    assert alias_state["unavailable_aliases"]["luna"] == "olive-jellyroll"

    with pytest.raises(ModelSelectionError) as error:
        model_registry.resolve_model_selection(client, "luna", "high")
    assert error.value.code == "model_not_available"


def test_static_selection_fallback_requires_explicit_enablement(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(
        model_registry,
        "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(tmp_path / "explicit-static-selection.sqlite3"),
    )
    client = PickerClient()
    client.error = RuntimeError("offline")

    monkeypatch.delenv("NOTION_MODEL_CATALOG_ALLOW_STATIC_SELECTION", raising=False)
    with pytest.raises(ModelCatalogUnavailable):
        model_registry.resolve_model_selection(client, "terra")

    monkeypatch.setenv("NOTION_MODEL_CATALOG_ALLOW_STATIC_SELECTION", "true")
    selected = model_registry.resolve_model_selection(client, "terra")
    assert selected["canonical_id"] == "orchid-muffin"
    assert selected["catalog_source"] == "static_fallback"


def test_live_catalog_accepts_new_canonical_route_without_terra_fallback(
    monkeypatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("NOTION_MODEL_CATALOG_ALLOW_STATIC_SELECTION", raising=False)
    monkeypatch.setattr(
        model_registry,
        "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(tmp_path / "future-model.sqlite3"),
    )
    payload = _picker_payload()
    payload["models"].append(
        {
            "model": "future-reasoner-v1",
            "modelMessage": "Future Reasoner 1",
            "modelFamily": "future-family",
            "modelProvider": "future-provider",
            "displayGroup": "intelligent",
            "modelConfiguration": {
                "supportedReasoningEfforts": ["low", "high"],
                "defaultReasoningEffort": "low",
            },
            "isDisabled": False,
            "modelCardAttributes": {
                "speed": 3,
                "intelligence": 5,
                "cost": 3,
            },
            "workflow": {"finalModelName": "future-reasoner-v1", "beta": True},
        }
    )
    client = PickerClient(payload)

    selected = model_registry.resolve_model_selection(
        client, "Future Reasoner 1", "high"
    )

    assert selected["canonical_id"] == "future-reasoner-v1"
    assert selected["public_name"] == "future-reasoner-v1"
    assert selected["resolved_reasoning_effort"] == "high"
    metadata = selected["model_metadata"]
    assert metadata["canonical_id"] == "future-reasoner-v1"
    assert metadata["model_family"] == "future-family"
    assert metadata["model_provider"] == "future-provider"
    assert metadata["upstream_host"] == "notion"
    assert metadata["public_name"] != "terra"

    with pytest.raises(ModelSelectionError) as error:
        model_registry.resolve_model_selection(client, "not-a-real-model", "high")
    assert error.value.code == "model_not_available"


def test_restricted_codename_disables_suffixed_route_even_without_inline_flag() -> None:
    payload = _picker_payload()
    fable = next(
        model for model in payload["models"] if model["model"] == "acai-budino-high"
    )
    fable["isDisabled"] = False
    fable.pop("disabledReason", None)

    catalog = parse_picker_catalog(payload)
    normalized = next(
        model
        for model in catalog["models"]
        if model["canonical_id"] == "acai-budino-high"
    )

    assert normalized["restricted_access_model_codename"] == "acai-budino"
    assert normalized["is_disabled"] is True
    assert normalized["disabled_reason"] == "business_or_enterprise_plan_required"
