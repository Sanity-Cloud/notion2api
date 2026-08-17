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


def test_parse_picker_catalog_rejects_invalid_default_effort() -> None:
    payload = _picker_payload()
    payload["models"][0]["modelConfiguration"]["defaultReasoningEffort"] = "ultra"

    with pytest.raises(ModelCatalogValidationError, match="default effort"):
        parse_picker_catalog(payload)


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


def test_catalog_uses_bounded_last_known_good_then_fails_closed(
    monkeypatch, tmp_path: Path
) -> None:
    now = [100.0]
    monkeypatch.setenv("NOTION_MODEL_CATALOG_CACHE_TTL_SECONDS", "5")
    monkeypatch.setenv("NOTION_MODEL_CATALOG_MAX_STALE_SECONDS", "20")
    cache = ModelRestrictionCache(tmp_path / "catalog.sqlite3", clock=lambda: now[0])
    service = ModelCatalogService(cache, clock=lambda: now[0])
    client = PickerClient()

    assert service.get(client).source == "authoritative_live"
    now[0] = 106.0
    client.error = RuntimeError("upstream unavailable")
    stale = service.get(client)
    assert stale.source == "last_known_good"
    assert stale.stale is True
    assert stale.age_seconds == 6.0
    assert "upstream unavailable" in stale.upstream_error

    now[0] = 121.0
    with pytest.raises(ModelCatalogUnavailable, match="maximum age"):
        service.get(client)


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
