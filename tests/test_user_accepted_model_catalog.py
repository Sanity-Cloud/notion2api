"""Exact cached-model acceptance preserves provenance and restrictions."""

from __future__ import annotations

import time
from types import SimpleNamespace

import pytest

from app import model_registry
from app.model_catalog import (
    ModelCatalogService,
    ModelCatalogUnavailable,
    ModelSelectionError,
    parse_picker_catalog,
)
from app.model_restriction_cache import ModelRestrictionCache
from app.workspace_routing import SANITY_MANAGEMENT, SANITY_MONKEY


class OfflinePickerClient:
    def __init__(self, space_id: str):
        self.space_id = space_id
        self.calls = 0

    def get_ai_model_picker_config(self):
        self.calls += 1
        raise AssertionError("An accepted cache must never query Notion's picker")


@pytest.fixture
def accepted_cache(monkeypatch, tmp_path):
    now = [time.time() - 20 * 86_400]
    cache = ModelRestrictionCache(tmp_path / "accepted.sqlite3", clock=lambda: now[0])
    payload = parse_picker_catalog({
        "models": [
            {
                "model": "orchid-muffin", "modelMessage": "GPT-5.6 Terra",
                "modelFamily": "openai", "isDisabled": False,
                "modelConfiguration": {
                    "supportedReasoningEfforts": ["low", "medium"],
                    "defaultReasoningEffort": "medium",
                },
                "workflow": {"finalModelName": "orchid-muffin", "beta": True},
            },
            {
                "model": "orlando-quinn", "modelMessage": "GPT-6 Astra",
                "modelFamily": "openai", "isDisabled": True,
                "disabledReason": "trial_not_allowed",
                "workflow": {"finalModelName": "orlando-quinn", "beta": True},
            },
        ],
        "restrictedAccessModelsInPickerConfig": [],
        "modelSelectionRestricted": False,
    })
    key = ModelCatalogService.cache_key(SANITY_MONKEY.contract.workspace_id)
    lease = cache.claim_refresh(key, owner_id="test")
    assert cache.store(key, payload, lease_token=lease, ttl_seconds=300)
    now[0] = time.time()
    monkeypatch.setattr(model_registry, "_SHARED_RESTRICTION_CACHE", cache)
    monkeypatch.setattr(
        model_registry, "ACCEPTED_CACHE_SHA256", payload["snapshot_sha256"]
    )
    monkeypatch.setattr(model_registry, "ACCEPTED_CACHE_MODEL_COUNT", 2)
    monkeypatch.setenv("NOTION_MODEL_CATALOG_SOURCE", "accepted_cache")
    monkeypatch.delenv("NOTION_MODEL_CATALOG_DISCOVERY_SOURCE", raising=False)
    return cache, OfflinePickerClient(SANITY_MONKEY.contract.workspace_id)


def test_accepted_cache_is_usable_without_falsifying_age(accepted_cache):
    _cache, client = accepted_cache
    envelope = model_registry.get_model_catalog_for_client(client)
    receipt = envelope.receipt()
    assert receipt["catalog_source"] == "user_accepted_cache"
    assert receipt["catalog_user_accepted_current"] is True
    assert receipt["catalog_route_evidence"] == (
        "user_accepted_cached_snapshot_not_live_verified"
    )
    assert receipt["catalog_runtime_verified"] is False
    assert receipt["catalog_stale"] is True
    assert receipt["catalog_stale_policy_exceeded"] is True
    assert receipt["catalog_age_seconds"] > 14 * 86_400
    assert client.calls == 0
    resolved = model_registry.resolve_model_selection(
        client, "GPT-5.6 Terra", surface="workflow"
    )
    assert resolved["canonical_id"] == "orchid-muffin"
    assert resolved["catalog_user_accepted_current"] is True
    assert resolved["resolved_reasoning_effort"] == "medium"
    assert client.calls == 0


def test_accepted_cache_keeps_explicit_restrictions(accepted_cache):
    _cache, client = accepted_cache
    with pytest.raises(ModelSelectionError) as restricted:
        model_registry.resolve_model_selection(client, "GPT-6 Astra")
    assert restricted.value.code == "model_disabled"
    with pytest.raises(ModelSelectionError) as missing:
        model_registry.resolve_model_selection(client, "GPT-6.2 Sol")
    assert missing.value.code == "model_not_available"
    with pytest.raises(ModelSelectionError) as unsupported:
        model_registry.resolve_model_selection(
            client, "GPT-5.6 Terra", surface="agent_service"
        )
    assert unsupported.value.code == "model_surface_not_supported"


def test_cached_listing_uses_metadata_client_not_rotating_account(accepted_cache):
    _cache, client = accepted_cache
    pool = SimpleNamespace(
        get_metadata_client=lambda: client,
        get_client=lambda **_: (_ for _ in ()).throw(
            AssertionError("The listing attempted live account rotation")
        ),
    )
    request = SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(account_pool=pool))
    )
    entries, receipt = model_registry.list_model_metadata_for_request(request)
    assert len(entries) == 2
    assert receipt["catalog_acceptance_date"] == "2026-11-22"
    assert receipt["catalog_workspace"] == SANITY_MONKEY.key
    assert receipt["catalog_accepted_model_count"] == 2
    assert [m["is_disabled"] for m in entries] == [False, True]
    assert client.calls == 0


def test_acceptance_fails_closed_for_other_workspace_and_sha(accepted_cache, monkeypatch):
    _cache, client = accepted_cache
    other = OfflinePickerClient(SANITY_MANAGEMENT.contract.workspace_id)
    with pytest.raises(ModelCatalogUnavailable, match="not approved"):
        model_registry.get_model_catalog_for_client(other)
    monkeypatch.setattr(model_registry, "ACCEPTED_CACHE_SHA256", "0" * 64)
    with pytest.raises(ModelCatalogUnavailable, match="fingerprint"):
        model_registry.get_model_catalog_for_client(client)
    assert client.calls == 0
    assert other.calls == 0


def test_accepted_cache_missing_does_not_fall_back_to_static(tmp_path, monkeypatch):
    monkeypatch.setattr(
        model_registry, "_SHARED_RESTRICTION_CACHE",
        ModelRestrictionCache(tmp_path / "empty.sqlite3"),
    )
    monkeypatch.setenv("NOTION_MODEL_CATALOG_SOURCE", "accepted_cache")
    client = OfflinePickerClient(SANITY_MONKEY.contract.workspace_id)
    with pytest.raises(ModelCatalogUnavailable, match="missing"):
        model_registry.get_model_catalog_for_client(client, allow_static_fallback=True)
    assert client.calls == 0
