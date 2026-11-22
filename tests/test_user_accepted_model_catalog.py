"""Exact cached-model acceptance preserves provenance and restrictions."""

from __future__ import annotations

import hashlib
import json
import time
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app import model_registry
from app.api.chat import _apply_notion_request_options
from app.conversation import build_standard_transcript
from app.notion_client import NotionOpusAPI, NotionUpstreamError
from app.schemas import ChatCompletionRequest, ChatMessage
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
            {
                "model": "future-routed-v1", "modelMessage": "Future Routed 1",
                "modelFamily": "future", "isDisabled": False,
                "workflow": {"finalModelName": "provider-special-v1", "beta": True},
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
    monkeypatch.setattr(model_registry, "ACCEPTED_CACHE_MODEL_COUNT", 3)
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
    assert len(entries) == 3
    assert receipt["catalog_acceptance_date"] == "2026-11-22"
    assert receipt["catalog_workspace"] == SANITY_MONKEY.key
    assert receipt["catalog_accepted_model_count"] == 3
    assert [m["is_disabled"] for m in entries] == [False, True, False]
    assert client.calls == 0


def test_acceptance_fails_closed_for_other_workspace_and_sha(accepted_cache, monkeypatch):
    _cache, client = accepted_cache
    other = OfflinePickerClient(SANITY_MANAGEMENT.contract.workspace_id)
    with pytest.raises(ModelCatalogUnavailable, match="not approved"):
        model_registry.get_model_catalog_for_client(other)
    monkeypatch.setattr(model_registry, "ACCEPTED_CACHE_SHA256", "0" * 64)
    with pytest.raises(ModelCatalogUnavailable, match="missing|fingerprint"):
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


def test_accepted_unknown_static_model_preserves_exact_outbound_route(accepted_cache):
    """Request -> legacy builder -> accepted binder must not become Terra."""
    _cache, client = accepted_cache
    selection = model_registry.resolve_model_selection(client, "Future Routed 1")
    assert selection["canonical_id"] == "future-routed-v1"
    assert selection["outbound_final_model_name"] == "provider-special-v1"
    request = ChatCompletionRequest(
        model="future-routed-v1", messages=[ChatMessage(role="user", content="check")]
    )
    original = build_standard_transcript(
        [{"role": "user", "content": "check"}], request.model,
        {"space_id": client.space_id},
    )
    assert original[0]["value"]["model"] == "orchid-muffin"
    transcript = _apply_notion_request_options(original, request)
    assert transcript[0]["value"]["model"] == "future-routed-v1"
    transport = NotionOpusAPI.__new__(NotionOpusAPI)
    transport.space_id = client.space_id
    converted = transport._to_notion_transcript(transcript)
    assert converted[0]["value"]["model"] == "future-routed-v1"
    outbound = transport._bind_accepted_model_route(converted, "workflow")
    assert outbound[0]["value"]["model"] == "provider-special-v1"
    assert outbound[0]["value"]["model"] != "orchid-muffin"
    assert request.model == "future-routed-v1"


def test_accepted_transport_checks_actual_attachment_surface(accepted_cache):
    _cache, client = accepted_cache
    transport = NotionOpusAPI.__new__(NotionOpusAPI)
    transport.space_id = client.space_id
    transcript = [{"type": "config", "value": {
        "type": "markdown-chat", "model": "future-routed-v1"
    }}]
    with pytest.raises(ModelSelectionError) as unsupported:
        transport._bind_accepted_model_route(transcript, "markdown-chat")
    assert unsupported.value.code == "model_surface_not_supported"
    assert transcript[0]["value"]["model"] == "future-routed-v1"
    with pytest.raises(ModelSelectionError) as restricted:
        transport._bind_accepted_model_route(
            [{"type": "config", "value": {
                "type": "workflow", "model": "orlando-quinn"
            }}], "workflow"
        )
    assert restricted.value.code == "model_disabled"


def test_accepted_pin_survives_mutable_refresh_and_maintenance(accepted_cache):
    cache, client = accepted_cache
    before = model_registry.get_model_catalog_for_client(client)
    approved_hash = before.snapshot["snapshot_sha256"]
    key = ModelCatalogService.cache_key(client.space_id)
    # An unrelated picker worker changes the normal mutable cache key.
    replacement = parse_picker_catalog({
        "models": [{"model": "temporary-only", "modelMessage": "Temporary",
                    "workflow": {"finalModelName": "temporary-only"}}],
        "modelSelectionRestricted": False,
    })
    lease = cache.claim_refresh(key, owner_id="refresh")
    assert cache.store(key, replacement, lease_token=lease)
    after = model_registry.get_model_catalog_for_client(client)
    assert after.snapshot["snapshot_sha256"] == approved_hash
    assert model_registry.resolve_model_selection(
        client, "future-routed-v1"
    )["canonical_id"] == "future-routed-v1"
    cache.clear()  # Ordinary maintenance must not erase immutable acceptance.
    restored = model_registry.get_model_catalog_for_client(client)
    assert restored.snapshot["snapshot_sha256"] == approved_hash
    assert restored.fetched_at == before.fetched_at


def test_accepted_pin_concurrent_workers(accepted_cache):
    from concurrent.futures import ThreadPoolExecutor

    cache, client = accepted_cache
    with ThreadPoolExecutor(max_workers=5) as executor:
        snapshots = list(executor.map(
            lambda _: model_registry.get_model_catalog_for_client(client),
            range(10),
        ))
    assert len({s.snapshot["snapshot_sha256"] for s in snapshots}) == 1
    key = ModelCatalogService.cache_key(client.space_id)
    with cache._connect() as conn:
        count = conn.execute(
            "SELECT count(*) FROM model_accepted_snapshots WHERE source_cache_key = ?",
            (key,),
        ).fetchone()[0]
    assert count == 1


def test_attachment_surface_fails_with_client_error(accepted_cache):
    from fastapi import HTTPException
    from app.api.chat import _validate_final_model_surface

    _cache, client = accepted_cache
    request = ChatCompletionRequest(
        model="future-routed-v1", messages=[ChatMessage(role="user", content="check")]
    )
    with pytest.raises(HTTPException) as error:
        _validate_final_model_surface(request, client, [{"filename": "sample.txt"}])
    assert error.value.status_code == 400


def test_accepted_cache_rejects_colliding_display_names(accepted_cache, monkeypatch):
    cache, client = accepted_cache
    key = ModelCatalogService.cache_key(client.space_id)
    original = cache.get(key, allow_stale=True)["payload"]
    original["models"][2]["display_name"] = "  GPT-5.6___Terra "
    canonical = dict(original)
    canonical.pop("snapshot_sha256", None)
    digest = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    original["snapshot_sha256"] = digest
    lease = cache.claim_refresh(key, owner_id="collision-test")
    assert cache.store(key, original, lease_token=lease)
    monkeypatch.setattr(model_registry, "ACCEPTED_CACHE_SHA256", digest)
    with pytest.raises(ModelCatalogUnavailable, match="ambiguous.*display names"):
        model_registry.get_model_catalog_for_client(client)


def test_http_dispatch_payload_uses_exact_accepted_route(accepted_cache):
    """Inspect the final HTTP JSON, not only catalog resolution."""
    _cache, accepted_client = accepted_cache
    client = NotionOpusAPI({
        "user_id": "synthetic-user",
        "space_id": accepted_client.space_id,
        "token_v2": "not-a-real-token",
    })
    refusal = MagicMock(status_code=403, text="denied")
    scraper = MagicMock()
    scraper.post.return_value = refusal
    profile = {
        "precreate_thread": False, "create_thread": False,
        "is_partial_transcript": True, "include_debug_overrides": False,
    }
    transcript = [{
        "id": "config-test", "type": "config",
        "value": {"type": "workflow", "model": "future-routed-v1"},
    }]
    with (
        patch.object(client, "_resolve_request_profile", return_value=profile),
        patch.object(client, "_build_cookie_header", return_value=""),
        patch("app.notion_client._create_notion_http_session", return_value=scraper),
        patch("app.notion_client.admitted_session", return_value=scraper),
        pytest.raises(NotionUpstreamError) as refused,
    ):
        list(client.stream_response(
            transcript, thread_id="existing-thread", persist_remote_chat=False,
        ))
    assert refused.value.status_code == 403
    assert scraper.post.call_count == 1
    request_payload = scraper.post.call_args.kwargs["json"]
    config = request_payload["transcript"][0]
    assert request_payload["threadType"] == "workflow"
    assert config["value"]["model"] == "provider-special-v1"
    assert config["value"]["model"] != "orchid-muffin"
