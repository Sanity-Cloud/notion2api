from __future__ import annotations

import threading
import time
from pathlib import Path

from app import model_registry
from app.model_restriction_cache import ModelRestrictionCache


def _reset_registry_cache(monkeypatch, database: Path) -> ModelRestrictionCache:
    cache = ModelRestrictionCache(database)
    monkeypatch.setattr(model_registry, "_SHARED_RESTRICTION_CACHE", cache)
    model_registry._RESTRICTED_MODELS_CACHE.clear()
    model_registry._RESTRICTED_MODELS_LOCKS.clear()
    return cache


def test_cross_process_refresh_lease_and_cache_round_trip(tmp_path: Path) -> None:
    database = tmp_path / "model-cache.sqlite3"
    first = ModelRestrictionCache(database)
    second = ModelRestrictionCache(database)

    token = first.claim_refresh("space-a", owner_id="process-a")
    assert token
    assert second.claim_refresh("space-a", owner_id="process-b") == ""

    assert first.store(
        "space-a",
        {"restricted_models": ["model-a", "model-b"]},
        lease_token=token,
        ttl_seconds=60,
    )
    cached = second.get("space-a")
    assert cached is not None
    assert cached["payload"]["restricted_models"] == ["model-a", "model-b"]


def test_parallel_registry_misses_use_one_upstream_request(
    monkeypatch, tmp_path: Path
) -> None:
    _reset_registry_cache(monkeypatch, tmp_path / "model-cache.sqlite3")
    monkeypatch.setenv("NOTION_MODEL_RESTRICTION_CACHE_TTL_SECONDS", "300")
    monkeypatch.setenv("NOTION_MODEL_RESTRICTION_REFRESH_WAIT_SECONDS", "2")

    class Client:
        space_id = "space-single-flight"

        def __init__(self) -> None:
            self.calls = 0
            self.lock = threading.Lock()

        def get_ai_model_picker_config(self):
            with self.lock:
                self.calls += 1
            time.sleep(0.1)
            return {
                "restrictedAccessModelsInPickerConfig": [
                    {"codename": "restricted-a"}
                ],
                "models": [],
            }

    client = Client()
    results: list[set[str]] = []
    start = threading.Barrier(6)

    def worker() -> None:
        start.wait()
        results.append(model_registry.get_restricted_models_for_space(client))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=3)

    assert all(not thread.is_alive() for thread in threads)
    assert client.calls == 1
    assert results == [{"restricted-a"}] * 6


def test_expired_refresh_owner_cannot_overwrite_takeover(tmp_path: Path) -> None:
    now = [100.0]
    database = tmp_path / "takeover.sqlite3"
    first = ModelRestrictionCache(database, clock=lambda: now[0])
    second = ModelRestrictionCache(database, clock=lambda: now[0])

    stale_token = first.claim_refresh(
        "space-a:user-a", owner_id="owner-a", lease_seconds=5
    )
    assert stale_token
    now[0] += 6
    takeover_token = second.claim_refresh(
        "space-a:user-a", owner_id="owner-b", lease_seconds=5
    )
    assert takeover_token
    assert takeover_token != stale_token

    assert not first.store(
        "space-a:user-a",
        {"restricted_models": ["stale"]},
        lease_token=stale_token,
    )
    assert second.store(
        "space-a:user-a",
        {"restricted_models": ["current"]},
        lease_token=takeover_token,
    )
    assert second.get("space-a:user-a")["payload"]["restricted_models"] == [
        "current"
    ]


def test_restriction_cache_is_global_to_workspace(
    monkeypatch, tmp_path: Path
) -> None:
    _reset_registry_cache(monkeypatch, tmp_path / "account-cache.sqlite3")

    class Client:
        space_id = "shared-space"

        def __init__(self, user_id: str, restricted: str) -> None:
            self.user_id = user_id
            self.restricted = restricted
            self.calls = 0

        def get_ai_model_picker_config(self):
            self.calls += 1
            return {
                "restrictedAccessModelsInPickerConfig": [
                    {"codename": self.restricted}
                ],
                "models": [],
            }

    first = Client("user-a", "restricted-a")
    second = Client("user-b", "restricted-b")

    assert model_registry.get_restricted_models_for_space(first) == {
        "restricted-a"
    }
    assert model_registry.get_restricted_models_for_space(second) == {
        "restricted-a"
    }
    assert first.calls == 1
    assert second.calls == 0
