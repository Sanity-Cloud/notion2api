"""Focused tests for capacity roles and health-aware account selection."""

from __future__ import annotations

import time

from app.account_capacity import CapacityRole, resolve_capacity_role
from app.account_health import score_account_health
from app.account_pool import AccountPool
from app.account_scheduler import select_account_index


def _account(email: str, user_id: str, *, space_id: str = "space-1") -> dict:
    return {
        "token_v2": "test-token",
        "space_id": space_id,
        "user_id": user_id,
        "user_email": email,
        "cookies": {"existing": "cookie"},
    }


def test_default_ordinal_roles_map_to_alpha_beta_canary_dev() -> None:
    accounts = [
        _account("a@example.com", "user-1"),
        _account("b@example.com", "user-2"),
        _account("c@example.com", "user-3"),
        _account("d@example.com", "user-4"),
    ]
    for index, account in enumerate(accounts, start=1):
        account["profile_name"] = f"profile-{index}"
    pool = AccountPool(accounts)
    summary = pool.get_selection_summary()
    roles = [item["capacity_role"] for item in summary["accounts"]]
    aliases = [item["account_alias"] for item in summary["accounts"]]
    assert roles == ["alpha", "beta", "canary", "dev"]
    assert aliases == ["Alpha", "Beta", "Canary", "Dev"]


def test_capacity_alias_selector_pins_without_renaming_credentials() -> None:
    first = _account("a@example.com", "user-1")
    first["profile_name"] = "personaltouch"
    second = _account("b@example.com", "user-2")
    second["profile_name"] = "other"
    pool = AccountPool([first, second])

    selection = pool.switch_account(mode="pinned", selector="Beta")
    client = pool.get_client()

    assert selection["mode"] == "pinned"
    assert client.account_key == "b@example.com"
    assert client.account_alias == "Beta"
    assert first["profile_name"] == "personaltouch"


def test_auto_mode_excludes_dev_and_rotates_production_peers() -> None:
    accounts = [
        _account("a@example.com", "user-1"),
        _account("b@example.com", "user-2"),
        _account("c@example.com", "user-3"),
        _account("d@example.com", "user-4"),
    ]
    for index, account in enumerate(accounts, start=1):
        account["profile_name"] = f"profile-{index}"
    pool = AccountPool(accounts)

    selected = [pool.get_client().account_key for _ in range(4)]
    assert set(selected) <= {"a@example.com", "b@example.com"}
    assert "d@example.com" not in selected


def test_development_workload_can_select_dev_account() -> None:
    accounts = [
        _account("a@example.com", "user-1"),
        _account("b@example.com", "user-2"),
        _account("c@example.com", "user-3"),
        _account("d@example.com", "user-4"),
    ]
    for index, account in enumerate(accounts, start=1):
        account["profile_name"] = f"profile-{index}"
    # Cool production peers so Dev is the only healthy development target.
    pool = AccountPool(accounts)
    future = time.time() + 60
    pool.cooldown_until[0] = future
    pool.cooldown_until[1] = future
    pool.cooldown_until[2] = future

    client = pool.get_client(workload={"workload_class": "development"})
    assert client.account_key == "d@example.com"
    assert client.capacity_role == "dev"


def test_unhealthy_peer_loses_to_healthy_peer() -> None:
    roles = {0: CapacityRole.ALPHA, 1: CapacityRole.BETA}
    health = {
        0: score_account_health(account_key="a", capacity_role="alpha"),
        1: score_account_health(account_key="b", capacity_role="beta"),
    }
    health[0].available = False
    health[0].health_score = 0.1
    health[0].health_reason = "cooldown:30.0s"
    decision = select_account_index(
        workspace_indices=[0, 1],
        roles_by_index=roles,
        health_by_index=health,
    )
    assert decision.selected_index == 1
    assert decision.reason == "health_aware_selection"


def test_canary_fraction_uses_stable_seed(monkeypatch) -> None:
    monkeypatch.setenv("NOTION_CANARY_ROUTE_FRACTION", "1.0")
    roles = {
        0: CapacityRole.ALPHA,
        1: CapacityRole.BETA,
        2: CapacityRole.CANARY,
    }
    health = {
        index: score_account_health(account_key=str(index), capacity_role=role.value)
        for index, role in roles.items()
    }
    decision = select_account_index(
        workspace_indices=[0, 1, 2],
        roles_by_index=roles,
        health_by_index=health,
        route_seed="always-canary-eligible",
    )
    assert decision.canary_included is True
    assert 2 in {item["index"] for item in decision.candidates if item["available"]}


def test_resolve_capacity_role_honors_explicit_config() -> None:
    role = resolve_capacity_role(
        {"capacity_role": "canary", "profile_name": "anything"},
        account_number=1,
    )
    assert role is CapacityRole.CANARY


def test_selection_summary_exposes_routing_telemetry_without_secrets() -> None:
    account = _account("safe@example.com", "user-1")
    account["profile_name"] = "safe"
    pool = AccountPool([account])
    pool.get_client()
    summary = pool.get_selection_summary()
    serialized = str(summary)
    assert "test-token" not in serialized
    assert summary["accounts"][0]["capacity_role"] == "alpha"
    assert "health_score" in summary["accounts"][0]
    assert "routing_decision" in summary
