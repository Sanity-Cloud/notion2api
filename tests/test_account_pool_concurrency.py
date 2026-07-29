from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from app.account_pool import AccountPool
from app.api.notion import (
    AccountSwitchRequest,
    list_accounts,
    rollback_account_switch,
    switch_account,
)
from app.notion_client import NotionOpusAPI


def _account(email: str = "team@example.com") -> dict:
    return {
        "token_v2": "test-token",
        "space_id": "space-1",
        "user_id": "user-1",
        "user_email": email,
        "cookies": {"existing": "cookie"},
    }


def test_one_account_produces_request_isolated_clients() -> None:
    pool = AccountPool([_account()])

    first = pool.get_client()
    second = pool.get_client()

    assert first is not second
    assert first is not pool.clients[0]
    assert second is not pool.clients[0]
    assert first.account_key == second.account_key == "team@example.com"
    assert first._scraper is not second._scraper

    first.current_thread_id = "thread-a"
    second.current_thread_id = "thread-b"
    assert first.current_thread_id == "thread-a"
    assert second.current_thread_id == "thread-b"


def test_request_client_failure_cools_its_source_account() -> None:
    pool = AccountPool([_account()])
    client = pool.get_client()

    pool.mark_failed(client, cooldown_seconds=30)

    assert pool.get_status_summary() == {"total": 1, "active": 0, "cooling": 1}


def test_round_robin_preserves_account_identity_with_fresh_clients() -> None:
    pool = AccountPool([_account("a@example.com"), _account("b@example.com")])

    first = pool.get_client()
    second = pool.get_client()
    third = pool.get_client()

    assert [first.account_key, second.account_key, third.account_key] == [
        "a@example.com",
        "b@example.com",
        "a@example.com",
    ]
    assert len({id(first), id(second), id(third)}) == 3


def test_pinned_mode_keeps_account_identity_until_changed() -> None:
    first_account = _account("a@example.com")
    first_account["profile_name"] = "primary"
    second_account = _account("b@example.com")
    second_account["profile_name"] = "har"
    second_account["user_id"] = "user-2"
    pool = AccountPool([first_account, second_account])

    selection = pool.switch_account(mode="pinned", selector="har")
    first = pool.get_client()
    second = pool.get_client()

    assert selection["mode"] == "pinned"
    assert selection["selected_profile_name"] == "har"
    assert first.account_key == second.account_key == "b@example.com"
    assert first is not second


def test_auto_mode_restores_round_robin_after_pin() -> None:
    first_account = _account("a@example.com")
    first_account["profile_name"] = "primary"
    second_account = _account("b@example.com")
    second_account["profile_name"] = "har"
    second_account["user_id"] = "user-2"
    pool = AccountPool([first_account, second_account])

    pool.switch_account(mode="pinned", selector="har")
    pool.switch_account(mode="auto")

    assert [pool.get_client().account_key, pool.get_client().account_key] == [
        "a@example.com",
        "b@example.com",
    ]


def test_account_summary_never_exposes_credentials() -> None:
    account = _account("safe@example.com")
    account["profile_name"] = "safe"
    pool = AccountPool([account])

    summary = pool.get_selection_summary()
    serialized = str(summary)

    assert "test-token" not in serialized
    assert "existing" not in serialized
    assert summary["accounts"][0]["profile_name"] == "safe"


def test_rollback_restores_previous_selection() -> None:
    first_account = _account("a@example.com")
    first_account["profile_name"] = "primary"
    second_account = _account("b@example.com")
    second_account["profile_name"] = "har"
    second_account["user_id"] = "user-2"
    pool = AccountPool([first_account, second_account])

    pool.switch_account(mode="pinned", selector="har")
    rolled_back = pool.rollback_account_switch()

    assert rolled_back["mode"] == "auto"
    assert rolled_back["selected_profile_name"] is None


def test_pinned_selection_persists_without_credentials(monkeypatch, tmp_path) -> None:
    state_path = tmp_path / "account-selection.json"
    monkeypatch.setenv("NOTION_ACCOUNT_SELECTION_STATE", str(state_path))
    first_account = _account("a@example.com")
    first_account["profile_name"] = "primary"
    second_account = _account("b@example.com")
    second_account["profile_name"] = "har"
    second_account["user_id"] = "user-2"

    pool = AccountPool([first_account, second_account])
    pool.switch_account(mode="pinned", selector="har")
    restored = AccountPool([first_account, second_account])

    summary = restored.get_selection_summary()
    serialized_state = state_path.read_text(encoding="utf-8")
    assert summary["mode"] == "pinned"
    assert summary["selected_profile_name"] == "har"
    assert summary["persistence_enabled"] is True
    assert "test-token" not in serialized_state
    assert "existing" not in serialized_state


def _governed_account(profile: str, email: str, user_id: str) -> dict:
    account = _account(email)
    account.update(
        {
            "profile_name": profile,
            "space_id": "workspace-canonical",
            "user_id": user_id,
            "user_name": profile.title(),
            "context_page_id": "authority-page",
            "repo_ai_parent_page_id": "output-root",
            "governance_contract_version": "test-v1",
            "governance_workspace_id": "workspace-canonical",
            "governance_teamspace_id": "teamspace-canonical",
            "governance_authority_page_id": "authority-page",
            "documented_output_parent_page_id": "output-root",
            "procedural_feedback_parent_page_id": "feedback-root",
        }
    )
    return account


def _request(pool: AccountPool) -> SimpleNamespace:
    return SimpleNamespace(
        app=SimpleNamespace(state=SimpleNamespace(account_pool=pool))
    )


def test_list_accounts_route_returns_safe_metadata_only() -> None:
    pool = AccountPool(
        [
            _governed_account("primary", "primary@example.com", "user-1"),
            _governed_account("har", "har@example.com", "user-2"),
        ]
    )

    response = asyncio.run(list_accounts(_request(pool)))
    payload = response.model_dump()

    assert payload["mode"] == "auto"
    assert [item["profile_name"] for item in payload["accounts"]] == [
        "primary",
        "har",
    ]
    assert "token_v2" not in str(payload)
    assert "cookies" not in str(payload)


def test_switch_route_pin_auto_and_rollback(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        NotionOpusAPI,
        "check_page_access",
        lambda self, page_id: {
            "ok": True,
            "page_id": page_id,
            "accessible": True,
            "status_code": 200,
            "space_id": self.space_id,
            "error": "",
        },
    )
    pool = AccountPool(
        [
            _governed_account("primary", "primary@example.com", "user-1"),
            _governed_account("har", "har@example.com", "user-2"),
        ]
    )
    request = _request(pool)

    pinned = asyncio.run(
        switch_account(request, AccountSwitchRequest(mode="pinned", selector="har"))
    )
    assert pinned.mode == "pinned"
    assert pinned.selected_profile_name == "har"
    assert pool.get_client().account_key == "har@example.com"

    automatic = asyncio.run(switch_account(request, AccountSwitchRequest(mode="auto")))
    assert automatic.mode == "auto"
    assert automatic.selected_profile_name is None

    rolled_back = asyncio.run(rollback_account_switch(request))
    assert rolled_back.mode == "pinned"
    assert rolled_back.selected_profile_name == "har"
