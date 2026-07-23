from __future__ import annotations

from app.account_pool import AccountPool


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
