from __future__ import annotations

from types import SimpleNamespace

from app.notion_admission import AdmittedSession, NotionAdmissionController


class _UnusedSession:
    pass


def _session() -> AdmittedSession:
    owner = SimpleNamespace(
        space_id="workspace-a",
        user_id="user-a",
        current_thread_id="",
        request_idempotency_key="repoai:sanity-management:page.create:test",
        last_admission_receipt={},
    )
    return AdmittedSession(
        _UnusedSession(),
        owner,
        NotionAdmissionController(shared_store=False),
    )


def test_load_page_chunk_is_read_like_for_idempotency() -> None:
    session = _session()
    key = session._idempotency_key(
        "POST",
        "https://www.notion.so/api/v3/loadPageChunk",
        {"pageId": "parent-page", "limit": 20},
    )
    assert key == ""


def test_save_transactions_keeps_mutation_idempotency() -> None:
    session = _session()
    key = session._idempotency_key(
        "POST",
        "https://www.notion.so/api/v3/saveTransactions",
        {"transactions": [{"id": "transaction-1", "operations": []}]},
    )
    assert key.startswith("repoai:sanity-management:page.create:test:POST:")
