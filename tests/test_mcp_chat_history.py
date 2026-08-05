import asyncio
import json
import os

os.environ.setdefault(
    "NOTION_ACCOUNTS",
    json.dumps(
        [
            {
                "profile_name": "test",
                "token_v2": "test-token",
                "space_id": "test-space",
                "user_id": "test-user",
            }
        ]
    ),
)

from app import mcp_server
from app.chat_history.store import ChatHistoryStore


def _server():
    return mcp_server.create_server(
        base_url="http://127.0.0.1:8120",
        api_key="test-key",
        timeout=1,
        host="127.0.0.1",
        port=8130,
        mcp_path="/mcp",
    )


def _call(server, arguments):
    _content, structured = asyncio.run(server.call_tool("chat_history", arguments))
    return structured


def _account_response():
    return {
        "ok": True,
        "status_code": 200,
        "mode": "pinned",
        "workspace_key": "sanity-management",
        "workspace_name": "Sanity Management",
        "workspace_id": "workspace-1",
        "teamspace_name": "Sanity-Cloud-InScene",
        "teamspace_id": "teamspace-1",
        "selected_account_number": 1,
        "selected_profile_name": "test",
        "governance": {"aligned": True},
        "accounts": [
            {
                "account_number": 1,
                "profile_name": "test",
                "workspace_key": "sanity-management",
                "workspace_name": "Sanity Management",
                "teamspace_name": "Sanity-Cloud-InScene",
                "space_id": "workspace-1",
                "selected": True,
                "available": True,
                "governance_aligned": True,
                "user_email": "must-not-leak@example.invalid",
                "user_id": "must-not-leak",
            }
        ],
    }


def test_chat_history_search_routes_with_pagination_and_redacted_provenance(monkeypatch):
    calls = []

    async def fake_get(self, path, params=None):
        calls.append((path, params))
        if path == "/v1/notion/accounts":
            return _account_response()
        assert path == "/chat-history/search"
        return {
            "ok": True,
            "status_code": 200,
            "results": [{"id": "message-1", "thread_id": "thread-1"}],
        }

    monkeypatch.setattr(mcp_server.Notion2APIClient, "get", fake_get)
    result = _call(
        _server(),
        {"action": "search", "query": "governance", "limit": 10, "offset": 5},
    )

    assert calls[-1] == (
        "/chat-history/search",
        {"q": "governance", "limit": 10, "offset": 5},
    )
    assert result["ok"] is True
    assert result["pagination"] == {
        "limit": 10,
        "offset": 5,
        "returned": 1,
        "next_offset": 6,
        "has_more": False,
    }
    assert result["provenance"]["workspace_id"] == "workspace-1"
    assert result["provenance"]["teamspace_id"] == "teamspace-1"
    assert result["provenance"]["requested_account"]["profile_name"] == "test"
    assert result["provenance"]["requested_account"]["workspace_id"] == "workspace-1"
    encoded = json.dumps(result)
    assert "must-not-leak@example.invalid" not in encoded
    assert '"user_email"' not in encoded
    assert '"user_id"' not in encoded


def test_chat_history_rejects_missing_action_input_before_http_dispatch(monkeypatch):
    calls = []

    async def fail_get(self, path, params=None):
        calls.append((path, params))
        raise AssertionError("HTTP dispatch must not occur for invalid action input")

    monkeypatch.setattr(mcp_server.Notion2APIClient, "get", fail_get)
    result = _call(_server(), {"action": "search"})

    assert result["ok"] is False
    assert result["status_code"] == 422
    assert "query is required" in result["error"]
    assert calls == []


def test_chat_history_get_thread_bounds_messages_and_process_steps(monkeypatch):
    async def fake_get(self, path, params=None):
        if path == "/v1/notion/accounts":
            return _account_response()
        assert path == "/chat-history/threads/thread-1"
        return {
            "ok": True,
            "status_code": 200,
            "id": "thread-1",
            "messages": [{"id": f"m-{index}"} for index in range(5)],
            "process_steps": [{"id": f"s-{index}"} for index in range(4)],
        }

    monkeypatch.setattr(mcp_server.Notion2APIClient, "get", fake_get)
    result = _call(
        _server(),
        {"action": "get_thread", "thread_id": "thread-1", "message_limit": 2},
    )

    assert [item["id"] for item in result["result"]["messages"]] == ["m-0", "m-1"]
    assert result["result"]["messages_total"] == 5
    assert result["result"]["messages_truncated"] is True
    assert [item["id"] for item in result["result"]["process_steps"]] == ["s-0", "s-1"]
    assert result["result"]["process_steps_truncated"] is True


def test_chat_history_sync_routes_account_and_reports_partial_results(monkeypatch):
    posts = []

    async def fake_get(self, path, params=None):
        assert path == "/v1/notion/accounts"
        return _account_response()

    async def fake_post(self, path, payload):
        posts.append((path, payload))
        return {
            "ok": True,
            "status_code": 200,
            "sync_summary": {"failed": [{"thread_id": "thread-2"}]},
            "imported": {"threads_inserted": 1},
        }

    monkeypatch.setattr(mcp_server.Notion2APIClient, "get", fake_get)
    monkeypatch.setattr(mcp_server.Notion2APIClient, "post", fake_post)
    result = _call(
        _server(),
        {
            "action": "sync_from_notion",
            "account_index": 0,
            "limit": 75,
            "max_pages": 3,
            "hydrate": True,
        },
    )

    assert posts == [
        (
            "/chat-history/sync/notion",
            {"account_index": 0, "limit": 75, "max_pages": 3, "hydrate": True},
        )
    ]
    assert result["ok"] is True
    assert result["partial"] is True
    assert result["idempotent"] is True


def test_chat_history_markdown_export_is_content_bounded(monkeypatch):
    calls = []

    async def fake_get(self, path, params=None):
        assert path == "/v1/notion/accounts"
        return _account_response()

    async def fake_get_text(self, path, params=None, *, max_chars=50_000):
        calls.append((path, max_chars))
        return {
            "ok": True,
            "status_code": 200,
            "text": "# Bounded",
            "text_chars": 120_000,
            "truncated": True,
        }

    monkeypatch.setattr(mcp_server.Notion2APIClient, "get", fake_get)
    monkeypatch.setattr(mcp_server.Notion2APIClient, "get_text", fake_get_text)
    result = _call(
        _server(),
        {
            "action": "export_markdown",
            "thread_id": "thread/with spaces",
            "content_limit": 25_000,
        },
    )

    assert calls == [("/chat-history/threads/thread%2Fwith%20spaces/markdown", 25_000)]
    assert result["result"]["markdown"] == "# Bounded"
    assert result["result"]["content_chars"] == 120_000
    assert result["result"]["content_truncated"] is True


def test_chat_history_store_search_pagination_is_deterministic(tmp_path):
    store = ChatHistoryStore(str(tmp_path / "chat-history.db"))
    store.upsert_bundle(
        {
            "threads": {
                "thread-1": {"id": "thread-1", "title": "First"},
                "thread-2": {"id": "thread-2", "title": "Second"},
            },
            "messages": {
                "message-a": {
                    "id": "message-a",
                    "thread_id": "thread-1",
                    "role": "assistant",
                    "text": "governance update alpha",
                },
                "message-b": {
                    "id": "message-b",
                    "thread_id": "thread-2",
                    "role": "assistant",
                    "text": "governance update beta",
                },
            },
        }
    )

    first = store.search("governance", limit=1, offset=0)
    second = store.search("governance", limit=1, offset=1)
    repeated_first = store.search("governance", limit=1, offset=0)

    assert first == repeated_first
    assert len(first) == 1
    assert len(second) == 1
    assert first[0]["id"] != second[0]["id"]
    assert {first[0]["id"], second[0]["id"]} == {"message-a", "message-b"}
