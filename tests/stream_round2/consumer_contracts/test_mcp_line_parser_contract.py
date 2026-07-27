"""Replay the MCP SSE client terminal-state contract with an offline transport."""
from __future__ import annotations

import asyncio
import json
import os

os.environ.setdefault("NOTION_ACCOUNTS", '[{"token_v2":"fixture","space_id":"fixture","user_id":"fixture"}]')

import httpx
import pytest

from app import mcp_server


def event(content=None, finish=None, **extra):
    payload = {"choices": [{"delta": {} if content is None else {"content": content}, "finish_reason": finish}], **extra}
    return "data: " + json.dumps(payload)


def replay(monkeypatch, lines, headers=None):
    body = ("\n".join(lines) + "\n").encode()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, headers={"content-type": "text/event-stream", **(headers or {})}, content=body))
    real = httpx.AsyncClient

    class OfflineClient(real):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(mcp_server.httpx, "AsyncClient", OfflineClient)
    updates = []
    result = asyncio.run(mcp_server.Notion2APIClient("http://offline").post_chat_stream("/v1/chat/completions", {"model": "fixture"}, lambda *x: updates.append(x)))
    return result, updates


@pytest.mark.parametrize("finish", ["stop", "length", "tool_calls", "function_call"])
def test_mcp_accepts_each_successful_finish_reason_after_done(monkeypatch, finish):
    result, updates = replay(monkeypatch, [event("hello"), event(finish=finish), "data: [DONE]"])
    assert result["ok"] is True
    assert result["choices"][0]["message"]["content"] == "hello"
    assert result["choices"][0]["finish_reason"] == finish
    assert result["terminal_state"] == {"success": True, "finish_reason": finish, "successful_finish_count": 1, "done_received": True}
    assert [update for update in updates if update[-1] is True] == [("", "hello", 1, True)]


@pytest.mark.parametrize("lines", [
    ["data: " + json.dumps({"type": "stream_error", "error": {"code": "ERR", "message": "before"}})],
    [event("partial"), "data: " + json.dumps({"type": "stream_error", "error": {"code": "ERR", "message": "after", "retriable": True}})],
    [event("secret"), event(finish="content_filter")],
    [event("partial"), event(finish="error")],
    [event("partial")],
    [event("partial"), event(finish="stop")],
    ["data: [DONE]"],
])
def test_mcp_rejects_incomplete_or_error_terminal_states(monkeypatch, lines):
    result, updates = replay(monkeypatch, lines)
    assert result["ok"] is False
    assert result["status_code"] == 200
    assert result["choices"] == []
    assert result["error"]["code"]
    assert result["terminal_state"]["success"] is False
    assert result["partial_content"]["char_count"] in {0, 6, 7}
    assert not [update for update in updates if update[-1] is True]


def test_mcp_ignores_post_done_frames_and_preserves_metadata(monkeypatch):
    result, updates = replay(
        monkeypatch,
        [event("kept", model="event-model", model_metadata={"actual_model": "actual", "event": "yes"}), event(finish="stop"), "data: [DONE]", event("ignored"), event(finish="error")],
        {"X-Conversation-Id": "conversation-123", "X-Notion-Thread-Id": "thread-123"},
    )
    assert result["ok"] is True
    assert result["model"] == "event-model"
    assert result["actual_model"] == "actual"
    assert result["model_metadata"] == {"actual_model": "actual", "event": "yes", "conversation_id": "conversation-123", "notion_thread_id": "thread-123", "remote_chat_id": "thread-123"}
    assert result["choices"][0]["message"]["content"] == "kept"
    assert len([update for update in updates if update[-1] is True]) == 1


def test_mcp_ignores_malformed_and_non_dict_frames_but_requires_terminal(monkeypatch):
    result, _ = replay(monkeypatch, ["data: {bad", "data: []", event("kept"), event(finish="stop"), "data: [DONE]"])
    assert result["ok"] is True
    assert result["choices"][0]["message"]["content"] == "kept"


def test_mcp_must_not_report_success_for_socket_eof_without_terminal(monkeypatch):
    result, _ = replay(monkeypatch, [event("partial")])
    assert result["ok"] is False
    assert result["error"]["code"] == "incomplete_terminal_state"
