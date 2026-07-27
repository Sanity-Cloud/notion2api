"""Replay the actual MCP SSE client with an offline httpx transport."""
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


CASES = {
    "clean": [event("hello"), event(finish="stop"), "data: [DONE]"],
    "stream_error_no_done": ["data: " + json.dumps({"type": "stream_error", "error": {"code": "ERR"}, "choices": [{"delta": {}, "finish_reason": "error"}]})],
    "content_filter_no_done": [event("secret"), event(finish="content_filter")],
    "malformed_missing_done": ["data: {not-json", event("partial"), event(finish="stop")],
    "socket_eof_partial": [event("partial")],
    "blank_and_non_data": [": comment", "event: message", "", event("kept")],
}


def replay(monkeypatch, lines):
    body = ("\n".join(lines) + "\n").encode()
    transport = httpx.MockTransport(lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"}, content=body))
    real = httpx.AsyncClient

    class OfflineClient(real):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(mcp_server.httpx, "AsyncClient", OfflineClient)
    updates = []
    result = asyncio.run(mcp_server.Notion2APIClient("http://offline").post_chat_stream("/v1/chat/completions", {"model": "fixture"}, lambda *x: updates.append(x)))
    return result, updates


@pytest.mark.parametrize("name,lines", CASES.items())
def test_mcp_line_data_replay_current_behavior(monkeypatch, name, lines):
    result, updates = replay(monkeypatch, lines)
    expected = {"clean": "hello", "stream_error_no_done": "", "content_filter_no_done": "secret", "malformed_missing_done": "partial", "socket_eof_partial": "partial", "blank_and_non_data": "kept"}[name]
    assert result["ok"] is True
    assert result["choices"][0]["message"]["content"] == expected
    assert result["choices"][0]["finish_reason"] == "stop"
    assert updates[-1][-1] is True


def test_mcp_uses_independent_lines_not_sse_event_blocks(monkeypatch):
    result, _ = replay(monkeypatch, ['data: {"choices":[{"delta":{"content":"a"}', 'data: "b"},"finish_reason":null}]}'])
    assert result["choices"][0]["message"]["content"] == ""


def test_mcp_does_not_recognize_done_or_terminal_semantics(monkeypatch):
    result, _ = replay(monkeypatch, ["data: [DONE]", event("after_done"), event(finish="error")])
    assert result["ok"] is True
    assert result["choices"][0]["message"]["content"] == "after_done"
    assert result["choices"][0]["finish_reason"] == "stop"


@pytest.mark.xfail(strict=True, reason="confirmed consumer defect: EOF without a valid terminal is synthesized as successful stop")
def test_mcp_must_not_report_success_for_socket_eof_without_terminal(monkeypatch):
    result, _ = replay(monkeypatch, [event("partial")])
    assert result["ok"] is False
