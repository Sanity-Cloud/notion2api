"""Offline contract tests for the MCP consumer terminal state."""
from __future__ import annotations
import asyncio
import json
import os
os.environ.setdefault("NOTION_ACCOUNTS", '[{"token_v2":"fixture","space_id":"fixture","user_id":"fixture"}]')
import httpx
import pytest
from app import mcp_server

def event(content=None, finish=None, **extra):
    return "data: " + json.dumps({"choices":[{"delta": {} if content is None else {"content":content},"finish_reason":finish}], **extra})

def replay(monkeypatch, lines, headers=None):
    transport=httpx.MockTransport(lambda request:httpx.Response(200,headers={"content-type":"text/event-stream",**(headers or {})},content=("\n".join(lines)+"\n").encode()))
    real=httpx.AsyncClient
    class Offline(real):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)
    monkeypatch.setattr(mcp_server.httpx,"AsyncClient",Offline)
    updates=[]
    result=asyncio.run(mcp_server.Notion2APIClient("http://offline").post_chat_stream("/v1/chat/completions",{"model":"fixture"},lambda *item:updates.append(item)))
    return result,updates

@pytest.mark.parametrize("finish",["stop","length","tool_calls","function_call"])
def test_mcp_accepts_each_successful_finish_once_followed_by_done(monkeypatch,finish):
    result, updates=replay(monkeypatch,[event("hello"),event(finish=finish),"data: [DONE]"])
    assert result["ok"] is True
    assert result["choices"][0]["finish_reason"] == finish
    assert result["choices"][0]["message"]["content"] == "hello"
    assert sum(item[-1] is True for item in updates) == 1

@pytest.mark.parametrize("lines",[
    [event("partial")], [event("partial"),event(finish="stop")], ["data: [DONE]"],
    [event("partial"),event(finish="content_filter")], [event("partial"),event(finish="error")],
    [event("partial"),event(finish="bogus"),"data: [DONE]"],
    ["data: "+json.dumps({"type":"stream_error","error":{"code":"E1","type":"provider","message":"nope"}})],
    [event("partial"),"data: "+json.dumps({"object":"error","error":{"code":"E2","type":"provider","message":"after partial"}})],
])
def test_mcp_rejects_non_success_or_incomplete_terminal(monkeypatch,lines):
    result,updates=replay(monkeypatch,lines)
    assert result["ok"] is False
    assert result["status_code"] == 200
    assert result["error"]["code"]
    assert result["terminal_state"]["seen_done"] is False
    assert not any(item[-1] is True for item in updates)

def test_mcp_must_not_report_success_for_socket_eof_without_terminal(monkeypatch):
    result,_=replay(monkeypatch,[event("partial")])
    assert result["ok"] is False
    assert result["error"]["type"] == "stream_missing_done"

def test_mcp_stops_at_done_preserves_metadata_and_never_appends_post_done(monkeypatch):
    result,updates=replay(monkeypatch,[event("kept"),event(finish="length",model="actual",model_metadata={"actual_model":"actual"}),"data: [DONE]",event("late")],{"X-Conversation-Id":"c1","X-Notion-Thread-Id":"t1"})
    assert result["ok"] is True
    assert result["choices"][0]["message"]["content"] == "kept"
    assert result["model_metadata"]["conversation_id"] == "c1"
    assert result["model_metadata"]["notion_thread_id"] == "t1"
    assert sum(item[-1] is True for item in updates) == 1
