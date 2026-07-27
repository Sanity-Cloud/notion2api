"""Historical replay evidence plus live remediation contract checks."""
from __future__ import annotations

import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]
STREAMING = (ROOT / "frontend/js/chat/streaming.js").read_text(encoding="utf-8")
EMBED = (ROOT / "frontend/embed.html").read_text(encoding="utf-8")
MCP = (ROOT / "app/mcp_server.py").read_text(encoding="utf-8")


def frame(content=None, finish=None, **extra):
    data = {"choices": [{"delta": {} if content is None else {"content": content}, "finish_reason": finish}], **extra}
    return "data: " + json.dumps(data) + "\n\n"


CASES = {
    "clean_success": [frame("hello"), frame(finish="stop"), "data: [DONE]\n\n"],
    "structured_error_no_done": ["data: " + json.dumps({"type": "stream_error", "error": {"code": "ERR", "message": "bad"}, "choices": [{"delta": {}, "finish_reason": "error"}]}) + "\n\n"],
    "content_filter_no_done": [frame("secret"), frame(finish="content_filter"), "data: " + json.dumps({"type": "output_hygiene", "hygiene": {"output_integrity": {"quarantine_required": True}}}) + "\n\n"],
    "character_limit_close": [frame("partial"), frame(finish="content_filter")],
    "chunk_limit_close": [frame("partial"), frame(finish="content_filter")],
    "ordinary_exception_before": [],
    "ordinary_exception_after_partial": [frame("partial")],
    "cancel_generator_close": [],
    "malformed_terminal": ["data: {bad}\n\n"],
    "missing_finish": [frame("partial"), "data: [DONE]\n\n"],
    "missing_done_socket_eof": [frame("partial"), frame(finish="stop")],
}


def replay_data_only(chunks):
    """Match both browser consumers' data-line parsing and EOF policy."""
    text = "".join(chunks)
    visible = ""
    saw_done = False
    for block in text.split("\n\n"):
        for line in block.split("\n"):
            if not line.strip().startswith("data:"):
                continue
            payload = line.strip()[5:].strip()
            if payload == "[DONE]":
                saw_done = True
                continue
            try:
                data = json.loads(payload)
            except json.JSONDecodeError:
                continue
            if data.get("type") == "content_replace":
                visible = str(data.get("content") or "")
            else:
                visible += str(((data.get("choices") or [{}])[0].get("delta") or {}).get("content") or "")
    return {"visible": visible, "done": saw_done}


@pytest.mark.parametrize("name,chunks", CASES.items())
def test_historical_replay_matrix_preserves_pre_fix_observations(name, chunks):
    observation = replay_data_only(chunks)
    # streaming.js and embed.html consume data payloads but do not classify stream_error,
    # finish_reason, hygiene, or absent [DONE]. EOF itself is completion for both loops.
    assert observation["done"] is (name in {"clean_success", "missing_finish"})
    if name in {"clean_success", "content_filter_no_done", "character_limit_close", "chunk_limit_close", "ordinary_exception_after_partial", "missing_finish", "missing_done_socket_eof"}:
        assert observation["visible"]
    else:
        assert observation["visible"] == ""


def test_web_ui_source_contract_enforces_terminal_and_structured_errors():
    assert "createTerminalState()" in STREAMING
    assert "validateTerminalState(terminalState)" in STREAMING
    assert "dataObj.type === 'stream_error'" in STREAMING
    assert "finish_reason" in STREAMING
    assert "finishCount !== 1" in STREAMING
    assert "while (!terminalState.done && !terminalState.failure)" in STREAMING
    assert "updateAIMessage(aiWrapper, '', false)" in STREAMING


def test_embed_source_contract_enforces_terminal_and_structured_errors():
    assert "createTerminalState()" in EMBED
    assert "data.type==='stream_error'" in EMBED
    assert "finish_reason" in EMBED
    assert "terminal.finishCount!==1" in EMBED
    assert "while(!terminal.done&&!terminal.failure)" in EMBED
    assert "status.textContent=error.name==='AbortError'?'Cancelled':'Failed'" in EMBED


def test_mcp_source_contract_requires_observed_finish_and_done():
    assert 'async for line in response.aiter_lines()' in MCP
    assert 'if raw == "[DONE]":' in MCP
    assert 'done_received = True' in MCP
    assert 'finish_reason = choice.get("finish_reason")' in MCP
    assert 'observed_finish_reason in successful_finishes' in MCP
    assert '"finish_reason": observed_finish_reason' in MCP
    assert '"ok": False' in MCP
