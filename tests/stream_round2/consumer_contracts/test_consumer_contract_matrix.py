"""Offline replay contract matrix for the bundled SSE consumers.

This lane records current behavior; it intentionally does not modify consumers.
"""
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
def test_replay_matrix_records_browser_terminal_blindness(name, chunks):
    observation = replay_data_only(chunks)
    # streaming.js and embed.html consume data payloads but do not classify stream_error,
    # finish_reason, hygiene, or absent [DONE]. EOF itself is completion for both loops.
    assert observation["done"] is (name in {"clean_success", "missing_finish"})
    if name in {"clean_success", "content_filter_no_done", "character_limit_close", "chunk_limit_close", "ordinary_exception_after_partial", "missing_finish", "missing_done_socket_eof"}:
        assert observation["visible"]
    else:
        assert observation["visible"] == ""


def test_web_ui_source_contract_has_no_terminal_or_structured_error_branch():
    assert "if (!payload || payload === '[DONE]')" in STREAMING
    assert "dataObj?.type === 'stream_error'" not in STREAMING
    assert "finish_reason" not in STREAMING
    assert "reader.read();\n            if (done) break;" in STREAMING


def test_embed_source_contract_has_no_terminal_or_structured_error_branch():
    assert "if(!payload||payload==='[DONE]')return" in EMBED
    assert "stream_error" not in EMBED
    assert "finish_reason" not in EMBED
    assert "if(done)break" in EMBED
    assert "No visible response received." in EMBED


def test_mcp_source_contract_explicitly_ignores_done_and_terminal_fields():
    assert 'async for line in response.aiter_lines()' in MCP
    assert 'if not raw or raw == "[DONE]":\n                        continue' in MCP
    assert 'choices[0].get("finish_reason")' not in MCP
    assert '"finish_reason": "stop"' in MCP
