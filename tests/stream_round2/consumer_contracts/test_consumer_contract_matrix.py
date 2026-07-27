"""Source-faithful remediation contract matrix; no browser smoke."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
STREAMING = (ROOT / "frontend/js/chat/streaming.js").read_text(encoding="utf-8")
INDEX = (ROOT / "frontend/index.html").read_text(encoding="utf-8")
EMBED = (ROOT / "frontend/embed.html").read_text(encoding="utf-8")
MCP = (ROOT / "app/mcp_server.py").read_text(encoding="utf-8")


def test_web_ui_terminal_contract():
    assert "seenFinish" in STREAMING
    assert "seenDone" in STREAMING
    assert "stream_error" in STREAMING
    assert "content_filter" in STREAMING
    assert "function_call" in STREAMING
    assert "updateAIMessage(aiWrapper, '', false)" in STREAMING


def test_inline_consumers_terminal_contract():
    for source in (INDEX, EMBED):
        assert "stream_error" in source
        assert "[DONE]" in source
        assert "tool_calls" in source
        assert "function_call" in source
        assert "Failed" in source


def test_mcp_terminal_contract():
    assert 'async for line in response.aiter_lines()' in MCP
    assert 'if raw == "[DONE]":' in MCP
    assert 'seen_finish = False' in MCP
    assert 'seen_done = False' in MCP
    assert 'terminal_error:' in MCP
    assert '"finish_reason": finish_reason' in MCP
    assert '"status_code": 200' in MCP
    assert 'MAX_CHAT_JOB_RESPONSE_PREVIEW_CHARS' in MCP
