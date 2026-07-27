"""Source-faithful terminal-contract checks; not browser smoke tests."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def test_modular_consumer_rejects_terminal_failures_and_quarantines_partial():
    source = (ROOT / "frontend/js/chat/streaming.js").read_text(encoding="utf-8")
    for token in ("stream_error", "content_filter", "[DONE]", "tool_calls", "function_call", "seenDone"):
        assert token in source
    assert "updateAIMessage(aiWrapper, '', false)" in source


def test_inline_consumers_use_success_finish_allowlists_and_failed_presentation():
    for relative in ("frontend/index.html", "frontend/embed.html"):
        source = (ROOT / relative).read_text(encoding="utf-8")
        for token in ("stream_error", "[DONE]", "tool_calls", "function_call", "Failed"):
            assert token in source
