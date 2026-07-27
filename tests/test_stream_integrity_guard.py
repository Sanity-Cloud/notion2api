from app.api import chat


def test_guard_releases_clean_stream_after_terminal_classification():
    source = [
        chat._build_stream_chunk("id", "model", role="assistant"),
        chat._build_stream_chunk("id", "model", content="safe"),
        chat._build_stream_chunk("id", "model", finish_reason="stop"),
        "data: [DONE]\n\n",
    ]
    assert (
        list(
            chat._guard_stream_until_integrity(source, response_id="id", model="model")
        )
        == source
    )


def test_guard_suppresses_content_when_final_hygiene_quarantines_output():
    hygiene = {
        "output_integrity": {
            "quarantine_required": True,
            "status": "indeterminate_output",
        }
    }
    source = [
        chat._build_stream_chunk("id", "model", role="assistant"),
        chat._build_stream_chunk("id", "model", content="secret-contaminated-text"),
        chat._build_hygiene_metadata_event(hygiene),
        chat._build_stream_chunk("id", "model", finish_reason="content_filter"),
        "data: [DONE]\n\n",
    ]
    emitted = "".join(
        chat._guard_stream_until_integrity(source, response_id="id", model="model")
    )
    assert "secret-contaminated-text" not in emitted
    assert '"type": "output_hygiene"' in emitted
    assert '"finish_reason": "content_filter"' in emitted
    assert emitted.endswith("data: [DONE]\n\n")


def test_guard_fails_closed_when_buffer_limit_is_exceeded(monkeypatch):
    monkeypatch.setattr(chat, "MAX_GUARDED_STREAM_BUFFER_CHARS", 10)
    source = [chat._build_stream_chunk("id", "model", content="long-output")]
    emitted = "".join(
        chat._guard_stream_until_integrity(source, response_id="id", model="model")
    )
    assert "long-output" not in emitted
    assert "guarded_stream_buffer_limit_exceeded" in emitted
    assert '"finish_reason": "content_filter"' in emitted


def test_browser_request_shape_and_operations_wrapper_get_explicit_terminal_failure():
    """Regression for the browser POST path that is wrapped by Operations.

    The request fields mirror the real UI. The drawer delegates all four
    streamResponse arguments, so a truncated HTTP-200 body must become an
    explicit terminal error before the wrapper marks the job complete.
    """
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    browser = (root / "frontend/index.html").read_text(encoding="utf-8")
    drawer = (root / "frontend/js/operations-drawer.js").read_text(encoding="utf-8")
    server = (root / "app/server.py").read_text(encoding="utf-8")
    for marker in (
        "headers:{'Content-Type':'application/json'",
        "'Authorization':`Bearer ${window.NotionAI.Core.State.get('apiKey')}`",
        "'X-Client-Type':window.NotionAI.Core.Constants.CLIENT_TYPE",
        "conversation_id:chat.conversationId||chat.id||null",
        "stream:true,attachments,metadata:{persist_remote_chat:",
    ):
        assert marker in browser
    assert '<script src="/js/operations-drawer.js"></script>' in server
    assert "original.call(this, chat, model, aiWrapper, attachments)" in drawer

    # Same browser body shape, with a 200 SSE body that lacks terminal frames.
    request_body = {
        "model": "terra",
        "messages": [{"role": "user", "content": "PILOT_OK"}],
        "conversation_id": "browser-conversation",
        "stream": True,
        "attachments": [],
        "metadata": {"persist_remote_chat": True},
    }
    assert request_body["stream"] is True
    emitted = "".join(
        chat._guard_stream_until_integrity(
            [chat._build_stream_chunk("id", request_body["model"], content="partial")],
            response_id="id",
            model=request_body["model"],
        )
    )
    assert "partial" not in emitted
    assert '"code": "incomplete_terminal_state"' in emitted
    assert '"finish_reason": "error"' in emitted
    assert emitted.endswith("data: [DONE]\n\n")


def test_guard_rejects_reordered_or_duplicate_terminal_frames():
    for source in (
        ["data: [DONE]\n\n", chat._build_stream_chunk("id", "model", finish_reason="stop")],
        [
            chat._build_stream_chunk("id", "model", finish_reason="stop"),
            chat._build_stream_chunk("id", "model", finish_reason="stop"),
            "data: [DONE]\n\n",
        ],
    ):
        emitted = "".join(chat._guard_stream_until_integrity(source, response_id="id", model="model"))
        assert '"code": "incomplete_terminal_state"' in emitted
        assert '"finish_reason": "error"' in emitted
