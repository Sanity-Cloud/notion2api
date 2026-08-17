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


def test_guard_spools_clean_stream_when_memory_limit_is_exceeded(monkeypatch):
    monkeypatch.setattr(chat, "MAX_GUARDED_STREAM_BUFFER_CHARS", 10)
    source = [
        chat._build_stream_chunk("id", "model", content="long-output"),
        chat._build_stream_chunk("id", "model", finish_reason="stop"),
        "data: [DONE]\n\n",
    ]
    assert (
        list(
            chat._guard_stream_until_integrity(source, response_id="id", model="model")
        )
        == source
    )


def test_guard_suppresses_contaminated_stream_after_spooling(monkeypatch):
    monkeypatch.setattr(chat, "MAX_GUARDED_STREAM_BUFFER_CHARS", 10)
    hygiene = {
        "output_integrity": {
            "quarantine_required": True,
            "status": "indeterminate_output",
        }
    }
    source = [
        chat._build_stream_chunk("id", "model", content="long-contaminated-output"),
        chat._build_hygiene_metadata_event(hygiene),
        chat._build_stream_chunk("id", "model", finish_reason="content_filter"),
        "data: [DONE]\n\n",
    ]
    emitted = "".join(
        chat._guard_stream_until_integrity(source, response_id="id", model="model")
    )
    assert "long-contaminated-output" not in emitted
    assert '"type": "output_hygiene"' in emitted
    assert '"finish_reason": "content_filter"' in emitted
