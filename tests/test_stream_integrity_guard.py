import asyncio

import pytest

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


def test_guard_terminalizes_upstream_exception_without_success_done():
    metadata = 'data: {"type":"model_metadata","model":"route"}\n\n'

    def source():
        yield metadata
        raise RuntimeError("upstream exploded")

    emitted = "".join(
        chat._guard_stream_until_integrity(source(), response_id="id", model="model")
    )

    assert metadata in emitted
    assert "ERR_PROVIDER_EMPTY_STREAM" in emitted
    assert '"finish_reason": "error"' in emitted
    assert not emitted.endswith("data: [DONE]\n\n")


def test_guard_rejects_stream_missing_done_sentinel():
    source = [
        chat._build_stream_chunk("id", "model", content="partial"),
        chat._build_stream_chunk("id", "model", finish_reason="stop"),
    ]

    emitted = "".join(
        chat._guard_stream_until_integrity(source, response_id="id", model="model")
    )

    assert "ERR_STREAM_MISSING_DONE" in emitted
    assert '"finish_reason": "error"' in emitted
    assert "partial" not in emitted
    assert not emitted.endswith("data: [DONE]\n\n")


def test_guard_preserves_generator_exit():
    def source():
        yield chat._build_stream_chunk("id", "model", content="partial")
        raise GeneratorExit()

    with pytest.raises(GeneratorExit):
        list(chat._guard_stream_until_integrity(source(), response_id="id", model="model"))


def test_guard_preserves_cancellation():
    def source():
        yield chat._build_stream_chunk("id", "model", content="partial")
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        list(chat._guard_stream_until_integrity(source(), response_id="id", model="model"))
