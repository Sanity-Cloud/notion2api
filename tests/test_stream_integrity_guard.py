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
    assert "data: [DONE]" not in emitted


def test_guard_fails_closed_when_buffer_limit_is_exceeded(monkeypatch):
    monkeypatch.setattr(chat, "MAX_GUARDED_STREAM_BUFFER_CHARS", 10)
    source = [chat._build_stream_chunk("id", "model", content="long-output")]
    emitted = "".join(
        chat._guard_stream_until_integrity(source, response_id="id", model="model")
    )
    assert "long-output" not in emitted
    assert "guarded_stream_buffer_limit_exceeded" in emitted
    assert '"finish_reason": "content_filter"' in emitted
    assert "data: [DONE]" not in emitted


def test_guard_fails_closed_when_chunk_count_limit_is_exceeded(monkeypatch):
    monkeypatch.setattr(chat, "MAX_GUARDED_STREAM_BUFFER_CHUNKS", 3)
    source = [
        chat._build_stream_chunk("id", "model", content="x")
        for _ in range(4)
    ]
    emitted = "".join(
        chat._guard_stream_until_integrity(source, response_id="id", model="model")
    )
    assert '"finish_reason": "content_filter"' in emitted
    assert "guarded_stream_buffer_chunk_limit_exceeded" in emitted
    assert '"content": "x"' not in emitted
    assert "data: [DONE]" not in emitted


def test_guard_accepts_multiple_complete_logical_sse_chunks():
    source = [
        chat._build_stream_chunk("id", "model", content="fragment-a"),
        chat._build_stream_chunk("id", "model", content="fragment-b"),
        chat._build_stream_chunk("id", "model", finish_reason="stop"),
        "data: [DONE]\n\n",
    ]
    assert list(chat._guard_stream_until_integrity(source, response_id="id", model="model")) == source


class _InfiniteCountingSource:
    def __init__(self, chunk: str) -> None:
        self.chunk = chunk
        self.pulls = 0
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        self.pulls += 1
        return self.chunk

    def close(self) -> None:
        self.close_calls += 1


def test_guard_limit_stops_consumption_and_closes_infinite_source(monkeypatch):
    monkeypatch.setattr(chat, "MAX_GUARDED_STREAM_BUFFER_CHARS", 10)
    source = _InfiniteCountingSource(chat._build_stream_chunk("id", "model", content="long-output"))

    emitted = "".join(chat._guard_stream_until_integrity(source, response_id="id", model="model"))

    assert source.pulls == 1
    assert source.close_calls == 1
    assert "guarded_stream_buffer_limit_exceeded" in emitted
    assert "data: [DONE]" not in emitted


def test_guard_limit_close_cascades_through_standard_generator(monkeypatch):
    monkeypatch.setattr(chat, "MAX_GUARDED_STREAM_BUFFER_CHARS", 10)
    upstream = _InfiniteCountingSource("unreachable")
    standard = chat._create_standard_stream_generator(
        "id",
        "model",
        {"type": "content", "text": "long-output"},
        upstream,
    )

    assert callable(getattr(standard, "close", None))
    emitted = "".join(chat._guard_stream_until_integrity(standard, response_id="id", model="model"))

    assert upstream.pulls == 0
    assert upstream.close_calls == 1
    assert "data: [DONE]" not in emitted


class _RaisingClosableSource:
    def __init__(self, error: BaseException) -> None:
        self.error = error
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        raise self.error

    def close(self) -> None:
        self.close_calls += 1


def test_guard_cancellation_closes_source_without_terminal_emission():
    source = _RaisingClosableSource(asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        list(chat._guard_stream_until_integrity(source, response_id="id", model="model"))

    assert source.close_calls == 1


def test_guard_generator_exit_closes_source_without_terminal_emission():
    source = _RaisingClosableSource(GeneratorExit())

    with pytest.raises(GeneratorExit):
        list(chat._guard_stream_until_integrity(source, response_id="id", model="model"))

    assert source.close_calls == 1
