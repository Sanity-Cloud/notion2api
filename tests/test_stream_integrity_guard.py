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
    def __init__(self, error: BaseException, *, close_error: bool = False) -> None:
        self.error = error
        self.close_error = close_error
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self) -> str:
        raise self.error

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error:
            raise RuntimeError("close failure")


class _ClosableSequence:
    def __init__(self, chunks, *, close_error: bool = False, error: Exception | None = None) -> None:
        self.chunks = iter(chunks)
        self.close_error = close_error
        self.error = error
        self.close_calls = 0

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return next(self.chunks)
        except StopIteration:
            if self.error is not None:
                raise self.error
            raise

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error:
            raise RuntimeError("close failure")


def _clean_chunks():
    return [
        chat._build_stream_chunk("id", "model", content="safe"),
        chat._build_stream_chunk("id", "model", finish_reason="stop"),
        "data: [DONE]\n\n",
    ]


def test_guard_clean_close_preserves_success_bytes_and_closes_once():
    source = _ClosableSequence(_clean_chunks())
    assert list(chat._guard_stream_until_integrity(source, response_id="id", model="model")) == _clean_chunks()
    assert source.close_calls == 1


def test_guard_clean_stream_close_failure_fails_closed_with_cleanup_receipt():
    source = _ClosableSequence(_clean_chunks(), close_error=True)
    emitted = "".join(chat._guard_stream_until_integrity(source, response_id="id", model="model"))
    assert "safe" not in emitted
    assert "data: [DONE]" not in emitted
    assert "ERR_STREAM_SOURCE_CLEANUP" in emitted
    assert "stream_source_cleanup_failed" in emitted
    assert '"error_type": "RuntimeError"' in emitted
    assert '"error_message": "redacted"' in emitted
    assert '"finish_reason": "error"' in emitted
    assert source.close_calls == 1


def test_guard_primary_source_error_wins_over_cleanup_failure():
    source = _ClosableSequence([chat._build_stream_chunk("id", "model", content="partial")], error=RuntimeError("source failure"), close_error=True)
    emitted = "".join(chat._guard_stream_until_integrity(source, response_id="id", model="model"))
    assert "partial" not in emitted
    assert "ERR_STREAM_INTERRUPTED" in emitted
    assert "stream_interrupted" in emitted
    assert '"error_type": "RuntimeError"' in emitted
    assert source.close_calls == 1


def test_guard_limit_close_failure_remains_fail_closed_and_closes_once(monkeypatch):
    monkeypatch.setattr(chat, "MAX_GUARDED_STREAM_BUFFER_CHARS", 10)
    source = _ClosableSequence([chat._build_stream_chunk("id", "model", content="long-output")], close_error=True)
    emitted = "".join(chat._guard_stream_until_integrity(source, response_id="id", model="model"))
    assert "long-output" not in emitted
    assert "data: [DONE]" not in emitted
    assert "ERR_STREAM_SOURCE_CLEANUP" in emitted
    assert '"finish_reason": "error"' in emitted
    assert source.close_calls == 1


def test_guard_cancellation_closes_source_without_terminal_emission():
    source = _RaisingClosableSource(asyncio.CancelledError(), close_error=True)

    with pytest.raises(asyncio.CancelledError):
        list(chat._guard_stream_until_integrity(source, response_id="id", model="model"))

    assert source.close_calls == 1


def test_guard_generator_exit_closes_source_without_terminal_emission():
    source = _RaisingClosableSource(GeneratorExit(), close_error=True)

    with pytest.raises(GeneratorExit):
        list(chat._guard_stream_until_integrity(source, response_id="id", model="model"))

    assert source.close_calls == 1


@pytest.mark.parametrize("factory", [chat._create_standard_stream_generator, chat._create_lite_stream_generator])
def test_guard_chain_close_failure_closes_upstream_once_and_fails_closed(factory):
    upstream = _ClosableSequence([], close_error=True)
    chained = factory("id", "model", {"type": "content", "text": "safe"}, upstream)

    emitted = "".join(chat._guard_stream_until_integrity(chained, response_id="id", model="model"))

    assert upstream.close_calls == 1
    assert "safe" not in emitted
    assert "ERR_STREAM_SOURCE_CLEANUP" in emitted
    assert '"finish_reason": "error"' in emitted
    assert "data: [DONE]" not in emitted


@pytest.mark.parametrize(
    "factory",
    [chat._create_standard_stream_generator, chat._create_lite_stream_generator],
)
def test_guard_captures_nested_generator_cleanup_failure(factory):
    upstream = _ClosableSequence([], close_error=True)
    wrapped = factory(
        "id",
        "model",
        {"type": "content", "text": "nested-safe"},
        upstream,
    )

    emitted = "".join(
        chat._guard_stream_until_integrity(wrapped, response_id="id", model="model")
    )

    assert upstream.close_calls == 1
    assert "nested-safe" not in emitted
    assert "ERR_STREAM_SOURCE_CLEANUP" in emitted
    assert "stream_source_cleanup_failed" in emitted
    assert '"finish_reason": "error"' in emitted
    assert "data: [DONE]" not in emitted
