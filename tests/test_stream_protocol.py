import asyncio
import json

import pytest

from app.api import chat
from app.stream_protocol import StreamProtocolTracker, classify_route_integrity


def frame(content=None, finish_reason=None):
    delta = {} if content is None else {"content": content}
    return "data: " + json.dumps({"choices": [{"delta": delta, "finish_reason": finish_reason}]}) + "\n\n"


def complete(tracker, content="ok"):
    for item in (frame(content), frame(finish_reason="stop"), "data: [DONE]\n\n"):
        tracker.observe(item)
    return tracker.finalize()


def test_clean_stream_is_complete_and_hash_is_replay_stable():
    first = complete(StreamProtocolTracker(), "hello")
    second = complete(StreamProtocolTracker(), "hello")
    assert first.ok and first.receipt["response_sha256"] == second.receipt["response_sha256"]
    assert first.receipt["raw_stream_bytes"] > first.receipt["visible_chars"]


@pytest.mark.parametrize(
    "frames, code",
    [
        (["\n\n"], "ERR_PROVIDER_EMPTY_STREAM"),
        (["data: {bad-json}\n\n"], "ERR_STREAM_MALFORMED_ONLY"),
        ([frame("text"), "data: [DONE]\n\n"], "ERR_STREAM_MISSING_FINISH"),
        ([frame("text"), frame(finish_reason="stop")], "ERR_STREAM_MISSING_DONE"),
    ],
)
def test_incomplete_or_malformed_streams_are_terminal_failures(frames, code):
    tracker = StreamProtocolTracker()
    for item in frames:
        tracker.observe(item)
    outcome = tracker.finalize()
    assert not outcome.ok and outcome.code == code


def test_duplicate_terminal_and_post_terminal_content_are_rejected_without_hash_mutation():
    duplicate = StreamProtocolTracker()
    for item in (frame("text"), frame(finish_reason="stop"), "data: [DONE]\n\n", "data: [DONE]\n\n"):
        duplicate.observe(item)
    assert duplicate.finalize().code == "ERR_STREAM_DUPLICATE_TERMINAL"

    late = StreamProtocolTracker()
    for item in (frame("text"), frame(finish_reason="stop"), "data: [DONE]\n\n"):
        late.observe(item)
    before = late.finalize().receipt["response_sha256"]
    late.observe(frame("late"))
    after = late.finalize()
    assert after.code == "ERR_STREAM_POST_TERMINAL_CONTENT"
    assert after.receipt["response_sha256"] == before
    assert after.receipt["quarantined_visible_chars"] == 4


@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"requested_provider": "openai", "requested_model": "gpt-x", "observed_provider": "openai", "observed_model": "gpt-x"}, "route_exact"),
        ({"requested_provider": "minimax", "requested_model": "minimax-x", "observed_provider": "openai", "observed_model": "opal"}, "provider_substituted"),
        ({"requested_provider": "openai", "requested_model": "gpt-x", "observed_provider": "openai", "observed_model": "gpt-y"}, "model_substituted"),
        ({"requested_provider": "openai", "requested_model": "gpt-x", "observed_provider": "", "observed_model": ""}, "route_unknown"),
    ],
)
def test_route_identity_receipts(kwargs, expected):
    assert classify_route_integrity(**kwargs) == expected


def test_guard_turns_late_generator_exception_after_metadata_into_sse_error_terminal():
    metadata = 'data: {"type":"model_metadata","model":"route"}\n\n'

    def source():
        yield metadata
        raise RuntimeError("upstream exploded")

    emitted = "".join(chat._guard_stream_until_integrity(source(), response_id="id", model="model"))
    assert metadata in emitted
    assert "ERR_PROVIDER_EMPTY_STREAM" in emitted
    assert '"finish_reason": "error"' in emitted
    assert not emitted.endswith("data: [DONE]\n\n")


def test_guard_failure_does_not_signal_success_to_naive_done_consumer():
    def source():
        yield frame("partial")
        raise RuntimeError("upstream exploded")

    emitted = list(chat._guard_stream_until_integrity(source(), response_id="id", model="model"))
    naive_completed = any(chunk.strip() == "data: [DONE]" for chunk in emitted)
    assert not naive_completed
    assert any('"object": "error"' in chunk for chunk in emitted)
    assert any('"finish_reason": "error"' in chunk for chunk in emitted)


def test_guard_preserves_generator_exit_without_terminal_emission():
    def source():
        yield frame("partial")
        raise GeneratorExit()

    with pytest.raises(GeneratorExit):
        list(chat._guard_stream_until_integrity(source(), response_id="id", model="model"))


def test_guard_preserves_cancellation():
    def source():
        raise asyncio.CancelledError()
        yield "unreachable"

    with pytest.raises(asyncio.CancelledError):
        list(chat._guard_stream_until_integrity(source(), response_id="id", model="model"))
