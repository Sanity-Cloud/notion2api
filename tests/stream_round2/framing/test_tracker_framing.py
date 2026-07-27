"""Deterministic framing evidence for the current StreamProtocolTracker boundary."""
from __future__ import annotations

import itertools
import json

import pytest

from app.stream_protocol import StreamProtocolTracker, classify_sse_frame


def event(content: str | None = None, finish: str | None = None, newline: str = "\n") -> str:
    delta = {} if content is None else {"content": content}
    payload = {"choices": [{"delta": delta, "finish_reason": finish}]}
    return "data: " + json.dumps(payload, ensure_ascii=False) + newline + newline


def outcome(*chunks: bytes | str, error: BaseException | None = None):
    tracker = StreamProtocolTracker()
    for chunk in chunks:
        tracker.observe(chunk)
    return tracker, tracker.finalize(source_error=error)


@pytest.mark.parametrize("newline", ["\n", "\r\n"])
def test_complete_logical_events_are_accepted_for_lf_and_crlf(newline: str):
    tracker, result = outcome(event("ok", newline=newline), event(finish="stop", newline=newline), f"data: [DONE]{newline}{newline}")
    assert result.classification == "success"
    assert tracker.semantic_counts == {"content_delta": 1, "finish": 1, "done": 1}


def test_blank_and_comment_keepalive_are_transport_only_and_do_not_contaminate_application_counts():
    tracker, result = outcome("\n", "\r\n", ": ping\n\n", event("ok"), event(finish="stop"), "data: [DONE]\n\n")
    assert result.classification == "success"
    assert tracker.transport_counts == {"sse_boundary": 2, "keepalive": 1}
    assert tracker.semantic_counts == {"content_delta": 1, "finish": 1, "done": 1}
    assert tracker.sequence == 3


@pytest.mark.parametrize(
    ("raw", "event_type", "normalization"),
    [
        ("data: {bad}\n\n", "malformed_frame", "invalid_json"),
        ("event: ping\n\n", "unknown_frame", "missing_data_field"),
        ("  data: {}\n\n", "unknown_frame", "missing_data_field"),
        ("data: \n\n", "sse_frame", "empty_data_field"),
    ],
)
def test_malformed_and_non_data_frames_are_distinct_from_keepalives(raw, event_type, normalization):
    frame = classify_sse_frame(raw)
    assert frame.layer == "transport"
    assert frame.event_type == event_type
    assert frame.normalization_status == normalization


def test_event_id_retry_and_multiple_data_lines_are_unsupported_at_tracker_boundary():
    payload = json.dumps({"choices": [{"delta": {"content": "x"}, "finish_reason": None}]})
    multiline = f"event: message\nid: 7\nretry: 1000\ndata: {payload[:25]}\ndata: {payload[25:]}\n\n"
    tracker, result = outcome(multiline, event(finish="stop"), "data: [DONE]\n\n")
    assert tracker.transport_counts["unknown_frame"] == 1
    assert tracker.semantic_counts == {"finish": 1, "done": 1}
    assert result.classification == "stream_empty_visible_output"


def test_multiple_logical_events_in_one_chunk_preserve_current_unsupported_framing_evidence():
    packed = event("first") + event(finish="stop") + "data: [DONE]\n\n"
    tracker, result = outcome(packed)
    assert tracker.transport_counts == {"malformed_frame": 1}
    assert result.classification == "stream_malformed_only"


def test_leading_whitespace_is_unknown_but_trailing_whitespace_is_normalized():
    payload = json.dumps({"choices": [{"delta": {"content": "x"}, "finish_reason": None}]})
    leading = classify_sse_frame("  data: " + payload + "\n\n")
    trailing = classify_sse_frame("data: " + payload + "  \n\n")
    assert (leading.event_type, leading.normalization_status) == ("unknown_frame", "missing_data_field")
    assert trailing.event_type == "content_delta"


def test_json_and_utf8_fragmentation_preserve_current_empty_visible_output_evidence():
    json_fragments = (b'data: {"choices":[{"delta":{"content":"a', b'b"},"finish_reason":null}]}\n\n')
    utf8_fragments = (b'data: {"choices":[{"delta":{"content":"\xf0\x9f', b'\x98\x80"},"finish_reason":null}]}\n\n')
    for fragments in (json_fragments, utf8_fragments):
        tracker, result = outcome(*fragments, event(finish="stop"), "data: [DONE]\n\n")
        assert tracker.semantic_counts == {"finish": 1, "done": 1}
        assert result.classification == "stream_empty_visible_output"
        assert tracker.transport_counts["malformed_frame"] >= 1


def test_connection_close_between_fragments_is_interrupted_without_assembling_partial_bytes():
    tracker, result = outcome(b'data: {"choices":[{"delta":{"content":"par', error=ConnectionError("closed"))
    assert tracker.transport_counts == {"malformed_frame": 1}
    assert result.classification == "stream_empty_no_terminal"
    assert result.code == "ERR_PROVIDER_EMPTY_STREAM"


def test_done_before_finish_is_quarantined_before_terminal_order_branch():
    tracker, result = outcome(event("x"), "data: [DONE]\n\n", event(finish="stop"))
    assert result.classification == "stream_post_terminal_content"
    assert result.code == "ERR_STREAM_POST_TERMINAL_CONTENT"
    assert tracker.finish_count == 0
    assert tracker.first_finish_sequence is None
    assert tracker.post_terminal_count == 1


def test_terminal_order_invalid_is_unreachable_from_normal_finish_done_orders():
    terminal = {"finish": event(finish="stop"), "done": "data: [DONE]\n\n"}
    observed = set()
    for order in itertools.permutations(("finish", "done")):
        _tracker, result = outcome(event("x"), *(terminal[name] for name in order))
        observed.add(result.classification)
    assert observed == {"success", "stream_post_terminal_content"}
    assert "stream_terminal_order_invalid" not in observed
