import json

import pytest

from app import stream_parser_safe
from app.api.chat import (
    _create_lite_stream_generator,
    _create_standard_stream_generator,
)
from app.stream_parser import parse_stream


class DummyResponse:
    def __init__(self, lines=None):
        self._lines = lines or []

    def iter_lines(self, decode_unicode=True):
        del decode_unicode
        yield from self._lines


def test_buffered_thinking_is_not_promoted_after_final_content(monkeypatch):
    def fake_parse_stream(_response):
        yield {"type": "thinking", "text": "The user is asking for a greeting."}
        yield {
            "type": "final_content",
            "text": "Hello.",
            "source_type": "agent-inference",
        }

    monkeypatch.setattr(stream_parser_safe, "_parse_stream", fake_parse_stream)

    assert list(stream_parser_safe.parse_stream(DummyResponse())) == [
        {"type": "final_content", "text": "Hello.", "source_type": "agent-inference"}
    ]


def test_buffered_thinking_fallback_remains_for_legacy_answer_segments(monkeypatch):
    def fake_parse_stream(_response):
        yield {"type": "thinking", "text": "Legacy answer-only agent-inference text."}

    monkeypatch.setattr(stream_parser_safe, "_parse_stream", fake_parse_stream)

    assert list(stream_parser_safe.parse_stream(DummyResponse())) == [
        {"type": "content", "text": "Legacy answer-only agent-inference text."}
    ]


def test_mixed_initial_value_array_keeps_thinking_out_of_visible_content():
    line = json.dumps(
        {
            "type": "patch",
            "v": [
                {
                    "o": "a",
                    "p": "/s/-",
                    "v": {
                        "type": "agent-inference",
                        "value": [
                            {"type": "thinking", "content": "Private reasoning."},
                            {"type": "text", "content": "Visible answer."},
                        ],
                    },
                }
            ],
        }
    )

    assert list(parse_stream(DummyResponse([line]))) == [
        {"type": "thinking", "text": "Private reasoning."},
        {"type": "content", "text": "Visible answer."},
    ]
    assert list(stream_parser_safe.parse_stream(DummyResponse([line]))) == [
        {"type": "content", "text": "Visible answer."},
    ]


def test_parser_emits_completion_only_for_finished_at_patch():
    response = DummyResponse(
        [
            json.dumps(
                {
                    "type": "patch",
                    "v": [
                        {"o": "x", "p": "/s/1/value/0/content", "v": "answer"},
                        {"o": "a", "p": "/s/1/finishedAt", "v": 1782249965672},
                    ],
                }
            )
        ]
    )

    events = list(parse_stream(response))

    assert {
        "type": "stream_complete",
        "finished_at": 1782249965672,
        "segment_index": 1,
    } in events


def _broken_items():
    yield {"type": "content", "text": "partial"}
    raise RuntimeError("upstream stream broke")


@pytest.mark.parametrize(
    "factory",
    [_create_lite_stream_generator, _create_standard_stream_generator],
)
def test_interrupted_proxy_stream_does_not_emit_done(factory):
    source = _broken_items()
    first_item = next(source)
    proxy_stream = factory("chatcmpl-test", "test-model", first_item, source)

    first_chunk = next(proxy_stream)
    assert "partial" in first_chunk

    with pytest.raises(RuntimeError, match="upstream stream broke"):
        next(proxy_stream)


@pytest.mark.parametrize(
    "factory",
    [_create_lite_stream_generator, _create_standard_stream_generator],
)
def test_successful_proxy_stream_emits_done(factory):
    source = iter([{"type": "content", "text": "complete"}])
    first_item = next(source)

    chunks = list(factory("chatcmpl-test", "test-model", first_item, source))

    assert chunks[-1] == "data: [DONE]\n\n"
    assert '"finish_reason": "stop"' in chunks[-2]


def test_stream_parser_safe_yields_unique_thinking(monkeypatch):
    """Verify stream_parser_safe appends thinking content if it is not duplicate of content."""
    mock_items = [
        {"type": "thinking", "text": "This is Kimi's actual detailed answer."},
        {"type": "content", "text": "[1] Sources."},
    ]
    monkeypatch.setattr(
        stream_parser_safe, "_parse_stream", lambda res: iter(mock_items)
    )
    res = list(stream_parser_safe.parse_stream(DummyResponse()))

    event_types = [item["type"] for item in res]
    assert "content" in event_types

    texts = [item.get("text", "") for item in res if item["type"] == "content"]
    assert "[1] Sources." in texts
    assert "\n\nThis is Kimi's actual detailed answer." in texts


def test_stream_parser_safe_suppresses_duplicate_thinking(monkeypatch):
    """Verify stream_parser_safe suppresses thinking if it is identical to yielded content."""
    mock_items = [
        {"type": "thinking", "text": "This is identical answer."},
        {"type": "content", "text": "This is identical answer."},
    ]
    monkeypatch.setattr(
        stream_parser_safe, "_parse_stream", lambda res: iter(mock_items)
    )
    res = list(stream_parser_safe.parse_stream(DummyResponse()))

    texts = [item.get("text", "") for item in res if item["type"] == "content"]
    assert len(texts) == 1
    assert texts[0] == "This is identical answer."


def test_parser_rejects_unscoped_nested_finished_at():
    line = json.dumps(
        {
            "type": "content",
            "value": {"metadata": {"finishedAt": "2026-07-27T12:00:00Z"}},
        }
    )
    assert not any(
        event.get("type") == "stream_complete"
        for event in parse_stream(DummyResponse([line]))
    )


def test_parser_emits_only_one_valid_completion_across_envelope_and_patch():
    lines = [
        json.dumps(
            {
                "type": "status",
                "value": {"finishedAt": "2026-07-27T12:00:00Z"},
            }
        ),
        json.dumps(
            {
                "type": "patch",
                "v": [{"o": "a", "p": "/s/1/finishedAt", "v": 1785153600000}],
            }
        ),
    ]
    completions = [
        event
        for event in parse_stream(DummyResponse(lines))
        if event.get("type") == "stream_complete"
    ]
    assert completions == [
        {"type": "stream_complete", "finished_at": "2026-07-27T12:00:00Z"}
    ]


@pytest.mark.parametrize(
    "invalid_value", [None, False, True, 0, -1, "", "pending", [], {}]
)
def test_parser_rejects_invalid_finished_at_values(invalid_value):
    line = json.dumps(
        {
            "type": "patch",
            "v": [{"o": "a", "p": "/s/1/finishedAt", "v": invalid_value}],
        }
    )
    assert not any(
        event.get("type") == "stream_complete"
        for event in parse_stream(DummyResponse([line]))
    )


def test_parser_rejects_finished_at_patch_outside_segment_path():
    line = json.dumps(
        {
            "type": "patch",
            "v": [{"o": "a", "p": "/metadata/finishedAt", "v": 1785153600000}],
        }
    )
    assert not any(
        event.get("type") == "stream_complete"
        for event in parse_stream(DummyResponse([line]))
    )



def test_record_map_error_step_becomes_upstream_error():
    line = json.dumps(
        {
            "type": "record-map",
            "recordMap": {
                "thread_message": {
                    "message-1": {
                        "value": {
                            "value": {
                                "step": {
                                    "type": "error",
                                    "message": "Something went wrong. Please try again later.",
                                    "subType": "temporarily-unavailable",
                                    "traceId": "trace-1",
                                    "isRetryable": False,
                                }
                            }
                        }
                    }
                }
            },
        }
    )

    events = list(parse_stream(DummyResponse([line])))

    assert events == [
        {
            "type": "upstream_error",
            "status_code": 502,
            "code": "temporarily-unavailable",
            "message": "Something went wrong. Please try again later.",
            "retriable": False,
            "raw_type": "record-map:error",
            "trace_id": "trace-1",
        }
    ]


def test_patch_error_step_becomes_upstream_error():
    line = json.dumps(
        {
            "type": "patch",
            "v": [
                {
                    "o": "a",
                    "p": "/s/-",
                    "v": {
                        "type": "error",
                        "message": "Something went wrong.",
                        "subType": "temporarily-unavailable",
                        "isRetryable": False,
                    },
                }
            ],
        }
    )

    events = list(parse_stream(DummyResponse([line])))

    assert events[0]["type"] == "upstream_error"
    assert events[0]["code"] == "temporarily-unavailable"
    assert events[0]["retriable"] is False
