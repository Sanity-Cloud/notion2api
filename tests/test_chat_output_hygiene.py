import json

import pytest

from app.api.chat import (
    _build_hygiene_metadata_event,
    _create_lite_stream_generator,
    _create_standard_stream_generator,
    _finalize_visible_reply,
    _select_best_final_reply,
)


def _iter_items(*items):
  return iter(items)


def _parse_sse_chunks(chunks):
    payloads = []
    for chunk in chunks:
        if not chunk.startswith("data: "):
            continue
        body = chunk[6:].strip()
        if body == "[DONE]":
            payloads.append("[DONE]")
            continue
        payloads.append(json.loads(body))
    return payloads


def test_lite_stream_repairs_glued_words_at_finalize():
    source = _iter_items(
        {"type": "content", "text": "Assessment of the"},
        {"type": "content", "text": "proposed"},
        {"type": "content", "text": " edits"},
        {
            "type": "final_content",
            "text": "Assessment of the proposed edits",
            "source_type": "markdown-chat",
        },
    )
    first_item = next(source)

    chunks = list(
        _create_lite_stream_generator("chatcmpl-test", "test-model", first_item, source)
    )
    payloads = _parse_sse_chunks(chunks)
    content = "".join(
        payload["choices"][0]["delta"].get("content", "")
        for payload in payloads
        if isinstance(payload, dict) and payload.get("choices")
    )
    assert content == "Assessment of the proposed edits"


def test_lite_stream_preserves_whitespace_only_chunks_between_tokens():
    source = _iter_items(
        {"type": "content", "text": "Corrected"},
        {"type": "content", "text": " "},
        {"type": "content", "text": "Chairman's"},
        {"type": "content", "text": " "},
        {"type": "content", "text": "Synthesis"},
    )
    first_item = next(source)

    chunks = list(
        _create_lite_stream_generator("chatcmpl-test", "test-model", first_item, source)
    )
    payloads = _parse_sse_chunks(chunks)
    content = "".join(
        payload["choices"][0]["delta"].get("content", "")
        for payload in payloads
        if isinstance(payload, dict) and payload.get("choices")
    )
    assert content == "Corrected Chairman's Synthesis"


def test_lite_stream_suppresses_thinking_and_preserves_visible_content():
    source = _iter_items(
        {"type": "thinking", "text": "Private reasoning."},
        {"type": "content", "text": "Visible answer."},
    )
    first_item = next(source)

    chunks = list(
        _create_lite_stream_generator("chatcmpl-test", "test-model", first_item, source)
    )
    payloads = _parse_sse_chunks(chunks)

    content = "".join(
        payload["choices"][0]["delta"].get("content", "")
        for payload in payloads
        if isinstance(payload, dict) and payload.get("choices")
    )
    assert content == "Visible answer."
    assert payloads[-2]["choices"][0]["finish_reason"] == "stop"


def test_lite_stream_strips_redacted_thinking_from_visible_content():
    source = _iter_items(
        {
            "type": "content",
            "text": "<think>hidden</think>\n\nVisible answer.",
        }
    )
    first_item = next(source)

    chunks = list(
        _create_lite_stream_generator("chatcmpl-test", "test-model", first_item, source)
    )
    payloads = _parse_sse_chunks(chunks)
    content = "".join(
        payload["choices"][0]["delta"].get("content", "")
        for payload in payloads
        if isinstance(payload, dict) and payload.get("choices")
    )
    assert content == "Visible answer."
    assert "<think>" not in content


def test_standard_stream_keeps_thinking_out_of_content_delta():
    source = _iter_items(
        {"type": "thinking", "text": "Private reasoning."},
        {"type": "content", "text": "Visible answer."},
    )
    first_item = next(source)

    chunks = list(
        _create_standard_stream_generator(
            "chatcmpl-test",
            "test-model",
            first_item,
            source,
            client_type="api",
        )
    )
    payloads = _parse_sse_chunks(chunks)

    content = "".join(
        payload["choices"][0]["delta"].get("content", "")
        for payload in payloads
        if isinstance(payload, dict) and payload.get("choices")
    )
    reasoning = "".join(
        payload["choices"][0]["delta"].get("reasoning_content", "")
        for payload in payloads
        if isinstance(payload, dict) and payload.get("choices")
    )
    assert content == "Visible answer."
    assert reasoning == "Private reasoning."


def test_finalize_visible_reply_surfaces_contamination_metadata():
    sanitized, decision, hygiene = _finalize_visible_reply(
        "Sonnet 5owever the issue remains.",
        "",
        "",
    )
    assert isinstance(decision, str)
    assert sanitized
    assert hygiene["visible_contamination_detected"] is True
    assert hygiene["retry_recommended"] is True


def test_build_hygiene_metadata_event_omits_clean_output():
    assert _build_hygiene_metadata_event(
        {
            "hidden_thinking_removed": False,
            "visible_contamination_detected": False,
            "retry_recommended": False,
        }
    ) == ""


@pytest.mark.parametrize(
    "factory",
    [_create_lite_stream_generator, _create_standard_stream_generator],
)
def test_stream_generators_preserve_finish_reason(factory):
    source = _iter_items({"type": "content", "text": "complete"})
    first_item = next(source)
    kwargs = {}
    if factory is _create_standard_stream_generator:
        kwargs["client_type"] = "api"

    chunks = list(factory("chatcmpl-test", "test-model", first_item, source, **kwargs))
    payloads = _parse_sse_chunks(chunks)

    assert payloads[-1] == "[DONE]"
    assert payloads[-2]["choices"][0]["finish_reason"] == "stop"



def test_final_reply_preserves_streamed_spacing_when_tokens_are_equivalent():
    streamed = (
        "MCP tool inventories have been refreshed.\n\n"
        "Two MCP servers failed to reload their tool definitions."
    )
    final = (
        "MCP tool inventories have been refreshed.\n\n"
        "Two MCP servers failed to reload the ir tool definitions."
    )

    selected, decision = _select_best_final_reply(
        streamed,
        final,
        "agent-inference",
    )

    assert selected == streamed
    assert decision == "streamed_whitespace_equivalent"
