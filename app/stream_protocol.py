"""Versioned SSE classification and terminal-integrity tracking.

The module is deliberately provider-neutral. It separates transport framing from
semantic events, preserves opaque frames, and produces deterministic terminal
receipts without retaining a second copy of the complete stream.
"""
from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

PARSER_VERSION = "stream-protocol/2"
REASONING_KEYS = {"thinking", "thinking_content", "reasoning", "reasoning_content"}
VALID_FINISH_REASONS = {"stop", "length", "content_filter", "tool_calls", "error"}


@dataclass(frozen=True)
class ClassifiedFrame:
    layer: str
    event_type: str
    raw_bytes: int
    raw_sha256: str
    normalization_status: str = "normalized"
    payload: Any = None
    visible_text: str = ""
    finish_reason: str = ""

    @property
    def is_boundary(self) -> bool:
        return self.layer == "transport" and self.event_type == "sse_boundary"


@dataclass(frozen=True)
class StreamOutcome:
    ok: bool
    status: str
    code: str
    classification: str
    retriable: bool
    receipt: dict[str, Any]


def _as_bytes(raw: bytes | str) -> bytes:
    return raw if isinstance(raw, bytes) else str(raw).encode("utf-8", errors="replace")


def classify_sse_frame(raw: bytes | str) -> ClassifiedFrame:
    """Classify one SSE line or one complete ``data: ...\n\n`` chunk.

    Empty physical lines are transport boundaries and never become semantic
    parse failures. Non-empty unknown or malformed frames remain observable.
    """
    raw_value = _as_bytes(raw)
    raw_hash = hashlib.sha256(raw_value).hexdigest()
    text = raw_value.decode("utf-8", errors="replace")
    line = text.rstrip("\r\n")

    if not line:
        return ClassifiedFrame("transport", "sse_boundary", len(raw_value), raw_hash)
    if line.startswith(":"):
        return ClassifiedFrame("transport", "keepalive", len(raw_value), raw_hash)
    if not line.startswith("data:"):
        return ClassifiedFrame(
            "transport",
            "unknown_frame",
            len(raw_value),
            raw_hash,
            normalization_status="missing_data_field",
            payload=line,
        )

    payload_text = line[5:].strip()
    if not payload_text:
        return ClassifiedFrame(
            "transport",
            "sse_frame",
            len(raw_value),
            raw_hash,
            normalization_status="empty_data_field",
        )
    if payload_text == "[DONE]":
        return ClassifiedFrame("semantic", "done", len(raw_value), raw_hash, payload="[DONE]")

    try:
        payload = json.loads(payload_text)
    except json.JSONDecodeError:
        return ClassifiedFrame(
            "transport",
            "malformed_frame",
            len(raw_value),
            raw_hash,
            normalization_status="invalid_json",
            payload=payload_text,
        )
    if not isinstance(payload, dict):
        return ClassifiedFrame(
            "transport",
            "unknown_frame",
            len(raw_value),
            raw_hash,
            normalization_status="non_object_payload",
            payload=payload,
        )

    explicit_type = str(payload.get("type") or "").strip().lower()
    explicit_map = {
        "model_metadata": "model_metadata",
        "output_hygiene": "output_hygiene",
        "thinking": "reasoning_metadata",
        "thinking_chunk": "reasoning_metadata",
        "search_metadata": "citation",
        "stream_error": "stream_error",
        "usage": "usage_metadata",
    }
    if explicit_type in explicit_map:
        visible = str(payload.get("text") or "") if explicit_type == "thinking_chunk" else ""
        return ClassifiedFrame(
            "semantic",
            explicit_map[explicit_type],
            len(raw_value),
            raw_hash,
            payload=payload,
            visible_text=visible,
        )

    choices = payload.get("choices")
    if isinstance(choices, list) and choices:
        choice = choices[0] if isinstance(choices[0], dict) else {}
        finish_reason = str(choice.get("finish_reason") or "").strip()
        if finish_reason:
            return ClassifiedFrame(
                "semantic",
                "finish",
                len(raw_value),
                raw_hash,
                payload=payload,
                finish_reason=finish_reason,
            )
        delta = choice.get("delta") if isinstance(choice.get("delta"), dict) else {}
        if isinstance(delta.get("tool_calls"), list):
            return ClassifiedFrame("semantic", "tool_call_delta", len(raw_value), raw_hash, payload=payload)
        if any(key in delta for key in REASONING_KEYS):
            return ClassifiedFrame("semantic", "reasoning_metadata", len(raw_value), raw_hash, payload=payload)
        if "content" in delta:
            visible = str(delta.get("content") or "")
            return ClassifiedFrame(
                "semantic", "content_delta", len(raw_value), raw_hash, payload=payload, visible_text=visible
            )
        if "role" in delta:
            return ClassifiedFrame("semantic", "role_delta", len(raw_value), raw_hash, payload=payload)

    if isinstance(payload.get("usage"), dict):
        return ClassifiedFrame("semantic", "usage_metadata", len(raw_value), raw_hash, payload=payload)
    return ClassifiedFrame(
        "transport",
        "unknown_frame",
        len(raw_value),
        raw_hash,
        normalization_status="schema_mismatch",
        payload=payload,
    )


@dataclass
class StreamProtocolTracker:
    """Incremental protocol receipt with bounded constant-size accounting state."""

    parser_version: str = PARSER_VERSION
    transport_counts: Counter[str] = field(default_factory=Counter)
    semantic_counts: Counter[str] = field(default_factory=Counter)
    raw_stream_bytes: int = 0
    visible_chars: int = 0
    sequence: int = 0
    finish_count: int = 0
    done_count: int = 0
    first_finish_sequence: int | None = None
    first_done_sequence: int | None = None
    finish_reason: str = ""
    post_terminal_count: int = 0
    duplicate_terminal_count: int = 0
    quarantined_visible_chars: int = 0
    _raw_digest: Any = field(default_factory=hashlib.sha256, repr=False)
    _visible_digest: Any = field(default_factory=hashlib.sha256, repr=False)

    def observe(self, raw: bytes | str) -> ClassifiedFrame:
        raw_value = _as_bytes(raw)
        frame = classify_sse_frame(raw_value)
        self.raw_stream_bytes += len(raw_value)
        self._raw_digest.update(raw_value)

        if frame.layer == "transport":
            self.transport_counts[frame.event_type] += 1
            return frame

        self.sequence += 1
        event_type = frame.event_type
        if self.done_count and event_type != "done":
            self.post_terminal_count += 1
            if frame.visible_text:
                self.quarantined_visible_chars += len(frame.visible_text)
            self.semantic_counts["post_terminal_content"] += 1
            return frame

        self.semantic_counts[event_type] += 1
        if event_type == "finish":
            self.finish_count += 1
            if self.finish_count == 1:
                self.first_finish_sequence = self.sequence
                self.finish_reason = frame.finish_reason
            else:
                self.duplicate_terminal_count += 1
                self.semantic_counts["duplicate_event"] += 1
        elif event_type == "done":
            self.done_count += 1
            if self.done_count == 1:
                self.first_done_sequence = self.sequence
            else:
                self.duplicate_terminal_count += 1
                self.semantic_counts["duplicate_event"] += 1
        elif frame.visible_text:
            encoded = frame.visible_text.encode("utf-8", errors="replace")
            self.visible_chars += len(frame.visible_text)
            self._visible_digest.update(encoded)
        return frame

    def finalize(self, *, source_error: BaseException | None = None) -> StreamOutcome:
        structural_status = "valid"
        content_status = "valid" if self.visible_chars else "empty"
        status = "completed"
        code = ""
        classification = "success"
        retriable = False

        if source_error is not None:
            status = "failed"
            retriable = True
            if self.visible_chars == 0 and self.finish_count == 0 and self.done_count == 0:
                code = "ERR_PROVIDER_EMPTY_STREAM"
                classification = "stream_empty_no_terminal"
            else:
                code = "ERR_STREAM_INTERRUPTED"
                classification = "stream_interrupted"
            structural_status = "interrupted"
        elif self.duplicate_terminal_count:
            status = "failed"
            code = "ERR_STREAM_DUPLICATE_TERMINAL"
            classification = "stream_duplicate_terminal"
            structural_status = "invalid"
        elif self.post_terminal_count:
            status = "quarantined"
            code = "ERR_STREAM_POST_TERMINAL_CONTENT"
            classification = "stream_post_terminal_content"
            structural_status = "invalid"
        elif (
            self.transport_counts["malformed_frame"] + self.transport_counts["unknown_frame"]
            and self.visible_chars == 0
            and self.finish_count == 0
            and self.done_count == 0
        ):
            status = "failed"
            code = "ERR_STREAM_MALFORMED_ONLY"
            classification = "stream_malformed_only"
            structural_status = "invalid"
            retriable = True
        elif self.finish_count == 0 and self.done_count == 0 and self.visible_chars == 0:
            status = "failed"
            code = "ERR_PROVIDER_EMPTY_STREAM"
            classification = "stream_empty_no_terminal"
            structural_status = "missing_terminal"
            retriable = True
        elif self.finish_count == 0:
            status = "failed"
            code = "ERR_STREAM_MISSING_FINISH"
            classification = "stream_missing_finish"
            structural_status = "missing_finish"
            retriable = True
        elif self.done_count == 0:
            status = "failed"
            code = "ERR_STREAM_MISSING_DONE"
            classification = "stream_missing_done"
            structural_status = "missing_done"
            retriable = True
        elif (
            self.first_finish_sequence is not None
            and self.first_done_sequence is not None
            and self.first_finish_sequence > self.first_done_sequence
        ):
            status = "failed"
            code = "ERR_STREAM_TERMINAL_ORDER"
            classification = "stream_terminal_order_invalid"
            structural_status = "invalid"
        elif self.finish_reason not in VALID_FINISH_REASONS:
            status = "failed"
            code = "ERR_STREAM_FINISH_REASON"
            classification = "stream_finish_reason_invalid"
            structural_status = "invalid"
        elif self.visible_chars == 0:
            status = "failed"
            code = "ERR_STREAM_EMPTY_VISIBLE_OUTPUT"
            classification = "stream_empty_visible_output"
            content_status = "invalid"
            retriable = True

        receipt = {
            "parser_version": self.parser_version,
            "status": status,
            "classification": classification,
            "structural_status": structural_status,
            "content_integrity_status": content_status,
            "transport_event_counts": dict(self.transport_counts),
            "semantic_event_counts": dict(self.semantic_counts),
            "normalized_sequence_count": self.sequence,
            "raw_stream_bytes": self.raw_stream_bytes,
            "raw_stream_sha256": self._raw_digest.hexdigest(),
            "visible_chars": self.visible_chars,
            "response_sha256": self._visible_digest.hexdigest(),
            "finish_count": self.finish_count,
            "done_count": self.done_count,
            "finish_reason": self.finish_reason or None,
            "post_terminal_count": self.post_terminal_count,
            "duplicate_terminal_count": self.duplicate_terminal_count,
            "quarantined_visible_chars": self.quarantined_visible_chars,
            "source_error_type": type(source_error).__name__ if source_error is not None else None,
        }
        return StreamOutcome(
            ok=status == "completed",
            status=status,
            code=code,
            classification=classification,
            retriable=retriable,
            receipt=receipt,
        )


def infer_provider_from_model(model: str | None) -> str:
    """Conservative provider-family inference for route receipts."""
    value = str(model or "").strip().lower()
    if not value:
        return ""
    markers = (
        (("anthropic", "claude"), "anthropic"),
        (("gemini", "vertex-"), "gemini"),
        (("xigua", "grok", "xai"), "xai"),
        (("minimax",), "minimax"),
        (("kimi",), "kimi"),
        (("glm",), "glm"),
        (("deepseek",), "deepseek"),
        (("orchid", "opal", "olive", "openai", "gpt-"), "openai"),
    )
    for candidates, provider in markers:
        if any(marker in value for marker in candidates):
            return provider
    return ""


def classify_route_integrity(
    *,
    requested_provider: str | None,
    requested_model: str | None,
    observed_provider: str | None,
    observed_model: str | None,
) -> str:
    requested_provider_value = str(requested_provider or "").strip().lower()
    observed_provider_value = str(observed_provider or "").strip().lower()
    requested_model_value = str(requested_model or "").strip().lower()
    observed_model_value = str(observed_model or "").strip().lower()
    if not observed_model_value and not observed_provider_value:
        return "route_unknown"
    if requested_provider_value and observed_provider_value and requested_provider_value != observed_provider_value:
        return "provider_substituted"
    if requested_model_value and observed_model_value and requested_model_value != observed_model_value:
        return "model_substituted"
    if requested_model_value and observed_model_value and requested_model_value == observed_model_value:
        return "route_exact"
    return "route_unknown"
