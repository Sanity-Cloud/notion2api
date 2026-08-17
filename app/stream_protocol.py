"""Bounded SSE terminal-integrity accounting for provider streams."""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

PARSER_VERSION = "stream-protocol/3"
VALID_FINISH_REASONS = {"stop", "length", "content_filter", "tool_calls", "error"}


class StreamSourceCleanupError(Exception):
    """Client-safe wrapper for a failure while explicitly closing a stream."""

    def __init__(self, original: Exception) -> None:
        self.original_type = type(original).__name__
        super().__init__("stream source cleanup failed")

    @classmethod
    def from_exception(cls, exc: Exception) -> "StreamSourceCleanupError":
        return cls(exc)


@dataclass(frozen=True)
class StreamOutcome:
    ok: bool
    status: str
    code: str
    classification: str
    retriable: bool
    receipt: dict[str, Any]


@dataclass
class StreamProtocolTracker:
    """Track terminal framing without retaining a second copy of stream content."""

    parser_version: str = PARSER_VERSION
    transport_counts: Counter[str] = field(default_factory=Counter)
    semantic_counts: Counter[str] = field(default_factory=Counter)
    raw_stream_bytes: int = 0
    visible_chars: int = 0
    finish_count: int = 0
    done_count: int = 0
    finish_reason: str = ""
    sequence: int = 0
    first_finish_sequence: int | None = None
    first_done_sequence: int | None = None
    post_terminal_count: int = 0
    duplicate_terminal_count: int = 0
    _raw_digest: Any = field(default_factory=hashlib.sha256, repr=False)
    _visible_digest: Any = field(default_factory=hashlib.sha256, repr=False)

    def observe(self, raw: str) -> None:
        encoded = str(raw).encode("utf-8", errors="replace")
        self.raw_stream_bytes += len(encoded)
        self._raw_digest.update(encoded)
        stripped = str(raw).strip()
        if not stripped.startswith("data:"):
            if stripped:
                self.transport_counts["unknown_frame"] += 1
            return
        payload_text = stripped[5:].strip()
        if payload_text == "[DONE]":
            self.sequence += 1
            self.done_count += 1
            self.semantic_counts["done"] += 1
            if self.done_count == 1:
                self.first_done_sequence = self.sequence
            else:
                self.duplicate_terminal_count += 1
            return
        if not payload_text:
            self.transport_counts["empty_data"] += 1
            return
        try:
            payload = json.loads(payload_text)
        except json.JSONDecodeError:
            self.transport_counts["malformed_frame"] += 1
            return
        if not isinstance(payload, dict):
            self.transport_counts["unknown_frame"] += 1
            return

        self.sequence += 1
        choices = payload.get("choices")
        choice = choices[0] if isinstance(choices, list) and choices and isinstance(choices[0], dict) else {}
        delta = choice.get("delta") if isinstance(choice.get("delta"), dict) else {}
        finish_reason = str(choice.get("finish_reason") or "").strip()

        if self.done_count:
            self.post_terminal_count += 1
            self.semantic_counts["post_terminal_content"] += 1
            return
        if finish_reason:
            self.finish_count += 1
            self.semantic_counts["finish"] += 1
            if self.finish_count == 1:
                self.first_finish_sequence = self.sequence
                self.finish_reason = finish_reason
            else:
                self.duplicate_terminal_count += 1
            return
        content = delta.get("content")
        if content not in (None, ""):
            text = str(content)
            self.visible_chars += len(text)
            self._visible_digest.update(text.encode("utf-8", errors="replace"))
            self.semantic_counts["content_delta"] += 1
        elif isinstance(delta.get("tool_calls"), list):
            self.semantic_counts["tool_call_delta"] += 1
        elif delta.get("role"):
            self.semantic_counts["role_delta"] += 1
        else:
            event_type = str(payload.get("type") or "").strip().lower()
            self.semantic_counts[event_type or "metadata"] += 1

    def finalize(
        self,
        *,
        source_error: BaseException | None = None,
        cleanup_attempted: bool = False,
        cleanup_error: StreamSourceCleanupError | None = None,
    ) -> StreamOutcome:
        status = "completed"
        code = ""
        classification = "success"
        retriable = False

        if isinstance(source_error, StreamSourceCleanupError):
            status, code, classification, retriable = (
                "failed",
                "ERR_STREAM_SOURCE_CLEANUP",
                "stream_source_cleanup_failed",
                True,
            )
        elif source_error is not None:
            status = "failed"
            retriable = True
            if not self.visible_chars and not self.finish_count and not self.done_count:
                code, classification = "ERR_PROVIDER_EMPTY_STREAM", "stream_empty_no_terminal"
            else:
                code, classification = "ERR_STREAM_INTERRUPTED", "stream_interrupted"
        elif self.duplicate_terminal_count:
            status, code, classification = (
                "failed",
                "ERR_STREAM_DUPLICATE_TERMINAL",
                "stream_duplicate_terminal",
            )
        elif self.post_terminal_count:
            status, code, classification = (
                "quarantined",
                "ERR_STREAM_POST_TERMINAL_CONTENT",
                "stream_post_terminal_content",
            )
        elif self.finish_count == 0:
            status, code, classification, retriable = (
                "failed",
                "ERR_STREAM_MISSING_FINISH",
                "stream_missing_finish",
                True,
            )
        elif self.done_count == 0:
            status, code, classification, retriable = (
                "failed",
                "ERR_STREAM_MISSING_DONE",
                "stream_missing_done",
                True,
            )
        elif (
            self.first_finish_sequence is not None
            and self.first_done_sequence is not None
            and self.first_finish_sequence > self.first_done_sequence
        ):
            status, code, classification = (
                "failed",
                "ERR_STREAM_TERMINAL_ORDER",
                "stream_terminal_order_invalid",
            )
        elif self.finish_reason not in VALID_FINISH_REASONS:
            status, code, classification = (
                "failed",
                "ERR_STREAM_FINISH_REASON",
                "stream_finish_reason_invalid",
            )

        receipt = {
            "parser_version": self.parser_version,
            "status": status,
            "classification": classification,
            "raw_stream_bytes": self.raw_stream_bytes,
            "raw_stream_sha256": self._raw_digest.hexdigest(),
            "response_sha256": self._visible_digest.hexdigest(),
            "visible_chars": self.visible_chars,
            "finish_count": self.finish_count,
            "done_count": self.done_count,
            "finish_reason": self.finish_reason or None,
            "post_terminal_count": self.post_terminal_count,
            "duplicate_terminal_count": self.duplicate_terminal_count,
            "transport_event_counts": dict(self.transport_counts),
            "semantic_event_counts": dict(self.semantic_counts),
            "source_error_type": type(source_error).__name__ if source_error else None,
            "cleanup": {
                "attempted": cleanup_attempted,
                "failed": cleanup_error is not None,
                "error_type": cleanup_error.original_type if cleanup_error else None,
            },
        }
        return StreamOutcome(
            ok=status == "completed",
            status=status,
            code=code,
            classification=classification,
            retriable=retriable,
            receipt=receipt,
        )
