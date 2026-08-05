from __future__ import annotations

import os
import threading
import time
from collections import Counter
from typing import Any

_LOCK = threading.Lock()
_COUNTERS: Counter[str] = Counter()
_LAST_WARNING_AT: dict[tuple[str, str], float] = {}
_SUPPRESSED_BY_KEY: Counter[tuple[str, str]] = Counter()
_LAST_EVENT: dict[str, Any] = {}


def _warning_interval_seconds() -> float:
    try:
        value = float(os.getenv("NOTION2API_COMPRESSION_WARNING_INTERVAL_SECONDS", "300"))
    except (TypeError, ValueError):
        value = 300.0
    return max(1.0, min(value, 86400.0))


def record_compression_event(event: str, **fields: Any) -> None:
    now_ms = int(time.time() * 1000)
    with _LOCK:
        _COUNTERS[str(event)] += 1
        _LAST_EVENT.clear()
        _LAST_EVENT.update(
            {
                "event": str(event),
                "updated_at": now_ms,
                **{
                    str(key): value
                    for key, value in fields.items()
                    if value not in (None, "")
                },
            }
        )


def log_compression_warning(
    logger: Any,
    message: str,
    *,
    event: str,
    conversation_id: str,
    **fields: Any,
) -> bool:
    """Emit a warning at most once per event/conversation interval.

    Returns True when a log record was emitted and False when it was coalesced.
    Every observation remains visible through counters.
    """

    now = time.monotonic()
    key = (str(event), str(conversation_id))
    with _LOCK:
        _COUNTERS[str(event)] += 1
        last_at = _LAST_WARNING_AT.get(key)
        if last_at is not None and now - last_at < _warning_interval_seconds():
            _SUPPRESSED_BY_KEY[key] += 1
            _COUNTERS["warnings_suppressed"] += 1
            _LAST_EVENT.clear()
            _LAST_EVENT.update(
                {
                    "event": str(event),
                    "conversation_id": str(conversation_id),
                    "coalesced": True,
                    "updated_at": int(time.time() * 1000),
                }
            )
            return False
        suppressed = int(_SUPPRESSED_BY_KEY.pop(key, 0))
        _LAST_WARNING_AT[key] = now
        _COUNTERS["warnings_emitted"] += 1
        _LAST_EVENT.clear()
        _LAST_EVENT.update(
            {
                "event": str(event),
                "conversation_id": str(conversation_id),
                "coalesced": False,
                "suppressed_since_last_emit": suppressed,
                "updated_at": int(time.time() * 1000),
            }
        )

    request_info = {
        "event": str(event),
        "conversation_id": str(conversation_id),
        "suppressed_since_last_emit": suppressed,
        **{str(key): value for key, value in fields.items() if value not in (None, "")},
    }
    logger.warning(message, extra={"request_info": request_info})
    return True


def compression_telemetry_snapshot() -> dict[str, Any]:
    from app.summarizer import summarizer_telemetry_snapshot

    with _LOCK:
        counters = dict(_COUNTERS)
        active_warning_keys = len(_LAST_WARNING_AT)
        pending_suppressed = int(sum(_SUPPRESSED_BY_KEY.values()))
        last_event = dict(_LAST_EVENT)
    return {
        "schema_version": 1,
        "warning_coalescing_interval_seconds": _warning_interval_seconds(),
        "active_warning_keys": active_warning_keys,
        "pending_suppressed_warnings": pending_suppressed,
        "counters": counters,
        "last_event": last_event,
        "summarizer": summarizer_telemetry_snapshot(),
    }


def reset_compression_telemetry_for_tests() -> None:
    with _LOCK:
        _COUNTERS.clear()
        _LAST_WARNING_AT.clear()
        _SUPPRESSED_BY_KEY.clear()
        _LAST_EVENT.clear()
