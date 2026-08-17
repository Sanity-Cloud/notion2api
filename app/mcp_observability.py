from __future__ import annotations

import logging
import os
import threading
import time
from collections import Counter
from typing import Any

_LOCK = threading.Lock()
_COUNTERS: Counter[str] = Counter()
_LAST_EMITTED_AT: dict[str, float] = {}
_LAST_EVENT: dict[str, Any] = {}
_INSTALLED_HANDLERS: set[int] = set()


def _coalesce_interval_seconds() -> float:
    try:
        value = float(os.getenv("NOTION2API_MCP_LOG_COALESCE_SECONDS", "300"))
    except (TypeError, ValueError):
        value = 300.0
    return max(1.0, min(value, 86400.0))


def _routine_key(message: str) -> str:
    text = str(message or "").strip()
    if text == "Terminating session: None":
        return "terminating_none"
    return ""


class RoutineMcpNoiseFilter(logging.Filter):
    """Coalesce known routine MCP lifecycle noise without hiding faults."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage()
        key = _routine_key(message)
        if not key:
            return True
        now = time.monotonic()
        with _LOCK:
            _COUNTERS[f"observed_{key}"] += 1
            last = _LAST_EMITTED_AT.get(key)
            if last is not None and now - last < _coalesce_interval_seconds():
                _COUNTERS[f"suppressed_{key}"] += 1
                _LAST_EVENT.clear()
                _LAST_EVENT.update(
                    {
                        "event": key,
                        "suppressed": True,
                        "updated_at": int(time.time() * 1000),
                    }
                )
                return False
            _LAST_EMITTED_AT[key] = now
            _COUNTERS[f"emitted_{key}"] += 1
            _LAST_EVENT.clear()
            _LAST_EVENT.update(
                {
                    "event": key,
                    "suppressed": False,
                    "updated_at": int(time.time() * 1000),
                }
            )
        return True


def install_mcp_noise_filter() -> int:
    """Install one shared filter on current MCP/root handlers idempotently."""

    installed = 0
    filter_instance = RoutineMcpNoiseFilter()
    logger_names = (
        "",
        "mcp",
        "mcp.server",
        "mcp.server.streamable_http",
        "uvicorn.error",
    )
    for name in logger_names:
        target = logging.getLogger(name)
        for handler in target.handlers:
            marker = id(handler)
            with _LOCK:
                if marker in _INSTALLED_HANDLERS:
                    continue
                _INSTALLED_HANDLERS.add(marker)
            handler.addFilter(filter_instance)
            installed += 1
    with _LOCK:
        _COUNTERS["filter_install_calls"] += 1
        _COUNTERS["filter_handlers_installed"] += installed
    return installed


def record_mcp_http_error(
    *,
    status_code: int,
    request_id: str,
    response_request_id: str,
    method: str,
    path: str,
) -> None:
    with _LOCK:
        _COUNTERS["http_errors"] += 1
        _COUNTERS[f"status_{int(status_code)}"] += 1
        _LAST_EVENT.clear()
        _LAST_EVENT.update(
            {
                "event": "mcp_backend_http_error",
                "status_code": int(status_code),
                "request_id": str(request_id),
                "response_request_id": str(response_request_id),
                "method": str(method),
                "path": str(path),
                "updated_at": int(time.time() * 1000),
            }
        )


def mcp_observability_snapshot() -> dict[str, Any]:
    with _LOCK:
        return {
            "schema_version": 1,
            "log_coalescing_interval_seconds": _coalesce_interval_seconds(),
            "counters": dict(_COUNTERS),
            "last_event": dict(_LAST_EVENT),
        }


def reset_mcp_observability_for_tests() -> None:
    with _LOCK:
        _COUNTERS.clear()
        _LAST_EMITTED_AT.clear()
        _LAST_EVENT.clear()
        _INSTALLED_HANDLERS.clear()
