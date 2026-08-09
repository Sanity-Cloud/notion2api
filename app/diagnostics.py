"""SanityCloud native diagnostic-event emitter.

The portal supervisor owns durable storage and redaction. Application code emits
only the small, versioned line protocol when it is running under a governed
SanityCloud diagnostic environment.
"""

from __future__ import annotations

import json
import os
import re
import sys
from typing import Any, Mapping


CONTRACT_VERSION = "sanitycloud.diagnostic.v1"
EVENT_PREFIX = "SANITYCLOUD_DIAGNOSTIC_EVENT "
_SENSITIVE_KEY_RE = re.compile(
    r"(?:token|secret|password|passwd|cookie|authorization|api[_-]?key|credential)",
    re.IGNORECASE,
)


def _safe_details(value: Any, *, key: str = "", depth: int = 0) -> Any:
    if key and _SENSITIVE_KEY_RE.search(key):
        return "[REDACTED]"
    if depth >= 6:
        return "[TRUNCATED]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return value[:2000]
    if isinstance(value, Mapping):
        return {
            str(item_key)[:120]: _safe_details(item_value, key=str(item_key), depth=depth + 1)
            for item_key, item_value in list(value.items())[:80]
        }
    if isinstance(value, (list, tuple, set)):
        return [_safe_details(item, depth=depth + 1) for item in list(value)[:80]]
    return str(value)[:2000]


def emit_diagnostic_event(
    *,
    code: str,
    message: str,
    operation: str,
    category: str = "application_runtime",
    severity: str = "error",
    kind: str = "error",
    retryable: bool = False,
    details: Mapping[str, Any] | None = None,
    evidence: list[Any] | None = None,
    component_id: str = "notion2api",
    source: str = "notion2api_runtime",
) -> bool:
    """Emit one supervisor-consumable diagnostic event; never raise."""

    if os.environ.get("SANITYCLOUD_DIAGNOSTIC_CONTRACT_VERSION") != CONTRACT_VERSION:
        return False
    payload = {
        "component_id": component_id,
        "code": str(code or "APPLICATION_ERROR").upper()[:160],
        "message": str(message or "Application error.")[:2000],
        "operation": str(operation or "application_runtime")[:240],
        "category": str(category or "application_runtime")[:120],
        "severity": str(severity or "error")[:32],
        "kind": str(kind or "error")[:120],
        "retryable": bool(retryable),
        "source": str(source or "notion2api_runtime")[:160],
        "details": _safe_details(dict(details or {})),
        "evidence": _safe_details(list(evidence or [])),
    }
    try:
        sys.stderr.write(EVENT_PREFIX + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
        sys.stderr.flush()
        return True
    except Exception:  # pragma: no cover - diagnostic emission must never mask the primary failure
        return False
