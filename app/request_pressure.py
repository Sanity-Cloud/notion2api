from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime
from typing import Iterable

REQUEST_PATHS = {"/v1/chat/completions", "/v1/responses"}
UVICORN_RE = re.compile(r'(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}):\d{2}.*"POST (?P<path>/v1/(?:chat/completions|responses)) HTTP/[^\"]+" (?P<status>\d{3})')


def _minute(value: object) -> str:
    text = str(value or "")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).strftime("%Y-%m-%dT%H:%M")
    except ValueError:
        return text[:16] if len(text) >= 16 else "unknown"


def analyze_lines(lines: Iterable[str]) -> dict[str, object]:
    requests_by_minute: Counter[str] = Counter()
    status_counts: Counter[str] = Counter()
    event_counts: Counter[str] = Counter()
    retry_attempt_counts: Counter[str] = Counter()
    callers: Counter[str] = Counter()
    sessions: Counter[str] = Counter()
    non_retriable_after_attempt_one = 0
    total_requests = 0

    for raw in lines:
        line = raw.strip()
        if not line:
            continue
        parsed = None
        if line.startswith("{"):
            try:
                parsed = json.loads(line)
            except json.JSONDecodeError:
                parsed = None
        if isinstance(parsed, dict):
            path = str(parsed.get("path") or "")
            method = str(parsed.get("method") or "").upper()
            message = str(parsed.get("message") or "")
            event = str(parsed.get("event") or "")
            timestamp = parsed.get("timestamp")
            if method == "POST" and path in REQUEST_PATHS and message == "Request processed":
                total_requests += 1
                requests_by_minute[_minute(timestamp)] += 1
                status_counts[str(parsed.get("status_code") or "unknown")] += 1
            if event:
                event_counts[event] += 1
            attempt = parsed.get("attempt")
            if attempt is not None:
                retry_attempt_counts[str(attempt)] += 1
                if parsed.get("retriable") is False and int(attempt) > 1:
                    non_retriable_after_attempt_one += 1
            caller = parsed.get("caller_id") or parsed.get("caller")
            session = parsed.get("session_name") or parsed.get("conversation_id")
            if caller:
                callers[str(caller)] += 1
            if session:
                sessions[str(session)] += 1
            continue

        match = UVICORN_RE.search(line)
        if match:
            total_requests += 1
            requests_by_minute[match.group("ts")] += 1
            status_counts[match.group("status")] += 1

    peak_minute, peak_count = ("", 0)
    if requests_by_minute:
        peak_minute, peak_count = requests_by_minute.most_common(1)[0]
    return {
        "total_inference_requests": total_requests,
        "peak_requests_per_minute": peak_count,
        "peak_minute": peak_minute,
        "status_counts": dict(status_counts),
        "event_counts": dict(event_counts),
        "retry_attempt_counts": dict(retry_attempt_counts),
        "non_retriable_failures_after_attempt_one": non_retriable_after_attempt_one,
        "top_callers": callers.most_common(10),
        "top_sessions": sessions.most_common(10),
    }
