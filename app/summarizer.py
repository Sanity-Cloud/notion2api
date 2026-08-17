from __future__ import annotations

import os
import re
import threading
import time
from collections import Counter
from typing import Any

import httpx

from app.config import SILICONFLOW_API_KEY


class SummarizerUnavailableError(Exception):
    """Raised when no configured summarizer backend can produce a summary."""


SILICONFLOW_ENDPOINT = "https://api.siliconflow.cn/v1/chat/completions"
MODEL_FALLBACK_CHAIN = ["Qwen/Qwen3-8B", "THUDM/glm-4-9b-chat"]

SYSTEM_PROMPT = (
    "Create a compact factual memory of the supplied conversation turn. "
    "Preserve decisions, constraints, identifiers, unresolved questions, and dissent. "
    "Do not invent facts or instructions."
)

_TELEMETRY_LOCK = threading.Lock()
_TELEMETRY_COUNTERS: Counter[str] = Counter()
_TELEMETRY_STATE: dict[str, Any] = {
    "last_backend": "",
    "last_model": "",
    "last_error": "",
    "last_updated_at": 0,
}


def _env_flag(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def local_fallback_enabled() -> bool:
    return _env_flag("NOTION2API_SUMMARIZER_LOCAL_FALLBACK", True)


def _record(event: str, *, backend: str = "", model: str = "", error: str = "") -> None:
    with _TELEMETRY_LOCK:
        _TELEMETRY_COUNTERS[event] += 1
        if backend:
            _TELEMETRY_STATE["last_backend"] = backend
        if model:
            _TELEMETRY_STATE["last_model"] = model
        if error:
            _TELEMETRY_STATE["last_error"] = error[:500]
        elif event.endswith("success"):
            _TELEMETRY_STATE["last_error"] = ""
        _TELEMETRY_STATE["last_updated_at"] = int(time.time() * 1000)


def summarizer_telemetry_snapshot() -> dict[str, Any]:
    with _TELEMETRY_LOCK:
        counters = dict(_TELEMETRY_COUNTERS)
        state = dict(_TELEMETRY_STATE)
    return {
        "configured": is_summarizer_configured(),
        "remote_configured": bool(str(SILICONFLOW_API_KEY or "").strip()),
        "local_fallback_enabled": local_fallback_enabled(),
        "local_fallback_max_chars": _env_int(
            "NOTION2API_SUMMARIZER_LOCAL_MAX_CHARS", 2400, minimum=400, maximum=12000
        ),
        "counters": counters,
        **state,
    }


def is_summarizer_configured() -> bool:
    return bool(str(SILICONFLOW_API_KEY or "").strip()) or local_fallback_enabled()


def _build_user_prompt(old_summaries: list[str], user_msg: str, assistant_msg: str) -> str:
    prompt_parts: list[str] = []
    if old_summaries:
        prompt_parts.extend(("Prior durable summaries:", "\n".join(old_summaries[-5:]), ""))
    prompt_parts.extend(
        (
            "Conversation turn:",
            f"User: {user_msg}",
            f"Assistant: {assistant_msg}",
            "",
            "Return only the factual memory summary.",
        )
    )
    return "\n".join(prompt_parts)


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _bounded_excerpt(value: str, limit: int) -> str:
    text = _normalize_text(value)
    if len(text) <= limit:
        return text
    head = max(1, int(limit * 0.62))
    tail = max(1, limit - head - 5)
    return f"{text[:head]} ... {text[-tail:]}"


def _deterministic_summary(
    old_summaries: list[str], user_msg: str, assistant_msg: str
) -> str:
    """Build a bounded extractive memory without model inference."""

    max_chars = _env_int(
        "NOTION2API_SUMMARIZER_LOCAL_MAX_CHARS", 2400, minimum=400, maximum=12000
    )
    prior_budget = max(100, int(max_chars * 0.20))
    user_budget = max(120, int(max_chars * 0.32))
    assistant_budget = max(160, max_chars - prior_budget - user_budget - 80)
    sections: list[str] = ["[deterministic-extractive-memory]"]
    prior = " | ".join(_normalize_text(item) for item in old_summaries[-2:] if item)
    if prior:
        sections.append(f"Prior: {_bounded_excerpt(prior, prior_budget)}")
    sections.append(f"User: {_bounded_excerpt(user_msg, user_budget) or '[empty]'}")
    sections.append(
        f"Assistant: {_bounded_excerpt(assistant_msg, assistant_budget) or '[empty]'}"
    )
    return "\n".join(sections)[:max_chars].strip()


async def _call_summarizer(
    model: str, old_summaries: list[str], user_msg: str, assistant_msg: str
) -> str:
    timeout = httpx.Timeout(connect=5.0, read=20.0, write=20.0, pool=20.0)
    headers = {
        "Authorization": f"Bearer {SILICONFLOW_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": _build_user_prompt(old_summaries, user_msg, assistant_msg),
            },
        ],
        "temperature": 0.2,
    }
    async with httpx.AsyncClient(timeout=timeout) as client:
        response = await client.post(SILICONFLOW_ENDPOINT, headers=headers, json=payload)
    if response.status_code != 200:
        raise SummarizerUnavailableError(
            f"Summarizer upstream returned status {response.status_code}"
        )
    data = response.json()
    content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
    summary = str(content).strip()
    if not summary:
        raise SummarizerUnavailableError("Summarizer returned empty summary")
    return summary


async def summarize_turn(
    old_summaries: list[str], user_msg: str, assistant_msg: str
) -> str:
    """Summarize one turn using remote models, then a deterministic local fallback."""

    api_key = str(SILICONFLOW_API_KEY or "").strip()
    last_error: Exception | None = None
    if api_key:
        for model in MODEL_FALLBACK_CHAIN:
            _record("remote_attempt", backend="siliconflow", model=model)
            try:
                summary = await _call_summarizer(
                    model, old_summaries, user_msg, assistant_msg
                )
            except Exception as exc:
                last_error = exc
                _record(
                    "remote_failure",
                    backend="siliconflow",
                    model=model,
                    error=f"{type(exc).__name__}: {exc}",
                )
                continue
            _record("remote_success", backend="siliconflow", model=model)
            return summary

    if local_fallback_enabled():
        summary = _deterministic_summary(old_summaries, user_msg, assistant_msg)
        if summary:
            _record("local_fallback_success", backend="deterministic_extractive")
            return summary
        last_error = SummarizerUnavailableError("Deterministic fallback returned empty summary")

    error_text = (
        f"All remote summarizer models failed: {last_error}"
        if api_key
        else "No remote summarizer key and deterministic fallback is disabled"
    )
    _record("unavailable", error=error_text)
    raise SummarizerUnavailableError(error_text) from last_error
