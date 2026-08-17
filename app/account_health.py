"""Observed per-account health/quota signals for adaptive scheduling.

Prefer admission telemetry and local cooldown evidence. Never invent provider
quota numbers that were not observed.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Mapping


@dataclass
class AccountHealthSignal:
    account_key: str
    profile_name: str = ""
    capacity_role: str = ""
    account_alias: str = ""
    available: bool = True
    cooldown_remaining_seconds: float = 0.0
    retry_after_seconds: float = 0.0
    recent_failures: int = 0
    recent_retries: int = 0
    recent_successes: int = 0
    avg_latency_seconds: float = 0.0
    inflight: int = 0
    queue_depth: int = 0
    max_inflight: int = 1
    health_score: float = 1.0
    health_reason: str = "healthy"
    observed_at: float = field(default_factory=time.time)
    evidence: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "account_key": self.account_key,
            "profile_name": self.profile_name,
            "capacity_role": self.capacity_role,
            "account_alias": self.account_alias,
            "available": self.available,
            "cooldown_remaining_seconds": round(self.cooldown_remaining_seconds, 3),
            "retry_after_seconds": round(self.retry_after_seconds, 3),
            "recent_failures": self.recent_failures,
            "recent_retries": self.recent_retries,
            "recent_successes": self.recent_successes,
            "avg_latency_seconds": round(self.avg_latency_seconds, 3),
            "inflight": self.inflight,
            "queue_depth": self.queue_depth,
            "max_inflight": self.max_inflight,
            "health_score": round(self.health_score, 4),
            "health_reason": self.health_reason,
            "observed_at": self.observed_at,
            "evidence": dict(self.evidence),
        }


def _admission_account_key(workspace_id: str, user_id: str) -> str:
    return (
        f"{str(workspace_id or '').replace('-', '').casefold()}:"
        f"{str(user_id or '').replace('-', '').casefold()}"
    )


def _attempts_for_account(
    telemetry: Mapping[str, Any] | None,
    admission_account_key: str,
) -> list[dict[str, Any]]:
    if not telemetry or not admission_account_key:
        return []
    recent = telemetry.get("recent_attempts")
    if not isinstance(recent, list):
        return []
    matches: list[dict[str, Any]] = []
    for item in recent:
        if not isinstance(item, dict):
            continue
        if str(item.get("account_key") or "") == admission_account_key:
            matches.append(item)
    return matches


def score_account_health(
    *,
    account_key: str,
    profile_name: str = "",
    capacity_role: str = "",
    account_alias: str = "",
    workspace_id: str = "",
    user_id: str = "",
    cooldown_remaining_seconds: float = 0.0,
    admission_snapshot: Mapping[str, Any] | None = None,
    now: float | None = None,
) -> AccountHealthSignal:
    """Derive a bounded health score from observed local evidence only."""

    observed_at = float(now if now is not None else time.time())
    cooldown = max(0.0, float(cooldown_remaining_seconds or 0.0))
    snapshot = dict(admission_snapshot or {})
    admission_key = _admission_account_key(workspace_id, user_id)
    inflight_map = snapshot.get("active_accounts")
    inflight = 0
    if isinstance(inflight_map, dict):
        inflight = max(0, int(inflight_map.get(admission_key) or 0))
    max_inflight = max(1, int(snapshot.get("account_max_inflight") or 1))
    telemetry = snapshot.get("request_telemetry")
    attempts = _attempts_for_account(
        telemetry if isinstance(telemetry, dict) else None,
        admission_key,
    )

    successes = 0
    failures = 0
    retries = 0
    retry_after = 0.0
    latency_samples: list[float] = []
    for attempt in attempts:
        outcome = str(attempt.get("outcome") or "").strip().casefold()
        if outcome == "succeeded":
            successes += 1
        elif outcome in {"failed", "error"}:
            failures += 1
        retries += max(0, int(attempt.get("retry_count") or 0))
        retry_after = max(
            retry_after, float(attempt.get("retry_after_seconds") or 0.0)
        )
        duration = attempt.get("duration_seconds")
        if duration is not None:
            try:
                latency_samples.append(max(0.0, float(duration)))
            except (TypeError, ValueError):
                pass

    avg_latency = (
        sum(latency_samples) / len(latency_samples) if latency_samples else 0.0
    )
    queue_depth = max(0, int(snapshot.get("account_queue_depth") or 0))

    score = 1.0
    reasons: list[str] = []
    available = cooldown <= 0.0 and retry_after <= 0.05

    if cooldown > 0:
        score -= min(0.7, 0.2 + cooldown / 60.0)
        reasons.append(f"cooldown:{cooldown:.1f}s")
        available = False
    if retry_after > 0.05:
        score -= min(0.5, 0.15 + retry_after / 120.0)
        reasons.append(f"retry_after:{retry_after:.1f}s")
        available = False
    if failures:
        score -= min(0.4, 0.1 * failures)
        reasons.append(f"recent_failures:{failures}")
    if retries:
        score -= min(0.2, 0.05 * retries)
        reasons.append(f"recent_retries:{retries}")
    if inflight >= max_inflight:
        score -= 0.35
        reasons.append(f"inflight_saturated:{inflight}/{max_inflight}")
        available = False
    elif inflight > 0:
        score -= min(0.2, 0.1 * (inflight / max_inflight))
        reasons.append(f"inflight:{inflight}/{max_inflight}")
    if avg_latency > 20.0:
        score -= min(0.15, (avg_latency - 20.0) / 200.0)
        reasons.append(f"latency:{avg_latency:.1f}s")
    if queue_depth > 0 and inflight >= max_inflight:
        score -= min(0.15, 0.02 * queue_depth)
        reasons.append(f"queue_pressure:{queue_depth}")

    score = max(0.0, min(1.0, score))
    if not reasons:
        reasons.append("healthy")
    if successes and "healthy" in reasons:
        reasons = [f"observed_successes:{successes}"]

    return AccountHealthSignal(
        account_key=account_key,
        profile_name=profile_name,
        capacity_role=capacity_role,
        account_alias=account_alias,
        available=available and score > 0.05,
        cooldown_remaining_seconds=cooldown,
        retry_after_seconds=retry_after,
        recent_failures=failures,
        recent_retries=retries,
        recent_successes=successes,
        avg_latency_seconds=avg_latency,
        inflight=inflight,
        queue_depth=queue_depth,
        max_inflight=max_inflight,
        health_score=score,
        health_reason=";".join(reasons),
        observed_at=observed_at,
        evidence={
            "admission_account_key": admission_key,
            "attempt_sample_size": len(attempts),
            "quota_numbers_invented": False,
        },
    )
