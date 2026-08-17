from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable


def _env_int(name: str, default: int, *, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def configured_session_retention_policy() -> dict[str, Any]:
    return {
        "retention_days": _env_int(
            "NOTION2API_MCP_SESSION_RETENTION_DAYS", 90, minimum=1, maximum=3650
        ),
        "max_records": _env_int(
            "NOTION2API_MCP_SESSION_MAX_RECORDS", 500, minimum=10, maximum=100000
        ),
        "default_mode": "preview_only",
        "archive_before_remove": True,
        "protect_missing_timestamp": True,
        "protected_name_prefixes": ["aigentbee-leader-"],
    }


def _record_timestamp(record: dict[str, Any]) -> int:
    for key in ("updated_at", "created_at"):
        try:
            value = int(record.get(key) or 0)
        except (TypeError, ValueError):
            value = 0
        if value > 0:
            return value
    return 0


def build_session_retention_plan(
    records: dict[str, dict[str, Any]],
    *,
    protected_session_names: Iterable[str] = (),
    protected_conversation_ids: Iterable[str] = (),
    now_ms: int | None = None,
    retention_days: int | None = None,
    max_records: int | None = None,
) -> dict[str, Any]:
    policy = configured_session_retention_policy()
    days = int(retention_days or policy["retention_days"])
    limit = int(max_records or policy["max_records"])
    current_ms = int(now_ms if now_ms is not None else time.time() * 1000)
    cutoff_ms = current_ms - days * 24 * 60 * 60 * 1000
    protected_names = {str(name).strip() for name in protected_session_names if str(name).strip()}
    protected_conversations = {
        str(value).strip() for value in protected_conversation_ids if str(value).strip()
    }

    protected: dict[str, str] = {}
    candidates: dict[str, str] = {}
    eligible: list[tuple[int, str]] = []
    for name, raw_record in records.items():
        record = dict(raw_record or {})
        conversation_id = str(record.get("conversation_id") or "").strip()
        timestamp = _record_timestamp(record)
        reason = ""
        if name in protected_names:
            reason = "referenced_by_chat_job"
        elif conversation_id in protected_conversations:
            reason = "conversation_referenced_by_chat_job"
        elif any(name.startswith(prefix) for prefix in policy["protected_name_prefixes"]):
            reason = "governance_leader_session"
        elif bool(record.get("evidence_bound") or record.get("mission_id") or record.get("governance_record_id")):
            reason = "explicit_evidence_binding"
        elif timestamp <= 0:
            reason = "missing_timestamp"
        if reason:
            protected[name] = reason
            continue
        eligible.append((timestamp, name))
        if timestamp < cutoff_ms:
            candidates[name] = "older_than_retention_window"

    retained_after_age = len(records) - len(candidates)
    overflow = max(0, retained_after_age - limit)
    if overflow:
        for _, name in sorted(eligible):
            if overflow <= 0:
                break
            if name in candidates:
                continue
            candidates[name] = "exceeds_max_records"
            overflow -= 1

    retained_names = sorted(name for name in records if name not in candidates)
    return {
        "schema_version": 1,
        "policy": {
            **policy,
            "retention_days": days,
            "max_records": limit,
            "cutoff_ms": cutoff_ms,
        },
        "counts": {
            "total": len(records),
            "protected": len(protected),
            "candidates": len(candidates),
            "retained": len(retained_names),
        },
        "protected": [
            {"session_name": name, "reason": protected[name]}
            for name in sorted(protected)
        ],
        "candidates": [
            {"session_name": name, "reason": candidates[name]}
            for name in sorted(candidates)
        ],
        "retained_session_names": retained_names,
    }


def archive_and_filter_sessions(
    records: dict[str, dict[str, Any]],
    plan: dict[str, Any],
    *,
    archive_path: Path,
    applied_by: str,
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    candidate_reasons = {
        str(item.get("session_name") or ""): str(item.get("reason") or "")
        for item in plan.get("candidates", [])
        if isinstance(item, dict) and str(item.get("session_name") or "")
    }
    if not candidate_reasons:
        return dict(records), {"archived": 0, "archive_path": str(archive_path)}

    archive_path.parent.mkdir(parents=True, exist_ok=True)
    archived_at = int(time.time() * 1000)
    archive_entries: list[dict[str, Any]] = []
    for name in sorted(candidate_reasons):
        record = records.get(name)
        if not isinstance(record, dict):
            continue
        archive_entries.append(
            {
                "schema_version": 1,
                "archived_at": archived_at,
                "applied_by": str(applied_by or "unknown")[:200],
                "session_name": name,
                "reason": candidate_reasons[name],
                "record": record,
            }
        )

    with archive_path.open("a", encoding="utf-8", newline="\n") as handle:
        for entry in archive_entries:
            handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())

    retained = {
        name: dict(record)
        for name, record in records.items()
        if name not in candidate_reasons
    }
    return retained, {
        "archived": len(archive_entries),
        "archive_path": str(archive_path),
        "archived_at": archived_at,
    }
