from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any

from app.notion_admission_store import default_admission_db_path


QUOTA_SCOPES = {"global", "account", "workload", "account_workload"}
MAX_QUOTA_WINDOW_SECONDS = 31 * 86400
NOTION_ALLOWANCE_ROLLING_WINDOW_SECONDS = 6 * 3600
NOTION_ALLOWANCE_EXCLUDED_PRODUCTS = ("custom_agents", "workers")


class UsageQuotaExceededError(RuntimeError):
    """Raised before dispatch when a configured operational quota is exhausted."""

    def __init__(self, status: dict[str, Any]) -> None:
        self.status = dict(status)
        self.quota_id = str(status.get("quota_id") or "quota")
        self.dimension = str(status.get("exceeded_dimension") or "usage")
        self.retry_after_seconds = max(
            0.0, float(status.get("retry_after_seconds") or 0.0)
        )
        super().__init__(
            f"Notion usage quota exceeded: {self.quota_id} ({self.dimension})"
        )


def _bounded_text(value: Any, *, maximum: int = 160) -> str:
    return str(value or "").strip()[:maximum]


def _bounded_error_class(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 96 and all(
        character.isalnum() or character in "._:-" for character in text
    ):
        return text
    return "other"


def _bounded_quota_id(value: Any) -> str:
    text = str(value or "").strip()
    if not text or len(text) > 96:
        raise ValueError("quota_id must contain 1-96 characters")
    if not all(character.isalnum() or character in "._:-" for character in text):
        raise ValueError("quota_id contains unsupported characters")
    return text


def _optional_limit(value: Any, name: str) -> int | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a non-negative integer or null")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a non-negative integer or null") from exc
    if parsed < 0:
        raise ValueError(f"{name} must be a non-negative integer or null")
    return parsed


def _opaque_account_id(account_key: str) -> str:
    if not account_key:
        return ""
    return hashlib.sha256(
        f"notion2api:usage-account:{account_key}".encode("utf-8")
    ).hexdigest()[:20]


class NotionRequestTelemetryStore:
    """Durable, sanitized request-attempt telemetry for Notion upstream calls."""

    def __init__(self, path: str | Path | None = None) -> None:
        self.path = Path(path) if path is not None else default_admission_db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            str(self.path),
            timeout=10.0,
            isolation_level=None,
            check_same_thread=False,
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS notion_request_attempts (
                    attempt_id TEXT PRIMARY KEY,
                    created_at REAL NOT NULL,
                    started_at REAL NOT NULL,
                    completed_at REAL,
                    duration_seconds REAL,
                    account_key TEXT NOT NULL DEFAULT '',
                    thread_key TEXT NOT NULL DEFAULT '',
                    operation TEXT NOT NULL DEFAULT '',
                    workload_class TEXT NOT NULL DEFAULT 'legacy',
                    admission_weight REAL NOT NULL DEFAULT 1.0,
                    trace_id TEXT NOT NULL DEFAULT '',
                    request_context_id TEXT NOT NULL DEFAULT '',
                    model_id TEXT NOT NULL DEFAULT '',
                    outcome TEXT NOT NULL DEFAULT 'started',
                    status_code INTEGER,
                    request_bytes INTEGER NOT NULL DEFAULT 0,
                    response_bytes INTEGER NOT NULL DEFAULT 0,
                    estimated_input_tokens INTEGER NOT NULL DEFAULT 0,
                    estimated_output_tokens INTEGER NOT NULL DEFAULT 0,
                    actual_input_tokens INTEGER,
                    actual_output_tokens INTEGER,
                    actual_total_tokens INTEGER,
                    retry_count INTEGER NOT NULL DEFAULT 0,
                    retry_after_seconds REAL NOT NULL DEFAULT 0,
                    error_class TEXT NOT NULL DEFAULT '',
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_notion_attempts_created
                    ON notion_request_attempts(created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_notion_attempts_operation
                    ON notion_request_attempts(operation, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_notion_attempts_context
                    ON notion_request_attempts(request_context_id, created_at DESC);

                CREATE TABLE IF NOT EXISTS notion_usage_quotas (
                    quota_id TEXT PRIMARY KEY,
                    revision INTEGER NOT NULL,
                    scope TEXT NOT NULL,
                    account_key TEXT NOT NULL DEFAULT '',
                    workload_class TEXT NOT NULL DEFAULT '',
                    window_seconds INTEGER NOT NULL,
                    max_requests INTEGER,
                    max_tokens INTEGER,
                    max_request_bytes INTEGER,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_notion_usage_quotas_match
                    ON notion_usage_quotas(enabled, scope, account_key, workload_class);

                CREATE TABLE IF NOT EXISTS notion_usage_quota_events (
                    event_id TEXT PRIMARY KEY,
                    quota_id TEXT NOT NULL,
                    revision INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    policy_json TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_notion_usage_quota_events_quota
                    ON notion_usage_quota_events(quota_id, revision DESC);

                CREATE TABLE IF NOT EXISTS notion_allowance_observations (
                    observation_id TEXT PRIMARY KEY,
                    observed_at REAL NOT NULL,
                    account_key TEXT NOT NULL,
                    rolling_used_percent REAL NOT NULL,
                    rolling_resets_at REAL,
                    monthly_used_percent REAL NOT NULL,
                    monthly_resets_at REAL,
                    source TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_notion_allowance_observations_account
                    ON notion_allowance_observations(account_key, observed_at DESC);
                """
            )

    @staticmethod
    def _allowance_percent(value: Any, field: str) -> float:
        if isinstance(value, bool):
            raise ValueError(f"{field} must be a number from 0 through 100")
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{field} must be a number from 0 through 100") from exc
        if not 0.0 <= result <= 100.0:
            raise ValueError(f"{field} must be a number from 0 through 100")
        return round(result, 3)

    @staticmethod
    def _optional_timestamp(value: Any, field: str) -> float | None:
        if value in (None, ""):
            return None
        if isinstance(value, bool):
            raise ValueError(f"{field} must be a positive Unix timestamp or null")
        try:
            result = float(value)
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"{field} must be a positive Unix timestamp or null"
            ) from exc
        if result <= 0:
            raise ValueError(f"{field} must be a positive Unix timestamp or null")
        return result

    @staticmethod
    def _public_allowance(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "observation_id": str(row["observation_id"]),
            "observed_at": float(row["observed_at"]),
            "account_id": _opaque_account_id(str(row["account_key"])),
            "rolling": {
                "window_seconds": NOTION_ALLOWANCE_ROLLING_WINDOW_SECONDS,
                "used_percent": float(row["rolling_used_percent"]),
                "resets_at": row["rolling_resets_at"],
                "behavior": "usage_from_the_past_six_hours_frees_over_time",
            },
            "monthly": {
                "used_percent": float(row["monthly_used_percent"]),
                "resets_at": row["monthly_resets_at"],
                "behavior": "provider_reported_plan_cycle",
            },
            "excluded_products": list(NOTION_ALLOWANCE_EXCLUDED_PRODUCTS),
            "source": str(row["source"]),
            "authoritative_for_enforcement": False,
        }

    def record_allowance_observation(
        self,
        *,
        account_key: str,
        rolling_used_percent: Any,
        monthly_used_percent: Any,
        rolling_resets_at: Any = None,
        monthly_resets_at: Any = None,
        observed_at: Any = None,
        source: Any = "notion_settings_ui",
    ) -> dict[str, Any]:
        account = _bounded_text(account_key)
        if not account:
            raise ValueError("account_key is required for allowance observations")
        normalized_source = _bounded_text(source, maximum=32).lower()
        if normalized_source not in {
            "notion_settings_ui",
            "notion_settings_api",
            "manual",
        }:
            raise ValueError(
                "source must be notion_settings_ui, notion_settings_api, or manual"
            )
        observation_time = (
            time.time()
            if observed_at in (None, "")
            else self._optional_timestamp(observed_at, "observed_at")
        )
        observation_id = uuid.uuid4().hex
        now = time.time()
        values = (
            observation_id,
            observation_time,
            account,
            self._allowance_percent(rolling_used_percent, "rolling_used_percent"),
            self._optional_timestamp(rolling_resets_at, "rolling_resets_at"),
            self._allowance_percent(monthly_used_percent, "monthly_used_percent"),
            self._optional_timestamp(monthly_resets_at, "monthly_resets_at"),
            normalized_source,
            now,
        )
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO notion_allowance_observations (
                    observation_id, observed_at, account_key,
                    rolling_used_percent, rolling_resets_at,
                    monthly_used_percent, monthly_resets_at, source, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                values,
            )
            row = conn.execute(
                "SELECT * FROM notion_allowance_observations WHERE observation_id = ?",
                (observation_id,),
            ).fetchone()
        return self._public_allowance(row)

    def latest_allowance_observation(
        self, *, account_key: str
    ) -> dict[str, Any] | None:
        account = _bounded_text(account_key)
        if not account:
            raise ValueError("account_key is required for allowance observations")
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM notion_allowance_observations
                WHERE account_key = ?
                ORDER BY observed_at DESC, created_at DESC
                LIMIT 1
                """,
                (account,),
            ).fetchone()
        return self._public_allowance(row) if row is not None else None

    @staticmethod
    def _normalize_quota(
        quota_id: Any,
        *,
        scope: Any,
        account_key: Any = "",
        workload_class: Any = "",
        window_seconds: Any = 3600,
        max_requests: Any = None,
        max_tokens: Any = None,
        max_request_bytes: Any = None,
        enabled: Any = True,
    ) -> dict[str, Any]:
        normalized_scope = str(scope or "").strip().lower()
        if normalized_scope not in QUOTA_SCOPES:
            raise ValueError(f"scope must be one of: {', '.join(sorted(QUOTA_SCOPES))}")
        account = _bounded_text(account_key)
        workload = _bounded_text(workload_class, maximum=32)
        if normalized_scope in {"account", "account_workload"} and not account:
            raise ValueError("account_key is required for account-scoped quotas")
        if normalized_scope in {"workload", "account_workload"} and not workload:
            raise ValueError("workload_class is required for workload-scoped quotas")
        if normalized_scope not in {"account", "account_workload"}:
            account = ""
        if normalized_scope not in {"workload", "account_workload"}:
            workload = ""
        try:
            window = int(window_seconds)
        except (TypeError, ValueError) as exc:
            raise ValueError("window_seconds must be an integer") from exc
        if window < 60 or window > MAX_QUOTA_WINDOW_SECONDS:
            raise ValueError(
                f"window_seconds must be between 60 and {MAX_QUOTA_WINDOW_SECONDS}"
            )
        limits = {
            "max_requests": _optional_limit(max_requests, "max_requests"),
            "max_tokens": _optional_limit(max_tokens, "max_tokens"),
            "max_request_bytes": _optional_limit(
                max_request_bytes, "max_request_bytes"
            ),
        }
        if all(value is None for value in limits.values()):
            raise ValueError("at least one quota limit must be configured")
        return {
            "quota_id": _bounded_quota_id(quota_id),
            "scope": normalized_scope,
            "account_key": account,
            "workload_class": workload,
            "window_seconds": window,
            **limits,
            "enabled": bool(enabled),
        }

    def upsert_quota(self, quota_id: str, **policy: Any) -> dict[str, Any]:
        normalized = self._normalize_quota(quota_id, **policy)
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT revision, created_at FROM notion_usage_quotas WHERE quota_id = ?",
                (normalized["quota_id"],),
            ).fetchone()
            revision = int(row["revision"] or 0) + 1 if row else 1
            created_at = float(row["created_at"]) if row else now
            conn.execute(
                """
                INSERT INTO notion_usage_quotas (
                    quota_id, revision, scope, account_key, workload_class,
                    window_seconds, max_requests, max_tokens, max_request_bytes,
                    enabled, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(quota_id) DO UPDATE SET
                    revision = excluded.revision,
                    scope = excluded.scope,
                    account_key = excluded.account_key,
                    workload_class = excluded.workload_class,
                    window_seconds = excluded.window_seconds,
                    max_requests = excluded.max_requests,
                    max_tokens = excluded.max_tokens,
                    max_request_bytes = excluded.max_request_bytes,
                    enabled = excluded.enabled,
                    updated_at = excluded.updated_at
                """,
                (
                    normalized["quota_id"],
                    revision,
                    normalized["scope"],
                    normalized["account_key"],
                    normalized["workload_class"],
                    normalized["window_seconds"],
                    normalized["max_requests"],
                    normalized["max_tokens"],
                    normalized["max_request_bytes"],
                    1 if normalized["enabled"] else 0,
                    created_at,
                    now,
                ),
            )
            event_policy = {
                key: value for key, value in normalized.items() if key != "account_key"
            }
            event_policy["account_id"] = _opaque_account_id(normalized["account_key"])
            conn.execute(
                """
                INSERT INTO notion_usage_quota_events (
                    event_id, quota_id, revision, action, policy_json, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    uuid.uuid4().hex,
                    normalized["quota_id"],
                    revision,
                    "enabled" if normalized["enabled"] else "disabled",
                    json.dumps(event_policy, sort_keys=True, separators=(",", ":")),
                    now,
                ),
            )
            conn.commit()
        return self.get_quota(normalized["quota_id"], public=True) or {}

    @staticmethod
    def _public_quota(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "quota_id": str(row["quota_id"]),
            "revision": int(row["revision"]),
            "scope": str(row["scope"]),
            "account_id": _opaque_account_id(str(row["account_key"])),
            "workload_class": str(row["workload_class"]),
            "window_seconds": int(row["window_seconds"]),
            "max_requests": row["max_requests"],
            "max_tokens": row["max_tokens"],
            "max_request_bytes": row["max_request_bytes"],
            "enabled": bool(row["enabled"]),
            "created_at": float(row["created_at"]),
            "updated_at": float(row["updated_at"]),
        }

    def get_quota(
        self, quota_id: str, *, public: bool = False
    ) -> dict[str, Any] | None:
        normalized_id = _bounded_quota_id(quota_id)
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM notion_usage_quotas WHERE quota_id = ?",
                (normalized_id,),
            ).fetchone()
        if row is None:
            return None
        if public:
            return self._public_quota(row)
        return {key: row[key] for key in row.keys()}

    def list_quotas(self, *, include_disabled: bool = True) -> list[dict[str, Any]]:
        query = "SELECT * FROM notion_usage_quotas"
        parameters: tuple[Any, ...] = ()
        if not include_disabled:
            query += " WHERE enabled = ?"
            parameters = (1,)
        query += " ORDER BY quota_id"
        with self._connect() as conn:
            rows = conn.execute(query, parameters).fetchall()
        return [self._public_quota(row) for row in rows]

    @staticmethod
    def _quota_matches(row: sqlite3.Row, account_key: str, workload_class: str) -> bool:
        scope = str(row["scope"])
        account_matches = str(row["account_key"]) == account_key
        workload_matches = str(row["workload_class"]) == workload_class
        return (
            scope == "global"
            or (scope == "account" and account_matches)
            or (scope == "workload" and workload_matches)
            or (scope == "account_workload" and account_matches and workload_matches)
        )

    @staticmethod
    def _usage_for_quota(
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        now: float,
    ) -> dict[str, Any]:
        conditions = ["created_at >= ?"]
        parameters: list[Any] = [now - int(row["window_seconds"])]
        scope = str(row["scope"])
        if scope in {"account", "account_workload"}:
            conditions.append("account_key = ?")
            parameters.append(str(row["account_key"]))
        if scope in {"workload", "account_workload"}:
            conditions.append("workload_class = ?")
            parameters.append(str(row["workload_class"]))
        usage = conn.execute(
            f"""
            SELECT COUNT(*) AS request_count,
                   COALESCE(SUM(request_bytes), 0) AS request_bytes,
                   COALESCE(SUM(
                       CASE
                           WHEN actual_total_tokens IS NOT NULL THEN actual_total_tokens
                           ELSE estimated_input_tokens + estimated_output_tokens
                       END
                   ), 0) AS tracked_tokens,
                   MIN(created_at) AS oldest_created_at
            FROM notion_request_attempts
            WHERE {" AND ".join(conditions)}
            """,
            tuple(parameters),
        ).fetchone()
        return {
            "request_count": int(usage["request_count"] or 0),
            "request_bytes": int(usage["request_bytes"] or 0),
            "tracked_tokens": int(usage["tracked_tokens"] or 0),
            "oldest_created_at": usage["oldest_created_at"],
        }

    @classmethod
    def _quota_status(
        cls,
        conn: sqlite3.Connection,
        row: sqlite3.Row,
        *,
        now: float,
        projected_requests: int = 0,
        projected_tokens: int = 0,
        projected_request_bytes: int = 0,
    ) -> dict[str, Any]:
        usage = cls._usage_for_quota(conn, row, now=now)
        projected = {
            "request_count": usage["request_count"] + max(0, projected_requests),
            "tracked_tokens": usage["tracked_tokens"] + max(0, projected_tokens),
            "request_bytes": usage["request_bytes"] + max(0, projected_request_bytes),
        }
        dimensions = (
            ("requests", "request_count", row["max_requests"]),
            ("tokens", "tracked_tokens", row["max_tokens"]),
            ("request_bytes", "request_bytes", row["max_request_bytes"]),
        )
        exceeded_dimension = ""
        remaining: dict[str, int | None] = {}
        limits: dict[str, int | None] = {}
        for public_name, usage_name, limit in dimensions:
            normalized_limit = int(limit) if limit is not None else None
            limits[public_name] = normalized_limit
            remaining[public_name] = (
                None
                if normalized_limit is None
                else max(0, normalized_limit - int(usage[usage_name]))
            )
            if (
                not exceeded_dimension
                and normalized_limit is not None
                and projected[usage_name] > normalized_limit
            ):
                exceeded_dimension = public_name
        oldest = usage["oldest_created_at"]
        retry_after = (
            max(0.0, float(oldest) + int(row["window_seconds"]) - now)
            if oldest is not None
            else float(row["window_seconds"])
        )
        return {
            **cls._public_quota(row),
            "usage": {
                "requests": usage["request_count"],
                "tokens": usage["tracked_tokens"],
                "request_bytes": usage["request_bytes"],
            },
            "projected_usage": {
                "requests": projected["request_count"],
                "tokens": projected["tracked_tokens"],
                "request_bytes": projected["request_bytes"],
            },
            "limits": limits,
            "remaining": remaining,
            "exceeded": bool(exceeded_dimension),
            "exceeded_dimension": exceeded_dimension,
            "retry_after_seconds": round(retry_after, 3) if exceeded_dimension else 0.0,
            "token_basis": "actual_total_when_available_else_estimated",
        }

    def quota_status(
        self,
        *,
        account_key: str,
        workload_class: str,
        projected_requests: int = 0,
        projected_tokens: int = 0,
        projected_request_bytes: int = 0,
    ) -> list[dict[str, Any]]:
        account = _bounded_text(account_key)
        workload = _bounded_text(workload_class or "legacy", maximum=32)
        now = time.time()
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM notion_usage_quotas WHERE enabled = 1 ORDER BY quota_id"
            ).fetchall()
            return [
                self._quota_status(
                    conn,
                    row,
                    now=now,
                    projected_requests=projected_requests,
                    projected_tokens=projected_tokens,
                    projected_request_bytes=projected_request_bytes,
                )
                for row in rows
                if self._quota_matches(row, account, workload)
            ]

    def usage_summary(
        self,
        *,
        window_seconds: int = 3600,
        account_key: str = "",
        workload_class: str = "",
    ) -> dict[str, Any]:
        window = max(60, min(int(window_seconds), MAX_QUOTA_WINDOW_SECONDS))
        conditions = ["created_at >= ?"]
        parameters: list[Any] = [time.time() - window]
        account = _bounded_text(account_key)
        workload = _bounded_text(workload_class, maximum=32)
        if account:
            conditions.append("account_key = ?")
            parameters.append(account)
        if workload:
            conditions.append("workload_class = ?")
            parameters.append(workload)
        with self._connect() as conn:
            row = conn.execute(
                f"""
                SELECT COUNT(*) AS request_count,
                       SUM(CASE WHEN outcome = 'succeeded' THEN 1 ELSE 0 END) AS succeeded,
                       SUM(CASE WHEN outcome = 'failed' THEN 1 ELSE 0 END) AS failed,
                       SUM(CASE WHEN outcome = 'started' THEN 1 ELSE 0 END) AS in_progress,
                       COALESCE(SUM(request_bytes), 0) AS request_bytes,
                       COALESCE(SUM(response_bytes), 0) AS response_bytes,
                       COALESCE(SUM(estimated_input_tokens), 0) AS estimated_input_tokens,
                       COALESCE(SUM(estimated_output_tokens), 0) AS estimated_output_tokens,
                       COALESCE(SUM(actual_total_tokens), 0) AS actual_total_tokens,
                       SUM(CASE WHEN actual_total_tokens IS NOT NULL THEN 1 ELSE 0 END)
                           AS actual_token_attempts,
                       COALESCE(SUM(retry_count), 0) AS retry_count
                FROM notion_request_attempts
                WHERE {" AND ".join(conditions)}
                """,
                tuple(parameters),
            ).fetchone()
        return {
            "window_seconds": window,
            "account_id": _opaque_account_id(account),
            "workload_class": workload,
            "request_count": int(row["request_count"] or 0),
            "succeeded": int(row["succeeded"] or 0),
            "failed": int(row["failed"] or 0),
            "in_progress": int(row["in_progress"] or 0),
            "request_bytes": int(row["request_bytes"] or 0),
            "response_bytes": int(row["response_bytes"] or 0),
            "estimated_input_tokens": int(row["estimated_input_tokens"] or 0),
            "estimated_output_tokens": int(row["estimated_output_tokens"] or 0),
            "actual_total_tokens": int(row["actual_total_tokens"] or 0),
            "actual_token_attempts": int(row["actual_token_attempts"] or 0),
            "retry_count": int(row["retry_count"] or 0),
            "token_accounting": "actual_and_estimated_reported_separately",
        }

    def _prune(self, conn: sqlite3.Connection, now: float) -> int:
        try:
            days = max(
                1.0, float(os.getenv("NOTION_REQUEST_TELEMETRY_RETENTION_DAYS", "30"))
            )
        except (TypeError, ValueError):
            days = 30.0
        cutoff = now - days * 86400.0
        return max(
            0,
            conn.execute(
                "DELETE FROM notion_request_attempts WHERE completed_at <= ?",
                (cutoff,),
            ).rowcount,
        )

    def start(self, receipt: dict[str, Any]) -> None:
        attempt_id = _bounded_text(receipt.get("attempt_id"), maximum=96)
        if not attempt_id:
            return
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._prune(conn, now)
            if conn.execute(
                "SELECT 1 FROM notion_request_attempts WHERE attempt_id = ?",
                (attempt_id,),
            ).fetchone():
                conn.commit()
                return
            account_key = _bounded_text(receipt.get("account_key"))
            workload_class = _bounded_text(
                receipt.get("workload_class") or "legacy", maximum=32
            )
            request_bytes = max(0, int(receipt.get("request_bytes") or 0))
            estimated_input_tokens = max(
                0, int(receipt.get("estimated_input_tokens") or 0)
            )
            quotas = conn.execute(
                "SELECT * FROM notion_usage_quotas WHERE enabled = 1 ORDER BY quota_id"
            ).fetchall()
            for quota in quotas:
                if not self._quota_matches(quota, account_key, workload_class):
                    continue
                status = self._quota_status(
                    conn,
                    quota,
                    now=now,
                    projected_requests=1,
                    projected_tokens=estimated_input_tokens,
                    projected_request_bytes=request_bytes,
                )
                if status["exceeded"]:
                    conn.rollback()
                    raise UsageQuotaExceededError(status)
            conn.execute(
                """
                INSERT OR IGNORE INTO notion_request_attempts (
                    attempt_id, created_at, started_at, account_key, thread_key,
                    operation, workload_class, admission_weight, trace_id,
                    request_context_id, model_id, outcome, request_bytes,
                    estimated_input_tokens, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'started', ?, ?, ?)
                """,
                (
                    attempt_id,
                    now,
                    now,
                    account_key,
                    _bounded_text(receipt.get("thread_key")),
                    _bounded_text(receipt.get("operation"), maximum=192),
                    workload_class,
                    float(receipt.get("admission_weight") or 1.0),
                    _bounded_text(receipt.get("trace_id")),
                    _bounded_text(receipt.get("request_context_id")),
                    _bounded_text(receipt.get("model_id")),
                    request_bytes,
                    estimated_input_tokens,
                    now,
                ),
            )
            conn.commit()

    def note_retry(
        self,
        attempt_id: str,
        *,
        retry_count: int,
        retry_after_seconds: float,
    ) -> None:
        attempt_id = _bounded_text(attempt_id, maximum=96)
        if not attempt_id:
            return
        now = time.time()
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE notion_request_attempts
                SET retry_count = ?, retry_after_seconds = ?, updated_at = ?
                WHERE attempt_id = ? AND completed_at IS NULL
                """,
                (
                    max(0, int(retry_count)),
                    max(0.0, float(retry_after_seconds)),
                    now,
                    str(attempt_id),
                ),
            )

    def finish(
        self,
        attempt_id: str,
        *,
        success: bool,
        status_code: int | None = None,
        response_bytes: int = 0,
        estimated_output_tokens: int = 0,
        actual_input_tokens: int | None = None,
        actual_output_tokens: int | None = None,
        actual_total_tokens: int | None = None,
        retry_count: int = 0,
        retry_after_seconds: float = 0.0,
        error_class: str = "",
    ) -> dict[str, Any]:
        attempt_id = _bounded_text(attempt_id, maximum=96)
        if not attempt_id:
            return {}
        now = time.time()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT started_at FROM notion_request_attempts WHERE attempt_id = ?",
                (str(attempt_id),),
            ).fetchone()
            started_at = float(row["started_at"]) if row else now
            duration_seconds = max(0.0, now - started_at)
            conn.execute(
                """
                UPDATE notion_request_attempts
                SET completed_at = ?, duration_seconds = ?, outcome = ?,
                    status_code = ?, response_bytes = ?,
                    estimated_output_tokens = ?, actual_input_tokens = ?,
                    actual_output_tokens = ?, actual_total_tokens = ?,
                    retry_count = ?, retry_after_seconds = ?, error_class = ?,
                    updated_at = ?
                WHERE attempt_id = ? AND completed_at IS NULL
                """,
                (
                    now,
                    duration_seconds,
                    "succeeded" if success else "failed",
                    status_code,
                    max(0, int(response_bytes or 0)),
                    max(0, int(estimated_output_tokens or 0)),
                    actual_input_tokens,
                    actual_output_tokens,
                    actual_total_tokens,
                    max(0, int(retry_count or 0)),
                    max(0.0, float(retry_after_seconds or 0.0)),
                    _bounded_error_class(error_class),
                    now,
                    str(attempt_id),
                ),
            )
            conn.commit()
        return self.get(attempt_id) or {}

    def get(self, attempt_id: str) -> dict[str, Any] | None:
        attempt_id = _bounded_text(attempt_id, maximum=96)
        if not attempt_id:
            return None
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM notion_request_attempts WHERE attempt_id = ?",
                (str(attempt_id),),
            ).fetchone()
        return {key: row[key] for key in row.keys()} if row else None

    def snapshot(self, *, limit: int = 20) -> dict[str, Any]:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            pruned = self._prune(conn, now)
            recent = conn.execute(
                """
                SELECT * FROM notion_request_attempts
                ORDER BY created_at DESC LIMIT ?
                """,
                (max(1, int(limit)),),
            ).fetchall()
            aggregates = conn.execute(
                """
                SELECT operation, workload_class, outcome,
                       COUNT(*) AS request_count,
                       AVG(duration_seconds) AS avg_duration_seconds,
                       SUM(request_bytes) AS request_bytes,
                       SUM(response_bytes) AS response_bytes,
                       SUM(estimated_input_tokens) AS estimated_input_tokens,
                       SUM(estimated_output_tokens) AS estimated_output_tokens,
                       SUM(COALESCE(actual_total_tokens, 0)) AS actual_total_tokens,
                       SUM(retry_count) AS retry_count
                FROM notion_request_attempts
                WHERE created_at >= ?
                GROUP BY operation, workload_class, outcome
                ORDER BY request_count DESC, operation
                """,
                (now - 3600.0,),
            ).fetchall()
            conn.commit()
        return {
            "database_path": str(self.path),
            "retention_pruned": pruned,
            "usage_last_hour": [
                {
                    "operation": str(row["operation"]),
                    "workload_class": str(row["workload_class"]),
                    "outcome": str(row["outcome"]),
                    "request_count": int(row["request_count"] or 0),
                    "avg_duration_seconds": round(
                        float(row["avg_duration_seconds"] or 0.0), 3
                    ),
                    "request_bytes": int(row["request_bytes"] or 0),
                    "response_bytes": int(row["response_bytes"] or 0),
                    "estimated_input_tokens": int(row["estimated_input_tokens"] or 0),
                    "estimated_output_tokens": int(row["estimated_output_tokens"] or 0),
                    "actual_total_tokens": int(row["actual_total_tokens"] or 0),
                    "retry_count": int(row["retry_count"] or 0),
                }
                for row in aggregates
            ],
            "recent_attempts": [
                {key: row[key] for key in row.keys()} for row in recent
            ],
        }
