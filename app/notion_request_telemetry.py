from __future__ import annotations

import os
import sqlite3
import time
from pathlib import Path
from typing import Any

from app.notion_admission_store import default_admission_db_path


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
                """
            )

    def _prune(self, conn: sqlite3.Connection, now: float) -> int:
        try:
            days = max(1.0, float(os.getenv("NOTION_REQUEST_TELEMETRY_RETENTION_DAYS", "30")))
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
                    _bounded_text(receipt.get("account_key")),
                    _bounded_text(receipt.get("thread_key")),
                    _bounded_text(receipt.get("operation"), maximum=192),
                    _bounded_text(
                        receipt.get("workload_class") or "legacy", maximum=32
                    ),
                    float(receipt.get("admission_weight") or 1.0),
                    _bounded_text(receipt.get("trace_id")),
                    _bounded_text(receipt.get("request_context_id")),
                    _bounded_text(receipt.get("model_id")),
                    max(0, int(receipt.get("request_bytes") or 0)),
                    max(0, int(receipt.get("estimated_input_tokens") or 0)),
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
                    "estimated_input_tokens": int(
                        row["estimated_input_tokens"] or 0
                    ),
                    "estimated_output_tokens": int(
                        row["estimated_output_tokens"] or 0
                    ),
                    "actual_total_tokens": int(row["actual_total_tokens"] or 0),
                    "retry_count": int(row["retry_count"] or 0),
                }
                for row in aggregates
            ],
            "recent_attempts": [
                {key: row[key] for key in row.keys()} for row in recent
            ],
        }
