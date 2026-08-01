from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


def default_admission_db_path() -> Path:
    configured = str(os.getenv("NOTION_ADMISSION_DB_PATH") or "").strip()
    if configured:
        return Path(configured)
    local_app_data = str(os.getenv("LOCALAPPDATA") or "").strip()
    if local_app_data:
        return (
            Path(local_app_data)
            / "SanityCloud"
            / "Notion2API"
            / "state"
            / "notion-admission.sqlite3"
        )
    return Path(tempfile.gettempdir()) / "sanitycloud-notion-admission.sqlite3"


@dataclass(frozen=True)
class SharedAcquireResult:
    status: str
    lease_id: str = ""
    retry_after_seconds: float = 0.05
    account_queue_depth: int = 0
    thread_queue_depth: int = 0
    reason: str = ""


class SharedAdmissionStore:
    """SQLite-backed cross-process Notion admission and token-bucket coordinator."""

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
                CREATE TABLE IF NOT EXISTS admission_waiters (
                    ticket_id TEXT PRIMARY KEY,
                    account_key TEXT NOT NULL,
                    thread_key TEXT NOT NULL DEFAULT '',
                    idempotency_key TEXT NOT NULL DEFAULT '',
                    owner_id TEXT NOT NULL,
                    operation TEXT NOT NULL DEFAULT '',
                    enqueued_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_admission_waiters_account
                    ON admission_waiters(account_key, enqueued_at, ticket_id);
                CREATE INDEX IF NOT EXISTS idx_admission_waiters_thread
                    ON admission_waiters(thread_key, enqueued_at, ticket_id);

                CREATE TABLE IF NOT EXISTS admission_leases (
                    lease_id TEXT PRIMARY KEY,
                    account_key TEXT NOT NULL,
                    thread_key TEXT NOT NULL DEFAULT '',
                    idempotency_key TEXT NOT NULL DEFAULT '',
                    owner_id TEXT NOT NULL,
                    operation TEXT NOT NULL DEFAULT '',
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_admission_leases_account
                    ON admission_leases(account_key, expires_at);
                CREATE UNIQUE INDEX IF NOT EXISTS idx_admission_leases_thread
                    ON admission_leases(thread_key)
                    WHERE thread_key <> '';
                CREATE UNIQUE INDEX IF NOT EXISTS idx_admission_leases_idempotency
                    ON admission_leases(idempotency_key)
                    WHERE idempotency_key <> '';

                CREATE TABLE IF NOT EXISTS admission_token_buckets (
                    account_key TEXT PRIMARY KEY,
                    tokens REAL NOT NULL,
                    updated_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admission_recent_idempotency (
                    idempotency_key TEXT PRIMARY KEY,
                    expires_at REAL NOT NULL
                );

                CREATE TABLE IF NOT EXISTS admission_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    created_at REAL NOT NULL,
                    process_id INTEGER NOT NULL,
                    event TEXT NOT NULL,
                    account_key TEXT NOT NULL DEFAULT '',
                    thread_key TEXT NOT NULL DEFAULT '',
                    idempotency_key TEXT NOT NULL DEFAULT '',
                    operation TEXT NOT NULL DEFAULT '',
                    disposition TEXT NOT NULL DEFAULT '',
                    queue_depth INTEGER NOT NULL DEFAULT 0,
                    waited_seconds REAL NOT NULL DEFAULT 0,
                    detail_json TEXT NOT NULL DEFAULT '{}'
                );
                CREATE INDEX IF NOT EXISTS idx_admission_events_created
                    ON admission_events(created_at DESC);
                """
            )

    @staticmethod
    def _event(
        conn: sqlite3.Connection,
        *,
        event: str,
        account_key: str = "",
        thread_key: str = "",
        idempotency_key: str = "",
        operation: str = "",
        disposition: str = "",
        queue_depth: int = 0,
        waited_seconds: float = 0.0,
        detail: dict[str, Any] | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO admission_events (
                created_at, process_id, event, account_key, thread_key,
                idempotency_key, operation, disposition, queue_depth,
                waited_seconds, detail_json
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                time.time(),
                os.getpid(),
                event,
                account_key,
                thread_key,
                idempotency_key,
                operation,
                disposition,
                int(queue_depth),
                float(waited_seconds),
                json.dumps(detail or {}, sort_keys=True, default=str),
            ),
        )

    @staticmethod
    def _prune(conn: sqlite3.Connection, now: float) -> dict[str, int]:
        stale_waiters = conn.execute(
            "DELETE FROM admission_waiters WHERE expires_at <= ?", (now,)
        ).rowcount
        stale_leases = conn.execute(
            "DELETE FROM admission_leases WHERE expires_at <= ?", (now,)
        ).rowcount
        stale_recent = conn.execute(
            "DELETE FROM admission_recent_idempotency WHERE expires_at <= ?", (now,)
        ).rowcount
        return {
            "waiters": max(0, stale_waiters),
            "leases": max(0, stale_leases),
            "recent": max(0, stale_recent),
        }

    def register_waiter(
        self,
        *,
        account_key: str,
        thread_key: str,
        idempotency_key: str,
        owner_id: str,
        operation: str,
        timeout_seconds: float,
    ) -> str:
        ticket_id = uuid.uuid4().hex
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            self._prune(conn, now)
            conn.execute(
                """
                INSERT INTO admission_waiters (
                    ticket_id, account_key, thread_key, idempotency_key,
                    owner_id, operation, enqueued_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ticket_id,
                    account_key,
                    thread_key,
                    idempotency_key,
                    owner_id,
                    operation,
                    now,
                    now + max(1.0, timeout_seconds + 5.0),
                ),
            )
            self._event(
                conn,
                event="waiter_registered",
                account_key=account_key,
                thread_key=thread_key,
                idempotency_key=idempotency_key,
                operation=operation,
            )
            conn.commit()
        return ticket_id

    def cancel_waiter(self, ticket_id: str, *, reason: str = "cancelled") -> None:
        if not ticket_id:
            return
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute(
                "SELECT * FROM admission_waiters WHERE ticket_id = ?", (ticket_id,)
            ).fetchone()
            conn.execute(
                "DELETE FROM admission_waiters WHERE ticket_id = ?", (ticket_id,)
            )
            if row:
                self._event(
                    conn,
                    event="waiter_cancelled",
                    account_key=str(row["account_key"]),
                    thread_key=str(row["thread_key"]),
                    idempotency_key=str(row["idempotency_key"]),
                    operation=str(row["operation"]),
                    disposition=reason,
                )
            conn.commit()

    def try_acquire(
        self,
        *,
        ticket_id: str,
        capacity: float,
        refill_per_second: float,
        max_account_inflight: int,
        lease_seconds: float,
        waited_seconds: float,
    ) -> SharedAcquireResult:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            pruned = self._prune(conn, now)
            waiter = conn.execute(
                "SELECT * FROM admission_waiters WHERE ticket_id = ?", (ticket_id,)
            ).fetchone()
            if waiter is None:
                conn.commit()
                return SharedAcquireResult(status="missing", reason="waiter_missing")

            account_key = str(waiter["account_key"])
            thread_key = str(waiter["thread_key"])
            idempotency_key = str(waiter["idempotency_key"])
            owner_id = str(waiter["owner_id"])
            operation = str(waiter["operation"])

            if idempotency_key:
                duplicate_lease = conn.execute(
                    "SELECT 1 FROM admission_leases WHERE idempotency_key = ? LIMIT 1",
                    (idempotency_key,),
                ).fetchone()
                duplicate_recent = conn.execute(
                    "SELECT 1 FROM admission_recent_idempotency WHERE idempotency_key = ? LIMIT 1",
                    (idempotency_key,),
                ).fetchone()
                if duplicate_lease or duplicate_recent:
                    conn.execute(
                        "DELETE FROM admission_waiters WHERE ticket_id = ?", (ticket_id,)
                    )
                    self._event(
                        conn,
                        event="duplicate_rejected",
                        account_key=account_key,
                        thread_key=thread_key,
                        idempotency_key=idempotency_key,
                        operation=operation,
                        disposition="duplicate",
                        waited_seconds=waited_seconds,
                    )
                    conn.commit()
                    return SharedAcquireResult(status="duplicate", reason="idempotency_key")

            account_predecessors = int(
                conn.execute(
                    """
                    SELECT COUNT(*) FROM admission_waiters
                    WHERE account_key = ?
                      AND (enqueued_at < ? OR (enqueued_at = ? AND ticket_id < ?))
                    """,
                    (
                        account_key,
                        float(waiter["enqueued_at"]),
                        float(waiter["enqueued_at"]),
                        ticket_id,
                    ),
                ).fetchone()[0]
            )
            thread_predecessors = 0
            if thread_key:
                thread_predecessors = int(
                    conn.execute(
                        """
                        SELECT COUNT(*) FROM admission_waiters
                        WHERE thread_key = ?
                          AND (enqueued_at < ? OR (enqueued_at = ? AND ticket_id < ?))
                        """,
                        (
                            thread_key,
                            float(waiter["enqueued_at"]),
                            float(waiter["enqueued_at"]),
                            ticket_id,
                        ),
                    ).fetchone()[0]
                )

            account_inflight = int(
                conn.execute(
                    "SELECT COUNT(*) FROM admission_leases WHERE account_key = ?",
                    (account_key,),
                ).fetchone()[0]
            )
            thread_inflight = bool(
                thread_key
                and conn.execute(
                    "SELECT 1 FROM admission_leases WHERE thread_key = ? LIMIT 1",
                    (thread_key,),
                ).fetchone()
            )

            queue_depth = account_predecessors + account_inflight
            thread_queue_depth = thread_predecessors + (1 if thread_inflight else 0)
            if account_predecessors or thread_predecessors:
                conn.commit()
                return SharedAcquireResult(
                    status="waiting",
                    retry_after_seconds=0.05,
                    account_queue_depth=queue_depth,
                    thread_queue_depth=thread_queue_depth,
                    reason="fifo_predecessor",
                )
            if account_inflight >= max(1, max_account_inflight):
                conn.commit()
                return SharedAcquireResult(
                    status="waiting",
                    retry_after_seconds=0.05,
                    account_queue_depth=queue_depth,
                    thread_queue_depth=thread_queue_depth,
                    reason="account_inflight",
                )
            if thread_inflight:
                conn.commit()
                return SharedAcquireResult(
                    status="waiting",
                    retry_after_seconds=0.05,
                    account_queue_depth=queue_depth,
                    thread_queue_depth=thread_queue_depth,
                    reason="thread_inflight",
                )

            capacity = max(1.0, float(capacity))
            refill_per_second = max(0.001, float(refill_per_second))
            bucket = conn.execute(
                "SELECT tokens, updated_at FROM admission_token_buckets WHERE account_key = ?",
                (account_key,),
            ).fetchone()
            if bucket:
                tokens = min(
                    capacity,
                    float(bucket["tokens"])
                    + max(0.0, now - float(bucket["updated_at"])) * refill_per_second,
                )
            else:
                tokens = capacity
            if tokens < 1.0:
                delay = (1.0 - tokens) / refill_per_second
                conn.execute(
                    """
                    INSERT INTO admission_token_buckets(account_key, tokens, updated_at)
                    VALUES (?, ?, ?)
                    ON CONFLICT(account_key) DO UPDATE SET
                        tokens = excluded.tokens,
                        updated_at = excluded.updated_at
                    """,
                    (account_key, tokens, now),
                )
                conn.commit()
                return SharedAcquireResult(
                    status="throttled",
                    retry_after_seconds=max(0.01, delay),
                    account_queue_depth=queue_depth,
                    thread_queue_depth=thread_queue_depth,
                    reason="token_bucket",
                )

            lease_id = uuid.uuid4().hex
            conn.execute(
                """
                INSERT INTO admission_token_buckets(account_key, tokens, updated_at)
                VALUES (?, ?, ?)
                ON CONFLICT(account_key) DO UPDATE SET
                    tokens = excluded.tokens,
                    updated_at = excluded.updated_at
                """,
                (account_key, max(0.0, tokens - 1.0), now),
            )
            conn.execute(
                """
                INSERT INTO admission_leases (
                    lease_id, account_key, thread_key, idempotency_key,
                    owner_id, operation, acquired_at, expires_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    lease_id,
                    account_key,
                    thread_key,
                    idempotency_key,
                    owner_id,
                    operation,
                    now,
                    now + max(5.0, lease_seconds),
                ),
            )
            conn.execute(
                "DELETE FROM admission_waiters WHERE ticket_id = ?", (ticket_id,)
            )
            self._event(
                conn,
                event="lease_acquired",
                account_key=account_key,
                thread_key=thread_key,
                idempotency_key=idempotency_key,
                operation=operation,
                disposition=(
                    "recovered_stale_lease"
                    if pruned.get("leases")
                    else "admitted"
                ),
                queue_depth=max(queue_depth, thread_queue_depth),
                waited_seconds=waited_seconds,
                detail={"stale_pruned": pruned},
            )
            conn.commit()
            return SharedAcquireResult(
                status="acquired",
                lease_id=lease_id,
                account_queue_depth=queue_depth,
                thread_queue_depth=thread_queue_depth,
            )

    def heartbeat(self, lease_id: str, *, lease_seconds: float) -> bool:
        if not lease_id:
            return False
        now = time.time()
        with self._connect() as conn:
            updated = conn.execute(
                "UPDATE admission_leases SET expires_at = ? WHERE lease_id = ?",
                (now + max(5.0, lease_seconds), lease_id),
            ).rowcount
            return bool(updated)

    def release(
        self,
        lease_id: str,
        *,
        success: bool,
        idempotency_ttl: float,
        waited_seconds: float = 0.0,
    ) -> None:
        if not lease_id:
            return
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                "SELECT * FROM admission_leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            conn.execute(
                "DELETE FROM admission_leases WHERE lease_id = ?", (lease_id,)
            )
            if lease:
                idempotency_key = str(lease["idempotency_key"])
                if success and idempotency_key and idempotency_ttl > 0:
                    conn.execute(
                        """
                        INSERT INTO admission_recent_idempotency(idempotency_key, expires_at)
                        VALUES (?, ?)
                        ON CONFLICT(idempotency_key) DO UPDATE SET
                            expires_at = excluded.expires_at
                        """,
                        (idempotency_key, now + idempotency_ttl),
                    )
                self._event(
                    conn,
                    event="lease_released",
                    account_key=str(lease["account_key"]),
                    thread_key=str(lease["thread_key"]),
                    idempotency_key=idempotency_key,
                    operation=str(lease["operation"]),
                    disposition="completed" if success else "failed",
                    waited_seconds=waited_seconds,
                )
            conn.commit()

    def note_retry(
        self,
        lease_id: str,
        *,
        retry_after_seconds: float,
        retry_count: int,
    ) -> None:
        if not lease_id:
            return
        with self._connect() as conn:
            lease = conn.execute(
                "SELECT * FROM admission_leases WHERE lease_id = ?", (lease_id,)
            ).fetchone()
            if not lease:
                return
            self._event(
                conn,
                event="retry_after",
                account_key=str(lease["account_key"]),
                thread_key=str(lease["thread_key"]),
                idempotency_key=str(lease["idempotency_key"]),
                operation=str(lease["operation"]),
                disposition="retry_after",
                detail={
                    "retry_after_seconds": retry_after_seconds,
                    "retry_count": retry_count,
                },
            )

    def snapshot(self) -> dict[str, Any]:
        now = time.time()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            pruned = self._prune(conn, now)
            account_rows = conn.execute(
                """
                SELECT account_key, COUNT(*) AS count
                FROM admission_leases GROUP BY account_key
                """
            ).fetchall()
            active_threads = int(
                conn.execute(
                    "SELECT COUNT(*) FROM admission_leases WHERE thread_key <> ''"
                ).fetchone()[0]
            )
            waiters = int(
                conn.execute("SELECT COUNT(*) FROM admission_waiters").fetchone()[0]
            )
            recent = int(
                conn.execute(
                    "SELECT COUNT(*) FROM admission_recent_idempotency"
                ).fetchone()[0]
            )
            events = conn.execute(
                """
                SELECT created_at, process_id, event, account_key, thread_key,
                       idempotency_key, operation, disposition, queue_depth,
                       waited_seconds, detail_json
                FROM admission_events ORDER BY id DESC LIMIT 20
                """
            ).fetchall()
            event_counts = conn.execute(
                """
                SELECT event, COUNT(*) AS count
                FROM admission_events
                WHERE created_at >= ?
                GROUP BY event
                """,
                (now - 3600.0,),
            ).fetchall()
            conn.commit()
        return {
            "enabled": True,
            "database_path": str(self.path),
            "active_accounts": {
                str(row["account_key"]): int(row["count"]) for row in account_rows
            },
            "active_threads": active_threads,
            "queue_depth": waiters,
            "recent_idempotency_keys": recent,
            "stale_pruned": pruned,
            "event_counts_last_hour": {
                str(row["event"]): int(row["count"]) for row in event_counts
            },
            "recent_events": [
                {
                    "created_at": float(row["created_at"]),
                    "process_id": int(row["process_id"]),
                    "event": str(row["event"]),
                    "account_key": str(row["account_key"]),
                    "thread_key": str(row["thread_key"]),
                    "idempotency_key": str(row["idempotency_key"]),
                    "operation": str(row["operation"]),
                    "disposition": str(row["disposition"]),
                    "queue_depth": int(row["queue_depth"]),
                    "waited_seconds": float(row["waited_seconds"]),
                    "detail": json.loads(str(row["detail_json"] or "{}")),
                }
                for row in events
            ],
        }
