from __future__ import annotations

import json
import sqlite3
import time
import uuid
from pathlib import Path
from typing import Any, Callable

from app.notion_admission_store import default_admission_db_path


class ModelRestrictionCache:
    """SQLite-backed cache with a cross-process single-flight refresh lease."""

    def __init__(
        self,
        path: str | Path | None = None,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.path = Path(path) if path is not None else default_admission_db_path()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._clock = clock
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
                CREATE TABLE IF NOT EXISTS model_restriction_cache (
                    cache_key TEXT PRIMARY KEY,
                    payload_json TEXT NOT NULL,
                    fetched_at REAL NOT NULL,
                    expires_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS model_restriction_refresh_leases (
                    cache_key TEXT PRIMARY KEY,
                    lease_token TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    acquired_at REAL NOT NULL,
                    expires_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_model_restriction_cache_expiry
                    ON model_restriction_cache(expires_at);
                CREATE INDEX IF NOT EXISTS idx_model_restriction_lease_expiry
                    ON model_restriction_refresh_leases(expires_at);
                """
            )

    def get(
        self,
        cache_key: str,
        *,
        allow_stale: bool = False,
    ) -> dict[str, Any] | None:
        now = self._clock()
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM model_restriction_cache WHERE cache_key = ?",
                (str(cache_key),),
            ).fetchone()
        if row is None:
            return None
        expires_at = float(row["expires_at"])
        if not allow_stale and expires_at <= now:
            return None
        try:
            payload = json.loads(str(row["payload_json"] or "{}"))
        except json.JSONDecodeError:
            return None
        return {
            "payload": payload,
            "fetched_at": float(row["fetched_at"]),
            "expires_at": expires_at,
            "state": "fresh" if expires_at > now else "stale",
        }

    def claim_refresh(
        self,
        cache_key: str,
        *,
        owner_id: str,
        lease_seconds: float = 30.0,
    ) -> str:
        now = self._clock()
        token = uuid.uuid4().hex
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            conn.execute(
                "DELETE FROM model_restriction_refresh_leases WHERE expires_at <= ?",
                (now,),
            )
            inserted = conn.execute(
                """
                INSERT OR IGNORE INTO model_restriction_refresh_leases (
                    cache_key, lease_token, owner_id, acquired_at, expires_at
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    str(cache_key),
                    token,
                    str(owner_id),
                    now,
                    now + max(5.0, float(lease_seconds)),
                ),
            ).rowcount
            conn.commit()
        return token if inserted else ""

    def store(
        self,
        cache_key: str,
        payload: dict[str, Any],
        *,
        lease_token: str,
        ttl_seconds: float = 300.0,
    ) -> bool:
        now = self._clock()
        with self._connect() as conn:
            conn.execute("BEGIN IMMEDIATE")
            lease = conn.execute(
                """
                SELECT 1 FROM model_restriction_refresh_leases
                WHERE cache_key = ? AND lease_token = ? AND expires_at > ?
                """,
                (str(cache_key), str(lease_token), now),
            ).fetchone()
            if lease is None:
                conn.commit()
                return False
            conn.execute(
                """
                INSERT INTO model_restriction_cache (
                    cache_key, payload_json, fetched_at, expires_at, updated_at
                ) VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    payload_json = excluded.payload_json,
                    fetched_at = excluded.fetched_at,
                    expires_at = excluded.expires_at,
                    updated_at = excluded.updated_at
                """,
                (
                    str(cache_key),
                    json.dumps(payload, sort_keys=True, default=str),
                    now,
                    now + max(1.0, float(ttl_seconds)),
                    now,
                ),
            )
            conn.execute(
                """
                DELETE FROM model_restriction_refresh_leases
                WHERE cache_key = ? AND lease_token = ?
                """,
                (str(cache_key), str(lease_token)),
            )
            conn.commit()
        return True

    def release_refresh(self, cache_key: str, *, lease_token: str) -> None:
        if not lease_token:
            return
        with self._connect() as conn:
            conn.execute(
                """
                DELETE FROM model_restriction_refresh_leases
                WHERE cache_key = ? AND lease_token = ?
                """,
                (str(cache_key), str(lease_token)),
            )

    def clear(self) -> None:
        """Clear cached values and leases. Intended for tests and maintenance."""
        with self._connect() as conn:
            conn.execute("DELETE FROM model_restriction_cache")
            conn.execute("DELETE FROM model_restriction_refresh_leases")
