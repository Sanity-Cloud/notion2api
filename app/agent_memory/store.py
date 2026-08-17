"""Durable SQLite state for derived memory and idempotent operation receipts."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
from typing import Any

from .models import AgentMemoryError, DerivedMemoryRecord, IdentityEnvelope, canonical_json


class AgentMemoryStore:
    def __init__(self, path: str) -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._init_schema()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self._lock, self._connect() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS memory_records (
                    derived_memory_id TEXT PRIMARY KEY,
                    workspace_ref TEXT NOT NULL,
                    memory_domain_id TEXT NOT NULL,
                    project_ref TEXT NOT NULL,
                    principal_ref TEXT NOT NULL,
                    lease_ref TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    state TEXT NOT NULL,
                    payload_text TEXT NOT NULL,
                    payload_hash TEXT NOT NULL,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_memory_scope
                  ON memory_records(workspace_ref, memory_domain_id, project_ref, principal_ref, lease_ref, state);
                CREATE TABLE IF NOT EXISTS operation_receipts (
                    idempotency_key TEXT PRIMARY KEY,
                    request_id TEXT NOT NULL,
                    operation TEXT NOT NULL,
                    request_hash TEXT NOT NULL,
                    identity_json TEXT NOT NULL DEFAULT '{}',
                    outcome TEXT NOT NULL,
                    result_json TEXT NOT NULL DEFAULT '{}',
                    upstream_locator TEXT NOT NULL DEFAULT '',
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS cancellations (
                    cancellation_ref TEXT PRIMARY KEY,
                    reason TEXT NOT NULL,
                    created_at REAL NOT NULL
                );
                """
            )
            columns = {
                str(row[1])
                for row in conn.execute("PRAGMA table_info(operation_receipts)").fetchall()
            }
            if "identity_json" not in columns:
                conn.execute(
                    "ALTER TABLE operation_receipts ADD COLUMN identity_json TEXT NOT NULL DEFAULT '{}'"
                )

    def begin_operation(
        self,
        *,
        idempotency_key: str,
        request_id: str,
        operation: str,
        request_hash: str,
        identity: dict[str, Any],
    ) -> dict[str, Any]:
        now = time.time()
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM operation_receipts WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
            if row:
                if row["request_hash"] != request_hash:
                    raise AgentMemoryError(
                        "IDEMPOTENCY_CONFLICT",
                        "idempotency key is already bound to a different request hash",
                    )
                existing = dict(row)
                existing["_new"] = False
                return existing
            conn.execute(
                """
                INSERT INTO operation_receipts
                  (idempotency_key, request_id, operation, request_hash, identity_json,
                   outcome, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'RUNNING', ?, ?)
                """,
                (
                    idempotency_key,
                    request_id,
                    operation,
                    request_hash,
                    canonical_json(identity),
                    now,
                    now,
                ),
            )
            return {
                "idempotency_key": idempotency_key,
                "request_id": request_id,
                "operation": operation,
                "request_hash": request_hash,
                "identity_json": canonical_json(identity),
                "outcome": "RUNNING",
                "result_json": "{}",
                "upstream_locator": "",
                "created_at": now,
                "updated_at": now,
                "_new": True,
            }

    def complete_operation(
        self,
        *,
        idempotency_key: str,
        outcome: str,
        result: dict[str, Any],
        upstream_locator: str = "",
    ) -> None:
        now = time.time()
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                UPDATE operation_receipts
                SET outcome = ?, result_json = ?, upstream_locator = ?, updated_at = ?
                WHERE idempotency_key = ?
                """,
                (outcome, canonical_json(result), str(upstream_locator or ""), now, idempotency_key),
            )

    def operation_result(self, idempotency_key: str) -> dict[str, Any] | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM operation_receipts WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone()
        if not row:
            return None
        result = dict(row)
        try:
            result["result"] = json.loads(result.pop("result_json"))
        except Exception:
            result["result"] = {}
        try:
            result["identity"] = json.loads(result.pop("identity_json"))
        except Exception:
            result["identity"] = {}
        return result

    def put_record(self, record: DerivedMemoryRecord) -> None:
        now = time.time()
        data = record.to_dict(include_payload=True)
        with self._lock, self._connect() as conn:
            conn.execute(
                """
                INSERT INTO memory_records
                  (derived_memory_id, workspace_ref, memory_domain_id, project_ref,
                   principal_ref, lease_ref, record_json, state, payload_text, payload_hash,
                   created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(derived_memory_id) DO UPDATE SET
                  record_json = excluded.record_json,
                  state = excluded.state,
                  payload_text = excluded.payload_text,
                  payload_hash = excluded.payload_hash,
                  updated_at = excluded.updated_at
                """,
                (
                    record.derived_memory_id,
                    record.identity.workspace_ref,
                    record.identity.memory_domain_id,
                    record.identity.project_ref,
                    record.identity.principal_ref,
                    record.identity.lease_ref,
                    canonical_json(data),
                    record.state,
                    record.payload,
                    record.payload_hash,
                    now,
                    now,
                ),
            )

    @staticmethod
    def _record_from_row(row: sqlite3.Row) -> DerivedMemoryRecord:
        data = json.loads(row["record_json"])
        identity = IdentityEnvelope(**data.pop("identity"))
        return DerivedMemoryRecord(identity=identity, **data)

    def get_record(self, derived_memory_id: str) -> DerivedMemoryRecord | None:
        with self._lock, self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM memory_records WHERE derived_memory_id = ?",
                (derived_memory_id,),
            ).fetchone()
        return self._record_from_row(row) if row else None

    def list_scope(self, identity: IdentityEnvelope) -> list[DerivedMemoryRecord]:
        with self._lock, self._connect() as conn:
            rows = conn.execute(
                """
                SELECT * FROM memory_records
                WHERE workspace_ref = ? AND memory_domain_id = ? AND project_ref = ?
                  AND principal_ref = ? AND lease_ref = ?
                ORDER BY updated_at DESC, derived_memory_id ASC
                """,
                identity.scope_key(),
            ).fetchall()
        return [self._record_from_row(row) for row in rows]

    def update_record(self, record: DerivedMemoryRecord) -> None:
        self.put_record(record)

    def cancel(self, cancellation_ref: str, reason: str) -> None:
        with self._lock, self._connect() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO cancellations(cancellation_ref, reason, created_at) VALUES (?, ?, ?)",
                (str(cancellation_ref), str(reason), time.time()),
            )

    def is_cancelled(self, cancellation_ref: str) -> bool:
        if not cancellation_ref:
            return False
        with self._lock, self._connect() as conn:
            return (
                conn.execute(
                    "SELECT 1 FROM cancellations WHERE cancellation_ref = ?",
                    (str(cancellation_ref),),
                ).fetchone()
                is not None
            )
