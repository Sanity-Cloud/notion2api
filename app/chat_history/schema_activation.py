"""Controlled, receipted activation of the additive chat-history archive schema."""

from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import time
from pathlib import Path
from typing import Any

from app.chat_history.contracts import HISTORY_SCHEMA_VERSION
from app.chat_history.lossless_archive import (
    archive_schema_is_current,
    ensure_archive_schema,
    history_schema_hash,
)


ACTIVATION_CONTRACT_VERSION = 1
_COUNT_TABLES = (
    "raw_notion_records",
    "raw_notion_record_observations",
    "notion_thread_messages",
    "notion_message_parts",
)


def _opaque_id(kind: str, value: str) -> str:
    return hashlib.sha256(f"notion2api:{kind}:{value}".encode("utf-8")).hexdigest()[:24]


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    return (
        conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)
        ).fetchone()
        is not None
    )


def _table_count(conn: sqlite3.Connection, table: str) -> int:
    if not _table_exists(conn, table):
        return 0
    return int(conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0])


def _canonical_raw_fingerprint(conn: sqlite3.Connection) -> str:
    digest = hashlib.sha256()
    if not _table_exists(conn, "raw_notion_records"):
        return digest.hexdigest()
    cursor = conn.execute(
        """
        SELECT account_key, workspace_id, table_name, record_id, version,
               last_version, raw_json, content_hash
        FROM raw_notion_records
        ORDER BY account_key, workspace_id, table_name, record_id, version
        """
    )
    for row in cursor:
        for value in row:
            encoded = b"<null>" if value is None else str(value).encode("utf-8", errors="replace")
            digest.update(len(encoded).to_bytes(8, "big"))
            digest.update(encoded)
    return digest.hexdigest()


def _database_integrity(conn: sqlite3.Connection) -> str:
    rows = [str(row[0]) for row in conn.execute("PRAGMA integrity_check").fetchall()]
    return "ok" if rows == ["ok"] else "; ".join(rows)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _state(conn: sqlite3.Connection) -> dict[str, Any]:
    return {
        "counts": {table: _table_count(conn, table) for table in _COUNT_TABLES},
        "canonical_raw_hash": _canonical_raw_fingerprint(conn),
    }


def absent_shard_receipt(db_path: str | Path) -> dict[str, Any]:
    """Receipt a governed shard target that has not been materialized yet."""
    source_path = Path(db_path).resolve()
    if source_path.suffix.lower() != ".db":
        raise ValueError("activation target must have a .db suffix")
    if source_path.exists():
        raise ValueError("absent-shard receipt requires a missing target")
    return {
        "activation_contract_version": ACTIVATION_CONTRACT_VERSION,
        "status": "not_present",
        "shard_id": _opaque_id("history-shard", str(source_path)),
        "history_schema_version": HISTORY_SCHEMA_VERSION,
        "history_schema_hash": history_schema_hash(),
        "history_schema_hash_scope": "declared_contract",
        "activated_at": int(time.time()),
    }


def activate_shard(
    db_path: str | Path,
    *,
    backup_dir: str | Path,
    timeout_seconds: float = 15.0,
    max_backup_attempts: int = 3,
) -> dict[str, Any]:
    """Activate schema v2 on one existing shard and return a non-secret receipt."""
    source_path = Path(db_path).resolve()
    if not source_path.is_file() or source_path.stat().st_size <= 0:
        raise ValueError("activation target must be an existing non-empty database file")
    if source_path.suffix.lower() != ".db":
        raise ValueError("activation target must have a .db suffix")

    backup_root = Path(backup_dir).resolve()
    backup_root.mkdir(parents=True, exist_ok=True)
    shard_id = _opaque_id("history-shard", str(source_path))
    timestamp = time.time_ns()
    backup_path = backup_root / f"{shard_id}-{timestamp}.pre-v{HISTORY_SCHEMA_VERSION}.db"

    conn = sqlite3.connect(source_path, timeout=timeout_seconds, isolation_level=None)
    conn.execute(f"PRAGMA busy_timeout={max(1, int(timeout_seconds * 1000))}")
    try:
        if _database_integrity(conn) != "ok":
            raise RuntimeError("pre-activation integrity_check failed")

        if archive_schema_is_current(conn):
            state = _state(conn)
            return {
                "activation_contract_version": ACTIVATION_CONTRACT_VERSION,
                "status": "already_active",
                "shard_id": shard_id,
                "history_schema_version": HISTORY_SCHEMA_VERSION,
                "history_schema_hash": history_schema_hash(),
                "history_schema_hash_scope": "declared_contract",
                "pre_state": state,
                "post_state": state,
                "integrity_check": "ok",
                "activated_at": int(time.time()),
            }

        for attempt in range(1, max_backup_attempts + 1):
            data_version_before = int(conn.execute("PRAGMA data_version").fetchone()[0])
            backup_conn = sqlite3.connect(backup_path)
            try:
                conn.backup(backup_conn, pages=256, sleep=0.05)
            finally:
                backup_conn.close()

            conn.execute("BEGIN EXCLUSIVE")
            data_version_after = int(conn.execute("PRAGMA data_version").fetchone()[0])
            if data_version_after == data_version_before:
                break
            conn.rollback()
            backup_path.unlink(missing_ok=True)
        else:
            raise RuntimeError("database changed during every bounded backup attempt")

        pre_state = _state(conn)
        ensure_archive_schema(conn)
        if not conn.in_transaction:
            raise RuntimeError("schema helper ended the exclusive activation transaction")

        post_state = _state(conn)
        if pre_state["canonical_raw_hash"] != post_state["canonical_raw_hash"]:
            raise RuntimeError("canonical raw history changed during schema activation")
        for table in ("raw_notion_records", "notion_thread_messages", "notion_message_parts"):
            if pre_state["counts"][table] != post_state["counts"][table]:
                raise RuntimeError(f"existing row count changed during activation: {table}")
        if (
            pre_state["counts"]["raw_notion_record_observations"]
            != post_state["counts"]["raw_notion_record_observations"]
        ):
            raise RuntimeError("schema activation backfilled raw observations")

        schema_version_row = conn.execute(
            "SELECT value FROM chat_history_archive_meta WHERE key='history_schema_version'"
        ).fetchone()
        if schema_version_row is None or str(schema_version_row[0]) != str(HISTORY_SCHEMA_VERSION):
            raise RuntimeError("activated schema version does not match the declared contract")
        if _database_integrity(conn) != "ok":
            raise RuntimeError("post-activation integrity_check failed")

        backup_conn = sqlite3.connect(f"file:{backup_path.as_posix()}?mode=ro", uri=True)
        try:
            backup_integrity = _database_integrity(backup_conn)
        finally:
            backup_conn.close()
        if backup_integrity != "ok":
            raise RuntimeError("backup integrity_check failed")
        backup_sha256 = _file_sha256(backup_path)
        backup_size = backup_path.stat().st_size
        conn.commit()
    except Exception:
        if conn.in_transaction:
            conn.rollback()
        raise
    finally:
        conn.close()

    return {
        "activation_contract_version": ACTIVATION_CONTRACT_VERSION,
        "status": "activated",
        "shard_id": shard_id,
        "backup_filename": backup_path.name,
        "backup_sha256": backup_sha256,
        "backup_size": backup_size,
        "history_schema_version": HISTORY_SCHEMA_VERSION,
        "history_schema_hash": history_schema_hash(),
        "history_schema_hash_scope": "declared_contract",
        "pre_state": pre_state,
        "post_state": post_state,
        "integrity_check": "ok",
        "backup_integrity_check": backup_integrity,
        "activated_at": int(time.time()),
    }


def write_activation_receipt(
    receipts: list[dict[str, Any]],
    *,
    receipt_path: str | Path,
    expected_commit: str,
    status: str = "completed",
    expected_shard_count: int | None = None,
    error_type: str | None = None,
) -> Path:
    if re.fullmatch(r"[0-9a-f]{7,64}", expected_commit) is None:
        raise ValueError("expected_commit must be a lowercase hexadecimal Git object ID")
    if status not in {"in_progress", "completed", "failed"}:
        raise ValueError("invalid activation receipt status")
    path = Path(receipt_path).resolve()
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "activation_contract_version": ACTIVATION_CONTRACT_VERSION,
        "expected_commit": expected_commit,
        "status": status,
        "expected_shard_count": expected_shard_count,
        "history_schema_version": HISTORY_SCHEMA_VERSION,
        "history_schema_hash": history_schema_hash(),
        "history_schema_hash_scope": "declared_contract",
        "shards": receipts,
        "updated_at": int(time.time()),
    }
    if error_type is not None:
        payload["error_type"] = error_type
    temporary_path = path.with_suffix(path.suffix + ".tmp")
    with temporary_path.open("w", encoding="utf-8", newline="\n") as handle:
        handle.write(json.dumps(payload, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary_path, path)
    return path
