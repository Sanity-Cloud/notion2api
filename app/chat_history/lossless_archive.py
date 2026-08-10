"""Additive lossless Notion thread/thread_message archive schema and helpers.

Notion server records are upstream authority. This module mirrors them version-aware
into account-scoped SQLite without discarding hidden/unknown steps. The legacy
chat_threads/chat_messages tables remain the visible-transcript projection.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from typing import Any

from app.chat_history.contracts import (
    HISTORY_SCHEMA_VERSION,
    PARSER_CONTRACT_VERSION,
    PROJECTION_VERSION,
)

ARCHIVE_DDL = """
CREATE TABLE IF NOT EXISTS raw_notion_records (
  account_key TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  notion_user_id TEXT,
  table_name TEXT NOT NULL,
  record_id TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT -1,
  last_version INTEGER,
  raw_json TEXT NOT NULL,
  content_hash TEXT,
  source_kind TEXT,
  source_endpoint TEXT,
  retrieved_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  imported_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  PRIMARY KEY (account_key, workspace_id, table_name, record_id, version)
);
CREATE TABLE IF NOT EXISTS raw_notion_record_observations (
  account_key TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  notion_user_id TEXT,
  table_name TEXT NOT NULL,
  record_id TEXT NOT NULL,
  version INTEGER NOT NULL DEFAULT -1,
  last_version INTEGER,
  content_hash TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  source_kind TEXT,
  source_endpoint TEXT,
  sync_run_id TEXT,
  observed_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  PRIMARY KEY (account_key, workspace_id, table_name, record_id, version, content_hash)
);
CREATE TABLE IF NOT EXISTS notion_thread_messages (
  account_key TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  notion_user_id TEXT,
  message_id TEXT NOT NULL,
  thread_id TEXT,
  step_type TEXT NOT NULL DEFAULT 'unknown',
  visible INTEGER NOT NULL DEFAULT 0,
  role TEXT,
  text TEXT NOT NULL DEFAULT '',
  created_time TEXT,
  version INTEGER NOT NULL DEFAULT -1,
  last_version INTEGER,
  inference_id TEXT,
  trace_id TEXT,
  model_metadata_json TEXT NOT NULL DEFAULT '{}',
  raw_json TEXT NOT NULL DEFAULT '{}',
  parser_contract_version TEXT NOT NULL DEFAULT 'unversioned',
  projection_version INTEGER NOT NULL DEFAULT 0,
  source_content_hash TEXT,
  projected_at INTEGER,
  normalization_outcome TEXT NOT NULL DEFAULT 'unversioned',
  normalization_warnings_json TEXT NOT NULL DEFAULT '[]',
  tombstone_status TEXT,
  imported_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  updated_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
  PRIMARY KEY (account_key, workspace_id, message_id, version)
);
CREATE TABLE IF NOT EXISTS notion_message_parts (
  account_key TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  message_id TEXT NOT NULL,
  message_version INTEGER NOT NULL DEFAULT -1,
  ordinal INTEGER NOT NULL,
  part_type TEXT NOT NULL DEFAULT 'unknown',
  text TEXT,
  content_json TEXT,
  raw_json TEXT NOT NULL DEFAULT '{}',
  parser_contract_version TEXT NOT NULL DEFAULT 'unversioned',
  projection_version INTEGER NOT NULL DEFAULT 0,
  source_content_hash TEXT,
  projected_at INTEGER,
  PRIMARY KEY (account_key, workspace_id, message_id, message_version, ordinal)
);
CREATE TABLE IF NOT EXISTS chat_history_archive_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_runs (
  sync_run_id TEXT PRIMARY KEY,
  account_key TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  notion_user_id TEXT,
  source_kind TEXT NOT NULL DEFAULT 'notion_server',
  status TEXT NOT NULL DEFAULT 'running',
  started_at INTEGER NOT NULL,
  finished_at INTEGER,
  pages_scanned INTEGER NOT NULL DEFAULT 0,
  records_persisted INTEGER NOT NULL DEFAULT 0,
  hydration_failed INTEGER NOT NULL DEFAULT 0,
  hydration_partial INTEGER NOT NULL DEFAULT 0,
  checkpoint_advanced INTEGER NOT NULL DEFAULT 0,
  metrics_json TEXT NOT NULL DEFAULT '{}',
  error_text TEXT
);
CREATE TABLE IF NOT EXISTS sync_cursors (
  account_key TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  cursor_name TEXT NOT NULL,
  cursor_value TEXT,
  sync_run_id TEXT,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (account_key, workspace_id, cursor_name)
);
CREATE TABLE IF NOT EXISTS reconciliation_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  account_key TEXT NOT NULL,
  workspace_id TEXT NOT NULL,
  sync_run_id TEXT,
  event_type TEXT NOT NULL,
  table_name TEXT,
  record_id TEXT,
  details_json TEXT NOT NULL DEFAULT '{}',
  created_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
"""

ARCHIVE_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_raw_notion_records_lookup
  ON raw_notion_records(account_key, workspace_id, table_name, record_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_raw_notion_record_observations_lookup
  ON raw_notion_record_observations(
    account_key, workspace_id, table_name, record_id, version DESC, observed_at DESC
  );
CREATE INDEX IF NOT EXISTS idx_notion_thread_messages_thread
  ON notion_thread_messages(account_key, workspace_id, thread_id, created_time, message_id);
CREATE INDEX IF NOT EXISTS idx_notion_thread_messages_latest
  ON notion_thread_messages(account_key, workspace_id, message_id, version DESC);
CREATE INDEX IF NOT EXISTS idx_notion_message_parts_message
  ON notion_message_parts(account_key, workspace_id, message_id, message_version, ordinal);
CREATE INDEX IF NOT EXISTS idx_sync_runs_account_started
  ON sync_runs(account_key, workspace_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_reconciliation_events_lookup
  ON reconciliation_events(account_key, workspace_id, event_type, created_at DESC);
"""


def _json_dumps(value: Any, fallback: Any = None) -> str:
    try:
        return json.dumps(value if value is not None else fallback, ensure_ascii=False, default=str)
    except TypeError:
        return json.dumps(fallback, ensure_ascii=False, default=str)


def content_hash_for(raw: Any) -> str:
    payload = raw if isinstance(raw, str) else _json_dumps(raw, {})
    return hashlib.sha256(payload.encode("utf-8", errors="replace")).hexdigest()


def coerce_version(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def ensure_archive_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(ARCHIVE_DDL)
    _ensure_projection_lineage_columns(conn)
    conn.executescript(ARCHIVE_INDEX_DDL)
    conn.execute(
        "INSERT OR REPLACE INTO chat_history_archive_meta(key,value) VALUES('history_schema_version',?)",
        (str(HISTORY_SCHEMA_VERSION),),
    )


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table_name})").fetchall()}


def _ensure_columns(
    conn: sqlite3.Connection, table_name: str, columns: dict[str, str]
) -> None:
    existing = _table_columns(conn, table_name)
    for name, definition in columns.items():
        if name not in existing:
            conn.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{name}" {definition}')


def _ensure_projection_lineage_columns(conn: sqlite3.Connection) -> None:
    common = {
        "parser_contract_version": "TEXT NOT NULL DEFAULT 'unversioned'",
        "projection_version": "INTEGER NOT NULL DEFAULT 0",
        "source_content_hash": "TEXT",
        "projected_at": "INTEGER",
    }
    _ensure_columns(
        conn,
        "notion_thread_messages",
        {
            **common,
            "normalization_outcome": "TEXT NOT NULL DEFAULT 'unversioned'",
            "normalization_warnings_json": "TEXT NOT NULL DEFAULT '[]'",
        },
    )
    _ensure_columns(conn, "notion_message_parts", common)


def history_schema_hash() -> str:
    """Return a stable fingerprint for the effective archive schema contract."""
    payload = f"{HISTORY_SCHEMA_VERSION}\n{ARCHIVE_DDL}\n{ARCHIVE_INDEX_DDL}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def latest_record_version(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    workspace_id: str,
    table_name: str,
    record_id: str,
) -> int | None:
    row = conn.execute(
        """
        SELECT version FROM raw_notion_records
        WHERE account_key=? AND workspace_id=? AND table_name=? AND record_id=?
        ORDER BY version DESC LIMIT 1
        """,
        (account_key, workspace_id, table_name, record_id),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def latest_message_version(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    workspace_id: str,
    message_id: str,
) -> int | None:
    row = conn.execute(
        """
        SELECT version FROM notion_thread_messages
        WHERE account_key=? AND workspace_id=? AND message_id=?
        ORDER BY version DESC LIMIT 1
        """,
        (account_key, workspace_id, message_id),
    ).fetchone()
    if row is None:
        return None
    return int(row[0])


def message_ids_fresh_at_versions(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    workspace_id: str,
    candidates: dict[str, int | None],
) -> set[str]:
    """Return message IDs whose archived version is present and >= candidate version.

    Missing candidates or candidates with unknown/higher server versions are NOT returned
    (they still need hydration). When candidate version is None/-1, any archived row counts
    as fresh only if a row exists (presence check).
    """
    fresh: set[str] = set()
    if not candidates:
        return fresh
    ids = sorted(candidates.keys())
    for start in range(0, len(ids), 400):
        chunk = ids[start : start + 400]
        placeholders = ",".join("?" for _ in chunk)
        rows = conn.execute(
            f"""
            SELECT message_id,
                   MAX(
                     CASE
                       WHEN version >= 0
                        AND COALESCE(NULLIF(TRIM(step_type), ''), 'unknown') != 'unknown'
                       THEN version
                       ELSE NULL
                     END
                   ) AS max_version
            FROM notion_thread_messages
            WHERE account_key=? AND workspace_id=? AND message_id IN ({placeholders})
            GROUP BY message_id
            """,
            (account_key, workspace_id, *chunk),
        ).fetchall()
        archived = {
            str(row[0]): int(row[1])
            for row in rows
            if row[1] is not None
        }
        for message_id in chunk:
            archived_version = archived.get(message_id)
            if archived_version is None:
                continue
            server_version = coerce_version(candidates.get(message_id))
            if server_version is None or server_version < 0:
                fresh.add(message_id)
                continue
            if archived_version >= server_version:
                fresh.add(message_id)
    return fresh


def persist_raw_record(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    workspace_id: str,
    notion_user_id: str | None,
    table_name: str,
    record_id: str,
    version: int | None,
    last_version: int | None,
    raw: Any,
    source_kind: str | None = None,
    source_endpoint: str | None = None,
    sync_run_id: str | None = None,
) -> bool:
    """Preserve one raw observation and refresh its canonical version projection.

    Returns True only when this identity/version/content-hash observation is new.
    """
    resolved_version = coerce_version(version)
    if resolved_version is None:
        resolved_version = -1
    raw_json = _json_dumps(raw, {})
    digest = content_hash_for(raw_json)
    now = int(time.time())
    observation_insert = conn.execute(
        """
        INSERT OR IGNORE INTO raw_notion_record_observations(
          account_key, workspace_id, notion_user_id, table_name, record_id, version,
          last_version, content_hash, raw_json, source_kind, source_endpoint,
          sync_run_id, observed_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        """,
        (
            account_key,
            workspace_id,
            notion_user_id,
            table_name,
            record_id,
            resolved_version,
            coerce_version(last_version),
            digest,
            raw_json,
            source_kind,
            source_endpoint,
            sync_run_id,
            now,
        ),
    )
    conn.execute(
        """
        INSERT INTO raw_notion_records(
          account_key, workspace_id, notion_user_id, table_name, record_id, version,
          last_version, raw_json, content_hash, source_kind, source_endpoint, retrieved_at, imported_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
        ON CONFLICT(account_key, workspace_id, table_name, record_id, version) DO UPDATE SET
          last_version=COALESCE(excluded.last_version, raw_notion_records.last_version),
          raw_json=excluded.raw_json,
          content_hash=excluded.content_hash,
          source_kind=COALESCE(excluded.source_kind, raw_notion_records.source_kind),
          source_endpoint=COALESCE(excluded.source_endpoint, raw_notion_records.source_endpoint),
          retrieved_at=excluded.retrieved_at
        """,
        (
            account_key,
            workspace_id,
            notion_user_id,
            table_name,
            record_id,
            resolved_version,
            coerce_version(last_version),
            raw_json,
            digest,
            source_kind,
            source_endpoint,
            now,
            now,
        ),
    )
    return observation_insert.rowcount == 1


def persist_thread_message_record(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    workspace_id: str,
    notion_user_id: str | None,
    record: dict[str, Any],
) -> dict[str, int]:
    """Persist a semantic thread_message + parts. Hidden/unknown steps are first-class."""
    result = {"messages_inserted": 0, "messages_updated": 0, "parts_written": 0, "skipped": 0}
    message_id = str(record.get("id") or "").strip()
    if not message_id:
        result["skipped"] += 1
        return result
    version = coerce_version(record.get("version"))
    if version is None:
        version = -1
    now = int(time.time())
    existing = conn.execute(
        """
        SELECT version, raw_json FROM notion_thread_messages
        WHERE account_key=? AND workspace_id=? AND message_id=? AND version=?
        """,
        (account_key, workspace_id, message_id, version),
    ).fetchone()
    # Match the exact wrapper hashed by persist_raw_record so the semantic row
    # can be traced back to one source observation without ambiguity.
    raw_json = _json_dumps(record.get("raw_wrapper") or record.get("raw") or {}, {})
    source_content_hash = content_hash_for(raw_json)
    source_observation = conn.execute(
        """
        SELECT 1 FROM raw_notion_record_observations
        WHERE account_key=? AND workspace_id=? AND table_name='thread_message'
          AND record_id=? AND version=? AND content_hash=?
        """,
        (
            account_key,
            workspace_id,
            message_id,
            version,
            source_content_hash,
        ),
    ).fetchone()
    if source_observation is None:
        raise ValueError(
            "semantic projection requires an exact archived source observation"
        )
    warnings = [
        str(warning)
        for warning in (record.get("normalization_warnings") or [])
        if str(warning).strip()
    ]
    # Compatibility for manually constructed semantic records that did not pass
    # through the current parser contract.
    if version < 0 and "unknown_version" not in warnings:
        warnings.append("unknown_version")
    if (
        str(record.get("step_type") or "unknown") == "unknown"
        and "unknown_step_type" not in warnings
    ):
        warnings.append("unknown_step_type")
    normalization_outcome = str(record.get("normalization_outcome") or "").strip()
    if not normalization_outcome:
        normalization_outcome = "normalized_with_warnings" if warnings else "normalized"
    conn.execute(
        """
        INSERT INTO notion_thread_messages(
          account_key, workspace_id, notion_user_id, message_id, thread_id, step_type,
          visible, role, text, created_time, version, last_version, inference_id, trace_id,
          model_metadata_json, raw_json, parser_contract_version, projection_version,
          source_content_hash, projected_at, normalization_outcome,
          normalization_warnings_json, tombstone_status, imported_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,NULL,?,?)
        ON CONFLICT(account_key, workspace_id, message_id, version) DO UPDATE SET
          thread_id=excluded.thread_id,
          step_type=excluded.step_type,
          visible=excluded.visible,
          role=excluded.role,
          text=excluded.text,
          created_time=excluded.created_time,
          last_version=COALESCE(excluded.last_version, notion_thread_messages.last_version),
          inference_id=excluded.inference_id,
          trace_id=excluded.trace_id,
          model_metadata_json=excluded.model_metadata_json,
          raw_json=excluded.raw_json,
          parser_contract_version=excluded.parser_contract_version,
          projection_version=excluded.projection_version,
          source_content_hash=excluded.source_content_hash,
          projected_at=excluded.projected_at,
          normalization_outcome=excluded.normalization_outcome,
          normalization_warnings_json=excluded.normalization_warnings_json,
          tombstone_status=NULL,
          updated_at=excluded.updated_at
        """,
        (
            account_key,
            workspace_id,
            notion_user_id,
            message_id,
            str(record.get("thread_id") or "") or None,
            str(record.get("step_type") or "unknown"),
            1 if record.get("visible") else 0,
            record.get("role"),
            str(record.get("text") or ""),
            str(record.get("created_time") or "") or None,
            version,
            coerce_version(record.get("last_version")),
            str(record.get("inference_id") or "") or None,
            str(record.get("trace_id") or "") or None,
            _json_dumps(record.get("model_metadata") or {}, {}),
            raw_json,
            PARSER_CONTRACT_VERSION,
            PROJECTION_VERSION,
            source_content_hash,
            now,
            normalization_outcome,
            _json_dumps(warnings, []),
            now,
            now,
        ),
    )
    if existing is None:
        result["messages_inserted"] += 1
    elif str(existing[1] or "") != raw_json:
        result["messages_updated"] += 1

    conn.execute(
        """
        DELETE FROM notion_message_parts
        WHERE account_key=? AND workspace_id=? AND message_id=? AND message_version=?
        """,
        (account_key, workspace_id, message_id, version),
    )
    parts = record.get("parts") if isinstance(record.get("parts"), list) else []
    for part in parts:
        if not isinstance(part, dict):
            continue
        ordinal = int(part.get("ordinal") or 0)
        conn.execute(
            """
            INSERT INTO notion_message_parts(
              account_key, workspace_id, message_id, message_version, ordinal,
              part_type, text, content_json, raw_json, parser_contract_version,
              projection_version, source_content_hash, projected_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                account_key,
                workspace_id,
                message_id,
                version,
                ordinal,
                str(part.get("part_type") or "unknown"),
                part.get("text"),
                _json_dumps(part.get("content"), None) if part.get("content") is not None else None,
                _json_dumps(part.get("raw") or part, {}),
                PARSER_CONTRACT_VERSION,
                PROJECTION_VERSION,
                source_content_hash,
                now,
            ),
        )
        result["parts_written"] += 1
    return result


def mark_server_omission(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    workspace_id: str,
    sync_run_id: str | None,
    table_name: str,
    record_id: str,
    details: dict[str, Any] | None = None,
) -> None:
    """Record server omission/deletion without destroying archived history."""
    if table_name == "thread_message":
        conn.execute(
            """
            UPDATE notion_thread_messages
            SET tombstone_status='server_omitted', updated_at=?
            WHERE account_key=? AND workspace_id=? AND message_id=?
              AND version = (
                SELECT MAX(version) FROM notion_thread_messages n2
                WHERE n2.account_key=notion_thread_messages.account_key
                  AND n2.workspace_id=notion_thread_messages.workspace_id
                  AND n2.message_id=notion_thread_messages.message_id
              )
            """,
            (int(time.time()), account_key, workspace_id, record_id),
        )
    conn.execute(
        """
        INSERT INTO reconciliation_events(
          account_key, workspace_id, sync_run_id, event_type, table_name, record_id, details_json
        ) VALUES (?,?,?,?,?,?,?)
        """,
        (
            account_key,
            workspace_id,
            sync_run_id,
            "server_omitted",
            table_name,
            record_id,
            _json_dumps(details or {}, {}),
        ),
    )


def begin_sync_run(
    conn: sqlite3.Connection,
    *,
    sync_run_id: str,
    account_key: str,
    workspace_id: str,
    notion_user_id: str | None = None,
    source_kind: str = "notion_server",
) -> None:
    conn.execute(
        """
        INSERT INTO sync_runs(
          sync_run_id, account_key, workspace_id, notion_user_id, source_kind, status, started_at
        ) VALUES (?,?,?,?,?,'running',?)
        """,
        (sync_run_id, account_key, workspace_id, notion_user_id, source_kind, int(time.time())),
    )


def finish_sync_run(
    conn: sqlite3.Connection,
    *,
    sync_run_id: str,
    status: str,
    pages_scanned: int = 0,
    records_persisted: int = 0,
    hydration_failed: int = 0,
    hydration_partial: int = 0,
    checkpoint_advanced: bool = False,
    metrics: dict[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    conn.execute(
        """
        UPDATE sync_runs SET
          status=?,
          finished_at=?,
          pages_scanned=?,
          records_persisted=?,
          hydration_failed=?,
          hydration_partial=?,
          checkpoint_advanced=?,
          metrics_json=?,
          error_text=?
        WHERE sync_run_id=?
        """,
        (
            status,
            int(time.time()),
            pages_scanned,
            records_persisted,
            hydration_failed,
            hydration_partial,
            1 if checkpoint_advanced else 0,
            _json_dumps(metrics or {}, {}),
            error_text,
            sync_run_id,
        ),
    )


def get_sync_cursor(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    workspace_id: str,
    cursor_name: str,
) -> str | None:
    row = conn.execute(
        """
        SELECT cursor_value FROM sync_cursors
        WHERE account_key=? AND workspace_id=? AND cursor_name=?
        """,
        (account_key, workspace_id, cursor_name),
    ).fetchone()
    if row is None:
        return None
    value = row[0]
    return str(value) if value is not None else None


def advance_sync_cursor(
    conn: sqlite3.Connection,
    *,
    account_key: str,
    workspace_id: str,
    cursor_name: str,
    cursor_value: str | None,
    sync_run_id: str | None,
    allow_advance: bool,
    expected_cursor_value: str | None = None,
    enforce_expected_cursor: bool = False,
) -> bool:
    """Advance durable cursor only when allow_advance is True (successful persist)."""
    if not allow_advance:
        conn.execute(
            """
            INSERT INTO reconciliation_events(
              account_key, workspace_id, sync_run_id, event_type, table_name, record_id, details_json
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                account_key,
                workspace_id,
                sync_run_id,
                "checkpoint_not_advanced",
                None,
                None,
                _json_dumps(
                    {
                        "cursor_name": cursor_name,
                        "attempted_cursor_value": cursor_value,
                        "reason": "partial_or_failed_sync",
                    },
                    {},
                ),
            ),
        )
        return False
    if enforce_expected_cursor:
        advanced = conn.execute(
            """
            INSERT INTO sync_cursors(
              account_key, workspace_id, cursor_name, cursor_value, sync_run_id, updated_at
            )
            SELECT ?,?,?,?,?,?
            WHERE ? IS NULL OR EXISTS (
              SELECT 1 FROM sync_cursors
              WHERE account_key=? AND workspace_id=? AND cursor_name=?
                AND cursor_value IS ?
            )
            ON CONFLICT(account_key, workspace_id, cursor_name) DO UPDATE SET
              cursor_value=excluded.cursor_value,
              sync_run_id=excluded.sync_run_id,
              updated_at=excluded.updated_at
            WHERE sync_cursors.cursor_value IS ?
            """,
            (
                account_key,
                workspace_id,
                cursor_name,
                cursor_value,
                sync_run_id,
                int(time.time()),
                expected_cursor_value,
                account_key,
                workspace_id,
                cursor_name,
                expected_cursor_value,
                expected_cursor_value,
            ),
        )
        if advanced.rowcount == 1:
            return True
        conn.execute(
            """
            INSERT INTO reconciliation_events(
              account_key, workspace_id, sync_run_id, event_type, table_name, record_id, details_json
            ) VALUES (?,?,?,?,?,?,?)
            """,
            (
                account_key,
                workspace_id,
                sync_run_id,
                "checkpoint_stale_writer_rejected",
                None,
                None,
                _json_dumps(
                    {
                        "cursor_name": cursor_name,
                        "expected_cursor_value": expected_cursor_value,
                        "attempted_cursor_value": cursor_value,
                    },
                    {},
                ),
            ),
        )
        return False
    conn.execute(
        """
        INSERT INTO sync_cursors(account_key, workspace_id, cursor_name, cursor_value, sync_run_id, updated_at)
        VALUES (?,?,?,?,?,?)
        ON CONFLICT(account_key, workspace_id, cursor_name) DO UPDATE SET
          cursor_value=excluded.cursor_value,
          sync_run_id=excluded.sync_run_id,
          updated_at=excluded.updated_at
        """,
        (account_key, workspace_id, cursor_name, cursor_value, sync_run_id, int(time.time())),
    )
    return True
