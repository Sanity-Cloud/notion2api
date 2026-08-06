"""One-time migration from the shared chat_history.db into per-account shards."""

from __future__ import annotations

import json
import os
import shutil
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from app.account_scope import AccountScopeError, canonical_account_key
from app.chat_history.store import (
    LEGACY_ACCOUNT_KEY,
    ChatHistoryStore,
    get_account_chat_history_db_path,
    get_chat_history_db_root,
    get_legacy_chat_history_db_path,
)


@dataclass
class MigrationResult:
    source_db: str
    legacy_db: str
    account_shards: dict[str, str] = field(default_factory=dict)
    attributed_threads: int = 0
    quarantined_threads: int = 0
    attributed_messages: int = 0
    quarantined_messages: int = 0
    skipped_existing: int = 0
    dry_run: bool = False
    notes: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "source_db": self.source_db,
            "legacy_db": self.legacy_db,
            "account_shards": dict(self.account_shards),
            "attributed_threads": self.attributed_threads,
            "quarantined_threads": self.quarantined_threads,
            "attributed_messages": self.attributed_messages,
            "quarantined_messages": self.quarantined_messages,
            "skipped_existing": self.skipped_existing,
            "dry_run": self.dry_run,
            "notes": list(self.notes),
        }


def _json_object(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if not value:
        return {}
    try:
        decoded = json.loads(value)
    except (TypeError, ValueError, json.JSONDecodeError):
        return {}
    return decoded if isinstance(decoded, dict) else {}


def infer_thread_account_key(
    thread_row: Mapping[str, Any],
    *,
    known_thread_accounts: Mapping[str, str] | None = None,
) -> str | None:
    """Best-effort attribution for a legacy thread row."""
    thread_id = str(thread_row["id"] if "id" in thread_row.keys() else thread_row.get("id") or "").strip()
    if known_thread_accounts and thread_id in known_thread_accounts:
        return str(known_thread_accounts[thread_id]).strip() or None

    raw = _json_object(thread_row["raw_json"] if "raw_json" in thread_row.keys() else thread_row.get("raw_json"))
    candidates = [
        raw.get("account_key"),
        (raw.get("live") or {}).get("account_key") if isinstance(raw.get("live"), dict) else None,
        raw.get("spaceId"),
        raw.get("space_id"),
        (raw.get("space") or {}).get("id") if isinstance(raw.get("space"), dict) else None,
    ]
    user_candidates = [
        raw.get("userId"),
        raw.get("user_id"),
        (raw.get("user") or {}).get("id") if isinstance(raw.get("user"), dict) else None,
        (raw.get("live") or {}).get("user_id") if isinstance(raw.get("live"), dict) else None,
    ]
    for account_key in candidates:
        text = str(account_key or "").strip()
        if text and ":" in text and not text.startswith("profile:"):
            try:
                workspace, _, user = text.partition(":")
                return canonical_account_key(workspace, user)
            except AccountScopeError:
                continue
    workspace = ""
    for item in candidates:
        text = str(item or "").strip()
        if text and ":" not in text:
            workspace = text
            break
    user = ""
    for item in user_candidates:
        text = str(item or "").strip()
        if text:
            user = text
            break
    if workspace and user:
        try:
            return canonical_account_key(workspace, user)
        except AccountScopeError:
            return None
    return None


def _copy_thread(
    source: sqlite3.Connection,
    destination: ChatHistoryStore,
    thread_id: str,
) -> tuple[int, int]:
    thread = source.execute(
        "SELECT * FROM chat_threads WHERE id = ?",
        (thread_id,),
    ).fetchone()
    if not thread:
        return 0, 0
    existing = destination.get_thread(thread_id)
    if existing:
        return 0, 0
    messages = source.execute(
        "SELECT * FROM chat_messages WHERE thread_id = ? ORDER BY created_time, id",
        (thread_id,),
    ).fetchall()
    message_map: dict[str, dict[str, Any]] = {}
    for row in messages:
        message_id = str(row["id"])
        raw = _json_object(row["raw_json"])
        message_map[message_id] = {
            "id": message_id,
            "thread_id": thread_id,
            "role": row["role"],
            "text": row["text"],
            "created_time": row["created_time"],
            "requested_model": row["requested_model"] if "requested_model" in row.keys() else "",
            "notion_requested_model": (
                row["notion_requested_model"] if "notion_requested_model" in row.keys() else ""
            ),
            "actual_model": row["actual_model"] if "actual_model" in row.keys() else "",
            "model_provider": row["model_provider"] if "model_provider" in row.keys() else "",
            "raw": raw,
        }
    thread_raw = _json_object(thread["raw_json"])
    bundle = {
        "threads": {
            thread_id: {
                "id": thread_id,
                "title": thread["title"],
                "created_time": thread["created_time"],
                "last_edited_time": thread["last_edited_time"],
                "alive": bool(thread["alive"]),
                "message_ids": json.loads(thread["message_ids_json"] or "[]"),
                "raw": thread_raw,
            }
        },
        "messages": message_map,
    }
    imported = destination.upsert_bundle(bundle)
    return 1, int(imported.get("messages_inserted", 0) + imported.get("messages_updated", 0))


def migrate_shared_chat_history(
    *,
    source_db: str | Path | None = None,
    known_thread_accounts: Mapping[str, str] | None = None,
    dry_run: bool = False,
    keep_source: bool = True,
) -> MigrationResult:
    """
    Attribute threads from the monolithic archive into per-account shards.

    Unattributed threads are quarantined into the legacy shard. The original
    shared database is left in place (renamed with .pre-account-migration)
    unless keep_source is False.
    """
    source_path = Path(source_db or get_legacy_chat_history_db_path()).expanduser().resolve()
    root = Path(get_chat_history_db_root())
    legacy_path = Path(get_account_chat_history_db_path(LEGACY_ACCOUNT_KEY))
    result = MigrationResult(
        source_db=str(source_path),
        legacy_db=str(legacy_path),
        dry_run=dry_run,
    )
    if not source_path.exists():
        result.notes.append("source chat_history.db does not exist; nothing to migrate")
        return result

    # Already migrated marker: shard root exists and source was renamed.
    if source_path.name.endswith(".pre-account-migration"):
        result.notes.append("source looks like a prior migration backup; refusing to re-run")
        return result

    conn = sqlite3.connect(str(source_path))
    conn.row_factory = sqlite3.Row
    try:
        tables = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        if "chat_threads" not in tables:
            result.notes.append("source database has no chat_threads table")
            return result
        threads = conn.execute("SELECT * FROM chat_threads ORDER BY id").fetchall()
        by_account: dict[str, list[str]] = {}
        quarantined: list[str] = []
        for thread in threads:
            account_key = infer_thread_account_key(
                thread, known_thread_accounts=known_thread_accounts
            )
            thread_id = str(thread["id"])
            if account_key:
                by_account.setdefault(account_key, []).append(thread_id)
            else:
                quarantined.append(thread_id)

        if dry_run:
            result.attributed_threads = sum(len(v) for v in by_account.values())
            result.quarantined_threads = len(quarantined)
            result.account_shards = {
                key: get_account_chat_history_db_path(key) for key in sorted(by_account)
            }
            result.notes.append("dry_run: no files were written")
            return result

        root.mkdir(parents=True, exist_ok=True)
        for account_key, thread_ids in sorted(by_account.items()):
            store = ChatHistoryStore(account_key=account_key)
            result.account_shards[account_key] = store.db_path
            for thread_id in thread_ids:
                added_threads, added_messages = _copy_thread(conn, store, thread_id)
                if added_threads:
                    result.attributed_threads += added_threads
                    result.attributed_messages += added_messages
                else:
                    result.skipped_existing += 1

        if quarantined:
            legacy_store = ChatHistoryStore(account_key=LEGACY_ACCOUNT_KEY)
            result.legacy_db = legacy_store.db_path
            for thread_id in quarantined:
                added_threads, added_messages = _copy_thread(conn, legacy_store, thread_id)
                if added_threads:
                    result.quarantined_threads += added_threads
                    result.quarantined_messages += added_messages
                else:
                    result.skipped_existing += 1

        backup = source_path.with_name(source_path.name + ".pre-account-migration")
        if keep_source:
            if not backup.exists():
                shutil.copy2(source_path, backup)
                result.notes.append(f"backed up source to {backup}")
            else:
                result.notes.append(f"backup already exists at {backup}")
        else:
            if backup.exists():
                backup.unlink()
            source_path.replace(backup)
            result.notes.append(f"moved source to {backup}")
            result.source_db = str(backup)
    finally:
        conn.close()
    return result


def resolve_migration_source() -> Path:
    explicit = str(os.getenv("CHAT_HISTORY_DB_PATH") or "").strip()
    if explicit:
        return Path(explicit).expanduser().resolve()
    return Path(get_legacy_chat_history_db_path())
