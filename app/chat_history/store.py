"""SQLite-backed chat history storage and query helpers."""

from __future__ import annotations

import contextlib
import json
import os
import sqlite3
from typing import Any

from app.account_scope import AccountScopeError, safe_account_key
from app.chat_history.extractor import (
    describe_thread_record,
    normalize_thread_message_record,
    visible_message_role,
    visible_message_text,
)
from app.chat_history.lossless_archive import (
    advance_sync_cursor,
    begin_sync_run,
    ensure_archive_schema,
    finish_sync_run,
    get_sync_cursor,
    latest_message_version,
    mark_server_omission,
    message_ids_fresh_at_versions,
    persist_raw_record,
    persist_thread_message_record,
)
from app.model_registry import NOTION_MODEL_REVERSE_MAP

LEGACY_ACCOUNT_KEY = "legacy"


DDL = """
CREATE TABLE IF NOT EXISTS chat_history_meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS chat_threads (
  id TEXT PRIMARY KEY,
  title TEXT,
  created_time TEXT,
  last_edited_time TEXT,
  alive INTEGER,
  message_ids_json TEXT NOT NULL DEFAULT '[]',
  raw_json TEXT NOT NULL DEFAULT '{}',
  imported_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
CREATE TABLE IF NOT EXISTS chat_messages (
  id TEXT PRIMARY KEY,
  thread_id TEXT,
  role TEXT,
  text TEXT NOT NULL DEFAULT '',
  created_time TEXT,
  requested_model TEXT,
  notion_requested_model TEXT,
  actual_model TEXT,
  model_provider TEXT,
  raw_json TEXT NOT NULL DEFAULT '{}',
  imported_at INTEGER NOT NULL DEFAULT (strftime('%s','now'))
);
CREATE VIRTUAL TABLE IF NOT EXISTS chat_messages_fts USING fts5(id UNINDEXED, thread_id UNINDEXED, role UNINDEXED, text, tokenize='unicode61');
"""

INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_chat_messages_thread_created
  ON chat_messages(thread_id, created_time, id);
CREATE INDEX IF NOT EXISTS idx_chat_messages_thread_role
  ON chat_messages(thread_id, role);
CREATE INDEX IF NOT EXISTS idx_chat_threads_workspace
  ON chat_threads(workspace_id, id);
CREATE INDEX IF NOT EXISTS idx_chat_messages_workspace
  ON chat_messages(workspace_id, thread_id, id);
"""

MODEL_METADATA_COLUMNS: dict[str, str] = {
    "requested_model": "TEXT",
    "notion_requested_model": "TEXT",
    "actual_model": "TEXT",
    "model_provider": "TEXT",
}

THREAD_OWNERSHIP_COLUMNS: dict[str, str] = {
    "account_key": "TEXT",
    "workspace_id": "TEXT",
    "notion_user_id": "TEXT",
}

MESSAGE_OWNERSHIP_COLUMNS: dict[str, str] = {
    "account_key": "TEXT",
    "workspace_id": "TEXT",
    "notion_user_id": "TEXT",
    "version": "INTEGER",
    "last_version": "INTEGER",
    "step_type": "TEXT",
    "visible": "INTEGER",
}


def _ensure_columns(conn: sqlite3.Connection, table: str, columns: dict[str, str]) -> list[str]:
    existing = {str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    added: list[str] = []
    for name, column_type in columns.items():
        if name in existing:
            continue
        conn.execute(f'ALTER TABLE {table} ADD COLUMN "{name}" {column_type}')
        added.append(name)
    return added


def _ensure_chat_message_columns(conn: sqlite3.Connection) -> list[str]:
    """Add columns introduced after the initial chat-history schema."""
    added = _ensure_columns(conn, "chat_messages", MODEL_METADATA_COLUMNS)
    added.extend(_ensure_columns(conn, "chat_messages", MESSAGE_OWNERSHIP_COLUMNS))
    added.extend(_ensure_columns(conn, "chat_threads", THREAD_OWNERSHIP_COLUMNS))
    return added


def get_chat_history_db_root() -> str:
    explicit = str(os.getenv("CHAT_HISTORY_DB_DIR") or "").strip()
    if explicit:
        return os.path.abspath(explicit)
    base = os.getenv("DB_PATH", "./data/conversations.db")
    return os.path.join(os.path.dirname(os.path.abspath(base)), "chat_history")


def get_account_chat_history_db_path(account_key: str) -> str:
    key = str(account_key or "").strip()
    if not key:
        raise AccountScopeError("account_key is required for an account history shard")
    return os.path.join(get_chat_history_db_root(), f"{safe_account_key(key)}.db")


def get_legacy_chat_history_db_path() -> str:
    """Path of the pre-partition monolithic archive (migration source)."""
    explicit = os.getenv("CHAT_HISTORY_DB_PATH")
    if explicit:
        return os.path.abspath(explicit)
    base = os.getenv("DB_PATH", "./data/conversations.db")
    return os.path.join(os.path.dirname(os.path.abspath(base)), "chat_history.db")


def get_default_chat_history_db_path(account_key: str | None = None) -> str:
    if account_key:
        return get_account_chat_history_db_path(account_key)
    # Unscoped accessor remains the legacy monolith for migration/compat only.
    return get_legacy_chat_history_db_path()


def _quote_fts_phrase(query: str) -> str:
    return '"' + query.replace('"', '""') + '"'


def _preview(text: str | None, limit: int = 180) -> str:
    value = " ".join(str(text or "").split())
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _json_object(value: str | None) -> dict[str, Any]:
    try:
        decoded = json.loads(value or "{}")
    except json.JSONDecodeError:
        return {}
    return decoded if isinstance(decoded, dict) else {}


def _is_scalar(value: Any) -> bool:
    return value is None or isinstance(value, (str, int, float, bool))


def _id_text(value: Any) -> str | None:
    if isinstance(value, bool) or value is None or isinstance(value, (dict, list, tuple, set)):
        return None
    if isinstance(value, (str, int, float)):
        text = str(value).strip()
        return text or None
    return None


def _text(value: Any) -> str:
    if value is None:
        return ""
    if _is_scalar(value):
        return str(value)
    try:
        return json.dumps(value, ensure_ascii=False, default=str)
    except TypeError:
        return str(value)


def _json_dumps(value: Any, fallback: Any) -> str:
    try:
        return json.dumps(value if value is not None else fallback, ensure_ascii=False, default=str)
    except TypeError:
        return json.dumps(fallback, ensure_ascii=False, default=str)


def _message_ids_json(value: Any) -> str:
    if not isinstance(value, list):
        return "[]"
    ids = [_id_text(item) for item in value]
    return json.dumps([item for item in ids if item], ensure_ascii=False)


def _clean_ids(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        item = _id_text(value)
        if not item or item in seen:
            continue
        seen.add(item)
        out.append(item)
    return out


def _thread_timestamp_expr(alias: str = "t") -> str:
    return (
        f"COALESCE("
        f"NULLIF({alias}.last_edited_time,''),"
        f"NULLIF({alias}.created_time,''),"
        f"CAST(json_extract({alias}.raw_json, '$.updated_at') AS TEXT),"
        f"CAST(json_extract({alias}.raw_json, '$.updated_time') AS TEXT),"
        f"CAST(json_extract({alias}.raw_json, '$.last_edited_time') AS TEXT),"
        f"CAST(json_extract({alias}.raw_json, '$.created_at') AS TEXT),"
        f"CAST(json_extract({alias}.raw_json, '$.created_time') AS TEXT),"
        f"{alias}.imported_at"
        f")"
    )


def _time_sort_value(value: Any) -> tuple[int, str]:
    text = str(value or "").strip()
    if not text:
        return (0, "")
    if text.isdigit():
        return (int(text), text)
    try:
        from datetime import datetime

        normalized = text.replace("Z", "+00:00")
        return (int(datetime.fromisoformat(normalized).timestamp() * 1000), text)
    except (TypeError, ValueError, OverflowError):
        return (0, text)


def _simple_model_display_name(actual_model: Any) -> str:
    model = str(actual_model or "").strip()
    if not model:
        return "[unknown]"
    standard = NOTION_MODEL_REVERSE_MAP.get(model, model)
    simple_names = {
        "claude-sonnet4.6": "Sonnet 4.6",
        "claude-sonnet5": "Sonnet 5",
        "claude-opus4.6": "Opus 4.6",
        "claude-opus4.7": "Opus 4.7",
        "claude-opus4.8": "Opus 4.8",
        "claude-haiku4.5": "Haiku 4.5",
        "claude-fable5": "Fable 5",
        "gpt-5.2": "GPT 5.2",
        "gpt-5.4": "GPT 5.4",
        "gpt-5.4mini": "GPT 5.4 Mini",
        "gpt-5.4nano": "GPT 5.4 Nano",
        "gpt-5.5": "GPT 5.5",
        "gemini-3flash": "Gemini 3 Flash",
        "gemini-3.1pro": "Gemini 3.1 Pro",
        "gemini-3.5flash": "Gemini 3.5 Flash",
        "gemini-2.5flash": "Gemini 2.5 Flash",
        "grok-4.3": "Grok 4.3",
        "grok-4.5": "Grok 4.5",
        "grok-build0.1": "Grok Build 0.1",
        "minimax-m2.5": "MiniMax M2.5",
        "kimi-2.6": "Kimi 2.6",
        "deepseek-v4pro": "DeepSeek V4 Pro",
    }
    return simple_names.get(standard, standard)



def _message_model_metadata(message: dict[str, Any]) -> dict[str, str]:
    raw = message.get("raw") if isinstance(message.get("raw"), dict) else {}
    step = raw.get("step") if isinstance(raw.get("step"), dict) else raw
    data = raw.get("data") if isinstance(raw.get("data"), dict) else {}
    value_parts = step.get("value") if isinstance(step.get("value"), list) else []
    top_metadata = message.get("model_metadata") if isinstance(message.get("model_metadata"), dict) else {}

    def first_text(*values: Any) -> str:
        for value in values:
            if isinstance(value, str) and value.strip():
                return value.strip()
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                return str(value)
        return ""

    notion_model_name = first_text(
        message.get("notion_model_name"),
        top_metadata.get("notion_model_name"),
        step.get("notionModelName"),
        raw.get("notionModelName"),
    )
    notion_step_model = first_text(
        top_metadata.get("notion_step_model"),
        step.get("model"),
        raw.get("model"),
    )
    model_provider = first_text(
        message.get("model_provider"),
        top_metadata.get("model_provider"),
        step.get("modelProvider"),
        raw.get("modelProvider"),
    )
    requested_model = first_text(
        message.get("requested_model"),
        top_metadata.get("requested_model"),
        raw.get("requested_model"),
        data.get("requested_model"),
    )
    notion_requested_model = first_text(
        message.get("notion_requested_model"),
        top_metadata.get("notion_requested_model"),
        raw.get("notion_requested_model"),
        data.get("notion_requested_model"),
    )

    for part in value_parts:
        if not isinstance(part, dict):
            continue
        notion_model_name = notion_model_name or first_text(part.get("notionModelName"))
        model_provider = model_provider or first_text(part.get("modelProvider"))

    actual_model = first_text(
        message.get("actual_model"),
        top_metadata.get("actual_model"),
        raw.get("actual_model"),
        data.get("actual_model"),
        notion_model_name,
    )
    metadata = {
        "requested_model": requested_model,
        "notion_requested_model": notion_requested_model,
        "actual_model": actual_model,
        "notion_model_name": notion_model_name,
        "notion_step_model": notion_step_model,
        "model_provider": model_provider,
    }
    if actual_model:
        metadata["actual_model_verified"] = "false"
        metadata["actual_model_unverified_reason"] = (
            "Imported Notion model metadata is an observation and is not independently verified."
        )
    elif notion_step_model:
        metadata["actual_model_verified"] = "false"
        metadata["actual_model_unverified_reason"] = (
            "Only step.model was observed; it is not authoritative responder identity."
        )
    return metadata

def _display_message(message: dict[str, Any]) -> dict[str, Any] | None:
    raw = message.get("raw") if isinstance(message.get("raw"), dict) else {}
    text = visible_message_text(raw) if raw else str(message.get("text") or "").strip()
    if not text:
        return None
    role = visible_message_role(raw) if raw else None
    if not role:
        role = str(message.get("role") or "").strip()
    if not role:
        return None
    if role not in {"user", "assistant"}:
        if role == "text":
            role = "assistant"
        else:
            return None
    created_time = str(message.get("created_time") or "").strip()
    if not created_time and raw:
        created_time = _text(raw.get("createdAt") or raw.get("created_time") or raw.get("startedAt"))
    out = {key: value for key, value in dict(message).items() if key != "raw"}
    metadata = message.get("model_metadata") if isinstance(message.get("model_metadata"), dict) else {}
    if not metadata:
        metadata = _message_model_metadata(message)
    out["role"] = role
    out["text"] = text
    out["created_time"] = created_time
    if metadata:
        out["model_metadata"] = metadata
        out["actual_model"] = metadata.get("actual_model")
        out["display_model"] = _simple_model_display_name(metadata.get("actual_model"))
        out["model_provider"] = metadata.get("model_provider")
        out["notion_model_name"] = metadata.get("notion_model_name")
    return out


def _visible_messages(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    visible: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for message in messages:
        display = _display_message(message)
        if not display:
            continue
        dedupe_key = (str(display.get("role") or ""), " ".join(str(display.get("text") or "").split()))
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        visible.append(display)
    return sorted(visible, key=lambda item: (_time_sort_value(item.get("created_time")), str(item.get("id") or "")))


def _process_steps(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []
    seen: set[str] = set()
    inference_step_ids: set[str] = set()
    for message in messages:
        raw_any = message.get("raw")
        raw = raw_any if isinstance(raw_any, dict) else {}
        if raw.get("type") == "agent-inference":
            inference_step_ids.add(str(raw.get("id")))

    for message in messages:
        raw_any = message.get("raw")
        raw = raw_any if isinstance(raw_any, dict) else {}
        raw_type = str(raw.get("type") or "").strip()
        label = ""
        detail = ""
        if raw_type == "agent-tool-result":
            if str(raw.get("agentStepId") or "") in inference_step_ids:
                continue
            label = str(raw.get("toolName") or raw.get("toolType") or "Tool result").strip()
            detail = str(raw.get("error") or raw.get("state") or "").strip()
        elif raw_type == "agent-inference":
            value_parts = raw.get("value")
            if not isinstance(value_parts, list):
                value_parts = []
            for part in value_parts:
                if not isinstance(part, dict):
                    continue
                if part.get("type") == "tool_use":
                    label = str(part.get("name") or "Tool use").strip()
                    break
        elif raw_type == "agent-search-query-generation":
            label = "Search query generation"
        if not label:
            continue
        key = f"{raw_type}:{label}:{detail}:{raw.get('id') or message.get('id')}"
        if key in seen:
            continue
        seen.add(key)
        created_time = str(message.get("created_time") or "").strip()
        if not created_time:
            created_time = _text(raw.get("createdAt") or raw.get("created_time") or raw.get("startedAt"))
        steps.append(
            {
                "id": str(raw.get("id") or message.get("id") or ""),
                "type": raw_type,
                "label": label,
                "detail": detail,
                "created_time": created_time,
            }
        )
    return sorted(steps, key=lambda item: (_time_sort_value(item.get("created_time")), str(item.get("id") or "")))


class ChatHistoryStore:
    def __init__(
        self,
        db_path: str | None = None,
        *,
        account_key: str | None = None,
    ):
        self.account_key = str(account_key or "").strip()
        self.db_path = db_path or get_default_chat_history_db_path(self.account_key or None)
        os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        with self._conn() as conn:
            conn.executescript(DDL)
            _ensure_chat_message_columns(conn)
            ensure_archive_schema(conn)
            self._bind_account_metadata(conn)
            conn.commit()
        self._ensure_indexes()

    def _bind_account_metadata(self, conn: sqlite3.Connection) -> None:
        if not self.account_key:
            return
        row = conn.execute(
            "SELECT value FROM chat_history_meta WHERE key='account_key'"
        ).fetchone()
        if row and str(row[0]) != self.account_key:
            raise AccountScopeError(
                "Chat-history shard is already bound to a different account_key"
            )
        conn.execute(
            "INSERT OR IGNORE INTO chat_history_meta(key,value) VALUES('account_key',?)",
            (self.account_key,),
        )

    def _ownership(
        self,
        *,
        workspace_id: str | None = None,
        notion_user_id: str | None = None,
        account_key: str | None = None,
    ) -> tuple[str, str, str | None]:
        resolved_account = str(account_key or self.account_key or LEGACY_ACCOUNT_KEY).strip() or LEGACY_ACCOUNT_KEY
        resolved_workspace = str(workspace_id or "").strip() or "unknown"
        resolved_user = str(notion_user_id or "").strip() or None
        return resolved_account, resolved_workspace, resolved_user

    def _ensure_indexes(self) -> None:
        """Best-effort indexes; another process may hold the live archive DB."""
        try:
            conn = sqlite3.connect(self.db_path, timeout=30)
            try:
                conn.executescript(INDEX_DDL)
                ensure_archive_schema(conn)
                conn.commit()
            finally:
                conn.close()
        except sqlite3.OperationalError:
            return

    @contextlib.contextmanager
    def _conn(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            with conn:
                yield conn
        finally:
            conn.close()

    def upsert_bundle(
        self,
        bundle: dict[str, Any],
        *,
        workspace_id: str | None = None,
        notion_user_id: str | None = None,
        account_key: str | None = None,
        source_kind: str | None = None,
        source_endpoint: str | None = None,
    ) -> dict[str, int]:
        threads = bundle.get("threads", {})
        messages = bundle.get("messages", {})
        thread_messages = bundle.get("thread_messages", {}) if isinstance(bundle.get("thread_messages"), dict) else {}
        raw_records = bundle.get("raw_records", []) if isinstance(bundle.get("raw_records"), list) else []
        account, workspace, user = self._ownership(
            workspace_id=workspace_id,
            notion_user_id=notion_user_id,
            account_key=account_key,
        )
        # Prefer explicit ownership from the bundle when present.
        if isinstance(bundle.get("workspace_id"), str) and bundle.get("workspace_id").strip():
            workspace = bundle["workspace_id"].strip()
        if isinstance(bundle.get("notion_user_id"), str) and bundle.get("notion_user_id").strip():
            user = bundle["notion_user_id"].strip()
        result = {
            "threads": len(threads) if isinstance(threads, dict) else 0,
            "messages": len(messages) if isinstance(messages, dict) else 0,
            "threads_inserted": 0,
            "threads_updated": 0,
            "messages_inserted": 0,
            "messages_updated": 0,
            "threads_skipped": 0,
            "messages_skipped": 0,
            "raw_records_inserted": 0,
            "semantic_messages_inserted": 0,
            "semantic_messages_updated": 0,
            "parts_written": 0,
        }
        with self._conn() as conn:
            ensure_archive_schema(conn)
            if isinstance(threads, dict):
                for t in threads.values():
                    if not isinstance(t, dict):
                        result["threads_skipped"] += 1
                        continue
                    thread_id = _id_text(t.get("id"))
                    if not thread_id:
                        result["threads_skipped"] += 1
                        continue
                    thread_workspace = _text(t.get("workspace_id") or workspace)
                    proposed = (
                        _text(t.get("title")),
                        _text(t.get("created_time")),
                        _text(t.get("last_edited_time") or t.get("updated_at")),
                        None if t.get("alive") is None else int(bool(t.get("alive"))),
                        _message_ids_json(t.get("message_ids")),
                        _json_dumps(t.get("raw"), {}),
                        account,
                        thread_workspace,
                        user,
                    )
                    existing = conn.execute(
                        "SELECT title,created_time,last_edited_time,alive,message_ids_json,raw_json,account_key,workspace_id,notion_user_id FROM chat_threads WHERE id=?",
                        (thread_id,),
                    ).fetchone()
                    conn.execute(
                        """INSERT INTO chat_threads(id,title,created_time,last_edited_time,alive,message_ids_json,raw_json,account_key,workspace_id,notion_user_id)
                        VALUES(?,?,?,?,?,?,?,?,?,?)
                        ON CONFLICT(id) DO UPDATE SET title=excluded.title,last_edited_time=excluded.last_edited_time,alive=excluded.alive,message_ids_json=excluded.message_ids_json,raw_json=excluded.raw_json,account_key=COALESCE(excluded.account_key, chat_threads.account_key),workspace_id=COALESCE(excluded.workspace_id, chat_threads.workspace_id),notion_user_id=COALESCE(excluded.notion_user_id, chat_threads.notion_user_id)""",
                        (thread_id, *proposed),
                    )
                    if existing is None:
                        result["threads_inserted"] += 1
                    elif tuple(existing) != proposed:
                        result["threads_updated"] += 1
            if isinstance(messages, dict):
                for m in messages.values():
                    if not isinstance(m, dict):
                        result["messages_skipped"] += 1
                        continue
                    message_id = _id_text(m.get("id"))
                    if not message_id:
                        result["messages_skipped"] += 1
                        continue
                    thread_id = _id_text(m.get("thread_id"))
                    role = _text(m.get("role"))
                    text = _text(m.get("text"))
                    created_time = _text(m.get("created_time"))
                    metadata = _message_model_metadata(m)
                    message_workspace = _text(m.get("workspace_id") or workspace)
                    version = m.get("version")
                    last_version = m.get("last_version")
                    step_type = _text(m.get("step_type")) or None
                    visible_flag = 1 if m.get("visible", True) else 0
                    proposed = (
                        thread_id,
                        role,
                        text,
                        created_time,
                        _text(metadata.get("requested_model")),
                        _text(metadata.get("notion_requested_model")),
                        _text(metadata.get("actual_model")),
                        _text(metadata.get("model_provider")),
                        _json_dumps(m.get("raw"), {}),
                        account,
                        message_workspace,
                        user,
                        version,
                        last_version,
                        step_type,
                        visible_flag,
                    )
                    existing = conn.execute(
                        "SELECT thread_id,role,text,created_time,requested_model,notion_requested_model,actual_model,model_provider,raw_json,account_key,workspace_id,notion_user_id,version,last_version,step_type,visible FROM chat_messages WHERE id=?",
                        (message_id,),
                    ).fetchone()
                    changed = existing is None or tuple(existing) != proposed
                    if changed:
                        conn.execute(
                            """INSERT INTO chat_messages(id,thread_id,role,text,created_time,requested_model,notion_requested_model,actual_model,model_provider,raw_json,account_key,workspace_id,notion_user_id,version,last_version,step_type,visible)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                            ON CONFLICT(id) DO UPDATE SET thread_id=excluded.thread_id,role=excluded.role,text=excluded.text,created_time=excluded.created_time,requested_model=excluded.requested_model,notion_requested_model=excluded.notion_requested_model,actual_model=excluded.actual_model,model_provider=excluded.model_provider,raw_json=excluded.raw_json,account_key=COALESCE(excluded.account_key, chat_messages.account_key),workspace_id=COALESCE(excluded.workspace_id, chat_messages.workspace_id),notion_user_id=COALESCE(excluded.notion_user_id, chat_messages.notion_user_id),version=excluded.version,last_version=excluded.last_version,step_type=excluded.step_type,visible=excluded.visible""",
                            (message_id, *proposed),
                        )
                        conn.execute("DELETE FROM chat_messages_fts WHERE id=?", (message_id,))
                        conn.execute(
                            "INSERT INTO chat_messages_fts(id,thread_id,role,text) VALUES(?,?,?,?)",
                            (message_id, thread_id, role, text),
                        )
                    if existing is None:
                        result["messages_inserted"] += 1
                    elif changed:
                        result["messages_updated"] += 1

            # Promote visible-only messages into semantic records when callers omit thread_messages.
            semantic_records = dict(thread_messages)
            if isinstance(messages, dict):
                for message_id, message in messages.items():
                    if message_id in semantic_records or not isinstance(message, dict):
                        continue
                    raw = message.get("raw") if isinstance(message.get("raw"), dict) else message
                    record = normalize_thread_message_record(str(message_id), raw, fallback_thread_id=_id_text(message.get("thread_id")))
                    if record:
                        semantic_records[record["id"]] = record

            for raw_record in raw_records:
                if not isinstance(raw_record, dict):
                    continue
                record_id = _id_text(raw_record.get("record_id"))
                table_name = str(raw_record.get("table_name") or "unknown").strip() or "unknown"
                if not record_id:
                    continue
                row_workspace = str(raw_record.get("workspace_id") or workspace).strip() or workspace
                inserted = persist_raw_record(
                    conn,
                    account_key=account,
                    workspace_id=row_workspace,
                    notion_user_id=user,
                    table_name=table_name,
                    record_id=record_id,
                    version=raw_record.get("version"),
                    last_version=raw_record.get("last_version"),
                    raw=raw_record.get("raw") or {},
                    source_kind=str(raw_record.get("source_kind") or source_kind or "bundle"),
                    source_endpoint=str(raw_record.get("source_endpoint") or source_endpoint or "") or None,
                )
                if inserted:
                    result["raw_records_inserted"] += 1

            for record in semantic_records.values():
                if not isinstance(record, dict):
                    continue
                row_workspace = str(record.get("workspace_id") or workspace).strip() or workspace
                # Always mirror semantic raw into versioned raw_notion_records.
                persist_raw_record(
                    conn,
                    account_key=account,
                    workspace_id=row_workspace,
                    notion_user_id=user,
                    table_name="thread_message",
                    record_id=str(record.get("id") or ""),
                    version=record.get("version"),
                    last_version=record.get("last_version"),
                    raw=record.get("raw_wrapper") or record.get("raw") or {},
                    source_kind=source_kind or "bundle",
                    source_endpoint=source_endpoint,
                )
                semantic_result = persist_thread_message_record(
                    conn,
                    account_key=account,
                    workspace_id=row_workspace,
                    notion_user_id=user,
                    record=record,
                )
                result["semantic_messages_inserted"] += semantic_result["messages_inserted"]
                result["semantic_messages_updated"] += semantic_result["messages_updated"]
                result["parts_written"] += semantic_result["parts_written"]
            conn.commit()
        return result

    def record_live_turn(self, bundle: dict[str, Any]) -> dict[str, int]:
        """Upsert a live chat turn without clobbering hydrated Notion raw/message ids."""
        threads = bundle.get("threads", {}) if isinstance(bundle, dict) else {}
        messages = bundle.get("messages", {}) if isinstance(bundle, dict) else {}
        result = {
            "threads": len(threads) if isinstance(threads, dict) else 0,
            "messages": len(messages) if isinstance(messages, dict) else 0,
            "threads_inserted": 0,
            "threads_updated": 0,
            "messages_inserted": 0,
            "messages_updated": 0,
            "threads_skipped": 0,
            "messages_skipped": 0,
        }
        if not isinstance(threads, dict) or not isinstance(messages, dict):
            return result

        with self._conn() as conn:
            for thread in threads.values():
                if not isinstance(thread, dict):
                    result["threads_skipped"] += 1
                    continue
                thread_id = _id_text(thread.get("id"))
                if not thread_id:
                    result["threads_skipped"] += 1
                    continue
                existing = conn.execute(
                    "SELECT title,created_time,last_edited_time,alive,message_ids_json,raw_json FROM chat_threads WHERE id=?",
                    (thread_id,),
                ).fetchone()
                existing_ids = json.loads((existing["message_ids_json"] if existing else None) or "[]")
                if not isinstance(existing_ids, list):
                    existing_ids = []
                incoming_ids = thread.get("message_ids") if isinstance(thread.get("message_ids"), list) else []
                merged_ids = _clean_ids([*existing_ids, *incoming_ids])
                existing_raw = _json_object(existing["raw_json"] if existing else None)
                incoming_raw = thread.get("raw") if isinstance(thread.get("raw"), dict) else {}
                merged_raw = dict(existing_raw)
                if incoming_raw:
                    live = incoming_raw.get("live") if isinstance(incoming_raw.get("live"), dict) else None
                    if live:
                        sources = merged_raw.get("live_sources")
                        if not isinstance(sources, list):
                            sources = []
                        sources.append(live)
                        merged_raw["live_sources"] = sources[-20:]
                        merged_raw["live"] = live
                    if not merged_raw.get("title") and incoming_raw.get("title"):
                        merged_raw["title"] = incoming_raw.get("title")
                    if not merged_raw.get("type"):
                        merged_raw["type"] = incoming_raw.get("type") or "live_chat"
                title = _text(thread.get("title")) or _text(existing["title"] if existing else "")
                created_time = _text(existing["created_time"] if existing else "") or _text(thread.get("created_time"))
                last_edited = _text(thread.get("last_edited_time") or thread.get("updated_at")) or _text(
                    existing["last_edited_time"] if existing else ""
                )
                alive = existing["alive"] if existing and existing["alive"] is not None else 1
                if thread.get("alive") is not None:
                    alive = int(bool(thread.get("alive")))
                proposed = (
                    title,
                    created_time,
                    last_edited,
                    alive,
                    _message_ids_json(merged_ids),
                    _json_dumps(merged_raw, {}),
                )
                conn.execute(
                    """INSERT INTO chat_threads(id,title,created_time,last_edited_time,alive,message_ids_json,raw_json)
                    VALUES(?,?,?,?,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET
                      title=CASE WHEN TRIM(COALESCE(excluded.title,''))='' THEN chat_threads.title ELSE excluded.title END,
                      last_edited_time=excluded.last_edited_time,
                      alive=excluded.alive,
                      message_ids_json=excluded.message_ids_json,
                      raw_json=excluded.raw_json""",
                    (thread_id, *proposed),
                )
                if existing is None:
                    result["threads_inserted"] += 1
                else:
                    result["threads_updated"] += 1

            for message in messages.values():
                if not isinstance(message, dict):
                    result["messages_skipped"] += 1
                    continue
                message_id = _id_text(message.get("id"))
                if not message_id:
                    result["messages_skipped"] += 1
                    continue
                thread_id = _id_text(message.get("thread_id"))
                role = _text(message.get("role"))
                text = _text(message.get("text"))
                created_time = _text(message.get("created_time"))
                metadata = _message_model_metadata(message)
                existing = conn.execute(
                    "SELECT thread_id,role,text,created_time,requested_model,notion_requested_model,actual_model,model_provider,raw_json FROM chat_messages WHERE id=?",
                    (message_id,),
                ).fetchone()
                if existing is not None and str(existing["text"] or "").strip():
                    # Keep hydrated Notion bodies; only fill missing model columns.
                    requested = _text(existing["requested_model"]) or _text(metadata.get("requested_model"))
                    notion_requested = _text(existing["notion_requested_model"]) or _text(
                        metadata.get("notion_requested_model")
                    )
                    actual = _text(existing["actual_model"]) or _text(metadata.get("actual_model"))
                    provider = _text(existing["model_provider"]) or _text(metadata.get("model_provider"))
                    existing_raw = _json_object(existing["raw_json"])
                    incoming_raw = message.get("raw") if isinstance(message.get("raw"), dict) else {}
                    if incoming_raw.get("live") and "live" not in existing_raw:
                        existing_raw["live"] = incoming_raw.get("live")
                    proposed = (
                        _text(existing["thread_id"]) or thread_id,
                        _text(existing["role"]) or role,
                        _text(existing["text"]),
                        _text(existing["created_time"]) or created_time,
                        requested,
                        notion_requested,
                        actual,
                        provider,
                        _json_dumps(existing_raw, {}),
                    )
                    changed = tuple(existing) != proposed
                    if changed:
                        conn.execute(
                            """UPDATE chat_messages
                            SET thread_id=?, role=?, text=?, created_time=?,
                                requested_model=?, notion_requested_model=?, actual_model=?, model_provider=?, raw_json=?
                            WHERE id=?""",
                            (*proposed, message_id),
                        )
                        result["messages_updated"] += 1
                    else:
                        result["messages_skipped"] += 1
                    continue

                proposed = (
                    thread_id,
                    role,
                    text,
                    created_time,
                    _text(metadata.get("requested_model")),
                    _text(metadata.get("notion_requested_model")),
                    _text(metadata.get("actual_model")),
                    _text(metadata.get("model_provider")),
                    _json_dumps(message.get("raw"), {}),
                )
                conn.execute(
                    """INSERT INTO chat_messages(id,thread_id,role,text,created_time,requested_model,notion_requested_model,actual_model,model_provider,raw_json)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(id) DO UPDATE SET thread_id=excluded.thread_id,role=excluded.role,text=excluded.text,created_time=excluded.created_time,requested_model=excluded.requested_model,notion_requested_model=excluded.notion_requested_model,actual_model=excluded.actual_model,model_provider=excluded.model_provider,raw_json=excluded.raw_json""",
                    (message_id, *proposed),
                )
                conn.execute("DELETE FROM chat_messages_fts WHERE id=?", (message_id,))
                conn.execute(
                    "INSERT INTO chat_messages_fts(id,thread_id,role,text) VALUES(?,?,?,?)",
                    (message_id, thread_id, role, text),
                )
                if existing is None:
                    result["messages_inserted"] += 1
                else:
                    result["messages_updated"] += 1
            conn.commit()
        return result

    def existing_message_ids(self, message_ids: list[Any]) -> set[str]:
        """Return message IDs that already have durable text in the local archive.

        Legacy helper used by visible-transcript hydration. Prefer
        fresh_message_ids()/message_ids_needing_hydration() for version-aware sync.
        """
        ids = _clean_ids(message_ids)
        if not ids:
            return set()
        existing: set[str] = set()
        with self._conn() as conn:
            for start in range(0, len(ids), 400):
                chunk = ids[start : start + 400]
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    SELECT id FROM chat_messages
                    WHERE id IN ({placeholders})
                      AND TRIM(COALESCE(text, '')) != ''
                    """,
                    chunk,
                ).fetchall()
                existing.update(str(row["id"]) for row in rows)
        return existing

    def fresh_message_ids(
        self,
        candidates: dict[str, Any] | list[Any],
        *,
        workspace_id: str | None = None,
        account_key: str | None = None,
    ) -> set[str]:
        """Return IDs that are present and not stale relative to candidate versions.

        ``candidates`` may be a list of IDs (presence-only) or a mapping of
        message_id -> server version.
        """
        if isinstance(candidates, dict):
            versioned = {
                str(message_id).strip(): candidates[message_id]
                for message_id in candidates
                if str(message_id or "").strip()
            }
        else:
            versioned = {message_id: None for message_id in _clean_ids(list(candidates))}
        if not versioned:
            return set()
        account, workspace, _user = self._ownership(workspace_id=workspace_id, account_key=account_key)
        with self._conn() as conn:
            ensure_archive_schema(conn)
            fresh = message_ids_fresh_at_versions(
                conn,
                account_key=account,
                workspace_id=workspace,
                candidates=versioned,
            )
            # Fall back to visible archive presence when lossless rows are absent
            # (pre-migration shards) and no authoritative server version was given.
            presence_only_ids = [
                message_id
                for message_id, version in versioned.items()
                if message_id not in fresh and version in (None, -1)
            ]
            if presence_only_ids:
                fresh |= self.existing_message_ids(presence_only_ids)
            return fresh

    def message_ids_needing_hydration(
        self,
        candidates: dict[str, Any] | list[Any],
        *,
        workspace_id: str | None = None,
        account_key: str | None = None,
    ) -> set[str]:
        if isinstance(candidates, dict):
            all_ids = {str(message_id).strip() for message_id in candidates if str(message_id or "").strip()}
        else:
            all_ids = set(_clean_ids(list(candidates)))
        fresh = self.fresh_message_ids(candidates, workspace_id=workspace_id, account_key=account_key)
        return all_ids - fresh

    def archived_message_version(
        self,
        message_id: str,
        *,
        workspace_id: str | None = None,
        account_key: str | None = None,
    ) -> int | None:
        account, workspace, _user = self._ownership(workspace_id=workspace_id, account_key=account_key)
        with self._conn() as conn:
            ensure_archive_schema(conn)
            return latest_message_version(
                conn,
                account_key=account,
                workspace_id=workspace,
                message_id=str(message_id),
            )

    def begin_sync_run(
        self,
        sync_run_id: str,
        *,
        workspace_id: str | None = None,
        notion_user_id: str | None = None,
        account_key: str | None = None,
        source_kind: str = "notion_server",
    ) -> None:
        account, workspace, user = self._ownership(
            workspace_id=workspace_id,
            notion_user_id=notion_user_id,
            account_key=account_key,
        )
        with self._conn() as conn:
            ensure_archive_schema(conn)
            begin_sync_run(
                conn,
                sync_run_id=sync_run_id,
                account_key=account,
                workspace_id=workspace,
                notion_user_id=user,
                source_kind=source_kind,
            )
            conn.commit()

    def finish_sync_run(
        self,
        sync_run_id: str,
        *,
        status: str,
        pages_scanned: int = 0,
        records_persisted: int = 0,
        hydration_failed: int = 0,
        hydration_partial: int = 0,
        checkpoint_advanced: bool = False,
        metrics: dict[str, Any] | None = None,
        error_text: str | None = None,
    ) -> None:
        with self._conn() as conn:
            ensure_archive_schema(conn)
            finish_sync_run(
                conn,
                sync_run_id=sync_run_id,
                status=status,
                pages_scanned=pages_scanned,
                records_persisted=records_persisted,
                hydration_failed=hydration_failed,
                hydration_partial=hydration_partial,
                checkpoint_advanced=checkpoint_advanced,
                metrics=metrics,
                error_text=error_text,
            )
            conn.commit()

    def get_sync_cursor(
        self,
        cursor_name: str,
        *,
        workspace_id: str | None = None,
        account_key: str | None = None,
    ) -> str | None:
        account, workspace, _user = self._ownership(workspace_id=workspace_id, account_key=account_key)
        with self._conn() as conn:
            ensure_archive_schema(conn)
            return get_sync_cursor(
                conn,
                account_key=account,
                workspace_id=workspace,
                cursor_name=cursor_name,
            )

    def advance_sync_cursor(
        self,
        cursor_name: str,
        cursor_value: str | None,
        *,
        sync_run_id: str | None = None,
        workspace_id: str | None = None,
        account_key: str | None = None,
        allow_advance: bool = True,
    ) -> bool:
        account, workspace, _user = self._ownership(workspace_id=workspace_id, account_key=account_key)
        with self._conn() as conn:
            ensure_archive_schema(conn)
            advanced = advance_sync_cursor(
                conn,
                account_key=account,
                workspace_id=workspace,
                cursor_name=cursor_name,
                cursor_value=cursor_value,
                sync_run_id=sync_run_id,
                allow_advance=allow_advance,
            )
            conn.commit()
            return advanced

    def mark_server_omission(
        self,
        *,
        table_name: str,
        record_id: str,
        workspace_id: str | None = None,
        account_key: str | None = None,
        sync_run_id: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        account, workspace, _user = self._ownership(workspace_id=workspace_id, account_key=account_key)
        with self._conn() as conn:
            ensure_archive_schema(conn)
            mark_server_omission(
                conn,
                account_key=account,
                workspace_id=workspace,
                sync_run_id=sync_run_id,
                table_name=table_name,
                record_id=record_id,
                details=details,
            )
            conn.commit()

    def list_thread_messages(
        self,
        thread_id: str,
        *,
        workspace_id: str | None = None,
        account_key: str | None = None,
        visible_only: bool = False,
    ) -> list[dict[str, Any]]:
        """Return latest semantic thread_message rows for a thread (hidden included by default)."""
        account, workspace, _user = self._ownership(workspace_id=workspace_id, account_key=account_key)
        with self._conn() as conn:
            ensure_archive_schema(conn)
            rows = conn.execute(
                """
                SELECT n.*
                FROM notion_thread_messages n
                JOIN (
                  SELECT message_id, MAX(version) AS max_version
                  FROM notion_thread_messages
                  WHERE account_key=? AND workspace_id=? AND thread_id=?
                  GROUP BY message_id
                ) latest
                  ON latest.message_id = n.message_id AND latest.max_version = n.version
                WHERE n.account_key=? AND n.workspace_id=? AND n.thread_id=?
                ORDER BY COALESCE(n.created_time, ''), n.message_id
                """,
                (account, workspace, thread_id, account, workspace, thread_id),
            ).fetchall()
            out: list[dict[str, Any]] = []
            for row in rows:
                item = dict(row)
                if visible_only and not int(item.get("visible") or 0):
                    continue
                out.append(item)
            return out

    def list_message_parts(
        self,
        message_id: str,
        *,
        workspace_id: str | None = None,
        account_key: str | None = None,
        version: int | None = None,
    ) -> list[dict[str, Any]]:
        account, workspace, _user = self._ownership(workspace_id=workspace_id, account_key=account_key)
        with self._conn() as conn:
            ensure_archive_schema(conn)
            resolved_version = version
            if resolved_version is None:
                resolved_version = latest_message_version(
                    conn,
                    account_key=account,
                    workspace_id=workspace,
                    message_id=message_id,
                )
            if resolved_version is None:
                return []
            rows = conn.execute(
                """
                SELECT * FROM notion_message_parts
                WHERE account_key=? AND workspace_id=? AND message_id=? AND message_version=?
                ORDER BY ordinal ASC
                """,
                (account, workspace, message_id, resolved_version),
            ).fetchall()
            return [dict(row) for row in rows]

    def existing_thread_ids(self, thread_ids: list[Any]) -> set[str]:
        ids = _clean_ids(thread_ids)
        if not ids:
            return set()
        existing: set[str] = set()
        with self._conn() as conn:
            for thread_id in ids:
                row = conn.execute("SELECT id FROM chat_threads WHERE id=?", (thread_id,)).fetchone()
                if row:
                    existing.add(str(row["id"]))
        return existing

    def delete_threads(self, thread_ids: list[Any]) -> dict[str, int]:
        ids = _clean_ids(thread_ids)
        result = {
            "requested": len(thread_ids) if isinstance(thread_ids, list) else 0,
            "valid_ids": len(ids),
            "threads_deleted": 0,
            "messages_deleted": 0,
            "fts_deleted": 0,
        }
        with self._conn() as conn:
            for thread_id in ids:
                message_rows = conn.execute(
                    "SELECT id FROM chat_messages WHERE thread_id=?",
                    (thread_id,),
                ).fetchall()
                for row in message_rows:
                    deleted = conn.execute(
                        "DELETE FROM chat_messages_fts WHERE id=?",
                        (row["id"],),
                    ).rowcount
                    if deleted and deleted > 0:
                        result["fts_deleted"] += deleted
                deleted_messages = conn.execute(
                    "DELETE FROM chat_messages WHERE thread_id=?",
                    (thread_id,),
                ).rowcount
                if deleted_messages and deleted_messages > 0:
                    result["messages_deleted"] += deleted_messages
                deleted_threads = conn.execute(
                    "DELETE FROM chat_threads WHERE id=?",
                    (thread_id,),
                ).rowcount
                if deleted_threads and deleted_threads > 0:
                    result["threads_deleted"] += deleted_threads
            conn.commit()
        return result

    def list_threads(self, limit: int = 50, offset: int = 0, include_inactive: bool = False) -> list[dict[str, Any]]:
        filters = [
            """(
              NULLIF(t.title, '') IS NOT NULL
              OR json_extract(t.raw_json, '$.type') IN ('workflow', 'markdown-chat', 'markdownChat')
              OR json_extract(t.raw_json, '$.title') IS NOT NULL
              OR json_extract(t.raw_json, '$.data.title') IS NOT NULL
              OR json_extract(t.raw_json, '$.data.name') IS NOT NULL
            )"""
        ]
        if not include_inactive:
            filters.append("COALESCE(t.alive,1) != 0")
        where = "WHERE " + " AND ".join(filters)
        order_expr = _thread_timestamp_expr("t")
        with self._conn() as conn:
            # Page thread metadata first, then aggregate messages only for that page.
            # Joining/grouping the full messages table before LIMIT was timing out on large DBs.
            page_rows = conn.execute(
                f"""
                SELECT
                  t.id,
                  t.title,
                  t.created_time,
                  t.last_edited_time,
                  {order_expr} AS updated_at,
                  t.alive
                FROM chat_threads t
                {where}
                ORDER BY {order_expr} DESC
                LIMIT ? OFFSET ?
                """,
                (limit, offset),
            ).fetchall()
            if not page_rows:
                return []

            ids = [str(row["id"]) for row in page_rows]
            placeholders = ",".join("?" for _ in ids)
            stats_by_id: dict[str, dict[str, Any]] = {
                thread_id: {
                    "raw_message_count": 0,
                    "visible_message_count": 0,
                    "user_message_count": 0,
                    "assistant_message_count": 0,
                    "error_message_count": 0,
                    "first_message_time": None,
                    "last_message_time": None,
                    "first_message_text": "",
                    "last_message_text": "",
                }
                for thread_id in ids
            }
            for row in conn.execute(
                f"""
                SELECT
                  m.thread_id AS id,
                  COUNT(m.id) AS raw_message_count,
                  SUM(CASE WHEN m.role IN ('user','assistant') THEN 1 ELSE 0 END) AS visible_message_count,
                  SUM(CASE WHEN m.role='user' THEN 1 ELSE 0 END) AS user_message_count,
                  SUM(CASE WHEN m.role='assistant' THEN 1 ELSE 0 END) AS assistant_message_count,
                  SUM(CASE WHEN m.role='error' THEN 1 ELSE 0 END) AS error_message_count,
                  MIN(m.created_time) AS first_message_time,
                  MAX(m.created_time) AS last_message_time
                FROM chat_messages m
                WHERE m.thread_id IN ({placeholders})
                GROUP BY m.thread_id
                """,
                ids,
            ).fetchall():
                stats_by_id[str(row["id"])].update(dict(row))

            for row in conn.execute(
                f"""
                SELECT
                  t.id,
                  (
                    SELECT m1.text FROM chat_messages m1
                    WHERE m1.thread_id=t.id
                    ORDER BY m1.created_time, m1.id LIMIT 1
                  ) AS first_message_text,
                  (
                    SELECT m2.text FROM chat_messages m2
                    WHERE m2.thread_id=t.id
                    ORDER BY m2.created_time DESC, m2.id DESC LIMIT 1
                  ) AS last_message_text
                FROM chat_threads t
                WHERE t.id IN ({placeholders})
                """,
                ids,
            ).fetchall():
                entry = stats_by_id[str(row["id"])]
                entry["first_message_text"] = row["first_message_text"] or ""
                entry["last_message_text"] = row["last_message_text"] or ""

            out: list[dict[str, Any]] = []
            for row in page_rows:
                item = dict(row)
                stats = stats_by_id[str(item["id"])]
                item["raw_message_count"] = int(stats.get("raw_message_count") or 0)
                item["visible_message_count"] = int(stats.get("visible_message_count") or 0)
                item["message_count"] = item["visible_message_count"]
                item["user_message_count"] = int(stats.get("user_message_count") or 0)
                item["assistant_message_count"] = int(stats.get("assistant_message_count") or 0)
                item["error_message_count"] = int(stats.get("error_message_count") or 0)
                item["first_message_time"] = stats.get("first_message_time")
                item["last_message_time"] = stats.get("last_message_time")
                item["export_success_eligible"] = item["raw_message_count"] == 2 and item["user_message_count"] == 1 and item["assistant_message_count"] == 1
                item["export_error_eligible"] = item["raw_message_count"] == 2 and item["user_message_count"] == 1 and item["error_message_count"] == 1
                item["hydrated"] = item["raw_message_count"] > 0
                item["first_message_preview"] = _preview(stats.get("first_message_text", ""))
                item["last_message_preview"] = _preview(stats.get("last_message_text", ""))
                out.append(item)
            self._attach_model_stats_to_threads(conn, out)
            return out

    def _attach_model_stats_to_threads(self, conn: sqlite3.Connection, threads: list[dict[str, Any]]) -> None:
        ids = [str(item.get("id") or "").strip() for item in threads if str(item.get("id") or "").strip()]
        for item in threads:
            item["model_stats"] = []
        if not ids:
            return
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"""
            SELECT
              thread_id,
              COALESCE(NULLIF(actual_model,''), '[unknown]') AS actual_model,
              COALESCE(NULLIF(model_provider,''), '[unknown]') AS model_provider,
              COUNT(1) AS responses
            FROM chat_messages
            WHERE role='assistant' AND thread_id IN ({placeholders})
            GROUP BY thread_id,
                     COALESCE(NULLIF(actual_model,''), '[unknown]'),
                     COALESCE(NULLIF(model_provider,''), '[unknown]')
            ORDER BY responses DESC, actual_model ASC
            """,
            ids,
        ).fetchall()
        by_thread: dict[str, list[dict[str, Any]]] = {thread_id: [] for thread_id in ids}
        for row in rows:
            by_thread.setdefault(str(row["thread_id"]), []).append(
                {
                    "actual_model": str(row["actual_model"]),
                    "display_model": _simple_model_display_name(row["actual_model"]),
                    "model_provider": str(row["model_provider"]),
                    "responses": int(row["responses"] or 0),
                }
            )
        for item in threads:
            item["model_stats"] = by_thread.get(str(item.get("id") or ""), [])

    def single_message_thread_ids(self, thread_ids: list[Any] | None = None) -> list[str]:
        ids = _clean_ids(thread_ids or [])
        params: list[Any] = []
        where = ""
        if ids:
            where = "WHERE t.id IN (" + ",".join("?" for _ in ids) + ")"
            params.extend(ids)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT t.id
                FROM chat_threads t
                JOIN chat_messages m ON m.thread_id=t.id
                {where}
                GROUP BY t.id
                HAVING SUM(CASE WHEN m.role IN ('user','assistant') THEN 1 ELSE 0 END)=1
                ORDER BY COALESCE(MAX(m.created_time), t.last_edited_time, t.created_time) DESC
                """,
                params,
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def errored_thread_ids(self, thread_ids: list[Any] | None = None) -> list[str]:
        """Return threads that contain error messages and no successful assistant response."""
        ids = _clean_ids(thread_ids or [])
        params: list[Any] = []
        where = ""
        if ids:
            where = "WHERE t.id IN (" + ",".join("?" for _ in ids) + ")"
            params.extend(ids)
        with self._conn() as conn:
            rows = conn.execute(
                f"""
                SELECT t.id
                FROM chat_threads t
                JOIN chat_messages m ON m.thread_id=t.id
                {where}
                GROUP BY t.id
                HAVING SUM(CASE WHEN m.role='error' THEN 1 ELSE 0 END) >= 1
                   AND SUM(CASE WHEN m.role='assistant' THEN 1 ELSE 0 END) = 0
                ORDER BY COALESCE(MAX(m.created_time), t.last_edited_time, t.created_time) DESC
                """,
                params,
            ).fetchall()
        return [str(row["id"]) for row in rows]

    def two_message_export_rows(self, thread_ids: list[Any] | None = None, *, include_errors: bool = False) -> dict[str, Any]:
        ids = _clean_ids(thread_ids or [])
        params: list[Any] = []
        where = ""
        if ids:
            where = "WHERE t.id IN (" + ",".join("?" for _ in ids) + ")"
            params.extend(ids)
        response_role_clause = "SUM(CASE WHEN m.role='assistant' THEN 1 ELSE 0 END)=1"
        if include_errors:
            response_role_clause = "(SUM(CASE WHEN m.role='assistant' THEN 1 ELSE 0 END)=1 OR SUM(CASE WHEN m.role='error' THEN 1 ELSE 0 END)=1)"
        with self._conn() as conn:
            candidate_rows = conn.execute(
                f"""
                SELECT t.id
                FROM chat_threads t
                JOIN chat_messages m ON m.thread_id=t.id
                {where}
                GROUP BY t.id
                HAVING COUNT(m.id)=2
                   AND SUM(CASE WHEN m.role='user' THEN 1 ELSE 0 END)=1
                   AND {response_role_clause}
                ORDER BY COALESCE(MAX(m.created_time), t.last_edited_time, t.created_time) DESC
                """,
                params,
            ).fetchall()
            eligible_ids = [str(row["id"]) for row in candidate_rows]
            if not eligible_ids:
                return {"thread_ids": [], "rows": []}
            placeholders = ",".join("?" for _ in eligible_ids)
            msg_rows = conn.execute(
                f"""
                SELECT
                  t.id AS thread_id,
                  t.title,
                  t.created_time AS thread_created_time,
                  t.last_edited_time AS thread_last_edited_time,
                  m.id AS message_id,
                  m.role,
                  m.text,
                  m.created_time AS message_created_time,
                  m.requested_model,
                  m.notion_requested_model,
                  m.actual_model,
                  m.model_provider
                FROM chat_threads t
                JOIN chat_messages m ON m.thread_id=t.id
                WHERE t.id IN ({placeholders})
                ORDER BY t.id, m.created_time, m.id
                """,
                eligible_ids,
            ).fetchall()
        grouped: dict[str, dict[str, Any]] = {}
        for row in msg_rows:
            item = grouped.setdefault(
                str(row["thread_id"]),
                {
                    "thread_id": str(row["thread_id"]),
                    "title": str(row["title"] or ""),
                    "thread_created_time": str(row["thread_created_time"] or ""),
                    "thread_last_edited_time": str(row["thread_last_edited_time"] or ""),
                    "sent_message": "",
                    "sent_message_time": "",
                    "received_message": "",
                    "received_message_time": "",
                    "response_role": "",
                    "response_is_error": False,
                    "actual_model": "",
                    "display_model": "",
                    "model_provider": "",
                    "requested_model": "",
                    "notion_requested_model": "",
                },
            )
            role = str(row["role"] or "")
            if role == "user":
                item["sent_message"] = str(row["text"] or "")
                item["sent_message_time"] = str(row["message_created_time"] or "")
            elif role == "assistant" or (include_errors and role == "error"):
                item["received_message"] = str(row["text"] or "")
                item["received_message_time"] = str(row["message_created_time"] or "")
                item["response_role"] = role
                item["response_is_error"] = role == "error"
                item["actual_model"] = str(row["actual_model"] or "")
                item["display_model"] = _simple_model_display_name(item["actual_model"])
                item["model_provider"] = str(row["model_provider"] or "")
                item["requested_model"] = str(row["requested_model"] or "")
                item["notion_requested_model"] = str(row["notion_requested_model"] or "")
        rows = [grouped[thread_id] for thread_id in eligible_ids if thread_id in grouped and grouped[thread_id].get("sent_message") and grouped[thread_id].get("received_message")]
        return {"thread_ids": [row["thread_id"] for row in rows], "rows": rows}

    def model_response_stats(self) -> dict[str, Any]:
        """Return aggregate model ownership stats for hydrated assistant responses."""
        with self._conn() as conn:
            thread_count = conn.execute("SELECT COUNT(1) FROM chat_threads").fetchone()[0]
            hydrated_thread_count = conn.execute(
                "SELECT COUNT(DISTINCT thread_id) FROM chat_messages WHERE COALESCE(thread_id,'') <> ''"
            ).fetchone()[0]
            assistant_response_count = conn.execute(
                "SELECT COUNT(1) FROM chat_messages WHERE role='assistant'"
            ).fetchone()[0]
            rows = conn.execute(
                """
                SELECT
                  COALESCE(NULLIF(actual_model,''), '[unknown]') AS actual_model,
                  COALESCE(NULLIF(model_provider,''), '[unknown]') AS model_provider,
                  COUNT(1) AS responses,
                  COUNT(DISTINCT thread_id) AS threads
                FROM chat_messages
                WHERE role='assistant'
                GROUP BY COALESCE(NULLIF(actual_model,''), '[unknown]'),
                         COALESCE(NULLIF(model_provider,''), '[unknown]')
                ORDER BY responses DESC, actual_model ASC
                """
            ).fetchall()
        models = []
        for row in rows:
            item = dict(row)
            item["display_model"] = _simple_model_display_name(item.get("actual_model"))
            models.append(item)
        known_response_count = sum(int(item.get("responses") or 0) for item in models if item.get("actual_model") != "[unknown]")
        unknown_response_count = sum(int(item.get("responses") or 0) for item in models if item.get("actual_model") == "[unknown]")
        return {
            "thread_count": int(thread_count or 0),
            "hydrated_thread_count": int(hydrated_thread_count or 0),
            "assistant_response_count": int(assistant_response_count or 0),
            "known_response_count": int(known_response_count),
            "unknown_response_count": int(unknown_response_count),
            "models": models,
        }

    def get_thread_hydration_source(self, thread_id: str) -> dict[str, Any] | None:
        """Lightweight thread metadata for Notion hydrate (no message bodies / raw blobs)."""
        with self._conn() as conn:
            row = conn.execute(
                "SELECT id, title, message_ids_json FROM chat_threads WHERE id=?",
                (thread_id,),
            ).fetchone()
        if not row:
            return None
        return {
            "id": str(row["id"]),
            "title": str(row["title"] or ""),
            "message_ids": json.loads(row["message_ids_json"] or "[]"),
        }

    def get_thread(self, thread_id: str) -> dict[str, Any] | None:
        with self._conn() as conn:
            t = conn.execute("SELECT * FROM chat_threads WHERE id=?", (thread_id,)).fetchone()
            if not t:
                return None
            msgs = conn.execute("SELECT id,thread_id,role,text,created_time,requested_model,notion_requested_model,actual_model,model_provider,raw_json FROM chat_messages WHERE thread_id=? ORDER BY created_time,id", (thread_id,)).fetchall()
        out = dict(t)
        out["message_ids"] = json.loads(out.pop("message_ids_json") or "[]")
        # Keep full raw thread/message records in SQLite, but do not return them from
        # the normal thread endpoint. Notion assistant records can contain very large
        # encryptedContent fields that make the frontend history view fail to fetch.
        out.pop("raw_json", None)
        messages: list[dict[str, Any]] = []
        for row in msgs:
            message = dict(row)
            message["raw"] = _json_object(message.pop("raw_json") or "{}")
            messages.append(message)
        out["messages"] = _visible_messages(messages)
        thread_model_counts: dict[tuple[str, str], dict[str, Any]] = {}
        for message in out["messages"]:
            if str(message.get("role") or "") != "assistant":
                continue
            actual_model = str(message.get("actual_model") or "[unknown]").strip() or "[unknown]"
            model_provider = str(message.get("model_provider") or "[unknown]").strip() or "[unknown]"
            key = (actual_model, model_provider)
            entry = thread_model_counts.setdefault(
                key,
                {"actual_model": actual_model, "display_model": _simple_model_display_name(actual_model), "model_provider": model_provider, "responses": 0},
            )
            entry["responses"] += 1
        out["model_stats"] = sorted(
            thread_model_counts.values(),
            key=lambda item: (-int(item.get("responses") or 0), str(item.get("actual_model") or "")),
        )
        out["steps"] = _process_steps(messages)
        out["message_count"] = len(out["messages"])
        out["hydrated"] = out["message_count"] > 0
        out["first_message_preview"] = _preview(out["messages"][0].get("text") if out["messages"] else "")
        out["last_message_preview"] = _preview(out["messages"][-1].get("text") if out["messages"] else "")
        out["updated_at"] = out.get("last_edited_time") or out.get("created_time") or ""
        return out

    def debug_thread(self, thread_id: str) -> dict[str, Any]:
        thread = self.get_thread(thread_id)
        if not thread:
            return {"thread_exists": False, "message_count": 0, "raw_fields_seen": [], "known_message_fields_found": [], "sample": {}}
        return describe_thread_record(thread, thread.get("messages") or [])

    def search(self, query: str, limit: int = 25) -> list[dict[str, Any]]:
        query = str(query or "").strip()
        if not query:
            return []
        sql = "SELECT id,thread_id,role,snippet(chat_messages_fts,3,'[',']',' ... ',16) AS snippet FROM chat_messages_fts WHERE chat_messages_fts MATCH ? LIMIT ?"
        with self._conn() as conn:
            try:
                rows = conn.execute(sql, (query, limit)).fetchall()
            except sqlite3.OperationalError:
                try:
                    rows = conn.execute(sql, (_quote_fts_phrase(query), limit)).fetchall()
                except sqlite3.OperationalError as exc:
                    raise ValueError("Invalid chat-history search query") from exc
            return [dict(r) for r in rows]

    def thread_to_markdown(self, thread_id: str) -> str | None:
        thread = self.get_thread(thread_id)
        if not thread:
            return None
        lines = [f"# {thread.get('title') or thread.get('id')}", "", f"Thread ID: `{thread.get('id')}`", "", f"Messages: {thread.get('message_count', 0)}", ""]
        if not thread.get("messages"):
            lines += ["> This thread exists in the archive, but no message records are hydrated yet.", ""]
        for msg in thread.get("messages", []):
            lines += [f"## {str(msg.get('role') or 'message').title()}", ""]
            model_bits = []
            if msg.get("actual_model"):
                model_bits.append(f"Actual model: `{msg.get('actual_model')}`")
            if msg.get("model_provider"):
                model_bits.append(f"Provider: `{msg.get('model_provider')}`")
            if msg.get("requested_model"):
                model_bits.append(f"Requested model: `{msg.get('requested_model')}`")
            if msg.get("notion_requested_model"):
                model_bits.append(f"Notion requested model: `{msg.get('notion_requested_model')}`")
            if model_bits:
                lines += model_bits + [""]
            lines += [str(msg.get("text") or ""), ""]
        return "\n".join(lines)
