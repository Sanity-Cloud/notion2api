import json
import sqlite3

import pytest

from app.chat_history.extractor import (
    normalize_thread_message_record,
    visible_transcript_message,
)
from app.chat_history.lossless_archive import (
    ensure_archive_schema,
    mark_server_omission,
    persist_raw_record,
    persist_thread_message_record,
)
from app.chat_history.notion_sync import sync_chat_history_from_notion
from app.chat_history.store import ChatHistoryStore
from app.conversation import ConversationManager


def _semantic_record(
    message_id: str,
    *,
    thread_id: str = "thread-1",
    step_type: str = "agent-inference",
    version: int = 1,
    value=None,
):
    raw = {
        "id": message_id,
        "version": version,
        "parent_id": thread_id,
        "parent_table": "thread",
        "step": {
            "id": message_id,
            "type": step_type,
            "value": value if value is not None else [{"type": "text", "content": "answer"}],
        },
    }
    record = normalize_thread_message_record(message_id, raw, fallback_thread_id=thread_id)
    assert record is not None
    return record


def test_hidden_step_is_retained_but_not_visible():
    record = _semantic_record(
        "tool-1",
        step_type="agent-tool-result",
        value={"tool": "search", "result": "internal"},
    )

    assert record["step_type"] == "agent-tool-result"
    assert record["visible"] is False
    assert visible_transcript_message(record) is None
    assert record["raw_wrapper"]["step"]["value"]["result"] == "internal"


def test_unknown_step_type_is_retained_losslessly():
    record = _semantic_record(
        "future-1",
        step_type="future-notion-step",
        value={"opaque": {"future": True}},
    )

    assert record["step_type"] == "future-notion-step"
    assert record["visible"] is False
    assert record["raw_wrapper"]["step"]["value"] == {"opaque": {"future": True}}


def test_agent_inference_parts_preserve_order_and_non_text_content():
    record = _semantic_record(
        "assistant-1",
        value=[
            {"type": "text", "content": "first"},
            {"type": "citation", "content": {"url": "https://example.invalid", "source": "x"}},
            {"type": "text", "content": "second"},
        ],
    )

    assert record["text"] == "first\n\nsecond"
    assert [part["ordinal"] for part in record["parts"]] == [0, 1, 2]
    assert [part["part_type"] for part in record["parts"]] == ["text", "citation", "text"]
    assert record["parts"][1]["content"] == {
        "url": "https://example.invalid",
        "source": "x",
    }


def test_same_record_ids_can_coexist_across_account_and_workspace_scopes():
    conn = sqlite3.connect(":memory:")
    ensure_archive_schema(conn)
    record = _semantic_record("same-message", version=3)

    for account_key, workspace_id in (("alpha", "ws-a"), ("beta", "ws-b")):
        assert persist_raw_record(
            conn,
            account_key=account_key,
            workspace_id=workspace_id,
            notion_user_id=f"user-{account_key}",
            table_name="thread_message",
            record_id="same-message",
            version=3,
            last_version=2,
            raw=record["raw_wrapper"],
        )
        persisted = persist_thread_message_record(
            conn,
            account_key=account_key,
            workspace_id=workspace_id,
            notion_user_id=f"user-{account_key}",
            record=record,
        )
        assert persisted["messages_inserted"] == 1

    assert conn.execute("SELECT COUNT(*) FROM raw_notion_records").fetchone()[0] == 2
    assert conn.execute("SELECT COUNT(*) FROM notion_thread_messages").fetchone()[0] == 2


def test_duplicate_raw_ingestion_is_idempotent():
    conn = sqlite3.connect(":memory:")
    ensure_archive_schema(conn)
    kwargs = dict(
        account_key="alpha",
        workspace_id="ws",
        notion_user_id="user",
        table_name="thread_message",
        record_id="m1",
        version=1,
        last_version=None,
        raw={"id": "m1", "version": 1},
    )

    assert persist_raw_record(conn, **kwargs) is True
    assert persist_raw_record(conn, **kwargs) is False
    assert conn.execute("SELECT COUNT(*) FROM raw_notion_records").fetchone()[0] == 1


def test_version_aware_freshness_requires_rehydrate_for_newer_server_version(tmp_path):
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    record = _semantic_record("versioned-message", version=1)
    store.upsert_bundle(
        {"threads": {}, "messages": {}, "thread_messages": {record["id"]: record}},
        workspace_id="ws",
        notion_user_id="user",
        account_key="alpha",
    )

    assert store.fresh_message_ids(
        {"versioned-message": 1}, workspace_id="ws", account_key="alpha"
    ) == {"versioned-message"}
    assert store.fresh_message_ids(
        {"versioned-message": 2}, workspace_id="ws", account_key="alpha"
    ) == set()
    assert store.message_ids_needing_hydration(
        {"versioned-message": 2}, workspace_id="ws", account_key="alpha"
    ) == {"versioned-message"}


def test_partial_sync_does_not_advance_existing_checkpoint(tmp_path):
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    assert store.advance_sync_cursor(
        "inference_transcripts",
        "cursor-old",
        workspace_id="ws",
        account_key="alpha",
        allow_advance=True,
    )

    assert not store.advance_sync_cursor(
        "inference_transcripts",
        "cursor-new",
        workspace_id="ws",
        account_key="alpha",
        sync_run_id="run-partial",
        allow_advance=False,
    )
    assert store.get_sync_cursor(
        "inference_transcripts", workspace_id="ws", account_key="alpha"
    ) == "cursor-old"


def test_server_omission_marks_tombstone_without_deleting_archive(tmp_path):
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    record = _semantic_record("omitted-message", version=1)
    store.upsert_bundle(
        {"threads": {}, "messages": {}, "thread_messages": {record["id"]: record}},
        workspace_id="ws",
        notion_user_id="user",
        account_key="alpha",
    )

    store.mark_server_omission(
        table_name="thread_message",
        record_id="omitted-message",
        workspace_id="ws",
        account_key="alpha",
        sync_run_id="run-1",
    )

    rows = store.list_thread_messages("thread-1", workspace_id="ws", account_key="alpha")
    assert len(rows) == 1
    assert rows[0]["message_id"] == "omitted-message"
    assert rows[0]["tombstone_status"] == "server_omitted"


def test_store_additively_upgrades_legacy_schema_without_losing_rows(tmp_path):
    db_path = tmp_path / "legacy.db"
    conn = sqlite3.connect(db_path)
    conn.executescript(
        """
        CREATE TABLE chat_threads (
          id TEXT PRIMARY KEY, title TEXT, created_time TEXT, last_edited_time TEXT,
          alive INTEGER, message_ids_json TEXT NOT NULL DEFAULT '[]',
          raw_json TEXT NOT NULL DEFAULT '{}', imported_at INTEGER
        );
        CREATE TABLE chat_messages (
          id TEXT PRIMARY KEY, thread_id TEXT, role TEXT, text TEXT NOT NULL DEFAULT '',
          created_time TEXT, raw_json TEXT NOT NULL DEFAULT '{}', imported_at INTEGER
        );
        INSERT INTO chat_threads(id,title,message_ids_json,raw_json) VALUES('t1','legacy','["m1"]','{}');
        INSERT INTO chat_messages(id,thread_id,role,text,raw_json) VALUES('m1','t1','assistant','legacy answer','{}');
        """
    )
    conn.commit()
    conn.close()

    ChatHistoryStore(str(db_path), account_key="alpha")

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT title FROM chat_threads WHERE id='t1'").fetchone()[0] == "legacy"
    assert conn.execute("SELECT text FROM chat_messages WHERE id='m1'").fetchone()[0] == "legacy answer"
    message_columns = {row[1] for row in conn.execute("PRAGMA table_info(chat_messages)")}
    assert {"account_key", "workspace_id", "version", "step_type", "visible"} <= message_columns
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"raw_notion_records", "notion_thread_messages", "sync_runs", "reconciliation_events"} <= tables
    conn.close()


def test_conversation_binding_generation_and_ownership(tmp_path):
    manager = ConversationManager(str(tmp_path / "conversations.db"))
    conversation_id = manager.new_conversation(workspace_id="ws-a", user_id="user-a")

    first = manager.create_conversation_binding(
        conversation_id=conversation_id,
        account_key="alpha",
        workspace_id="ws-a",
        remote_thread_id="remote-1",
        bee_id="bee-1",
        hive_id="hive-1",
    )
    assert first["binding_generation"] == 1

    with pytest.raises(ValueError, match="active binding"):
        manager.create_conversation_binding(
            conversation_id=conversation_id,
            account_key="alpha",
            workspace_id="ws-a",
            remote_thread_id="remote-2",
        )

    retired = manager.retire_conversation_binding(
        first["binding_id"], account_key="alpha", workspace_id="ws-a"
    )
    assert retired is not None and retired["status"] == "retired"

    second = manager.create_conversation_binding(
        conversation_id=conversation_id,
        account_key="alpha",
        workspace_id="ws-a",
        remote_thread_id="remote-2",
        predecessor_binding_id=first["binding_id"],
        bee_id="bee-1",
        hive_id="hive-1",
    )
    assert second["binding_generation"] == 2
    active = manager.get_active_conversation_binding(
        conversation_id=conversation_id, account_key="alpha", workspace_id="ws-a"
    )
    assert active is not None and active["binding_id"] == second["binding_id"]

    with manager._get_conn() as conn:
        predecessor = conn.execute(
            "SELECT successor_binding_id FROM conversation_bindings WHERE binding_id=?",
            (first["binding_id"],),
        ).fetchone()
    assert predecessor["successor_binding_id"] == second["binding_id"]


def test_sync_persist_path_advances_durable_checkpoint(monkeypatch, tmp_path):
    class FakeClient:
        space_id = "ws"
        user_id = "user"
        account_key = "alpha"

    responses = [
        {
            "transcripts": [
                {
                    "id": "thread-1",
                    "title": "Thread",
                    "messages": [],
                }
            ],
            "nextCursor": "cursor-next",
            "hasMore": True,
        }
    ]

    monkeypatch.setattr(
        "app.chat_history.notion_sync._post_json",
        lambda _client, _url, _payload: responses.pop(0),
    )
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")

    bundle = sync_chat_history_from_notion(
        FakeClient(),
        max_pages=1,
        hydrate=False,
        store=store,
        persist=True,
    )

    assert bundle["sync_summary"]["persisted"] is True
    assert bundle["sync_summary"]["checkpoint_advanced"] is True
    assert store.get_sync_cursor(
        "inference_transcripts", workspace_id="ws", account_key="alpha"
    ) == "cursor-next"
    assert isinstance(bundle.get("persist_result"), dict)
