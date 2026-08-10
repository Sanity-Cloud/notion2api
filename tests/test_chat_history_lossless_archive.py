import json
import sqlite3
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.chat_history.extractor import (
    normalize_thread_message_record,
    visible_transcript_message,
)
from app.chat_history.contracts import runtime_history_contract
from app.chat_history.lossless_archive import (
    ensure_archive_schema,
    persist_raw_record,
    persist_thread_message_record,
)
from app.chat_history.notion_sync import sync_chat_history_from_notion
from app.chat_history.schema_activation import absent_shard_receipt, activate_shard
from app.chat_history.store import ChatHistoryStore
from app.conversation import ConversationManager


DRIFT_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "notion-chat-history-drift.json"


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


def test_sanitized_notion_drift_fixture_contract_and_parser_warnings():
    fixture_text = DRIFT_FIXTURE_PATH.read_text(encoding="utf-8")
    lowered = fixture_text.lower()
    for forbidden in (
        "token_v2",
        "authorization",
        "cookie",
        "password",
        "api_key",
        "user_email",
        "@example.",
        "@gmail.",
    ):
        assert forbidden not in lowered

    fixture = json.loads(fixture_text)
    assert fixture["fixture_contract"] == "synthetic-notion-chat-history-drift-v1"
    assert len(fixture["cases"]) >= 8
    for case in fixture["cases"]:
        record = normalize_thread_message_record(case["message_id"], case["raw"])
        assert record is not None, case["name"]
        assert record["visible"] is case["expected_visible"], case["name"]
        assert set(record["normalization_warnings"]) >= set(
            case["expected_warnings"]
        ), case["name"]


def test_health_handler_exposes_bounded_history_contract(monkeypatch, tmp_path):
    monkeypatch.setenv(
        "NOTION_ADMISSION_DB_PATH", str(tmp_path / "admission" / "admission.db")
    )
    from app.server import health_check

    receipt_commit = "6ec66fdab5c7fc6b85ac9ffc790ebc93b0429e55"
    monkeypatch.setenv("NOTION2API_BUILD_COMMIT", receipt_commit)
    monkeypatch.setattr("app.server.get_chat_history_db_root", lambda: str(tmp_path / "history"))
    monkeypatch.setattr(
        "app.server.get_notion_admission_controller",
        lambda: SimpleNamespace(snapshot=lambda: {"status": "fixture"}),
    )

    class FakePool:
        def get_status_summary(self):
            return {"active": 1, "total": 1, "cooling": 0}

        def get_selection_summary(self):
            return {"strategy": "fixture"}

        def get_governance_summary(self):
            return {"contract": "fixture"}

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                start_time=time.time() - 5,
                account_pool=FakePool(),
                conversation_manager=SimpleNamespace(
                    db_path=str(tmp_path / "private" / "conversations.db")
                ),
            )
        )
    )
    response = health_check(request)
    history = response["history"]

    assert history["build_commit"] == receipt_commit
    assert history["history_schema_hash_scope"] == "declared_contract"
    assert history["live_server_archive_mode"] == "reconciliation_only"
    assert len(history["conversation_store_id"]) == 24
    assert len(history["history_store_id"]) == 24
    assert "private" not in repr(history).lower()
    assert "conversations.db" not in repr(history).lower()


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


def test_current_notion_double_wrapped_thread_message_is_unwrapped_and_versioned():
    raw = {
        "spaceId": "ws",
        "value": {
            "role": "editor",
            "value": {
                "id": "assistant-live",
                "version": 7,
                "space_id": "ws",
                "parent_id": "thread-live",
                "parent_table": "thread",
                "created_time": 1786278235427,
                "step": {
                    "id": "assistant-live",
                    "type": "agent-inference",
                    "value": [
                        {"type": "text", "content": "live answer"},
                        {"type": "citation", "content": {"pageId": "page-1"}},
                    ],
                },
            },
        },
    }

    record = normalize_thread_message_record("assistant-live", raw)

    assert record is not None
    assert record["thread_id"] == "thread-live"
    assert record["step_type"] == "agent-inference"
    assert record["visible"] is True
    assert record["role"] == "assistant"
    assert record["text"] == "live answer"
    assert record["version"] == 7
    assert [part["part_type"] for part in record["parts"]] == ["text", "citation"]


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


@pytest.mark.parametrize("version", [7, None])
def test_distinct_payloads_survive_under_same_resolved_version(version):
    conn = sqlite3.connect(":memory:")
    ensure_archive_schema(conn)
    kwargs = dict(
        account_key="alpha",
        workspace_id="ws",
        notion_user_id="user",
        table_name="thread_message",
        record_id="edited-message",
        version=version,
        last_version=None,
        source_kind="notion_server",
        source_endpoint="syncRecordValuesSpaceInitial",
    )

    assert persist_raw_record(conn, **kwargs, raw={"id": "edited-message", "text": "A"})
    assert persist_raw_record(conn, **kwargs, raw={"id": "edited-message", "text": "B"})
    assert not persist_raw_record(conn, **kwargs, raw={"id": "edited-message", "text": "B"})

    resolved_version = 7 if version == 7 else -1
    observations = conn.execute(
        """
        SELECT raw_json FROM raw_notion_record_observations
        WHERE account_key='alpha' AND workspace_id='ws' AND record_id='edited-message'
          AND version=? ORDER BY content_hash
        """,
        (resolved_version,),
    ).fetchall()
    canonical = conn.execute(
        """
        SELECT raw_json FROM raw_notion_records
        WHERE account_key='alpha' AND workspace_id='ws' AND record_id='edited-message'
          AND version=?
        """,
        (resolved_version,),
    ).fetchone()
    assert len(observations) == 2
    assert json.loads(canonical[0])["text"] == "B"


def test_observations_preserve_account_workspace_and_sync_run_provenance():
    conn = sqlite3.connect(":memory:")
    ensure_archive_schema(conn)
    for account_key, workspace_id, sync_run_id in (
        ("alpha", "ws-a", "run-a"),
        ("beta", "ws-b", "run-b"),
    ):
        assert persist_raw_record(
            conn,
            account_key=account_key,
            workspace_id=workspace_id,
            notion_user_id=f"user-{account_key}",
            table_name="thread_message",
            record_id="same-message",
            version=3,
            last_version=2,
            raw={"id": "same-message", "version": 3},
            sync_run_id=sync_run_id,
        )

    rows = conn.execute(
        """
        SELECT account_key, workspace_id, sync_run_id
        FROM raw_notion_record_observations ORDER BY account_key
        """
    ).fetchall()
    assert rows == [("alpha", "ws-a", "run-a"), ("beta", "ws-b", "run-b")]


def test_schema_activation_does_not_backfill_existing_raw_records():
    conn = sqlite3.connect(":memory:")
    conn.execute(
        """
        CREATE TABLE raw_notion_records (
          account_key TEXT NOT NULL, workspace_id TEXT NOT NULL, notion_user_id TEXT,
          table_name TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL,
          last_version INTEGER, raw_json TEXT NOT NULL, content_hash TEXT,
          source_kind TEXT, source_endpoint TEXT, retrieved_at INTEGER NOT NULL,
          imported_at INTEGER NOT NULL,
          PRIMARY KEY (account_key, workspace_id, table_name, record_id, version)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO raw_notion_records VALUES
        ('alpha','ws','user','thread_message','legacy',1,NULL,'{}',NULL,NULL,NULL,1,1)
        """
    )

    ensure_archive_schema(conn)

    assert conn.execute("SELECT COUNT(*) FROM raw_notion_records").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM raw_notion_record_observations").fetchone()[0] == 0


def test_raw_observation_and_canonical_write_roll_back_with_semantic_failure(
    tmp_path, monkeypatch
):
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    record = _semantic_record("atomic-message", version=1)

    def fail_semantic_write(*_args, **_kwargs):
        raise RuntimeError("semantic write failed")

    monkeypatch.setattr(
        "app.chat_history.store.persist_thread_message_record", fail_semantic_write
    )
    with pytest.raises(RuntimeError, match="semantic write failed"):
        store.upsert_bundle(
            {"threads": {}, "messages": {}, "thread_messages": {record["id"]: record}},
            workspace_id="ws",
            notion_user_id="user",
            account_key="alpha",
            sync_run_id="run-atomic",
        )

    with sqlite3.connect(store.db_path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM raw_notion_records").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM raw_notion_record_observations").fetchone()[0] == 0


def test_parser_warnings_are_persisted_with_projection():
    raw = {
        "value": {
            "value": {
                "value": {
                    "version": 1,
                    "step": {
                        "type": "future-step",
                        "value": [{"type": "future-part", "content": {"opaque": True}}],
                    },
                }
            }
        }
    }
    record = normalize_thread_message_record(None, raw)
    assert record is not None
    assert set(record["normalization_warnings"]) >= {
        "future_wrapper_nesting",
        "synthetic_message_id",
        "unknown_step_type",
        "unsupported_inference_part_type",
        "missing_thread_reference",
    }

    conn = sqlite3.connect(":memory:")
    ensure_archive_schema(conn)
    persist_raw_record(
        conn,
        account_key="alpha",
        workspace_id="ws",
        notion_user_id="user",
        table_name="thread_message",
        record_id=record["id"],
        version=record["version"],
        last_version=record["last_version"],
        raw=record["raw_wrapper"],
    )
    persist_thread_message_record(
        conn,
        account_key="alpha",
        workspace_id="ws",
        notion_user_id="user",
        record=record,
    )
    row = conn.execute(
        """
        SELECT normalization_outcome, normalization_warnings_json
        FROM notion_thread_messages
        """
    ).fetchone()
    assert row[0] == "normalized_with_warnings"
    assert set(json.loads(row[1])) == set(record["normalization_warnings"])


def test_semantic_projection_rejects_missing_source_observation():
    record = _semantic_record("orphan-message", version=1)
    conn = sqlite3.connect(":memory:")
    ensure_archive_schema(conn)

    with pytest.raises(ValueError, match="exact archived source observation"):
        persist_thread_message_record(
            conn,
            account_key="alpha",
            workspace_id="ws",
            notion_user_id="user",
            record=record,
        )


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


def test_malformed_unknown_negative_version_semantic_row_is_not_fresh(tmp_path):
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    malformed = {
        "id": "message-unknown",
        "thread_id": "thread-1",
        "step_type": "unknown",
        "visible": False,
        "role": None,
        "semantic_role": None,
        "text": "",
        "created_time": None,
        "version": -1,
        "last_version": None,
        "parts": [],
        "raw": {"value": {"id": "message-unknown", "version": 1}},
        "raw_wrapper": {"value": {"id": "message-unknown", "version": 1}},
    }
    store.upsert_bundle(
        {"threads": {}, "messages": {}, "thread_messages": {malformed["id"]: malformed}},
        workspace_id="ws",
        notion_user_id="user",
        account_key="alpha",
    )

    assert store.fresh_message_ids(
        {"message-unknown": None}, workspace_id="ws", account_key="alpha"
    ) == set()


def test_visible_projection_does_not_suppress_missing_semantic_message_once_archive_exists(tmp_path):
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    # Seed one valid semantic record so this account/workspace is no longer a
    # pre-lossless shard.
    store.upsert_bundle(
        {
            "threads": {},
            "messages": {},
            "thread_messages": {
                "semantic-existing": _semantic_record(
                    "semantic-existing",
                    version=1,
                    thread_id="thread-1",
                )
            },
        },
        workspace_id="ws",
        notion_user_id="user",
        account_key="alpha",
    )
    # Simulate a live flattened assistant projection whose authoritative
    # thread_message has not yet been hydrated.
    store.record_live_turn(
        {
            "threads": {
                "thread-1": {
                    "id": "thread-1",
                    "title": "Thread",
                    "message_ids": ["assistant-flat"],
                    "raw": {"type": "live_chat"},
                }
            },
            "messages": {
                "assistant-flat": {
                    "id": "assistant-flat",
                    "thread_id": "thread-1",
                    "role": "assistant",
                    "text": "flattened answer",
                    "raw": {"role": "assistant", "text": "flattened answer"},
                }
            },
        }
    )

    assert store.existing_message_ids(["assistant-flat"]) == {"assistant-flat"}
    assert store.fresh_message_ids(
        {"assistant-flat": None}, workspace_id="ws", account_key="alpha"
    ) == set()


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


def test_stale_concurrent_sync_cannot_overwrite_newer_checkpoint(tmp_path):
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    assert store.advance_sync_cursor(
        "inference_transcripts",
        "cursor-old",
        workspace_id="ws",
        account_key="alpha",
    )
    # Both runs began from cursor-old, but only the first writer may advance.
    assert store.advance_sync_cursor(
        "inference_transcripts",
        "cursor-new",
        workspace_id="ws",
        account_key="alpha",
        sync_run_id="run-winner",
        expected_cursor_value="cursor-old",
        enforce_expected_cursor=True,
    )
    assert not store.advance_sync_cursor(
        "inference_transcripts",
        "cursor-stale",
        workspace_id="ws",
        account_key="alpha",
        sync_run_id="run-stale",
        expected_cursor_value="cursor-old",
        enforce_expected_cursor=True,
    )
    assert store.get_sync_cursor(
        "inference_transcripts", workspace_id="ws", account_key="alpha"
    ) == "cursor-new"
    with sqlite3.connect(store.db_path) as conn:
        event = conn.execute(
            """
            SELECT event_type FROM reconciliation_events
            WHERE sync_run_id='run-stale'
            """
        ).fetchone()
    assert event == ("checkpoint_stale_writer_rejected",)


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

    with pytest.raises(RuntimeError, match="controlled activation"):
        ChatHistoryStore(str(db_path), account_key="alpha")

    activate_shard(db_path, backup_dir=tmp_path / "legacy-backup", timeout_seconds=2.0)
    ChatHistoryStore(str(db_path), account_key="alpha")

    conn = sqlite3.connect(db_path)
    assert conn.execute("SELECT title FROM chat_threads WHERE id='t1'").fetchone()[0] == "legacy"
    assert conn.execute("SELECT text FROM chat_messages WHERE id='m1'").fetchone()[0] == "legacy answer"
    message_columns = {row[1] for row in conn.execute("PRAGMA table_info(chat_messages)")}
    assert {"account_key", "workspace_id", "version", "step_type", "visible"} <= message_columns
    tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"raw_notion_records", "notion_thread_messages", "sync_runs", "reconciliation_events"} <= tables
    conn.close()


def test_controlled_schema_activation_backs_up_and_preserves_existing_history(tmp_path):
    db_path = tmp_path / "account-history.db"
    backup_dir = tmp_path / "backups"
    conn = sqlite3.connect(db_path)
    assert conn.execute("PRAGMA journal_mode=WAL").fetchone()[0] == "wal"
    conn.execute(
        """
        CREATE TABLE raw_notion_records (
          account_key TEXT NOT NULL, workspace_id TEXT NOT NULL, notion_user_id TEXT,
          table_name TEXT NOT NULL, record_id TEXT NOT NULL, version INTEGER NOT NULL,
          last_version INTEGER, raw_json TEXT NOT NULL, content_hash TEXT,
          source_kind TEXT, source_endpoint TEXT, retrieved_at INTEGER NOT NULL,
          imported_at INTEGER NOT NULL,
          PRIMARY KEY (account_key, workspace_id, table_name, record_id, version)
        )
        """
    )
    conn.execute(
        """
        INSERT INTO raw_notion_records VALUES
        ('fixture','fixture-space','fixture-user','thread_message','fixture-message',1,
         NULL,'{"id":"fixture-message","text":"neutral"}','fixture-hash',
         'fixture','fixture-endpoint',1,1)
        """
    )
    conn.commit()
    conn.close()

    receipt = activate_shard(db_path, backup_dir=backup_dir, timeout_seconds=2.0)

    assert receipt["integrity_check"] == "ok"
    assert receipt["backup_integrity_check"] == "ok"
    assert receipt["pre_state"]["canonical_raw_hash"] == receipt["post_state"][
        "canonical_raw_hash"
    ]
    assert receipt["post_state"]["counts"]["raw_notion_record_observations"] == 0
    assert (backup_dir / receipt["backup_filename"]).is_file()
    with sqlite3.connect(db_path) as conn:
        assert conn.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert conn.execute(
            "SELECT value FROM chat_history_archive_meta WHERE key='history_schema_version'"
        ).fetchone()[0] == "2"
        assert conn.execute("SELECT COUNT(*) FROM raw_notion_records").fetchone()[0] == 1
        assert conn.execute(
            "SELECT COUNT(*) FROM raw_notion_record_observations"
        ).fetchone()[0] == 0

    second_receipt = activate_shard(db_path, backup_dir=backup_dir, timeout_seconds=2.0)
    assert second_receipt["status"] == "already_active"
    assert "backup_filename" not in second_receipt


@pytest.mark.parametrize(
    "damage_sql",
    [
        "DROP TABLE sync_cursors",
        "ALTER TABLE notion_message_parts DROP COLUMN projected_at",
        "DROP INDEX idx_sync_runs_account_started",
    ],
)
def test_runtime_refuses_partially_activated_archive_schema(tmp_path, damage_sql):
    db_path = tmp_path / "damaged-current.db"
    ChatHistoryStore(str(db_path), account_key="alpha")
    with sqlite3.connect(db_path) as conn:
        conn.execute(damage_sql)
        conn.commit()

    with pytest.raises(RuntimeError, match="controlled activation"):
        ChatHistoryStore(str(db_path), account_key="alpha")


def test_absent_governed_shard_is_receipted_without_creating_it(tmp_path):
    db_path = tmp_path / "not-yet-materialized.db"
    receipt = absent_shard_receipt(db_path)
    assert receipt["status"] == "not_present"
    assert len(receipt["shard_id"]) == 24
    assert not db_path.exists()

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


def test_conversation_scope_aliases_and_remote_binding_are_stamped(tmp_path):
    manager = ConversationManager(str(tmp_path / "conversations.db"))
    conversation_id = manager.new_conversation()

    manager.bind_conversation_scope(
        conversation_id,
        workspace_id="ws-live",
        teamspace_id="team-live",
        user_id="user-live",
        profile_name="profile-live",
    )
    manager.set_conversation_thread_id(
        conversation_id,
        "thread-live",
        model_name="terra",
    )

    with manager._get_conn() as conn:
        row = conn.execute(
            """SELECT workspace_id,user_id,thread_id,account_key,notion_user_id,
                      notion_space_id,notion_thread_id,account_scope,account_binding_status
               FROM conversations WHERE id=?""",
            (conversation_id,),
        ).fetchone()
        binding = conn.execute(
            """SELECT account_key,workspace_id,notion_user_id,remote_thread_id,
                      binding_generation,status
               FROM conversation_bindings WHERE conversation_id=?""",
            (conversation_id,),
        ).fetchone()

    assert dict(row) == {
        "workspace_id": "ws-live",
        "user_id": "user-live",
        "thread_id": "thread-live",
        "account_key": "ws-live:user-live",
        "notion_user_id": "user-live",
        "notion_space_id": "ws-live",
        "notion_thread_id": "thread-live",
        "account_scope": "ws-live:user-live",
        "account_binding_status": "thread_bound",
    }
    assert dict(binding) == {
        "account_key": "ws-live:user-live",
        "workspace_id": "ws-live",
        "notion_user_id": "user-live",
        "remote_thread_id": "thread-live",
        "binding_generation": 1,
        "status": "active",
    }

    manager.clear_conversation_thread(conversation_id)
    with manager._get_conn() as conn:
        cleared = conn.execute(
            "SELECT thread_id,notion_thread_id,account_binding_status FROM conversations WHERE id=?",
            (conversation_id,),
        ).fetchone()
        retired = conn.execute(
            "SELECT status,retired_at FROM conversation_bindings WHERE conversation_id=?",
            (conversation_id,),
        ).fetchone()
    assert cleared["thread_id"] is None
    assert cleared["notion_thread_id"] is None
    assert cleared["account_binding_status"] == "account_bound"
    assert retired["status"] == "retired"
    assert retired["retired_at"] is not None


def test_live_turn_stamps_account_workspace_and_user_ownership(tmp_path):
    store = ChatHistoryStore(
        str(tmp_path / "history.db"),
        account_key="ws-live:user-live",
    )
    bundle = {
        "threads": {
            "thread-live": {
                "id": "thread-live",
                "title": "Live",
                "message_ids": ["user-live", "assistant-live"],
                "raw": {"type": "live_chat"},
            }
        },
        "messages": {
            "user-live": {
                "id": "user-live",
                "thread_id": "thread-live",
                "role": "user",
                "text": "question",
                "raw": {"role": "user", "text": "question"},
            },
            "assistant-live": {
                "id": "assistant-live",
                "thread_id": "thread-live",
                "role": "assistant",
                "text": "answer",
                "raw": {"role": "assistant", "text": "answer"},
            },
        },
    }

    store.record_live_turn(bundle)

    conn = sqlite3.connect(store.db_path)
    conn.row_factory = sqlite3.Row
    thread = conn.execute(
        "SELECT account_key,workspace_id,notion_user_id FROM chat_threads WHERE id='thread-live'"
    ).fetchone()
    messages = conn.execute(
        "SELECT account_key,workspace_id,notion_user_id FROM chat_messages ORDER BY id"
    ).fetchall()
    conn.close()

    assert dict(thread) == {
        "account_key": "ws-live:user-live",
        "workspace_id": "ws-live",
        "notion_user_id": "user-live",
    }
    assert all(
        dict(row)
        == {
            "account_key": "ws-live:user-live",
            "workspace_id": "ws-live",
            "notion_user_id": "user-live",
        }
        for row in messages
    )


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


def test_sync_hydrates_metadata_threads_before_messages_and_parses_live_wrappers(monkeypatch, tmp_path):
    class FakeClient:
        space_id = "ws"
        user_id = "user"
        account_key = "alpha"

    transcript_page = {
        "transcripts": [
            {
                "id": "thread-live",
                "title": "Live Thread",
                "created_at": 100,
                "updated_at": 200,
                "type": "workflow",
                "usage_summary": {"completion_count": 1, "agent_inference_count": 1},
            }
        ],
        "nextCursor": "cursor-next",
        "hasMore": True,
    }
    thread_record = {
        "recordMap": {
            "thread": {
                "thread-live": {
                    "spaceId": "ws",
                    "value": {
                        "role": "editor",
                        "value": {
                            "id": "thread-live",
                            "version": 3,
                            "space_id": "ws",
                            "messages": ["user-live", "assistant-live"],
                            "parent_id": "ws",
                            "parent_table": "space",
                            "created_time": 100,
                            "updated_time": 200,
                            "data": {"title": "Live Thread"},
                            "alive": True,
                            "type": "workflow",
                        },
                    },
                }
            }
        }
    }
    message_records = {
        "recordMap": {
            "thread_message": {
                "user-live": {
                    "spaceId": "ws",
                    "value": {
                        "role": "editor",
                        "value": {
                            "id": "user-live",
                            "version": 1,
                            "space_id": "ws",
                            "parent_id": "thread-live",
                            "parent_table": "thread",
                            "created_time": 110,
                            "step": {
                                "id": "user-live",
                                "type": "user",
                                "value": [{"type": "text", "content": "question"}],
                            },
                        },
                    },
                },
                "assistant-live": {
                    "spaceId": "ws",
                    "value": {
                        "role": "editor",
                        "value": {
                            "id": "assistant-live",
                            "version": 2,
                            "space_id": "ws",
                            "parent_id": "thread-live",
                            "parent_table": "thread",
                            "created_time": 120,
                            "step": {
                                "id": "assistant-live",
                                "type": "agent-inference",
                                "value": [{"type": "text", "content": "answer"}],
                            },
                        },
                    },
                },
            }
        }
    }

    calls = []

    def fake_post(_client, url, payload):
        calls.append((url, payload))
        if "getInferenceTranscriptsForUser" in url:
            return transcript_page
        tables = {request["pointer"]["table"] for request in payload.get("requests", [])}
        if tables == {"thread"}:
            return thread_record
        if tables == {"thread_message"}:
            return message_records
        raise AssertionError(f"unexpected hydrate payload: {payload}")

    monkeypatch.setattr("app.chat_history.notion_sync._post_json", fake_post)
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")

    bundle = sync_chat_history_from_notion(
        FakeClient(),
        max_pages=1,
        hydrate=True,
        store=store,
        persist=True,
        fresh_message_ids_lookup=lambda candidates: store.fresh_message_ids(
            candidates, workspace_id="ws", account_key="alpha"
        ),
    )

    assert bundle["sync_summary"]["thread_hydration_batches"] == 1
    assert bundle["sync_summary"]["hydration_candidate_ids"] == 2
    assert bundle["sync_summary"]["hydration_failed_ids"] == 0
    assert bundle["sync_summary"]["thread_hydration_failed_ids"] == 0
    assert bundle["sync_summary"]["hydration_graph_incomplete"] is False
    assert bundle["sync_summary"]["checkpoint_advanced"] is True
    assert bundle["persist_result"]["semantic_messages_inserted"] == 2
    assert bundle["persist_result"]["parts_written"] == 2
    rows = store.list_thread_messages("thread-live", workspace_id="ws", account_key="alpha")
    assert [(row["step_type"], row["version"], row["text"]) for row in rows] == [
        ("user", 1, "question"),
        ("agent-inference", 2, "answer"),
    ]
    with sqlite3.connect(store.db_path) as conn:
        unmatched = conn.execute(
            """
            SELECT COUNT(*)
            FROM notion_thread_messages AS message
            LEFT JOIN raw_notion_record_observations AS observation
              ON observation.account_key=message.account_key
             AND observation.workspace_id=message.workspace_id
             AND observation.table_name='thread_message'
             AND observation.record_id=message.message_id
             AND observation.version=message.version
             AND observation.content_hash=message.source_content_hash
            WHERE observation.content_hash IS NULL
            """
        ).fetchone()[0]
    assert unmatched == 0
    assert [
        {request["pointer"]["table"] for request in payload.get("requests", [])}
        for url, payload in calls
        if "syncRecordValuesSpaceInitial" in url
    ] == [{"thread"}, {"thread_message"}]


def test_sync_run_is_partial_when_checkpoint_cas_rejects_writer(monkeypatch, tmp_path):
    class FakeClient:
        space_id = "ws"
        user_id = "user"
        account_key = "alpha"

    monkeypatch.setattr(
        "app.chat_history.notion_sync._post_json",
        lambda *_args, **_kwargs: {"nextCursor": "cursor-new", "hasMore": False},
    )
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    monkeypatch.setattr(store, "advance_sync_cursor", lambda *_args, **_kwargs: False)

    bundle = sync_chat_history_from_notion(
        FakeClient(), max_pages=1, store=store, persist=True
    )

    sync_run_id = bundle["sync_summary"]["sync_run_id"]
    with sqlite3.connect(store.db_path) as conn:
        row = conn.execute(
            "SELECT status, checkpoint_advanced, metrics_json FROM sync_runs WHERE sync_run_id=?",
            (sync_run_id,),
        ).fetchone()
    assert row[0:2] == ("partial", 0)
    assert json.loads(row[2])["checkpoint_rejected"] is True


def test_sync_surfaces_completion_ledger_failure(monkeypatch, tmp_path):
    class FakeClient:
        space_id = "ws"
        user_id = "user"
        account_key = "alpha"

    monkeypatch.setattr(
        "app.chat_history.notion_sync._post_json",
        lambda *_args, **_kwargs: {"nextCursor": "cursor-new", "hasMore": False},
    )
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")
    monkeypatch.setattr(
        store,
        "finish_sync_run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("ledger unavailable")),
    )

    with pytest.raises(RuntimeError, match="sync_run_finish_failed: ledger unavailable"):
        sync_chat_history_from_notion(
            FakeClient(), max_pages=1, store=store, persist=True
        )


def test_runtime_history_contract_is_secret_safe_and_receipt_bound(tmp_path, monkeypatch):
    monkeypatch.delenv("NOTION2API_BUILD_COMMIT", raising=False)
    monkeypatch.setenv("BUILD_COMMIT", "ambient-must-not-be-used")
    conversation_path = tmp_path / "private" / "conversations.db"
    history_root = tmp_path / "private" / "chat_history"

    unverified = runtime_history_contract(
        conversation_store_path=conversation_path,
        history_store_root=history_root,
        history_schema_hash="schema-hash",
    )
    assert unverified["runtime_contract_version"] == 1
    assert unverified["build_commit"] == "unverified"
    assert unverified["live_server_archive_mode"] == "reconciliation_only"
    assert str(conversation_path.resolve()) not in repr(unverified)
    assert str(history_root.resolve()) not in repr(unverified)

    monkeypatch.setenv("NOTION2API_BUILD_COMMIT", "not-a-git-commit-or-receipt")
    malformed = runtime_history_contract(
        conversation_store_path=conversation_path,
        history_store_root=history_root,
        history_schema_hash="schema-hash",
    )
    assert malformed["build_commit"] == "unverified"

    receipt_commit = "6ec66fdab5c7fc6b85ac9ffc790ebc93b0429e55"
    monkeypatch.setenv("NOTION2API_BUILD_COMMIT", receipt_commit)
    verified = runtime_history_contract(
        conversation_store_path=conversation_path,
        history_store_root=history_root,
        history_schema_hash="schema-hash",
    )
    assert verified["build_commit"] == receipt_commit
    assert verified["history_schema_hash_scope"] == "declared_contract"


def test_sync_uses_bound_store_account_key_over_noncanonical_client_key(monkeypatch, tmp_path):
    class FakeClient:
        space_id = "ws-live"
        user_id = "user-live"
        # Reproduces the live client behavior that exposed only user_id here.
        account_key = "user-live"

    responses = iter(
        [
            {
                "transcripts": [{"id": "thread-1", "title": "Thread"}],
                "nextCursor": "cursor-next",
                "hasMore": True,
            },
            {
                "recordMap": {
                    "thread": {
                        "thread-1": {
                            "value": {
                                "value": {
                                    "id": "thread-1",
                                    "version": 1,
                                    "space_id": "ws-live",
                                    "messages": ["message-1"],
                                    "data": {"title": "Thread"},
                                }
                            }
                        }
                    }
                }
            },
            {
                "recordMap": {
                    "thread_message": {
                        "message-1": {
                            "value": {
                                "value": {
                                    "id": "message-1",
                                    "version": 1,
                                    "space_id": "ws-live",
                                    "parent_id": "thread-1",
                                    "parent_table": "thread",
                                    "step": {
                                        "id": "message-1",
                                        "type": "agent-inference",
                                        "value": [{"type": "text", "content": "answer"}],
                                    },
                                }
                            }
                        }
                    }
                }
            },
        ]
    )
    monkeypatch.setattr(
        "app.chat_history.notion_sync._post_json",
        lambda _client, _url, _payload: next(responses),
    )
    store = ChatHistoryStore(
        str(tmp_path / "history.db"),
        account_key="ws-live:user-live",
    )

    bundle = sync_chat_history_from_notion(
        FakeClient(),
        max_pages=1,
        hydrate=True,
        store=store,
        persist=True,
    )

    assert bundle["sync_summary"]["checkpoint_advanced"] is True
    conn = sqlite3.connect(store.db_path)
    cursor_rows = conn.execute(
        "SELECT account_key,workspace_id,cursor_value FROM sync_cursors"
    ).fetchall()
    run_rows = conn.execute(
        "SELECT DISTINCT account_key,workspace_id FROM sync_runs"
    ).fetchall()
    conn.close()
    assert cursor_rows == [("ws-live:user-live", "ws-live", "cursor-next")]
    assert run_rows == [("ws-live:user-live", "ws-live")]


def test_sync_does_not_advance_checkpoint_when_thread_graph_cannot_be_resolved(monkeypatch, tmp_path):
    class FakeClient:
        space_id = "ws"
        user_id = "user"
        account_key = "alpha"

    responses = iter(
        [
            {
                "transcripts": [{"id": "thread-1", "title": "Thread"}],
                "nextCursor": "cursor-next",
                "hasMore": True,
            },
            {"recordMap": {"thread": {}}},
        ]
    )
    monkeypatch.setattr(
        "app.chat_history.notion_sync._post_json",
        lambda _client, _url, _payload: next(responses),
    )
    store = ChatHistoryStore(str(tmp_path / "history.db"), account_key="alpha")

    bundle = sync_chat_history_from_notion(
        FakeClient(),
        max_pages=1,
        hydrate=True,
        store=store,
        persist=True,
    )

    assert bundle["sync_summary"]["hydration_graph_incomplete"] is True
    assert bundle["sync_summary"]["checkpoint_advanced"] is False
    assert store.get_sync_cursor(
        "inference_transcripts", workspace_id="ws", account_key="alpha"
    ) is None
