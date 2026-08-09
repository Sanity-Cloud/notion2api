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
    assert [
        {request["pointer"]["table"] for request in payload.get("requests", [])}
        for url, payload in calls
        if "syncRecordValuesSpaceInitial" in url
    ] == [{"thread"}, {"thread_message"}]


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
