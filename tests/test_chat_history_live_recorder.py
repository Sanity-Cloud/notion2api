from __future__ import annotations

import sqlite3
from pathlib import Path
from tempfile import TemporaryDirectory

from app.chat_history.live_recorder import (
    build_live_turn_bundle,
    infer_source_system,
    record_live_chat_turn,
)
from app.chat_history.store import ChatHistoryStore


def test_infer_source_system_for_repoai_and_aigentbee() -> None:
    assert (
        infer_source_system(
            {
                "caller": {"type": "repoai", "id": "repo-ai-orchestrator"},
                "repo_ai_review_instance_id": "rev-1",
            }
        )
        == "repoai"
    )
    assert (
        infer_source_system({"session_name": "aigentbee-leader-mission-abc1234567"})
        == "aigentbee"
    )
    # AIgentBee leader tools currently label caller.type as "widget".
    assert (
        infer_source_system(
            {
                "session_name": "aigentbee-leader-demo-aaaaaaaaaa",
                "caller": {
                    "id": "aigentbee-swarm-workbench",
                    "type": "widget",
                    "mission_id": "mission-1",
                    "work_unit_id": "wu-1",
                },
            }
        )
        == "aigentbee"
    )


def test_live_turn_uses_notion_message_id_and_records_model_metadata() -> None:
    bundle = build_live_turn_bundle(
        thread_id="thread-live-1",
        conversation_id="conv-1",
        user_prompt="Review this archive",
        assistant_reply="Patch looks good",
        requested_model="claude-sonnet-4",
        model_metadata={
            "source_message_id": "notion-msg-99",
            "actual_model": "claude-sonnet-4-20250514",
            "model_provider": "anthropic",
            "requested_model": "claude-sonnet-4",
        },
        request_metadata={
            "caller": {"type": "repoai", "id": "repo-ai"},
            "session_name": "RepoAI review",
            "repo_ai_review_instance_id": "rev-9",
        },
    )

    assert "thread-live-1" in bundle["threads"]
    assert bundle["threads"]["thread-live-1"]["raw"]["live"]["source_system"] == "repoai"
    assert "notion-msg-99" in bundle["messages"]
    assert bundle["messages"]["notion-msg-99"]["actual_model"] == "claude-sonnet-4-20250514"
    assert bundle["messages"]["notion-msg-99"]["raw"]["live"]["repo_ai_review_instance_id"] == "rev-9"


def test_record_live_turn_merges_without_clobbering_hydrated_raw() -> None:
    with TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "chat_history.db"
        store = ChatHistoryStore(str(db_path))
        store.upsert_bundle(
            {
                "threads": {
                    "thread-1": {
                        "id": "thread-1",
                        "title": "Hydrated title",
                        "message_ids": ["hydrated-msg"],
                        "raw": {"type": "workflow", "encryptedContent": "keep-me"},
                    }
                },
                "messages": {
                    "hydrated-msg": {
                        "id": "hydrated-msg",
                        "thread_id": "thread-1",
                        "role": "assistant",
                        "text": "Already hydrated body",
                        "actual_model": "",
                        "raw": {"role": "assistant", "encryptedContent": "secret"},
                    }
                },
            }
        )

        result = record_live_chat_turn(
            thread_id="thread-1",
            conversation_id="conv-1",
            user_prompt="follow up",
            assistant_reply="Already hydrated body",
            model_metadata={
                "source_message_id": "hydrated-msg",
                "actual_model": "gpt-5.4",
                "model_provider": "openai",
            },
            request_metadata={"caller": {"type": "aigentbee"}},
            store=store,
        )
        assert result["recorded"] is True
        assert result["source_system"] == "aigentbee"

        # Second live turn adds a new user/assistant pair.
        record_live_chat_turn(
            thread_id="thread-1",
            conversation_id="conv-1",
            user_prompt="second question",
            assistant_reply="second answer",
            request_metadata={
                "session_name": "aigentbee-leader-demo-aaaaaaaaaa",
                "caller": {"type": "aigentbee"},
            },
            store=store,
        )

        thread = store.get_thread("thread-1")
        assert thread is not None
        assert "hydrated-msg" in thread["message_ids"]
        assert len(thread["message_ids"]) >= 3

        conn = sqlite3.connect(db_path)
        try:
            raw = conn.execute(
                "SELECT raw_json FROM chat_threads WHERE id='thread-1'"
            ).fetchone()[0]
            msg_raw = conn.execute(
                "SELECT raw_json, actual_model FROM chat_messages WHERE id='hydrated-msg'"
            ).fetchone()
        finally:
            conn.close()

        assert "encryptedContent" in raw
        assert "keep-me" in raw
        assert "live_sources" in raw
        assert msg_raw[0].find("encryptedContent") >= 0
        assert msg_raw[1] == "gpt-5.4"
