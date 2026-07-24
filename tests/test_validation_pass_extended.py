"""Extended validation suite for Notion2API corrective reliability patch."""

from __future__ import annotations

import json
from unittest.mock import MagicMock
from fastapi.testclient import TestClient
import pytest

from app.config import API_KEY
from app.conversation import ConversationManager
from app.mcp_server import (
    _mark_chat_job_stale,
    _normalize_terminal_output,
    _read_last_local_response,
    _read_local_messages,
)
from app.output_integrity import assess_output_integrity
from app.server import app


def test_replay_rejection_prevents_account_acquisition(monkeypatch, tmp_path):
    """Verify history replay rejections occur BEFORE pool.get_client() or provider dispatch."""
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    manager = ConversationManager()
    conv_id = manager.new_conversation(conversation_id="replay-no-pool-conv")
    manager.set_conversation_thread_id(conv_id, "remote-thread-999", "terra")

    # Set mock_pool on app.state before TestClient context enters
    mock_pool = MagicMock()
    mock_pool.clients = [MagicMock()]
    app.state.account_pool = mock_pool

    with TestClient(app) as client:
        # Reset mock calls after lifespan startup finishes
        mock_pool.get_client.reset_mock()

        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}"},
            json={
                "model": "terra",
                "conversation_id": conv_id,
                "messages": [
                    {"role": "user", "content": "previous prompt"},
                    {"role": "assistant", "content": "previous response"},
                    {"role": "user", "content": "new prompt"},
                ],
            },
        )

        assert response.status_code == 409
        body = response.json()
        assert body["error"]["code"] == "BOUND_THREAD_HISTORY_REPLAY"
        # Assert get_client was NEVER called for the chat completion request
        assert mock_pool.get_client.call_count == 0, f"get_client calls: {mock_pool.get_client.call_args_list}"


def test_replay_rejection_preserves_previous_valid_response(monkeypatch, tmp_path):
    """Verify that a rejected replay does not corrupt or overwrite previous valid conversation turns."""
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    manager = ConversationManager()
    conv_id = manager.new_conversation(conversation_id="preserve-history-conv")
    manager.persist_round(conv_id, "Initial valid user prompt", "Initial valid assistant answer")
    manager.set_conversation_thread_id(conv_id, "remote-thread-888", "terra")

    with TestClient(app) as client:
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}"},
            json={
                "model": "terra",
                "conversation_id": conv_id,
                "messages": [
                    {"role": "user", "content": "Initial valid user prompt"},
                    {"role": "assistant", "content": "Initial valid assistant answer"},
                    {"role": "user", "content": "New prompt with replay"},
                ],
            },
        )
        assert response.status_code == 409

    # Inspect stored messages in DB
    with manager._get_conn() as conn:
        history = manager.get_sliding_window(conn, conv_id)
    assert len(history) == 2
    assert history[0]["content"] == "Initial valid user prompt"
    assert history[1]["content"] == "Initial valid assistant answer"


def test_internal_tool_syntax_re_negative_control():
    """Verify negative control: ordinary technical prose is NOT quarantined, raw tool calls ARE quarantined."""
    # 1. Negative control: normal technical prose explaining tool calls / JSON
    clean_text = (
        "In Python, you can construct a JSON payload like `{\"my_field\": \"value\"}` to interact "
        "with an API. The notion_search tool allows searching pages by title."
    )
    clean_res = assess_output_integrity(clean_text)
    assert clean_res["status"] == "validated"
    assert clean_res["contaminated"] is False
    assert clean_res["quarantine_required"] is False

    # 2. Positive control: raw unexecuted research tool syntax / action plan envelope
    with open("tests/fixtures/failed_legal_research_output.json", "r", encoding="utf-8") as f:
        fixture_data = json.load(f)
    raw_syntax = fixture_data["raw_escaping_output"]
    contaminated_res = assess_output_integrity(raw_syntax)
    assert contaminated_res["status"] == "indeterminate_output"
    assert contaminated_res["contaminated"] is True
    assert contaminated_res["quarantine_required"] is True
    assert "internal_tool_syntax_exposed" in contaminated_res["reasons"]


def test_stale_job_terminalization():
    """Verify stranded jobs in pending/running status are terminalized to 'stale' with retry_safe=False."""
    job = {
        "request_id": "req-stale-test",
        "status": "running",
        "created_at": 100000,
        "updated_at": 100000,
    }
    stale_job = _mark_chat_job_stale(job)
    assert stale_job["status"] == "stale"
    assert "restarted or lost" in stale_job["error"]


def test_restart_durability(monkeypatch, tmp_path):
    """Verify SQLite database persistence remains readable across new manager process cycles."""
    db_file = tmp_path / "restart_test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))

    # Manager cycle 1
    m1 = ConversationManager()
    cid = m1.new_conversation(conversation_id="durability-conv")
    m1.persist_round(cid, "Message before restart", "Response before restart")
    m1.set_conversation_thread_id(cid, "thread-restart-101", "terra")

    # Manager cycle 2 (simulating process restart)
    m2 = ConversationManager()
    with m2._get_conn() as conn:
        messages = m2.get_sliding_window(conn, cid)
    assert len(messages) == 2
    assert messages[0]["content"] == "Message before restart"
    assert messages[1]["content"] == "Response before restart"
    assert m2.get_conversation_thread_id(cid) == "thread-restart-101"


def test_quarantined_output_normalization():
    """Verify _normalize_terminal_output quarantines contaminated output into indeterminate_output."""
    raw_output = {
        "status": "completed",
        "response_text": '{"tool_call_id": "call_123", "action_plan": ["search"]}',
        "ok": True,
    }
    normalized, evidence = _normalize_terminal_output(raw_output, source="test")
    assert normalized["status"] == "indeterminate_output"
    assert normalized["quarantined"] is True
    assert normalized["response_text"] == ""
    assert normalized["retry_safe"] is False


def test_persistence_source_provenance_readback(monkeypatch, tmp_path):
    """Verify that readback APIs explicitly report persistence_source, durable_persisted, and reconciliation_required."""
    db_file = tmp_path / "provenance_test.db"
    monkeypatch.setenv("DB_PATH", str(db_file))

    manager = ConversationManager()
    cid = manager.new_conversation(conversation_id="prov-conv")
    manager.persist_round(cid, "Hello", "Hi there!")

    resp = _read_last_local_response(conversation_id=cid)
    assert resp.ok is True
    assert resp.found is True
    assert resp.persistence_source == "conversation_db"
    assert resp.durable_persisted is True
    assert resp.reconciliation_required is False

    msgs = _read_local_messages(conversation_id=cid)
    assert msgs.ok is True
    assert msgs.persistence_source == "conversation_db"
    assert msgs.durable_persisted is True
    assert msgs.reconciliation_required is False
