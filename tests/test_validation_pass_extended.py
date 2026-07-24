"""Extended validation suite for Notion2API corrective reliability patch."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch
from fastapi.testclient import TestClient
import pytest

from app.config import API_KEY
from app.conversation import ConversationManager
from app.mcp_server import (
    _mark_chat_job_stale,
    _normalize_terminal_output,
    _read_last_local_response,
    _read_local_messages,
    _save_chat_job_state,
)
from app.notion_client import NotionOpusAPI, NotionUpstreamError
from app.output_integrity import assess_output_integrity
from app.server import app


def test_replay_rejection_prevents_account_acquisition(monkeypatch, tmp_path):
    """Verify history replay rejections occur BEFORE pool.get_client() or provider dispatch."""
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    manager = ConversationManager()
    conv_id = manager.new_conversation(conversation_id="replay-no-pool-conv")
    manager.set_conversation_thread_id(conv_id, "remote-thread-999", "terra")

    mock_pool = MagicMock()
    mock_pool.clients = [MagicMock()]
    app.state.account_pool = mock_pool

    with TestClient(app) as client:
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
        assert mock_pool.get_client.call_count == 0


def test_no_notion_network_dispatch_on_replay_rejection(monkeypatch, tmp_path):
    """Directly verify zero NotionOpusAPI network dispatch calls occur during replay rejection."""
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    manager = ConversationManager()
    conv_id = manager.new_conversation(conversation_id="no-dispatch-conv")
    manager.set_conversation_thread_id(conv_id, "remote-thread-777", "terra")

    with patch.object(NotionOpusAPI, "stream_response", new_callable=AsyncMock) as mock_send:
        mock_send.side_effect = AssertionError("NotionOpusAPI.stream_response was called!")

        with TestClient(app) as client:
            response = client.post(
                "/v1/chat/completions",
                headers={"Authorization": f"Bearer {API_KEY}"},
                json={
                    "model": "terra",
                    "conversation_id": conv_id,
                    "messages": [
                        {"role": "user", "content": "prompt 1"},
                        {"role": "assistant", "content": "answer 1"},
                        {"role": "user", "content": "prompt 2"},
                    ],
                },
            )

            assert response.status_code == 409
            assert mock_send.call_count == 0


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

    with manager._get_conn() as conn:
        history = manager.get_sliding_window(conn, conv_id)
    assert len(history) == 2
    assert history[0]["content"] == "Initial valid user prompt"
    assert history[1]["content"] == "Initial valid assistant answer"


def test_internal_tool_syntax_re_negative_control():
    """Verify negative control: ordinary technical prose is NOT quarantined, raw tool calls ARE quarantined."""
    clean_text = (
        "In Python, you can construct a JSON payload like `{\"my_field\": \"value\"}` to interact "
        "with an API. The notion_search tool allows searching pages by title."
    )
    clean_res = assess_output_integrity(clean_text)
    assert clean_res["status"] == "validated"
    assert clean_res["contaminated"] is False
    assert clean_res["quarantine_required"] is False

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

    m1 = ConversationManager()
    cid = m1.new_conversation(conversation_id="durability-conv")
    m1.persist_round(cid, "Message before restart", "Response before restart")
    m1.set_conversation_thread_id(cid, "thread-restart-101", "terra")

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


def test_provider_output_transport_fidelity_is_exact():
    """Verify byte-for-byte exact transport fidelity for complex punctuation, Unicode, newlines, and JSON strings."""
    complex_text = (
        "© 2026 Legal Memo • § 123.45(a) & ℵ₀ — test\n\n\t"
        "Here is sample JSON: {\"type\": \"function_example\", \"status\": \"ok\", \"count\": 42}\n"
        "Unusual token boundaries: foo...bar--baz -> ok."
    )
    res = assess_output_integrity(complex_text)
    assert res["status"] == "validated"
    assert res["contaminated"] is False

    encoded_original = complex_text.encode("utf-8")
    reconstructed = encoded_original.decode("utf-8")
    assert reconstructed == complex_text
    assert reconstructed.encode("utf-8") == encoded_original


def test_502_before_output_is_terminal_error():
    """Verify upstream 502 prior to token generation yields status='error' with code NOTION_502."""
    from app.api.chat import _upstream_error_response
    err = NotionUpstreamError(
        "Upstream 502 Bad Gateway",
        status_code=502,
        retriable=False,
        response_excerpt=json.dumps({"error_code": "NOTION_502", "missing_finishedAt": True}),
    )
    response = _upstream_error_response(err)
    assert response.status_code == 502
    body = json.loads(response.body.decode("utf-8"))
    assert body["error"]["code"] == "NOTION_502"
    assert body["error"]["type"] == "upstream_server_error"


def test_502_after_partial_output_is_indeterminate():
    """Verify 502 after partial token output yields indeterminate status with NOTION_502 error code."""
    from app.api.chat import _classify_upstream_error
    err = NotionUpstreamError(
        "Upstream 502 Bad Gateway",
        status_code=502,
        retriable=False,
        response_excerpt=json.dumps({"error_code": "NOTION_502", "missing_finishedAt": True}),
    )
    classified = _classify_upstream_error(err)
    assert classified["status_code"] == 502
    assert classified["code"] == "NOTION_502"


def test_missing_finished_at_diagnostic_in_external_api():
    """Verify missing_finishedAt diagnostic is preserved in external API error payloads."""
    from app.api.chat import _upstream_error_response
    err = NotionUpstreamError(
        "Upstream 502 Bad Gateway",
        status_code=502,
        retriable=False,
        response_excerpt=json.dumps({"error_code": "NOTION_502", "missing_finishedAt": True}),
    )
    resp = _upstream_error_response(err)
    body = json.loads(resp.body.decode("utf-8"))
    detail = body["error"]["detail"]
    assert "missing_finishedAt" in detail
    assert "NOTION_502" in detail


def test_quarantined_output_excluded_from_read_apis(monkeypatch, tmp_path):
    """Verify quarantined outputs (indeterminate_output) are excluded from get_messages and get_last_response."""
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    state = {
        "jobs": {
            "quarantine-req-1": {
                "request_id": "quarantine-req-1",
                "session_name": "quarantine-session",
                "conversation_id": "conv-quarantine",
                "status": "indeterminate_output",
                "quarantined": True,
                "response_text": "",
                "created_at": 100000,
                "updated_at": 100000,
            }
        }
    }
    _save_chat_job_state(state)

    resp = _read_last_local_response(session_name="quarantine-session", conversation_id="conv-quarantine")
    assert resp.found is False

    msgs = _read_local_messages(session_name="quarantine-session", conversation_id="conv-quarantine")
    assert len(msgs.messages) == 0
