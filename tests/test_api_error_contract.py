import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.config import API_KEY
from app.conversation import ConversationManager
from app.notion_client import NotionUpstreamError
from app.output_integrity import assess_output_integrity
from app.server import app


def test_bound_thread_replay_returns_http_409(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    manager = ConversationManager()
    conv_id = manager.new_conversation(conversation_id="bound-test-conv")
    manager.set_conversation_thread_id(conv_id, "remote-thread-123", "terra")

    with TestClient(app) as client:
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}"},
            json={
                "model": "terra",
                "conversation_id": conv_id,
                "messages": [
                    {"role": "user", "content": "historical user turn"},
                    {"role": "assistant", "content": "historical assistant turn"},
                    {"role": "user", "content": "new user turn"},
                ],
            },
        )

        assert response.status_code == 409
        body = response.json()
        assert body["error"]["code"] == "BOUND_THREAD_HISTORY_REPLAY"
        assert body["error"]["type"] == "conversation_integrity_error"
        assert "retry later" not in body["error"].get("suggestion", "").lower()
        assert "do not resubmit" in body["error"].get("suggestion", "").lower() or "do not replay" in body["error"].get("suggestion", "").lower()


def test_persistent_unbound_session_rejects_assistant_history(monkeypatch, tmp_path):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    with TestClient(app) as client:
        response = client.post(
            "/v1/chat/completions",
            headers={"Authorization": f"Bearer {API_KEY}"},
            json={
                "model": "terra",
                "messages": [
                    {"role": "user", "content": "prior turn"},
                    {"role": "assistant", "content": "prior assistant reply"},
                    {"role": "user", "content": "new turn"},
                ],
                "metadata": {"persist_remote_chat": True},
            },
        )

        assert response.status_code == 409
        body = response.json()
        assert body["error"]["code"] == "BOUND_THREAD_HISTORY_REPLAY"


def test_internal_tool_syntax_quarantined_by_integrity_gate():
    fixture_path = Path(__file__).parent / "fixtures" / "failed_legal_research_output.json"
    with open(fixture_path, "r", encoding="utf-8") as f:
        fixture_data = json.load(f)

    raw_output = fixture_data["raw_escaping_output"]
    result = assess_output_integrity(raw_output)

    assert result["contaminated"] is True
    assert result["status"] == "indeterminate_output"
    assert "internal_tool_syntax_exposed" in result["reasons"]


def test_upstream_502_preserves_notion_502_code():
    exc = NotionUpstreamError(
        "Notion upstream stream ended before completion metadata.",
        status_code=502,
        retriable=True,
        response_excerpt="missing_finishedAt",
    )
    from app.api.chat import _classify_upstream_error, _upstream_error_response

    classified = _classify_upstream_error(exc)
    assert classified["code"] == "NOTION_502"
    assert classified["status_code"] == 502

    resp = _upstream_error_response(exc)
    assert resp.status_code == 502
    data = json.loads(resp.body)
    assert data["error"]["code"] == "NOTION_502"


def test_model_provenance_separate_fields():
    from app.api.chat import _response_model_metadata

    meta = _response_model_metadata(
        requested_model="terra",
        model_metadata={
            "notion_requested_model": "orchid-muffin",
            "notion_step_model": "orchid-muffin",
        },
    )

    assert meta["route_alias"] == "terra"
    assert meta["resolved_route_model"] == "orchid-muffin"
    assert meta["observed_step_model"] == "orchid-muffin"
    assert meta.get("verified_model", "") == ""
    assert meta["model_identity_verified"] is False
    assert "verified" in meta["model_identity_warning"].lower()
