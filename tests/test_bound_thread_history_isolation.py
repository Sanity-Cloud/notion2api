from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

from app.config import API_KEY
from app.conversation import ConversationManager
from app.notion_client import (
    BOUND_THREAD_HISTORY_REPLAY,
    NotionOpusAPI,
    NotionUpstreamError,
    validate_bound_thread_transcript,
)
from app.server import app


TRIGGER_PHRASES = [
    "location history",
    "earlier placeholders",
    "before any filing allegation",
    "remember the service date",
    "review prior history",
]


def _manager(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    return ConversationManager()


@pytest.mark.parametrize("trigger", TRIGGER_PHRASES)
def test_bound_thread_trigger_words_never_replay_local_history(
    tmp_path, monkeypatch, trigger
):
    manager = _manager(tmp_path, monkeypatch)
    conversation_id = manager.new_conversation(conversation_id="bound-conversation")
    manager.add_message(conversation_id, "user", "historical user message")
    manager.add_message(conversation_id, "assistant", "historical assistant answer")
    manager.set_conversation_thread_id(conversation_id, "remote-thread", "terra")
    client = NotionOpusAPI(
        {"user_id": "user", "space_id": "space", "token_v2": "token"}
    )

    payload = manager.get_transcript_payload(
        notion_client=client,
        conversation_id=conversation_id,
        new_prompt=f"Please {trigger} for this request.",
        model_name="terra",
        recall_query=trigger,
    )

    transcript = payload["transcript"]
    assert [block["type"] for block in transcript] == ["config", "context", "user"]
    assert "historical user message" not in repr(transcript)
    assert "historical assistant answer" not in repr(transcript)
    assert payload["memory_mode"] == "remote_native"
    assert payload["history_messages_sent"] == 0
    assert payload["replayed_local_history"] is False
    assert payload["recall_query_ignored"] is True


def test_transport_rejects_assistant_history_before_network():
    client = NotionOpusAPI(
        {"user_id": "user", "space_id": "space", "token_v2": "token"}
    )
    client._scraper = MagicMock()
    transcript = [
        {"type": "config", "value": {"type": "workflow", "model": "terra"}},
        {"type": "assistant", "value": "historical answer"},
        {"type": "user", "value": "new request"},
    ]

    with pytest.raises(NotionUpstreamError) as captured:
        list(client.stream_response(transcript, thread_id="remote-thread"))

    assert captured.value.status_code == 409
    assert captured.value.retriable is False
    assert BOUND_THREAD_HISTORY_REPLAY in captured.value.response_excerpt
    client._scraper.post.assert_not_called()


def test_transport_rejects_multiple_user_messages():
    transcript = [
        {"type": "config", "value": {"type": "workflow", "model": "terra"}},
        {"type": "user", "value": "historical user message"},
        {"type": "user", "value": "new request"},
    ]

    with pytest.raises(NotionUpstreamError) as captured:
        validate_bound_thread_transcript(transcript)

    assert BOUND_THREAD_HISTORY_REPLAY in captured.value.response_excerpt


def test_transport_rejects_recalled_archive_marker_inside_user_block():
    transcript = [
        {"type": "config", "value": {"type": "workflow", "model": "terra"}},
        {
            "type": "user",
            "value": "[Recalled conversation archive]\nold user and assistant rounds",
        },
    ]

    with pytest.raises(NotionUpstreamError) as captured:
        validate_bound_thread_transcript(transcript)

    assert BOUND_THREAD_HISTORY_REPLAY in captured.value.response_excerpt


def test_non_dialog_bound_control_call_remains_allowed():
    validate_bound_thread_transcript(
        [{"type": "config", "value": {"type": "workflow", "model": "terra"}}]
    )


def test_api_rejects_bound_client_history_before_account_acquisition(
    tmp_path, monkeypatch
):
    manager = _manager(tmp_path, monkeypatch)
    conversation_id = manager.new_conversation(conversation_id="api-bound-conversation")
    manager.set_conversation_thread_id(conversation_id, "remote-api-thread", "terra")
    account_pool = MagicMock()
    headers = {"Authorization": f"Bearer {API_KEY}"} if API_KEY else {}
    payload = {
        "model": "terra",
        "conversation_id": conversation_id,
        "messages": [
            {"role": "user", "content": "historical user message"},
            {"role": "assistant", "content": "historical assistant answer"},
            {"role": "user", "content": "new request"},
        ],
    }

    with TestClient(app) as client:
        original_manager = app.state.conversation_manager
        original_pool = app.state.account_pool
        try:
            app.state.conversation_manager = manager
            app.state.account_pool = account_pool
            response = client.post(
                "/v1/chat/completions",
                json=payload,
                headers=headers,
            )
        finally:
            app.state.conversation_manager = original_manager
            app.state.account_pool = original_pool

    assert response.status_code == 409
    body = response.json()
    assert body["error"]["code"] == "BOUND_THREAD_HISTORY_REPLAY"
    assert body["error"]["type"] == "conversation_integrity_error"
    assert "history_message_count" in body["error"]["detail"]
    account_pool.acquire.assert_not_called()
