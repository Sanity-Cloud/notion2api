"""Regression tests for targeted upstream Notion HTTP and model parity."""

from unittest.mock import MagicMock, patch

import pytest

from app.model_registry import (
    MODEL_MAP,
    _catalog_model_for_request,
    get_notion_model,
)
from app.notion_client import NotionOpusAPI, NotionUpstreamError


def _client() -> NotionOpusAPI:
    return NotionOpusAPI({
        "user_id": "user-1",
        "space_id": "space-1",
        "token_v2": "fake-token",
    })


def test_endpoint_defaults_preserve_existing_host_split(monkeypatch):
    monkeypatch.delenv("NOTION_INFERENCE_BASE_URL", raising=False)
    monkeypatch.delenv("NOTION_API_BASE_URL", raising=False)
    client = _client()
    assert client.url == "https://www.notion.so/api/v3/runInferenceTranscript"
    assert client.delete_url == "https://app.notion.com/api/v3/saveTransactions"


@pytest.mark.parametrize("url", [
    "http://www.notion.so",
    "https://www.notion.so@attacker.example",
    "https://app.notion.com/path",
    "https://mirror.example",
])
def test_untrusted_or_invalid_notions_hosts_fail_closed(monkeypatch, url):
    monkeypatch.delenv("NOTION_ALLOW_CUSTOM_HOSTS", raising=False)
    monkeypatch.setenv("NOTION_INFERENCE_BASE_URL", url)
    with pytest.raises(ValueError):
        _client()


def test_explicit_custom_origin_requires_https_and_opt_in(monkeypatch):
    monkeypatch.setenv("NOTION_ALLOW_CUSTOM_HOSTS", "true")
    monkeypatch.setenv("NOTION_INFERENCE_BASE_URL", "https://mirror.example")
    assert _client().url == "https://mirror.example/api/v3/runInferenceTranscript"


def test_history_browser_profile_is_opt_in_and_has_single_accept(monkeypatch):
    client = _client()
    monkeypatch.delenv("NOTION_HISTORY_BROWSER_HEADERS", raising=False)
    original = client._build_chat_history_headers()
    assert original["referer"] == "https://www.notion.so/ai"
    assert "sec-fetch-site" not in original

    monkeypatch.setenv("NOTION_HISTORY_BROWSER_HEADERS", "true")
    browser = client._build_chat_history_headers()
    assert browser["origin"] == "https://app.notion.com"
    assert browser["referer"] == "https://app.notion.com/chat"
    assert browser["sec-fetch-site"] == "same-origin"
    assert browser["sec-ch-ua-platform"] == '"Linux"'
    assert browser["accept"] == "application/json"
    assert "Accept" not in browser
    browser["accept"] = "application/x-ndjson"
    assert browser["accept"] == "application/x-ndjson"


def test_gpt6_alias_requires_an_available_catalog_model():
    assert MODEL_MAP["gpt-6-astra"] == "orlando-quinn"
    assert get_notion_model("gpt-6-astra") == "orlando-quinn"
    record = {"canonical_id": "orlando-quinn", "display_name": "GPT-6 Astra"}
    snapshot = {
        "models": [record],
        "alias_reconciliation": {
            "active_aliases": {"gpt-6-astra": "orlando-quinn"},
        },
    }
    assert _catalog_model_for_request(snapshot, "gpt-6-astra") is record
    snapshot["models"] = [{"canonical_id": "orchid-muffin"}]
    assert _catalog_model_for_request(snapshot, "gpt-6-astra") is None


@pytest.mark.parametrize("events,expected_reason", [
    ([{"type": "stream_complete"}], "empty_stream_after_http_200"),
    ([{"type": "content", "text": "partial"}], "missing_finishedAt"),
])
def test_http_200_stream_errors_are_diagnostic_and_non_replayable(events, expected_reason):
    client = _client()
    response = MagicMock(status_code=200, text="")
    response.headers = {"Content-Type": "application/x-ndjson"}
    scraper = MagicMock()
    scraper.post.return_value = response
    profile = {
        "precreate_thread": False,
        "create_thread": False,
        "is_partial_transcript": True,
        "include_debug_overrides": False,
    }
    transcript = [{"id": "cfg", "type": "config", "value": {"type": "workflow"}}]

    with (
        patch.object(client, "_resolve_request_profile", return_value=profile),
        patch.object(client, "_build_cookie_header", return_value=""),
        patch("app.notion_client._create_notion_http_session", return_value=scraper),
        patch("app.notion_client.parse_stream", return_value=iter(events)),
        pytest.raises(NotionUpstreamError) as caught,
    ):
        list(client.stream_response(
            transcript, thread_id="thread-1", persist_remote_chat=True,
        ))

    assert caught.value.retriable is False
    assert caught.value.status_code == 502
    assert caught.value.response_excerpt == expected_reason
    assert scraper.post.call_count == 1
