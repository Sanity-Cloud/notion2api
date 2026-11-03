"""Host overrides, shared browser headers, and empty-stream retry policy."""

# pylint: disable=missing-function-docstring,protected-access

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

from app.notion_client import NotionOpusAPI, NotionUpstreamError
from app.notion_hosts import (
    notion_app_api,
    notion_app_origin,
    notion_cookie_domain,
    notion_web_api,
    notion_web_origin,
    resolve_notion_origin,
    sec_fetch_site,
)


def _client() -> NotionOpusAPI:
    return NotionOpusAPI(
        {"user_id": "user-1", "space_id": "space-1", "token_v2": "token"}
    )


def test_default_hosts_stay_on_www_and_app(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("NOTION_URL", raising=False)
    monkeypatch.delenv("NOTION_DOMAIN", raising=False)

    assert notion_web_origin() == "https://www.notion.so"
    assert notion_app_origin() == "https://app.notion.com"
    assert notion_cookie_domain() == "www.notion.so"
    assert notion_web_api("runInferenceTranscript").startswith("https://www.notion.so/")
    assert notion_app_api("saveTransactions").startswith("https://app.notion.com/")


@pytest.mark.parametrize(
    "raw",
    [
        "",
        "   ",
        "http://www.notion.so",
        "https://user:pass@www.notion.so",
        "https://www.notion.so:8443",
        "https://www.notion.so/chat",
        "https://www.notion.so.evil.com",
        "https://evil.com",
        "javascript:alert(1)",
        "app.notion.com.evil.com",
    ],
)
def test_unsafe_host_overrides_keep_defaults(raw: str) -> None:
    assert (
        resolve_notion_origin(raw, default="https://www.notion.so")
        == "https://www.notion.so"
    )
    assert (
        resolve_notion_origin(raw, default="https://app.notion.com")
        == "https://app.notion.com"
    )


def test_safe_host_overrides_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTION_URL", "custom.notion.so")
    monkeypatch.setenv("NOTION_DOMAIN", "https://alt.notion.com")

    assert notion_web_origin() == "https://custom.notion.so"
    assert notion_app_origin() == "https://alt.notion.com"
    assert notion_cookie_domain() == "custom.notion.so"
    client = _client()
    assert client.url == "https://custom.notion.so/api/v3/runInferenceTranscript"
    assert client.delete_url == "https://alt.notion.com/api/v3/saveTransactions"


def test_sec_fetch_site_distinguishes_web_and_app_hosts() -> None:
    page = "https://www.notion.so"
    assert sec_fetch_site(page, notion_web_api("runInferenceTranscript")) == "same-origin"
    assert sec_fetch_site(page, notion_app_api("getAvailableModels")) == "cross-site"
    assert sec_fetch_site(page, "https://files.notion.so/api") == "same-site"


def test_history_and_inference_headers_share_fingerprint_per_endpoint() -> None:
    client = _client()
    with patch.object(client, "_build_cookie_header", return_value="token_v2=secret"):
        inference = client._build_browser_headers(
            client.url,
            accept="application/x-ndjson",
            referer_path="/chat",
            extra={"x-request-id": "trace-1"},
        )
        history = client._build_chat_history_headers(
            notion_web_api("getInferenceTranscriptsForUser")
        )
        app_call = client._build_chat_history_headers(
            notion_app_api("getAvailableModels")
        )

    assert inference["Accept"] == "application/x-ndjson"
    assert inference["referer"] == "https://www.notion.so/chat"
    assert inference["sec-fetch-site"] == "same-origin"
    assert inference["cookie"] == "token_v2=secret"
    assert inference["x-request-id"] == "trace-1"
    assert inference["x-notion-active-user-header"] == "user-1"
    assert history["referer"] == "https://www.notion.so/ai"
    assert history["sec-fetch-site"] == "same-origin"
    assert history["cookie"] == "token_v2=secret"
    assert history["User-Agent"] == inference["User-Agent"]
    assert app_call["origin"] == "https://www.notion.so"
    assert app_call["sec-fetch-site"] == "cross-site"
    assert app_call["referer"] == "https://www.notion.so/chat"


@pytest.mark.parametrize(
    ("events", "message"),
    [
        ([{"type": "stream_complete"}], "empty stream"),
        ([], "empty stream"),
        ([{"type": "content", "text": "partial"}], "completion metadata"),
    ],
)
def test_incomplete_inference_is_diagnostic_and_not_retried(
    events: list[dict[str, object]],
    message: str,
) -> None:
    client = _client()
    response = MagicMock(status_code=200, text="")
    response.headers = {"content-type": "application/x-ndjson"}
    client._scraper = MagicMock()
    client._scraper.post.return_value = response

    with (
        patch.object(client, "_build_cookie_header", return_value=""),
        patch("app.notion_client._create_notion_http_session", return_value=client._scraper),
        patch("app.notion_client.parse_stream", return_value=iter(events)),
        pytest.raises(NotionUpstreamError) as error,
    ):
        list(client.stream_response([{"type": "config", "value": {}}], thread_id="thread-1"))

    assert client._scraper.post.call_count == 1
    assert error.value.retriable is False
    assert message in error.value.args[0]
    facts = json.loads(error.value.response_excerpt)
    assert facts["http_status"] == 200
    assert facts["emitted"] is bool(events and events[0].get("type") != "stream_complete")
    assert "token" not in error.value.response_excerpt


def test_empty_inference_stream_is_not_retried_and_omits_secrets() -> None:
    client = _client()
    response = MagicMock(status_code=200, text="")
    response.headers = {
        "content-type": "application/x-ndjson; charset=utf-8",
        "content-length": "0",
        "set-cookie": "token_v2=secret",
    }
    client._scraper = MagicMock()
    client._scraper.post.return_value = response

    with (
        patch.object(client, "_build_cookie_header", return_value="token_v2=secret"),
        patch("app.notion_client._create_notion_http_session", return_value=client._scraper),
        patch(
            "app.notion_client.parse_stream",
            return_value=iter([{"type": "stream_complete"}]),
        ),
    ):
        with pytest.raises(NotionUpstreamError) as error:
            list(client.stream_response([{"type": "config", "value": {}}], thread_id="thread-1"))

    assert client._scraper.post.call_count == 1
    assert error.value.retriable is False
    facts = json.loads(error.value.response_excerpt)
    assert facts["http_status"] == 200
    assert facts["content_type"] == "application/x-ndjson"
    assert facts["content_length"] == "0"
    assert facts["emitted"] is False
    assert facts["stream_completed"] is True
    assert "secret" not in error.value.response_excerpt
    assert "token_v2" not in error.value.response_excerpt


def test_rejected_inference_is_posted_once() -> None:
    client = _client()
    response = MagicMock(status_code=403, text="forbidden")
    client._scraper = MagicMock()
    client._scraper.post.return_value = response

    with (
        patch.object(client, "_build_cookie_header", return_value=""),
        patch("app.notion_client._create_notion_http_session", return_value=client._scraper),
    ):
        with pytest.raises(NotionUpstreamError) as error:
            list(client.stream_response([{"type": "config", "value": {}}], thread_id="thread-1"))

    assert client._scraper.post.call_count == 1
    assert error.value.status_code == 403
    assert error.value.retriable is False
