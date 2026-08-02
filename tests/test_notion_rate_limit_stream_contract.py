from __future__ import annotations

import asyncio
import json
from unittest.mock import MagicMock, patch

import httpx
import pytest

from app import mcp_server
from app.api.chat_history import _history_upstream_http_exception
from app.notion_client import NotionOpusAPI, NotionUpstreamError
from app.stream_parser import parse_stream


class _NdjsonResponse:
    def __init__(self, events: list[dict[str, object]]):
        self.events = events

    def iter_lines(self, decode_unicode: bool = True):
        del decode_unicode
        for event in self.events:
            yield json.dumps(event)


class _JsonResponse:
    def __init__(self, status_code: int, payload: dict[str, object] | None = None, text: str = ""):
        self.status_code = status_code
        self.payload = payload or {}
        self.text = text

    def json(self) -> dict[str, object]:
        return self.payload


class _Scraper:
    def __init__(self, responses: list[_JsonResponse]):
        self.responses = list(responses)
        self.calls: list[dict[str, object]] = []

    def post(self, url: str, **kwargs):
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError("Unexpected additional upstream request")
        return self.responses.pop(0)


def _client() -> NotionOpusAPI:
    return NotionOpusAPI({"token_v2": "token", "space_id": "space", "user_id": "user"})


def _install_stream_transport(monkeypatch, body: bytes) -> None:
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=body,
        )
    )
    real_client = httpx.AsyncClient

    class TestAsyncClient(real_client):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(mcp_server.httpx, "AsyncClient", TestAsyncClient)


def test_ndjson_parser_preserves_explicit_rate_limit_error() -> None:
    events = list(
        parse_stream(
            _NdjsonResponse(
                [
                    {
                        "type": "error",
                        "error": {
                            "code": "too_many_requests",
                            "message": "Too many requests. Try again later.",
                        },
                    }
                ]
            )
        )
    )

    assert events == [
        {
            "type": "upstream_error",
            "status_code": 429,
            "code": "too_many_requests",
            "message": "Too many requests. Try again later.",
            "retriable": True,
            "raw_type": "error",
        }
    ]


def test_stream_response_raises_rate_limit_instead_of_missing_finished_at() -> None:
    client = _client()
    response = MagicMock(status_code=200, text="")
    scraper = MagicMock()
    scraper.cookies = MagicMock()
    scraper.post.return_value = response
    transcript = [{"id": "config", "type": "config", "value": {"type": "workflow"}}]

    with (
        patch("app.notion_client.cloudscraper.create_scraper", return_value=scraper),
        patch(
            "app.notion_client.parse_stream",
            return_value=iter(
                [
                    {
                        "type": "upstream_error",
                        "status_code": 429,
                        "code": "too_many_requests",
                        "message": "Too many requests.",
                        "retriable": True,
                        "raw_type": "error",
                    }
                ]
            ),
        ),
        pytest.raises(NotionUpstreamError) as captured,
    ):
        list(client.stream_response(transcript, thread_id="thread-1", persist_remote_chat=True))

    assert captured.value.status_code == 429
    assert captured.value.retriable is True
    assert "too_many_requests" in captured.value.response_excerpt
    assert "missing_finishedAt" not in captured.value.response_excerpt


def test_history_429_stops_payload_shape_probing() -> None:
    client = _client()
    scraper = _Scraper([_JsonResponse(429, text="Too many requests")])
    client._scraper = scraper

    with pytest.raises(NotionUpstreamError) as captured:
        client.fetch_chat_history(limit=100, max_pages=1)

    assert captured.value.status_code == 429
    assert len(scraper.calls) == 1


def test_history_schema_mismatch_can_fall_back_once() -> None:
    client = _client()
    scraper = _Scraper(
        [
            _JsonResponse(400, text="unknown field"),
            _JsonResponse(200, payload={"recordMap": {"thread": {}}}),
        ]
    )
    client._scraper = scraper

    result = client.fetch_chat_history(limit=25, max_pages=1)

    assert result == {"recordMap": {"thread": {}}}
    assert len(scraper.calls) == 2
    request_ids = [str(call["json"]["requestId"]) for call in scraper.calls]
    assert request_ids[0] != request_ids[1]


def test_history_api_preserves_upstream_429_contract() -> None:
    exc = NotionUpstreamError(
        "Notion chat history returned HTTP 429.",
        status_code=429,
        retriable=True,
        response_excerpt="Too many requests",
    )

    error = _history_upstream_http_exception(exc, action="sync")

    assert error.status_code == 429
    assert error.detail["error"]["code"] == "NOTION_429"
    assert error.detail["error"]["type"] == "upstream_rate_limit"
    assert error.detail["error"]["upstream_status_code"] == 429


def test_mcp_stream_preserves_explicit_rate_limit_event(monkeypatch) -> None:
    event = {
        "type": "error",
        "error": {
            "code": "NOTION_429",
            "message": "Too many requests.",
            "status_code": 429,
        },
    }
    body = f"data: {json.dumps(event)}\n\n".encode("utf-8")
    _install_stream_transport(monkeypatch, body)

    updates: list[tuple[object, ...]] = []
    result = asyncio.run(
        mcp_server.Notion2APIClient("http://test").post_chat_stream(
            "/v1/chat/completions",
            {"model": "test-model"},
            lambda *args: updates.append(args),
        )
    )

    assert result["ok"] is False
    assert result["status_code"] == 429
    assert result["status"] == "upstream_rate_limit"
    assert result["error"]["code"] == "NOTION_429"
    assert updates[-1][1] == ""


def test_mcp_stream_rejects_truncated_success(monkeypatch) -> None:
    event = {
        "model": "test-model",
        "choices": [{"delta": {"content": "partial"}, "finish_reason": None}],
    }
    body = f"data: {json.dumps(event)}\n\n".encode("utf-8")
    _install_stream_transport(monkeypatch, body)

    result = asyncio.run(
        mcp_server.Notion2APIClient("http://test").post_chat_stream(
            "/v1/chat/completions",
            {"model": "test-model"},
            lambda *_args: None,
        )
    )

    assert result["ok"] is False
    assert result["status_code"] == 502
    assert result["status"] == "stream_incomplete"
    assert result["error"]["code"] == "STREAM_INCOMPLETE"
    assert result["error"]["partial_content_chars"] == len("partial")
