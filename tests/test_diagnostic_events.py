from __future__ import annotations

import json

from app.api.chat import _record_notion_upstream_diagnostic
from app.diagnostics import CONTRACT_VERSION, EVENT_PREFIX, emit_diagnostic_event
from app.notion_client import NotionUpstreamError


def _event_from_stderr(stderr: str) -> dict:
    line = stderr.strip()
    assert line.startswith(EVENT_PREFIX)
    return json.loads(line[len(EVENT_PREFIX) :])


def test_native_diagnostic_event_is_gated_and_secret_safe(monkeypatch, capsys) -> None:
    monkeypatch.delenv("SANITYCLOUD_DIAGNOSTIC_CONTRACT_VERSION", raising=False)
    assert emit_diagnostic_event(code="TEST", message="no supervisor", operation="test") is False
    assert capsys.readouterr().err == ""

    monkeypatch.setenv("SANITYCLOUD_DIAGNOSTIC_CONTRACT_VERSION", CONTRACT_VERSION)
    assert emit_diagnostic_event(
        code="TEST_FAILURE",
        message="diagnostic test",
        operation="test_event",
        details={"token": "do-not-emit", "status_code": 502},
    ) is True
    event = _event_from_stderr(capsys.readouterr().err)
    assert event["code"] == "TEST_FAILURE"
    assert event["details"]["token"] == "[REDACTED]"
    assert event["details"]["status_code"] == 502


def test_missing_finished_at_gets_specific_diagnostic_code(monkeypatch, capsys) -> None:
    monkeypatch.setenv("SANITYCLOUD_DIAGNOSTIC_CONTRACT_VERSION", CONTRACT_VERSION)
    exc = NotionUpstreamError(
        "Notion upstream stream ended before completion metadata.",
        status_code=502,
        retriable=True,
        response_excerpt="missing_finishedAt",
    )

    _record_notion_upstream_diagnostic(
        mode="full",
        exc=exc,
        attempt=2,
        max_retries=3,
    )

    event = _event_from_stderr(capsys.readouterr().err)
    assert event["code"] == "NOTION_MISSING_FINISHED_AT"
    assert event["kind"] == "protocol_completion_failure"
    assert event["retryable"] is True
    assert event["details"]["diagnostic_marker"] == "missing_finishedAt"
    assert event["details"]["attempt"] == 2
