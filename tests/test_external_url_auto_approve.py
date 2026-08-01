from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from app.notion_client import NotionOpusAPI, NotionUpstreamError
from app.unsafe_url_continuation import (
    auto_approve_external_url_confirmation,
    claim_external_url_auto_approval,
    external_url_approval_policy,
    get_external_url_audit_receipts,
    reset_external_url_approval_state_for_tests,
    should_auto_continue_external_url_confirmations,
)


@pytest.fixture(autouse=True)
def _reset_external_url_state(monkeypatch):
    monkeypatch.setenv("EXTERNAL_URL_APPROVAL_POLICY", "allow_all")
    monkeypatch.setenv("AUTO_CONTINUE_EXTERNAL_URL_CONFIRMATIONS", "true")
    reset_external_url_approval_state_for_tests()
    yield
    reset_external_url_approval_state_for_tests()


def test_policy_defaults_to_manual_and_no_auto_continue(monkeypatch):
    monkeypatch.delenv("EXTERNAL_URL_APPROVAL_POLICY", raising=False)
    monkeypatch.delenv("Notion__ExternalUrlApprovalPolicy", raising=False)
    monkeypatch.delenv("AUTO_CONTINUE_EXTERNAL_URL_CONFIRMATIONS", raising=False)
    monkeypatch.delenv("Notion__AutoContinueExternalUrlConfirmations", raising=False)
    assert external_url_approval_policy() == "manual"
    assert should_auto_continue_external_url_confirmations() is False


def test_mn_gov_and_arbitrary_domains_are_auto_approved_without_hostname_filter():
    client = SimpleNamespace(account_key="acct-1", user_id="user-1", space_id="space-1")
    calls: list[dict] = []

    def _continue(**kwargs):
        calls.append(kwargs)
        return {
            "approved": True,
            "stream_completed": True,
            "reason": "approved",
            "trace_id": "trace-1",
            "event_count": 2,
            "applied_tool_step_ids": list(kwargs["tool_step_ids"]),
            "unresolved_tool_step_ids": [],
            "tool_step_ids": list(kwargs["tool_step_ids"]),
            "urls": ["https://www.revisor.mn.gov/statutes/", "https://example.com/x"],
        }

    client.continue_confirmed_tool_steps = _continue

    first = auto_approve_external_url_confirmation(
        client,
        thread_id="thread-1",
        tool_step_id="step-mn",
        urls=["https://www.revisor.mn.gov/statutes/"],
    )
    second = auto_approve_external_url_confirmation(
        client,
        thread_id="thread-1",
        tool_step_id="step-other",
        urls=["https://totally-unrelated.example.org/path"],
    )

    assert first["approved"] is True
    assert first["state"] == "external_url_auto_approved"
    assert second["approved"] is True
    assert second["state"] == "external_url_auto_approved"
    assert calls[0]["thread_id"] == "thread-1"
    assert calls[1]["thread_id"] == "thread-1"
    assert calls[0]["tool_step_ids"] == ["step-mn"]
    assert calls[1]["tool_step_ids"] == ["step-other"]


def test_multiple_and_mixed_urls_approved_without_manual_review():
    client = SimpleNamespace(account_key="acct-1", user_id="user-1", space_id="space-1")
    client.continue_confirmed_tool_steps = lambda **kwargs: {
        "approved": True,
        "stream_completed": True,
        "reason": "approved",
        "trace_id": "trace-mixed",
        "event_count": 1,
        "applied_tool_step_ids": list(kwargs["tool_step_ids"]),
        "unresolved_tool_step_ids": [],
        "tool_step_ids": list(kwargs["tool_step_ids"]),
        "urls": [
            "https://www.revisor.mn.gov/statutes/",
            "https://www.example.com/a",
            "https://news.ycombinator.com/",
        ],
    }

    result = auto_approve_external_url_confirmation(
        client,
        thread_id="thread-mix",
        tool_step_id="step-mix",
        urls=[
            "https://www.revisor.mn.gov/statutes/",
            "https://www.example.com/a",
            "https://news.ycombinator.com/",
        ],
    )

    assert result["approved"] is True
    assert result["url_count"] == 3
    assert result["error_type"] is None
    assert "waiting_for_user_approval" not in str(result)


def test_duplicate_tool_step_events_produce_one_approval_call():
    client = SimpleNamespace(account_key="acct-1", user_id="user-1", space_id="space-1")
    calls: list[dict] = []

    def _continue(**kwargs):
        calls.append(kwargs)
        return {
            "approved": True,
            "stream_completed": True,
            "reason": "approved",
            "trace_id": "trace-dup",
            "event_count": 1,
            "applied_tool_step_ids": list(kwargs["tool_step_ids"]),
            "unresolved_tool_step_ids": [],
            "tool_step_ids": list(kwargs["tool_step_ids"]),
            "urls": ["https://www.revisor.mn.gov/statutes/"],
        }

    client.continue_confirmed_tool_steps = _continue

    first = auto_approve_external_url_confirmation(
        client,
        thread_id="thread-dup",
        tool_step_id="step-dup",
        urls=["https://www.revisor.mn.gov/statutes/"],
    )
    second = auto_approve_external_url_confirmation(
        client,
        thread_id="thread-dup",
        tool_step_id="step-dup",
        urls=["https://www.revisor.mn.gov/statutes/"],
    )

    assert first["approved"] is True
    assert second["duplicate"] is True
    assert len(calls) == 1


def test_approval_failure_is_operational_error_not_access_denial():
    client = SimpleNamespace(account_key="acct-1", user_id="user-1", space_id="space-1")
    client.continue_confirmed_tool_steps = lambda **kwargs: {
        "approved": False,
        "stream_completed": True,
        "reason": "confirmation_not_applied",
        "trace_id": "trace-fail",
        "event_count": 1,
        "applied_tool_step_ids": [],
        "unresolved_tool_step_ids": list(kwargs["tool_step_ids"]),
        "tool_step_ids": list(kwargs["tool_step_ids"]),
        "urls": ["https://example.com"],
    }

    result = auto_approve_external_url_confirmation(
        client,
        thread_id="thread-fail",
        tool_step_id="step-fail",
        urls=["https://example.com"],
    )

    assert result["approved"] is False
    assert result["state"] == "external_url_approval_failed"
    assert result["error_type"] == "operational_error"
    assert "denied" not in str(result.get("reason") or "").lower()
    assert "policy" not in str(result.get("reason") or "").lower()


def test_audit_receipt_includes_required_fields():
    client = SimpleNamespace(account_key="acct-audit", user_id="user-1", space_id="space-1")
    client.continue_confirmed_tool_steps = lambda **kwargs: {
        "approved": True,
        "stream_completed": True,
        "reason": "approved",
        "trace_id": "trace-audit",
        "event_count": 3,
        "applied_tool_step_ids": list(kwargs["tool_step_ids"]),
        "unresolved_tool_step_ids": [],
        "tool_step_ids": list(kwargs["tool_step_ids"]),
        "urls": ["https://www.revisor.mn.gov/statutes/", "https://example.com"],
    }

    result = auto_approve_external_url_confirmation(
        client,
        thread_id="thread-audit",
        tool_step_id="step-audit",
        urls=["https://www.revisor.mn.gov/statutes/", "https://example.com"],
    )
    receipts = get_external_url_audit_receipts(limit=5)
    receipt = result["receipt"]

    assert receipt["tool_step_id"] == "step-audit"
    assert receipt["thread_id"] == "thread-audit"
    assert receipt["url_count"] == 2
    assert receipt["approval_result"] == "approved"
    assert receipt["continuation_result"]["approved"] is True
    assert receipts[-1]["receipt_id"] == receipt["receipt_id"]


def test_continue_payload_stays_on_same_thread_without_restaging_or_new_inference():
    client = NotionOpusAPI({"token_v2": "token", "space_id": "space", "user_id": "user"})
    client._scraper = MagicMock()
    client._scraper.post.return_value = MagicMock(status_code=200, text="")

    with patch(
        "app.notion_client.parse_stream",
        return_value=iter(
            [
                {
                    "type": "tool_result_status",
                    "tool_step_id": "step-1",
                    "state": "applied",
                    "has_result": True,
                },
                {"type": "stream_complete"},
            ]
        ),
    ):
        result = client.continue_confirmed_tool_steps(
            thread_id="thread-same",
            tool_step_ids=["step-1"],
        )

    payload = client._scraper.post.call_args.kwargs["json"]
    assert payload["threadId"] == "thread-same"
    assert payload["createThread"] is False
    assert payload["transcript"] == []
    assert payload["confirmToolStepIds"] == ["step-1"]
    assert "attachments" not in payload
    assert result["approved"] is True


def test_stream_response_auto_approves_without_waiting_for_user():
    client = NotionOpusAPI({"token_v2": "token", "space_id": "space", "user_id": "user"})
    client.account_key = "acct-stream"
    response = MagicMock(status_code=200, text="")
    scraper = MagicMock()
    scraper.post.return_value = response

    confirmation = {
        "type": "tool_confirmation",
        "confirmation_type": "potentially_unsafe_url",
        "tool_step_id": "step-live",
        "tool_step_ids": ["step-live"],
        "urls": ["https://www.revisor.mn.gov/statutes/", "https://example.com/docs"],
    }

    def _iter_continue(*, thread_id, tool_step_ids, summary=None):
        assert thread_id == "thread-live"
        assert tool_step_ids == ["step-live"]
        if summary is not None:
            summary.clear()
            summary.update(
                {
                    "approved": True,
                    "stream_completed": True,
                    "reason": "approved",
                    "trace_id": "trace-live",
                    "event_count": 2,
                    "applied_tool_step_ids": ["step-live"],
                    "unresolved_tool_step_ids": [],
                    "tool_step_ids": ["step-live"],
                }
            )
        yield {
            "type": "tool_result_status",
            "tool_step_id": "step-live",
            "state": "applied",
            "has_result": True,
        }
        yield {"type": "content", "text": "grounded review complete"}

    with (
        patch.object(
            client,
            "_resolve_request_profile",
            return_value={
                "precreate_thread": False,
                "create_thread": False,
                "is_partial_transcript": True,
                "include_debug_overrides": False,
            },
        ),
        patch.object(client, "_build_cookie_header", return_value=""),
        patch.object(client, "_to_notion_transcript", side_effect=lambda value: value),
        patch.object(client, "_resolve_thread_type", return_value="workflow"),
        patch("app.notion_client.cloudscraper") as cloudscraper_mod,
        patch("app.notion_client.parse_stream", return_value=iter([confirmation])),
        patch.object(client, "iter_continue_confirmed_tool_steps", side_effect=_iter_continue),
        patch("app.notion_client.validate_bound_thread_transcript", return_value=None),
    ):
        cloudscraper_mod.create_scraper.return_value = scraper
        events = list(
            client.stream_response(
                transcript=[{"type": "user", "value": "research mn.gov"}],
                thread_id="thread-live",
            )
        )

    types = [event.get("type") for event in events if isinstance(event, dict)]
    assert "external_url_confirmation_received" in types
    assert "external_url_auto_approved" in types
    assert "waiting_for_user_approval" not in types
    assert any(event.get("type") == "content" for event in events)
    approved = next(event for event in events if event.get("type") == "external_url_auto_approved")
    assert approved["url_count"] == 2
    assert approved["thread_id"] == "thread-live"


def test_claim_requires_usable_tool_step_id():
    client = SimpleNamespace(account_key="acct-1", user_id="user-1", space_id="space-1")
    claim = claim_external_url_auto_approval(
        client,
        thread_id="thread-1",
        tool_step_id="",
        urls=["https://example.com"],
    )
    assert claim["claimed"] is False
    assert claim["reason"] == "missing_tool_step_id"


def test_stream_auto_approve_failure_raises_operational_error():
    client = NotionOpusAPI({"token_v2": "token", "space_id": "space", "user_id": "user"})
    client.account_key = "acct-fail"
    response = MagicMock(status_code=200, text="")
    scraper = MagicMock()
    scraper.post.return_value = response
    confirmation = {
        "type": "tool_confirmation",
        "tool_step_id": "step-fail",
        "tool_step_ids": ["step-fail"],
        "urls": ["https://example.com"],
    }

    def _iter_continue(*, thread_id, tool_step_ids, summary=None):
        if summary is not None:
            summary.clear()
            summary.update(
                {
                    "approved": False,
                    "stream_completed": True,
                    "reason": "confirmation_not_applied",
                    "trace_id": "trace-fail",
                    "event_count": 1,
                    "applied_tool_step_ids": [],
                    "unresolved_tool_step_ids": ["step-fail"],
                    "tool_step_ids": ["step-fail"],
                }
            )
        return iter(())

    with (
        patch.object(
            client,
            "_resolve_request_profile",
            return_value={
                "precreate_thread": False,
                "create_thread": False,
                "is_partial_transcript": True,
                "include_debug_overrides": False,
            },
        ),
        patch.object(client, "_build_cookie_header", return_value=""),
        patch.object(client, "_to_notion_transcript", side_effect=lambda value: value),
        patch.object(client, "_resolve_thread_type", return_value="workflow"),
        patch("app.notion_client.cloudscraper") as cloudscraper_mod,
        patch("app.notion_client.parse_stream", return_value=iter([confirmation])),
        patch.object(client, "iter_continue_confirmed_tool_steps", side_effect=_iter_continue),
        patch("app.notion_client.validate_bound_thread_transcript", return_value=None),
    ):
        cloudscraper_mod.create_scraper.return_value = scraper
        with pytest.raises(NotionUpstreamError) as exc_info:
            list(
                client.stream_response(
                    transcript=[{"type": "user", "value": "research"}],
                    thread_id="thread-fail",
                )
            )

    assert "auto-approval continuation failed" in str(exc_info.value)
    assert "external_url_approval_failed" in (exc_info.value.response_excerpt or "")
