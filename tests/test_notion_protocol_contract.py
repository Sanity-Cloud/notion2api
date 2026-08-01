from unittest.mock import MagicMock, patch
import json
from types import SimpleNamespace

from app.conversation import apply_notion_ai_options, build_standard_transcript
from app.notion_client import NOTION_CLIENT_VERSION, NotionOpusAPI
from app.stream_parser import parse_stream
from app.unsafe_url_continuation import (
    allow_pending_unsafe_urls_once,
    discover_pending_unsafe_url_steps,
    find_pending_unsafe_url_steps,
    get_remembered_unsafe_url_steps,
    remember_pending_unsafe_url_steps,
)


def test_default_client_version_matches_captured_protocol():
    assert NOTION_CLIENT_VERSION == "23.13.20260623.1532"


def test_workflow_request_uses_patch_protocol_v2():
    client = NotionOpusAPI(
        {
            "user_id": "user-1",
            "space_id": "space-1",
            "token_v2": "token",
        }
    )
    response = MagicMock(status_code=200)
    client._scraper = MagicMock()
    client._scraper.post.return_value = response

    profile = {
        "precreate_thread": False,
        "create_thread": False,
        "is_partial_transcript": True,
        "include_debug_overrides": False,
    }
    transcript = [
        {
            "id": "config-1",
            "type": "config",
            "value": {"type": "workflow", "model": "gpt-5.5"},
        }
    ]

    with (
        patch.object(client, "_resolve_request_profile", return_value=profile),
        patch.object(client, "_build_cookie_header", return_value=""),
        patch("app.notion_client.cloudscraper.create_scraper", return_value=client._scraper),
        patch("app.notion_client.parse_stream", return_value=iter([
            {"type": "content", "text": "ok"},
            {"type": "stream_complete"},
        ])),
    ):
        assert list(
            client.stream_response(
                transcript,
                thread_id="thread-1",
                persist_remote_chat=True,
            )
        ) == [{"type": "content", "text": "ok"}]

    request = client._scraper.post.call_args.kwargs
    assert request["headers"]["Accept"] == "application/x-ndjson"
    assert request["json"]["asPatchResponse"] is True
    assert request["json"]["patchResponseVersion"] == 2
    assert request["json"]["createdSource"] == "workflows"


def test_standard_transcript_injects_timezone_and_instruction_page():
    transcript = build_standard_transcript(
        [{"role": "user", "content": "hello"}],
        "gpt-5.5",
        {
            "user_id": "user-1",
            "space_id": "space-1",
            "timezone": "America/Chicago",
            "context_page_id": "37abf4af-15b3-80ba-bd7d-ff1a5bb018ca",
        },
    )

    context = next(item for item in transcript if item["type"] == "context")
    assert context["value"]["timezone"] == "America/Chicago"
    assert (
        context["value"]["context_page_id"]
        == "37abf4af-15b3-80ba-bd7d-ff1a5bb018ca"
    )


def test_finds_only_pending_web_load_confirmations():
    pending = {
        "id": "step-pending",
        "type": "agent-tool-result",
        "state": "confirmation:requested",
        "input": {
            "function": "connections.web.loadPage",
            "args": {"url": "https://www.revisor.mn.gov/statutes/"},
        },
    }
    applied = {
        **pending,
        "id": "step-applied",
        "state": "applied",
        "requestedConfirmation": True,
        "result": {"output": "done"},
    }

    assert find_pending_unsafe_url_steps({"pending": pending, "applied": applied}) == [
        {
            "tool_step_id": "step-pending",
            "urls": ["https://www.revisor.mn.gov/statutes/"],
        }
    ]


def test_continue_payload_uses_har_confirm_tool_step_ids():
    client = NotionOpusAPI({"token_v2": "token", "space_id": "space", "user_id": "user"})
    response = MagicMock(status_code=200, text="")
    client._scraper = MagicMock()
    client._scraper.post.return_value = response

    with patch(
        "app.notion_client.parse_stream",
        return_value=iter([
            {
                "type": "tool_result_status",
                "tool_step_id": "step-1",
                "state": "applied",
                "has_result": True,
            },
            {"type": "stream_complete"},
        ]),
    ):
        result = client.continue_confirmed_tool_steps(
            thread_id="thread-1",
            tool_step_ids=["step-1", "step-1"],
        )

    request = client._scraper.post.call_args.kwargs["json"]
    assert request["threadId"] == "thread-1"
    assert request["confirmToolStepIds"] == ["step-1"]
    assert request["transcript"] == []
    assert request["createThread"] is False
    assert request["isPartialTranscript"] is True
    assert request["createdSource"] == "workflows"
    assert request["supportsCustomAgentNudgeTranscriptStep"] is True
    assert result["stream_completed"] is True
    assert result["approved"] is True
    assert result["applied_tool_step_ids"] == ["step-1"]
    assert result["unresolved_tool_step_ids"] == []


def test_discovery_scans_raw_hydrated_message_records():
    client = MagicMock(space_id="space-1")
    thread_response = {
        "recordMap": {
            "thread": {
                "thread-1": {"value": {"value": {"messages": ["step-pending"]}}}
            }
        }
    }
    message_response = {
        "recordMap": {
            "thread_message": {
                "step-pending": {
                    "value": {
                        "value": {
                            "step": {
                                "id": "step-pending",
                                "type": "agent-tool-result",
                                "state": "confirmation:requested",
                                "input": {
                                    "function": "connections.web.loadPage",
                                    "args": {"url": "https://www.revisor.mn.gov/statutes/"},
                                },
                            }
                        }
                    }
                }
            }
        }
    }

    with patch(
        "app.unsafe_url_continuation._post_json",
        side_effect=[thread_response, message_response],
    ):
        assert discover_pending_unsafe_url_steps(client, "thread-1") == [
            {
                "tool_step_id": "step-pending",
                "urls": ["https://www.revisor.mn.gov/statutes/"],
            }
        ]


def test_notion_ai_mcp_options_map_to_workflow_config():
    transcript = [{"type": "config", "value": {"type": "workflow", "model": "orchid-muffin"}}]

    apply_notion_ai_options(
        transcript,
        mode="ask",
        task="create_slides",
        sources=["notion", "github", "web"],
        web_access=True,
        persona="analyst",
        instructions="Use the attached brand guidance.",
    )

    config = transcript[0]["value"]
    assert config["useReadOnlyMode"] is True
    assert config["isAgentResearchRequest"] is False
    assert config["searchScopes"] == [{"type": "notion"}, {"type": "github"}, {"type": "web"}]
    assert config["availableConnectors"] == ["github"]
    assert config["useWebSearch"] is True
    assert config["enableComputer"] is True
    assert config["enableAgentGenerateImage"] is True
    assert "structured, logical" in config["ephemeralInstructions"]
    assert "brand guidance" in config["ephemeralInstructions"]


class _FakeNdjsonResponse:
    def __init__(self, lines):
        self._lines = lines

    def iter_lines(self, decode_unicode=True):
        del decode_unicode
        for line in self._lines:
            yield json.dumps(line)


def test_stream_parser_captures_transient_unsafe_url_confirmation():
    step = {
        "id": "step-live",
        "type": "agent-tool-result",
        "state": "confirmation:requested",
        "requestedConfirmation": True,
        "pendingConfirmations": [
            {"type": "urlSafety", "urls": ["www.revisor.mn.gov/**"]}
        ],
        "input": {
            "function": "connections.web.loadPage",
            "args": {"url": "https://www.revisor.mn.gov/statutes/"},
        },
    }
    response = _FakeNdjsonResponse([
        {"type": "patch", "v": [{"o": "a", "p": "/s/-", "v": step}]}
    ])

    events = list(parse_stream(response))
    event = next(item for item in events if item.get("type") == "tool_confirmation")

    assert event["tool_step_ids"] == ["step-live"]
    assert event["urls"] == ["www.revisor.mn.gov/**"]


def test_allow_once_prefers_live_stream_registry_over_hydration():
    client = SimpleNamespace(account_key="acct-1", user_id="user-1", space_id="space-1")
    client.continue_confirmed_tool_steps = lambda **kwargs: {
        "approved": True,
        "stream_completed": True,
        "reason": "approved",
        **kwargs,
    }
    remember_pending_unsafe_url_steps(
        client,
        "thread-live",
        [{"tool_step_id": "step-live", "urls": ["www.revisor.mn.gov/**"]}],
    )

    with patch(
        "app.unsafe_url_continuation.discover_pending_unsafe_url_steps",
        side_effect=AssertionError("hydration must not run for a captured live step"),
    ):
        result = allow_pending_unsafe_urls_once(client, thread_id="thread-live")

    assert result["ok"] is True
    assert result["approved"] is True
    assert result["tool_step_ids"] == ["step-live"]
    assert get_remembered_unsafe_url_steps(client, "thread-live") == []


def test_continue_does_not_claim_approval_without_applied_step_evidence():
    client = NotionOpusAPI({"token_v2": "token", "space_id": "space", "user_id": "user"})
    response = MagicMock(status_code=200, text="")
    client._scraper = MagicMock()
    client._scraper.post.return_value = response

    with patch(
        "app.notion_client.parse_stream",
        return_value=iter([{"type": "stream_complete"}]),
    ):
        result = client.continue_confirmed_tool_steps(
            thread_id="thread-1",
            tool_step_ids=["step-unproven"],
        )

    assert result["approved"] is False
    assert result["reason"] == "confirmation_not_applied"
    assert result["applied_tool_step_ids"] == []
    assert result["unresolved_tool_step_ids"] == ["step-unproven"]
