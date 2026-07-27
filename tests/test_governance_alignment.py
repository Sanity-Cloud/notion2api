from __future__ import annotations

from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from app.api.chat import (
    _apply_notion_request_options,
    _request_context_page_id,
    _response_model_metadata,
)
from app.api.responses import _chat_completion_to_response
from app.governance import GovernanceContract, governance_receipt_from_client
from app.mcp_server import _chat_output_from_backend, _responses_output_from_backend
from app.notion_client import NotionOpusAPI
from app.schemas import ChatCompletionRequest, ChatMessage


def _contract() -> GovernanceContract:
    return GovernanceContract(
        version="test-v1",
        teamspace_id="space-canonical",
        authority_page_id="authority-page",
        documented_output_parent_page_id="output-root",
        procedural_feedback_parent_page_id="feedback-root",
    )


def _account(**overrides) -> dict:
    payload = {
        "token_v2": "token",
        "space_id": "space-canonical",
        "user_id": "user-1",
    }
    payload.update(overrides)
    return payload


def test_contract_binds_every_account_to_one_authority_and_routing_tree() -> None:
    accounts = _contract().bind_accounts([_account(), _account(user_id="user-2")])

    assert {item["space_id"] for item in accounts} == {"space-canonical"}
    assert {item["context_page_id"] for item in accounts} == {"authority-page"}
    assert {item["repo_ai_parent_page_id"] for item in accounts} == {"output-root"}
    assert {item["procedural_feedback_parent_page_id"] for item in accounts} == {
        "feedback-root"
    }


def test_contract_rejects_teamspace_drift() -> None:
    with pytest.raises(ValueError, match="teamspace"):
        _contract().bind_accounts([_account(space_id="other-space")])


def test_contract_rejects_authority_and_output_root_drift() -> None:
    with pytest.raises(ValueError, match="context_page_id"):
        _contract().bind_accounts([_account(context_page_id="other-authority")])
    with pytest.raises(ValueError, match="repo_ai_parent_page_id"):
        _contract().bind_accounts([_account(repo_ai_parent_page_id="other-output")])


def test_client_exposes_complete_governance_receipt() -> None:
    client = NotionOpusAPI(_contract().bind_accounts([_account()])[0])

    receipt = governance_receipt_from_client(client)
    assert receipt == {
        "contract_version": "test-v1",
        "teamspace_id": "space-canonical",
        "authority_page_id": "authority-page",
        "documented_output_parent_page_id": "output-root",
        "procedural_feedback_parent_page_id": "feedback-root",
        "aligned": True,
    }


def test_request_context_cannot_replace_canonical_authority() -> None:
    client = NotionOpusAPI(_contract().bind_accounts([_account()])[0])
    request = ChatCompletionRequest(
        model="terra",
        messages=[ChatMessage(role="user", content="hello")],
        metadata={"context_page_id": "other-authority"},
    )

    with pytest.raises(HTTPException) as exc_info:
        _request_context_page_id(request, client)
    assert exc_info.value.status_code == 409


def test_governance_instruction_precedes_provider_specific_instruction() -> None:
    client = NotionOpusAPI(_contract().bind_accounts([_account()])[0])
    request = ChatCompletionRequest(
        model="terra",
        messages=[ChatMessage(role="user", content="hello")],
        notion_instructions="Use the project glossary.",
    )
    transcript = [{"type": "config", "value": {}}]

    _apply_notion_request_options(transcript, request, client)

    instructions = transcript[0]["value"]["ephemeralInstructions"]
    assert "authority-page" in instructions
    assert "output-root" in instructions
    assert "feedback-root" in instructions
    assert instructions.endswith("Use the project glossary.")


def test_governance_receipt_is_preserved_in_provider_output_metadata() -> None:
    request_metadata = {
        "governance": {
            "contract_version": "test-v1",
            "teamspace_id": "space-canonical",
            "authority_page_id": "authority-page",
            "documented_output_parent_page_id": "output-root",
            "procedural_feedback_parent_page_id": "feedback-root",
            "aligned": True,
        }
    }

    metadata = _response_model_metadata("terra", {}, request_metadata)

    assert metadata["governance"]["authority_page_id"] == "authority-page"
    assert metadata["governance"]["aligned"] is True


def test_responses_projection_keeps_governance_receipt() -> None:
    payload = {
        "model": "terra",
        "model_metadata": {
            "governance": {
                "contract_version": "test-v1",
                "authority_page_id": "authority-page",
                "aligned": True,
            }
        },
        "choices": [{"message": {"content": "done"}}],
    }

    response = _chat_completion_to_response(payload, "terra")

    assert response["governance"]["authority_page_id"] == "authority-page"
    assert response["model_metadata"]["governance"]["aligned"] is True


def test_mcp_chat_and_responses_outputs_keep_governance_receipt() -> None:
    backend = {
        "ok": True,
        "model": "terra",
        "model_metadata": {
            "governance": {
                "contract_version": "test-v1",
                "authority_page_id": "authority-page",
                "aligned": True,
            }
        },
        "choices": [{"message": {"content": "done"}}],
        "output": [{"content": [{"type": "output_text", "text": "done"}]}],
    }
    client = SimpleNamespace(base_url="http://127.0.0.1:8120", timeout=900)

    chat = _chat_output_from_backend(
        data=backend,
        client=client,
        model="terra",
        session_key="session",
        conversation_id="conversation",
        session_created=True,
        request_id="request",
        wait_seconds=0,
    )
    responses = _responses_output_from_backend(
        data=backend,
        client=client,
        model="terra",
        provenance={},
    )

    assert chat["governance"]["authority_page_id"] == "authority-page"
    assert responses["governance"]["authority_page_id"] == "authority-page"


def test_runtime_binding_rejects_unaligned_provider_account() -> None:
    with pytest.raises(ValueError, match="teamspace"):
        _contract().bind_accounts([_account(space_id="sandbox-space")])


@pytest.mark.parametrize(
    "model",
    [
        "terra",
        "orchid-muffin",
        "claude-sonnet4.6",
        "gpt-5.6-sol",
        "glm-5.2",
    ],
)
def test_all_provider_routes_receive_the_same_governance_contract(model: str) -> None:
    client = NotionOpusAPI(_contract().bind_accounts([_account()])[0])
    request = ChatCompletionRequest(
        model=model,
        messages=[ChatMessage(role="user", content="hello")],
    )
    transcript = [{"type": "config", "value": {}}]

    _apply_notion_request_options(transcript, request, client)

    instructions = transcript[0]["value"]["ephemeralInstructions"]
    assert "space-canonical" in instructions
    assert "authority-page" in instructions
    assert "output-root" in instructions
    assert "feedback-root" in instructions
