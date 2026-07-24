from __future__ import annotations

from types import SimpleNamespace

from app.api.chat import _attach_response_model_metadata, _response_model_metadata
from app.mcp_server import _chat_output_from_backend, _model_identity_trace
from app.schemas import ChatCompletionResponse, ChatMessage, ChatMessageResponseChoice


def _response(model: str = "requested-route") -> ChatCompletionResponse:
    return ChatCompletionResponse(
        id="response-1",
        model=model,
        choices=[
            ChatMessageResponseChoice(
                message=ChatMessage(role="assistant", content="done")
            )
        ],
    )


def test_echoed_model_route_is_observed_but_not_verified() -> None:
    metadata = _response_model_metadata(
        "terra",
        {
            "notion_requested_model": "orange-mousse",
            "actual_model": "orange-mousse",
        },
    )

    assert metadata["resolved_model"] == "orange-mousse"
    assert metadata["actual_model"] == "orange-mousse"
    assert metadata["actual_model_verified"] is False
    assert metadata["model_identity_confidence"] == "observed"
    assert "verified_model" not in metadata


def test_mismatched_upstream_model_is_recorded_as_verified_substitution() -> None:
    metadata = _response_model_metadata(
        "terra",
        {
            "notion_requested_model": "orange-mousse",
            "actual_model": "baseten-glm-5.2",
            "actual_model_source": "notion_step_model_mismatch",
        },
    )

    assert metadata["verified_model"] == "baseten-glm-5.2"
    assert metadata["model_identity_verified"] is True
    assert metadata["model_identity_confidence"] == "verified"
    assert metadata["model_substitution"]["resolved_model"] == "orange-mousse"
    assert metadata["model_substitution"]["responding_model"] == "baseten-glm-5.2"


def test_unverified_observation_does_not_replace_response_model() -> None:
    response = _response()

    _attach_response_model_metadata(
        response,
        "terra",
        {
            "notion_requested_model": "orange-mousse",
            "actual_model": "orange-mousse",
        },
    )

    assert response.model == "orange-mousse"
    assert response.actual_model == "orange-mousse"
    assert response.model_metadata["model_identity_verified"] is False
    assert "verified_model" not in response.model_metadata


def test_verified_responder_replaces_response_model() -> None:
    response = _response()

    _attach_response_model_metadata(
        response,
        "terra",
        {
            "notion_requested_model": "orange-mousse",
            "actual_model": "baseten-glm-5.2",
            "actual_model_source": "notion_step_model_mismatch",
        },
    )

    assert response.model == "baseten-glm-5.2"
    assert response.model_metadata["verified_model"] == "baseten-glm-5.2"


def test_caller_and_model_chain_are_returned_by_mcp_output() -> None:
    client = SimpleNamespace(base_url="http://127.0.0.1:8120", timeout=900)
    data = {
        "ok": True,
        "model": "orange-mousse",
        "actual_model": "orange-mousse",
        "model_metadata": {
            "requested_model": "terra",
            "notion_requested_model": "orange-mousse",
            "actual_model": "orange-mousse",
            "actual_model_verified": False,
            "actual_model_source": "notion_model_name_observation",
        },
        "request_metadata": {
            "caller": {
                "id": "repoai:run-1:model-1",
                "type": "repoai",
                "run_id": "run-1",
                "team_id": "TEAM-A",
            }
        },
        "choices": [{"message": {"content": "done"}}],
    }

    output = _chat_output_from_backend(
        data=data,
        client=client,
        model="terra",
        session_key="team-a",
        conversation_id="conversation-a",
        session_created=True,
        request_id="request-a",
        wait_seconds=0,
    )

    assert output["caller_id"] == "repoai:run-1:model-1"
    assert output["caller_type"] == "repoai"
    assert output["caller_metadata"]["team_id"] == "TEAM-A"
    assert output["requested_model"] == "terra"
    assert output["resolved_model"] == "orange-mousse"
    assert output["actual_model"] == "orange-mousse"
    assert output["verified_model"] == ""
    assert output["model_identity_verified"] is False
    assert output["model_identity_confidence"] == "observed"


def test_mcp_identity_trace_requires_explicit_verification() -> None:
    trace = _model_identity_trace(
        {
            "actual_model": "orange-mousse",
            "model_metadata": {
                "notion_requested_model": "orange-mousse",
                "actual_model": "orange-mousse",
                "actual_model_verified": False,
            },
        },
        "terra",
    )

    assert trace["resolved_model"] == "orange-mousse"
    assert trace["verified_model"] == ""
    assert trace["model_identity_confidence"] == "observed"


def test_terra_alias_resolution_is_not_reported_as_substitution() -> None:
    metadata = _response_model_metadata(
        "terra",
        {
            "actual_model": "orchid-muffin",
            "actual_model_source": "notion_model_name_observation",
        },
    )

    assert metadata["requested_model"] == "terra"
    assert metadata["resolved_model"] == "orchid-muffin"
    assert metadata["alias_resolution"]["display_name"] == "GPT-5.6 Terra"
    assert metadata["model_route_disposition"] == "alias_resolution"
    assert metadata["model_identity_verified"] is False
    assert "model_substitution" not in metadata


def test_mcp_trace_preserves_terra_alias_without_substitution() -> None:
    trace = _model_identity_trace(
        {
            "actual_model": "orchid-muffin",
            "model_metadata": {
                "requested_model": "terra",
                "actual_model": "orchid-muffin",
                "actual_model_verified": False,
            },
        },
        "terra",
    )

    assert trace["requested_model"] == "terra"
    assert trace["resolved_model"] == "orchid-muffin"
    assert trace["alias_resolution"]["resolution_kind"] == "configured_alias"
    assert trace["model_route_disposition"] == "alias_resolution"
    assert trace["model_substitution"] is None
