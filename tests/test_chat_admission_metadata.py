from __future__ import annotations

from types import SimpleNamespace

from fastapi import Response

from app.api import chat


def test_public_admission_receipt_excludes_account_and_idempotency() -> None:
    client = SimpleNamespace(
        last_admission_receipt={
            "account_key": "workspace:user",
            "thread_key": "workspace:user:thread",
            "idempotency_key": "secret-dedup-key",
            "attempt_id": "attempt-a",
            "workload_class": "inference",
            "admission_weight": 1.25,
            "request_context_id": "repo-run-a",
            "estimated_input_tokens": 120,
            "completion": {
                "account_key": "workspace:user",
                "outcome": "succeeded",
                "status_code": 200,
                "response_bytes": 400,
            },
        }
    )

    projected = chat._public_admission_receipt(client)

    assert projected["attempt_id"] == "attempt-a"
    assert projected["completion"]["status_code"] == 200
    assert "account_key" not in projected
    assert "thread_key" not in projected
    assert "idempotency_key" not in projected
    assert "account_key" not in projected["completion"]


def test_bind_request_metadata_sets_repoai_lineage(monkeypatch) -> None:
    monkeypatch.setattr(chat, "governance_receipt_from_client", lambda client: {})
    request = SimpleNamespace(
        metadata={
            "repo_ai_run_id": "run-42",
            "caller": {
                "type": "repoai",
                "request_fingerprint": "trace-42",
            },
        },
        model="terra",
        conversation_id="conversation-fallback",
    )
    client = SimpleNamespace()

    chat._bind_governance_request_metadata(request, client)

    assert client.request_context_id == "run-42"
    assert client.request_trace_id == "trace-42"
    assert client.request_model_id == "terra"
    assert client.request_idempotency_key == "trace-42"


def test_thread_metadata_includes_bounded_admission_receipt() -> None:
    response = Response()
    client = SimpleNamespace(
        current_thread_id="thread-123",
        last_admission_receipt={
            "attempt_id": "attempt-123",
            "operation": "POST /api/v3/runInferenceTranscript",
            "workload_class": "inference",
            "admission_weight": 1.1,
            "account_key": "workspace:user",
            "completion": {
                "outcome": "succeeded",
                "status_code": 200,
                "duration_seconds": 1.25,
            },
        },
    )

    metadata = chat._attach_notion_thread_metadata(
        response=response,
        client=client,
        model_metadata={"requested_model": "terra"},
    )

    assert metadata["notion_thread_id"] == "thread-123"
    assert metadata["notion_admission"]["attempt_id"] == "attempt-123"
    assert "account_key" not in metadata["notion_admission"]
    assert response.headers["X-Notion-Thread-Id"] == "thread-123"
