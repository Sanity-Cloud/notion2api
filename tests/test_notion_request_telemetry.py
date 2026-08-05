from __future__ import annotations

import sqlite3
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import notion_admission
from app.notion_admission import (
    AdmittedSession,
    AdmissionError,
    AdmissionTimeoutError,
    NotionAdmissionController,
)
from app.notion_admission_store import SharedAdmissionStore
from app.notion_request_telemetry import NotionRequestTelemetryStore


class FakeResponse:
    def __init__(self, status_code: int = 200, content: bytes = b"ok") -> None:
        self.status_code = status_code
        self.content = content
        self.headers = {"Content-Length": str(len(content))}
        self.closed = False

    def close(self) -> None:
        self.closed = True

    def iter_lines(self, *args, **kwargs):
        del args, kwargs
        yield b"first"
        yield b"second"


class FailingStreamResponse(FakeResponse):
    def iter_lines(self, *args, **kwargs):
        del args, kwargs
        yield b"first"
        raise RuntimeError("upstream stream failed")


class FakeSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, str, dict]] = []

    def request(self, method: str, url: str, **kwargs):
        self.calls.append((method, url, kwargs))
        return self.responses.pop(0)


def configure(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NOTION_ADMISSION_ACCOUNT_CAPACITY", "2")
    monkeypatch.setenv("NOTION_ADMISSION_ACCOUNT_REFILL_PER_SECOND", "1000")
    monkeypatch.setenv("NOTION_ADMISSION_ACCOUNT_MAX_INFLIGHT", "2")
    monkeypatch.setenv("NOTION_ADMISSION_QUEUE_TIMEOUT_SECONDS", "2")
    monkeypatch.setenv("NOTION_ADMISSION_BACKOFF_JITTER_RATIO", "0")


def owner() -> SimpleNamespace:
    return SimpleNamespace(
        space_id="workspace-a",
        user_id="user-a",
        current_thread_id="thread-a",
        request_idempotency_key="",
        request_trace_id="trace-a",
        request_context_id="repo-run-a",
        request_model_id="orchid-muffin",
        last_admission_receipt={},
        last_request_telemetry={},
    )


def test_shared_bucket_consumes_fractional_weight(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configure(monkeypatch)
    database = tmp_path / "weighted.sqlite3"
    store = SharedAdmissionStore(database)
    controller = NotionAdmissionController(shared_store=store)

    permit = controller.acquire(
        workspace_id="workspace-a",
        user_id="user-a",
        operation="model-metadata",
        admission_weight=0.25,
    )
    with sqlite3.connect(database) as conn:
        tokens = conn.execute(
            "SELECT tokens FROM admission_token_buckets"
        ).fetchone()[0]
    permit.release()

    assert tokens == pytest.approx(1.75, abs=0.02)


def test_non_stream_request_persists_sanitized_telemetry(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configure(monkeypatch)
    telemetry = NotionRequestTelemetryStore(tmp_path / "telemetry.sqlite3")
    monkeypatch.setattr(notion_admission, "_REQUEST_TELEMETRY", telemetry)
    controller = NotionAdmissionController(shared_store=False)
    client = owner()
    session = AdmittedSession(FakeSession([FakeResponse(content=b"result")]), client, controller)

    response = session.post(
        "https://www.notion.so/api/v3/runInferenceTranscript",
        json={"threadId": "thread-a", "prompt": "review this repository"},
    )

    assert response.status_code == 200
    receipt = client.last_admission_receipt
    attempt = client.last_request_telemetry
    assert receipt["workload_class"] == "inference"
    assert receipt["admission_weight"] >= 1.0
    assert receipt["request_context_id"] == "repo-run-a"
    assert receipt["trace_id"] == "trace-a"
    assert receipt["model_id"] == "orchid-muffin"
    assert receipt["request_bytes"] > 0
    assert receipt["estimated_input_tokens"] > 0
    assert attempt["outcome"] == "succeeded"
    assert attempt["status_code"] == 200
    assert attempt["response_bytes"] == len(b"result")
    assert "prompt" not in attempt


def test_stream_request_finishes_after_consumption(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configure(monkeypatch)
    telemetry = NotionRequestTelemetryStore(tmp_path / "stream-telemetry.sqlite3")
    monkeypatch.setattr(notion_admission, "_REQUEST_TELEMETRY", telemetry)
    controller = NotionAdmissionController(shared_store=False)
    client = owner()
    client.request_idempotency_key = ""
    session = AdmittedSession(FakeSession([FakeResponse()]), client, controller)

    response = session.post(
        "https://www.notion.so/api/v3/runInferenceTranscript",
        json={"threadId": "thread-a"},
        stream=True,
    )
    attempt_id = client.last_admission_receipt["attempt_id"]
    assert telemetry.get(attempt_id)["completed_at"] is None

    assert list(response.iter_lines()) == [b"first", b"second"]
    attempt = telemetry.get(attempt_id)
    assert attempt["outcome"] == "succeeded"
    assert attempt["response_bytes"] > 0
    assert attempt["estimated_output_tokens"] > 0


def test_snapshot_aggregates_usage(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    configure(monkeypatch)
    telemetry = NotionRequestTelemetryStore(tmp_path / "aggregate.sqlite3")
    telemetry.start(
        {
            "attempt_id": "attempt-a",
            "operation": "POST /api/v3/runInferenceTranscript",
            "workload_class": "inference",
            "admission_weight": 1.2,
            "request_bytes": 400,
            "estimated_input_tokens": 100,
        }
    )
    telemetry.finish(
        "attempt-a",
        success=True,
        status_code=200,
        response_bytes=200,
        estimated_output_tokens=50,
        retry_count=1,
    )

    snapshot = telemetry.snapshot()
    row = snapshot["usage_last_hour"][0]
    assert row["request_count"] == 1
    assert row["request_bytes"] == 400
    assert row["response_bytes"] == 200
    assert row["estimated_input_tokens"] == 100
    assert row["estimated_output_tokens"] == 50
    assert row["retry_count"] == 1


def test_telemetry_failure_does_not_leak_admission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    configure(monkeypatch)

    class FailingTelemetry:
        def start(self, receipt):
            raise OSError("telemetry unavailable")

        def finish(self, attempt_id, **kwargs):
            raise OSError("telemetry unavailable")

        def note_retry(self, attempt_id, **kwargs):
            raise OSError("telemetry unavailable")

        def snapshot(self):
            raise OSError("telemetry unavailable")

    monkeypatch.setattr(notion_admission, "_REQUEST_TELEMETRY", FailingTelemetry())
    controller = NotionAdmissionController(shared_store=False)
    client = owner()
    session = AdmittedSession(FakeSession([FakeResponse()]), client, controller)

    assert session.post("https://www.notion.so/api/v3/getAvailableModels", json={}).status_code == 200
    assert controller.snapshot()["active_accounts"] == {}
    assert controller.snapshot()["active_threads"] == 0


def test_stream_close_before_first_chunk_is_terminal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configure(monkeypatch)
    telemetry = NotionRequestTelemetryStore(tmp_path / "zero-close.sqlite3")
    monkeypatch.setattr(notion_admission, "_REQUEST_TELEMETRY", telemetry)
    controller = NotionAdmissionController(shared_store=False)
    client = owner()
    client.request_idempotency_key = ""
    response = AdmittedSession(
        FakeSession([FakeResponse()]), client, controller
    ).post(
        "https://www.notion.so/api/v3/runInferenceTranscript",
        json={"threadId": "thread-a"},
        stream=True,
    )
    attempt_id = client.last_admission_receipt["attempt_id"]

    response.close()

    attempt = telemetry.get(attempt_id)
    assert attempt["outcome"] == "failed"
    assert attempt["completed_at"] is not None
    assert controller.snapshot()["active_threads"] == 0


def test_stream_generator_close_midstream_is_terminal_once(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configure(monkeypatch)
    database = tmp_path / "mid-close.sqlite3"
    telemetry = NotionRequestTelemetryStore(database)
    monkeypatch.setattr(notion_admission, "_REQUEST_TELEMETRY", telemetry)
    controller = NotionAdmissionController(shared_store=False)
    client = owner()
    client.request_idempotency_key = ""
    response = AdmittedSession(
        FakeSession([FakeResponse()]), client, controller
    ).post(
        "https://www.notion.so/api/v3/runInferenceTranscript",
        json={"threadId": "thread-a"},
        stream=True,
    )
    attempt_id = client.last_admission_receipt["attempt_id"]
    iterator = response.iter_lines()

    assert next(iterator) == b"first"
    iterator.close()
    response.close()

    attempt = telemetry.get(attempt_id)
    assert attempt["outcome"] == "failed"
    assert attempt["response_bytes"] > 0
    with sqlite3.connect(database) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM notion_request_attempts WHERE attempt_id = ?",
            (attempt_id,),
        ).fetchone()[0]
    assert count == 1
    assert controller.snapshot()["active_threads"] == 0


def test_upstream_stream_exception_is_terminal(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configure(monkeypatch)
    telemetry = NotionRequestTelemetryStore(tmp_path / "stream-error.sqlite3")
    monkeypatch.setattr(notion_admission, "_REQUEST_TELEMETRY", telemetry)
    controller = NotionAdmissionController(shared_store=False)
    client = owner()
    client.request_idempotency_key = ""
    response = AdmittedSession(
        FakeSession([FailingStreamResponse()]), client, controller
    ).post(
        "https://www.notion.so/api/v3/runInferenceTranscript",
        json={"threadId": "thread-a"},
        stream=True,
    )
    attempt_id = client.last_admission_receipt["attempt_id"]

    with pytest.raises(RuntimeError, match="upstream stream failed"):
        list(response.iter_lines())

    attempt = telemetry.get(attempt_id)
    assert attempt["outcome"] == "failed"
    assert attempt["completed_at"] is not None
    assert controller.snapshot()["active_threads"] == 0


def test_attempt_terminal_transition_is_idempotent(tmp_path: Path) -> None:
    database = tmp_path / "idempotent.sqlite3"
    telemetry = NotionRequestTelemetryStore(database)
    receipt = {
        "attempt_id": "attempt-stable",
        "operation": "POST /api/v3/runInferenceTranscript",
        "workload_class": "inference",
    }
    telemetry.start(receipt)
    first = telemetry.finish(
        "attempt-stable",
        success=True,
        status_code=200,
        response_bytes=10,
    )
    telemetry.start({**receipt, "operation": "replacement"})
    second = telemetry.finish(
        "attempt-stable",
        success=False,
        status_code=500,
        response_bytes=999,
    )

    assert first["outcome"] == "succeeded"
    assert second["outcome"] == "succeeded"
    assert second["status_code"] == 200
    assert second["response_bytes"] == 10
    assert second["operation"] == "POST /api/v3/runInferenceTranscript"
    with sqlite3.connect(database) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM notion_request_attempts WHERE attempt_id = ?",
            ("attempt-stable",),
        ).fetchone()[0]
    assert count == 1


@pytest.mark.parametrize(
    "weight",
    [0, -1, float("nan"), float("inf"), float("-inf"), True, "invalid"],
)
def test_invalid_admission_weights_are_rejected(
    monkeypatch: pytest.MonkeyPatch,
    weight,
) -> None:
    configure(monkeypatch)
    controller = NotionAdmissionController(shared_store=False)

    with pytest.raises(AdmissionError):
        controller.acquire(
            workspace_id="workspace-a",
            user_id="user-a",
            admission_weight=weight,
        )


def test_shared_weight_bucket_uses_exact_fixed_point_boundaries(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    configure(monkeypatch)
    monkeypatch.setenv("NOTION_ADMISSION_ACCOUNT_REFILL_PER_SECOND", "0.001")
    monkeypatch.setenv("NOTION_ADMISSION_QUEUE_TIMEOUT_SECONDS", "0.05")
    local = NotionAdmissionController(shared_store=False)
    for index in range(8):
        permit = local.acquire(
            workspace_id="workspace-local",
            user_id="user-local",
            operation=f"local-fractional-{index}",
            admission_weight=0.25,
        )
        permit.release()
    with pytest.raises(AdmissionTimeoutError):
        local.acquire(
            workspace_id="workspace-local",
            user_id="user-local",
            operation="local-fractional-overflow",
            admission_weight=0.25,
        )

    database = tmp_path / "fixed-point.sqlite3"
    controller = NotionAdmissionController(
        shared_store=SharedAdmissionStore(database)
    )

    for index in range(8):
        permit = controller.acquire(
            workspace_id="workspace-a",
            user_id="user-a",
            operation=f"fractional-{index}",
            admission_weight=0.25,
        )
        permit.release()

    with sqlite3.connect(database) as conn:
        units = conn.execute(
            "SELECT token_units FROM admission_weight_buckets"
        ).fetchone()[0]
    assert units == 0

    with pytest.raises(AdmissionTimeoutError):
        controller.acquire(
            workspace_id="workspace-a",
            user_id="user-a",
            operation="fractional-overflow",
            admission_weight=0.25,
        )
