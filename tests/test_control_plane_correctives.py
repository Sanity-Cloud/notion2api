from __future__ import annotations

import asyncio
import json
import logging
import threading
import time
from pathlib import Path

import httpx
import pytest

import app.summarizer as summarizer
from app.aigentbee_workbench import (
    prerequisite_loop_guard_state,
    validate_prerequisite_progression,
)
from app.compression_observability import (
    compression_telemetry_snapshot,
    log_compression_warning,
    reset_compression_telemetry_for_tests,
)
from app.hive_runtime import HiveEvent, HiveMissionSnapshot
from app.mcp_observability import (
    RoutineMcpNoiseFilter,
    mcp_observability_snapshot,
    reset_mcp_observability_for_tests,
)
from app.mcp_server import HealthOutput, ListSessionsOutput, _json_or_error
from app.notion_admission import NotionAdmissionController
from app.session_retention import (
    archive_and_filter_sessions,
    build_session_retention_plan,
)


def test_deterministic_summarizer_fallback_is_bounded_and_observable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(summarizer, "SILICONFLOW_API_KEY", "")
    monkeypatch.setenv("NOTION2API_SUMMARIZER_LOCAL_FALLBACK", "true")
    monkeypatch.setenv("NOTION2API_SUMMARIZER_LOCAL_MAX_CHARS", "500")

    result = asyncio.run(
        summarizer.summarize_turn(
            ["Prior decision: preserve dissent."],
            "Implement the bounded corrective action.",
            "Created an isolated branch and did not deploy it.",
        )
    )

    assert result.startswith("[deterministic-extractive-memory]")
    assert "preserve dissent" in result
    assert "did not deploy" in result
    assert len(result) <= 500
    telemetry = summarizer.summarizer_telemetry_snapshot()
    assert telemetry["configured"] is True
    assert telemetry["counters"]["local_fallback_success"] >= 1


def test_summarizer_can_be_explicitly_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(summarizer, "SILICONFLOW_API_KEY", "")
    monkeypatch.setenv("NOTION2API_SUMMARIZER_LOCAL_FALLBACK", "false")

    with pytest.raises(summarizer.SummarizerUnavailableError):
        asyncio.run(summarizer.summarize_turn([], "user", "assistant"))


def test_compression_warning_is_coalesced(caplog: pytest.LogCaptureFixture) -> None:
    reset_compression_telemetry_for_tests()
    logger = logging.getLogger("test.compression.correctives")
    with caplog.at_level(logging.WARNING, logger=logger.name):
        first = log_compression_warning(
            logger,
            "summary unavailable",
            event="sliding_window_compress_summary_unavailable",
            conversation_id="conversation-1",
            round_number=7,
        )
        second = log_compression_warning(
            logger,
            "summary unavailable",
            event="sliding_window_compress_summary_unavailable",
            conversation_id="conversation-1",
            round_number=8,
        )

    assert first is True
    assert second is False
    assert [record.message for record in caplog.records].count("summary unavailable") == 1
    snapshot = compression_telemetry_snapshot()
    assert snapshot["counters"]["warnings_emitted"] == 1
    assert snapshot["counters"]["warnings_suppressed"] == 1


def test_admission_metrics_separate_jobs_from_wait_events(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("NOTION_ADMISSION_ACCOUNT_CAPACITY", "10")
    monkeypatch.setenv("NOTION_ADMISSION_ACCOUNT_REFILL_PER_SECOND", "100")
    monkeypatch.setenv("NOTION_ADMISSION_ACCOUNT_MAX_INFLIGHT", "1")
    controller = NotionAdmissionController(shared_store=False)
    first = controller.acquire(
        workspace_id="workspace",
        user_id="user",
        thread_id="thread-1",
        idempotency_key="job-1",
    )
    errors: list[BaseException] = []

    def acquire_second() -> None:
        try:
            permit = controller.acquire(
                workspace_id="workspace",
                user_id="user",
                thread_id="thread-2",
                idempotency_key="job-2",
                timeout_seconds=2.0,
            )
            permit.release(success=True)
        except BaseException as exc:  # pragma: no cover - diagnostic capture
            errors.append(exc)

    worker = threading.Thread(target=acquire_second, daemon=True)
    worker.start()
    deadline = time.time() + 1.0
    snapshot = controller.snapshot()
    while snapshot["counters"]["queued_unique_jobs"] < 1 and time.time() < deadline:
        time.sleep(0.01)
        snapshot = controller.snapshot()

    assert snapshot["metric_schema_version"] == 2
    assert snapshot["counters"]["queue_entries"] == 2
    assert snapshot["counters"]["queued_unique_jobs"] == 1
    assert snapshot["counters"]["queued"] == 1
    assert snapshot["counters"]["queue_wait_events"] >= 1

    first.release(success=True)
    worker.join(timeout=2.0)
    assert not worker.is_alive()
    assert errors == []
    final = controller.snapshot()
    assert final["counters"]["admitted"] == 2
    assert final["counters"]["completed"] == 2


def test_session_retention_protects_governance_and_evidence_bindings() -> None:
    day_ms = 24 * 60 * 60 * 1000
    now_ms = 200 * day_ms
    records = {
        "ordinary-old": {"conversation_id": "c-old", "updated_at": 1 * day_ms},
        "ordinary-new": {"conversation_id": "c-new", "updated_at": 199 * day_ms},
        "aigentbee-leader-hive": {"conversation_id": "c-leader", "updated_at": 1},
        "job-bound": {"conversation_id": "c-job", "updated_at": 1},
        "evidence-bound": {
            "conversation_id": "c-evidence",
            "updated_at": 1,
            "mission_id": "mission-1",
        },
        "legacy-no-time": {"conversation_id": "c-legacy"},
    }

    plan = build_session_retention_plan(
        records,
        protected_session_names={"job-bound"},
        protected_conversation_ids={"c-job"},
        now_ms=now_ms,
        retention_days=90,
        max_records=500,
    )

    candidates = {item["session_name"] for item in plan["candidates"]}
    protected = {item["session_name"]: item["reason"] for item in plan["protected"]}
    assert candidates == {"ordinary-old"}
    assert protected["aigentbee-leader-hive"] == "governance_leader_session"
    assert protected["job-bound"] == "referenced_by_chat_job"
    assert protected["evidence-bound"] == "explicit_evidence_binding"
    assert protected["legacy-no-time"] == "missing_timestamp"


def test_session_retention_archives_before_filtering(tmp_path: Path) -> None:
    records = {
        "remove-me": {"conversation_id": "c1", "updated_at": 1},
        "keep-me": {"conversation_id": "c2", "updated_at": 2},
    }
    plan = {
        "candidates": [
            {"session_name": "remove-me", "reason": "older_than_retention_window"}
        ]
    }
    archive_path = tmp_path / "sessions.archive.jsonl"

    retained, receipt = archive_and_filter_sessions(
        records,
        plan,
        archive_path=archive_path,
        applied_by="test",
    )

    assert set(retained) == {"keep-me"}
    assert receipt["archived"] == 1
    entry = json.loads(archive_path.read_text(encoding="utf-8").strip())
    assert entry["session_name"] == "remove-me"
    assert entry["record"]["conversation_id"] == "c1"


def test_http_error_contains_correlation_receipt() -> None:
    response = httpx.Response(
        400,
        headers={
            "content-type": "application/json",
            "X-Request-ID": "backend-request-7",
        },
        json={"error": {"message": "bad request"}},
    )

    result = _json_or_error(
        response,
        correlation_id="mcp-request-3",
        method="POST",
        path="/v1/chat/completions",
    )

    assert result["ok"] is False
    assert result["status_code"] == 400
    assert result["correlation"] == {
        "request_id": "mcp-request-3",
        "response_request_id": "backend-request-7",
        "method": "POST",
        "path": "/v1/chat/completions",
    }


def test_routine_mcp_termination_noise_is_coalesced() -> None:
    reset_mcp_observability_for_tests()
    noise_filter = RoutineMcpNoiseFilter()
    first = logging.LogRecord(
        "mcp.server", logging.INFO, __file__, 1, "Terminating session: None", (), None
    )
    second = logging.LogRecord(
        "mcp.server", logging.INFO, __file__, 2, "Terminating session: None", (), None
    )
    fault = logging.LogRecord(
        "mcp.server", logging.ERROR, __file__, 3, "HTTP 400 invalid session", (), None
    )

    assert noise_filter.filter(first) is True
    assert noise_filter.filter(second) is False
    assert noise_filter.filter(fault) is True
    counters = mcp_observability_snapshot()["counters"]
    assert counters["observed_terminating_none"] == 2
    assert counters["suppressed_terminating_none"] == 1


def _loop_guard_snapshot() -> HiveMissionSnapshot:
    events = []
    for index in range(2):
        events.append(
            HiveEvent(
                event_id=f"event-{index}",
                mission_id="mission-loop",
                work_unit_id="lane-governance",
                event_type="LEADER_REQUEST_SUBMITTED",
                sender="scheduler",
                recipient="leader",
                payload={
                    "request_type": "review",
                    "request_preview": "Review the trust anchor governance contract again.",
                },
                context_version=index + 1,
                created_at=1000 + index,
            )
        )
    return HiveMissionSnapshot(
        mission_id="mission-loop",
        title="Dashboard governance",
        objective="Implement a governed dashboard.",
        lifecycle_stage="Build",
        status="ACTIVE",
        authority_ceiling="A3",
        created_at=1,
        updated_at=2,
        revision=2,
        work_unit_count=0,
        event_count=len(events),
        action_count=0,
        work_units=[],
        events=events,
    )


def test_prerequisite_guard_blocks_analysis_loop_but_allows_implementation() -> None:
    snapshot = _loop_guard_snapshot()
    state = prerequisite_loop_guard_state(snapshot)
    assert state["mode"] == "implementation_only"
    assert state["recent_unresolved_analysis_requests"] == 2

    with pytest.raises(ValueError, match="further trust-anchor"):
        validate_prerequisite_progression(
            snapshot,
            "Review the trust anchor governance contract one more time.",
            "review",
        )

    validate_prerequisite_progression(
        snapshot,
        "Implement the isolated authority-test packet with a pinned test root.",
        "instruction",
    )


def test_output_models_preserve_backward_compatible_defaults() -> None:
    sessions = ListSessionsOutput(
        count=0,
        default_session="auto-generated",
        state_path="sessions.json",
    )
    health = HealthOutput(ok=True)
    assert sessions.retention == {}
    assert health.conversation_compression == {}
    assert health.mcp_runtime == {}
