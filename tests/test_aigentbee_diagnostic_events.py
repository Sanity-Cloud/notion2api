from __future__ import annotations

import json
from pathlib import Path

from app.diagnostics import CONTRACT_VERSION, EVENT_PREFIX, emit_diagnostic_event


ROOT = Path(__file__).resolve().parents[1]


def _event(stderr: str) -> dict:
    line = stderr.strip()
    assert line.startswith(EVENT_PREFIX)
    return json.loads(line[len(EVENT_PREFIX) :])


def test_aigentbee_event_preserves_lineage_and_redacts_secrets(monkeypatch, capsys) -> None:
    monkeypatch.setenv("SANITYCLOUD_DIAGNOSTIC_CONTRACT_VERSION", CONTRACT_VERSION)
    assert emit_diagnostic_event(
        code="HIVE_ADAPTER_FAILED",
        message="worker adapter failed",
        operation="hive_execution",
        retryable=True,
        component_id="aigentbee",
        source="aigentbee_runtime",
        parent_record_id="exec-123",
        project_id="plan-456",
        lane_id="lane-789",
        details={"token": "do-not-emit", "duration_ms": 1200},
    )
    event = _event(capsys.readouterr().err)
    assert event["component_id"] == "aigentbee"
    assert event["source"] == "aigentbee_runtime"
    assert event["code"] == "HIVE_ADAPTER_FAILED"
    assert event["parent_record_id"] == "exec-123"
    assert event["project_id"] == "plan-456"
    assert event["lane_id"] == "lane-789"
    assert event["details"]["token"] == "[REDACTED]"
    assert event["details"]["duration_ms"] == 1200


def test_dispatcher_emits_terminal_execution_and_acknowledgement_diagnostics() -> None:
    source = (ROOT / "app" / "hive_dispatcher.py").read_text(encoding="utf-8")
    assert "HIVE_ACKNOWLEDGEMENT_FAILED" in source
    assert "HIVE_EXECUTION_TIMEOUT" in source
    assert "HIVE_ADAPTER_FAILED" in source
    assert 'component_id="aigentbee"' in source
    assert "parent_record_id=execution_id" in source
