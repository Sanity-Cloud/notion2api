import json
from pathlib import Path

import pytest

from tests.stream_round2 import harness


def rows_for(fixture_id: str):
    rows, _ = harness.run()
    return [row for row in rows if row["fixture_id"] == fixture_id]


def test_schema_and_repetitions_are_stable():
    rows, _ = harness.run()
    assert len(rows) == len(harness.fixtures()) * harness.REPETITIONS
    required = {"schema_version", "fixture_id", "repetition", "execution_id", "subject_commit", "base_commit", "harness_content_commit", "harness_gate_commit", "parser_version", "mode", "raw_frame_encoding", "raw_bytes", "logical_chunks", "expected_outcome", "expected_code", "observed_outcome", "observed_code", "underlying_classification", "transport_counts", "semantic_counts", "visible_chars", "response_hash", "raw_hash", "finish_count", "done_count", "sequence_count", "input_done_presence", "emitted_done_presence", "pull_count", "close_count", "close_error", "propagated_exception", "client_interpretation", "stop_condition", "adjudication_state", "invariant_comparison", "deterministic_replay_status", "result"}
    assert required <= set(rows[0])
    assert {row["deterministic_replay_status"] for row in rows} == {"reproducible"}


def test_guard_error_receipt_is_not_reparsed_as_provider_stream():
    before = rows_for("ordinary-exception-before-content")[0]
    after = rows_for("ordinary-exception-after-partial")[0]
    assert before["observed_outcome"] == "stream_empty_no_terminal"
    assert after["observed_outcome"] == "stream_interrupted"
    assert before["client_interpretation"] == "error_receipt"
    assert not before["emitted_done_presence"]
    assert before["input_done_presence"] is False


def test_done_fields_and_source_accounting_are_separate():
    row = rows_for("finite-source")[0]
    assert row["input_done_presence"] is True
    assert row["emitted_done_presence"] is True
    assert row["pull_count"] >= 1
    assert row["close_count"] == 1
    assert row["socket_iteration_completed"] is True


def test_close_failure_and_stop_conditions_are_captured():
    close = rows_for("close-failure")[0]
    limit = rows_for("character-limit")[0]
    cancelled = rows_for("asyncio-cancelled")[0]
    assert close["close_error"] == "RuntimeError: close failure"
    assert limit["stop_condition"] == "guard_limit"
    assert cancelled["stop_condition"] == "propagated"
    assert cancelled["propagated_exception"] == "CancelledError"


def test_spec_disagreement_is_preserved_with_reachability_evidence():
    row = rows_for("done-before-finish")[0]
    assert row["observed_outcome"] == "stream_post_terminal_content"
    assert row["expected_outcome"] == "stream_terminal_order_invalid"
    assert row["adjudication_state"] == "needs_adjudication"
    assert "spec_disagreement" in row["adjudication_note"]
    assert row["result"] == "needs_adjudication"


def test_unsupported_framing_preserves_current_observation():
    for fixture_id in ("multiline-sse-discovery", "physical-byte-fragment-discovery"):
        row = rows_for(fixture_id)[0]
        assert row["expected_semantic_intent"] == "malformed-only"
        assert row["observed_outcome"] == "stream_empty_visible_output"
        assert row["underlying_classification"] == "stream_empty_visible_output"
        assert row["adjudication_state"] == "needs_adjudication"
        assert row["semantic_counts"]


def test_atomic_outputs_and_summary(tmp_path: Path):
    rows, summary = harness.run(tmp_path)
    assert summary["harness_gate_pass"] is True
    assert summary["product_gate_pass"] is True
    assert json.loads((tmp_path / "fixture-matrix.json").read_text(encoding="utf-8")) == sorted(rows, key=lambda row: (row["fixture_id"], row["repetition"]))
    assert len((tmp_path / "fixture-matrix.jsonl").read_text(encoding="utf-8").splitlines()) == len(rows)
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_failure_preserves_existing_target_and_cleans_temp(tmp_path: Path):
    target = tmp_path / "target.json"
    target.write_text("original", encoding="utf-8")
    with pytest.raises(OSError):
        harness.atomic_write(target, "replacement", replace=lambda _src, _dst: (_ for _ in ()).throw(OSError("simulated")))
    assert target.read_text(encoding="utf-8") == "original"
    assert not list(tmp_path.glob("*.tmp"))


def test_summary_counts_need_adjudication_not_failures(tmp_path: Path):
    _, summary = harness.run(tmp_path)
    assert summary["result_counts"]["needs_adjudication"] == 9
    assert summary["result_counts"].get("fail", 0) == 0
    assert summary["harness_implementation_failures"] == 0
    assert summary["nondeterministic_repetitions"] == 0
    assert summary["false_success_violations"] == 0
    assert summary["cleanup_failures"] == 3
    assert summary["handled_cleanup_failures"] == 3
    assert summary["unhandled_cleanup_failures"] == 0
    assert summary["product_gate_pass"] is True
