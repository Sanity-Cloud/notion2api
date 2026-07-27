import asyncio
import json
from pathlib import Path

import pytest

from tests.stream_round2 import harness


def rows_for(fixture_id: str):
    rows, _ = harness.run()
    return [row for row in rows if row["fixture_id"] == fixture_id]


def test_full_schema_and_stable_repetitions():
    rows, _ = harness.run()
    required = {
        "schema_version", "fixture_id", "repetition", "execution_id", "run_id", "subject_commit", "base_commit", "harness_content_commit", "harness_gate_commit",
        "parser_version", "mode", "raw_frame_encoding", "raw_bytes", "raw_hash", "logical_chunks",
        "expected_semantic_intent", "expected_outcome", "expected_code", "observed_outcome",
        "observed_code", "underlying_stream_outcome", "operational_outcome", "transport_counts",
        "semantic_counts", "visible_chars", "response_hash", "finish_count", "done_count",
        "input_done_presence", "emitted_done_presence", "pull_count", "close_count", "close_error",
        "socket_iteration_completed", "propagated_exception", "client_interpretation", "stop_condition",
        "adjudication_state", "adjudication_note", "invariant_comparison", "deterministic_replay_status", "result",
    }
    assert len(rows) == 96
    assert required <= rows[0].keys()
    assert {row["deterministic_replay_status"] for row in rows} == {"reproducible"}


def test_guard_receipts_done_separation_and_limits():
    before = rows_for("ordinary-exception-before-content")[0]
    finite = rows_for("finite-source")[0]
    limited = rows_for("chunk-limit")[0]
    assert before["client_interpretation"] == "error_receipt"
    assert before["observed_outcome"] == "stream_empty_no_terminal"
    assert not before["emitted_done_presence"]
    assert finite["input_done_presence"] and finite["emitted_done_presence"]
    assert finite["close_count"] == 1
    assert limited["stop_condition"] == "guard_limit" and limited["pull_count"] >= 3


def test_cleanup_hard_stop_and_propagation_are_preserved():
    close = rows_for("close-failure")[0]
    cancelled = rows_for("asyncio-cancelled")[0]
    generator_exit = rows_for("generator-exit")[0]
    assert close["underlying_stream_outcome"] == "success"
    assert close["operational_outcome"] == "cleanup_failure"
    assert close["close_count"] == 1 and close["result"] == "fail"
    assert cancelled["propagated_exception"] == asyncio.CancelledError.__name__
    assert generator_exit["propagated_exception"] == GeneratorExit.__name__


def test_discoveries_and_bytes_are_not_normalized():
    done = rows_for("done-before-finish")[0]
    assert done["observed_outcome"] == "stream_post_terminal_content"
    assert "ERR_STREAM_TERMINAL_ORDER is unreachable" in done["adjudication_note"]
    for fixture_id in ("multiline-sse-discovery", "physical-byte-fragment-discovery"):
        row = rows_for(fixture_id)[0]
        assert row["underlying_stream_outcome"] == "stream_empty_visible_output"
        assert row["adjudication_state"] == "needs_adjudication"
    assert rows_for("unicode-emoji")[0]["raw_bytes"] > 0
    assert rows_for("invalid-utf8-replacement")[0]["raw_frame_encoding"][0]["encoding"] == "hex"


def test_atomic_write_and_summary_gate_separation(tmp_path: Path):
    rows, summary = harness.run(tmp_path)
    assert summary["harness_gate_pass"] is True
    assert summary["product_gate_pass"] is False
    assert summary["cleanup_failures"] == 3
    assert summary["result_counts"] == {"fail": 3, "needs_adjudication": 9, "pass": 84}
    ordered = json.loads((tmp_path / "fixture-matrix.json").read_text(encoding="utf-8"))
    assert ordered == sorted(rows, key=lambda row: (row["fixture_id"], row["repetition"]))
    target = tmp_path / "target.json"
    harness.atomic_write(target, "original")
    with pytest.raises(OSError):
        harness.atomic_write(target, "replacement", replace=lambda _s, _d: (_ for _ in ()).throw(OSError("simulated")))
    assert target.read_text(encoding="utf-8") == "original"
    assert not list(tmp_path.glob("*.tmp"))


def test_direct_false_success_count_and_thin_cli(tmp_path: Path):
    row = rows_for("clean-success")[0].copy()
    row.update({"emitted_done_presence": True, "underlying_stream_outcome": "content_filter", "result": "pass"})
    summary = harness.write_outputs(tmp_path / "summary", [row])
    assert summary["false_success_violations"] == 1
    wrapper = Path("scripts/round2_stream_fixture_harness.py").read_text(encoding="utf-8")
    assert "from tests.stream_round2.harness import main" in wrapper
    assert "class Fixture" not in wrapper and "def execute(" not in wrapper


def test_provenance_roles_are_explicit():
    from tests.stream_round2 import harness

    row = rows_for("clean-success")[0]
    assert row["subject_commit"] == harness.SUBJECT_COMMIT
    assert row["base_commit"] == harness.SUBJECT_COMMIT
    assert row["harness_content_commit"] == harness.HARNESS_CONTENT_COMMIT
    assert row["harness_gate_commit"] == harness.HARNESS_GATE_COMMIT
    assert len({row["subject_commit"], row["harness_content_commit"], row["harness_gate_commit"]}) == 3

    _, summary = harness.run()
    assert summary == {}
    schema = json.loads((Path(__file__).with_name("fixture_schema.json")).read_text(encoding="utf-8"))
    assert schema["$id"] == "hive-stream-round2-harness/2"
    for field in ("subject_commit", "base_commit", "harness_content_commit", "harness_gate_commit"):
        assert field in schema["required"]
