import json
from pathlib import Path

import pytest

from tests.stream_round2.harness import (
    SCHEMA_VERSION,
    Fixture,
    atomic_write_jsonl_and_summary,
    replay_fixture,
)

BASE = "de52b8ac571f77b130a3c20919cd1666a4af3ce5"


def frame(content=None, finish=None):
    delta = {} if content is None else {"content": content}
    return "data: " + json.dumps({"choices": [{"delta": delta, "finish_reason": finish}]}) + "\n\n"


def fixture(fixture_id="clean", **kwargs):
    defaults = {
        "fixture_id": fixture_id,
        "mode": "tracker",
        "physical_fragments": (frame("hello"), frame(finish="stop"), "data: [DONE]\n\n"),
        "logical_chunks": ({"chunk_id": "content", "kind": "sse", "fragment_indexes": [0]},),
        "expected_outcome": "success",
    }
    defaults.update(kwargs)
    return Fixture(**defaults)


def test_schema_record_contains_versioned_required_round2_fields():
    record = replay_fixture(fixture("unicode", physical_fragments=(frame("ðŸŒŠe\u0301"), frame(finish="stop"), b"data: [DONE]\n\n")), base_commit=BASE)[0]
    required = {"fixture_id", "repetition", "base_commit", "parser_version", "physical_fragments", "raw_bytes", "logical_chunks", "expected_outcome", "actual_outcome", "actual_code", "response_sha256", "raw_sha256", "semantic_counts", "transport_counts", "terminal", "done_presence", "source_accounting", "client_interpretation", "deterministic_replay"}
    assert record["schema_version"] == SCHEMA_VERSION
    assert required <= record.keys()
    assert record["done_presence"] is True
    assert record["raw_bytes"] > 0


def test_replay_requires_three_runs_and_compares_invariants():
    records = replay_fixture(fixture(), base_commit=BASE)
    assert len(records) == 3
    assert {row["deterministic_replay"] for row in records} == {"reproducible"}
    assert len({row["response_sha256"] for row in records}) == 1
    with pytest.raises(ValueError):
        replay_fixture(fixture(), base_commit=BASE, repetitions=2)


def test_source_accounting_infinite_guard_and_close_failure():
    infinite = fixture("infinite", mode="guard", physical_fragments=(frame("long-output"),), infinite=True, expected_outcome="silent_empty_200")
    # The guard emits a bounded failure without a successful [DONE] terminal.
    record = replay_fixture(infinite, base_commit=BASE)[0]
    assert record["source_accounting"]["pull_count"] >= 1
    assert record["source_accounting"]["close_count"] >= 1

    closing = fixture("close-failing", physical_fragments=(frame("x"),), close_fails=True, expected_outcome="cleanup_failure")
    record = replay_fixture(closing, base_commit=BASE)[0]
    assert record["actual_outcome"] == "cleanup_failure"
    assert record["source_accounting"]["cleanup_error"] == "RuntimeError"


@pytest.mark.parametrize(
    ("name", "chunks", "expected"),
    [
        ("false-success", (frame("x"), "data: [DONE]\n\n"), "false_success_done"),
        ("empty", tuple(), "silent_empty_200"),
        ("corrupt", (frame("x"), frame(finish="stop"), "data: [DONE]\n\n", "data: [DONE]\n\n"), "terminal_corruption"),
        ("post-terminal", (frame("x"), frame(finish="stop"), "data: [DONE]\n\n", frame("late")), "post_terminal_mutation"),
        ("unsupported", (b"data: {bad}\n\n",), "unsupported_framing"),
    ],
)
def test_failure_classifications_are_explicit(name, chunks, expected):
    record = replay_fixture(fixture(name, physical_fragments=chunks, expected_outcome=expected), base_commit=BASE)[0]
    assert record["actual_outcome"] == expected
    assert record["pass"]


def test_atomic_jsonl_and_stable_summary_order(tmp_path: Path):
    records = replay_fixture(fixture("z-last"), base_commit=BASE) + replay_fixture(fixture("a-first"), base_commit=BASE)
    jsonl, summary = atomic_write_jsonl_and_summary(records, tmp_path)
    lines = jsonl.read_text(encoding="utf-8").splitlines()
    assert [json.loads(line)["fixture_id"] for line in lines[:3]] == ["a-first"] * 3
    assert not list(tmp_path.glob(".*.tmp"))
    body = json.loads(summary.read_text(encoding="utf-8"))
    assert body["record_count"] == 6
    assert body["fixture_ids"] == ["a-first", "z-last"]
