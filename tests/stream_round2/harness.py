"""Deterministic, offline fixture harness for later stream Round 2 lanes."""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from app.stream_protocol import PARSER_VERSION, StreamProtocolTracker

SCHEMA_VERSION = "hive-stream-round2-fixture-result/1"
INVARIANT_FIELDS = (
    "fixture_id", "base_commit", "parser_version", "expected_outcome", "actual_outcome",
    "actual_code", "response_sha256", "raw_sha256", "semantic_counts", "transport_counts",
    "terminal", "done_presence", "source_accounting", "client_interpretation",
)


@dataclass(frozen=True)
class Fixture:
    fixture_id: str
    mode: str
    physical_fragments: tuple[bytes | str, ...]
    logical_chunks: tuple[dict[str, Any], ...]
    expected_outcome: str
    expected_code: str = ""
    source_error: Callable[[], BaseException] | None = None
    infinite: bool = False
    close_fails: bool = False
    client_interpretation: str = "tracker_receipt"


@dataclass
class CountingSource:
    fragments: tuple[bytes | str, ...]
    error: BaseException | None = None
    infinite: bool = False
    close_fails: bool = False
    pulls: int = 0
    closes: int = 0
    index: int = 0

    def __iter__(self) -> "CountingSource":
        return self

    def __next__(self) -> bytes | str:
        self.pulls += 1
        if self.index < len(self.fragments):
            value = self.fragments[self.index]
            self.index += 1
            return value
        if self.infinite and self.fragments:
            return self.fragments[-1]
        if self.error is not None:
            raise self.error
        raise StopIteration

    def close(self) -> None:
        self.closes += 1
        if self.close_fails:
            raise RuntimeError("fixture close failure")


def _bytes(value: bytes | str) -> bytes:
    return value if isinstance(value, bytes) else value.encode("utf-8")


def _encoded_fragment(value: bytes | str) -> dict[str, str]:
    raw = _bytes(value)
    return {"encoding": "hex", "value": raw.hex()}


def _classify(outcome: Any) -> tuple[str, str]:
    code = str(outcome.code or "")
    classification = str(outcome.classification or "")
    if code == "ERR_STREAM_MISSING_FINISH" and outcome.receipt.get("done_count"):
        return "false_success_done", code
    if code == "ERR_PROVIDER_EMPTY_STREAM":
        return "silent_empty_200", code
    if code in {"ERR_STREAM_DUPLICATE_TERMINAL", "ERR_STREAM_TERMINAL_ORDER"}:
        return "terminal_corruption", code
    if code == "ERR_STREAM_POST_TERMINAL_CONTENT":
        return "post_terminal_mutation", code
    if outcome.receipt.get("transport_event_counts", {}).get("malformed_frame"):
        return "unsupported_framing", code
    return classification or ("success" if outcome.ok else "stream_failure"), code


def _tracker_run(source: CountingSource) -> tuple[Any, str]:
    tracker = StreamProtocolTracker()
    source_error: BaseException | None = None
    try:
        for fragment in source:
            tracker.observe(fragment)
    except (asyncio.CancelledError, GeneratorExit):
        raise
    except BaseException as exc:
        source_error = exc
    return tracker.finalize(source_error=source_error), "tracker_receipt"


def _guard_run(source: CountingSource) -> tuple[Any, str]:
    from app.api import chat
    emitted: list[bytes | str] = []
    try:
        emitted = list(chat._guard_stream_until_integrity(source, response_id="fixture", model="fixture"))
    except (asyncio.CancelledError, GeneratorExit):
        raise
    tracker = StreamProtocolTracker()
    for item in emitted:
        tracker.observe(item)
    return tracker.finalize(), "guarded_stream"


def execute_fixture(fixture: Fixture, *, repetition: int, base_commit: str) -> dict[str, Any]:
    source = CountingSource(fixture.physical_fragments, fixture.source_error() if fixture.source_error else None, fixture.infinite, fixture.close_fails)
    propagated = ""
    try:
        outcome, interpretation = _tracker_run(source) if fixture.mode == "tracker" else _guard_run(source)
        actual, code = _classify(outcome)
    except (asyncio.CancelledError, GeneratorExit) as exc:
        propagated = type(exc).__name__
        actual, code, interpretation = "source_cancelled", propagated, "propagated"
        outcome = None
    finally:
        cleanup_error = ""
        try:
            source.close()
        except BaseException as exc:
            cleanup_error = type(exc).__name__
    if cleanup_error:
        actual, code = "cleanup_failure", cleanup_error
    raw = b"".join(_bytes(value) for value in fixture.physical_fragments)
    receipt = outcome.receipt if outcome else {}
    terminal = {
        "finish_count": receipt.get("finish_count", 0),
        "done_count": receipt.get("done_count", 0),
        "first_finish_sequence": receipt.get("first_finish_sequence"),
        "first_done_sequence": receipt.get("first_done_sequence"),
        "ordering": "finish_before_done" if receipt.get("first_finish_sequence", 0) < receipt.get("first_done_sequence", 0) else "invalid_or_missing",
    }
    record = {
        "schema_version": SCHEMA_VERSION,
        "fixture_id": fixture.fixture_id,
        "repetition": repetition,
        "base_commit": base_commit,
        "parser_version": PARSER_VERSION,
        "mode": fixture.mode,
        "physical_fragments": [_encoded_fragment(item) for item in fixture.physical_fragments],
        "raw_bytes": len(raw),
        "logical_chunks": list(fixture.logical_chunks),
        "expected_outcome": fixture.expected_outcome,
        "expected_code": fixture.expected_code,
        "actual_outcome": actual,
        "actual_code": code,
        "response_sha256": receipt.get("response_sha256", ""),
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
        "semantic_counts": dict(sorted(receipt.get("semantic_event_counts", {}).items())),
        "transport_counts": dict(sorted(receipt.get("transport_event_counts", {}).items())),
        "terminal": terminal,
        "done_presence": bool(receipt.get("done_count")),
        "source_accounting": {"pull_count": source.pulls, "close_count": source.closes, "cleanup_error": cleanup_error},
        "client_interpretation": interpretation,
        "deterministic_replay": "pending",
        "propagated": propagated,
    }
    record["pass"] = actual == fixture.expected_outcome and (not fixture.expected_code or code == fixture.expected_code)
    return record


def replay_fixture(fixture: Fixture, *, base_commit: str, repetitions: int = 3) -> list[dict[str, Any]]:
    if repetitions < 3:
        raise ValueError("Round 2 fixtures require at least three repetitions")
    records = [execute_fixture(fixture, repetition=index + 1, base_commit=base_commit) for index in range(repetitions)]
    baseline = {key: records[0][key] for key in INVARIANT_FIELDS}
    stable = all({key: record[key] for key in INVARIANT_FIELDS} == baseline for record in records[1:])
    status = "reproducible" if stable else "replay_divergence"
    for record in records:
        record["deterministic_replay"] = status
        record["pass"] = record["pass"] and stable
    return records


def atomic_write_jsonl_and_summary(records: Iterable[dict[str, Any]], output_dir: Path) -> tuple[Path, Path]:
    ordered = sorted(records, key=lambda row: (row["fixture_id"], row["repetition"]))
    output_dir.mkdir(parents=True, exist_ok=True)
    jsonl = output_dir / "results.jsonl"
    summary = output_dir / "summary.json"
    _atomic_write(jsonl, "".join(json.dumps(row, sort_keys=True, ensure_ascii=False, separators=(",", ":")) + "\n" for row in ordered))
    summary_body = {
        "schema_version": SCHEMA_VERSION,
        "record_count": len(ordered),
        "fixture_ids": sorted({row["fixture_id"] for row in ordered}),
        "failed_count": sum(not row["pass"] for row in ordered),
        "replay_statuses": sorted({row["deterministic_replay"] for row in ordered}),
    }
    _atomic_write(summary, json.dumps(summary_body, sort_keys=True, ensure_ascii=False, indent=2) + "\n")
    return jsonl, summary


def _atomic_write(path: Path, content: str) -> None:
    fd, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent, text=True)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if os.path.exists(temp_name):
            os.unlink(temp_name)
