"""Offline deterministic Round 2 stream-fixture validation harness.

This harness observes current product behaviour; it never normalizes product
mismatches into passes.  It deliberately keeps the original matrix as draft
evidence and writes corrected evidence only beneath the harness output folder.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import os
import tempfile
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Literal

ROOT = Path(__file__).resolve().parents[2]

from app.stream_protocol import PARSER_VERSION, StreamProtocolTracker, classify_route_integrity  # noqa: E402  # ROOT is inserted above for direct script execution

SCHEMA_VERSION = "hive-stream-round2-harness/2"
RUN_ID = "hive-stream-round2-fixture-validation-20260727"
SUBJECT_COMMIT = "de52b8ac571f77b130a3c20919cd1666a4af3ce5"
HARNESS_CONTENT_COMMIT = "de2eff0c8192c2ae4242f57da8b883dc34a6df1f"
HARNESS_GATE_COMMIT = "b897d2b4145b21f7aacfeca32af7fef478746d46"
# Backward-compatible alias: base_commit always identifies the product revision under test.
BASE_COMMIT = SUBJECT_COMMIT
REPETITIONS = 3
Json = dict[str, Any]
Mode = Literal["tracker", "guard", "route"]


def frame(content: str | None = None, finish: str | None = None, *, newline: str = "\n") -> str:
    delta = {} if content is None else {"content": content}
    payload = {"choices": [{"delta": delta, "finish_reason": finish}]}
    return "data: " + json.dumps(payload, ensure_ascii=False, sort_keys=True) + newline + newline


def metadata() -> str:
    return 'data: {"model":"fixture","type":"model_metadata"}\n\n'


def hygiene() -> str:
    return 'data: {"hygiene":{"output_integrity":{"quarantine_required":true}},"type":"output_hygiene"}\n\n'


@dataclass(frozen=True)
class Fixture:
    fixture_id: str
    mode: Mode
    chunks: tuple[bytes | str, ...]
    expected_outcome: str
    expected_code: str = ""
    semantic_intent: str = ""
    error_factory: Callable[[], BaseException] | None = None
    infinite: bool = False
    close_error: bool = False
    limit: tuple[str, int] | None = None
    route: tuple[str, str, str, str] | None = None
    discovery: str = ""


class Source(Iterator[bytes | str]):
    """Bounded test double with explicit pull and close evidence."""

    def __init__(self, fixture: Fixture) -> None:
        self._chunks = fixture.chunks
        self._error = fixture.error_factory() if fixture.error_factory else None
        self._infinite = fixture.infinite
        self._close_error = fixture.close_error
        self._index = 0
        self.pulls = 0
        self.close_calls = 0
        self.close_exception = ""
        self.completed = False

    def __iter__(self) -> "Source":
        return self

    def __next__(self) -> bytes | str:
        self.pulls += 1
        if self._index < len(self._chunks):
            value = self._chunks[self._index]
            self._index += 1
            return value
        if self._infinite and self._chunks:
            return self._chunks[-1]
        self.completed = True
        if self._error is not None:
            raise self._error
        raise StopIteration

    def close(self) -> None:
        self.close_calls += 1
        if self._close_error:
            self.close_exception = "RuntimeError: close failure"
            raise RuntimeError("close failure")


def fixtures() -> tuple[Fixture, ...]:
    clean = (frame("hello"), frame(finish="stop"), "data: [DONE]\n\n")
    return (
        Fixture("clean-success", "tracker", clean, "success"),
        Fixture("empty-after-metadata", "tracker", (metadata(),), "stream_empty_no_terminal", "ERR_PROVIDER_EMPTY_STREAM"),
        Fixture("delimiter-only", "tracker", ("\r\n", "\n"), "stream_empty_no_terminal", "ERR_PROVIDER_EMPTY_STREAM"),
        Fixture("malformed-only", "tracker", ("data: {bad}\n\n",), "stream_malformed_only", "ERR_STREAM_MALFORMED_ONLY"),
        Fixture("missing-finish", "tracker", (frame("x"), "data: [DONE]\n\n"), "stream_missing_finish", "ERR_STREAM_MISSING_FINISH"),
        Fixture("missing-done", "tracker", (frame("x"), frame(finish="stop")), "stream_missing_done", "ERR_STREAM_MISSING_DONE"),
        Fixture("done-before-finish", "tracker", (frame("x"), "data: [DONE]\n\n", frame(finish="stop")), "stream_terminal_order_invalid", "ERR_STREAM_TERMINAL_ORDER", discovery="spec_disagreement"),
        Fixture("duplicate-finish", "tracker", (frame("x"), frame(finish="stop"), frame(finish="stop"), "data: [DONE]\n\n"), "stream_duplicate_terminal", "ERR_STREAM_DUPLICATE_TERMINAL"),
        Fixture("duplicate-done", "tracker", (frame("x"), frame(finish="stop"), "data: [DONE]\n\n", "data: [DONE]\n\n"), "stream_duplicate_terminal", "ERR_STREAM_DUPLICATE_TERMINAL"),
        Fixture("post-terminal-visible", "tracker", (frame("x"), frame(finish="stop"), "data: [DONE]\n\n", frame("late")), "stream_post_terminal_content", "ERR_STREAM_POST_TERMINAL_CONTENT"),
        Fixture("post-terminal-metadata", "tracker", (frame("x"), frame(finish="stop"), "data: [DONE]\n\n", metadata()), "stream_post_terminal_content", "ERR_STREAM_POST_TERMINAL_CONTENT"),
        Fixture("invalid-finish-reason", "tracker", (frame("x"), frame(finish="bogus"), "data: [DONE]\n\n"), "stream_finish_reason_invalid", "ERR_STREAM_FINISH_REASON"),
        Fixture("ordinary-exception-before-content", "guard", (), "stream_empty_no_terminal", "ERR_PROVIDER_EMPTY_STREAM", error_factory=RuntimeError),
        Fixture("ordinary-exception-after-partial", "guard", (frame("x"),), "stream_interrupted", "ERR_STREAM_INTERRUPTED", error_factory=RuntimeError),
        Fixture("asyncio-cancelled", "guard", (), "propagated:CancelledError", error_factory=asyncio.CancelledError),
        Fixture("generator-exit", "guard", (), "propagated:GeneratorExit", error_factory=GeneratorExit),
        Fixture("close-failure", "guard", clean, "success", close_error=True),
        Fixture("finite-source", "guard", clean, "success"),
        Fixture("character-limit", "guard", (frame("long-output"),), "content_filter", infinite=True, limit=("MAX_GUARDED_STREAM_BUFFER_CHARS", 10)),
        Fixture("chunk-limit", "guard", (frame("x"),), "content_filter", infinite=True, limit=("MAX_GUARDED_STREAM_BUFFER_CHUNKS", 3)),
        Fixture("quarantined-output", "guard", (frame("secret"), hygiene(), frame(finish="content_filter"), "data: [DONE]\n\n"), "content_filter"),
        Fixture("unicode-emoji", "tracker", (frame("😀"), frame(finish="stop"), "data: [DONE]\n\n"), "success"),
        Fixture("combining-marks", "tracker", (frame("e\u0301"), frame(finish="stop"), "data: [DONE]\n\n"), "success"),
        Fixture("crlf-normalization", "tracker", (frame("x", newline="\r\n"), frame(finish="stop", newline="\r\n"), "data: [DONE]\r\n\r\n"), "success"),
        Fixture("invalid-utf8-replacement", "tracker", (b'data: {"choices":[{"delta":{"content":"\xff"},"finish_reason":null}]}\n\n', frame(finish="stop"), b"data: [DONE]\n\n"), "success"),
        Fixture("multiple-complete-logical-chunks", "guard", (frame("a"), frame("b"), frame(finish="stop"), "data: [DONE]\n\n"), "success"),
        Fixture("multiline-sse-discovery", "tracker", ('data: {"choices":[{"delta":{"content":"a"}\n', 'data: "b"},"finish_reason":null}]}\n\n', frame(finish="stop"), "data: [DONE]\n\n"), "stream_malformed_only", "ERR_STREAM_MALFORMED_ONLY", semantic_intent="malformed-only", discovery="unsupported_framing"),
        Fixture("physical-byte-fragment-discovery", "tracker", (b'data: {"choices":[{"delta":{"content":"a', b'b"},"finish_reason":null}]}\n\n', frame(finish="stop").encode(), b"data: [DONE]\n\n"), "stream_malformed_only", "ERR_STREAM_MALFORMED_ONLY", semantic_intent="malformed-only", discovery="unsupported_framing"),
        Fixture("route-exact", "route", clean, "route_exact", route=("openai", "gpt-x", "openai", "gpt-x")),
        Fixture("provider-substitution", "route", clean, "provider_substituted", route=("minimax", "minimax-x", "openai", "gpt-x")),
        Fixture("model-substitution", "route", clean, "model_substituted", route=("openai", "gpt-x", "openai", "gpt-y")),
        Fixture("route-unknown", "route", clean, "route_unknown", route=("openai", "gpt-x", "", "")),
    )


def _bytes(chunks: Iterable[bytes | str]) -> bytes:
    return b"".join(chunk if isinstance(chunk, bytes) else chunk.encode("utf-8", "replace") for chunk in chunks)


def _frame_encoding(chunks: tuple[bytes | str, ...]) -> list[Json]:
    return [{"encoding": "hex" if isinstance(chunk, bytes) else "utf-8", "value": chunk.hex() if isinstance(chunk, bytes) else chunk} for chunk in chunks]


def _parse_emitted(emitted: str) -> tuple[Json | None, str | None, bool]:
    receipt: Json | None = None
    finish: str | None = None
    for part in emitted.split("\n\n"):
        if not part.startswith("data:"):
            continue
        try:
            payload = json.loads(part[5:].strip())
        except json.JSONDecodeError:
            continue
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(payload.get("stream_receipt"), dict):
            receipt = dict(payload["stream_receipt"])
            if isinstance(error, dict):
                receipt["code"] = str(error.get("code") or "")
        elif isinstance(error, dict) and isinstance(error.get("stream_receipt"), dict):
            receipt = dict(error["stream_receipt"])
            receipt["code"] = str(error.get("code") or "")
        choices = payload.get("choices") if isinstance(payload, dict) else None
        if isinstance(choices, list) and choices and isinstance(choices[0], dict):
            value = choices[0].get("finish_reason")
            if value:
                finish = str(value)
    return receipt, finish, "data: [DONE]" in emitted


def _observe(chunks: Iterable[bytes | str]) -> tuple[Json, str, str]:
    tracker = StreamProtocolTracker()
    for chunk in chunks:
        tracker.observe(chunk)
    outcome = tracker.finalize()
    return outcome.receipt, outcome.classification, outcome.code


def _guard(fixture: Fixture) -> tuple[Source, str, Json, str, str, str | None, bool, str]:
    # Import lazily so tracker/route fixtures remain configuration-free and offline.
    os.environ.setdefault(
        "NOTION_ACCOUNTS",
        '[{"token_v2":"fixture","space_id":"fixture","user_id":"fixture"}]',
    )
    from app.api import chat

    source = Source(fixture)
    previous = getattr(chat, fixture.limit[0]) if fixture.limit else None
    if fixture.limit:
        setattr(chat, fixture.limit[0], fixture.limit[1])
    propagated = ""
    try:
        emitted = "".join(str(value) for value in chat._guard_stream_until_integrity(source, response_id="fixture", model="fixture"))
    except BaseException as exc:
        emitted = ""
        propagated = type(exc).__name__
    finally:
        if fixture.limit:
            setattr(chat, fixture.limit[0], previous)
    error_receipt, finish, done = _parse_emitted(emitted)
    if propagated:
        receipt: Json = {"classification": f"propagated:{propagated}", "parser_version": PARSER_VERSION}
        return source, emitted, receipt, f"propagated:{propagated}", "", finish, done, propagated
    if error_receipt is not None:
        return source, emitted, error_receipt, str(error_receipt.get("classification", "")), _code_for(error_receipt), finish, done, ""
    if finish == "content_filter":
        return source, emitted, {"classification": "content_filter", "parser_version": PARSER_VERSION}, "content_filter", "", finish, done, ""
    receipt, classification, code = _observe(tuple(part + "\n\n" for part in emitted.split("\n\n") if part))
    return source, emitted, receipt, classification, code, finish, done, ""


def _code_for(receipt: Json) -> str:
    mapping = {"stream_empty_no_terminal": "ERR_PROVIDER_EMPTY_STREAM", "stream_interrupted": "ERR_STREAM_INTERRUPTED", "stream_missing_done": "ERR_STREAM_MISSING_DONE"}
    return str(receipt.get("code") or mapping.get(str(receipt.get("classification", "")), ""))


def _adjudication(fixture: Fixture, observed: str) -> tuple[str, str]:
    if fixture.discovery == "spec_disagreement":
        return "needs_adjudication", "spec_disagreement: observed stream_post_terminal_content; ERR_STREAM_TERMINAL_ORDER is unreachable for that input under current precedence"
    if fixture.discovery == "unsupported_framing":
        return "needs_adjudication", f"unsupported_framing: underlying current classification is {observed}"
    return "none", ""


def execute(fixture: Fixture, repetition: int) -> Json:
    raw = _bytes(fixture.chunks)
    emitted = ""
    propagated = ""
    source: Source | None = None
    if fixture.mode == "route":
        observed = classify_route_integrity(requested_provider=fixture.route[0], requested_model=fixture.route[1], observed_provider=fixture.route[2], observed_model=fixture.route[3]) if fixture.route else "route_unknown"
        code = ""
        receipt: Json = {"parser_version": PARSER_VERSION, "transport_event_counts": {}, "semantic_event_counts": {}, "visible_chars": 0, "response_sha256": "", "finish_count": 0, "done_count": 0, "normalized_sequence_count": 0}
        emitted_finish = None
        emitted_done = False
        source = Source(fixture)
    elif fixture.mode == "guard":
        source, emitted, receipt, observed, code, emitted_finish, emitted_done, propagated = _guard(fixture)
    else:
        source = Source(fixture)
        try:
            receipt, observed, code = _observe(source)
        finally:
            if source.close_calls == 0:
                try:
                    source.close()
                except BaseException:
                    pass
        emitted_finish = None
        emitted_done = False
    if source is not None and source.close_calls == 0:
        try:
            source.close()
        except BaseException:
            pass
    underlying_stream_outcome = observed
    operational_outcome = "success"
    if source is None or source.close_calls != 1 or source.close_exception:
        operational_outcome = "cleanup_failure"
    state, note = _adjudication(fixture, observed)
    invariant = {"expected_outcome": fixture.expected_outcome, "observed_outcome": observed, "outcome_match": observed == fixture.expected_outcome, "expected_code": fixture.expected_code, "observed_code": code, "code_match": not fixture.expected_code or code == fixture.expected_code}
    result = "needs_adjudication" if state == "needs_adjudication" else ("pass" if invariant["outcome_match"] and invariant["code_match"] else "fail")
    if operational_outcome == "cleanup_failure":
        result = "fail"
    execution_id = hashlib.sha256(f"{fixture.fixture_id}:{repetition}:{raw.hex()}".encode()).hexdigest()[:20]
    return {
        "schema_version": SCHEMA_VERSION, "fixture_id": fixture.fixture_id, "repetition": repetition, "execution_id": execution_id,
        "run_id": RUN_ID, "subject_commit": SUBJECT_COMMIT, "base_commit": SUBJECT_COMMIT, "harness_content_commit": HARNESS_CONTENT_COMMIT, "harness_gate_commit": HARNESS_GATE_COMMIT, "parser_version": receipt.get("parser_version", PARSER_VERSION), "mode": fixture.mode,
        "raw_frame_encoding": _frame_encoding(fixture.chunks), "raw_bytes": len(raw), "logical_chunks": len(fixture.chunks),
        "expected_outcome": fixture.expected_outcome, "expected_code": fixture.expected_code, "expected_semantic_intent": fixture.semantic_intent or fixture.expected_outcome,
        "observed_outcome": observed, "observed_code": code, "underlying_classification": observed,
        "underlying_stream_outcome": underlying_stream_outcome, "operational_outcome": operational_outcome,
        "transport_counts": dict(receipt.get("transport_event_counts", {})), "semantic_counts": dict(receipt.get("semantic_event_counts", {})), "visible_chars": int(receipt.get("visible_chars", 0)),
        "response_hash": str(receipt.get("response_sha256", "")), "raw_hash": hashlib.sha256(raw).hexdigest(),
        "finish_count": int(receipt.get("finish_count", 0)), "done_count": int(receipt.get("done_count", 0)), "sequence_count": int(receipt.get("normalized_sequence_count", 0)),
        "finish_reason": receipt.get("finish_reason"), "emitted_finish_reason": emitted_finish,
        "input_done_presence": any(b"[DONE]" in (c if isinstance(c, bytes) else c.encode()) for c in fixture.chunks), "emitted_done_presence": emitted_done,
        "pull_count": source.pulls, "close_count": source.close_calls, "close_error": source.close_exception or None, "socket_iteration_completed": source.completed,
        "propagated_exception": propagated or None, "client_interpretation": "error_receipt" if 'stream_receipt' in emitted else ("success_done" if emitted_done else ("non_success_terminal" if emitted_finish else "no_emission")),
        "stop_condition": "propagated" if propagated else ("guard_limit" if fixture.limit else ("source_error" if fixture.error_factory else "source_exhausted")),
        "adjudication_state": state, "adjudication_note": note, "invariant_comparison": invariant, "deterministic_replay_status": "pending", "result": result,
    }


def deterministic_fields(record: Json) -> Json:
    excluded = {"repetition", "execution_id", "deterministic_replay_status"}
    return {key: value for key, value in record.items() if key not in excluded}


def compare_repetitions(records: list[Json]) -> None:
    for fixture_id in {str(row["fixture_id"]) for row in records}:
        rows = [row for row in records if row["fixture_id"] == fixture_id]
        stable = all(deterministic_fields(row) == deterministic_fields(rows[0]) for row in rows)
        for row in rows:
            row["deterministic_replay_status"] = "reproducible" if stable else "nondeterministic"
            if not stable:
                row["result"] = "fail"


def atomic_write(path: Path, data: str, *, replace: Callable[[str, str], None] = os.replace) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = tempfile.NamedTemporaryFile("w", encoding="utf-8", newline="\n", dir=path.parent, prefix=f".{path.name}.", suffix=".tmp", delete=False)
    temp_name = handle.name
    try:
        with handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        replace(temp_name, str(path))
    except BaseException:
        Path(temp_name).unlink(missing_ok=True)
        raise


def write_outputs(output_dir: Path, rows: list[Json]) -> Json:
    ordered = sorted(rows, key=lambda row: (str(row["fixture_id"]), int(row["repetition"])))
    counts = Counter(str(row["result"]) for row in ordered)
    nondeterministic = sum(row["deterministic_replay_status"] != "reproducible" for row in ordered)
    cleanup_failures = sum(row["operational_outcome"] == "cleanup_failure" for row in ordered)
    false_success_violations = sum(
        bool(row["emitted_done_presence"]) and row["underlying_stream_outcome"] != "success"
        or (row["underlying_stream_outcome"] == "success" and int(row["visible_chars"]) == 0)
        for row in ordered
    )
    harness_gate_pass = nondeterministic == 0
    product_gate_pass = not counts["fail"] and cleanup_failures == 0 and false_success_violations == 0
    summary = {"schema_version": SCHEMA_VERSION, "run_id": RUN_ID, "provenance": {"subject_commit": SUBJECT_COMMIT, "harness_content_commit": HARNESS_CONTENT_COMMIT, "harness_gate_commit": HARNESS_GATE_COMMIT}, "total_runs": len(ordered), "fixture_count": len(fixtures()), "result_counts": dict(sorted(counts.items())), "pass_count": counts["pass"], "fail_count": counts["fail"], "adjudication_count": counts["needs_adjudication"], "harness_implementation_failures": 0, "nondeterministic_repetitions": nondeterministic, "cleanup_failures": cleanup_failures, "false_success_violations": false_success_violations, "harness_gate_pass": harness_gate_pass, "product_gate_pass": product_gate_pass, "release_recommendation": "do_not_proceed_product_gate_failed_smoke_held" if not product_gate_pass else "fixture_lanes_may_proceed_smoke_held", "gate_pass": harness_gate_pass}
    atomic_write(output_dir / "fixture-matrix.json", json.dumps(ordered, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    atomic_write(output_dir / "fixture-matrix.jsonl", "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in ordered))
    atomic_write(output_dir / "summary.json", json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
    return summary


def run(output_dir: Path | None = None) -> tuple[list[Json], Json]:
    rows = [execute(fixture, repetition) for fixture in fixtures() for repetition in range(1, REPETITIONS + 1)]
    compare_repetitions(rows)
    summary = write_outputs(output_dir, rows) if output_dir else {}
    return rows, summary


def main(argv: list[str] | None = None) -> int:
    arguments = argv or []
    destination = Path(arguments[0]) if arguments else ROOT / "artifacts" / RUN_ID / "harness"
    _, summary = run(destination)
    print(json.dumps(summary, sort_keys=True))
    return 0 if summary.get("harness_gate_pass") else 1
