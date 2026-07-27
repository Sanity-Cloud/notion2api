"""Offline Unicode and byte-accounting validation for Round 2 streams."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

from app.stream_protocol import StreamProtocolTracker

RUN_ID = "hive-stream-round2-fixture-validation-20260727"
REPETITIONS = 3


@dataclass(frozen=True)
class Case:
    case_id: str
    chunks: tuple[bytes | str, ...]
    expected_visible: str
    boundary: str = "tracker"


def stream_frame(content: str, newline: str = "\n") -> str:
    payload = {"choices": [{"delta": {"content": content}, "finish_reason": None}]}
    return "data: " + json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + newline + newline


def finish(newline: str = "\n") -> str:
    payload = {"choices": [{"delta": {}, "finish_reason": "stop"}]}
    return "data: " + json.dumps(payload, separators=(",", ":")) + newline + newline


def completed(content: str, newline: str = "\n") -> tuple[bytes | str, ...]:
    return (stream_frame(content, newline), finish(newline), "data: [DONE]" + newline + newline)


def invalid_content(value: bytes) -> tuple[bytes | str, ...]:
    return (
        b'data: {"choices":[{"delta":{"content":"' + value + b'"},"finish_reason":null}]}\n\n',
        finish(),
        "data: [DONE]\n\n",
    )


def cases() -> tuple[Case, ...]:
    surrogate = 'data: {"choices":[{"delta":{"content":"\ud800"},"finish_reason":null}]}\n\n'
    split = (
        b'data: {"choices":[{"delta":{"content":"\xf0\x9f',
        b'\x98\x80"},"finish_reason":null}]}\n\n',
        finish().encode(),
        b"data: [DONE]\n\n",
    )
    return (
        Case("ascii", completed("ASCII"), "ASCII"),
        Case("bmp-cjk", completed("漢"), "漢"),
        Case("emoji-non-bmp", completed("😀"), "😀"),
        Case("composed-e-acute", completed("é"), "é"),
        Case("decomposed-e-acute", completed("e\u0301"), "e\u0301"),
        Case("zwj-emoji", completed("👩\u200d💻"), "👩\u200d💻"),
        Case("variation-selector", completed("✈️"), "✈️"),
        Case("embedded-nul-controls", completed("A\x00B\x1fC"), "A\x00B\x1fC"),
        Case("visible-crlf", completed("A\r\nB"), "A\r\nB"),
        Case("sse-lf", completed("line", "\n"), "line"),
        Case("sse-crlf", completed("line", "\r\n"), "line"),
        Case("lone-surrogate-replaced-on-string-encode", (surrogate, finish(), "data: [DONE]\n\n"), "?"),
        Case("invalid-utf8-content-ff", invalid_content(b"\xff"), "�"),
        Case("invalid-utf8-content-fe", invalid_content(b"\xfe"), "�"),
        Case("invalid-utf8-metadata", (b'data: {"type":"model_metadata","model":"\xff"}\n\n', *completed("ok")), "ok"),
        Case("split-multibyte-byte-sequence", split, ""),
    )


def as_bytes(chunks: Iterable[bytes | str]) -> bytes:
    return b"".join(value if isinstance(value, bytes) else value.encode("utf-8", "replace") for value in chunks)


def observe(chunks: tuple[bytes | str, ...]) -> dict[str, object]:
    tracker = StreamProtocolTracker()
    for chunk in chunks:
        tracker.observe(chunk)
    outcome = tracker.finalize()
    return dict(outcome.receipt, code=outcome.code, ok=outcome.ok)


def independent(case: Case) -> dict[str, object]:
    raw = as_bytes(case.chunks)
    visible = case.expected_visible
    return {
        "raw_bytes": len(raw),
        "raw_sha256": hashlib.sha256(raw).hexdigest(),
        "visible_code_points": len(visible),
        "visible_utf8_bytes": len(visible.encode("utf-8", "replace")),
        "visible_sha256": hashlib.sha256(visible.encode("utf-8", "replace")).hexdigest(),
        "visible_text_escaped": visible.encode("unicode_escape").decode("ascii"),
    }


def execute(case: Case, repetition: int) -> dict[str, object]:
    expected = independent(case)
    receipt = observe(case.chunks)
    return {
        "run_id": RUN_ID,
        "case_id": case.case_id,
        "repetition": repetition,
        "boundary": case.boundary,
        "raw_frame_hex": [as_bytes((chunk,)).hex() for chunk in case.chunks],
        "expected": expected,
        "observed": {
            "raw_stream_bytes": receipt["raw_stream_bytes"],
            "raw_stream_sha256": receipt["raw_stream_sha256"],
            "visible_chars": receipt["visible_chars"],
            "response_sha256": receipt["response_sha256"],
            "classification": receipt["classification"],
            "code": receipt["code"],
            "ok": receipt["ok"],
        },
        "consistent": (
            receipt["raw_stream_bytes"] == expected["raw_bytes"]
            and receipt["raw_stream_sha256"] == expected["raw_sha256"]
            and receipt["visible_chars"] == expected["visible_code_points"]
            and receipt["response_sha256"] == expected["visible_sha256"]
        ),
    }


def stable(row: dict[str, object]) -> dict[str, object]:
    return {key: value for key, value in row.items() if key != "repetition"}


def run() -> list[dict[str, object]]:
    rows = [execute(case, repetition) for case in cases() for repetition in range(1, REPETITIONS + 1)]
    for case in cases():
        group = [row for row in rows if row["case_id"] == case.case_id]
        reproducible = all(stable(row) == stable(group[0]) for row in group)
        for row in group:
            row["deterministic_replay_status"] = "reproducible" if reproducible else "nondeterministic"
    return rows


def write(output: Path) -> dict[str, object]:
    rows = run()
    output.mkdir(parents=True, exist_ok=True)
    convergence = [row for row in rows if row["case_id"] in {"invalid-utf8-content-ff", "invalid-utf8-content-fe"} and row["repetition"] == 1]
    composed = next(row for row in rows if row["case_id"] == "composed-e-acute" and row["repetition"] == 1)
    decomposed = next(row for row in rows if row["case_id"] == "decomposed-e-acute" and row["repetition"] == 1)
    summary = {
        "run_id": RUN_ID,
        "repetitions": REPETITIONS,
        "case_count": len(cases()),
        "all_consistent": all(bool(row["consistent"]) for row in rows),
        "all_reproducible": all(row["deterministic_replay_status"] == "reproducible" for row in rows),
        "canonicalization": {
            "unicode_normalization": "absent",
            "composed_and_decomposed_response_hashes_differ": composed["observed"]["response_sha256"] != decomposed["observed"]["response_sha256"],
            "line_ending_raw_hashes_differ_visible_hashes_match": next(row for row in rows if row["case_id"] == "sse-lf" and row["repetition"] == 1)["observed"]["raw_stream_sha256"] != next(row for row in rows if row["case_id"] == "sse-crlf" and row["repetition"] == 1)["observed"]["raw_stream_sha256"],
        },
        "replacement_convergence": {
            "raw_hashes_differ": convergence[0]["observed"]["raw_stream_sha256"] != convergence[1]["observed"]["raw_stream_sha256"],
            "visible_hashes_match": convergence[0]["observed"]["response_sha256"] == convergence[1]["observed"]["response_sha256"],
            "visible_text": "U+FFFD",
        },
    }
    (output / "encoding-results.json").write_text(json.dumps(rows, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    (output / "encoding-summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary
