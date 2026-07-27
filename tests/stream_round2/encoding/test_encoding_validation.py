from __future__ import annotations

from tests.stream_round2.encoding.encoding_validation import cases, run


def rows(case_id: str):
    return [row for row in run() if row["case_id"] == case_id]


def test_all_encoding_cases_match_independent_byte_and_visible_oracles():
    records = run()
    assert len(records) == len(cases()) * 3
    assert all(record["consistent"] for record in records)
    assert {record["deterministic_replay_status"] for record in records} == {"reproducible"}


def test_unicode_normalization_is_absent_and_line_endings_only_change_raw_hash():
    composed = rows("composed-e-acute")[0]["observed"]
    decomposed = rows("decomposed-e-acute")[0]["observed"]
    assert composed["response_sha256"] != decomposed["response_sha256"]
    lf = rows("sse-lf")[0]["observed"]
    crlf = rows("sse-crlf")[0]["observed"]
    assert lf["raw_stream_sha256"] != crlf["raw_stream_sha256"]
    assert lf["response_sha256"] == crlf["response_sha256"]


def test_replacement_preserves_distinct_raw_hashes_but_converges_visible_hash():
    ff = rows("invalid-utf8-content-ff")[0]["observed"]
    fe = rows("invalid-utf8-content-fe")[0]["observed"]
    assert ff["raw_stream_sha256"] != fe["raw_stream_sha256"]
    assert ff["response_sha256"] == fe["response_sha256"]
    assert rows("split-multibyte-byte-sequence")[0]["observed"]["visible_chars"] == 0


def test_lone_surrogate_and_control_bytes_follow_replace_encoding_contract():
    surrogate = rows("lone-surrogate-replaced-on-string-encode")[0]
    controls = rows("embedded-nul-controls")[0]
    assert surrogate["expected"]["visible_text_escaped"] == "?"
    assert controls["expected"]["visible_text_escaped"] == "A\\x00B\\x1fC"


def test_guard_boundary_rejects_raw_byte_chunks_instead_of_preserving_them():
    import os

    os.environ.setdefault(
        "NOTION_ACCOUNTS",
        '[{"token_v2":"fixture","space_id":"fixture","user_id":"fixture"}]',
    )
    from app.api import chat

    source = [
        b'data: {"choices":[{"delta":{"content":"\xff"},"finish_reason":null}]}\n\n',
        b'data: {"choices":[{"delta":{},"finish_reason":"stop"}]}\n\n',
        b"data: [DONE]\n\n",
    ]
    emitted = "".join(chat._guard_stream_until_integrity(source, response_id="id", model="model"))
    assert "ERR_STREAM_MALFORMED_ONLY" in emitted
    assert "data: [DONE]" not in emitted
