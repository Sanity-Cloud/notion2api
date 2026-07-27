# Encoding decision table

| Area | Decision | Evidence |
|---|---|---|
| Raw byte accounting | PASS | All 48 tracker observations equal independently computed bytes and SHA-256. |
| Visible accounting | PASS | Python code-point counts and SHA-256 of UTF-8 visible text match receipts. |
| Unicode normalization | ABSENT | NFC `é` and NFD `e\u0301` retain distinct counts, raw hashes, and response hashes. |
| CRLF/LF | PASS | Raw hashes differ (148 vs 142 bytes); visible `line` hash is identical. |
| Invalid UTF-8 tracker | PASS WITH RISK | `0xff` and `0xfe` retain distinct raw hashes but decode to U+FFFD and converge to one response hash. |
| Invalid UTF-8 metadata | PASS WITH RISK | Raw invalid metadata is preserved in raw accounting; decoding is replacement-based. |
| Split multibyte bytes | EXPECTED LIMIT | Independent chunk decoding does not reassemble fragments; result is empty-visible-output. |
| Lone surrogate | EXPECTED LIMIT | `str.encode(errors='replace')` yields literal `?`, not U+FFFD. |
| Guard bytes boundary | DEFECT CANDIDATE | Guard stringifies bytes (`str(raw_chunk)`), producing malformed-frame error instead of byte-preserving tracking. |
| Replay | PASS | Every case reproduced identically across three repetitions. |

**Recommendation:** do not alter Unicode normalization. Preserve raw and visible hashes together. Open a separate, minimized patch decision for the guard's bytes-boundary type contract; current guard annotation is `Iterable[str]`, so this is an integration-boundary risk rather than a confirmed in-contract product defect.
