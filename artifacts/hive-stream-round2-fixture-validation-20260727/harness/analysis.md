# WU-R2-HARNESS analysis

## Scope and evidence preservation

The pre-existing `artifacts/hive-stream-round2-fixture-validation-20260727/fixture-matrix.json` remains untouched as the initial draft evidence (78 runs, 15 reported mismatches). Corrected outputs are isolated in this `harness/` directory. No provider, network, service, merge, or product-code changes were made.

## Corrected result

- 32 fixtures × 3 deterministic repetitions = **96 runs**.
- **87 pass**, **9 needs_adjudication**, **0 fail**.
- Harness implementation failures: **0**.
- Nondeterministic repetitions: **0**.
- False-success violations: **0**.
- Gate: **PASS**.

Guard-error fixtures now extract the emitted structured error event's top-level `stream_receipt`, instead of reparsing error-only emissions as provider stream frames. Before-content is recorded as `stream_empty_no_terminal` / `ERR_PROVIDER_EMPTY_STREAM`; after-partial is `stream_interrupted` / `ERR_STREAM_INTERRUPTED`. Input and emitted DONE state, emitted finish reason, source completion, pull/close accounting, close errors, propagated exceptions, and client interpretation are independent fields.

## Product adjudication questions

1. **spec_disagreement — done-before-finish (3 repetitions):** the tracker increments `post_terminal_count` for every non-DONE event after DONE before it reaches the terminal-order comparison. Current observed classification is `stream_post_terminal_content`; the semantic expectation remains `stream_terminal_order_invalid` / `ERR_STREAM_TERMINAL_ORDER`. The fixture is intentionally `needs_adjudication`, not a pass. Its executable test preserves the reachability evidence.
2. **unsupported_framing — multiline data (3) and physical byte fragmentation (3):** current product observation is `stream_empty_visible_output`, including the recorded event counts. The preserved semantic intent is malformed-only. These six runs are discovery outcomes and require framing-policy adjudication rather than a harness failure.

## Coverage added

Close failure; Unicode emoji; combining marks; CRLF/LF; invalid UTF-8 replacement; complete logical chunks; multiline and physical framing discovery; cancellation; GeneratorExit; finite/infinite sources; character/chunk limits; terminal anomalies; route states; quarantine; stable JSON/JSONL/summary output; temp cleanup; simulated atomic replacement failure.

## Commands and results

```text
python scripts/round2_stream_fixture_harness.py artifacts/hive-stream-round2-fixture-validation-20260727/harness
=> gate_pass=true; total_runs=96; pass=87; needs_adjudication=9; fail=0

python -m pytest tests/stream_round2/test_fixture_harness.py -q
=> 9 passed

python -m pytest tests/test_stream_protocol.py tests/test_stream_integrity_guard.py -q
=> 23 passed

python -m ruff check scripts/round2_stream_fixture_harness.py tests/stream_round2/test_fixture_harness.py
=> All checks passed

python -m compileall -q scripts/round2_stream_fixture_harness.py tests/stream_round2/test_fixture_harness.py
=> passed

git diff --check
=> passed

encoding guard (UTF-8 no BOM/NUL)
=> passed
```

## Release recommendation

**Release WU-R2-HARNESS to the next lane for adjudication only.** The harness is complete and durable; do not treat the nine discovery runs as harness defects. Product policy decisions are required before any framing or terminal-order product change.

**WU-R2-HARNESS: COMPLETED**
