# WU-R2-MATRIX stop record

## Decision
Stopped before deterministic replay. The authoritative harness has a proven provenance defect: `BASE_COMMIT` is `de52b8ac571f77b130a3c20919cd1666a4af3ce5`, while the required base and current isolated-worktree HEAD are `b897d2b4145b21f7aacfeca32af7fef478746d46`.

## Why this blocks execution
The harness writes its `BASE_COMMIT` into every record. Running the requested 320+ records would therefore persist evidence attributed to the wrong revision. Per the lane instruction, the harness was not patched and replay/stress execution stopped.

## Evidence
- Authoritative harness: `tests/stream_round2/harness.py`
- Baseline preserved: `artifacts/hive-stream-round2-fixture-validation-20260727/harness/fixture-matrix.json` (96 records)
- No product code, fixture expectation, network/provider, service, merge, deploy, or smoke activity occurred.

## Unperformed required observations
Stable ordering/invariants/hashes, boundary stress, close-failure repetition, statistics, and independent output-hash validation are **not assessed**, not passed. No `fixture-matrix.json`, JSONL, or summary was emitted under `results` because their provenance would be invalid.

## Dissent and recommendation
Dissent: a harness-only deterministic gate could still run, but it would not meet the explicit requested-base provenance requirement. Recommendation: correct the authoritative harness provenance outside this lane, then rerun the full matrix from the stated base. `WU-R2-MATRIX` remains incomplete.
