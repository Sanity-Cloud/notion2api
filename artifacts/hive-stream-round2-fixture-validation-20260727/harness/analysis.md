# WU-R2-HARNESS closure analysis

## Preserved draft evidence

The initial 78-run / 15-mismatch matrix is preserved at `artifacts/hive-stream-round2-fixture-validation-20260727/draft/initial-78-run-15-mismatch-fixture-matrix.json` with SHA-256 `bffbd1102a3d704825828fe39c11cdd04b1b2ea379878231bfa8edc5367d80f6`. It was not deleted or overwritten.

## Consolidated offline harness

`tests/stream_round2/harness.py` is the authoritative version-2 implementation. The CLI is a thin importer/caller only. The regenerated matrix contains **96 deterministic runs** across 32 fixtures: **84 pass**, **3 fail**, and **9 needs adjudication**. There are **0 nondeterministic repetitions** and **0 false-success violations**.

The three failures are the close-failure fixture repetitions. Each preserves `underlying_stream_outcome=success` while setting `operational_outcome=cleanup_failure` and `result=fail`; cleanup is a hard product failure. Therefore **harness_gate_pass=true** while **product_gate_pass=false**.

Needs-adjudication remains deliberately unchanged for done-before-finish (observed `stream_post_terminal_content`; `ERR_STREAM_TERMINAL_ORDER` is unreachable for that input under current precedence) and for multiline / physical-fragment framing discoveries (underlying `stream_empty_visible_output`).

## Validation and release posture

Focused harness plus inherited stream tests passed (**61 passed**); Ruff and compileall passed; the CLI regenerated JSON, JSONL, and summary. Work used no network, provider, service, merge, deploy, or downstream lane execution.

Dependencies recorded for this work unit: WU-R2-MATRIX, WU-R2-CONSUMERS, WU-R2-ENCODING, and WU-R2-FRAMING. **Downstream fixture lanes must not proceed while smoke remains held**, because the product gate is failed by cleanup handling. The exact file hashes and pre-final-amend commit are in `receipt.json`.
## Provenance schema correction

The subject under test is `de52b8ac571f77b130a3c20919cd1666a4af3ce5`. The reusable harness implementation is `de2eff0c8192c2ae4242f57da8b883dc34a6df1f`, and its management gate is `b897d2b4145b21f7aacfeca32af7fef478746d46`. The legacy `base_commit` field remains an alias for the subject commit; new explicit fields prevent lane-worktree revisions from being confused with the product revision under test. The version-2 JSON schema now matches the actual record shape.

Diagnostic matrix, consumer, encoding, and framing lanes may proceed from the gate revision. Smoke remains held while `product_gate_pass=false`.

