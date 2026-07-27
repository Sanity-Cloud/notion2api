# WU-R2-MATRIX completed execution

## Supersession
This completed record supersedes the stopped execution documented in commit `25eee7c`; that stop record remains preserved in Git history. The authoritative provenance correction from `7de68ae` distinguishes the product subject (`de52b8ac571f77b130a3c20919cd1666a4af3ce5`), harness content (`de2eff0c8192c2ae4242f57da8b883dc34a6df1f`), and harness gate (`b897d2b4145b21f7aacfeca32af7fef478746d46`). Lane HEAD is not interpreted as the product subject.

## Matrix and replay
Executed 320 records: all 32 fixtures, 10 repetitions each. Determinism, invariants, ordering, raw/response hashes, pull/close counts, DONE presence, classifications, operational outcomes, and adjudication fields were compared across repetitions.

## Baseline and stress
The preserved 96-record baseline was behaviorally compared with the corresponding expanded repetitions. Boundary stress ran character sizes 9/10/11 against limit 10 and chunk counts 2/3/4 against limit 3, with finite and infinite sources. A separate 10-run close-failure replay preserved underlying stream success while reporting the deliberate source-double cleanup failure.

## Findings
See `summary.json`, `statistics.json`, `boundary-stress.json`, and `independent-hash-validation.json` for exact records and counts. Smoke, network/provider calls, services, merge, and deployment were prohibited and not run.

