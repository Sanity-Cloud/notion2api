# WU-R2-HARNESS analysis

The Round 2 harness is deliberately offline and bounded: it sends supplied physical fragments directly to `StreamProtocolTracker` or through the guarded stream, then emits versioned per-repetition records. Physical byte/string fragments are hex-preserved, while logical SSE chunks remain separately declared so fragmented or multiline data is observable rather than repaired.

Three or more repetitions compare stable invariant fields. JSONL and summary JSON are written through temporary siblings and atomic replacement in stable fixture/repetition order. The schema captures terminal ordering, `[DONE]`, raw and response digests, source pull/close accounting, client interpretation, and replay status.

Focused tests cover schema shape, replay invariants, source accounting (including an infinite guard source and close failure), atomic output, and explicit false-success, empty, terminal-corruption, post-terminal, cleanup, and unsupported-framing classifications. No full matrix, network request, provider call, deployment, or service action was performed.
