# Round 2 bounded harness design

## Scope
Validate the Round 1 protocol receipt against recorded fixtures first. Provider trials are optional and must not use production thresholds or high-cost requests.

## Remediation carry-forward
- Treat `[DONE]` as a success-only sentinel. Failure fixtures must contain a structured error and error finish but **no** `[DONE]`; report any consumer that treats socket close or `finish_reason: "error"` as success.
- Record both raw-byte and logical-chunk counts. Exercise the 500,000-character and 10,000-chunk guard thresholds independently, including many tiny logical frames, and assert that retained output is bounded after a breach.
- Exercise `asyncio.CancelledError` and `GeneratorExit` as propagation fixtures. They must not be converted to error receipts or terminal frames; verify finally-safe upstream `close()` without adding tracker finalization that changes disconnect teardown.
- Preserve dissent: arbitrary byte-fragment reassembly is not implemented by the logical-frame guard. Add adapter-specific byte-fragment/multi-line SSE fixtures before proposing a reassembler.

## Phases and timing
1. **Request setup**: record start time, requested provider/model, correlation ID, fixture/trial ID.
2. **First-byte phase**: record first raw frame timestamp, raw-byte count, and logical-chunk count.
3. **Stream phase**: record raw-frame sequence, bytes, chunks, normalized classification, and post-terminal frames; do not interpret inter-frame gaps as token cadence.
4. **Terminal/receipt phase**: record finish/done ordering, response hash, route receipt, outcome, quarantine state, and whether `[DONE]` was present only on success.
5. **Replay phase**: replay identical raw frames and require the same response hash, terminal classification, normalized count, and failure-sentinel behavior.

## Fixture matrix
- Clean stream, empty-after-metadata, delimiter-only, malformed-only, missing finish, missing done, duplicate terminal, post-terminal content, and route substitution.
- Upstream ordinary exception after metadata: structured error + error finish + no `[DONE]`.
- `asyncio.CancelledError` and `GeneratorExit`: propagation with no synthesized terminal receipt.
- Character-limit breach and chunk-limit breach, each with an infinite/counting source proving bounded pulls and close invocation. Include the standard generator to upstream parser/response cleanup chain.
- Logical multi-frame and adapter-specific byte-fragment/multi-line SSE cases; the latter are discovery fixtures, not an implied repair policy.

## Trial plan and stop conditions
- If already-authorized local provider configuration supports it, run at most one harmless smoke request per selected adapter; otherwise stop at fixtures.
- Run each fixture at least three times to detect local nondeterminism. Preserve raw bytes only for anomalies and redact request content.
- Stop the lane on a silent HTTP-success empty outcome, a failure `[DONE]`, terminal-order failure, replay hash divergence, duplicate terminal, post-terminal mutation, or cancellation/disconnect conversion.
- Feed each anomaly back into a fixture before another provider trial. Stop provider trials on authentication failure, unexpected cost/rate-limit evidence, or any indication that a production service would be touched.

## Calibration and exclusions
Collect per-provider raw-byte/event/chunk distributions and terminal outcomes separately. Establish only descriptive baselines until a representative sample and operator review exist; no thresholds from this harness are production-ready.

No hidden-reasoning inference, speculative splice repair, output-hygiene blocking policy expansion, production threshold enforcement, or generic byte-fragment repair is in scope.
