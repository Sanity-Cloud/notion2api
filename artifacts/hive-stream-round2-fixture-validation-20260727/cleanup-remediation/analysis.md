# Round 2 cleanup remediation

## Contract

A successful source iteration is now successful only after source cleanup succeeds. A close failure after normal iteration emits `stream_source_cleanup_failed` / `ERR_STREAM_SOURCE_CLEANUP`, an error terminal, no `[DONE]`, and no buffered visible content. The receipt preserves the cleanup exception type while redacting its message from the client receipt; the chained exception remains available in logs.

If iteration already failed, the original source failure remains the terminal classification while cleanup metadata records the secondary cleanup failure. Cancellation and `GeneratorExit` continue to propagate without synthetic frames.

## Regenerated baseline

The authoritative 96-run matrix has 87 passes and 9 preserved adjudications, with zero failures, zero false-success violations, three cleanup-failure fixtures handled fail-closed, and `product_gate_pass=true`.

## Preserved dissent

The nine existing adjudications remain: the done-before-finish precedence disagreement and unsupported multiline / physical-fragment framing discoveries. They were not normalized into passes.

## Validation

Focused guard and Round 2 tests: 31 passed. Inherited stream/MCP selection: 136 passed. Ruff, compileall, diff check, and UTF-8 no-BOM checks passed. No mypy or pyright executable was available. No network/provider calls, merge, deployment, or smoke was performed.
