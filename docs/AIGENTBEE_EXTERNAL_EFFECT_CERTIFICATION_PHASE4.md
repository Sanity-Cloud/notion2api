# AIgentBee External-Effect Adapter Certification - Phase 4

## Lifecycle and authority

Phase 4 implements the `Concept -> Evaluate -> Design -> Validate -> Build -> Pilot -> Adopt` gate for external effects.

It does not provide a general shell, browser, network, repository, service-control, credential, or dynamic-code adapter. The only Phase 4 implementation is a compiled reversible sandbox-artifact adapter.

```text
Human-approved certification
  -> independent governance reviewer
  -> disabled compiled adapter registration
  -> explicit adapter enablement
  -> Phase 2 appointed worker and active lease
  -> Phase 3 guarded execution
  -> Phase 4 certified dry run
  -> reviewed reversible effect
  -> reviewed rollback or compensation receipt
  -> human fan-in
```

## Compiled adapter

Implementation identifier: `builtin.sandbox_artifact.v1`

- Capability: `sandbox_artifact`
- Writable domain: `external_sandbox`
- Minimum authority: A2
- Human approval: mandatory
- Independent review: mandatory
- Maximum timeout: 5,000 ms
- Maximum payload and effect size: 65,536 bytes

## Certification contract

A certification requires:

- A distinct appointed `GOVERNANCE_REVIEWER` with A2 or higher authority.
- A named sandbox below `SANITYCLOUD_EXTERNAL_EFFECT_ROOT`.
- An extension allowlist limited to `.json`, `.md`, and `.txt`.
- A threat model defining attack surface, abuse cases, mitigations, and residual risk.
- Credential boundary `none`.
- Rollback strategy `preimage_restore` with a retention period of at least 60 seconds.
- A SHA-256 contract fingerprint that is verified before each effect.

Certification states are `CERTIFIED`, `SUSPENDED`, and terminal `REVOKED`.

## Effect protocol

Dry run and live execution are separate Phase 3 lanes. A live mutation must reference a matching `PLANNED` dry-run receipt with the same certification, operation, filename, preimage hash, and intended after-image hash.

Supported operations are `write` and `delete` for one filename directly inside the certified sandbox. Directory traversal, absolute paths, hidden names, unsupported extensions, symlinks, reparse points, oversized content, and changed preimages fail closed.

Writes use a temporary file, flush, `fsync`, and atomic replacement. The result is verified after commit. Any pre-commit or verification failure restores the preimage.

## Rollback protocol

Each applied effect records:

- Preimage existence and SHA-256.
- Preimage bytes in the local durable ledger.
- After-image existence and SHA-256.
- A one-time rollback token returned to the authorized caller; only its SHA-256 is persisted.
- Certification and execution lineage.

Rollback requires human approval, a distinct appointed governance reviewer, a valid rollback token, an unchanged after-image, and the active certification boundary. Target tampering is recorded as `TAMPERED`; restoration failure is recorded as `COMPENSATION_FAILED`.

## Durable tables

```text
hive_external_adapter_certifications
hive_external_certification_events
hive_external_effect_receipts
hive_external_effect_events
```

The migration is additive and preserves Hive SQLite schema version 1.

## MCP operations

```text
hive_certify_external_adapter
hive_list_external_certifications
hive_transition_external_certification
hive_list_external_effects
hive_rollback_external_effect
```

The dedicated AIgentBee profile exposes the same methods with the `aigentbee_` prefix.

## Explicit exclusions

Phase 4 does not certify shell, browser, arbitrary filesystem, repository, service-control, network, cloud-provider, credential-bearing, or dynamic-code effects. Each future external-effect family requires a separate threat model, compiled implementation, credential boundary, rollback contract, adversarial validation, pilot, and human adoption gate.
