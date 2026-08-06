# Model catalog and reasoning-effort governance

## Status

- Lifecycle stage: Build → automated Validate
- Catalog authority: Notion `getAvailableModels`
- Availability scope: workspace-global
- Static registry role: aliases, compatibility metadata, and explicitly labeled fallback only

## Authority contract

Notion2API treats each complete, validated `getAvailableModels` response as one immutable catalog snapshot. A snapshot includes:

- canonical route ID, display name, family, provider, and display group;
- supported and default reasoning efforts;
- speed, intelligence, and cost ratings;
- workflow, Custom Agent, and Agent Service routes;
- disabled, restricted, rate-limit, and disaster-recovery states;
- snapshot timestamp, expiry, source, and SHA-256 digest.

The catalog is global to the workspace. Account rotation changes capacity and continuity, not the model catalog.

## Freshness and fallback

A live refresh is published only after the full payload validates. Readers use the shared SQLite cache and cross-process refresh lease.

| Setting | Default | Purpose |
| --- | ---: | --- |
| `NOTION_MODEL_CATALOG_CACHE_TTL_SECONDS` | `300` | Fresh catalog lifetime |
| `NOTION_MODEL_CATALOG_MAX_STALE_SECONDS` | `86400` | Maximum last-known-good age |
| `NOTION_MODEL_CATALOG_REFRESH_WAIT_SECONDS` | `5` | Wait for another refresher |

When live refresh fails:

1. Use a last-known-good snapshot only while it remains within the configured maximum age.
2. Mark the source as `last_known_good`, the snapshot as stale, and preserve the upstream error.
3. Fail with `model_catalog_unavailable` after the maximum age.
4. Never silently substitute Terra or another static model.

`NOTION_MODEL_CATALOG_ALLOW_STATIC_SELECTION` exists for isolated legacy tests. Production selection must leave it disabled. Static model listings are explicitly marked `static_fallback` and are not authoritative availability evidence.

## Reasoning-effort contract

`reasoning_effort` is an optional HTTP and MCP request field. Values are exact and model-specific.

- Omitted: use the live catalog default when the model advertises one.
- Explicit and supported: transmit the exact value to Notion as `reasoningEffort`.
- Explicit and unsupported: fail with `reasoning_effort_not_supported`.
- No effort selector advertised: explicit effort fails; omission preserves provider-default behavior.

Provider labels are not normalized into a universal ordinal scale. For example, one provider's `high` is not assumed equivalent to another provider's `high`.

## Selection algorithm

1. Normalize the requested selector without assigning a fallback model.
2. Resolve a known local alias when available.
3. Otherwise match the live canonical ID, public name, display name, or advertised alias.
4. Require the model to exist in the authoritative or policy-valid last-known-good snapshot.
5. Reject disabled or restricted models with the upstream reason.
6. Require the selected surface route.
7. Validate the exact reasoning effort.
8. Bind the canonical route and resolved effort into the transcript.
9. Persist a bounded selection receipt.

Unknown selectors fail with `model_not_available`. They never resolve to Terra.

## API and MCP exposure

`GET /v1/models` and the MCP model-list tool expose:

- `supported_reasoning_efforts`;
- `default_reasoning_effort`;
- `model_card_attributes`;
- surface-specific `routes`;
- restriction and disabled state;
- provider/family metadata;
- catalog source, age, stale state, and snapshot digest.

Chat and Responses outputs promote:

- `requested_reasoning_effort`;
- `resolved_reasoning_effort`;
- `reasoning_effort_source`;
- catalog provenance in model metadata.

Raw private reasoning remains outside the durable governance ledger. The ledger records effort configuration and bounded public progress only.

## Failure codes

| Code | Meaning |
| --- | --- |
| `model_catalog_unavailable` | No live or policy-valid last-known-good snapshot |
| `model_not_available` | Requested selector is absent from the catalog |
| `model_disabled` | Notion disabled or restricted the selected model |
| `model_surface_not_supported` | Selected model lacks the requested surface route |
| `reasoning_effort_not_supported` | Exact effort is absent from the model's advertised set |

These failures are terminal request-validation failures. They must not trigger automatic model or effort substitution.

## Compatibility rules

- Existing aliases such as `terra`, `sol`, `luna`, and provider-friendly names remain accepted.
- New canonical routes can be selected immediately from the live picker without a code release.
- Local alias additions for Opus 5 and Kimi K3 preserve friendly-name compatibility.
- Legacy restriction cache keys are workspace-global.
- Additive response fields preserve existing OpenAI-style model-list shape.

## Security and cost governance

Model ratings are upstream ordinal metadata, not dollar prices or benchmark proof. Selection governance must distinguish:

- declared rating;
- observed completion reliability;
- output-integrity and security results;
- end-to-end latency;
- actual Notion credit consumption when available.

Small or economy models must not be selected solely on cost for untrusted content, external connectors, publication, or write-capable workflows. Actual cost enforcement remains blocked until Notion credit telemetry is ingested.

## Automated validation gates

The implementation must retain tests for:

- complete picker parsing and atomic rejection;
- invalid default effort rejection;
- workspace-global single-flight caching;
- bounded last-known-good behavior;
- expired-catalog fail-closed behavior;
- exact and case-sensitive effort validation;
- disabled and surface-incompatible models;
- future live routes without Terra fallback;
- HTTP transcript binding and selection receipts;
- `/v1/models` rich contract;
- MCP schemas and output receipts;
- full repository regression.

## Decision gate

This branch is eligible for merge review only after focused tests, full tests, Ruff, Python compilation, `git diff --check`, and UTF-8 integrity checks pass. Merge, restart, live migration, and publication are separate authorized actions.