# SanityCloud Notion2API independent model catalog

Status: **EXPERIMENTAL / SOURCE UPDATED**. Not deployed or route-verified.

## Evidence and decision

On November 22, 2026, the user confirmed that six supplied model-picker
screenshots came from the **current Sanity Monkey workspace**. The UI shows
models while all four configured accounts receive an empty,
`modelSelectionRestricted=true` response from Notion's
`getAvailableModels` endpoint. That endpoint is **not reliable as the
authoritative model-discovery catalog** for this integration.

The screenshots provide 35 distinct observed names. They explicitly show
GPT-6 Astra restricted and Grok 4.3 selected in the picker. They do **not**
prove any submission worked or reveal GPT-6.2 Sol's internal route ID.

## Separation of authority

- **Discovery:** curated, reviewed workspace UI evidence and reproducible
  browser observations, with evidence dates and source identification.
- **Route identity:** independently observed canonical IDs or prior mappings,
  distinguished as *unverified candidates* until checked.
- **Selection entitlement:** explicit restrictions and successful authorized
  account/workspace-bound routing validation. Never infer from a label alone.
- **Notion:** still the messaging provider and project-governance workspace.
  Its empty model-picker response is retained as diagnostic conflict evidence,
  **not** a veto over what UI names are displayed.
- **Governance:** Notion is still SanityCloud's project record of decisions;
  this policy does not make its broken model picker authoritative.

## Implementation and activation

`NOTION_MODEL_CATALOG_DISCOVERY_SOURCE=curated_ui` switches **only model
listing** to the versioned `app/independent_model_catalog.py` evidence
snapshot, without calling Notion's `getAvailableModels`. Chat selection
continues to use its separately configured route source. The legacy
`NOTION_MODEL_CATALOG_SOURCE=curated_ui` also exists for isolated tests,
but is *not* a production route-activation option.

All UI catalog entries carry `route_verified=false`, `selectable=false`,
and non-routable public IDs `unverified:<slug>`. Any historical provider ID
is exposed only as `previously_observed_route_id` for later investigation.
The resolver refuses unverified entries with `model_route_unverified` until
a separately approved, receipt-backed route-verification mechanism exists.

The previous `notion_picker` route mode remains the default while routes
are unverified. The independent UI listing is allowed only for the canonical
**Sanity Monkey workspace ID**, with a fail-closed cross-workspace guard.
Listing obtains a non-routing metadata client, rather than performing an
account health/cooldown selection, and does not contact the model-picker API.
Do **not** switch production chat selection to curated mode before routes are
verified, because the snapshot is display-only.

Source freshness is **date-granular** (2026-11-22, not a falsely precise
capture timestamp); it expires for route authority after seven days.
The snapshot can still be shown as dated historical UI evidence.

## UI-evidence-only discovery limitations

This section describes the experimental, display-only `curated_ui` mode.
It does not block the separately approved pinned-cache acceptance policy
described below. The user confirmed that new provider routes **cannot be
captured from the workspace** and explicitly accepted the 41 stored routes.

For `curated_ui`, new entries remain non-routable until a positive route
ID is established by an authorized method. Validate aliases, restrictions,
streaming, profile binding, rollback and live behavior before any release.
Do not fabricate new IDs from screenshot labels.

Do not copy cookies, tokens, private HAR responses, or other user data into
this repository. Do not bypass provider permissions or assume model names
are internal codenames.

## Source image provenance

The six current-workspace screenshots were supplied in this ChatGPT
conversation. They remain conversation evidence, not copied into Git.
SHA-256 values were calculated from the uploaded bytes:

| Image filename | SHA-256 |
|---|---|
| `1000007037.jpg` | `14f8ff6b1119b1cf15ce46f4ed2b0fb040059d55fceb84b31cd2feef68cabde9` |
| `1000007038.jpg` | `84f8ffdec171b6496e3c6e968b87b4eaa622fc4d9ae9a4df1c5aa2515e0fe75b` |
| `1000007039.jpg` | `2ee9e254782db124f15929038a9070282dac77a03ab982c1f8ca81300ed404cf` |
| `1000007040.jpg` | `8cde460de89a97ba0c98477e7b2fe9c7f011e25cdf4acc60b40e3bfb34804365` |
| `1000007041.jpg` | `2f9456cb2d081fd58e6e6c159249eafd69be00e6ded74759b08d0d22f0df3244` |
| `1000007042.jpg` | `b9ac0c35e0e796eec485bca003092e03fe38425d231e838af49de730e09a1633` |

This list is only the subset of picker entries visible in the screenshots;
it is **not** asserted to be the workspace's entire catalog.

## User-accepted cached routes (2026-11-22)

The user explicitly approved treating the existing **41-route SQLite
snapshot** as the operational model catalog, because current-workspace
model routes cannot be captured independently and Notion's picker endpoint
returns an unusable empty list. This acceptance does not establish a
fresh provider observation or a successful current model execution.

- **Workspace:** Sanity Monkey only, checked against its canonical space ID.
- **Pinned SHA-256:** `f9feed133087743274253b54277217429456e986a0057657b3f93f49868456f2`.
- **Inventory:** 41 cached models; 37 not disabled, four marked disabled.
- **Excluded from selection:** GPT-6 Astra, Kimi K3, Fable 5, Fable 5.1.
  A missing disabled reason is still treated as disabled.
- **Source acquisition:** existing persisted snapshot; original `fetched_at`,
  `expires_at` and age are preserved without rewriting the record.
- **Route acceptance:** historically cached supported surfaces may be tried
  with their existing provider IDs. No unknown IDs or routes are invented.
- **Runtime proof:** not established; a provider can still reject a request.

After release-gate approval, setting
`NOTION_MODEL_CATALOG_SOURCE=accepted_cache` selects the accepted
snapshot for **both listing and chat route selection**, without querying
Notion's model-picker endpoint. The distinct
`NOTION_MODEL_CATALOG_DISCOVERY_SOURCE=accepted_cache` setting can
enable the accepted catalog for listings alone.

The acceptance mode is deliberately guarded by the exact persisted
SHA, model count, structural integrity, workspace ID, individual disabled
flags and supported route surfaces. It rejects missing, altered or
incomplete cache records, and never falls back to guessed static routes.

The receipt reports `catalog_user_accepted_current=true` and
`catalog_route_evidence=user_accepted_cached_snapshot_not_live_verified`.
It *also* reports the actual age, stale status and exceeded original
freshness threshold. These are separate factual and policy attributes.
The policy is **reversible**: restore
`NOTION_MODEL_CATALOG_SOURCE=notion_picker` (or the previous release).
The accepted snapshot is pinned on first successful validation to the
separate `model_accepted_snapshots` SQLite table under a SHA-scoped key.
Ordinary picker refresh replaces only `model_restriction_cache` and
cannot overwrite the approved snapshot. The pinned copy retains original
`fetched_at` and `expires_at`. It must be seeded and its exact SHA
reread **before** promoting the new process; rollback must preserve this
table, not merely the mutable picker row.

**Release status:** source-only until PR review, rollback and deployment
SHA gates pass. A dry-run route resolution is not a verified provider
submission. Acceptance of old routes does not waive authentication,
workspace binding, model restrictions, or error propagation.


## November 22 independent release review and corrective gate

An independent source review of PR #15 at `5e27d4b3` found that
accepted model IDs could be silently substituted with Terra during
transcript construction or transport mapping. The review also identified
mutable shared-cache and attachment-surface inconsistencies.

**Source corrections under review:**

- Every validated chat request retains the canonical selected model
  through the legacy transcript-builder layer in accepted-cache mode.
- The Notion transport binds the exact, allowed
  `routes[final_surface].final_model_name` from the SHA-pinned catalog
  immediately before thread creation, attachment upload or inference.
  Unknown or unsupported routes fail instead of defaulting to Terra.
- Ordinary attachments select the `markdown-chat` surface; if this
  surface has no supported accepted route, the request fails as a 400.
  Computer-use mode is not enabled for accepted-cache routes without
  separate surface evidence.
- The cache's immutable acceptance table is created through an
  additive SQLite schema migration. The original picker cache remains
  mutable for legacy use. Concurrent workers pin the same acceptance.
- Duplicate normalized display names now fail validation, rather than
  depending on ambiguous first-match selection.

An isolated read-only copy of the current shared SQLite database was
checked under production Python 3.10 against the accepted SHA. All 41
entries were accounted for: 32 exact serialized workflow routes,
4 explicitly blocked disabled entries, 5 unsupported workflow routes,
0 display-name collisions, and 0 unexpected validation failures.
The probe did not submit any provider request or copy credentials.

**Further release gates:** complete final-source regression tests, push
and review the corrected commit, seed/verify the pinned acceptance table
in the actual release path without modifying the mutable snapshot,
validate the managed launcher and rollback state, and perform one
authorized native model canary with verified outbound identity and
provider response. No provider authorization claim follows merely
from a successful local resolver test.


## Independent Copilot review — 2026-11-22 (HEAD `2175987`)

AiGentAI Copilot Think Deeper completed a source-grounded review and
persisted job `chatjob-61983de2035d443b93fa96c3`. It confirmed the
previous Terra fallback, immutable snapshot, and attachment-surface fixes
in the literal `accepted_cache` execution path, but returned **NO-GO**
on that head for a configuration-normalization split.

The review identified that `model_registry.py` used `.strip().lower()`,
while chat construction and Notion transport required exact raw
`accepted_cache`. Case or surrounding whitespace could therefore cause
the registry to approve the pinned model while the transport substituted
the legacy Terra fallback. It also requested consistent API error
classification and lookup-key collision protection.

The successor patch introduces one shared catalog-source parser,
one normalized selectable-key rule across canonical IDs, display names,
public names and aliases, and maps late model-catalog failures to the
same 503 OpenAI-compatible error instead of POOL_COOLING. Invalid modes
fail closed; tests cover mixed case, whitespace, erroneous modes,
exact HTTP payload route IDs and controlled failure responses.

AiGentAI ChatGPT's separate pinned job
`chatjob-cc8ff8d7a5c84defafd17bad` timed out at
`send_action_started` with an **unknown outcome** and no result receipt.
It was not resent; no ChatGPT review is claimed. The Copilot verdict
applies to the *reviewed* head only, not to a successor until separately
reviewed. Context7 FastAPI error guidance was consulted against
installed FastAPI 0.136.1 and Pydantic 2.13.3.

The corrected source remains a **draft / release NO-GO** until final
tests, exact-head independent review and authorized native canary.
