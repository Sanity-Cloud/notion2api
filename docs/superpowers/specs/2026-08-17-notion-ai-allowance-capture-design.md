# Notion AI allowance capture design

## Governance boundary

- **Parent:** Sanity Cloud AI Portal → Notion AI Usage.
- **Purpose:** show current Notion plan allowance percentages for the six-hour rolling window and billing-period window.
- **Accountable human:** the SanityCloud operator; restart, merge, push, and publication were explicitly authorized for this change.
- **Authority ceiling:** read Notion allowance status through already governed Notion2API profiles and persist a redacted observation locally. No credential changes or broader provider mutation.
- **Exclusions:** credit balances, token estimates, billing analytics, Session Broker changes, raw cookies/tokens, automatic quota enforcement, and background polling.

## Provider contract

The current Notion client bundle uses `getCreditRateLimitStatus` as the source for the AI settings allowance UI. A live authenticated read verified that the response contains `window.used/limit` for the six-hour allowance and `billingPeriodWindow.used/limit` for the monthly plan cycle.

Notion2API therefore owns provider retrieval. It normalizes those windows to percentages, stores them through the existing allowance-observation ledger, and exposes a bounded per-profile refresh endpoint. The Portal calls only that local endpoint and falls back to the last stored observation when refresh is unavailable.

## Acceptance and rollback

Acceptance requires provider retrieval tests, persistence tests, Portal refresh/fallback tests, full relevant regression suites, static checks, clean diffs, live refresh for all governed profiles, and post-restart Portal verification. Rollback is a normal Git revert of the two feature commits plus controlled service/UI restart; persisted observations are non-authoritative telemetry and need not be deleted.
