"""Bounded retry policy shared by provider and continuation workflows."""

from __future__ import annotations

import os


MIN_ATTEMPTS = 1
DEFAULT_MAX_PROVIDER_ATTEMPTS = 3
HARD_MAX_PROVIDER_ATTEMPTS = 5


def _configured_max_attempts() -> int:
    raw = os.getenv(
        "SANITYCLOUD_MAX_PROVIDER_ATTEMPTS",
        str(DEFAULT_MAX_PROVIDER_ATTEMPTS),
    )
    try:
        parsed = int(raw or DEFAULT_MAX_PROVIDER_ATTEMPTS)
    except (TypeError, ValueError):
        parsed = DEFAULT_MAX_PROVIDER_ATTEMPTS
    return max(MIN_ATTEMPTS, min(HARD_MAX_PROVIDER_ATTEMPTS, parsed))


def bounded_provider_attempts(account_count: int) -> int:
    """Return a strict total-attempt ceiling, including the initial attempt."""
    available = max(MIN_ATTEMPTS, int(account_count or 0))
    return min(available, _configured_max_attempts())


def bounded_retry_receipt(account_count: int) -> dict[str, int]:
    attempts = bounded_provider_attempts(account_count)
    return {
        "account_count": max(0, int(account_count or 0)),
        "max_total_attempts": attempts,
        "max_retries_after_initial": max(0, attempts - 1),
        "hard_max_total_attempts": HARD_MAX_PROVIDER_ATTEMPTS,
    }
