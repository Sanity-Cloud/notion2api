from __future__ import annotations

import os


def _bounded_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def upstream_max_attempts(account_count: int) -> int:
    """Return a bounded per-request attempt budget.

    The historical minimum of three attempts amplified failures even with one
    account. The default is two attempts and the hard ceiling is five.
    """
    configured = _bounded_int("NOTION_UPSTREAM_MAX_ATTEMPTS", 2, 1, 5)
    useful_ceiling = max(1, int(account_count or 0) + 1)
    return min(configured, useful_ceiling)


def should_retry_upstream(*, retriable: bool, attempt: int, max_attempts: int) -> bool:
    return bool(retriable) and attempt < max_attempts
