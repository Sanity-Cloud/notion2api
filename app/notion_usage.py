from __future__ import annotations

from typing import Any


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be numeric")
    try:
        return float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be numeric") from exc


def _window_percent(payload: Any, field: str) -> float:
    window = payload if isinstance(payload, dict) else {}
    used = _number(window.get("used"), f"{field}.used")
    limit = _number(window.get("limit"), f"{field}.limit")
    if limit <= 0:
        raise ValueError(f"{field}.limit must be greater than zero")
    percent = used / limit * 100
    if percent < 0:
        raise ValueError(f"{field}.used cannot be negative")
    return round(percent, 6)


def normalize_notion_ai_allowance(payload: dict[str, Any]) -> dict[str, Any]:
    """Normalize Notion's getCreditRateLimitStatus plan-allowance response."""
    rolling = payload.get("window")
    monthly = payload.get("billingPeriodWindow")
    result: dict[str, Any] = {
        "rolling_used_percent": _window_percent(rolling, "window"),
        "monthly_used_percent": _window_percent(monthly, "billingPeriodWindow"),
        "monthly_resets_at": None,
    }
    monthly_window = monthly if isinstance(monthly, dict) else {}
    period_end_ms = monthly_window.get("periodEndMs")
    if period_end_ms is not None:
        period_end = _number(period_end_ms, "billingPeriodWindow.periodEndMs")
        if period_end <= 0:
            raise ValueError("billingPeriodWindow.periodEndMs must be positive")
        result["monthly_resets_at"] = period_end / 1000
    return result


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _fields(payload: Any, names: tuple[str, ...]) -> dict[str, int | None]:
    source = payload if isinstance(payload, dict) else {}
    return {name: _integer(source.get(name)) for name in names}


def _credit_source(payload: Any) -> dict[str, int | None]:
    return _fields(payload, ("usageTotal", "limit"))


def normalize_notion_ai_usage(payload: dict[str, Any]) -> dict[str, Any]:
    """Return a strict, identifier-free view of getAIUsageEligibilityV2."""
    usage = payload.get("usage") if isinstance(payload.get("usage"), dict) else {}
    current = usage.get("currentServicePeriod")
    lifetime = usage.get("lifetime")
    limits = payload.get("limits") if isinstance(payload.get("limits"), dict) else {}
    purchased = (
        limits.get("purchased") if isinstance(limits.get("purchased"), dict) else {}
    )
    purchased_sources = (
        purchased.get("perSource")
        if isinstance(purchased.get("perSource"), dict)
        else {}
    )
    free = limits.get("free") if isinstance(limits.get("free"), dict) else {}
    basic = (
        payload.get("basicCredits")
        if isinstance(payload.get("basicCredits"), dict)
        else {}
    )
    premium = (
        payload.get("premiumCredits")
        if isinstance(payload.get("premiumCredits"), dict)
        else {}
    )
    premium_sources = (
        premium.get("perSource") if isinstance(premium.get("perSource"), dict) else {}
    )
    return {
        "provider": "notion",
        "contract": "getAIUsageEligibilityV2",
        "provider_reported": True,
        "credit_based_products": {
            "counts_toward_plan_ai_allowance": False,
            "current_service_period": _fields(current, ("spaceUsage", "userUsage")),
            "lifetime": _fields(
                lifetime,
                ("spaceUsage", "userUsage", "userPromotionalUsage"),
            ),
            "total_credit_balance": _integer(usage.get("totalCreditBalance")),
            "credits_in_overage": _integer(usage.get("creditsInOverage")),
            "last_space_usage_at_ms": _integer(usage.get("lastSpaceUsageAtMs")),
            "last_user_usage_at_ms": _integer(usage.get("lastSpaceUserUsageAtMs")),
            "limits": {
                "purchased_total": _integer(purchased.get("totalLimit")),
                "purchased_per_source": _fields(
                    purchased_sources,
                    (
                        "allocated",
                        "flexible_recurring",
                        "monthlyAllocated",
                        "monthlyCommitted",
                        "yearlyElastic",
                    ),
                ),
                "free": _fields(
                    free,
                    ("spaceLimit", "userLimit", "userPromotionalLimit"),
                ),
            },
            "basic_credits": _fields(
                basic,
                (
                    "spaceUsage",
                    "userUsage",
                    "userPromotionalUsage",
                    "spaceLimit",
                    "userLimit",
                    "userPromotionalLimit",
                    "lastSpaceUsageAtMs",
                    "lastSpaceUserUsageAtMs",
                ),
            ),
            "premium_credits": {
                **_fields(
                    premium,
                    (
                        "totalCreditBalance",
                        "creditsInOverage",
                        "overageLimit",
                        "servicePeriodStartMs",
                    ),
                ),
                "per_source": {
                    name: _credit_source(premium_sources.get(name))
                    for name in (
                        "monthlyAllocated",
                        "monthlyCommitted",
                        "stackedTrial",
                        "yearlyElastic",
                    )
                },
            },
        },
        "plan_ai_allowance": {
            "rolling_window_seconds": 21600,
            "rolling_used_percent": None,
            "rolling_resets_at": None,
            "monthly_used_percent": None,
            "monthly_resets_at": None,
            "retrieval_status": "not_present_in_har_response_contract",
        },
    }
