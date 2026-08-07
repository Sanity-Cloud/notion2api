"""Operational capacity roles for Notion accounts.

Credential/base profile identifiers remain immutable. Alpha/Beta/Canary/Dev are
scheduling aliases only; city, NotebookLM, and governance-domain labels are
workload metadata and never hard account bindings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from enum import Enum
from typing import Any, Mapping


class CapacityRole(str, Enum):
    ALPHA = "alpha"
    BETA = "beta"
    CANARY = "canary"
    DEV = "dev"


DEFAULT_ORDINAL_ROLES: tuple[CapacityRole, ...] = (
    CapacityRole.ALPHA,
    CapacityRole.BETA,
    CapacityRole.CANARY,
    CapacityRole.DEV,
)

_ROLE_ALIASES: dict[str, CapacityRole] = {
    "alpha": CapacityRole.ALPHA,
    "account-1": CapacityRole.ALPHA,
    "account1": CapacityRole.ALPHA,
    "acct-1": CapacityRole.ALPHA,
    "beta": CapacityRole.BETA,
    "account-2": CapacityRole.BETA,
    "account2": CapacityRole.BETA,
    "acct-2": CapacityRole.BETA,
    "canary": CapacityRole.CANARY,
    "account-3": CapacityRole.CANARY,
    "account3": CapacityRole.CANARY,
    "acct-3": CapacityRole.CANARY,
    "dev": CapacityRole.DEV,
    "development": CapacityRole.DEV,
    "account-4": CapacityRole.DEV,
    "account4": CapacityRole.DEV,
    "acct-4": CapacityRole.DEV,
}


def _normalize(value: Any) -> str:
    return str(value or "").strip().casefold().replace("_", "-").replace(" ", "-")


def parse_capacity_role(value: Any) -> CapacityRole | None:
    normalized = _normalize(value)
    if not normalized:
        return None
    return _ROLE_ALIASES.get(normalized)


def default_role_for_ordinal(account_number: int) -> CapacityRole:
    if account_number < 1:
        raise ValueError("account_number must be >= 1")
    if account_number <= len(DEFAULT_ORDINAL_ROLES):
        return DEFAULT_ORDINAL_ROLES[account_number - 1]
    # Extra accounts beyond the four named roles stay production peers as Alpha.
    return CapacityRole.ALPHA


def resolve_capacity_role(
    account: Mapping[str, Any],
    *,
    account_number: int,
) -> CapacityRole:
    for key in (
        "capacity_role",
        "account_alias",
        "operational_alias",
        "scheduling_role",
    ):
        parsed = parse_capacity_role(account.get(key))
        if parsed is not None:
            return parsed
    return default_role_for_ordinal(account_number)


def capacity_alias(role: CapacityRole) -> str:
    return role.value.capitalize()


def production_auto_route_roles() -> frozenset[CapacityRole]:
    """Roles eligible for ordinary production auto-routing."""
    return frozenset({CapacityRole.ALPHA, CapacityRole.BETA})


def canary_fraction() -> float:
    try:
        value = float(os.getenv("NOTION_CANARY_ROUTE_FRACTION", "0.1"))
    except (TypeError, ValueError):
        value = 0.1
    return max(0.0, min(1.0, value))


def is_development_workload(workload: Mapping[str, Any] | None = None) -> bool:
    if not workload:
        return False
    for key in ("workload_class", "workload_kind", "environment", "routing_class"):
        text = _normalize(workload.get(key))
        if text in {"dev", "development", "test", "testing", "sandbox", "local"}:
            return True
    if bool(workload.get("development_workload") or workload.get("test_workload")):
        return True
    return False


@dataclass(frozen=True)
class WorkloadMetadata:
    """Non-binding labels that must never own an account."""

    city: str = ""
    notebooklm_profile: str = ""
    governance_domain: str = ""
    task_domain: str = ""
    workload_class: str = "production"

    def as_dict(self) -> dict[str, str]:
        return {
            "city": self.city,
            "notebooklm_profile": self.notebooklm_profile,
            "governance_domain": self.governance_domain,
            "task_domain": self.task_domain,
            "workload_class": self.workload_class or "production",
        }

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any] | None) -> "WorkloadMetadata":
        raw = payload or {}
        return cls(
            city=str(raw.get("city") or "").strip(),
            notebooklm_profile=str(
                raw.get("notebooklm_profile") or raw.get("notebooklm") or ""
            ).strip(),
            governance_domain=str(raw.get("governance_domain") or "").strip(),
            task_domain=str(raw.get("task_domain") or "").strip(),
            workload_class=str(
                raw.get("workload_class") or raw.get("workload_kind") or "production"
            ).strip()
            or "production",
        )


def role_matches_selector(role: CapacityRole, selector: str) -> bool:
    normalized = _normalize(selector)
    if not normalized:
        return False
    if parse_capacity_role(normalized) == role:
        return True
    return normalized == _normalize(capacity_alias(role))
