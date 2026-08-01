"""Governance- and plan-based capability policy for Notion mutation effects.

Routine actions are authorized by an adopted plan, governance alignment, bounded
risk, evidence, confidence, reversibility, and authority ceilings. No per-action
human approval flag is required. Reserved or insufficiently supported actions
fail closed and return to planning/governance review.
"""

from __future__ import annotations

from dataclasses import dataclass
import os
from typing import Any, Iterable, Mapping


class MutationPolicyError(PermissionError):
    """Raised when governance and plan evidence do not authorize a mutation."""


PUBLICATION_CAPABILITIES = {
    "page.create",
    "page.append",
    "page.upload",
}
KNOWN_MUTATION_CAPABILITIES = PUBLICATION_CAPABILITIES | {
    "page.delete_children",
}
RISK_RANK = {"low": 0, "moderate": 1, "high": 2, "critical": 3}
AUTHORITY_RISK_CEILING = {
    "A0": "low",
    "A1": "low",
    "A2": "moderate",
    "A3": "high",
    "A4": "critical",
}


def _env_flag(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _capabilities(value: str | None) -> frozenset[str]:
    return frozenset(
        item.strip().lower()
        for item in str(value or "").split(",")
        if item.strip()
    )


def _bounded_float(value: Any, default: float = 0.0) -> float:
    try:
        return max(0.0, min(1.0, float(value)))
    except (TypeError, ValueError):
        return default


def _bounded_int(value: Any, default: int = 0) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class PlanAuthorization:
    plan_id: str
    action_id: str
    authorized: bool
    authority_ceiling: str
    inferred_risk: str
    confidence: float
    evidence_count: int
    reversible: bool
    publication_authorized: bool = False
    rationale: str = ""

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any] | None) -> "PlanAuthorization":
        data = dict(value or {})
        risk = str(data.get("inferred_risk") or "critical").strip().lower()
        if risk not in RISK_RANK:
            risk = "critical"
        authority = str(data.get("authority_ceiling") or "A0").strip().upper()
        if authority not in AUTHORITY_RISK_CEILING:
            authority = "A0"
        return cls(
            plan_id=str(data.get("plan_id") or "").strip(),
            action_id=str(data.get("action_id") or "").strip(),
            authorized=bool(data.get("authorized", False)),
            authority_ceiling=authority,
            inferred_risk=risk,
            confidence=_bounded_float(data.get("confidence")),
            evidence_count=_bounded_int(data.get("evidence_count")),
            reversible=bool(data.get("reversible", False)),
            publication_authorized=bool(data.get("publication_authorized", False)),
            rationale=str(data.get("rationale") or "").strip(),
        )

    def validate(self) -> None:
        if not self.plan_id or not self.action_id:
            raise MutationPolicyError(
                "Mutation denied because plan_id and action_id are required."
            )
        if not self.authorized:
            raise MutationPolicyError(
                "Mutation denied because the adopted plan does not authorize the action."
            )
        allowed_risk = AUTHORITY_RISK_CEILING[self.authority_ceiling]
        if RISK_RANK[self.inferred_risk] > RISK_RANK[allowed_risk]:
            raise MutationPolicyError(
                "Mutation denied because inferred risk exceeds the plan authority ceiling."
            )


@dataclass(frozen=True)
class MutationPolicy:
    enabled: bool
    publication_suppressed: bool
    capabilities: frozenset[str]
    minimum_confidence: float = 0.75
    minimum_evidence_count: int = 1
    require_reversible: bool = True

    @classmethod
    def from_env(cls) -> "MutationPolicy":
        return cls(
            enabled=_env_flag("SANITYCLOUD_NOTION_MUTATIONS_ENABLED", default=False),
            publication_suppressed=_env_flag(
                "SANITYCLOUD_PUBLICATION_SUPPRESSED", default=True
            ),
            capabilities=_capabilities(
                os.getenv("SANITYCLOUD_NOTION_MUTATION_CAPABILITIES")
            ),
            minimum_confidence=_bounded_float(
                os.getenv("SANITYCLOUD_PLAN_AUTH_MIN_CONFIDENCE", "0.75"), 0.75
            ),
            minimum_evidence_count=_bounded_int(
                os.getenv("SANITYCLOUD_PLAN_AUTH_MIN_EVIDENCE", "1"), 1
            ),
            require_reversible=_env_flag(
                "SANITYCLOUD_PLAN_AUTH_REQUIRE_REVERSIBLE", default=True
            ),
        )

    def require(
        self,
        capability: str,
        *,
        governance_aligned: bool,
        plan: PlanAuthorization,
    ) -> dict[str, object]:
        requested = str(capability or "").strip().lower()
        if requested not in KNOWN_MUTATION_CAPABILITIES:
            raise MutationPolicyError(f"Unknown mutation capability: {requested or '<empty>'}")
        if not self.enabled:
            raise MutationPolicyError("Notion mutations are disabled by governance policy.")
        if requested not in self.capabilities:
            raise MutationPolicyError(f"Mutation capability is not granted: {requested}")
        if not governance_aligned:
            raise MutationPolicyError(
                "Mutation denied because the selected account is not governance-aligned."
            )
        plan.validate()
        if plan.confidence < self.minimum_confidence:
            raise MutationPolicyError(
                "Mutation denied because inferred authorization confidence is below policy."
            )
        if plan.evidence_count < self.minimum_evidence_count:
            raise MutationPolicyError(
                "Mutation denied because the plan has insufficient supporting evidence."
            )
        if self.require_reversible and not plan.reversible:
            raise MutationPolicyError(
                "Mutation denied because the plan does not provide a reversible path."
            )
        if requested in PUBLICATION_CAPABILITIES:
            if self.publication_suppressed or not plan.publication_authorized:
                raise MutationPolicyError(
                    "Publication is not authorized by the current governance plan."
                )
        return {
            "capability": requested,
            "enabled": True,
            "governance_aligned": True,
            "plan_id": plan.plan_id,
            "action_id": plan.action_id,
            "plan_authorized": True,
            "authority_ceiling": plan.authority_ceiling,
            "inferred_risk": plan.inferred_risk,
            "confidence": plan.confidence,
            "evidence_count": plan.evidence_count,
            "reversible": plan.reversible,
            "publication_authorized": plan.publication_authorized,
            "publication_suppressed": self.publication_suppressed,
            "authorization_basis": "governance_plan_inference",
        }


def require_mutation_capability(
    capability: str,
    *,
    governance_aligned: bool,
    plan_authorization: Mapping[str, Any] | PlanAuthorization | None,
    policy: MutationPolicy | None = None,
) -> dict[str, object]:
    plan = (
        plan_authorization
        if isinstance(plan_authorization, PlanAuthorization)
        else PlanAuthorization.from_mapping(plan_authorization)
    )
    return (policy or MutationPolicy.from_env()).require(
        capability,
        governance_aligned=governance_aligned,
        plan=plan,
    )


def mutation_policy_receipt(
    policy: MutationPolicy | None = None,
) -> dict[str, object]:
    current = policy or MutationPolicy.from_env()
    return {
        "enabled": current.enabled,
        "publication_suppressed": current.publication_suppressed,
        "capabilities": sorted(current.capabilities),
        "known_capabilities": sorted(KNOWN_MUTATION_CAPABILITIES),
        "minimum_confidence": current.minimum_confidence,
        "minimum_evidence_count": current.minimum_evidence_count,
        "require_reversible": current.require_reversible,
        "authorization_basis": "governance_plan_inference",
        "per_action_human_approval_required": False,
        "credential_values_in_policy": False,
    }


def validate_capabilities(values: Iterable[str]) -> frozenset[str]:
    normalized = frozenset(str(item or "").strip().lower() for item in values)
    unknown = normalized.difference(KNOWN_MUTATION_CAPABILITIES)
    if unknown:
        raise MutationPolicyError(f"Unknown mutation capabilities: {sorted(unknown)}")
    return normalized
