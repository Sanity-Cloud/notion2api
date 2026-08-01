"""Governance-plan authorization for Hive admission and execution gates.

The runtime may infer and authorize routine actions from an adopted plan and
bounded evidence. A legacy human_approval boolean remains accepted only for
backward compatibility; it is not required by the new operating model.
"""

from __future__ import annotations

from typing import Any, Mapping

from pydantic import BaseModel, Field


class GovernedAuthorizationError(PermissionError):
    """Raised when a governance decision cannot authorize the requested action."""


AUTHORITY_RANK = {"A0": 0, "A1": 1, "A2": 2, "A3": 3, "A4": 4}
RISK_RANK = {"low": 0, "moderate": 1, "high": 2, "critical": 3}
AUTHORITY_RISK_CEILING = {
    "A0": "low",
    "A1": "low",
    "A2": "moderate",
    "A3": "high",
    "A4": "critical",
}


class GovernedAuthorization(BaseModel):
    decision_id: str = Field(min_length=1)
    plan_id: str = Field(min_length=1)
    authorized: bool = False
    governance_aligned: bool = False
    authority_ceiling: str = "A0"
    inferred_risk: str = "critical"
    confidence: float = 0.0
    evidence_count: int = 0
    reversible: bool = False
    reserved_action: bool = False
    source_boundary_ok: bool = False
    writable_domain_ok: bool = False
    dependency_state_ok: bool = False
    rationale: str = ""
    decided_by: str = "governance_engine"

    @classmethod
    def from_mapping(
        cls, value: Mapping[str, Any] | None
    ) -> "GovernedAuthorization":
        data = dict(value or {})
        authority = str(data.get("authority_ceiling") or "A0").strip().upper()
        if authority not in AUTHORITY_RANK:
            authority = "A0"
        risk = str(data.get("inferred_risk") or "critical").strip().lower()
        if risk not in RISK_RANK:
            risk = "critical"
        try:
            confidence = max(0.0, min(1.0, float(data.get("confidence") or 0.0)))
        except (TypeError, ValueError):
            confidence = 0.0
        try:
            evidence_count = max(0, int(data.get("evidence_count") or 0))
        except (TypeError, ValueError):
            evidence_count = 0
        return cls(
            decision_id=str(data.get("decision_id") or "").strip(),
            plan_id=str(data.get("plan_id") or "").strip(),
            authorized=bool(data.get("authorized", False)),
            governance_aligned=bool(data.get("governance_aligned", False)),
            authority_ceiling=authority,
            inferred_risk=risk,
            confidence=confidence,
            evidence_count=evidence_count,
            reversible=bool(data.get("reversible", False)),
            reserved_action=bool(data.get("reserved_action", False)),
            source_boundary_ok=bool(data.get("source_boundary_ok", False)),
            writable_domain_ok=bool(data.get("writable_domain_ok", False)),
            dependency_state_ok=bool(data.get("dependency_state_ok", False)),
            rationale=str(data.get("rationale") or "").strip(),
            decided_by=str(data.get("decided_by") or "governance_engine").strip(),
        )

    def require(
        self,
        *,
        required_authority: str = "A0",
        minimum_confidence: float = 0.75,
        minimum_evidence_count: int = 1,
        require_reversible: bool = True,
    ) -> dict[str, Any]:
        required = str(required_authority or "A0").strip().upper()
        if required not in AUTHORITY_RANK:
            required = "A0"
        if not self.authorized:
            raise GovernedAuthorizationError(
                "The adopted plan does not authorize this action."
            )
        if not self.governance_aligned:
            raise GovernedAuthorizationError(
                "The authorization decision is not governance-aligned."
            )
        if self.reserved_action:
            raise GovernedAuthorizationError(
                "The action is reserved and must return to governance planning."
            )
        if AUTHORITY_RANK[self.authority_ceiling] < AUTHORITY_RANK[required]:
            raise GovernedAuthorizationError(
                "The governance authority ceiling is below the action requirement."
            )
        allowed_risk = AUTHORITY_RISK_CEILING[self.authority_ceiling]
        if RISK_RANK[self.inferred_risk] > RISK_RANK[allowed_risk]:
            raise GovernedAuthorizationError(
                "Inferred risk exceeds the governance authority ceiling."
            )
        if self.confidence < minimum_confidence:
            raise GovernedAuthorizationError(
                "Authorization confidence is below the governance threshold."
            )
        if self.evidence_count < minimum_evidence_count:
            raise GovernedAuthorizationError(
                "Authorization evidence is below the governance threshold."
            )
        if not self.source_boundary_ok:
            raise GovernedAuthorizationError("The source boundary is not satisfied.")
        if not self.writable_domain_ok:
            raise GovernedAuthorizationError("The writable-domain lease is not satisfied.")
        if not self.dependency_state_ok:
            raise GovernedAuthorizationError("The dependency state is not satisfied.")
        if require_reversible and not self.reversible:
            raise GovernedAuthorizationError("A reversible or rollback path is required.")
        return {
            "authorization_basis": "governance_plan_inference",
            "decision_id": self.decision_id,
            "plan_id": self.plan_id,
            "authorized": True,
            "governance_aligned": True,
            "authority_ceiling": self.authority_ceiling,
            "inferred_risk": self.inferred_risk,
            "confidence": self.confidence,
            "evidence_count": self.evidence_count,
            "reversible": self.reversible,
            "source_boundary_ok": self.source_boundary_ok,
            "writable_domain_ok": self.writable_domain_ok,
            "dependency_state_ok": self.dependency_state_ok,
            "decided_by": self.decided_by,
            "rationale": self.rationale,
            "per_action_human_approval_required": False,
        }


def require_governed_authorization(
    authorization: Mapping[str, Any] | GovernedAuthorization | None,
    *,
    required_authority: str = "A0",
    minimum_confidence: float = 0.75,
    minimum_evidence_count: int = 1,
    require_reversible: bool = True,
    legacy_human_approval: bool = False,
) -> dict[str, Any]:
    if isinstance(authorization, GovernedAuthorization):
        decision = authorization
    elif isinstance(authorization, Mapping):
        decision = GovernedAuthorization.from_mapping(authorization)
    else:
        decision = None
    if decision is not None:
        return decision.require(
            required_authority=required_authority,
            minimum_confidence=minimum_confidence,
            minimum_evidence_count=minimum_evidence_count,
            require_reversible=require_reversible,
        )
    if legacy_human_approval:
        return {
            "authorization_basis": "legacy_human_approval_compatibility",
            "authorized": True,
            "authority_ceiling": required_authority,
            "per_action_human_approval_required": False,
            "deprecated_input_used": True,
        }
    raise GovernedAuthorizationError(
        "Governance-plan authorization is required for this action."
    )
