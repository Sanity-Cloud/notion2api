from __future__ import annotations

from types import SimpleNamespace

import pytest

from app.identity_scope import IdentityScopeError, identity_scope_from_client
from app.mutation_policy import (
    MutationPolicy,
    MutationPolicyError,
    PlanAuthorization,
    mutation_policy_receipt,
    require_mutation_capability,
)
from app.retry_policy import bounded_provider_attempts, bounded_retry_receipt


def governed_plan(**overrides):
    values = {
        "plan_id": "plan-1",
        "action_id": "action-1",
        "authorized": True,
        "authority_ceiling": "A2",
        "inferred_risk": "moderate",
        "confidence": 0.91,
        "evidence_count": 3,
        "reversible": True,
        "publication_authorized": False,
        "rationale": "Adopted plan, bounded reversible operation.",
    }
    values.update(overrides)
    return PlanAuthorization.from_mapping(values)


def test_governance_plan_authorizes_without_per_action_human_approval():
    policy = MutationPolicy(
        enabled=True,
        publication_suppressed=True,
        capabilities=frozenset({"page.delete_children"}),
        minimum_confidence=0.75,
        minimum_evidence_count=1,
        require_reversible=True,
    )

    receipt = require_mutation_capability(
        "page.delete_children",
        governance_aligned=True,
        plan_authorization=governed_plan(),
        policy=policy,
    )

    assert receipt["authorization_basis"] == "governance_plan_inference"
    assert receipt["plan_authorized"] is True
    assert "accountable_human_approved" not in receipt
    assert mutation_policy_receipt(policy)["per_action_human_approval_required"] is False


def test_inferred_risk_must_fit_authority_ceiling():
    policy = MutationPolicy(
        enabled=True,
        publication_suppressed=True,
        capabilities=frozenset({"page.delete_children"}),
    )

    with pytest.raises(MutationPolicyError, match="risk exceeds"):
        policy.require(
            "page.delete_children",
            governance_aligned=True,
            plan=governed_plan(authority_ceiling="A1", inferred_risk="high"),
        )


def test_intelligence_confidence_and_evidence_fail_closed():
    policy = MutationPolicy(
        enabled=True,
        publication_suppressed=True,
        capabilities=frozenset({"page.delete_children"}),
        minimum_confidence=0.8,
        minimum_evidence_count=2,
    )

    with pytest.raises(MutationPolicyError, match="confidence"):
        policy.require(
            "page.delete_children",
            governance_aligned=True,
            plan=governed_plan(confidence=0.55),
        )
    with pytest.raises(MutationPolicyError, match="insufficient"):
        policy.require(
            "page.delete_children",
            governance_aligned=True,
            plan=governed_plan(evidence_count=1),
        )


def test_publication_requires_plan_authorization_and_unsuppressed_policy():
    suppressed = MutationPolicy(
        enabled=True,
        publication_suppressed=True,
        capabilities=frozenset({"page.create"}),
    )
    with pytest.raises(MutationPolicyError, match="Publication"):
        suppressed.require(
            "page.create",
            governance_aligned=True,
            plan=governed_plan(publication_authorized=True),
        )

    enabled = MutationPolicy(
        enabled=True,
        publication_suppressed=False,
        capabilities=frozenset({"page.create"}),
    )
    receipt = enabled.require(
        "page.create",
        governance_aligned=True,
        plan=governed_plan(publication_authorized=True),
    )
    assert receipt["publication_authorized"] is True


def test_identity_scope_requires_complete_tuple_and_prevents_default_bucket():
    with pytest.raises(IdentityScopeError, match="workspace_id"):
        identity_scope_from_client(
            SimpleNamespace(account_key="profile-a", user_id="user-a"),
            "thread-a",
        )

    first = identity_scope_from_client(
        SimpleNamespace(
            account_key="profile-a", user_id="user-a", space_id="workspace-a"
        ),
        "thread-a",
    )
    second = identity_scope_from_client(
        SimpleNamespace(
            account_key="profile-a", user_id="user-a", space_id="workspace-a"
        ),
        "thread-b",
    )
    assert first.key != second.key
    assert "token" not in first.receipt()


def test_provider_attempts_are_bounded_by_capacity_and_hard_limit(monkeypatch):
    monkeypatch.setenv("SANITYCLOUD_MAX_PROVIDER_ATTEMPTS", "3")
    assert bounded_provider_attempts(1) == 1
    assert bounded_provider_attempts(20) == 3
    receipt = bounded_retry_receipt(20)
    assert receipt["max_total_attempts"] == 3
    assert receipt["max_retries_after_initial"] == 2
