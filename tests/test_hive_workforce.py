from __future__ import annotations

import sqlite3

import pytest

from app.hive_runtime import HiveIdempotencyConflict, HiveRuntimeStore, HiveTransitionError
from app.hive_workforce import HiveWorkforceStore


def _store(tmp_path):
    return HiveWorkforceStore(tmp_path / "hive.sqlite3")


def _register(store: HiveWorkforceStore, worker_id: str = "worker-1"):
    return store.register_worker(
        worker_id=worker_id,
        display_name="Builder Bee",
        worker_class="persistent_member",
        role="Runtime Engineer",
        accountable_owner="Mathias",
        competencies=["python", "testing"],
        writable_domains=["app", "tests"],
        authority_ceiling="A2",
        source_boundary="SanityCloud engineering sources",
        appointment_scope="bounded repository work",
        actor="hiring-owner",
        idempotency_key=f"register-{worker_id}",
    )


def _appoint(store: HiveWorkforceStore, worker_id: str = "worker-1"):
    _register(store, worker_id)
    shadow = store.transition_worker(
        worker_id=worker_id,
        target_stage="shadow",
        actor="hiring-owner",
        reason="Begin supervised observation.",
        idempotency_key=f"shadow-{worker_id}",
    )
    probation = store.transition_worker(
        worker_id=worker_id,
        target_stage="probation",
        actor="Mathias",
        reason="Authorize bounded probation.",
        human_approval=True,
        expected_revision=shadow.workers[0].revision,
        idempotency_key=f"probation-{worker_id}",
    )
    return store.transition_worker(
        worker_id=worker_id,
        target_stage="appointed",
        actor="Mathias",
        reason="Probation evidence accepted.",
        human_approval=True,
        expected_revision=probation.workers[0].revision,
        idempotency_key=f"appointed-{worker_id}",
    )


def test_registry_coexists_with_hive_runtime_schema(tmp_path):
    path = tmp_path / "hive.sqlite3"
    HiveRuntimeStore(path)
    store = HiveWorkforceStore(path)
    created = _register(store)

    assert created.count == 1
    assert created.workers[0].stage == "REQUISITIONED"
    with sqlite3.connect(path) as conn:
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 1
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert {"hive_missions", "hive_workers", "hive_worker_events"} <= tables


def test_register_is_idempotent_and_conflicting_reuse_fails(tmp_path):
    store = _store(tmp_path)
    first = _register(store)
    second = _register(store)

    assert second.model_dump() == first.model_dump()
    with pytest.raises(HiveIdempotencyConflict):
        store.register_worker(
            worker_id="worker-2",
            display_name="Different Bee",
            worker_class="temporary_worker",
            role="Scout",
            accountable_owner="Mathias",
            idempotency_key="register-worker-1",
        )


def test_probation_and_appointment_require_governance_authorization(tmp_path):
    store = _store(tmp_path)
    _register(store)
    shadow = store.transition_worker(
        worker_id="worker-1",
        target_stage="shadow",
        actor="lead",
        reason="Observe existing work.",
    )

    with pytest.raises(HiveTransitionError, match="Governance-plan authorization"):
        store.transition_worker(
            worker_id="worker-1",
            target_stage="probation",
            actor="lead",
            reason="Attempt unauthorized probation.",
            expected_revision=shadow.workers[0].revision,
        )

    governance = {
        "decision_id": "decision-worker-1",
        "plan_id": "plan-worker-1",
        "authorized": True,
        "governance_aligned": True,
        "authority_ceiling": "A2",
        "inferred_risk": "moderate",
        "confidence": 0.95,
        "evidence_count": 3,
        "reversible": True,
        "source_boundary_ok": True,
        "writable_domain_ok": True,
        "dependency_state_ok": True,
    }
    probation = store.transition_worker(
        worker_id="worker-1",
        target_stage="probation",
        actor="governance-engine",
        reason="Plan evidence supports bounded probation.",
        governance_authorization=governance,
        expected_revision=shadow.workers[0].revision,
    )
    appointed_by_plan = store.transition_worker(
        worker_id="worker-1",
        target_stage="appointed",
        actor="governance-engine",
        reason="Probation evidence satisfies the appointment plan.",
        governance_authorization={**governance, "decision_id": "decision-worker-2"},
        expected_revision=probation.workers[0].revision,
    )
    assert appointed_by_plan.workers[0].stage == "APPOINTED"

    appointed = _appoint(_store(tmp_path), worker_id="worker-2")
    assert appointed.workers[0].stage == "APPOINTED"
    assert appointed.workers[0].revision == 4


def test_revision_and_terminal_transition_controls(tmp_path):
    store = _store(tmp_path)
    created = _register(store)
    with pytest.raises(HiveTransitionError, match="Stale worker revision"):
        store.transition_worker(
            worker_id="worker-1",
            target_stage="shadow",
            actor="lead",
            reason="Stale transition.",
            expected_revision=created.workers[0].revision + 1,
        )

    rejected = store.transition_worker(
        worker_id="worker-1",
        target_stage="rejected",
        actor="Mathias",
        reason="Candidate does not satisfy the role charter.",
    )
    assert rejected.workers[0].stage == "REJECTED"
    with pytest.raises(HiveTransitionError, match="Illegal worker transition"):
        store.transition_worker(
            worker_id="worker-1",
            target_stage="shadow",
            actor="lead",
            reason="Terminal records cannot be reopened.",
        )


def test_list_filters_by_stage_and_worker_class(tmp_path):
    store = _store(tmp_path)
    _appoint(store, "worker-appointed")
    store.register_worker(
        worker_id="worker-scout",
        display_name="Scout Bee",
        worker_class="roaming_scout",
        role="Workspace Scout",
        accountable_owner="Mathias",
        competencies=["inventory"],
        authority_ceiling="A0",
    )

    appointed = store.list_workers(stage="appointed")
    scouts = store.list_workers(worker_class="roaming_scout")
    assert [item.worker_id for item in appointed.workers] == ["worker-appointed"]
    assert [item.worker_id for item in scouts.workers] == ["worker-scout"]


def test_invocation_planner_selects_one_bounded_agent(tmp_path):
    store = _store(tmp_path)
    _appoint(store)

    plan = store.plan_invocation(
        objective="Update one Python module and its tests.",
        required_competencies=["python", "testing"],
        writable_domains=["app", "tests"],
        authority_ceiling="A2",
    )

    assert plan.mode == "single_agent"
    assert plan.governance_gate_required is False
    assert plan.human_gate_required is False  # deprecated compatibility mirror
    assert plan.authorization_basis == "governance_plan_inference"
    assert [item.worker_id for item in plan.selected_workers] == ["worker-1"]
    assert plan.missing_competencies == []
    assert plan.missing_writable_domains == []


def test_invocation_planner_routes_complex_work_to_hive_and_preserves_gaps(tmp_path):
    store = _store(tmp_path)
    _appoint(store)

    plan = store.plan_invocation(
        objective="Patch runtime, validate governance, and deploy.",
        required_competencies=["python", "testing", "governance"],
        writable_domains=["app", "tests", "deployment"],
        dependency_count=2,
        parallelizable_workstreams=3,
        risk_level="high",
        authority_ceiling="A3",
        independent_review_required=True,
        external_effects=True,
    )

    assert plan.mode == "hive"
    assert plan.suggested_lane_count == 3
    assert plan.governance_gate_required is True
    assert plan.human_gate_required is True  # deprecated compatibility mirror
    assert plan.authorization_basis == "governance_plan_inference"
    assert plan.missing_competencies == ["governance"]
    assert plan.missing_writable_domains == ["deployment"]
    assert any("reserved-action authorization" in reason for reason in plan.reasons)
