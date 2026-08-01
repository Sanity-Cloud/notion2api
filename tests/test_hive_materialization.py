from __future__ import annotations

import sqlite3

import pytest

from app.hive_materialization import (
    DispatchStatus,
    HiveMaterializationStore,
    LeaseStatus,
    MaterializationStatus,
)
from app.hive_runtime import (
    HiveIdempotencyConflict,
    HiveRuntimeStore,
    HiveTransitionError,
)
from app.hive_workforce import HiveWorkforceStore


def _stores(tmp_path):
    path = tmp_path / "hive.sqlite3"
    return (
        HiveMaterializationStore(path),
        HiveWorkforceStore(path),
        HiveRuntimeStore(path),
    )


def _appoint(
    workforce: HiveWorkforceStore,
    *,
    worker_id: str,
    worker_class: str = "TEMPORARY_WORKER",
    role: str = "builder",
    competencies: list[str] | None = None,
    writable_domains: list[str] | None = None,
    authority: str = "A2",
):
    created = workforce.register_worker(
        worker_id=worker_id,
        display_name=worker_id,
        worker_class=worker_class,
        role=role,
        accountable_owner="human-owner",
        competencies=competencies or [],
        writable_domains=writable_domains or [],
        authority_ceiling=authority,
        source_boundary="approved test sources",
        appointment_scope="phase-2 validation",
        actor="test-owner",
        idempotency_key=f"register-{worker_id}",
    )
    shadow = workforce.transition_worker(
        worker_id=worker_id,
        target_stage="SHADOW",
        actor="test-owner",
        reason="Begin shadow evaluation.",
        expected_revision=created.workers[0].revision,
        idempotency_key=f"shadow-{worker_id}",
    )
    probation = workforce.transition_worker(
        worker_id=worker_id,
        target_stage="PROBATION",
        actor="test-owner",
        reason="Approved bounded probation.",
        human_approval=True,
        expected_revision=shadow.workers[0].revision,
        idempotency_key=f"probation-{worker_id}",
    )
    return workforce.transition_worker(
        worker_id=worker_id,
        target_stage="APPOINTED",
        actor="test-owner",
        reason="Approved appointment.",
        human_approval=True,
        expected_revision=probation.workers[0].revision,
        idempotency_key=f"appoint-{worker_id}",
    ).workers[0]


def test_schema_is_additive_and_preserves_runtime_version(tmp_path):
    path = tmp_path / "hive.sqlite3"
    runtime = HiveRuntimeStore(path)
    runtime.create_mission(
        title="Existing mission",
        objective="Preserve me",
        lifecycle_stage="Pilot",
        mission_id="existing-mission",
    )
    with sqlite3.connect(path) as conn:
        before_version = conn.execute("PRAGMA user_version").fetchone()[0]

    HiveMaterializationStore(path)

    with sqlite3.connect(path) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        after_version = conn.execute("PRAGMA user_version").fetchone()[0]
        mission_count = conn.execute("SELECT COUNT(*) FROM hive_missions").fetchone()[0]
    assert before_version == after_version == 1
    assert mission_count == 1
    assert {
        "hive_invocation_materializations",
        "hive_materialization_events",
        "hive_worker_leases",
        "hive_dispatch_receipts",
    }.issubset(tables)


def test_no_worker_path_is_durably_blocked_and_idempotent(tmp_path):
    store, _workforce, runtime = _stores(tmp_path)
    blocked = store.materialize_invocation(
        objective="Patch and validate one module.",
        required_competencies=["python", "testing"],
        writable_domains=["app", "tests"],
        plan_id="blocked-plan",
        mission_id="blocked-mission",
        idempotency_key="blocked-request",
    )
    assert blocked.status == MaterializationStatus.BLOCKED.value
    assert blocked.selected_worker_ids == []
    assert runtime.get_mission("blocked-mission").found is False

    replay = store.materialize_invocation(
        objective="Patch and validate one module.",
        required_competencies=["python", "testing"],
        writable_domains=["app", "tests"],
        plan_id="blocked-plan",
        mission_id="blocked-mission",
        idempotency_key="blocked-request",
    )
    assert replay.model_dump() == blocked.model_dump()
    with pytest.raises(HiveIdempotencyConflict):
        store.materialize_invocation(
            objective="Different request.",
            plan_id="different-plan",
            mission_id="different-mission",
            idempotency_key="blocked-request",
        )


def test_governance_gate_waits_for_authorization_then_materializes(tmp_path):
    store, workforce, runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="a3-builder",
        competencies=["python"],
        writable_domains=["app"],
        authority="A3",
    )
    prepared = store.materialize_invocation(
        objective="Apply an A3 governed patch.",
        required_competencies=["python"],
        writable_domains=["app"],
        authority_ceiling="A3",
        preferred_worker_ids=[worker.worker_id],
        plan_id="approval-plan",
        mission_id="approval-mission",
        idempotency_key="approval-request",
    )
    assert prepared.status == MaterializationStatus.AWAITING_APPROVAL.value
    assert prepared.governance_gate_required is True
    assert prepared.human_gate_required is True  # persisted compatibility mirror
    assert prepared.authorization_basis == "governance_plan_inference"
    assert runtime.get_mission("approval-mission").found is False

    approved = store.approve_materialization(
        plan_id="approval-plan",
        actor="accountable-human",
        reason="Approved the bounded A3 mission.",
        idempotency_key="approve-approval-plan",
    )
    assert approved.status == MaterializationStatus.MATERIALIZED.value
    assert approved.governance_gate_required is True
    assert approved.authorization_basis == "governance_plan_inference"
    assert approved.human_approval is True  # deprecated persisted compatibility field
    assert approved.approved_by == "accountable-human"
    assert len(approved.leases) == len(approved.dispatch_receipts) == 1
    assert runtime.get_mission("approval-mission").found is True


def test_probation_worker_cannot_receive_execution_lease(tmp_path):
    store, workforce, runtime = _stores(tmp_path)
    created = workforce.register_worker(
        worker_id="probation-worker",
        display_name="Probation worker",
        worker_class="TEMPORARY_WORKER",
        role="builder",
        accountable_owner="human-owner",
        competencies=["python"],
        writable_domains=["app"],
        authority_ceiling="A2",
    )
    shadow = workforce.transition_worker(
        worker_id="probation-worker",
        target_stage="SHADOW",
        actor="human-owner",
        reason="Shadow.",
        expected_revision=created.workers[0].revision,
    )
    workforce.transition_worker(
        worker_id="probation-worker",
        target_stage="PROBATION",
        actor="human-owner",
        reason="Probation.",
        human_approval=True,
        expected_revision=shadow.workers[0].revision,
    )
    result = store.materialize_invocation(
        objective="Attempt probation execution.",
        required_competencies=["python"],
        writable_domains=["app"],
        authority_ceiling="A1",
        preferred_worker_ids=["probation-worker"],
        human_approval=True,
        plan_id="probation-plan",
        mission_id="probation-mission",
    )
    assert result.status == MaterializationStatus.BLOCKED.value
    assert result.leases == []
    assert runtime.get_mission("probation-mission").found is False


def _materialize_three_lane_hive(tmp_path):
    store, workforce, runtime = _stores(tmp_path)
    workers = [
        _appoint(
            workforce,
            worker_id="phase2-builder",
            worker_class="TEMPORARY_WORKER",
            role="Runtime Builder",
            competencies=["python"],
            writable_domains=["app"],
            authority="A2",
        ),
        _appoint(
            workforce,
            worker_id="phase2-tester",
            worker_class="SPECIALIST_CONTRACTOR",
            role="Validation Engineer",
            competencies=["testing"],
            writable_domains=["tests"],
            authority="A2",
        ),
        _appoint(
            workforce,
            worker_id="phase2-reviewer",
            worker_class="GOVERNANCE_REVIEWER",
            role="Independent Reviewer",
            competencies=["governance"],
            writable_domains=["review"],
            authority="A2",
        ),
    ]
    result = store.materialize_invocation(
        objective="Build, validate, and independently review Phase 2.",
        required_competencies=["python", "testing", "governance"],
        writable_domains=["app", "tests"],
        dependency_count=1,
        parallelizable_workstreams=3,
        risk_level="high",
        authority_ceiling="A3",
        independent_review_required=True,
        preferred_worker_ids=[item.worker_id for item in workers],
        parent_context_id="phase2-parent",
        lifecycle_stage="Build â†’ Pilot",
        human_approval=True,
        actor="accountable-human",
        plan_id="phase2-plan",
        mission_id="phase2-mission",
        idempotency_key="phase2-materialize",
    )
    return store, workforce, runtime, result


def test_three_lane_hive_has_bindings_leases_and_review_dependencies(tmp_path):
    store, _workforce, runtime, result = _materialize_three_lane_hive(tmp_path)
    assert result.status == MaterializationStatus.MATERIALIZED.value
    assert result.mode == "hive"
    assert len(result.work_unit_ids) == 3
    assert len(result.leases) == 3
    assert len(result.dispatch_receipts) == 3
    assert {item.status for item in result.leases} == {LeaseStatus.ACTIVE.value}
    assert {item.authority_ceiling for item in result.leases} == {"A2"}
    assert {item.status for item in result.dispatch_receipts} == {
        DispatchStatus.READY.value
    }
    assert all(
        item.conversation_id.startswith("aigentbee:worker:")
        for item in result.dispatch_receipts
    )
    assert len({item.conversation_id for item in result.dispatch_receipts}) == 3

    mission = runtime.get_mission("phase2-mission")
    assert mission.found is True
    assert mission.parent_context_id == "phase2-parent"
    assert len(mission.work_units) == 3
    reviewer = next(
        item for item in mission.work_units if item.role == "Independent Reviewer"
    )
    non_review = {
        item.work_unit_id
        for item in mission.work_units
        if item.role != "Independent Reviewer"
    }
    assert set(reviewer.dependencies) == non_review
    by_mission = store.get_materialization(mission_id="phase2-mission")
    assert by_mission.plan_id == "phase2-plan"


def test_dispatch_completion_releases_leases_and_enters_fan_in(tmp_path):
    store, _workforce, runtime, result = _materialize_three_lane_hive(tmp_path)
    current = result
    for receipt in result.dispatch_receipts:
        current = store.record_dispatch_receipt(
            plan_id=result.plan_id,
            work_unit_id=receipt.work_unit_id,
            status="COMPLETED",
            actor=receipt.worker_id,
            evidence={"validated": True},
            expected_revision=receipt.revision,
            idempotency_key=f"complete-{receipt.work_unit_id}",
        )
    assert current.status == MaterializationStatus.READY_FOR_FAN_IN.value
    assert {item.status for item in current.leases} == {LeaseStatus.RELEASED.value}
    assert {item.status for item in current.dispatch_receipts} == {
        DispatchStatus.COMPLETED.value
    }
    mission = runtime.get_mission(result.mission_id)
    assert {item.status for item in mission.work_units} == {"COMPLETED"}
    assert (
        sum(event.event_type == "DISPATCH_COMPLETED" for event in mission.events) == 3
    )
    completed = current.dispatch_receipts[0]
    with pytest.raises(HiveTransitionError, match="Illegal dispatch"):
        store.record_dispatch_receipt(
            plan_id=result.plan_id,
            work_unit_id=completed.work_unit_id,
            status="ACKNOWLEDGED",
            actor="late-worker",
            expected_revision=completed.revision,
        )


def test_failed_lane_closes_with_failure_and_releases_leases(tmp_path):
    store, _workforce, runtime, result = _materialize_three_lane_hive(tmp_path)
    current = result
    for index, receipt in enumerate(result.dispatch_receipts):
        current = store.record_dispatch_receipt(
            plan_id=result.plan_id,
            work_unit_id=receipt.work_unit_id,
            status="FAILED" if index == 0 else "COMPLETED",
            actor=receipt.worker_id,
            evidence={"index": index},
            expected_revision=receipt.revision,
        )
    assert current.status == MaterializationStatus.CLOSED_WITH_FAILURE.value
    assert {item.status for item in current.leases} == {LeaseStatus.RELEASED.value}
    mission_statuses = {
        item.status for item in runtime.get_mission(result.mission_id).work_units
    }
    assert mission_statuses == {"COMPLETED", "FAILED"}


def test_acknowledgement_records_event_without_closing_work_unit(tmp_path):
    store, _workforce, runtime, result = _materialize_three_lane_hive(tmp_path)
    receipt = result.dispatch_receipts[0]
    acknowledged = store.record_dispatch_receipt(
        plan_id=result.plan_id,
        work_unit_id=receipt.work_unit_id,
        status="ACKNOWLEDGED",
        actor=receipt.worker_id,
        evidence={"accepted": True},
        expected_revision=receipt.revision,
        idempotency_key="acknowledge-phase2-lane",
    )
    updated = next(
        item
        for item in acknowledged.dispatch_receipts
        if item.work_unit_id == receipt.work_unit_id
    )
    assert updated.status == DispatchStatus.ACKNOWLEDGED.value
    mission = runtime.get_mission(result.mission_id)
    lane = next(
        item for item in mission.work_units if item.work_unit_id == receipt.work_unit_id
    )
    assert lane.status == "ACTIVE"
    assert mission.events[-1].event_type == "DISPATCH_ACKNOWLEDGED"


def test_explicit_lease_revocation_is_durable_and_idempotent(tmp_path):
    store, _workforce, _runtime, result = _materialize_three_lane_hive(tmp_path)
    revoked = store.release_leases(
        plan_id=result.plan_id,
        actor="accountable-human",
        reason="Pilot stop condition reached.",
        revoke=True,
        idempotency_key="revoke-phase2",
    )
    assert {item.status for item in revoked.leases} == {LeaseStatus.REVOKED.value}
    replay = store.release_leases(
        plan_id=result.plan_id,
        actor="accountable-human",
        reason="Pilot stop condition reached.",
        revoke=True,
        idempotency_key="revoke-phase2",
    )
    assert replay.model_dump() == revoked.model_dump()


def test_idempotent_retry_resumes_interrupted_materialization(tmp_path, monkeypatch):
    store, workforce, runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="resume-builder",
        competencies=["python"],
        writable_domains=["app"],
        authority="A2",
    )
    original = store._complete_materialization

    def interrupted(_plan_id: str, *, actor: str):
        raise RuntimeError(f"interrupted:{actor}")

    monkeypatch.setattr(store, "_complete_materialization", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        store.materialize_invocation(
            objective="Resume a bounded materialization.",
            required_competencies=["python"],
            writable_domains=["app"],
            preferred_worker_ids=[worker.worker_id],
            human_approval=True,
            actor="accountable-human",
            idempotency_key="resume-materialization",
        )
    monkeypatch.setattr(store, "_complete_materialization", original)

    resumed = store.materialize_invocation(
        objective="Resume a bounded materialization.",
        required_competencies=["python"],
        writable_domains=["app"],
        preferred_worker_ids=[worker.worker_id],
        human_approval=True,
        actor="accountable-human",
        idempotency_key="resume-materialization",
    )
    assert resumed.status == MaterializationStatus.MATERIALIZED.value
    assert runtime.get_mission(resumed.mission_id).found is True


def test_idempotent_retry_resumes_interrupted_approval(tmp_path, monkeypatch):
    store, workforce, runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="resume-a3-builder",
        competencies=["python"],
        writable_domains=["app"],
        authority="A3",
    )
    prepared = store.materialize_invocation(
        objective="Approve and resume an A3 plan.",
        required_competencies=["python"],
        writable_domains=["app"],
        authority_ceiling="A3",
        preferred_worker_ids=[worker.worker_id],
        plan_id="resume-approval-plan",
        mission_id="resume-approval-mission",
    )
    assert prepared.status == MaterializationStatus.AWAITING_APPROVAL.value
    original = store._complete_materialization

    def interrupted(_plan_id: str, *, actor: str):
        raise RuntimeError(f"approval-interrupted:{actor}")

    monkeypatch.setattr(store, "_complete_materialization", interrupted)
    with pytest.raises(RuntimeError, match="approval-interrupted"):
        store.approve_materialization(
            plan_id=prepared.plan_id,
            actor="accountable-human",
            reason="Approve bounded work.",
            idempotency_key="resume-approval",
        )
    monkeypatch.setattr(store, "_complete_materialization", original)

    resumed = store.approve_materialization(
        plan_id=prepared.plan_id,
        actor="accountable-human",
        reason="Approve bounded work.",
        idempotency_key="resume-approval",
    )
    assert resumed.status == MaterializationStatus.MATERIALIZED.value
    assert runtime.get_mission("resume-approval-mission").found is True
