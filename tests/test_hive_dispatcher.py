from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from app.hive_dispatcher import (
    AdapterStatus,
    ExecutionStatus,
    HiveExecutionDispatcherStore,
)
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
        HiveExecutionDispatcherStore(path),
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
        source_boundary="approved phase-3 test sources",
        appointment_scope="phase-3 validation",
        actor="test-owner",
        idempotency_key=f"register-p3-{worker_id}",
    )
    shadow = workforce.transition_worker(
        worker_id=worker_id,
        target_stage="SHADOW",
        actor="test-owner",
        reason="Begin shadow evaluation.",
        expected_revision=created.workers[0].revision,
        idempotency_key=f"shadow-p3-{worker_id}",
    )
    probation = workforce.transition_worker(
        worker_id=worker_id,
        target_stage="PROBATION",
        actor="test-owner",
        reason="Approved bounded probation.",
        human_approval=True,
        expected_revision=shadow.workers[0].revision,
        idempotency_key=f"probation-p3-{worker_id}",
    )
    return workforce.transition_worker(
        worker_id=worker_id,
        target_stage="APPOINTED",
        actor="test-owner",
        reason="Approved bounded appointment.",
        human_approval=True,
        expected_revision=probation.workers[0].revision,
        idempotency_key=f"appoint-p3-{worker_id}",
    ).workers[0]


def _materialize_one(
    materialization: HiveMaterializationStore,
    worker_id: str,
    *,
    plan_id: str,
    competency: str = "python",
    domains: list[str] | None = None,
    authority: str = "A2",
):
    return materialization.materialize_invocation(
        objective=f"Execute bounded Phase 3 plan {plan_id}.",
        required_competencies=[competency],
        writable_domains=domains or [],
        authority_ceiling=authority,
        preferred_worker_ids=[worker_id],
        human_approval=True,
        actor="accountable-human",
        plan_id=plan_id,
        mission_id=f"mission-{plan_id}",
        idempotency_key=f"materialize-{plan_id}",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)


def _enable(
    dispatcher: HiveExecutionDispatcherStore,
    adapter_id: str,
    *,
    domains: list[str] | None = None,
):
    current = dispatcher.list_adapters(adapter_id=adapter_id).adapters[0]
    return dispatcher.upsert_adapter(
        adapter_id=adapter_id,
        implementation_id=adapter_id,
        display_name=current.display_name,
        capabilities=current.capabilities,
        writable_domains=domains if domains is not None else current.writable_domains,
        required_authority=current.required_authority,
        max_timeout_ms=current.max_timeout_ms,
        max_payload_bytes=current.max_payload_bytes,
        requires_human_approval=current.requires_human_approval,
        requires_independent_review=current.requires_independent_review,
        enabled=True,
        actor="accountable-human",
        human_approval=True,
        expected_revision=current.revision,
        idempotency_key=f"enable-{adapter_id}",
    ).adapters[0]


def test_schema_is_additive_and_builtin_adapters_start_disabled(tmp_path):
    path = tmp_path / "hive.sqlite3"
    runtime = HiveRuntimeStore(path)
    runtime.create_mission(
        title="Existing mission",
        objective="Preserve existing state.",
        lifecycle_stage="Pilot",
        mission_id="existing-phase3-mission",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)
    with sqlite3.connect(path) as conn:
        before_version = conn.execute("PRAGMA user_version").fetchone()[0]
    dispatcher = HiveExecutionDispatcherStore(path)
    with sqlite3.connect(path) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        after_version = conn.execute("PRAGMA user_version").fetchone()[0]
        mission_count = conn.execute("SELECT COUNT(*) FROM hive_missions").fetchone()[0]
    assert before_version == after_version == 2
    assert mission_count == 1
    assert {
        "hive_execution_adapters",
        "hive_execution_adapter_events",
        "hive_dispatch_executions",
        "hive_execution_reviews",
        "hive_execution_events",
    }.issubset(tables)
    adapters = dispatcher.list_adapters().adapters
    assert {item.adapter_id for item in adapters} == {
        "builtin.noop.v1",
        "builtin.evidence_digest.v1",
        "builtin.bounded_delay.v1",
        "builtin.sandbox_artifact.v1",
    }
    assert {item.status for item in adapters} == {AdapterStatus.DISABLED.value}


def test_unknown_adapter_and_unapproved_enable_are_rejected(tmp_path):
    dispatcher, _materialization, _workforce, _runtime = _stores(tmp_path)
    with pytest.raises(HiveTransitionError, match="Unknown adapter implementation"):
        dispatcher.upsert_adapter(
            adapter_id="unsafe.shell",
            implementation_id="unsafe.shell",
            display_name="Unsafe",
            capabilities=["shell"],
            writable_domains=["system"],
            required_authority="A4",
            max_timeout_ms=1000,
            max_payload_bytes=1000,
            requires_human_approval=True,
            requires_independent_review=True,
            enabled=False,
            actor="test-owner",
        )
    current = dispatcher.list_adapters(adapter_id="builtin.noop.v1").adapters[0]
    with pytest.raises(HiveTransitionError, match="Governance-plan authorization"):
        dispatcher.upsert_adapter(
            adapter_id=current.adapter_id,
            implementation_id=current.implementation_id,
            display_name=current.display_name,
            capabilities=current.capabilities,
            writable_domains=current.writable_domains,
            required_authority=current.required_authority,
            max_timeout_ms=current.max_timeout_ms,
            max_payload_bytes=current.max_payload_bytes,
            requires_human_approval=False,
            requires_independent_review=False,
            enabled=True,
            actor="test-owner",
            expected_revision=current.revision,
        )

    authorized = dispatcher.upsert_adapter(
        adapter_id=current.adapter_id,
        implementation_id=current.implementation_id,
        display_name=current.display_name,
        capabilities=current.capabilities,
        writable_domains=current.writable_domains,
        required_authority=current.required_authority,
        max_timeout_ms=current.max_timeout_ms,
        max_payload_bytes=current.max_payload_bytes,
        requires_human_approval=False,
        requires_independent_review=False,
        enabled=True,
        actor="governance-engine",
        governance_authorization={
            "decision_id": "enable-noop",
            "plan_id": "adapter-plan",
            "authorized": True,
            "governance_aligned": True,
            "authority_ceiling": "A2",
            "inferred_risk": "low",
            "confidence": 0.97,
            "evidence_count": 4,
            "reversible": True,
            "source_boundary_ok": True,
            "writable_domain_ok": True,
            "dependency_state_ok": True,
        },
        expected_revision=current.revision,
    )
    assert authorized.adapters[0].status == AdapterStatus.ENABLED.value


def test_compiled_requirements_cannot_be_weakened(tmp_path):
    dispatcher, _materialization, _workforce, _runtime = _stores(tmp_path)
    current = dispatcher.list_adapters(adapter_id="builtin.bounded_delay.v1").adapters[
        0
    ]
    updated = dispatcher.upsert_adapter(
        adapter_id=current.adapter_id,
        implementation_id=current.implementation_id,
        display_name=current.display_name,
        capabilities=current.capabilities,
        writable_domains=current.writable_domains,
        required_authority=current.required_authority,
        max_timeout_ms=current.max_timeout_ms,
        max_payload_bytes=current.max_payload_bytes,
        requires_human_approval=False,
        requires_independent_review=False,
        enabled=False,
        actor="test-owner",
        expected_revision=current.revision,
    ).adapters[0]
    assert updated.requires_human_approval is True


def test_disabled_adapter_is_durably_denied_without_consuming_lane(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="disabled-builder",
        competencies=["python"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="disabled-plan",
        authority="A1",
    )
    denied = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.noop.v1",
        requested_capability="noop",
        payload={"message": "blocked"},
        requested_writable_domains=[],
        timeout_ms=500,
        actor=worker.worker_id,
        idempotency_key="disabled-execution",
    )
    assert denied.executions[0].status == ExecutionStatus.DENIED.value
    assert denied.executions[0].error_code == "POLICY_DENIED"
    current = materialization.get_materialization(plan_id=plan.plan_id)
    assert current.status == MaterializationStatus.MATERIALIZED.value
    assert current.dispatch_receipts[0].status == DispatchStatus.READY.value
    assert current.leases[0].status == LeaseStatus.ACTIVE.value


def test_noop_execution_completes_lane_and_replay_is_idempotent(tmp_path):
    dispatcher, materialization, workforce, runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="noop-builder",
        competencies=["python"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="noop-plan",
        authority="A1",
    )
    _enable(dispatcher, "builtin.noop.v1")
    kwargs = {
        "plan_id": plan.plan_id,
        "work_unit_id": plan.work_unit_ids[0],
        "adapter_id": "builtin.noop.v1",
        "requested_capability": "noop",
        "payload": {"message": "safe", "metadata": {"pilot": True}},
        "requested_writable_domains": [],
        "timeout_ms": 500,
        "actor": worker.worker_id,
        "idempotency_key": "noop-execution",
    }
    completed = dispatcher.execute_dispatch(**kwargs)
    replay = dispatcher.execute_dispatch(**kwargs)
    execution = completed.executions[0]
    assert execution.status == ExecutionStatus.COMPLETED.value
    assert execution.result["performed_external_effect"] is False
    assert replay.model_dump() == completed.model_dump()
    current = materialization.get_materialization(plan_id=plan.plan_id)
    assert current.status == MaterializationStatus.READY_FOR_FAN_IN.value
    assert current.dispatch_receipts[0].status == DispatchStatus.COMPLETED.value
    assert current.leases[0].status == LeaseStatus.RELEASED.value
    mission = runtime.get_mission(plan.mission_id)
    assert mission.work_units[0].status == "COMPLETED"


def test_execution_idempotency_conflict_is_detected(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="idempotent-builder",
        competencies=["python"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="idempotent-plan",
        authority="A1",
    )
    _enable(dispatcher, "builtin.noop.v1")
    dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.noop.v1",
        requested_capability="noop",
        payload={"message": "first"},
        requested_writable_domains=[],
        timeout_ms=500,
        actor=worker.worker_id,
        idempotency_key="same-execution-key",
    )
    with pytest.raises(HiveIdempotencyConflict):
        dispatcher.execute_dispatch(
            plan_id=plan.plan_id,
            work_unit_id=plan.work_unit_ids[0],
            adapter_id="builtin.noop.v1",
            requested_capability="noop",
            payload={"message": "different"},
            requested_writable_domains=[],
            timeout_ms=500,
            actor=worker.worker_id,
            idempotency_key="same-execution-key",
        )


def test_capability_domain_and_authority_checks_fail_closed(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="bounded-builder",
        competencies=["digest"],
        writable_domains=["evidence"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="bounded-plan",
        competency="digest",
        domains=["evidence"],
        authority="A1",
    )
    _enable(dispatcher, "builtin.evidence_digest.v1", domains=["evidence"])
    denied_capability = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.evidence_digest.v1",
        requested_capability="shell",
        payload={"label": "x", "items": []},
        requested_writable_domains=["evidence"],
        timeout_ms=500,
        actor=worker.worker_id,
        idempotency_key="denied-capability",
    )
    assert denied_capability.executions[0].status == ExecutionStatus.DENIED.value
    denied_domain = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.evidence_digest.v1",
        requested_capability="evidence_digest",
        payload={"label": "x", "items": []},
        requested_writable_domains=["system"],
        timeout_ms=500,
        actor=worker.worker_id,
        idempotency_key="denied-domain",
    )
    assert denied_domain.executions[0].status == ExecutionStatus.DENIED.value
    current = dispatcher.list_adapters(
        adapter_id="builtin.evidence_digest.v1"
    ).adapters[0]
    dispatcher.upsert_adapter(
        adapter_id=current.adapter_id,
        implementation_id=current.implementation_id,
        display_name=current.display_name,
        capabilities=current.capabilities,
        writable_domains=current.writable_domains,
        required_authority="A2",
        max_timeout_ms=current.max_timeout_ms,
        max_payload_bytes=current.max_payload_bytes,
        requires_human_approval=current.requires_human_approval,
        requires_independent_review=current.requires_independent_review,
        enabled=True,
        actor="accountable-human",
        human_approval=True,
        expected_revision=current.revision,
        idempotency_key="raise-adapter-authority",
    )
    denied_authority = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.evidence_digest.v1",
        requested_capability="evidence_digest",
        payload={"label": "x", "items": []},
        requested_writable_domains=["evidence"],
        timeout_ms=500,
        actor=worker.worker_id,
        idempotency_key="denied-authority",
    )
    assert denied_authority.executions[0].status == ExecutionStatus.DENIED.value
    assert (
        materialization.get_materialization(plan_id=plan.plan_id)
        .dispatch_receipts[0]
        .status
        == DispatchStatus.READY.value
    )


def test_payload_keys_and_payload_size_are_bounded(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="payload-builder",
        competencies=["python"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="payload-plan",
        authority="A1",
    )
    _enable(dispatcher, "builtin.noop.v1")
    with pytest.raises(HiveTransitionError, match="Blocked execution payload key"):
        dispatcher.execute_dispatch(
            plan_id=plan.plan_id,
            work_unit_id=plan.work_unit_ids[0],
            adapter_id="builtin.noop.v1",
            requested_capability="noop",
            payload={"command": "whoami"},
            requested_writable_domains=[],
            timeout_ms=500,
            actor=worker.worker_id,
        )
    with pytest.raises(HiveTransitionError, match="payload exceeds"):
        dispatcher.execute_dispatch(
            plan_id=plan.plan_id,
            work_unit_id=plan.work_unit_ids[0],
            adapter_id="builtin.noop.v1",
            requested_capability="noop",
            payload={"message": "x" * 5000},
            requested_writable_domains=[],
            timeout_ms=500,
            actor=worker.worker_id,
        )
    assert dispatcher.get_execution(plan_id=plan.plan_id).count == 0


def test_review_required_execution_needs_distinct_governance_reviewer(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    builder = _appoint(
        workforce,
        worker_id="digest-builder",
        competencies=["digest"],
        writable_domains=["evidence"],
        authority="A2",
    )
    reviewer = _appoint(
        workforce,
        worker_id="digest-reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
        authority="A2",
    )
    ordinary = _appoint(
        workforce,
        worker_id="ordinary-reviewer",
        competencies=["governance"],
        authority="A2",
    )
    plan = _materialize_one(
        materialization,
        builder.worker_id,
        plan_id="review-plan",
        competency="digest",
        domains=["evidence"],
        authority="A2",
    )
    _enable(dispatcher, "builtin.evidence_digest.v1", domains=["evidence"])
    pending = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.evidence_digest.v1",
        requested_capability="evidence_digest",
        payload={"label": "pilot", "items": [1, {"a": 2}]},
        requested_writable_domains=["evidence"],
        timeout_ms=1000,
        actor=builder.worker_id,
        idempotency_key="digest-execution",
    )
    execution = pending.executions[0]
    assert execution.status == ExecutionStatus.REVIEW_REQUIRED.value
    assert (
        materialization.get_materialization(plan_id=plan.plan_id)
        .dispatch_receipts[0]
        .status
        == DispatchStatus.ACKNOWLEDGED.value
    )
    with pytest.raises(HiveTransitionError, match="cannot independently review"):
        dispatcher.review_execution(
            execution_id=execution.execution_id,
            reviewer_worker_id=builder.worker_id,
            approved=True,
            actor="human-review",
            findings={"valid": True},
            human_approval=True,
        )
    with pytest.raises(HiveTransitionError, match="GOVERNANCE_REVIEWER"):
        dispatcher.review_execution(
            execution_id=execution.execution_id,
            reviewer_worker_id=ordinary.worker_id,
            approved=True,
            actor="human-review",
            findings={"valid": True},
            human_approval=True,
        )
    approved = dispatcher.review_execution(
        execution_id=execution.execution_id,
        reviewer_worker_id=reviewer.worker_id,
        approved=True,
        actor="human-review",
        findings={"valid": True},
        human_approval=True,
        idempotency_key="approve-digest-execution",
    )
    assert approved.executions[0].status == ExecutionStatus.COMPLETED.value
    assert approved.reviews[0].approved is True
    current = materialization.get_materialization(plan_id=plan.plan_id)
    assert current.status == MaterializationStatus.READY_FOR_FAN_IN.value
    assert current.dispatch_receipts[0].status == DispatchStatus.COMPLETED.value
    assert current.leases[0].status == LeaseStatus.RELEASED.value


def test_rejected_independent_review_fails_lane(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    builder = _appoint(
        workforce,
        worker_id="rejected-builder",
        competencies=["digest"],
        writable_domains=["evidence"],
        authority="A2",
    )
    reviewer = _appoint(
        workforce,
        worker_id="rejected-reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
        authority="A2",
    )
    plan = _materialize_one(
        materialization,
        builder.worker_id,
        plan_id="rejected-plan",
        competency="digest",
        domains=["evidence"],
        authority="A2",
    )
    _enable(dispatcher, "builtin.evidence_digest.v1", domains=["evidence"])
    pending = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.evidence_digest.v1",
        requested_capability="evidence_digest",
        payload={"label": "reject", "items": [1]},
        requested_writable_domains=["evidence"],
        timeout_ms=1000,
        actor=builder.worker_id,
    )
    rejected = dispatcher.review_execution(
        execution_id=pending.executions[0].execution_id,
        reviewer_worker_id=reviewer.worker_id,
        approved=False,
        actor="human-review",
        findings={"reason": "insufficient evidence"},
        human_approval=True,
    )
    assert rejected.executions[0].status == ExecutionStatus.FAILED.value
    current = materialization.get_materialization(plan_id=plan.plan_id)
    assert current.status == MaterializationStatus.CLOSED_WITH_FAILURE.value
    assert current.dispatch_receipts[0].status == DispatchStatus.FAILED.value
    assert current.leases[0].status == LeaseStatus.RELEASED.value


def test_bounded_delay_times_out_and_fails_lane(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="timeout-builder",
        competencies=["delay"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="timeout-plan",
        competency="delay",
        authority="A1",
    )
    _enable(dispatcher, "builtin.bounded_delay.v1")
    timed_out = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.bounded_delay.v1",
        requested_capability="bounded_delay",
        payload={"delay_ms": 250, "label": "timeout"},
        requested_writable_domains=[],
        timeout_ms=30,
        actor=worker.worker_id,
        human_approval=True,
        idempotency_key="timeout-execution",
    )
    execution = timed_out.executions[0]
    assert execution.status == ExecutionStatus.TIMED_OUT.value
    assert execution.error_code == "TIMEOUT"
    current = materialization.get_materialization(plan_id=plan.plan_id)
    assert current.status == MaterializationStatus.CLOSED_WITH_FAILURE.value
    assert current.dispatch_receipts[0].status == DispatchStatus.FAILED.value
    assert current.leases[0].status == LeaseStatus.RELEASED.value


def test_running_execution_can_be_cooperatively_cancelled(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="cancel-builder",
        competencies=["delay"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="cancel-plan",
        competency="delay",
        authority="A1",
    )
    _enable(dispatcher, "builtin.bounded_delay.v1")
    holder: dict[str, object] = {}

    def run_execution():
        holder["result"] = dispatcher.execute_dispatch(
            plan_id=plan.plan_id,
            work_unit_id=plan.work_unit_ids[0],
            adapter_id="builtin.bounded_delay.v1",
            requested_capability="bounded_delay",
            payload={"delay_ms": 1500, "label": "cancel"},
            requested_writable_domains=[],
            timeout_ms=3000,
            actor=worker.worker_id,
            human_approval=True,
            idempotency_key="cancel-execution",
        )

    thread = threading.Thread(target=run_execution)
    thread.start()
    execution_id = ""
    deadline = time.monotonic() + 3
    while time.monotonic() < deadline:
        snapshot = dispatcher.get_execution(plan_id=plan.plan_id)
        if (
            snapshot.executions
            and snapshot.executions[0].status == ExecutionStatus.RUNNING.value
        ):
            execution_id = snapshot.executions[0].execution_id
            break
        time.sleep(0.02)
    assert execution_id
    cancellation = dispatcher.cancel_execution(
        execution_id=execution_id,
        actor="accountable-human",
        reason="Stop the bounded pilot.",
        idempotency_key="cancel-running-execution",
    )
    assert cancellation.executions[0].cancellation_requested is True
    thread.join(timeout=5)
    assert not thread.is_alive()
    final = holder["result"]
    assert final.executions[0].status == ExecutionStatus.CANCELLED.value
    current = materialization.get_materialization(plan_id=plan.plan_id)
    assert current.dispatch_receipts[0].status == DispatchStatus.CANCELLED.value
    assert current.leases[0].status == LeaseStatus.RELEASED.value


def test_stale_claimed_execution_recovers_idempotently(tmp_path, monkeypatch):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="recovery-builder",
        competencies=["python"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="recovery-plan",
        authority="A1",
    )
    _enable(dispatcher, "builtin.noop.v1")
    original = dispatcher._execute_existing

    def interrupted(_execution_id: str, *, actor: str):
        raise RuntimeError(f"interrupted:{actor}")

    monkeypatch.setattr(dispatcher, "_execute_existing", interrupted)
    with pytest.raises(RuntimeError, match="interrupted"):
        dispatcher.execute_dispatch(
            plan_id=plan.plan_id,
            work_unit_id=plan.work_unit_ids[0],
            adapter_id="builtin.noop.v1",
            requested_capability="noop",
            payload={"message": "recover"},
            requested_writable_domains=[],
            timeout_ms=500,
            actor=worker.worker_id,
            idempotency_key="interrupted-execution",
        )
    monkeypatch.setattr(dispatcher, "_execute_existing", original)
    interrupted_snapshot = dispatcher.get_execution(plan_id=plan.plan_id)
    execution = interrupted_snapshot.executions[0]
    assert execution.status == ExecutionStatus.CLAIMED.value
    with sqlite3.connect(dispatcher.path) as conn:
        conn.execute(
            "UPDATE hive_dispatch_executions SET updated_at = 0 WHERE execution_id = ?",
            (execution.execution_id,),
        )
        conn.commit()
    recovered = dispatcher.recover_execution(
        execution_id=execution.execution_id,
        actor="accountable-human",
        reason="Recover stale claimed execution.",
        stale_after_ms=1,
        human_approval=True,
        idempotency_key="recover-interrupted-execution",
    )
    assert recovered.executions[0].status == ExecutionStatus.COMPLETED.value
    assert recovered.executions[0].attempt == 2
    replay = dispatcher.recover_execution(
        execution_id=execution.execution_id,
        actor="accountable-human",
        reason="Recover stale claimed execution.",
        stale_after_ms=1,
        human_approval=True,
        idempotency_key="recover-interrupted-execution",
    )
    assert replay.model_dump() == recovered.model_dump()
    assert (
        materialization.get_materialization(plan_id=plan.plan_id).status
        == MaterializationStatus.READY_FOR_FAN_IN.value
    )


def test_fresh_execution_recovery_requires_staleness_and_governance_authorization(
    tmp_path, monkeypatch
):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="fresh-builder",
        competencies=["python"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="fresh-plan",
        authority="A1",
    )
    _enable(dispatcher, "builtin.noop.v1")
    original = dispatcher._execute_existing

    def interrupted(_execution_id: str, *, actor: str):
        raise RuntimeError(f"interrupted:{actor}")

    monkeypatch.setattr(dispatcher, "_execute_existing", interrupted)
    with pytest.raises(RuntimeError):
        dispatcher.execute_dispatch(
            plan_id=plan.plan_id,
            work_unit_id=plan.work_unit_ids[0],
            adapter_id="builtin.noop.v1",
            requested_capability="noop",
            payload={"message": "fresh"},
            requested_writable_domains=[],
            timeout_ms=500,
            actor=worker.worker_id,
        )
    monkeypatch.setattr(dispatcher, "_execute_existing", original)
    execution = dispatcher.get_execution(plan_id=plan.plan_id).executions[0]
    with pytest.raises(HiveTransitionError, match="Governance-plan authorization"):
        dispatcher.recover_execution(
            execution_id=execution.execution_id,
            actor="human",
            reason="No approval.",
            stale_after_ms=0,
        )
    with pytest.raises(HiveTransitionError, match="not stale"):
        dispatcher.recover_execution(
            execution_id=execution.execution_id,
            actor="human",
            reason="Too fresh.",
            stale_after_ms=60000,
            human_approval=True,
        )


def test_review_required_execution_can_be_cancelled_before_review(tmp_path):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    builder = _appoint(
        workforce,
        worker_id="cancel-review-builder",
        competencies=["digest"],
        writable_domains=["evidence"],
        authority="A2",
    )
    plan = _materialize_one(
        materialization,
        builder.worker_id,
        plan_id="cancel-review-plan",
        competency="digest",
        domains=["evidence"],
        authority="A2",
    )
    _enable(dispatcher, "builtin.evidence_digest.v1", domains=["evidence"])
    pending = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.evidence_digest.v1",
        requested_capability="evidence_digest",
        payload={"label": "cancel", "items": []},
        requested_writable_domains=["evidence"],
        timeout_ms=1000,
        actor=builder.worker_id,
    )
    cancelled = dispatcher.cancel_execution(
        execution_id=pending.executions[0].execution_id,
        actor="accountable-human",
        reason="Review gate cancelled.",
        idempotency_key="cancel-review-gate",
    )
    assert cancelled.executions[0].status == ExecutionStatus.CANCELLED.value
    current = materialization.get_materialization(plan_id=plan.plan_id)
    assert current.dispatch_receipts[0].status == DispatchStatus.CANCELLED.value
    assert current.leases[0].status == LeaseStatus.RELEASED.value


def test_adapter_upsert_is_idempotent_and_revision_checked(tmp_path):
    dispatcher, _materialization, _workforce, _runtime = _stores(tmp_path)
    current = dispatcher.list_adapters(adapter_id="builtin.noop.v1").adapters[0]
    kwargs = {
        "adapter_id": current.adapter_id,
        "implementation_id": current.implementation_id,
        "display_name": current.display_name,
        "capabilities": current.capabilities,
        "writable_domains": current.writable_domains,
        "required_authority": current.required_authority,
        "max_timeout_ms": current.max_timeout_ms,
        "max_payload_bytes": current.max_payload_bytes,
        "requires_human_approval": current.requires_human_approval,
        "requires_independent_review": current.requires_independent_review,
        "enabled": True,
        "actor": "accountable-human",
        "human_approval": True,
        "expected_revision": current.revision,
        "idempotency_key": "idempotent-adapter-enable",
    }
    enabled = dispatcher.upsert_adapter(**kwargs)
    replay = dispatcher.upsert_adapter(**kwargs)
    assert replay.model_dump() == enabled.model_dump()
    with pytest.raises(HiveTransitionError, match="Stale adapter revision"):
        dispatcher.upsert_adapter(
            **{
                **kwargs,
                "idempotency_key": "stale-adapter-update",
                "display_name": "Changed",
            }
        )


def test_idempotent_recovery_replay_resumes_after_second_crash(tmp_path, monkeypatch):
    dispatcher, materialization, workforce, _runtime = _stores(tmp_path)
    worker = _appoint(
        workforce,
        worker_id="double-recovery-builder",
        competencies=["python"],
        authority="A1",
    )
    plan = _materialize_one(
        materialization,
        worker.worker_id,
        plan_id="double-recovery-plan",
        authority="A1",
    )
    _enable(dispatcher, "builtin.noop.v1")
    original = dispatcher._execute_existing

    def interrupted(_execution_id: str, *, actor: str):
        raise RuntimeError(f"interrupted:{actor}")

    monkeypatch.setattr(dispatcher, "_execute_existing", interrupted)
    with pytest.raises(RuntimeError):
        dispatcher.execute_dispatch(
            plan_id=plan.plan_id,
            work_unit_id=plan.work_unit_ids[0],
            adapter_id="builtin.noop.v1",
            requested_capability="noop",
            payload={"message": "recover twice"},
            requested_writable_domains=[],
            timeout_ms=500,
            actor=worker.worker_id,
            idempotency_key="double-recovery-execution",
        )
    execution = dispatcher.get_execution(plan_id=plan.plan_id).executions[0]
    with sqlite3.connect(dispatcher.path) as conn:
        conn.execute(
            "UPDATE hive_dispatch_executions SET updated_at = 0 WHERE execution_id = ?",
            (execution.execution_id,),
        )
        conn.commit()
    with pytest.raises(RuntimeError):
        dispatcher.recover_execution(
            execution_id=execution.execution_id,
            actor="accountable-human",
            reason="First recovery attempt crashes after recording the recovery event.",
            stale_after_ms=1,
            human_approval=True,
            idempotency_key="double-recovery-key",
        )
    monkeypatch.setattr(dispatcher, "_execute_existing", original)
    resumed = dispatcher.recover_execution(
        execution_id=execution.execution_id,
        actor="accountable-human",
        reason="First recovery attempt crashes after recording the recovery event.",
        stale_after_ms=1,
        human_approval=True,
        idempotency_key="double-recovery-key",
    )
    assert resumed.executions[0].status == ExecutionStatus.COMPLETED.value
    assert resumed.executions[0].attempt == 2
    assert (
        materialization.get_materialization(plan_id=plan.plan_id).status
        == MaterializationStatus.READY_FOR_FAN_IN.value
    )
