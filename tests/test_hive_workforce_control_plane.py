from __future__ import annotations

import sqlite3

from app.hive_materialization import HiveMaterializationStore
from app.hive_workforce_governor import HiveWorkforceGovernor


def _store(tmp_path):
    return HiveMaterializationStore(tmp_path / "hive.sqlite3")


def _authorization(authority: str = "A3") -> dict[str, object]:
    return {
        "decision_id": f"decision-{authority.lower()}",
        "plan_id": "workforce-lifecycle-test-plan",
        "authorized": True,
        "governance_aligned": True,
        "authority_ceiling": authority,
        "inferred_risk": "moderate",
        "confidence": 1.0,
        "evidence_count": 3,
        "reversible": True,
        "source_boundary_ok": True,
        "writable_domain_ok": True,
        "dependency_state_ok": True,
        "decided_by": "test-governance",
        "authorization_basis": "adopted_plan",
    }


def test_requisition_only_populates_portal_control_plane(tmp_path):
    store = _store(tmp_path)
    materialized = store.materialize_invocation(
        objective="Implement a missing portal projection.",
        required_competencies=["portal projection", "python"],
        writable_domains=["app", "tests"],
        risk_level="high",
        recruitment_mode="requisition_only",
        plan_id="portal-gap-plan",
        mission_id="portal-gap-mission",
        idempotency_key="portal-gap-request",
    )

    assert materialized.status == "RECRUITING"
    assert len(materialized.recruited_worker_ids) == 1

    overview = store.control_plane.overview()
    assert overview.policy.automatic_hiring_enabled is False
    assert len(overview.requisitions) == 1
    requisition = overview.requisitions[0]
    assert requisition.plan_id == "portal-gap-plan"
    assert requisition.urgency == "HIGH"
    assert requisition.status == "REQUISITIONED"
    assert requisition.matching_attempts == 1
    assert requisition.candidate_count == 1
    assert requisition.evaluations[0].model_id == "terra"
    assert requisition.evaluations[0].account_profile == "auto"

    worker = next(
        item
        for item in overview.registry
        if item.worker_id == materialized.recruited_worker_ids[0]
    )
    assert worker.appointment_state == "REQUISITIONED"
    assert worker.model_id == "terra"
    assert worker.account_profile == "auto"
    assert worker.runtime_state == "UNASSIGNED"
    assert worker.current_assignment is None
    assert overview.metrics.open_requisitions == 1
    assert overview.metrics.blocked_plans_competency_gaps == 1


def test_enabled_policy_processes_candidate_and_records_appointment(tmp_path):
    store = _store(tmp_path)
    materialized = store.materialize_invocation(
        objective="Fill one governed competency gap.",
        required_competencies=["candidate evaluation"],
        writable_domains=["tests"],
        recruitment_mode="requisition_only",
        plan_id="queued-appointment-plan",
        mission_id="queued-appointment-mission",
        idempotency_key="queued-appointment-request",
    )
    worker_id = materialized.recruited_worker_ids[0]

    current = store.control_plane.get_policy()
    updated = current.model_copy(
        update={
            "automatic_hiring_enabled": True,
            "recruitment_mode": "auto_appoint",
            "minimum_evaluation_score": 0.8,
        }
    )
    policy = store.control_plane.update_policy(
        actor="governance-owner",
        policy=updated,
        governance_authorization=_authorization(),
        expected_revision=current.revision,
    )
    assert policy.automatic_hiring_enabled is True

    result = store.control_plane.process_recruitment_queue(
        actor="workforce-governor",
        governance_authorization=_authorization("A2"),
    )
    assert result["appointed"] == [
        {
            "requisition_id": store.control_plane.overview().requisitions[0].requisition_id,
            "worker_id": worker_id,
        }
    ]

    overview = store.control_plane.overview()
    requisition = overview.requisitions[0]
    assert requisition.status == "APPOINTED"
    assert requisition.appointed_worker_id == worker_id
    worker = next(item for item in overview.registry if item.worker_id == worker_id)
    assert worker.appointment_state == "APPOINTED"
    assert worker.quarantine_state == "CLEARED"
    assert overview.metrics.successful_appointments == 1


def test_policy_update_requires_governed_authorization(tmp_path):
    store = _store(tmp_path)
    current = store.control_plane.get_policy()
    updated = current.model_copy(update={"automatic_hiring_enabled": True})

    try:
        store.control_plane.update_policy(actor="operator", policy=updated)
    except Exception as exc:
        assert "authorization" in str(exc).lower()
    else:
        raise AssertionError("Policy update unexpectedly bypassed governance authorization.")


def test_lease_monitor_and_governor_cleanup_are_backend_owned(tmp_path):
    store = _store(tmp_path)
    materialized = store.materialize_invocation(
        objective="Create one expiring execution lease.",
        required_competencies=["lease testing"],
        writable_domains=["tests"],
        recruitment_mode="auto_appoint",
        governance_authorization=_authorization("A2"),
        plan_id="stale-lease-plan",
        mission_id="stale-lease-mission",
        idempotency_key="stale-lease-request",
    )
    assert materialized.status == "MATERIALIZED"
    lease = materialized.leases[0]

    with sqlite3.connect(store.path) as conn:
        conn.execute(
            """
            UPDATE hive_worker_leases
            SET expires_at=1, last_heartbeat_at=1, heartbeat_status='OFFLINE'
            WHERE lease_id=?
            """,
            (lease.lease_id,),
        )

    monitored = store.control_plane.overview().leases[0]
    assert monitored.stale is True
    assert monitored.automatic_cleanup_action == "EXPIRE"
    assert monitored.execution_live is False

    governor = HiveWorkforceGovernor(db_path=store.path)
    result = governor.run_once()
    assert result["status"] == "COMPLETED"
    assert result["lease_reconciliation"]["changed_count"] == 1

    refreshed = store.control_plane.overview()
    cleaned = next(item for item in refreshed.leases if item.lease_id == lease.lease_id)
    assert cleaned.status == "EXPIRED"
    assert refreshed.metrics.stale_leases_removed == 1


def test_server_registers_hive_workforce_http_contract(monkeypatch):
    monkeypatch.setenv(
        "NOTION_ACCOUNTS",
        '[{"token_v2":"test","space_id":"space","user_id":"user"}]',
    )
    from app.server import app

    paths = {route.path for route in app.routes}
    assert "/v1/hive/workforce/overview" in paths
    assert "/v1/hive/workforce/registry" in paths
    assert "/v1/hive/workforce/requisitions" in paths
    assert "/v1/hive/workforce/leases" in paths
    assert "/v1/hive/workforce/policy" in paths
    assert "/v1/hive/workforce/lease/heartbeat" in paths
    assert "/v1/hive/workforce/leases/reconcile" in paths
    assert "/v1/hive/workforce/recruitment/process" in paths
    assert "/v1/hive/workforce/audits" in paths


def test_worker_limit_blocks_new_recruitment_and_records_outcome(tmp_path):
    store = _store(tmp_path)
    current = store.control_plane.get_policy()
    limited = current.model_copy(update={"max_workers": 1})
    store.control_plane.update_policy(
        actor="governance-owner",
        policy=limited,
        governance_authorization=_authorization(),
        expected_revision=current.revision,
    )
    store.workforce.register_worker(
        worker_id="existing-worker",
        display_name="Existing Worker",
        worker_class="persistent_member",
        role="Existing Role",
        accountable_owner="owner",
        competencies=["existing"],
        writable_domains=["tests"],
        authority_ceiling="A1",
    )

    result = store.materialize_invocation(
        objective="Require a worker beyond the policy ceiling.",
        required_competencies=["new competency"],
        writable_domains=["app"],
        recruitment_mode="requisition_only",
        plan_id="worker-limit-plan",
        mission_id="worker-limit-mission",
        idempotency_key="worker-limit-request",
    )

    assert result.status == "BLOCKED"
    assert result.recruited_worker_ids == []
    overview = store.control_plane.overview()
    requisition = overview.requisitions[0]
    assert requisition.status == "FAILED"
    assert requisition.appointment_outcome == "WORKER_LIMIT_REACHED"
    assert requisition.candidate_count == 0


def test_governor_lane_failure_does_not_suppress_cleanup_or_audit(tmp_path):
    store = _store(tmp_path)
    materialized = store.materialize_invocation(
        objective="Create a stale lease and an open recruitment queue.",
        required_competencies=["governor isolation"],
        writable_domains=["tests"],
        recruitment_mode="auto_appoint",
        governance_authorization=_authorization("A2"),
        plan_id="governor-isolation-plan",
        mission_id="governor-isolation-mission",
        idempotency_key="governor-isolation-request",
    )
    lease = materialized.leases[0]
    with sqlite3.connect(store.path) as conn:
        conn.execute(
            "UPDATE hive_worker_leases SET expires_at=1 WHERE lease_id=?",
            (lease.lease_id,),
        )

    policy = store.control_plane.get_policy()
    enabled = policy.model_copy(update={"automatic_hiring_enabled": True})
    store.control_plane.update_policy(
        actor="governance-owner",
        policy=enabled,
        governance_authorization=_authorization(),
        expected_revision=policy.revision,
    )
    store.control_plane.open_requisition(
        plan_id="unapproved-queue-plan",
        objective="Queue requires appointment authorization.",
        requested_competencies=["missing"],
        requested_writable_domains=["tests"],
        urgency="NORMAL",
    )

    result = HiveWorkforceGovernor(db_path=store.path).run_once()
    assert result["status"] in {"COMPLETED", "PARTIAL"}
    assert result["lane_statuses"]["lease_reconciliation"] == "COMPLETED"
    assert result["lane_statuses"]["workforce_audit"] == "COMPLETED"
    cleaned = store.control_plane.overview()
    assert any(item.lease_id == lease.lease_id and item.status == "EXPIRED" for item in cleaned.leases)
    assert cleaned.governor_status["run_id"] == result["run_id"]
    assert cleaned.governor_status["status"] == result["status"]
