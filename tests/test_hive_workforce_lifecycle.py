from __future__ import annotations

import json
import sqlite3

from app.hive_materialization import (
    DispatchStatus,
    HiveMaterializationStore,
    MaterializationStatus,
)
from app.hive_workforce import HiveWorkforceStore, WorkerStage
from app.hive_workforce_lifecycle import (
    HiveWorkforceLifecycleStore,
    RecruitmentMode,
)


def _materialization(tmp_path) -> HiveMaterializationStore:
    return HiveMaterializationStore(tmp_path / "hive.sqlite3")


def _auto_materialize(tmp_path, *, plan_id: str = "auto-plan"):
    store = _materialization(tmp_path)
    result = store.materialize_invocation(
        objective="Implement one bounded Python lifecycle repair.",
        required_competencies=["python engineering", "lease lifecycle management"],
        writable_domains=["repo:notion2api"],
        authority_ceiling="A2",
        recruitment_mode=RecruitmentMode.AUTO_APPOINT.value,
        human_approval=True,
        plan_id=plan_id,
        mission_id=f"{plan_id}-mission",
        idempotency_key=f"{plan_id}-request",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)
    return store, result


def test_lifecycle_migrates_legacy_lease_schema_and_backfills_expiry(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    workforce = HiveWorkforceStore(path)
    now = 1_700_000_000_000
    with sqlite3.connect(path) as conn:
        conn.executescript(
            """
            CREATE TABLE hive_invocation_materializations (
                plan_id TEXT PRIMARY KEY
            );
            INSERT INTO hive_invocation_materializations(plan_id) VALUES ('legacy-plan');
            CREATE TABLE hive_worker_leases (
                lease_id TEXT PRIMARY KEY,
                plan_id TEXT NOT NULL,
                mission_id TEXT NOT NULL,
                work_unit_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                status TEXT NOT NULL,
                authority_ceiling TEXT NOT NULL,
                writable_domains_json TEXT NOT NULL DEFAULT '[]',
                source_boundary TEXT NOT NULL DEFAULT '',
                release_reason TEXT NOT NULL DEFAULT '',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                revision INTEGER NOT NULL
            );
            """
        )
        conn.execute(
            """
            INSERT INTO hive_worker_leases(
                lease_id, plan_id, mission_id, work_unit_id, worker_id,
                status, authority_ceiling, created_at, updated_at, revision
            ) VALUES ('lease-1', 'legacy-plan', 'mission-1', 'lane-1',
                      'worker-legacy', 'ACTIVE', 'A1', ?, ?, 1)
            """,
            (now, now),
        )
    HiveWorkforceLifecycleStore(path, workforce)
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        lease_columns = {
            row["name"] for row in conn.execute("PRAGMA table_info(hive_worker_leases)")
        }
        plan_columns = {
            row["name"]
            for row in conn.execute(
                "PRAGMA table_info(hive_invocation_materializations)"
            )
        }
        row = conn.execute(
            "SELECT issued_at, expires_at, heartbeat_status FROM hive_worker_leases"
        ).fetchone()
    assert {
        "issued_at",
        "expires_at",
        "last_heartbeat_at",
        "heartbeat_status",
        "renewal_count",
        "liveness_evidence_json",
    } <= lease_columns
    assert {"recruitment_mode", "recruited_worker_ids_json"} <= plan_columns
    assert row["issued_at"] == now
    assert row["expires_at"] > now
    assert row["heartbeat_status"] == "UNKNOWN"


def test_requisition_only_creates_deterministic_gap_worker_and_recruiting_plan(tmp_path):
    store = _materialization(tmp_path)
    first = store.materialize_invocation(
        objective="Fill missing runtime capability.",
        required_competencies=["worker heartbeat and liveness"],
        writable_domains=["repo:notion2api"],
        authority_ceiling="A2",
        recruitment_mode=RecruitmentMode.REQUISITION_ONLY.value,
        plan_id="recruiting-plan",
        mission_id="recruiting-mission",
        idempotency_key="recruiting-request",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)
    replay = store.materialize_invocation(
        objective="Fill missing runtime capability.",
        required_competencies=["worker heartbeat and liveness"],
        writable_domains=["repo:notion2api"],
        authority_ceiling="A2",
        recruitment_mode=RecruitmentMode.REQUISITION_ONLY.value,
        plan_id="recruiting-plan",
        mission_id="recruiting-mission",
        idempotency_key="recruiting-request",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)
    assert first.status == MaterializationStatus.RECRUITING.value
    assert first.recruitment_mode == RecruitmentMode.REQUISITION_ONLY.value
    assert len(first.recruited_worker_ids) == 1
    assert replay.recruited_worker_ids == first.recruited_worker_ids
    worker = store.workforce.list_workers(
        stage=WorkerStage.REQUISITIONED.value
    ).workers[0]
    assert worker.worker_id == first.recruited_worker_ids[0]
    assert worker.competencies == ["worker heartbeat and liveness"]
    assert worker.writable_domains == ["repo:notion2api"]


def test_governed_auto_appointment_closes_gap_and_materializes(tmp_path):
    store, result = _auto_materialize(tmp_path)
    assert result.status == MaterializationStatus.MATERIALIZED.value
    assert result.recruitment_mode == RecruitmentMode.AUTO_APPOINT.value
    assert len(result.recruited_worker_ids) == 1
    assert result.selected_worker_ids == result.recruited_worker_ids
    worker = store.workforce.list_workers(
        stage=WorkerStage.APPOINTED.value
    ).workers[0]
    assert worker.worker_id == result.recruited_worker_ids[0]
    assert worker.authority_ceiling == "A2"
    assert result.leases[0].status == "ACTIVE"
    assert result.leases[0].liveness_status == "UNKNOWN"
    assert result.leases[0].execution_live is False


def test_auto_appointment_requires_governed_authorization(tmp_path):
    store = _materialization(tmp_path)
    result = store.materialize_invocation(
        objective="Fill a protected gap.",
        required_competencies=["sqlite schema migration"],
        writable_domains=["repo:notion2api"],
        authority_ceiling="A2",
        recruitment_mode=RecruitmentMode.AUTO_APPOINT.value,
        plan_id="unauthorized-plan",
        mission_id="unauthorized-mission",
        idempotency_key="unauthorized-request",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)
    assert result.status == MaterializationStatus.BLOCKED.value
    assert result.recruited_worker_ids == []
    assert result.missing_competencies == ["sqlite schema migration"]


def test_lease_heartbeat_proves_liveness_and_renews_expiry(tmp_path):
    store, result = _auto_materialize(tmp_path, plan_id="heartbeat-plan")
    lease = result.leases[0]
    heartbeat = store.record_lease_heartbeat(
        lease_id=lease.lease_id,
        actor="worker-runtime",
        heartbeat_status="RUNNING",
        extend_seconds=172800,
        evidence={"execution_id": "exec-1", "progress": "started"},
        idempotency_key="heartbeat-1",
    )
    assert heartbeat.changed_count == 1
    updated = store.get_materialization(plan_id=result.plan_id).leases[0]
    assert updated.status == "ACTIVE"
    assert updated.heartbeat_status == "RUNNING"
    assert updated.liveness_status == "LIVE"
    assert updated.execution_live is True
    assert updated.expires_at > lease.expires_at
    assert updated.liveness_evidence["execution_id"] == "exec-1"


def test_acknowledged_dispatch_records_running_heartbeat(tmp_path):
    store, result = _auto_materialize(tmp_path, plan_id="ack-plan")
    receipt = result.dispatch_receipts[0]
    updated = store.record_dispatch_receipt(
        plan_id=result.plan_id,
        work_unit_id=receipt.work_unit_id,
        status=DispatchStatus.ACKNOWLEDGED.value,
        actor="worker-runtime",
        evidence={"execution_id": "exec-ack"},
        expected_revision=receipt.revision,
        idempotency_key="ack-receipt",
    )
    lease = updated.leases[0]
    assert lease.heartbeat_status == "RUNNING"
    assert lease.liveness_status == "LIVE"
    assert lease.execution_live is True
    assert lease.renewal_count == 1


def test_stale_lease_reconciliation_dry_run_then_expires(tmp_path):
    store, result = _auto_materialize(tmp_path, plan_id="stale-plan")
    lease = result.leases[0]
    with sqlite3.connect(store.path) as conn:
        conn.execute(
            """
            UPDATE hive_worker_leases
            SET expires_at = 1, last_heartbeat_at = 1,
                heartbeat_status = 'RUNNING'
            WHERE lease_id = ?
            """,
            (lease.lease_id,),
        )
    preview = store.reconcile_stale_leases(
        actor="lease-reconciler",
        plan_id=result.plan_id,
        dry_run=True,
        idempotency_key="stale-preview",
    )
    assert preview.stale_count == 1
    assert preview.changed_count == 0
    assert preview.items[0].action == "WOULD_EXPIRE"
    applied = store.reconcile_stale_leases(
        actor="lease-reconciler",
        plan_id=result.plan_id,
        dry_run=False,
        idempotency_key="stale-apply",
    )
    assert applied.changed_count == 1
    current = store.get_materialization(plan_id=result.plan_id).leases[0]
    assert current.status == "EXPIRED"
    assert current.liveness_status == "EXPIRED"
    assert current.execution_live is False


def test_workforce_audit_dry_run_and_apply_offboards_only_unprotected(tmp_path):
    store = _materialization(tmp_path)
    placeholder = store.workforce.register_worker(
        worker_id="placeholder-worker",
        display_name="DO NOT USE Placeholder",
        worker_class="temporary_worker",
        role="placeholder",
        accountable_owner="owner",
        actor="owner",
        idempotency_key="placeholder-register",
    ).workers[0]
    protected = store.workforce.register_worker(
        worker_id="placeholder-leader",
        display_name="DO NOT USE Leader",
        worker_class="hive_leader",
        role="placeholder",
        accountable_owner="owner",
        actor="owner",
        idempotency_key="placeholder-leader-register",
    ).workers[0]
    preview = store.audit_workforce(
        actor="workforce-auditor",
        dry_run=True,
        stale_after_days=1,
        idempotency_key="audit-preview",
    )
    assert {item.worker_id for item in preview.findings} == {
        placeholder.worker_id,
        protected.worker_id,
    }
    assert preview.action_count == 0
    applied = store.audit_workforce(
        actor="workforce-auditor",
        dry_run=False,
        stale_after_days=1,
        human_approval=True,
        idempotency_key="audit-apply",
    )
    assert applied.acted_worker_ids == [placeholder.worker_id]
    states = {
        item.worker_id: item.stage
        for item in store.workforce.list_workers(limit=10).workers
    }
    assert states[placeholder.worker_id] == WorkerStage.OFFBOARDED.value
    assert states[protected.worker_id] == WorkerStage.REQUISITIONED.value
    replay = store.audit_workforce(
        actor="workforce-auditor",
        dry_run=False,
        stale_after_days=1,
        human_approval=True,
        idempotency_key="audit-apply",
    )
    assert replay.audit_id == applied.audit_id
    assert replay.acted_worker_ids == applied.acted_worker_ids


def test_reconciliation_receipts_are_append_only_and_sanitized(tmp_path):
    store, result = _auto_materialize(tmp_path, plan_id="event-plan")
    lease = result.leases[0]
    store.record_lease_heartbeat(
        lease_id=lease.lease_id,
        actor="worker-runtime",
        evidence={"status": "bounded"},
        idempotency_key="event-heartbeat",
    )
    with sqlite3.connect(store.path) as conn:
        conn.row_factory = sqlite3.Row
        events = conn.execute(
            "SELECT event_type, payload_json FROM hive_lease_events ORDER BY created_at"
        ).fetchall()
    assert [row["event_type"] for row in events] == ["LEASE_HEARTBEAT"]
    payload = json.loads(events[0]["payload_json"])
    assert payload["evidence"] == {"status": "bounded"}
    assert "credential" not in json.dumps(payload).lower()
