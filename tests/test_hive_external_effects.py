from __future__ import annotations

import sqlite3

import pytest

from app.hive_dispatcher import (
    AdapterStatus,
    ExecutionStatus,
    HiveExecutionDispatcherStore,
)
from app.hive_external_effects import (
    CertificationStatus,
    EffectStatus,
    ExternalEffectCertificationStore,
)
from app.hive_materialization import HiveMaterializationStore
from app.hive_runtime import HiveIdempotencyConflict, HiveTransitionError
from app.hive_workforce import HiveWorkforceStore


def _stores(tmp_path, monkeypatch):
    db = tmp_path / "hive.sqlite3"
    root = tmp_path / "effects"
    monkeypatch.setenv("SANITYCLOUD_EXTERNAL_EFFECT_ROOT", str(root))
    return (
        HiveExecutionDispatcherStore(db),
        ExternalEffectCertificationStore(db, root),
        HiveMaterializationStore(db),
        HiveWorkforceStore(db),
        db,
        root,
    )


def _appoint(
    workforce: HiveWorkforceStore,
    worker_id: str,
    *,
    worker_class: str = "TEMPORARY_WORKER",
    role: str = "builder",
    competencies: list[str] | None = None,
    domains: list[str] | None = None,
):
    created = workforce.register_worker(
        worker_id=worker_id,
        display_name=worker_id,
        worker_class=worker_class,
        role=role,
        accountable_owner="human-owner",
        competencies=competencies or [],
        writable_domains=domains or [],
        authority_ceiling="A2",
        source_boundary="phase-4 tests",
        appointment_scope="phase-4 validation",
        actor="owner",
        idempotency_key=f"register-{worker_id}",
    )
    shadow = workforce.transition_worker(
        worker_id=worker_id,
        target_stage="SHADOW",
        actor="owner",
        reason="shadow",
        expected_revision=created.workers[0].revision,
        idempotency_key=f"shadow-{worker_id}",
    )
    probation = workforce.transition_worker(
        worker_id=worker_id,
        target_stage="PROBATION",
        actor="owner",
        reason="probation",
        human_approval=True,
        expected_revision=shadow.workers[0].revision,
        idempotency_key=f"probation-{worker_id}",
    )
    return workforce.transition_worker(
        worker_id=worker_id,
        target_stage="APPOINTED",
        actor="owner",
        reason="appointment",
        human_approval=True,
        expected_revision=probation.workers[0].revision,
        idempotency_key=f"appoint-{worker_id}",
    ).workers[0]


def _certify(effects, reviewer_id: str, *, key: str = "certify"):
    return effects.certify_adapter(
        adapter_id="builtin.sandbox_artifact.v1",
        implementation_id="builtin.sandbox_artifact.v1",
        sandbox_name="pilot",
        allowed_extensions=[".json", ".txt"],
        max_effect_bytes=4096,
        threat_model={
            "attack_surface": "single-file sandbox",
            "abuse_cases": ["traversal", "tampering"],
            "mitigations": ["allowlist", "preimage rollback"],
            "residual_risk": "low",
        },
        credential_boundary="none",
        rollback_contract={"strategy": "preimage_restore", "retention_seconds": 3600},
        reviewer_worker_id=reviewer_id,
        actor="accountable-human",
        human_approval=True,
        idempotency_key=key,
    ).certifications[0]


def _enable(dispatcher):
    adapter = dispatcher.list_adapters(
        adapter_id="builtin.sandbox_artifact.v1"
    ).adapters[0]
    return dispatcher.upsert_adapter(
        adapter_id=adapter.adapter_id,
        implementation_id=adapter.implementation_id,
        display_name=adapter.display_name,
        capabilities=adapter.capabilities,
        writable_domains=adapter.writable_domains,
        required_authority=adapter.required_authority,
        max_timeout_ms=adapter.max_timeout_ms,
        max_payload_bytes=adapter.max_payload_bytes,
        requires_human_approval=adapter.requires_human_approval,
        requires_independent_review=adapter.requires_independent_review,
        enabled=True,
        actor="accountable-human",
        human_approval=True,
        expected_revision=adapter.revision,
        idempotency_key="enable-external-adapter",
    ).adapters[0]


def _materialize(materialization, worker_id: str, plan_id: str):
    return materialization.materialize_invocation(
        objective=f"Phase 4 plan {plan_id}",
        required_competencies=["external_effect"],
        writable_domains=["external_sandbox"],
        authority_ceiling="A2",
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


def _execute(dispatcher, plan, payload, worker_id: str, key: str):
    return dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.sandbox_artifact.v1",
        requested_capability="sandbox_artifact",
        payload=payload,
        requested_writable_domains=["external_sandbox"],
        timeout_ms=1000,
        actor=worker_id,
        human_approval=True,
        idempotency_key=key,
    ).executions[0]


def test_schema_and_builtin_adapter_are_additive(tmp_path, monkeypatch):
    dispatcher, effects, _materialization, _workforce, db, _root = _stores(
        tmp_path, monkeypatch
    )
    with sqlite3.connect(db) as conn:
        tables = {
            row[0]
            for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        assert conn.execute("PRAGMA user_version").fetchone()[0] == 3
    assert {
        "hive_external_adapter_certifications",
        "hive_external_certification_events",
        "hive_external_effect_receipts",
        "hive_external_effect_events",
    }.issubset(tables)
    adapter = dispatcher.list_adapters(
        adapter_id="builtin.sandbox_artifact.v1"
    ).adapters[0]
    assert adapter.status == AdapterStatus.DISABLED.value
    assert effects.get_certifications().count == 0


def test_certification_requires_independent_reviewer_and_no_credentials(
    tmp_path, monkeypatch
):
    _dispatcher, effects, _materialization, workforce, _db, _root = _stores(
        tmp_path, monkeypatch
    )
    builder = _appoint(
        workforce,
        "builder",
        competencies=["external_effect"],
        domains=["external_sandbox"],
    )
    reviewer = _appoint(
        workforce,
        "reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
    )
    with pytest.raises(HiveTransitionError, match="Governance-plan authorization"):
        effects.certify_adapter(
            adapter_id="builtin.sandbox_artifact.v1",
            implementation_id="builtin.sandbox_artifact.v1",
            sandbox_name="pilot",
            allowed_extensions=[".json"],
            max_effect_bytes=100,
            threat_model={
                "attack_surface": "sandbox",
                "abuse_cases": [],
                "mitigations": [],
                "residual_risk": "low",
            },
            credential_boundary="none",
            rollback_contract={"strategy": "preimage_restore", "retention_seconds": 60},
            reviewer_worker_id=reviewer.worker_id,
            actor="human",
        )
    with pytest.raises(HiveTransitionError, match="GOVERNANCE_REVIEWER"):
        effects.certify_adapter(
            adapter_id="builtin.sandbox_artifact.v1",
            implementation_id="builtin.sandbox_artifact.v1",
            sandbox_name="pilot",
            allowed_extensions=[".json"],
            max_effect_bytes=100,
            threat_model={
                "attack_surface": "sandbox",
                "abuse_cases": [],
                "mitigations": [],
                "residual_risk": "low",
            },
            credential_boundary="none",
            rollback_contract={"strategy": "preimage_restore", "retention_seconds": 60},
            reviewer_worker_id=builder.worker_id,
            actor="human",
            human_approval=True,
        )
    with pytest.raises(HiveTransitionError, match="cannot receive or use credentials"):
        effects.certify_adapter(
            adapter_id="builtin.sandbox_artifact.v1",
            implementation_id="builtin.sandbox_artifact.v1",
            sandbox_name="pilot",
            allowed_extensions=[".json"],
            max_effect_bytes=100,
            threat_model={
                "attack_surface": "sandbox",
                "abuse_cases": [],
                "mitigations": [],
                "residual_risk": "low",
            },
            credential_boundary="environment",
            rollback_contract={"strategy": "preimage_restore", "retention_seconds": 60},
            reviewer_worker_id=reviewer.worker_id,
            actor="human",
            human_approval=True,
        )
    certified = _certify(effects, reviewer.worker_id)
    replay = _certify(effects, reviewer.worker_id)
    assert certified.model_dump() == replay.model_dump()
    with pytest.raises(HiveIdempotencyConflict):
        effects.certify_adapter(
            adapter_id="builtin.sandbox_artifact.v1",
            implementation_id="builtin.sandbox_artifact.v1",
            sandbox_name="different",
            allowed_extensions=[".json"],
            max_effect_bytes=100,
            threat_model={
                "attack_surface": "sandbox",
                "abuse_cases": [],
                "mitigations": [],
                "residual_risk": "low",
            },
            credential_boundary="none",
            rollback_contract={"strategy": "preimage_restore", "retention_seconds": 60},
            reviewer_worker_id=reviewer.worker_id,
            actor="human",
            human_approval=True,
            idempotency_key="certify",
        )


def test_reviewed_dry_run_apply_and_rollback(tmp_path, monkeypatch):
    dispatcher, effects, materialization, workforce, _db, root = _stores(
        tmp_path, monkeypatch
    )
    builder = _appoint(
        workforce,
        "builder",
        competencies=["external_effect"],
        domains=["external_sandbox"],
    )
    reviewer = _appoint(
        workforce,
        "reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
    )
    cert = _certify(effects, reviewer.worker_id)
    _enable(dispatcher)
    dry_plan = _materialize(materialization, builder.worker_id, "dry")
    dry = _execute(
        dispatcher,
        dry_plan,
        {
            "certification_id": cert.certification_id,
            "operation": "write",
            "relative_name": "receipt.json",
            "content": '{"phase":4}',
            "dry_run": True,
        },
        builder.worker_id,
        "execute-dry",
    )
    assert dry.status == ExecutionStatus.REVIEW_REQUIRED.value
    assert dry.result["performed_external_effect"] is False
    assert not (root / "pilot" / "receipt.json").exists()
    dispatcher.review_execution(
        execution_id=dry.execution_id,
        reviewer_worker_id=reviewer.worker_id,
        approved=True,
        actor="human",
        findings={"dry_run": "approved"},
        human_approval=True,
        idempotency_key="review-dry",
    )
    live_plan = _materialize(materialization, builder.worker_id, "live")
    live = _execute(
        dispatcher,
        live_plan,
        {
            "certification_id": cert.certification_id,
            "operation": "write",
            "relative_name": "receipt.json",
            "content": '{"phase":4}',
            "dry_run": False,
            "dry_run_effect_id": dry.result["effect_id"],
        },
        builder.worker_id,
        "execute-live",
    )
    assert live.status == ExecutionStatus.REVIEW_REQUIRED.value
    assert live.result["performed_external_effect"] is True
    target = root / "pilot" / "receipt.json"
    assert target.read_text(encoding="utf-8") == '{"phase":4}'
    completed = dispatcher.review_execution(
        execution_id=live.execution_id,
        reviewer_worker_id=reviewer.worker_id,
        approved=True,
        actor="human",
        findings={"effect": "approved"},
        human_approval=True,
        idempotency_key="review-live",
    ).executions[0]
    assert completed.status == ExecutionStatus.COMPLETED.value
    rolled = effects.rollback_effect(
        effect_id=live.result["effect_id"],
        rollback_token=live.result["rollback_token"],
        reviewer_worker_id=reviewer.worker_id,
        actor="human",
        reason="validation rollback",
        human_approval=True,
        idempotency_key="rollback-live",
    ).effects[0]
    assert rolled.status == EffectStatus.ROLLED_BACK.value
    assert not target.exists()


def test_path_extension_and_dry_run_mismatch_fail_closed(tmp_path, monkeypatch):
    dispatcher, effects, materialization, workforce, _db, root = _stores(
        tmp_path, monkeypatch
    )
    builder = _appoint(
        workforce,
        "builder",
        competencies=["external_effect"],
        domains=["external_sandbox"],
    )
    reviewer = _appoint(
        workforce,
        "reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
    )
    cert = _certify(effects, reviewer.worker_id)
    _enable(dispatcher)
    with pytest.raises(HiveTransitionError, match="one sandbox filename"):
        effects.execute_sandbox_artifact(
            execution_id="direct-traversal",
            payload={
                "certification_id": cert.certification_id,
                "operation": "write",
                "relative_name": "../escape.json",
                "content": "x",
                "dry_run": True,
            },
            actor=builder.worker_id,
            cancelled=lambda: False,
        )
    with pytest.raises(HiveTransitionError, match="not certified"):
        effects.execute_sandbox_artifact(
            execution_id="direct-extension",
            payload={
                "certification_id": cert.certification_id,
                "operation": "write",
                "relative_name": "escape.exe",
                "content": "x",
                "dry_run": True,
            },
            actor=builder.worker_id,
            cancelled=lambda: False,
        )
    dry_plan = _materialize(materialization, builder.worker_id, "mismatch-dry")
    dry = _execute(
        dispatcher,
        dry_plan,
        {
            "certification_id": cert.certification_id,
            "operation": "write",
            "relative_name": "one.json",
            "content": "one",
            "dry_run": True,
        },
        builder.worker_id,
        "mismatch-dry-exec",
    )
    dispatcher.review_execution(
        execution_id=dry.execution_id,
        reviewer_worker_id=reviewer.worker_id,
        approved=True,
        actor="human",
        findings={"dry_run": "approved"},
        human_approval=True,
        idempotency_key="mismatch-dry-review",
    )
    live_plan = _materialize(materialization, builder.worker_id, "mismatch-live")
    failed = _execute(
        dispatcher,
        live_plan,
        {
            "certification_id": cert.certification_id,
            "operation": "write",
            "relative_name": "two.json",
            "content": "two",
            "dry_run": False,
            "dry_run_effect_id": dry.result["effect_id"],
        },
        builder.worker_id,
        "mismatch-live-exec",
    )
    assert failed.status == ExecutionStatus.FAILED.value
    assert "does not match" in failed.error_message
    assert not (root / "pilot" / "two.json").exists()


def test_suspended_and_tampered_certifications_fail_closed(tmp_path, monkeypatch):
    _dispatcher, effects, _materialization, workforce, db, _root = _stores(
        tmp_path, monkeypatch
    )
    reviewer = _appoint(
        workforce,
        "reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
    )
    cert = _certify(effects, reviewer.worker_id)
    suspended = effects.transition_certification(
        certification_id=cert.certification_id,
        target_status="SUSPENDED",
        actor="human",
        reason="security hold",
        human_approval=True,
        expected_revision=cert.revision,
        idempotency_key="suspend-cert",
    ).certifications[0]
    assert suspended.status == CertificationStatus.SUSPENDED.value
    with pytest.raises(HiveTransitionError, match="not active"):
        effects.execute_sandbox_artifact(
            execution_id="suspended",
            payload={
                "certification_id": cert.certification_id,
                "operation": "write",
                "relative_name": "one.json",
                "content": "one",
                "dry_run": True,
            },
            actor="worker",
            cancelled=lambda: False,
        )
    recertified = effects.transition_certification(
        certification_id=cert.certification_id,
        target_status="CERTIFIED",
        actor="human",
        reason="hold cleared",
        human_approval=True,
        expected_revision=suspended.revision,
        idempotency_key="recertify-cert",
    ).certifications[0]
    assert recertified.status == CertificationStatus.CERTIFIED.value
    with sqlite3.connect(db) as conn:
        conn.execute(
            "UPDATE hive_external_adapter_certifications SET max_effect_bytes = max_effect_bytes + 1 WHERE certification_id = ?",
            (cert.certification_id,),
        )
        conn.commit()
    with pytest.raises(HiveTransitionError, match="tamper verification"):
        effects.execute_sandbox_artifact(
            execution_id="tampered-cert",
            payload={
                "certification_id": cert.certification_id,
                "operation": "write",
                "relative_name": "one.json",
                "content": "one",
                "dry_run": True,
            },
            actor="worker",
            cancelled=lambda: False,
        )


def test_rollback_refuses_post_effect_tampering(tmp_path, monkeypatch):
    dispatcher, effects, materialization, workforce, _db, root = _stores(
        tmp_path, monkeypatch
    )
    builder = _appoint(
        workforce,
        "builder",
        competencies=["external_effect"],
        domains=["external_sandbox"],
    )
    reviewer = _appoint(
        workforce,
        "reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
    )
    cert = _certify(effects, reviewer.worker_id)
    _enable(dispatcher)
    dry_plan = _materialize(materialization, builder.worker_id, "tamper-dry")
    dry = _execute(
        dispatcher,
        dry_plan,
        {
            "certification_id": cert.certification_id,
            "operation": "write",
            "relative_name": "tamper.json",
            "content": "certified",
            "dry_run": True,
        },
        builder.worker_id,
        "tamper-dry-exec",
    )
    dispatcher.review_execution(
        execution_id=dry.execution_id,
        reviewer_worker_id=reviewer.worker_id,
        approved=True,
        actor="human",
        findings={"dry_run": "approved"},
        human_approval=True,
        idempotency_key="tamper-dry-review",
    )
    live_plan = _materialize(materialization, builder.worker_id, "tamper-live")
    live = _execute(
        dispatcher,
        live_plan,
        {
            "certification_id": cert.certification_id,
            "operation": "write",
            "relative_name": "tamper.json",
            "content": "certified",
            "dry_run": False,
            "dry_run_effect_id": dry.result["effect_id"],
        },
        builder.worker_id,
        "tamper-live-exec",
    )
    target = root / "pilot" / "tamper.json"
    target.write_text("changed-after-effect", encoding="utf-8")
    with pytest.raises(HiveTransitionError, match="target changed"):
        effects.rollback_effect(
            effect_id=live.result["effect_id"],
            rollback_token=live.result["rollback_token"],
            reviewer_worker_id=reviewer.worker_id,
            actor="human",
            reason="rollback after tamper",
            human_approval=True,
            idempotency_key="tampered-rollback",
        )
    receipt = effects.get_effects(effect_id=live.result["effect_id"]).effects[0]
    assert receipt.status == EffectStatus.TAMPERED.value
    assert target.read_text(encoding="utf-8") == "changed-after-effect"


def test_failed_atomic_replace_restores_preimage(tmp_path, monkeypatch):
    _dispatcher, effects, _materialization, workforce, _db, root = _stores(
        tmp_path, monkeypatch
    )
    reviewer = _appoint(
        workforce,
        "reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
    )
    cert = _certify(effects, reviewer.worker_id)
    target = root / "pilot" / "restore.json"
    target.write_text("original", encoding="utf-8")
    dry = effects.execute_sandbox_artifact(
        execution_id="atomic-dry",
        payload={
            "certification_id": cert.certification_id,
            "operation": "write",
            "relative_name": "restore.json",
            "content": "replacement",
            "dry_run": True,
            "expected_preimage_sha256": effects._sha256(b"original"),
        },
        actor="worker",
        cancelled=lambda: False,
    )
    from app import hive_external_effects as external_module

    def fail_replace(_source, _target):
        raise OSError("forced replacement failure")

    monkeypatch.setattr(external_module.os, "replace", fail_replace)
    with pytest.raises(OSError, match="forced replacement failure"):
        effects.execute_sandbox_artifact(
            execution_id="atomic-live",
            payload={
                "certification_id": cert.certification_id,
                "operation": "write",
                "relative_name": "restore.json",
                "content": "replacement",
                "dry_run": False,
                "dry_run_effect_id": dry["effect_id"],
                "expected_preimage_sha256": effects._sha256(b"original"),
            },
            actor="worker",
            cancelled=lambda: False,
        )
    assert target.read_text(encoding="utf-8") == "original"
    assert list(target.parent.glob(".phase4-*.tmp")) == []


def test_revoked_certification_is_terminal(tmp_path, monkeypatch):
    _dispatcher, effects, _materialization, workforce, _db, _root = _stores(
        tmp_path, monkeypatch
    )
    reviewer = _appoint(
        workforce,
        "reviewer",
        worker_class="GOVERNANCE_REVIEWER",
        role="reviewer",
        competencies=["governance"],
    )
    cert = _certify(effects, reviewer.worker_id)
    revoked = effects.transition_certification(
        certification_id=cert.certification_id,
        target_status="REVOKED",
        actor="human",
        reason="end of pilot",
        human_approval=True,
        expected_revision=cert.revision,
        idempotency_key="revoke-cert",
    ).certifications[0]
    assert revoked.status == CertificationStatus.REVOKED.value
    with pytest.raises(HiveTransitionError, match="terminal"):
        effects.transition_certification(
            certification_id=cert.certification_id,
            target_status="CERTIFIED",
            actor="human",
            reason="invalid recovery",
            human_approval=True,
            expected_revision=revoked.revision,
            idempotency_key="revoked-recertification",
        )
