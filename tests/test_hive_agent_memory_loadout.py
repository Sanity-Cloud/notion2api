from __future__ import annotations

import pytest

from app.hive_agent_memory_loadout import (
    AgentMemoryLoadoutContext,
    AgentMemoryLoadoutOutcomeUnknown,
    run_agent_memory_loadout,
)
from app.hive_dispatcher import ExecutionStatus, HiveExecutionDispatcherStore
from app.hive_materialization import HiveMaterializationStore
from app.hive_runtime import HiveTransitionError
from app.hive_workforce import HiveWorkforceStore


class FakeBroker:
    def __init__(self) -> None:
        self.calls = []

    def read_loadout(self, **kwargs):
        self.calls.append(kwargs)
        return {
            "result": {
                "status": "success",
                "data": {"content": "synthetic loadout nonce auto-1"},
                "canonical": False,
                "cross_principal_allowed": False,
            },
            "receipt_ids": {
                "lease_receipt_id": "lease-receipt",
                "admission_receipt_id": "admission-receipt",
                "execute_receipt_id": "execute-receipt",
            },
        }


class FakeSink:
    def __init__(self) -> None:
        self.calls = []

    def submit_loadout(self, **kwargs):
        self.calls.append(kwargs)
        assert "lease_id" not in kwargs["loadout"]
        assert kwargs["loadout"]["canonical"] is False
        return {
            "submitted": True,
            "conversation_id": kwargs["context"].conversation_id,
            "request_id": kwargs["idempotency_key"],
        }


def _context() -> AgentMemoryLoadoutContext:
    return AgentMemoryLoadoutContext(
        execution_id="exec-auto-1",
        plan_id="plan-auto-1",
        mission_id="mission-auto-1",
        work_unit_id="work-auto-1",
        worker_id="worker-auto-1",
        conversation_id="aigentbee:worker:worker-auto-1:abc",
        hive_worker_lease_id="raw-hive-worker-lease",
        dispatch_receipt_id="dispatch-auto-1",
        authority_ceiling="A1",
        profile_name="profile-test",
        workspace_id="ws-test",
        user_id="user-test",
        source_boundary="synthetic sources only",
        writable_domains=(),
    )


def test_loadout_runner_keeps_raw_hive_lease_in_control_plane_and_injects_sanitized_packet():
    broker = FakeBroker()
    sink = FakeSink()
    result, evidence = run_agent_memory_loadout(
        context=_context(),
        payload={"memory_operation": "memory.core.read", "context_label": "auto-test"},
        cancelled=lambda: False,
        broker=broker,
        worker_sink=sink,
    )
    assert result["status"] == "HANDOFF_SUBMITTED"
    assert result["canonical"] is False
    assert result["broker_receipts"]["execute_receipt_id"] == "execute-receipt"
    assert evidence["raw_broker_lease_exposed_to_worker"] is False
    assert evidence["worker_identity_source"] == "canonical_hive_materialization"
    assert broker.calls[0]["context"].hive_worker_lease_id == "raw-hive-worker-lease"
    assert sink.calls[0]["receipt_ids"] == result["broker_receipts"]


def test_core_loadout_requires_explicit_noncanonical_and_cross_principal_denial():
    class BadBroker(FakeBroker):
        def read_loadout(self, **kwargs):
            result = super().read_loadout(**kwargs)
            result["result"].pop("canonical")
            return result

    with pytest.raises(Exception, match="canonical=false"):
        run_agent_memory_loadout(
            context=_context(),
            payload={"memory_operation": "memory.core.read"},
            cancelled=lambda: False,
            broker=BadBroker(),
            worker_sink=FakeSink(),
        )


def _appoint(workforce: HiveWorkforceStore, worker_id: str):
    created = workforce.register_worker(
        worker_id=worker_id,
        display_name=worker_id,
        worker_class="TEMPORARY_WORKER",
        role="memory-loadout-worker",
        accountable_owner="human-owner",
        competencies=["agent_memory_loadout"],
        writable_domains=[],
        authority_ceiling="A1",
        source_boundary="synthetic Agent Memory only",
        appointment_scope="automatic loadout validation",
        actor="test-owner",
        idempotency_key=f"register-{worker_id}",
    ).workers[0]
    shadow = workforce.transition_worker(
        worker_id=worker_id,
        target_stage="SHADOW",
        actor="test-owner",
        reason="shadow",
        expected_revision=created.revision,
        idempotency_key=f"shadow-{worker_id}",
    ).workers[0]
    probation = workforce.transition_worker(
        worker_id=worker_id,
        target_stage="PROBATION",
        actor="test-owner",
        reason="probation",
        human_approval=True,
        expected_revision=shadow.revision,
        idempotency_key=f"probation-{worker_id}",
    ).workers[0]
    return workforce.transition_worker(
        worker_id=worker_id,
        target_stage="APPOINTED",
        actor="test-owner",
        reason="appointed",
        human_approval=True,
        expected_revision=probation.revision,
        idempotency_key=f"appointed-{worker_id}",
    ).workers[0]


def _materialized(tmp_path, *, runner):
    path = tmp_path / "hive.sqlite3"
    materialization = HiveMaterializationStore(path)
    workforce = HiveWorkforceStore(path)
    worker = _appoint(workforce, "memory-worker")
    plan = materialization.materialize_invocation(
        objective="Automatic bounded Agent Memory loadout.",
        required_competencies=["agent_memory_loadout"],
        writable_domains=[],
        authority_ceiling="A1",
        preferred_worker_ids=[worker.worker_id],
        human_approval=True,
        actor="human-owner",
        plan_id="auto-loadout-plan",
        mission_id="auto-loadout-mission",
        idempotency_key="auto-loadout-materialize",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
    )
    dispatcher = HiveExecutionDispatcherStore(path, agent_memory_loadout_runner=runner)
    current = dispatcher.list_adapters(
        adapter_id="builtin.agent_memory_loadout.v1"
    ).adapters[0]
    dispatcher.upsert_adapter(
        adapter_id=current.adapter_id,
        implementation_id=current.implementation_id,
        display_name=current.display_name,
        capabilities=current.capabilities,
        writable_domains=current.writable_domains,
        required_authority=current.required_authority,
        max_timeout_ms=current.max_timeout_ms,
        max_payload_bytes=current.max_payload_bytes,
        requires_human_approval=current.requires_human_approval,
        requires_independent_review=current.requires_independent_review,
        enabled=True,
        actor="human-owner",
        human_approval=True,
        expected_revision=current.revision,
        idempotency_key="enable-auto-loadout",
    )
    return dispatcher, plan, worker


def test_dispatcher_derives_exact_materialized_worker_context_and_requires_review(tmp_path):
    captured = {}

    def runner(*, context, payload, cancelled):
        captured["context"] = context
        return (
            {
                "status": "HANDOFF_SUBMITTED",
                "canonical": False,
                "broker_receipts": {"execute_receipt_id": "exec-receipt"},
            },
            {
                "performed_external_effect": True,
                "raw_broker_lease_exposed_to_worker": False,
            },
        )

    dispatcher, plan, worker = _materialized(tmp_path, runner=runner)
    snapshot = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.agent_memory_loadout.v1",
        requested_capability="agent_memory_loadout",
        payload={"memory_operation": "memory.core.read"},
        requested_writable_domains=[],
        timeout_ms=30000,
        actor=worker.worker_id,
        idempotency_key="auto-loadout-execution",
    )
    execution = snapshot.executions[0]
    assert execution.status == ExecutionStatus.REVIEW_REQUIRED.value
    assert captured["context"].mission_id == plan.mission_id
    assert captured["context"].work_unit_id == plan.work_unit_ids[0]
    assert captured["context"].worker_id == worker.worker_id
    assert captured["context"].conversation_id.startswith("aigentbee:worker:")
    assert execution.result["canonical"] is False
    assert execution.result["loadout_evidence"]["raw_broker_lease_exposed_to_worker"] is False


def test_outcome_unknown_blocks_replay_until_explicit_reconciliation(tmp_path):
    def runner(*, context, payload, cancelled):
        raise AgentMemoryLoadoutOutcomeUnknown(
            "worker submission uncertain",
            stage="worker_context_submit",
            evidence={"execute_receipt_id": "broker-exec-1"},
        )

    dispatcher, plan, worker = _materialized(tmp_path, runner=runner)
    first = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.agent_memory_loadout.v1",
        requested_capability="agent_memory_loadout",
        payload={"memory_operation": "memory.core.read"},
        requested_writable_domains=[],
        timeout_ms=30000,
        actor=worker.worker_id,
        idempotency_key="unknown-loadout-execution",
    )
    execution = first.executions[0]
    assert execution.status == ExecutionStatus.OUTCOME_UNKNOWN.value
    assert execution.evidence["reconciliation_required"] is True

    blocked = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.agent_memory_loadout.v1",
        requested_capability="agent_memory_loadout",
        payload={"memory_operation": "memory.core.read"},
        requested_writable_domains=[],
        timeout_ms=30000,
        actor=worker.worker_id,
        idempotency_key="unknown-loadout-second-semantic-attempt",
    )
    assert blocked.executions[0].status == ExecutionStatus.DENIED.value

    reconciled = dispatcher.reconcile_outcome_unknown(
        execution_id=execution.execution_id,
        actor="independent-reconciler",
        resolved_status="FAILED",
        evidence={"worker_conversation_checked": True, "matching_turn_found": False},
        idempotency_key="reconcile-unknown-loadout",
    )
    final = reconciled.executions[0]
    assert final.status == ExecutionStatus.FAILED.value
    assert final.evidence["semantic_replay_performed"] is False


def test_cancel_rejects_outcome_unknown_and_requires_reconciliation(tmp_path):
    def runner(*, context, payload, cancelled):
        raise AgentMemoryLoadoutOutcomeUnknown(
            "worker submission uncertain",
            stage="worker_context_submit",
        )

    dispatcher, plan, worker = _materialized(tmp_path, runner=runner)
    execution = dispatcher.execute_dispatch(
        plan_id=plan.plan_id,
        work_unit_id=plan.work_unit_ids[0],
        adapter_id="builtin.agent_memory_loadout.v1",
        requested_capability="agent_memory_loadout",
        payload={"memory_operation": "memory.core.read"},
        requested_writable_domains=[],
        timeout_ms=30000,
        actor=worker.worker_id,
        idempotency_key="unknown-cancel-execution",
    ).executions[0]
    with pytest.raises(HiveTransitionError, match="must be reconciled"):
        dispatcher.cancel_execution(
            execution_id=execution.execution_id,
            actor="operator",
            reason="do not guess remote outcome",
            idempotency_key="cancel-unknown",
        )
