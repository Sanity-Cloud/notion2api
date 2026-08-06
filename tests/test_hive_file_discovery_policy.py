from __future__ import annotations

from app.hive_materialization import HiveMaterializationStore, MaterializationStatus
from app.hive_workforce import HiveWorkforceStore


def _appoint_worker(store: HiveWorkforceStore, worker_id: str = "file-worker"):
    created = store.register_worker(
        worker_id=worker_id,
        display_name="File routing worker",
        worker_class="TEMPORARY_WORKER",
        role="Implementation Engineer",
        accountable_owner="governed-manager",
        competencies=["python"],
        writable_domains=["app"],
        authority_ceiling="A3",
        source_boundary="Everything-resolved project files",
        appointment_scope="bounded file routing validation",
        actor="test-manager",
    )
    shadow = store.transition_worker(
        worker_id=worker_id,
        target_stage="SHADOW",
        actor="test-manager",
        reason="Begin bounded shadow review.",
        expected_revision=created.workers[0].revision,
    )
    probation = store.transition_worker(
        worker_id=worker_id,
        target_stage="PROBATION",
        actor="test-manager",
        reason="Governed probation.",
        human_approval=True,
        expected_revision=shadow.workers[0].revision,
    )
    return store.transition_worker(
        worker_id=worker_id,
        target_stage="APPOINTED",
        actor="test-manager",
        reason="Governed appointment.",
        human_approval=True,
        expected_revision=probation.workers[0].revision,
    ).workers[0]


def test_invocation_plan_embeds_canonical_file_discovery_policy(tmp_path):
    workforce = HiveWorkforceStore(tmp_path / "hive.sqlite3")
    worker = _appoint_worker(workforce)
    plan = workforce.plan_invocation(
        objective="Locate and patch one Python module.",
        required_competencies=["python"],
        writable_domains=["app"],
        preferred_worker_ids=[worker.worker_id],
        file_operation_intent="discover",
        file_search_text="routing",
        file_search_roots=["code"],
        file_types=["py"],
    )
    policy = plan.file_discovery_policy
    assert policy is not None
    assert policy.allowed is True
    assert policy.primary_tool == "Everything_MCP.search_files"
    assert policy.denied_tools == ["DesktopCommander.start_search"]
    assert policy.search_batches[0].query.startswith("routing ext:py")
    assert "!path:.git" in policy.search_batches[0].query
    assert policy.result_verification_required is True


def test_materialization_dispatch_receipt_carries_routing_evidence(tmp_path):
    path = tmp_path / "hive.sqlite3"
    workforce = HiveWorkforceStore(path)
    worker = _appoint_worker(workforce)
    materialization = HiveMaterializationStore(path)
    result = materialization.materialize_invocation(
        objective="Resolve and modify a known Python project file.",
        required_competencies=["python"],
        writable_domains=["app"],
        preferred_worker_ids=[worker.worker_id],
        file_operation_intent="discover",
        file_search_text="file_discovery_routing",
        file_search_roots=["code"],
        file_types=["py"],
        plan_id="file-routing-plan",
        mission_id="file-routing-mission",
        idempotency_key="file-routing-materialization",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)
    assert result.status == MaterializationStatus.MATERIALIZED.value
    assert len(result.dispatch_receipts) == 1
    evidence = result.dispatch_receipts[0].evidence
    assert evidence["routing_policy_version"] == "1.0"
    assert evidence["shared_pooled_tool_list"] is False
    policy = evidence["file_discovery_policy"]
    assert policy["primary_tool"] == "Everything_MCP.search_files"
    assert policy["search_batches"][0]["root_id"] == "code"


def test_materialization_blocks_when_everything_is_unavailable_without_gate(tmp_path):
    path = tmp_path / "hive.sqlite3"
    workforce = HiveWorkforceStore(path)
    worker = _appoint_worker(workforce)
    materialization = HiveMaterializationStore(path)
    result = materialization.materialize_invocation(
        objective="Search for a project file.",
        required_competencies=["python"],
        writable_domains=["app"],
        preferred_worker_ids=[worker.worker_id],
        file_operation_intent="discover",
        everything_available=False,
        degraded_search_authorized=False,
        authority_ceiling="A2",
        plan_id="blocked-file-routing-plan",
        mission_id="blocked-file-routing-mission",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)
    assert result.status == MaterializationStatus.BLOCKED.value
    assert result.dispatch_receipts == []
