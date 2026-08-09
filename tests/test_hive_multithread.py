from __future__ import annotations

import pytest

from app.hive_multithread import (
    BeeNotionCallEnvelope,
    CrossThreadEnvelope,
    CrossThreadMessageKind,
    HiveMissionBus,
    LaneDescriptor,
    MCPInvocationEnvelope,
    MultithreadContractError,
    ThreadBinding,
    ThreadKind,
    leader_conversation_id,
    plan_lane_dependencies,
    select_concurrent_lanes,
    topology_receipt,
    worker_conversation_id,
)
from app.hive_runtime import HiveRuntimeStore, HiveWorkUnitSpec


def worker_binding(*, worker_id: str = "worker-a", work_unit_id: str = "lane-a") -> ThreadBinding:
    mission_id = "mission-1"
    return ThreadBinding(
        mission_id=mission_id,
        work_unit_id=work_unit_id,
        worker_id=worker_id,
        thread_kind=ThreadKind.WORKER,
        conversation_id=worker_conversation_id("plan-1", worker_id),
        leader_conversation_id=leader_conversation_id(mission_id),
        profile_id="profile-1",
        notion_user_id="user-1",
        workspace_id="workspace-1",
        authority_ceiling="A2",
        writable_domains=[f"domain/{worker_id}"],
    )


def test_one_leader_many_independent_worker_threads():
    mission_id = "mission-1"
    leader_id = leader_conversation_id(mission_id)
    leader = ThreadBinding(
        mission_id=mission_id,
        thread_kind=ThreadKind.LEADER,
        conversation_id=leader_id,
        leader_conversation_id=leader_id,
        profile_id="profile-1",
        notion_user_id="user-1",
        workspace_id="workspace-1",
        authority_ceiling="A3",
    )
    worker_a = worker_binding(worker_id="worker-a", work_unit_id="lane-a")
    worker_b = worker_binding(worker_id="worker-b", work_unit_id="lane-b")

    assert leader.conversation_id == worker_a.leader_conversation_id
    assert worker_a.leader_conversation_id == worker_b.leader_conversation_id
    assert worker_a.conversation_id != worker_b.conversation_id
    assert worker_a.conversation_id != leader.conversation_id


def test_worker_cannot_reuse_leader_conversation():
    with pytest.raises(ValueError, match="must be independent"):
        ThreadBinding(
            mission_id="mission-1",
            work_unit_id="lane-a",
            worker_id="worker-a",
            thread_kind=ThreadKind.WORKER,
            conversation_id=leader_conversation_id("mission-1"),
            leader_conversation_id=leader_conversation_id("mission-1"),
            profile_id="profile-1",
            notion_user_id="user-1",
            workspace_id="workspace-1",
        )


def test_leader_conversation_can_coordinate_a_named_worker_lane():
    mission_id = "mission-1"
    leader_id = leader_conversation_id(mission_id)
    binding = ThreadBinding(
        mission_id=mission_id,
        thread_kind=ThreadKind.LEADER,
        conversation_id=leader_id,
        leader_conversation_id=leader_id,
        profile_id="profile-1",
        notion_user_id="user-1",
        workspace_id="workspace-1",
    )
    envelope = BeeNotionCallEnvelope(
        account_key="workspace-1:user-1",
        conversation_id=leader_id,
        mission_id=mission_id,
        work_unit_id="lane-a",
        worker_id="worker-a",
        idempotency_key="leader-request-1",
        profile_id="profile-1",
        workspace_id="workspace-1",
        user_id="user-1",
    )

    envelope.validate_lane(
        binding,
        mission_conversation_ids=[worker_conversation_id("plan-1", "worker-a")],
    )


def test_dependency_planner_maximizes_parallel_first_wave():
    plan = plan_lane_dependencies(
        ["build-a", "build-b", "build-c", "review"],
        reviewer_ids=["review"],
        dependency_count=3,
        parallelizable_workstreams=2,
    )

    assert plan["build-a"] == []
    assert plan["build-b"] == []
    assert plan["build-c"] == ["build-a"]
    assert plan["review"] == ["build-a", "build-b", "build-c"]


def test_concurrent_selector_respects_dependencies_capacity_and_write_conflicts():
    lanes = [
        LaneDescriptor("lane-a", "worker-a", ("app/a.py",), (), 10),
        LaneDescriptor("lane-b", "worker-b", ("app/b.py",), (), 20),
        LaneDescriptor("lane-conflict", "worker-c", ("app/a.py",), (), 15),
        LaneDescriptor("lane-review", "reviewer", ("evidence",), ("lane-a", "lane-b"), 1, True),
    ]

    selected = select_concurrent_lanes(lanes, max_parallel=3)

    assert [item.work_unit_id for item in selected] == ["lane-a", "lane-b"]
    review = select_concurrent_lanes(
        lanes,
        completed=["lane-a", "lane-b", "lane-conflict"],
        max_parallel=3,
    )
    assert [item.work_unit_id for item in review] == ["lane-review"]


def test_worker_mcp_envelope_allows_any_server_but_requires_exact_identity_and_no_secrets():
    binding = worker_binding()
    envelope = MCPInvocationEnvelope(
        mission_id=binding.mission_id,
        work_unit_id=binding.work_unit_id,
        worker_id=binding.worker_id,
        conversation_id=binding.conversation_id,
        notion_thread_id="notion-thread-1",
        profile_id=binding.profile_id,
        notion_user_id=binding.notion_user_id,
        workspace_id=binding.workspace_id,
        mcp_server_id="RepoAI",
        tool_name="review_repository",
        request_id="request-1",
        receipt_id="receipt-1",
        authority_ceiling="A2",
        payload={"repository": "X:/Code/example", "mode": "review"},
    )

    envelope.validate_binding(binding)
    assert envelope.mcp_server_id == "RepoAI"

    mismatched = envelope.model_copy(update={"conversation_id": "other-thread"})
    with pytest.raises(MultithreadContractError, match="does not match"):
        mismatched.validate_binding(binding)

    with pytest.raises(ValueError, match="credential-bearing key"):
        MCPInvocationEnvelope(
            **{
                **envelope.model_dump(),
                "request_id": "request-2",
                "receipt_id": "receipt-2",
                "payload": {"token": "do-not-pass"},
            }
        )


def test_typed_mission_bus_supports_leader_worker_and_worker_worker_crosstalk(tmp_path):
    store = HiveRuntimeStore(tmp_path / "hive.sqlite3")
    mission = store.create_mission(
        title="Concurrent mission",
        objective="Coordinate independent lanes",
        lifecycle_stage="Build",
        mission_id="mission-1",
        work_units=[
            HiveWorkUnitSpec(
                work_unit_id="lane-a",
                title="Lane A",
                role="worker-a",
                conversation_id="thread-a",
            ),
            HiveWorkUnitSpec(
                work_unit_id="lane-b",
                title="Lane B",
                role="worker-b",
                conversation_id="thread-b",
            ),
        ],
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)
    bus = HiveMissionBus(store)
    leader_id = leader_conversation_id("mission-1")
    first = bus.publish(
        CrossThreadEnvelope(
            mission_id="mission-1",
            work_unit_id="lane-a",
            message_kind=CrossThreadMessageKind.TASK_ASSIGNMENT,
            sender_thread_id=leader_id,
            recipient_thread_id="thread-a",
            context_version=mission.revision,
            payload={"task": "Inspect contract"},
        ),
        expected_mission_revision=mission.revision,
    )
    bus.publish(
        CrossThreadEnvelope(
            mission_id="mission-1",
            work_unit_id="lane-a",
            message_kind=CrossThreadMessageKind.QUESTION,
            sender_thread_id="thread-a",
            recipient_thread_id="thread-b",
            context_version=first.revision,
            payload={"question": "Does your schema use the same identity tuple?"},
        ),
        expected_mission_revision=first.revision,
    )

    inbox_a = bus.inbox("mission-1", "thread-a")
    inbox_b = bus.inbox("mission-1", "thread-b")

    assert [message.message_kind for message in inbox_a] == [CrossThreadMessageKind.TASK_ASSIGNMENT]
    assert [message.message_kind for message in inbox_b] == [CrossThreadMessageKind.QUESTION]


def test_topology_receipt_is_secret_free_and_declares_non_pooled_mcp_access():
    receipt = topology_receipt(
        mission_id="mission-1",
        plan_id="plan-1",
        workers=[("worker-a", "lane-a"), ("worker-b", "lane-b")],
        profile_id="profile-1",
        notion_user_id="user-1",
        workspace_id="workspace-1",
    )

    assert receipt["topology"] == "one_leader_many_independent_workers"
    assert receipt["all_mcp_servers_discoverable_per_worker"] is True
    assert receipt["shared_pooled_tool_list"] is False
    assert len({item["conversation_id"] for item in receipt["worker_bindings"]}) == 2
