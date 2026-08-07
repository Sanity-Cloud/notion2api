from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.hive_runtime import (
    HiveDelegatedTaskSpec,
    HiveHandoffReceipt,
    HiveIdempotencyConflict,
    HiveProjectContract,
    HiveRuntimeStore,
    HiveSchemaVersionError,
    HiveTransitionError,
    HiveWorkUnitSpec,
)


def _store(tmp_path):
    return HiveRuntimeStore(tmp_path / "hive.sqlite3")


def _create(store: HiveRuntimeStore, mission_id: str = "m1"):
    return store.create_mission(
        title="Build the Hive",
        objective="Create a durable parallel runtime",
        lifecycle_stage="Build",
        mission_id=mission_id,
        idempotency_key=f"create-{mission_id}",
        work_units=[
            HiveWorkUnitSpec(
                work_unit_id=f"{mission_id}-scout",
                title="Scout repository",
                role="cartographer",
                conversation_id="conv-scout",
                writable_domain="read-only",
            ),
            HiveWorkUnitSpec(
                work_unit_id=f"{mission_id}-builder",
                title="Build runtime",
                role="builder",
                conversation_id="conv-builder",
                writable_domain="app/hive_runtime.py",
                dependencies=[f"{mission_id}-scout"],
            ),
        ],
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)


def _task(
    task_id: str,
    *,
    lane_id: str = "m1-builder",
    dependencies: list[str] | None = None,
    writable_domains: list[str] | None = None,
):
    return HiveDelegatedTaskSpec(
        task_id=task_id,
        parent_lane_id=lane_id,
        objective=f"Complete {task_id}",
        scope=f"Bounded scope for {task_id}",
        exclusions=["External publication"],
        required_context=["Mission contract"],
        source_boundary=["Approved project sources"],
        writable_domains=writable_domains or ["app/hive_runtime.py"],
        authority_ceiling="A2",
        dependencies=dependencies or [],
        acceptance_criteria=["Evidence is attached"],
        deliverables=[f"{task_id} result"],
        evidence_requirements=["Test receipt"],
        checkpoint="Lane captain review",
        fan_in_owner="lane-captain",
        closure_condition="Handoff accepted",
    )


def test_mission_round_trip_includes_actions_and_bindings(tmp_path):
    store = _store(tmp_path)
    created = _create(store)

    assert created.status == "ACTIVE"
    assert created.authority_ceiling == "A2"
    assert {item.authority_ceiling for item in created.work_units} == {"A2"}
    assert created.revision == 1
    assert created.work_unit_count == 2
    assert created.event_count == 1
    assert created.action_count == 1
    assert created.events[0].event_type == "MISSION_OPENED"
    assert created.actions[0].action_type == "MISSION_CREATED"
    bindings = {item.work_unit_id: item.conversation_id for item in created.work_units}
    assert bindings["m1-scout"] == "conv-scout"

    loaded = store.get_mission("m1")
    assert loaded.model_dump() == created.model_dump()


def test_governed_project_contract_produces_graph_receipt(tmp_path):
    store = _store(tmp_path)
    created = store.create_mission(
        title="Create a SanityCloud campaign system",
        objective="Coordinate coding, business, and creative delivery",
        lifecycle_stage="Plan",
        parent_context_id="sanitycloud-governance",
        mission_id="hybrid-project",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        authority_ceiling="A2",
        project_contract=HiveProjectContract(
            project_kind="hybrid",
            scope="Plan, build, create, review, and close one governed project.",
            exclusions=["Production publication without a decision receipt"],
            accountable_human="SanityCloud Founder",
            source_boundary=["Approved project sources", "Repository evidence"],
            risks=[{"risk": "cross-branch mutation", "mitigation": "domain locks"}],
            acceptance_criteria=["Every artifact has evidence lineage"],
            decision_gates=["Human publication approval"],
            fan_in_owner="AIgentBee leader",
            closure_condition="Accepted outcome and terminal receipts recorded",
        ),
        work_units=[
            HiveWorkUnitSpec(
                work_unit_id="strategy",
                title="Define strategy",
                role="business strategist",
                writable_domain="notion:project",
                authority_ceiling="A2",
            ),
            HiveWorkUnitSpec(
                work_unit_id="prototype",
                title="Build prototype",
                role="developer",
                writable_domain="repo:prototype",
                authority_ceiling="A2",
            ),
            HiveWorkUnitSpec(
                work_unit_id="review",
                title="Review integrated outcome",
                role="independent reviewer",
                dependencies=["strategy", "prototype"],
                writable_domain="notion:review",
                authority_ceiling="A2",
            ),
        ],
    )
    assert created.project_contract.project_kind.value == "hybrid"
    assert created.graph_receipt.validated is True
    assert created.graph_receipt.dependency_waves == [
        ["prototype", "strategy"],
        ["review"],
    ]
    assert created.graph_receipt.max_parallel_width == 2
    assert created.graph_receipt.authority_level == "Execute bounded work (A2)"
    assert created.events[0].payload["graph_receipt"]["dependency_edge_count"] == 2
    assert store.get_mission("hybrid-project").model_dump() == created.model_dump()


@pytest.mark.parametrize(
    ("work_units", "message"),
    [
        (
            [HiveWorkUnitSpec(work_unit_id="a", title="A", role="worker", dependencies=["missing"], authority_ceiling="A2")],
            "unknown dependencies",
        ),
        (
            [
                HiveWorkUnitSpec(work_unit_id="a", title="A", role="worker", dependencies=["b"], authority_ceiling="A2"),
                HiveWorkUnitSpec(work_unit_id="b", title="B", role="worker", dependencies=["a"], authority_ceiling="A2"),
            ],
            "contains a cycle",
        ),
        (
            [HiveWorkUnitSpec(work_unit_id="a", title="A", role="worker", authority_ceiling="A3")],
            "exceeds mission ceiling",
        ),
    ],
)
def test_invalid_work_graph_is_rejected_before_mutation(tmp_path, work_units, message):
    store = _store(tmp_path)
    with pytest.raises(ValueError, match=message):
        store.create_mission(
            title="Invalid graph",
            objective="Must fail closed",
            lifecycle_stage="Plan",
            mission_id="invalid",
            workspace_id="ws-test",
            user_id="user-test",
            account_key="ws-test:user-test",
            authority_ceiling="A2",
            work_units=work_units,
        )

    with sqlite3.connect(store.path) as conn:
        assert conn.execute("SELECT COUNT(*) FROM hive_missions").fetchone()[0] == 0


def test_work_unit_without_explicit_authority_inherits_mission_ceiling(tmp_path):
    store = _store(tmp_path)
    created = store.create_mission(
        title="Bounded project",
        objective="Keep child authority monotonic",
        lifecycle_stage="Plan",
        mission_id="bounded",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        authority_ceiling="A1",
        work_units=[HiveWorkUnitSpec(work_unit_id="a", title="A", role="worker")],
    )

    assert created.work_units[0].authority_ceiling == "A1"


def test_create_idempotency_dedupes_and_conflicting_reuse_fails(tmp_path):
    store = _store(tmp_path)
    first = _create(store)
    second = _create(store)

    assert second.event_count == first.event_count == 1
    assert second.action_count == first.action_count == 1

    with pytest.raises(HiveIdempotencyConflict):
        store.create_mission(
            title="Different",
            objective="Different",
            lifecycle_stage="Build",
            mission_id="m2",
            idempotency_key="create-m1",
        workspace_id="ws-test",
        user_id="user-test",
        account_key="ws-test:user-test",
        profile_name="profile-test",
)


def test_concurrent_create_replay_produces_one_mission(tmp_path):
    store = _store(tmp_path)

    def run(_index: int):
        return _create(store, mission_id="parallel")

    with ThreadPoolExecutor(max_workers=6) as pool:
        results = list(pool.map(run, range(12)))

    assert {item.mission_id for item in results} == {"parallel"}
    assert {item.event_count for item in results} == {1}
    assert store.get_mission("parallel").action_count == 1

def test_event_revision_idempotency_and_transition_controls(tmp_path):
    store = _store(tmp_path)
    created = _create(store)

    updated = store.append_event(
        mission_id="m1",
        event_type="SYNC_PULSE",
        sender="builder",
        payload={"pace": "ON_PACE"},
        work_unit_id="m1-builder",
        work_unit_status="COMPLETED",
        expected_mission_revision=created.revision,
        idempotency_key="event-1",
    )
    assert updated.revision == 2
    assert updated.event_count == 2
    assert updated.action_count == 2
    assert next(
        item for item in updated.work_units if item.work_unit_id == "m1-builder"
    ).status == "COMPLETED"

    replay = store.append_event(
        mission_id="m1",
        event_type="SYNC_PULSE",
        sender="builder",
        payload={"pace": "ON_PACE"},
        work_unit_id="m1-builder",
        work_unit_status="COMPLETED",
        expected_mission_revision=created.revision,
        idempotency_key="event-1",
    )
    assert replay.revision == 2
    assert replay.event_count == 2

    with pytest.raises(HiveTransitionError, match="Stale mission revision"):
        store.append_event(
            mission_id="m1",
            event_type="EVIDENCE",
            sender="toto",
            expected_mission_revision=1,
        )

    with pytest.raises(HiveTransitionError, match="Illegal work-unit transition"):
        store.append_event(
            mission_id="m1",
            event_type="WORK_REOPENED",
            sender="builder",
            work_unit_id="m1-builder",
            work_unit_status="ACTIVE",
            expected_mission_revision=2,
        )


def test_cancel_preserves_completed_work_and_locks_new_events(tmp_path):
    store = _store(tmp_path)
    created = _create(store)
    progressed = store.append_event(
        mission_id="m1",
        event_type="WORK_COMPLETED",
        sender="scout",
        work_unit_id="m1-scout",
        work_unit_status="COMPLETED",
        expected_mission_revision=created.revision,
    )
    cancelled = store.cancel_mission(
        mission_id="m1",
        reason="Operator stopped the mission",
        idempotency_key="cancel-1",
    )

    states = {item.work_unit_id: item.status for item in cancelled.work_units}
    assert states["m1-scout"] == "COMPLETED"
    assert states["m1-builder"] == "CANCELLED"
    assert cancelled.status == "CANCELLED"
    assert cancelled.revision == progressed.revision + 1

    with pytest.raises(HiveTransitionError, match="terminal mission"):
        store.append_event(
            mission_id="m1",
            event_type="HEARTBEAT",
            sender="builder",
        )


def test_fan_in_preserves_dissent_and_can_close(tmp_path):
    store = _store(tmp_path)
    _create(store)
    result = store.fan_in(
        mission_id="m1",
        status="GO",
        summary="Candidate meets the first gate.",
        dissent=[{"worker": "lion", "concern": "pilot only"}],
        evidence=[{"type": "pytest", "result": "passed"}],
        close_mission=True,
        idempotency_key="fanin-1",
    )

    assert result.status == "CLOSED"
    assert result.decision is not None
    assert result.decision.dissent[0]["worker"] == "lion"
    assert result.decision.evidence[0]["type"] == "pytest"
    assert all(item.status == "CANCELLED" for item in result.work_units)

    replay = store.fan_in(
        mission_id="m1",
        status="GO",
        summary="Candidate meets the first gate.",
        dissent=[{"worker": "lion", "concern": "pilot only"}],
        evidence=[{"type": "pytest", "result": "passed"}],
        close_mission=True,
        idempotency_key="fanin-1",
    )
    assert replay.decision.decision_id == result.decision.decision_id

def test_future_schema_is_rejected_before_runtime_tables_are_created(tmp_path):
    db_path = tmp_path / "future.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.execute("PRAGMA user_version = 99")
        conn.commit()

    with pytest.raises(HiveSchemaVersionError, match="newer than supported"):
        HiveRuntimeStore(db_path)

    with sqlite3.connect(db_path) as conn:
        assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == 99
        tables = {
            row[0]
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert not {name for name in tables if name.startswith("hive_")}


def test_snapshot_limits_bound_payload_but_preserve_counts(tmp_path):
    store = _store(tmp_path)
    _create(store)
    for index in range(5):
        store.append_event(
            mission_id="m1",
            event_type="EVIDENCE",
            sender="toto",
            payload={"index": index},
            idempotency_key=f"evidence-{index}",
        )

    bounded = store.get_mission("m1", event_limit=2, action_limit=3)
    assert bounded.event_count == 6
    assert bounded.action_count == 6
    assert len(bounded.events) == 2
    assert len(bounded.actions) == 3
    assert [item.payload["index"] for item in bounded.events] == [3, 4]


def test_delegated_task_graph_is_durable_idempotent_and_lane_local(tmp_path):
    store = _store(tmp_path)
    created = _create(store)
    delegated = store.delegate_tasks(
        mission_id="m1",
        tasks=[
            _task("inspect"),
            _task("change", dependencies=["inspect"]),
        ],
        expected_mission_revision=created.revision,
        idempotency_key="delegate-1",
    )

    assert delegated.delegated_task_count == 2
    assert delegated.revision == 2
    assert {item.task_id for item in delegated.delegated_tasks} == {
        "inspect",
        "change",
    }
    receipt = delegated.task_graph_receipts[0]
    assert receipt.parent_lane_id == "m1-builder"
    assert receipt.dependency_waves == [["inspect"], ["change"]]
    assert receipt.ready_task_ids == ["inspect"]
    assert receipt.fan_in_ready is False
    assert delegated.events[-2].event_type == "TASK_DELEGATED"

    replay = store.delegate_tasks(
        mission_id="m1",
        tasks=[
            _task("inspect"),
            _task("change", dependencies=["inspect"]),
        ],
        expected_mission_revision=created.revision,
        idempotency_key="delegate-1",
    )
    assert replay.revision == delegated.revision
    assert replay.delegated_task_count == 2

def test_blocked_task_is_not_reported_ready_until_explicit_reactivation(tmp_path):
    store = _store(tmp_path)
    _create(store)
    store.delegate_tasks(mission_id="m1", tasks=[_task("blocked")])
    store.transition_delegated_task(
        mission_id="m1",
        task_id="blocked",
        status="ACCEPTED",
        actor="worker-a",
    )
    blocked = store.transition_delegated_task(
        mission_id="m1",
        task_id="blocked",
        status="BLOCKED",
        actor="worker-a",
    )

    receipt = blocked.task_graph_receipts[0]
    assert receipt.blocked_task_ids == ["blocked"]
    assert receipt.ready_task_ids == []

    reactivated = store.transition_delegated_task(
        mission_id="m1",
        task_id="blocked",
        status="ACTIVE",
        actor="worker-a",
    )
    assert reactivated.task_graph_receipts[0].blocked_task_ids == []


def test_lane_local_writable_conflicts_are_scheduled_and_locked(tmp_path):
    store = _store(tmp_path)
    _create(store)
    delegated = store.delegate_tasks(
        mission_id="m1",
        tasks=[_task("left"), _task("right")],
    )

    receipt = delegated.task_graph_receipts[0]
    assert receipt.mutation_conflicts == [["left", "right"]]
    assert receipt.execution_waves == [["left"], ["right"]]
    for task_id in ("left", "right"):
        store.transition_delegated_task(
            mission_id="m1",
            task_id=task_id,
            status="ACCEPTED",
            actor=task_id,
        )
    store.transition_delegated_task(
        mission_id="m1", task_id="left", status="ACTIVE", actor="left"
    )
    with pytest.raises(HiveTransitionError, match="writable-domain conflicts"):
        store.transition_delegated_task(
            mission_id="m1", task_id="right", status="ACTIVE", actor="right"
        )


@pytest.mark.parametrize(
    ("task", "message"),
    [
        (
            _task("too-powerful").model_copy(
                update={"authority_ceiling": "A4"}
            ),
            "authority must not exceed",
        ),
        (
            _task("outside-domain").model_copy(
                update={"writable_domains": ["app/mcp_server.py"]}
            ),
            "writable domain exceeds lane",
        ),
    ],
)
def test_delegated_task_inheritance_violations_fail_before_mutation(
    tmp_path, task, message
):
    store = _store(tmp_path)
    _create(store)

    with pytest.raises(ValueError, match=message):
        store.delegate_tasks(mission_id="m1", tasks=[task])

    assert store.get_mission("m1").delegated_task_count == 0


def test_task_dependencies_leases_handoffs_and_lane_fan_in(tmp_path):
    store = _store(tmp_path)
    created = _create(store)
    store.delegate_tasks(
        mission_id="m1",
        tasks=[_task("first"), _task("second", dependencies=["first"])],
        expected_mission_revision=created.revision,
    )

    with pytest.raises(HiveTransitionError, match="incomplete dependencies"):
        store.transition_delegated_task(
            mission_id="m1",
            task_id="second",
            status="ACCEPTED",
            actor="worker-b",
        )

    accepted = store.transition_delegated_task(
        mission_id="m1",
        task_id="first",
        status="ACCEPTED",
        actor="worker-a",
        worker_binding="worker-a",
        lease_seconds=60,
    )
    leased = next(item for item in accepted.delegated_tasks if item.task_id == "first")
    assert leased.execution_lease_owner == "worker-a"
    assert leased.execution_lease_expires_at > leased.updated_at

    with pytest.raises(HiveTransitionError, match="lease is held"):
        store.transition_delegated_task(
            mission_id="m1",
            task_id="first",
            status="ACTIVE",
            actor="worker-b",
            worker_binding="worker-b",
        )

    store.transition_delegated_task(
        mission_id="m1",
        task_id="first",
        status="ACTIVE",
        actor="worker-a",
    )
    handoff = HiveHandoffReceipt(
        summary="First task is verified",
        deliverables=[{"artifact": "patch"}],
        evidence=[{"check": "pytest", "result": "passed"}],
        next_owner="lane-captain",
    )
    store.transition_delegated_task(
        mission_id="m1",
        task_id="first",
        status="HANDOFF_READY",
        actor="worker-a",
        evidence=[{"check": "pytest", "result": "passed"}],
        handoff_receipt=handoff,
    )
    first_done = store.transition_delegated_task(
        mission_id="m1",
        task_id="first",
        status="COMPLETED",
        actor="lane-captain",
    )
    assert first_done.task_graph_receipts[0].ready_task_ids == ["second"]

    for status, actor in (
        ("ACCEPTED", "worker-b"),
        ("ACTIVE", "worker-b"),
    ):
        store.transition_delegated_task(
            mission_id="m1", task_id="second", status=status, actor=actor
        )
    store.transition_delegated_task(
        mission_id="m1",
        task_id="second",
        status="HANDOFF_READY",
        actor="worker-b",
        evidence=[{"check": "review", "result": "accepted"}],
        handoff_receipt=handoff,
    )
    completed = store.transition_delegated_task(
        mission_id="m1",
        task_id="second",
        status="COMPLETED",
        actor="lane-captain",
    )

    assert completed.task_graph_receipts[0].fan_in_ready is True
    assert {item.event_type for item in completed.events} >= {
        "TASK_ACCEPTED",
        "HANDOFF_READY",
        "HANDOFF_ACCEPTED",
        "LANE_FAN_IN_READY",
    }


def test_lane_completion_waits_for_delegated_task_fan_in(tmp_path):
    store = _store(tmp_path)
    _create(store)
    store.delegate_tasks(mission_id="m1", tasks=[_task("pending")])

    with pytest.raises(HiveTransitionError, match="fan-in is ready"):
        store.append_event(
            mission_id="m1",
            event_type="LANE_COMPLETED",
            sender="lane-captain",
            work_unit_id="m1-builder",
            work_unit_status="COMPLETED",
        )


def test_schema_v2_migrates_to_delegated_task_table(tmp_path):
    store = _store(tmp_path)
    with sqlite3.connect(store.path) as conn:
        conn.execute("DROP TABLE hive_delegated_tasks")
        conn.execute("PRAGMA user_version = 2")
        conn.commit()

    migrated = HiveRuntimeStore(store.path)
    with sqlite3.connect(migrated.path) as conn:
        assert int(conn.execute("PRAGMA user_version").fetchone()[0]) == 3
        assert conn.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'hive_delegated_tasks'"
        ).fetchone()
