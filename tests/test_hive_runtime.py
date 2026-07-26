from __future__ import annotations

import sqlite3
from concurrent.futures import ThreadPoolExecutor

import pytest

from app.hive_runtime import (
    HiveIdempotencyConflict,
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
    )


def test_mission_round_trip_includes_actions_and_bindings(tmp_path):
    store = _store(tmp_path)
    created = _create(store)

    assert created.status == "ACTIVE"
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