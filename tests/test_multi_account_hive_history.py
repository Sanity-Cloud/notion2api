from __future__ import annotations

from pathlib import Path

from app.chat_history.migrate import (
    infer_thread_account_key,
    migrate_shared_chat_history,
)
from app.chat_history.store import (
    LEGACY_ACCOUNT_KEY,
    ChatHistoryStore,
    get_account_chat_history_db_path,
)
from app.hive_bee_call import extract_bee_call_envelope, validate_bee_notion_call
from app.hive_lane_scope import ensure_mission_lane_conversation_scopes
from app.hive_multithread import (
    BeeNotionCallEnvelope,
    MultithreadContractError,
    ThreadBinding,
    ThreadKind,
    leader_conversation_id,
    worker_conversation_id,
)
from app.hive_runtime import HiveRuntimeStore, HiveWorkUnitSpec
from app.conversation import ConversationManager
from app.notion_admission import NotionAdmissionController
import pytest


def test_infer_thread_account_key_from_live_provenance() -> None:
    key = infer_thread_account_key(
        {
            "id": "t1",
            "raw_json": '{"live":{"account_key":"ws-a:user-a","source_system":"aigentbee"}}',
        }
    )
    assert key == "ws-a:user-a"


def test_migrate_shared_chat_history_partitions_and_quarantines(
    tmp_path, monkeypatch
) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    monkeypatch.delenv("CHAT_HISTORY_DB_PATH", raising=False)
    monkeypatch.delenv("CHAT_HISTORY_DB_DIR", raising=False)

    source = tmp_path / "chat_history.db"
    legacy_store = ChatHistoryStore(str(source))
    legacy_store.upsert_bundle(
        {
            "threads": {
                "owned": {
                    "id": "owned",
                    "title": "Owned",
                    "message_ids": ["m-owned"],
                    "raw": {"live": {"account_key": "ws-1:user-1"}},
                },
                "orphan": {
                    "id": "orphan",
                    "title": "Orphan",
                    "message_ids": ["m-orphan"],
                    "raw": {"type": "workflow"},
                },
            },
            "messages": {
                "m-owned": {
                    "id": "m-owned",
                    "thread_id": "owned",
                    "role": "user",
                    "text": "hello owned",
                    "raw": {},
                },
                "m-orphan": {
                    "id": "m-orphan",
                    "thread_id": "orphan",
                    "role": "user",
                    "text": "hello orphan",
                    "raw": {},
                },
            },
        }
    )

    result = migrate_shared_chat_history(source_db=source, dry_run=False)
    assert result.attributed_threads == 1
    assert result.quarantined_threads == 1
    assert "ws-1:user-1" in result.account_shards

    owned = ChatHistoryStore(account_key="ws-1:user-1")
    assert owned.get_thread("owned") is not None
    assert owned.get_thread("orphan") is None

    quarantined = ChatHistoryStore(account_key=LEGACY_ACCOUNT_KEY)
    assert quarantined.get_thread("orphan") is not None
    assert Path(get_account_chat_history_db_path(LEGACY_ACCOUNT_KEY)).exists()
    assert Path(str(source) + ".pre-account-migration").exists()


def test_hive_create_requires_account_and_binds_lanes(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("DB_PATH", str(tmp_path / "conversations.db"))
    store = HiveRuntimeStore(tmp_path / "hive.sqlite3")
    with pytest.raises(Exception, match="workspace_id and user_id"):
        store.create_mission(
            title="No account",
            objective="Fail closed",
            lifecycle_stage="Build",
        )

    snapshot = store.create_mission(
        title="Bound mission",
        objective="Isolate bees",
        lifecycle_stage="Build",
        mission_id="mission-bound",
        workspace_id="ws-bound",
        user_id="user-bound",
        profile_name="profile-bound",
        work_units=[
            HiveWorkUnitSpec(
                work_unit_id="lane-a",
                title="Lane A",
                role="worker-a",
                conversation_id="conv-a",
            )
        ],
    )
    assert snapshot.account_key == "ws-bound:user-bound"

    manager = ConversationManager()
    bound = ensure_mission_lane_conversation_scopes(
        manager,
        mission_id=snapshot.mission_id,
        account_key=snapshot.account_key,
        workspace_id=snapshot.workspace_id,
        user_id=snapshot.user_id,
        profile_name=snapshot.profile_name,
        work_unit_conversation_ids=["conv-a"],
    )
    assert {item["conversation_id"] for item in bound} >= {
        leader_conversation_id(snapshot.mission_id),
        "conv-a",
    }
    scope = manager.get_conversation_scope("conv-a")
    assert scope["workspace_id"] == "ws-bound"
    assert scope["user_id"] == "user-bound"


def test_bee_call_envelope_rejects_account_and_lane_mismatch(tmp_path) -> None:
    store = HiveRuntimeStore(tmp_path / "hive.sqlite3")
    mission = store.create_mission(
        title="Bee contract",
        objective="Validate envelopes",
        lifecycle_stage="Build",
        mission_id="mission-bee",
        workspace_id="ws-1",
        user_id="user-1",
        work_units=[
            HiveWorkUnitSpec(
                work_unit_id="lane-a",
                title="Lane A",
                role="worker-a",
                conversation_id="conv-a",
            ),
            HiveWorkUnitSpec(
                work_unit_id="lane-b",
                title="Lane B",
                role="worker-b",
                conversation_id="conv-b",
            ),
        ],
    )

    class _Client:
        space_id = "ws-1"
        user_id = "user-1"
        profile_name = "p1"

    ok = validate_bee_notion_call(
        metadata={
            "idempotency_key": "bee-1",
            "caller": {
                "type": "aigentbee",
                "mission_id": mission.mission_id,
                "work_unit_id": "lane-a",
                "worker_id": "worker-a",
            },
        },
        conversation_id="conv-a",
        client=_Client(),
        runtime_store=store,
    )
    assert ok is not None
    assert ok.account_key == "ws-1:user-1"

    with pytest.raises(MultithreadContractError, match="conversation_id"):
        validate_bee_notion_call(
            metadata={
                "idempotency_key": "bee-2",
                "caller": {
                    "type": "aigentbee",
                    "mission_id": mission.mission_id,
                    "work_unit_id": "lane-a",
                },
            },
            conversation_id="conv-b",
            client=_Client(),
            runtime_store=store,
        )

    class _Other:
        space_id = "ws-other"
        user_id = "user-other"
        profile_name = "other"

    with pytest.raises(MultithreadContractError, match="account_key"):
        validate_bee_notion_call(
            metadata={
                "idempotency_key": "bee-3",
                "caller": {
                    "type": "aigentbee",
                    "mission_id": mission.mission_id,
                    "work_unit_id": "lane-a",
                },
            },
            conversation_id="conv-a",
            client=_Other(),
            runtime_store=store,
        )


def test_bee_envelope_requires_idempotency_key() -> None:
    with pytest.raises(MultithreadContractError, match="idempotency_key"):
        extract_bee_call_envelope(
            {
                "caller": {
                    "type": "aigentbee",
                    "mission_id": "m1",
                    "work_unit_id": "wu1",
                }
            },
            conversation_id="c1",
            account_key="ws:u",
        )


def test_admission_default_allows_multi_bee_per_account(monkeypatch) -> None:
    monkeypatch.delenv("NOTION_ADMISSION_ACCOUNT_MAX_INFLIGHT", raising=False)
    controller = NotionAdmissionController(shared_store=False)
    assert controller.max_account_inflight == 4


def test_thread_binding_fills_account_key() -> None:
    binding = ThreadBinding(
        mission_id="mission-1",
        work_unit_id="lane-a",
        worker_id="worker-a",
        thread_kind=ThreadKind.WORKER,
        conversation_id=worker_conversation_id("plan-1", "worker-a"),
        leader_conversation_id=leader_conversation_id("mission-1"),
        profile_id="profile-1",
        notion_user_id="user-1",
        workspace_id="workspace-1",
    )
    assert binding.account_key == "workspace-1:user-1"
    envelope = BeeNotionCallEnvelope(
        account_key=binding.account_key,
        conversation_id=binding.conversation_id,
        mission_id=binding.mission_id,
        work_unit_id=binding.work_unit_id,
        worker_id=binding.worker_id,
        idempotency_key="idemp-1",
        workspace_id=binding.workspace_id,
        user_id=binding.notion_user_id,
        profile_id=binding.profile_id,
    )
    envelope.validate_lane(
        binding,
        mission_conversation_ids=[binding.conversation_id],
    )
