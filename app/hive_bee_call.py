"""Validate bee→bee / bee→lane notion2api chat envelopes before upstream work."""

from __future__ import annotations

from typing import Any, Iterable, Mapping

from app.account_scope import account_key_from_client, account_key_matches
from app.chat_history.live_recorder import infer_source_system
from app.hive_multithread import (
    BeeNotionCallEnvelope,
    MultithreadContractError,
    ThreadBinding,
    ThreadKind,
    leader_conversation_id,
)
from app.hive_runtime import get_hive_runtime_store


def extract_bee_call_envelope(
    metadata: Mapping[str, Any] | None,
    *,
    conversation_id: str,
    account_key: str,
) -> BeeNotionCallEnvelope | None:
    """Return a bee envelope when hive lane metadata is present; else None."""
    meta = metadata if isinstance(metadata, Mapping) else {}
    caller = meta.get("caller") if isinstance(meta.get("caller"), dict) else {}
    source = infer_source_system(dict(meta))
    mission_id = str(
        caller.get("mission_id") or meta.get("mission_id") or ""
    ).strip()
    work_unit_id = str(
        caller.get("work_unit_id") or meta.get("work_unit_id") or ""
    ).strip()
    if source != "aigentbee" or not mission_id or not work_unit_id:
        return None
    idempotency_key = str(
        meta.get("idempotency_key")
        or meta.get("request_fingerprint")
        or caller.get("request_fingerprint")
        or caller.get("idempotency_key")
        or ""
    ).strip()
    if not idempotency_key:
        raise MultithreadContractError(
            "idempotency_key is required for bee notion2api chat calls"
        )
    return BeeNotionCallEnvelope(
        account_key=str(account_key or meta.get("account_key") or "").strip(),
        conversation_id=str(
            conversation_id or meta.get("conversation_id") or ""
        ).strip(),
        mission_id=mission_id,
        work_unit_id=work_unit_id,
        worker_id=str(caller.get("worker_id") or meta.get("worker_id") or "").strip(),
        notion_thread_id=str(
            meta.get("notion_thread_id") or caller.get("notion_thread_id") or ""
        ).strip(),
        idempotency_key=idempotency_key,
        profile_id=str(
            meta.get("account_profile")
            or meta.get("profile_name")
            or caller.get("profile_id")
            or ""
        ).strip(),
        workspace_id=str(
            meta.get("workspace_id") or caller.get("workspace_id") or ""
        ).strip(),
        user_id=str(meta.get("user_id") or caller.get("user_id") or "").strip(),
    )


def validate_bee_notion_call(
    *,
    metadata: Mapping[str, Any] | None,
    conversation_id: str,
    client: Any,
    conversation_scope: Mapping[str, Any] | None = None,
    bound_thread_id: str = "",
    runtime_store: Any = None,
) -> BeeNotionCallEnvelope | None:
    """
    Fail closed when an AIgentBee lane chat envelope mismatches account/lane.

    Non-hive callers return None and are left to the normal chat path.
    """
    account_key = account_key_from_client(client)
    envelope = extract_bee_call_envelope(
        metadata,
        conversation_id=conversation_id,
        account_key=account_key,
    )
    if envelope is None:
        return None

    scope = conversation_scope if isinstance(conversation_scope, Mapping) else {}
    if scope and (
        scope.get("workspace_id") or scope.get("user_id")
    ) and not account_key_matches(
        envelope.account_key,
        workspace_id=scope.get("workspace_id"),
        user_id=scope.get("user_id"),
        profile_name=scope.get("profile_name"),
    ):
        raise MultithreadContractError(
            "bee call account_key does not match conversation scope"
        )
    scoped_thread = str(
        (scope.get("thread_id") if scope else "") or bound_thread_id or ""
    ).strip()

    store = runtime_store or get_hive_runtime_store()
    snapshot = store.get_mission(envelope.mission_id)
    if not snapshot.found:
        raise MultithreadContractError(
            f"bee call mission does not exist: {envelope.mission_id}"
        )
    if snapshot.account_key and not account_key_matches(
        envelope.account_key,
        workspace_id=snapshot.workspace_id,
        user_id=snapshot.user_id,
        profile_name=snapshot.profile_name,
    ):
        raise MultithreadContractError(
            "bee call account_key does not match mission account_key"
        )

    lane = next(
        (
            unit
            for unit in snapshot.work_units
            if unit.work_unit_id == envelope.work_unit_id
        ),
        None,
    )
    if lane is None:
        raise MultithreadContractError(
            f"bee call work_unit_id is not part of mission {envelope.mission_id}"
        )
    if lane.conversation_id and lane.conversation_id != envelope.conversation_id:
        raise MultithreadContractError(
            "bee call conversation_id does not match the mission lane conversation"
        )

    workspace = snapshot.workspace_id or envelope.workspace_id
    user = snapshot.user_id or envelope.user_id
    if not workspace or not user:
        raise MultithreadContractError(
            "mission account workspace_id/user_id are required for bee chat calls"
        )

    binding = ThreadBinding(
        mission_id=envelope.mission_id,
        work_unit_id=envelope.work_unit_id,
        worker_id=envelope.worker_id or lane.role or "worker",
        thread_kind=ThreadKind.WORKER,
        conversation_id=envelope.conversation_id,
        leader_conversation_id=leader_conversation_id(envelope.mission_id),
        profile_id=envelope.profile_id or snapshot.profile_name or "hive",
        notion_user_id=user,
        workspace_id=workspace,
        account_key=envelope.account_key,
    )
    mission_conversation_ids: Iterable[str] = [
        unit.conversation_id for unit in snapshot.work_units if unit.conversation_id
    ]
    lane_threads = {
        unit.conversation_id: scoped_thread
        for unit in snapshot.work_units
        if unit.conversation_id == envelope.conversation_id and scoped_thread
    }
    envelope.validate_lane(
        binding,
        mission_conversation_ids=mission_conversation_ids,
        bound_thread_id=scoped_thread,
        lane_thread_ids=lane_threads,
    )
    return envelope
