"""Bind Hive leader/worker conversation lanes to a hard account scope."""

from __future__ import annotations

from typing import Any

from app.account_scope import require_matching_account_key
from app.hive_runtime import resolve_mission_account_scope


def leader_conversation_id(mission_id: str) -> str:
    # Local import avoids a package cycle with hive_multithread.
    from app.hive_multithread import leader_conversation_id as _leader_conversation_id

    return _leader_conversation_id(mission_id)


def ensure_mission_lane_conversation_scopes(
    manager: Any,
    *,
    mission_id: str,
    account_key: str,
    workspace_id: str,
    user_id: str,
    profile_name: str = "",
    work_unit_conversation_ids: list[str] | None = None,
    teamspace_id: str = "",
) -> list[dict[str, str]]:
    """
    Create (if needed) and bind leader + worker conversation lanes.

    Fail closed when an existing conversation is bound to a different account.
    """
    scope = resolve_mission_account_scope(
        account_key=account_key,
        workspace_id=workspace_id,
        user_id=user_id,
        profile_name=profile_name,
    )
    conversation_ids: list[str] = [leader_conversation_id(mission_id)]
    for item in work_unit_conversation_ids or []:
        text = str(item or "").strip()
        if text and text not in conversation_ids:
            conversation_ids.append(text)

    bound: list[dict[str, str]] = []
    for conversation_id in conversation_ids:
        if not manager.conversation_exists(conversation_id):
            manager.new_conversation(
                title=f"Hive {mission_id}",
                conversation_id=conversation_id,
                workspace_id=scope["workspace_id"],
                user_id=scope["user_id"],
                profile_name=scope["profile_name"],
                teamspace_id=teamspace_id,
            )
        else:
            manager.bind_conversation_scope(
                conversation_id,
                workspace_id=scope["workspace_id"],
                user_id=scope["user_id"],
                profile_name=scope["profile_name"],
                teamspace_id=teamspace_id,
            )
        stored = manager.get_conversation_scope(conversation_id)
        require_matching_account_key(
            scope["account_key"],
            workspace_id=stored.get("workspace_id"),
            user_id=stored.get("user_id"),
            profile_name=stored.get("profile_name") or scope["profile_name"],
        )
        bound.append(
            {
                "conversation_id": conversation_id,
                "account_key": scope["account_key"],
                "workspace_id": str(stored.get("workspace_id") or ""),
                "user_id": str(stored.get("user_id") or ""),
                "thread_id": str(stored.get("thread_id") or ""),
            }
        )
    return bound
