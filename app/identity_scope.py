"""Fail-closed identity scoping for account-, workspace-, and thread-bound state."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


class IdentityScopeError(ValueError):
    """Raised when state cannot be bound to an explicit identity tuple."""


def _required(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise IdentityScopeError(f"{field_name} is required for identity-scoped state")
    return text


@dataclass(frozen=True)
class IdentityScope:
    profile_id: str
    notion_user_id: str
    workspace_id: str
    thread_id: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "profile_id", _required(self.profile_id, "profile_id"))
        object.__setattr__(
            self,
            "notion_user_id",
            _required(self.notion_user_id, "notion_user_id"),
        )
        object.__setattr__(
            self,
            "workspace_id",
            _required(self.workspace_id, "workspace_id"),
        )
        object.__setattr__(self, "thread_id", _required(self.thread_id, "thread_id"))

    @property
    def key(self) -> tuple[str, str, str, str]:
        return (
            self.profile_id,
            self.notion_user_id,
            self.workspace_id,
            self.thread_id,
        )

    def receipt(self) -> dict[str, str]:
        return {
            "profile_id": self.profile_id,
            "notion_user_id": self.notion_user_id,
            "workspace_id": self.workspace_id,
            "thread_id": self.thread_id,
        }


def identity_scope_from_client(client: Any, thread_id: str) -> IdentityScope:
    """Resolve a stable identity tuple without exposing credentials.

    ``profile_id`` is preferred. ``account_key`` and ``profile_name`` are accepted
    compatibility identifiers because they are stable named profile identifiers,
    not secrets. Anonymous/default fallbacks are intentionally rejected.
    """
    profile_id = (
        getattr(client, "profile_id", "")
        or getattr(client, "account_key", "")
        or getattr(client, "profile_name", "")
    )
    notion_user_id = getattr(client, "user_id", "") or getattr(
        client, "notion_user_id", ""
    )
    workspace_id = (
        getattr(client, "space_id", "")
        or getattr(client, "workspace_id", "")
        or getattr(client, "governance_workspace_id", "")
    )
    return IdentityScope(
        profile_id=str(profile_id or ""),
        notion_user_id=str(notion_user_id or ""),
        workspace_id=str(workspace_id or ""),
        thread_id=str(thread_id or ""),
    )
