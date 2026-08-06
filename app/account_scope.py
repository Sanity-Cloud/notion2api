from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Any


class AccountScopeError(ValueError):
    """Raised when a Notion account identity is absent or inconsistent."""


def _required(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise AccountScopeError(f"{field_name} is required")
    return text


def normalized_scope_id(value: Any) -> str:
    """Normalize UUID-like scope identifiers for equality checks only."""
    return str(value or "").strip().replace("-", "").casefold()


def canonical_account_key(
    workspace_id: Any,
    user_id: Any,
    *,
    profile_name: Any = "",
    allow_profile_fallback: bool = False,
) -> str:
    workspace = str(workspace_id or "").strip()
    user = str(user_id or "").strip()
    if workspace and user:
        return f"{workspace}:{user}"
    profile = str(profile_name or "").strip()
    if allow_profile_fallback and profile:
        return f"profile:{profile}"
    raise AccountScopeError(
        "workspace_id and user_id are required to resolve account_key"
    )


def account_key_from_client(client: Any, *, allow_profile_fallback: bool = False) -> str:
    return canonical_account_key(
        getattr(client, "space_id", ""),
        getattr(client, "user_id", ""),
        profile_name=(
            getattr(client, "profile_name", "")
            or getattr(client, "base_profile_name", "")
        ),
        allow_profile_fallback=allow_profile_fallback,
    )


def account_key_matches(
    account_key: Any,
    *,
    workspace_id: Any,
    user_id: Any,
    profile_name: Any = "",
) -> bool:
    supplied = str(account_key or "").strip()
    if not supplied:
        return False
    try:
        expected = canonical_account_key(
            workspace_id,
            user_id,
            profile_name=profile_name,
            allow_profile_fallback=supplied.startswith("profile:"),
        )
    except AccountScopeError:
        return False
    if supplied.startswith("profile:") or expected.startswith("profile:"):
        return supplied.casefold() == expected.casefold()
    left_workspace, _, left_user = supplied.partition(":")
    right_workspace, _, right_user = expected.partition(":")
    return (
        normalized_scope_id(left_workspace) == normalized_scope_id(right_workspace)
        and normalized_scope_id(left_user) == normalized_scope_id(right_user)
    )


def require_matching_account_key(
    account_key: Any,
    *,
    workspace_id: Any,
    user_id: Any,
    profile_name: Any = "",
) -> str:
    key = _required(account_key, "account_key")
    if not account_key_matches(
        key,
        workspace_id=workspace_id,
        user_id=user_id,
        profile_name=profile_name,
    ):
        raise AccountScopeError(
            "account_key does not match the bound workspace_id and user_id"
        )
    return key


def safe_account_key(account_key: Any, *, max_slug_length: int = 72) -> str:
    """Return a stable filesystem-safe shard name without losing uniqueness."""
    key = _required(account_key, "account_key")
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", key).strip("-._").lower()
    slug = (slug or "account")[:max_slug_length]
    digest = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return f"{slug}-{digest}"


@dataclass(frozen=True)
class AccountScope:
    account_key: str
    workspace_id: str
    user_id: str
    profile_name: str = ""

    @classmethod
    def from_client(cls, client: Any) -> "AccountScope":
        workspace_id = _required(getattr(client, "space_id", ""), "workspace_id")
        user_id = _required(getattr(client, "user_id", ""), "user_id")
        profile_name = str(
            getattr(client, "profile_name", "")
            or getattr(client, "base_profile_name", "")
            or ""
        ).strip()
        return cls(
            account_key=canonical_account_key(workspace_id, user_id),
            workspace_id=workspace_id,
            user_id=user_id,
            profile_name=profile_name,
        )
