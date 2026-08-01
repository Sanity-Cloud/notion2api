from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from app.governance import GovernanceContract


@dataclass(frozen=True)
class WorkspaceDefinition:
    key: str
    name: str
    teamspace_name: str
    aliases: tuple[str, ...]
    contract: GovernanceContract

    def descriptor(self) -> dict[str, Any]:
        return {
            "workspace_key": self.key,
            "workspace_name": self.name,
            "workspace_id": self.contract.workspace_id,
            "teamspace_name": self.teamspace_name,
            "teamspace_id": self.contract.teamspace_id,
            **self.contract.receipt(),
        }


SANITY_MANAGEMENT = WorkspaceDefinition(
    key="sanity-management",
    name="Sanity Management",
    teamspace_name="Sanity-Cloud-InScene",
    aliases=(
        "sanity-management",
        "sanity management",
        "management",
        "sm",
        "fe8b13aa-3ad2-811e-8292-0003b78a02f9",
    ),
    contract=GovernanceContract(
        version="sanitycloud-governance-v2-sanity-management",
        workspace_id="fe8b13aa-3ad2-811e-8292-0003b78a02f9",
        teamspace_id="3acb13aa-3ad2-8176-9ff3-004220d4868f",
        authority_page_id="3acb13aa-3ad2-816c-b6ec-d4930a87e12e",
        documented_output_parent_page_id="3acb13aa-3ad2-8161-b851-f9bb30c31ecc",
        procedural_feedback_parent_page_id="3acb13aa-3ad2-813f-81c9-c659c579c093",
    ).validated(),
)

SANITYCLOUD_HQ = WorkspaceDefinition(
    key="sanitycloud-hq",
    name="SanityCloud-HQ",
    teamspace_name="Sanity-Cloud-InScene",
    aliases=(
        "sanitycloud-hq",
        "sanitycloud hq",
        "sanityhq",
        "sanity-hq",
        "hq",
        "034bf4af-15b3-81a9-a6ce-000330e15c65",
    ),
    contract=GovernanceContract(
        version="sanitycloud-governance-v1",
        workspace_id="034bf4af-15b3-81a9-a6ce-000330e15c65",
        teamspace_id="3aabf4af-15b3-810f-a1e8-004254c8eb80",
        authority_page_id="3a8bf4af-15b3-811e-aca0-d011efea6b50",
        documented_output_parent_page_id="1f2e3064-f1f9-424d-9892-ca82f88238d7",
        procedural_feedback_parent_page_id="3a8bf4af-15b3-81f1-a9bf-ebf67111b1ab",
    ).validated(),
)

KNOWN_WORKSPACES = (SANITY_MANAGEMENT, SANITYCLOUD_HQ)


def _normalize(value: Any) -> str:
    return " ".join(str(value or "").strip().casefold().replace("_", "-").split())


def resolve_workspace_definition(selector: Any) -> WorkspaceDefinition:
    normalized = _normalize(selector)
    if not normalized:
        raise ValueError("A workspace selector is required")
    matches = [
        item
        for item in KNOWN_WORKSPACES
        if normalized
        in {
            _normalize(item.key),
            _normalize(item.name),
            _normalize(item.contract.workspace_id),
            *(_normalize(alias) for alias in item.aliases),
        }
    ]
    if not matches:
        raise ValueError(f"Unknown Notion workspace selector: {selector}")
    if len(matches) > 1:
        raise ValueError(f"Ambiguous Notion workspace selector: {selector}")
    return matches[0]


def configured_workspace_definitions() -> tuple[WorkspaceDefinition, ...]:
    raw = os.getenv("SANITYCLOUD_ENABLED_WORKSPACES", "").strip()
    if not raw:
        return KNOWN_WORKSPACES
    selected: list[WorkspaceDefinition] = []
    for token in raw.split(","):
        item = resolve_workspace_definition(token)
        if item not in selected:
            selected.append(item)
    if not selected:
        raise ValueError("SANITYCLOUD_ENABLED_WORKSPACES did not select any workspaces")
    return tuple(selected)


def default_workspace_definition() -> WorkspaceDefinition:
    selector = os.getenv("SANITYCLOUD_DEFAULT_WORKSPACE", SANITY_MANAGEMENT.key)
    selected = resolve_workspace_definition(selector)
    enabled = configured_workspace_definitions()
    if selected not in enabled:
        raise ValueError(
            f"Default workspace {selected.key!r} is not in SANITYCLOUD_ENABLED_WORKSPACES"
        )
    return selected


def expand_accounts_for_workspaces(
    accounts: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Create one credential-safe routing record per user/workspace membership.

    The browser credential and active-user selector are reused, but each record gets
    its own workspace, teamspace, and governance contract. This is the identity used
    for chat binding. Durable pages remain workspace/teamspace scoped.
    """
    expanded: list[dict[str, Any]] = []
    for workspace in configured_workspace_definitions():
        workspace_accounts: list[dict[str, Any]] = []
        for source in accounts:
            account = dict(source)
            base_profile = str(account.get("base_profile_name") or account.get("profile_name") or "").strip()
            user_id = str(account.get("user_id") or "").strip()
            account.update(
                {
                    "base_profile_name": base_profile,
                    "credential_profile_name": base_profile,
                    "routing_profile_name": f"{workspace.key}:{base_profile}",
                    "chat_scope_key": f"{workspace.contract.workspace_id}:{user_id}",
                    "page_scope_key": (
                        f"{workspace.contract.workspace_id}:"
                        f"{workspace.contract.teamspace_id}"
                    ),
                    "workspace_key": workspace.key,
                    "workspace_name": workspace.name,
                    "teamspace_name": workspace.teamspace_name,
                    "space_id": workspace.contract.workspace_id,
                    "workspace_membership_verified": True,
                }
            )
            workspace_accounts.append(account)
        expanded.extend(workspace.contract.bind_accounts(workspace_accounts))
    return expanded


def workspace_descriptors() -> list[dict[str, Any]]:
    return [item.descriptor() for item in configured_workspace_definitions()]
