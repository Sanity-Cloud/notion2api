from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any, Iterable, Mapping


DEFAULT_CONTRACT_VERSION = "sanitycloud-governance-v2-sanity-management"
DEFAULT_WORKSPACE_ID = "fe8b13aa-3ad2-811e-8292-0003b78a02f9"
DEFAULT_TEAMSPACE_ID = "3acb13aa-3ad2-8176-9ff3-004220d4868f"
DEFAULT_AUTHORITY_PAGE_ID = "3acb13aa-3ad2-816c-b6ec-d4930a87e12e"
DEFAULT_OUTPUT_PARENT_PAGE_ID = "3acb13aa-3ad2-8161-b851-f9bb30c31ecc"
DEFAULT_FEEDBACK_PARENT_PAGE_ID = "3acb13aa-3ad2-813f-81c9-c659c579c093"


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _normalized_notion_id(value: Any) -> str:
    return _clean(value).replace("-", "").lower()


@dataclass(frozen=True)
class GovernanceContract:
    """Canonical governance and knowledge-routing contract for every provider route."""

    version: str
    workspace_id: str
    teamspace_id: str
    authority_page_id: str
    documented_output_parent_page_id: str
    procedural_feedback_parent_page_id: str

    @classmethod
    def from_env(cls) -> "GovernanceContract":
        return cls(
            version=_clean(os.getenv("SANITYCLOUD_GOVERNANCE_CONTRACT_VERSION"))
            or DEFAULT_CONTRACT_VERSION,
            workspace_id=_clean(os.getenv("SANITYCLOUD_NOTION_WORKSPACE_ID"))
            or DEFAULT_WORKSPACE_ID,
            teamspace_id=_clean(os.getenv("SANITYCLOUD_NOTION_TEAMSPACE_ID"))
            or DEFAULT_TEAMSPACE_ID,
            authority_page_id=_clean(
                os.getenv("SANITYCLOUD_GOVERNANCE_AUTHORITY_PAGE_ID")
            )
            or DEFAULT_AUTHORITY_PAGE_ID,
            documented_output_parent_page_id=_clean(
                os.getenv("SANITYCLOUD_DOCUMENTED_OUTPUT_PARENT_PAGE_ID")
            )
            or DEFAULT_OUTPUT_PARENT_PAGE_ID,
            procedural_feedback_parent_page_id=_clean(
                os.getenv("SANITYCLOUD_PROCEDURAL_FEEDBACK_PARENT_PAGE_ID")
            )
            or DEFAULT_FEEDBACK_PARENT_PAGE_ID,
        ).validated()

    def validated(self) -> "GovernanceContract":
        fields = {
            "version": self.version,
            "workspace_id": self.workspace_id,
            "teamspace_id": self.teamspace_id,
            "authority_page_id": self.authority_page_id,
            "documented_output_parent_page_id": self.documented_output_parent_page_id,
            "procedural_feedback_parent_page_id": self.procedural_feedback_parent_page_id,
        }
        missing = [name for name, value in fields.items() if not _clean(value)]
        if missing:
            raise ValueError(
                "SanityCloud governance contract is incomplete: " + ", ".join(missing)
            )
        return self

    def receipt(self) -> dict[str, Any]:
        return {
            "contract_version": self.version,
            "workspace_id": self.workspace_id,
            "teamspace_id": self.teamspace_id,
            "authority_page_id": self.authority_page_id,
            "documented_output_parent_page_id": self.documented_output_parent_page_id,
            "procedural_feedback_parent_page_id": self.procedural_feedback_parent_page_id,
            "aligned": True,
        }

    def operating_instruction(self) -> str:
        return (
            f"SanityCloud governance contract {self.version}: use workspace "
            f"{self.workspace_id} and teamspace {self.teamspace_id}. Treat page "
            f"{self.authority_page_id} as the ultimate governance authority and source "
            "of truth. Preserve source lineage and distinguish verified evidence, "
            "hypotheses, drafts, and adopted decisions. Route durable documented outputs "
            f"beneath {self.documented_output_parent_page_id}. Route procedural feedback, "
            "contradictions, exceptions, and lessons learned beneath "
            f"{self.procedural_feedback_parent_page_id}. Do not silently substitute another "
            "workspace, teamspace, authority page, output root, or feedback root."
        )

    def bind_accounts(
        self, accounts: Iterable[Mapping[str, Any]]
    ) -> list[dict[str, Any]]:
        bound: list[dict[str, Any]] = []
        for index, source in enumerate(accounts):
            account = dict(source)
            actual_space = _clean(account.get("space_id"))
            if _normalized_notion_id(actual_space) != _normalized_notion_id(
                self.workspace_id
            ):
                raise ValueError(
                    "Notion account workspace does not match the SanityCloud governance "
                    f"contract (account {index}: {actual_space or '<missing>'}; expected "
                    f"{self.workspace_id})."
                )

            configured_teamspace = _clean(account.get("governance_teamspace_id"))
            if configured_teamspace and _normalized_notion_id(
                configured_teamspace
            ) != _normalized_notion_id(self.teamspace_id):
                raise ValueError(
                    "Notion account governance_teamspace_id conflicts with the canonical "
                    f"teamspace (account {index})."
                )

            configured_context = _clean(account.get("context_page_id"))
            if configured_context and _normalized_notion_id(
                configured_context
            ) != _normalized_notion_id(self.authority_page_id):
                raise ValueError(
                    "Notion account context_page_id conflicts with the canonical governance "
                    f"authority (account {index})."
                )

            configured_output = _clean(account.get("repo_ai_parent_page_id"))
            if configured_output and _normalized_notion_id(
                configured_output
            ) != _normalized_notion_id(self.documented_output_parent_page_id):
                raise ValueError(
                    "Notion account repo_ai_parent_page_id conflicts with the canonical "
                    f"documented-output root (account {index})."
                )

            account.update(
                {
                    "space_id": actual_space,
                    "context_page_id": self.authority_page_id,
                    "repo_ai_parent_page_id": self.documented_output_parent_page_id,
                    "governance_contract_version": self.version,
                    "governance_workspace_id": self.workspace_id,
                    "governance_teamspace_id": self.teamspace_id,
                    "governance_authority_page_id": self.authority_page_id,
                    "documented_output_parent_page_id": self.documented_output_parent_page_id,
                    "procedural_feedback_parent_page_id": self.procedural_feedback_parent_page_id,
                    "governance_operating_instruction": self.operating_instruction(),
                }
            )
            bound.append(account)
        return bound


def governance_receipt_from_client(client: Any) -> dict[str, Any]:
    return {
        "contract_version": _clean(getattr(client, "governance_contract_version", "")),
        "workspace_id": _clean(getattr(client, "governance_workspace_id", "")),
        "teamspace_id": _clean(getattr(client, "governance_teamspace_id", "")),
        "authority_page_id": _clean(
            getattr(client, "governance_authority_page_id", "")
        ),
        "documented_output_parent_page_id": _clean(
            getattr(client, "documented_output_parent_page_id", "")
        ),
        "procedural_feedback_parent_page_id": _clean(
            getattr(client, "procedural_feedback_parent_page_id", "")
        ),
        "aligned": bool(getattr(client, "governance_aligned", False)),
    }


def governance_instruction_from_client(client: Any) -> str:
    instruction = _clean(getattr(client, "governance_operating_instruction", ""))
    return instruction


def resolve_governed_context_page_id(client: Any, requested: str = "") -> str:
    authority = _clean(getattr(client, "governance_authority_page_id", ""))
    fallback = _clean(getattr(client, "context_page_id", ""))
    canonical = authority or fallback
    requested_clean = _clean(requested)
    if (
        requested_clean
        and canonical
        and _normalized_notion_id(requested_clean) != _normalized_notion_id(canonical)
    ):
        raise ValueError(
            "Requested context_page_id conflicts with the canonical SanityCloud governance "
            f"authority page {canonical}."
        )
    return canonical or requested_clean


def combine_governance_instructions(client: Any, requested: str | None) -> str:
    governance = governance_instruction_from_client(client)
    caller = _clean(requested)
    return "\n".join(part for part in (governance, caller) if part)
