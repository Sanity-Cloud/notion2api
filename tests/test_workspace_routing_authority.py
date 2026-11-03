from __future__ import annotations

import pytest

from app.workspace_routing import (
    SANITY_MANAGEMENT,
    SANITY_MONKEY,
    SANITYCLOUD_HQ,
    configured_workspace_definitions,
    default_workspace_definition,
    expand_accounts_for_workspaces,
    resolve_workspace_definition,
    workspace_descriptors,
)


def _clear_workspace_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SANITYCLOUD_ENABLED_WORKSPACES", raising=False)
    monkeypatch.delenv("SANITYCLOUD_DEFAULT_WORKSPACE", raising=False)


def test_sanity_monkey_is_the_only_default_workspace(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)

    assert configured_workspace_definitions() == (SANITY_MONKEY,)
    assert default_workspace_definition() is SANITY_MONKEY
    assert workspace_descriptors() == [SANITY_MONKEY.descriptor()]
    assert SANITY_MONKEY.authoritative is True
    assert SANITY_MONKEY.deprecated is False


def test_legacy_hq_matches_the_renamed_deprecated_teamspace() -> None:
    assert SANITYCLOUD_HQ.name == "SanityCloud-HQ (Deprecated)"
    assert SANITYCLOUD_HQ.teamspace_name == "Sanity-Cloud-InScene-Deprecated"
    assert SANITYCLOUD_HQ.contract.teamspace_id == (
        "3aabf4af-15b3-810f-a1e8-004254c8eb80"
    )
    assert SANITYCLOUD_HQ.authoritative is False
    assert SANITYCLOUD_HQ.deprecated is True


def test_deprecated_hq_requires_explicit_opt_in(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    monkeypatch.setenv(
        "SANITYCLOUD_ENABLED_WORKSPACES",
        "sanity-management,sanitycloud-hq",
    )

    assert configured_workspace_definitions() == (
        SANITY_MANAGEMENT,
        SANITYCLOUD_HQ,
    )
    assert resolve_workspace_definition("sanitycloud-hq") is SANITYCLOUD_HQ
    assert resolve_workspace_definition(
        "034bf4af-15b3-81a9-a6ce-000330e15c65"
    ) is SANITYCLOUD_HQ


def test_ambiguous_hq_shortcuts_are_not_routing_selectors() -> None:
    for selector in ("hq", "sanity-hq", "sanityhq"):
        with pytest.raises(ValueError, match="Unknown Notion workspace selector"):
            resolve_workspace_definition(selector)


def test_deprecated_hq_cannot_be_selected_as_default(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    monkeypatch.setenv(
        "SANITYCLOUD_ENABLED_WORKSPACES",
        "sanity-management,sanitycloud-hq",
    )
    monkeypatch.setenv("SANITYCLOUD_DEFAULT_WORKSPACE", "sanitycloud-hq")

    with pytest.raises(ValueError, match="cannot be the default workspace"):
        default_workspace_definition()


def test_default_account_expansion_uses_only_authoritative_scope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _clear_workspace_env(monkeypatch)
    accounts = [
        {
            "profile_name": "primary",
            "user_id": "user-1",
        }
    ]

    expanded = expand_accounts_for_workspaces(accounts)

    assert len(expanded) == 1
    assert expanded[0]["workspace_key"] == "sanity-monkey"
    assert expanded[0]["workspace_name"] == "Sanity Monkey"
    assert expanded[0]["teamspace_name"] == "Sanity-Monk"
    assert expanded[0]["space_id"] == "d6b4f527-e627-8130-9f4b-0003742d8be6"


def test_sanity_monkey_contract_uses_verified_destination_ids() -> None:
    assert SANITY_MONKEY.name == "Sanity Monkey"
    assert SANITY_MONKEY.teamspace_name == "Sanity-Monk"
    assert SANITY_MONKEY.contract.workspace_id == "d6b4f527-e627-8130-9f4b-0003742d8be6"
    assert SANITY_MONKEY.contract.teamspace_id == "3d14f527-e627-8141-ad57-0042fe5872c1"
    assert SANITY_MONKEY.contract.authority_page_id == "9444f527-e627-82aa-a146-81b63c534c18"


def test_account_workspace_keys_limit_expansion(monkeypatch: pytest.MonkeyPatch) -> None:
    _clear_workspace_env(monkeypatch)
    monkeypatch.setenv("SANITYCLOUD_ENABLED_WORKSPACES", "sanity-management,sanity-monkey")
    accounts = [
        {"profile_name": "management", "user_id": "user-1", "workspace_keys": ["sanity-management"]},
        {"profile_name": "monkey", "user_id": "user-2", "workspace_keys": ["sanity-monkey"]},
    ]

    expanded = expand_accounts_for_workspaces(accounts)

    assert [(item["profile_name"], item["workspace_key"]) for item in expanded] == [
        ("management", "sanity-management"),
        ("monkey", "sanity-monkey"),
    ]
