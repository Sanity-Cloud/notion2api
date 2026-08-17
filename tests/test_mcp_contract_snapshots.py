from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.generate_mcp_contract_snapshots import build_profile_contract

ROOT = Path(__file__).resolve().parents[1]
SNAPSHOT_DIR = ROOT / "contracts" / "mcp"
EXPECTED_COUNTS = {"notion2api": 57, "aigentbee": 60}
WORKBENCH_SUFFIXES = {
    "show_swarm_workbench",
    "get_swarm_workbench",
    "send_leader_request",
}


@pytest.mark.parametrize("profile", ["notion2api", "aigentbee"])
def test_mcp_profile_contract_matches_reviewed_snapshot(profile: str) -> None:
    snapshot_path = SNAPSHOT_DIR / f"{profile}.json"
    expected = json.loads(snapshot_path.read_text(encoding="utf-8"))
    actual = build_profile_contract(profile)

    assert actual == expected, (
        f"The {profile} MCP contract changed. Review the client-visible schema, then run "
        "python scripts/generate_mcp_contract_snapshots.py only for an intentional change."
    )


@pytest.mark.parametrize("profile,expected_count", EXPECTED_COUNTS.items())
def test_mcp_profile_snapshot_has_expected_tool_count(
    profile: str,
    expected_count: int,
) -> None:
    snapshot = json.loads(
        (SNAPSHOT_DIR / f"{profile}.json").read_text(encoding="utf-8")
    )
    assert snapshot["tool_count"] == expected_count
    assert len(snapshot["tools"]) == expected_count


def test_workbench_tools_remain_profile_specific() -> None:
    plain = build_profile_contract("notion2api")
    aigentbee = build_profile_contract("aigentbee")
    plain_names = {tool["name"] for tool in plain["tools"]}
    aigentbee_names = {tool["name"] for tool in aigentbee["tools"]}

    assert not any(name in plain_names for name in WORKBENCH_SUFFIXES)
    assert {
        f"aigentbee_{suffix}" for suffix in WORKBENCH_SUFFIXES
    }.issubset(aigentbee_names)


def test_snapshots_do_not_capture_account_secrets_or_personal_fields() -> None:
    serialized = "\n".join(
        path.read_text(encoding="utf-8")
        for path in sorted(SNAPSHOT_DIR.glob("*.json"))
    ).lower()
    for forbidden in (
        "test-token",
        "snapshot@example.invalid",
        "test-user",
        "token_v2",
        "user_email",
    ):
        assert forbidden not in serialized


def test_chat_history_annotation_contract_is_explicitly_non_destructive() -> None:
    snapshot = build_profile_contract("notion2api")
    chat_history = next(tool for tool in snapshot["tools"] if tool["name"] == "chat_history")
    annotations = chat_history["annotations"]

    assert annotations["readOnlyHint"] is False
    assert annotations["destructiveHint"] is False
    assert annotations["idempotentHint"] is True
    assert annotations["openWorldHint"] is True
