from __future__ import annotations

import pytest

from app.file_discovery_routing import (
    DESKTOP_SEARCH_TOOL,
    EVERYTHING_TOOL,
    FileOperationIntent,
    enforce_dispatch_file_route,
    is_sensitive_filename,
    load_file_discovery_profile,
    route_file_operation,
)


def test_profile_covers_important_roots_and_file_types():
    profile = load_file_discovery_profile()
    root_ids = {root.id for root in profile.roots}
    assert {
        "code",
        "mcp",
        "documents",
        "downloads",
        "desktop",
        "sanitycloud-local",
    }.issubset(root_ids)
    extensions = {
        ext
        for group in profile.default_file_groups
        for ext in profile.file_groups[group]
    }
    assert {"py", "ps1", "json", "md", "pdf", "docx", "xlsx", "har", "sqlite3", "zip"}.issubset(extensions)


def test_discovery_routes_only_through_everything_with_bounded_batches():
    decision = route_file_operation(intent="discover", search_text="portal")
    assert decision.allowed is True
    assert decision.primary_tool == EVERYTHING_TOOL
    assert DESKTOP_SEARCH_TOOL in decision.denied_tools
    assert decision.permitted_tools == [EVERYTHING_TOOL]
    assert len(decision.search_batches) == 6
    assert sum(batch.count for batch in decision.search_batches) <= 500
    assert all(batch.path_search for batch in decision.search_batches)
    assert all("ext:" in batch.query for batch in decision.search_batches)
    assert all("!path:.git" in batch.query for batch in decision.search_batches)
    assert decision.result_verification_required is True
    assert "DesktopCommander.read_file" in decision.verification_tools
    assert any(batch.path.lower().endswith("\\code") for batch in decision.search_batches)
    assert {batch.root_id for batch in decision.search_batches} == {
        "code",
        "mcp",
        "documents",
        "downloads",
        "desktop",
        "sanitycloud-local",
    }


def test_requested_root_and_file_types_are_applied_exactly():
    decision = route_file_operation(
        intent="resolve_path",
        search_text="mission",
        requested_roots=["code"],
        requested_extensions=[".py", "JSON", "py"],
    )
    assert len(decision.search_batches) == 1
    assert decision.search_batches[0].root_id == "code"
    query = decision.search_batches[0].query
    assert query.startswith("mission ext:json;py")
    assert "!path:.git" in query
    assert "!path:node_modules" in query
    assert decision.requested_extensions == ["json", "py"]


def test_subpath_is_allowed_but_outside_profile_is_rejected():
    allowed = route_file_operation(
        intent="enumerate",
        requested_roots=[r"X:\Code\notion2api-hive"],
    )
    assert allowed.search_batches[0].path.lower().endswith("notion2api-hive")
    with pytest.raises(ValueError, match="outside the configured coverage profile"):
        route_file_operation(intent="discover", requested_roots=[r"C:\Windows"])


def test_known_file_operations_do_not_authorize_discovery():
    read = route_file_operation(intent=FileOperationIntent.READ_KNOWN.value)
    write = route_file_operation(intent=FileOperationIntent.WRITE_KNOWN.value)
    assert read.primary_tool == "DesktopCommander.read_file"
    assert write.primary_tool == "DesktopCommander.edit_block"
    assert read.search_batches == write.search_batches == []
    assert DESKTOP_SEARCH_TOOL in read.denied_tools
    assert DESKTOP_SEARCH_TOOL in write.denied_tools


def test_degraded_fallback_requires_provider_failure_explicit_gate_and_a3():
    blocked = route_file_operation(
        intent="discover",
        everything_available=False,
        degraded_mode_authorized=True,
        authority_ceiling="A2",
    )
    assert blocked.allowed is False
    assert blocked.error == "FILE_DISCOVERY_PROVIDER_UNAVAILABLE"
    assert blocked.authority_required == "A3"

    allowed = route_file_operation(
        intent="discover",
        everything_available=False,
        degraded_mode_authorized=True,
        authority_ceiling="A3",
    )
    assert allowed.allowed is True
    assert allowed.primary_tool == DESKTOP_SEARCH_TOOL
    assert allowed.fallback_authorized is True


def test_sensitive_names_are_classified_without_opening_them():
    assert is_sensitive_filename(".env") is True
    assert is_sensitive_filename("credentials-prod.json") is True
    assert is_sensitive_filename("service.key") is True
    assert is_sensitive_filename("README.md") is False


def test_content_search_requires_everything_resolution_first():
    decision = route_file_operation(intent="content_search", requested_roots=["code"])
    assert decision.allowed is True
    assert decision.prerequisite_tools == [EVERYTHING_TOOL]
    assert EVERYTHING_TOOL in decision.permitted_tools
    assert DESKTOP_SEARCH_TOOL in decision.denied_tools
    assert len(decision.search_batches) == 1
    assert decision.search_batches[0].root_id == "code"


def test_dispatch_enforcement_denies_desktopcommander_discovery_bypass():
    result = enforce_dispatch_file_route(
        plan_request={
            "file_search_text": "portal",
            "file_search_roots": ["code"],
            "file_types": ["py"],
            "everything_available": True,
            "authority_ceiling": "A3",
        },
        adapter_id="desktop-commander",
        implementation_id="desktopcommander.start_search",
        display_name="DesktopCommander Search",
        requested_capability="file_search",
        payload={"query": "portal", "path": r"X:\Code"},
    )
    assert result.applies is True
    assert result.allowed is False
    assert result.actual_tool == DESKTOP_SEARCH_TOOL
    assert result.routing_decision is not None
    assert result.routing_decision.primary_tool == EVERYTHING_TOOL
    assert result.error.startswith("FILE_DISCOVERY_ROUTE_DENIED")


def test_dispatch_enforcement_accepts_everything_discovery():
    result = enforce_dispatch_file_route(
        plan_request={
            "file_search_text": "portal",
            "file_search_roots": ["code"],
            "file_types": ["py"],
            "everything_available": True,
            "authority_ceiling": "A2",
        },
        adapter_id="everything-mcp",
        implementation_id="Everything_MCP.search_files",
        display_name="Everything Search",
        requested_capability="search_files",
        payload={"query": "portal ext:py", "path": r"X:\Code"},
    )
    assert result.applies is True
    assert result.allowed is True
    assert result.actual_tool == EVERYTHING_TOOL
    assert result.error == ""


def test_dispatch_enforcement_allows_authorized_degraded_search_only_at_a3():
    result = enforce_dispatch_file_route(
        plan_request={
            "everything_available": False,
            "degraded_search_authorized": True,
            "authority_ceiling": "A3",
        },
        adapter_id="desktop-commander",
        implementation_id="desktopcommander.start_search",
        display_name="DesktopCommander Search",
        requested_capability="find_files",
        payload={"query": "*.py"},
    )
    assert result.applies is True
    assert result.allowed is True
    assert result.actual_tool == DESKTOP_SEARCH_TOOL
    assert result.routing_decision is not None
    assert result.routing_decision.fallback_authorized is True


def test_dispatch_enforcement_preserves_known_file_read():
    result = enforce_dispatch_file_route(
        plan_request={"authority_ceiling": "A1"},
        adapter_id="desktop-commander",
        implementation_id="desktopcommander.read_file",
        display_name="DesktopCommander Read File",
        requested_capability="read_file",
        payload={"path": r"X:\Code\notion2api-hive\README.md"},
    )
    assert result.applies is True
    assert result.allowed is True
    assert result.actual_tool == "DesktopCommander.read_file"
    assert result.routing_decision is not None
    assert result.routing_decision.intent == "read_known"


def test_dispatch_enforcement_denies_unclassified_tool_for_discovery():
    result = enforce_dispatch_file_route(
        plan_request={"everything_available": True, "authority_ceiling": "A3"},
        adapter_id="generic-tool",
        implementation_id="generic.file_scan",
        display_name="Generic File Scanner",
        requested_capability="filesystem_search",
        payload={"query": "secrets"},
    )
    assert result.applies is True
    assert result.allowed is False
    assert result.actual_tool == ""
    assert result.error.startswith("FILE_DISCOVERY_ROUTE_DENIED")


def test_content_search_blocks_without_everything_or_governed_fallback():
    blocked = route_file_operation(
        intent="content_search",
        requested_roots=["code"],
        everything_available=False,
        degraded_mode_authorized=False,
        authority_ceiling="A3",
    )
    assert blocked.allowed is False
    assert blocked.error == "FILE_DISCOVERY_PROVIDER_UNAVAILABLE"
    assert blocked.primary_tool == EVERYTHING_TOOL

    fallback = route_file_operation(
        intent="content_search",
        requested_roots=["code"],
        everything_available=False,
        degraded_mode_authorized=True,
        authority_ceiling="A3",
    )
    assert fallback.allowed is True
    assert fallback.primary_tool == "RepoAI.repository_search"
    assert fallback.prerequisite_tools == [DESKTOP_SEARCH_TOOL]
    assert fallback.fallback_authorized is True
