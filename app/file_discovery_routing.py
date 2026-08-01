from __future__ import annotations

import fnmatch
import json
import os
from enum import Enum
from pathlib import Path, PureWindowsPath
from typing import Any

from pydantic import BaseModel, Field

DEFAULT_PROFILE_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "file-discovery-profile.json"
)
EVERYTHING_TOOL = "Everything_MCP.search_files"
DESKTOP_SEARCH_TOOL = "DesktopCommander.start_search"


class FileOperationIntent(str, Enum):
    DISCOVER = "discover"
    RESOLVE_PATH = "resolve_path"
    ENUMERATE = "enumerate"
    CONTENT_SEARCH = "content_search"
    READ_KNOWN = "read_known"
    WRITE_KNOWN = "write_known"
    PROCESS_INSPECTION = "process_inspection"
    EXECUTE_KNOWN = "execute_known"


class SearchRoot(BaseModel):
    id: str
    path: str
    purpose: str = ""


class FileDiscoveryProfile(BaseModel):
    schema_version: str = "1.0"
    provider: str = EVERYTHING_TOOL
    default_root_ids: list[str] = Field(default_factory=list)
    roots: list[SearchRoot] = Field(default_factory=list)
    default_file_groups: list[str] = Field(default_factory=list)
    file_groups: dict[str, list[str]] = Field(default_factory=dict)
    excluded_directory_names: list[str] = Field(default_factory=list)
    sensitive_name_patterns: list[str] = Field(default_factory=list)
    max_results_per_root: int = 100
    max_roots_per_request: int = 8
    max_total_results: int = 500
    fallback: dict[str, Any] = Field(default_factory=dict)


class EverythingSearchBatch(BaseModel):
    root_id: str
    path: str
    query: str
    count: int
    path_search: bool = True
    excluded_directory_names: list[str] = Field(default_factory=list)


class FileRoutingDecision(BaseModel):
    ok: bool = True
    intent: str
    allowed: bool
    primary_tool: str = ""
    prerequisite_tools: list[str] = Field(default_factory=list)
    permitted_tools: list[str] = Field(default_factory=list)
    denied_tools: list[str] = Field(default_factory=list)
    verification_tools: list[str] = Field(default_factory=list)
    result_verification_required: bool = False
    search_batches: list[EverythingSearchBatch] = Field(default_factory=list)
    requested_extensions: list[str] = Field(default_factory=list)
    sensitive_name_patterns: list[str] = Field(default_factory=list)
    fallback_authorized: bool = False
    fallback_tool: str = ""
    authority_required: str = "A0"
    reasons: list[str] = Field(default_factory=list)
    error: str = ""


def load_file_discovery_profile(
    path: str | Path | None = None,
) -> FileDiscoveryProfile:
    resolved = Path(path or DEFAULT_PROFILE_PATH).expanduser().resolve()
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    profile = FileDiscoveryProfile.model_validate(payload)
    if profile.provider != EVERYTHING_TOOL:
        raise ValueError("Everything_MCP.search_files must remain the primary provider")
    return profile


def _expand_path(value: str) -> str:
    return os.path.expandvars(os.path.expanduser(str(value))).rstrip("\\/")


def _windows_key(value: str) -> str:
    return str(PureWindowsPath(_expand_path(value))).lower()


def _is_within(candidate: str, root: str) -> bool:
    candidate_key = _windows_key(candidate)
    root_key = _windows_key(root)
    return candidate_key == root_key or candidate_key.startswith(root_key + "\\")


def _normalize_extensions(values: list[str] | None) -> list[str]:
    return sorted(
        {
            str(value).strip().lower().lstrip(".")
            for value in (values or [])
            if str(value).strip()
        }
    )


def _profile_extensions(profile: FileDiscoveryProfile) -> list[str]:
    return _normalize_extensions(
        [
            extension
            for group in profile.default_file_groups
            for extension in profile.file_groups.get(group, [])
        ]
    )


def _resolve_roots(
    profile: FileDiscoveryProfile,
    requested_roots: list[str] | None,
) -> list[SearchRoot]:
    configured = {root.id: root for root in profile.roots}
    requested = requested_roots or profile.default_root_ids
    resolved: list[SearchRoot] = []
    for value in requested:
        text = str(value).strip()
        if not text:
            continue
        if text in configured:
            root = configured[text]
            resolved.append(
                SearchRoot(id=root.id, path=_expand_path(root.path), purpose=root.purpose)
            )
            continue
        containing = next(
            (root for root in profile.roots if _is_within(text, root.path)),
            None,
        )
        if containing is None:
            raise ValueError(f"Search root is outside the configured coverage profile: {text}")
        resolved.append(SearchRoot(id=containing.id, path=_expand_path(text), purpose=containing.purpose))
    unique = {(_windows_key(root.path), root.id): root for root in resolved}
    roots = list(unique.values())
    if len(roots) > profile.max_roots_per_request:
        raise ValueError("Search root count exceeds the configured maximum")
    return roots


def is_sensitive_filename(
    filename: str,
    profile: FileDiscoveryProfile | None = None,
) -> bool:
    active = profile or load_file_discovery_profile()
    name = Path(str(filename)).name.lower()
    return any(fnmatch.fnmatch(name, pattern.lower()) for pattern in active.sensitive_name_patterns)


def _build_everything_query(
    search_text: str,
    extensions: list[str],
    excluded_directory_names: list[str],
) -> str:
    parts = [str(search_text or "*").strip() or "*"]
    if extensions:
        parts.append(f"ext:{';'.join(extensions)}")
    parts.extend(
        f"!path:{name}"
        for name in excluded_directory_names
        if str(name).strip()
    )
    return " ".join(parts)


def route_file_operation(
    *,
    intent: str,
    search_text: str = "",
    requested_roots: list[str] | None = None,
    requested_extensions: list[str] | None = None,
    everything_available: bool = True,
    degraded_mode_authorized: bool = False,
    authority_ceiling: str = "A0",
    profile: FileDiscoveryProfile | None = None,
) -> FileRoutingDecision:
    active = profile or load_file_discovery_profile()
    operation = FileOperationIntent(str(intent).strip().lower())
    authority = str(authority_ceiling or "A0").strip().upper()

    if operation in {
        FileOperationIntent.READ_KNOWN,
        FileOperationIntent.WRITE_KNOWN,
        FileOperationIntent.PROCESS_INSPECTION,
        FileOperationIntent.EXECUTE_KNOWN,
    }:
        mapping = {
            FileOperationIntent.READ_KNOWN: [
                "DesktopCommander.read_file",
                "DesktopCommander.export_project_file",
            ],
            FileOperationIntent.WRITE_KNOWN: [
                "DesktopCommander.edit_block",
                "DesktopCommander.write_file",
            ],
            FileOperationIntent.PROCESS_INSPECTION: [
                "DesktopCommander.start_process",
            ],
            FileOperationIntent.EXECUTE_KNOWN: [
                "DesktopCommander.start_process",
            ],
        }
        tools = mapping[operation]
        return FileRoutingDecision(
            intent=operation.value,
            allowed=True,
            primary_tool=tools[0],
            permitted_tools=tools,
            denied_tools=[DESKTOP_SEARCH_TOOL],
            sensitive_name_patterns=active.sensitive_name_patterns,
            reasons=["The target path is already known; no filesystem discovery is authorized."],
        )

    extensions = _normalize_extensions(requested_extensions) or _profile_extensions(active)
    roots = _resolve_roots(active, requested_roots)
    fallback = active.fallback
    required_authority = str(fallback.get("minimum_authority") or "A3").upper()
    rank = {f"A{level}": level for level in range(5)}
    fallback_allowed = (
        bool(degraded_mode_authorized)
        and rank.get(authority, -1) >= rank.get(required_authority, 99)
    )
    if operation == FileOperationIntent.CONTENT_SEARCH:
        content_count = min(
            active.max_results_per_root,
            max(1, active.max_total_results // max(1, len(roots))),
        )
        content_batches = [
            EverythingSearchBatch(
                root_id=root.id,
                path=root.path,
                query=_build_everything_query(
                    search_text,
                    extensions,
                    active.excluded_directory_names,
                ),
                count=content_count,
                excluded_directory_names=active.excluded_directory_names,
            )
            for root in roots
        ]
        if everything_available:
            return FileRoutingDecision(
                intent=operation.value,
                allowed=True,
                primary_tool="RepoAI.repository_search",
                prerequisite_tools=[EVERYTHING_TOOL],
                permitted_tools=[EVERYTHING_TOOL, "RepoAI.repository_search"],
                denied_tools=[DESKTOP_SEARCH_TOOL],
                verification_tools=[
                    "DesktopCommander.read_file",
                    "DesktopCommander.export_project_file",
                ],
                result_verification_required=True,
                search_batches=content_batches,
                requested_extensions=extensions,
                sensitive_name_patterns=active.sensitive_name_patterns,
                reasons=[
                    "Resolve the repository or file set with Everything before bounded content search.",
                    "Specialized repository search may inspect content only inside the resolved boundary.",
                    "Everything index metadata is discovery evidence; verify the selected file before mutation.",
                ],
            )
        if fallback_allowed:
            return FileRoutingDecision(
                intent=operation.value,
                allowed=True,
                primary_tool="RepoAI.repository_search",
                prerequisite_tools=[DESKTOP_SEARCH_TOOL],
                permitted_tools=[DESKTOP_SEARCH_TOOL, "RepoAI.repository_search"],
                verification_tools=[
                    "DesktopCommander.read_file",
                    "DesktopCommander.export_project_file",
                ],
                result_verification_required=True,
                search_batches=content_batches,
                requested_extensions=extensions,
                sensitive_name_patterns=active.sensitive_name_patterns,
                fallback_authorized=True,
                fallback_tool=DESKTOP_SEARCH_TOOL,
                authority_required=required_authority,
                reasons=[
                    "Everything is unavailable; a governed A3 degraded discovery prerequisite was authorized.",
                    "RepoAI content search remains bounded to the resolved candidate set.",
                ],
            )
        return FileRoutingDecision(
            intent=operation.value,
            allowed=False,
            primary_tool=EVERYTHING_TOOL,
            permitted_tools=[],
            denied_tools=[DESKTOP_SEARCH_TOOL],
            search_batches=content_batches,
            requested_extensions=extensions,
            sensitive_name_patterns=active.sensitive_name_patterns,
            fallback_tool=DESKTOP_SEARCH_TOOL,
            authority_required=required_authority,
            reasons=[
                "Content search requires Everything candidate resolution.",
                "The governed degraded discovery fallback is not authorized.",
            ],
            error="FILE_DISCOVERY_PROVIDER_UNAVAILABLE",
        )

    per_root_count = min(
        active.max_results_per_root,
        max(1, active.max_total_results // max(1, len(roots))),
    )
    batches = [
        EverythingSearchBatch(
            root_id=root.id,
            path=root.path,
            query=_build_everything_query(
                search_text,
                extensions,
                active.excluded_directory_names,
            ),
            count=per_root_count,
            excluded_directory_names=active.excluded_directory_names,
        )
        for root in roots
    ]

    if everything_available:
        return FileRoutingDecision(
            intent=operation.value,
            allowed=True,
            primary_tool=EVERYTHING_TOOL,
            permitted_tools=[EVERYTHING_TOOL],
            denied_tools=[DESKTOP_SEARCH_TOOL],
            verification_tools=[
                "DesktopCommander.read_file",
                "DesktopCommander.export_project_file",
            ],
            result_verification_required=True,
            search_batches=batches,
            requested_extensions=extensions,
            sensitive_name_patterns=active.sensitive_name_patterns,
            reasons=[
                "Filesystem discovery and path resolution are reserved to Everything_MCP.",
                "Each configured root is searched as a separate bounded batch.",
                "Everything index metadata is discovery evidence; verify the selected file before mutation.",
            ],
        )

    if fallback_allowed:
        return FileRoutingDecision(
            intent=operation.value,
            allowed=True,
            primary_tool=DESKTOP_SEARCH_TOOL,
            permitted_tools=[DESKTOP_SEARCH_TOOL],
            verification_tools=[
                "DesktopCommander.read_file",
                "DesktopCommander.export_project_file",
            ],
            result_verification_required=True,
            search_batches=batches,
            requested_extensions=extensions,
            sensitive_name_patterns=active.sensitive_name_patterns,
            fallback_authorized=True,
            fallback_tool=DESKTOP_SEARCH_TOOL,
            authority_required=required_authority,
            reasons=["Everything is unavailable and an explicit governed degraded-mode authorization was supplied."],
        )

    return FileRoutingDecision(
        intent=operation.value,
        allowed=False,
        primary_tool=EVERYTHING_TOOL,
        permitted_tools=[],
        denied_tools=[DESKTOP_SEARCH_TOOL],
        search_batches=batches,
        requested_extensions=extensions,
        sensitive_name_patterns=active.sensitive_name_patterns,
        fallback_authorized=False,
        fallback_tool=DESKTOP_SEARCH_TOOL,
        authority_required=required_authority,
        reasons=[
            "Everything_MCP is unavailable.",
            "DesktopCommander discovery requires an explicit degraded-mode authorization at or above the configured authority ceiling.",
        ],
        error="FILE_DISCOVERY_PROVIDER_UNAVAILABLE",
    )


class FileDispatchEnforcement(BaseModel):
    applies: bool = False
    allowed: bool = True
    actual_tool: str = ""
    expected_tools: list[str] = Field(default_factory=list)
    routing_decision: FileRoutingDecision | None = None
    error: str = ""


_DISCOVERY_CAPABILITIES = frozenset(
    {
        "discover",
        "discover_files",
        "enumerate",
        "enumerate_files",
        "file_discovery",
        "file_search",
        "filesystem_search",
        "find_files",
        "path_resolution",
        "resolve_path",
        "search_files",
    }
)


def _payload_strings(value: Any) -> list[str]:
    if isinstance(value, dict):
        return [item for child in value.values() for item in _payload_strings(child)]
    if isinstance(value, (list, tuple, set)):
        return [item for child in value for item in _payload_strings(child)]
    if isinstance(value, str):
        return [value]
    return []


def _actual_file_tool(
    *,
    adapter_id: str,
    implementation_id: str,
    display_name: str,
    requested_capability: str,
    payload: dict[str, Any],
) -> str:
    text = " ".join(
        [adapter_id, implementation_id, display_name, requested_capability]
        + _payload_strings(payload)
    ).lower()
    if "everything" in text and ("search" in text or "find" in text or "discover" in text):
        return EVERYTHING_TOOL
    if (
        any(token in text for token in ("desktopcommander", "desktop_commander", "desktop-commander"))
        and any(token in text for token in ("start_search", "search_files", "find_files", "file_search"))
    ):
        return DESKTOP_SEARCH_TOOL
    if "repoai" in text and "search" in text:
        return "RepoAI.repository_search"
    if "desktopcommander" in text or "desktop_commander" in text or "desktop-commander" in text:
        if "read_file" in text or "export_project_file" in text:
            return "DesktopCommander.read_file"
        if "edit_block" in text or "write_file" in text:
            return "DesktopCommander.edit_block"
        if "start_process" in text:
            return "DesktopCommander.start_process"
    return ""


def _dispatch_intent(requested_capability: str, actual_tool: str) -> str | None:
    capability = str(requested_capability or "").strip().lower()
    if capability in _DISCOVERY_CAPABILITIES:
        return "discover"
    if capability == "content_search" or actual_tool == "RepoAI.repository_search":
        return "content_search"
    if actual_tool == "DesktopCommander.read_file":
        return "read_known"
    if actual_tool == "DesktopCommander.edit_block":
        return "write_known"
    if actual_tool == "DesktopCommander.start_process":
        return "execute_known"
    return None


def enforce_dispatch_file_route(
    *,
    plan_request: dict[str, Any],
    adapter_id: str,
    implementation_id: str,
    display_name: str,
    requested_capability: str,
    payload: dict[str, Any],
) -> FileDispatchEnforcement:
    actual_tool = _actual_file_tool(
        adapter_id=adapter_id,
        implementation_id=implementation_id,
        display_name=display_name,
        requested_capability=requested_capability,
        payload=payload,
    )
    intent = _dispatch_intent(requested_capability, actual_tool)
    if intent is None:
        return FileDispatchEnforcement(applies=False, allowed=True)
    decision = route_file_operation(
        intent=intent,
        search_text=str(plan_request.get("file_search_text") or ""),
        requested_roots=plan_request.get("file_search_roots") or None,
        requested_extensions=plan_request.get("file_types") or None,
        everything_available=bool(plan_request.get("everything_available", True)),
        degraded_mode_authorized=bool(plan_request.get("degraded_search_authorized", False)),
        authority_ceiling=str(plan_request.get("authority_ceiling") or "A0"),
    )
    expected = list(dict.fromkeys([decision.primary_tool, *decision.permitted_tools]))
    if actual_tool == EVERYTHING_TOOL:
        allowed = True
    else:
        allowed = bool(actual_tool and actual_tool in expected and decision.allowed)
    error = ""
    if not allowed:
        error = (
            "FILE_DISCOVERY_ROUTE_DENIED: filesystem discovery and path resolution "
            "must use Everything_MCP unless a governed degraded fallback is active."
        )
    return FileDispatchEnforcement(
        applies=True,
        allowed=allowed,
        actual_tool=actual_tool,
        expected_tools=expected,
        routing_decision=decision,
        error=error,
    )
