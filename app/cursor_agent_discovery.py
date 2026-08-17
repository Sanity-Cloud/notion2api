"""HAR-backed Cursor agent discovery helpers with mandatory secret redaction.

The HAR may contain credential material. Importers must never persist or return
raw Cursor API keys, cookies, or authorization headers.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

from app.bitwarden_secrets import redact_secret_fields
from app.cursor_agent_registry import CursorAgentRegistry


_SECRET_PATTERNS = (
    re.compile(r"(?i)(authorization\s*[:=]\s*)(\S+)"),
    re.compile(r'(?i)("?cursor_?api_?key"?\s*[:=]\s*"?)([^"\s,}]+)'),
    re.compile(r"(?i)(cookie\s*[:=]\s*)([^\n]+)"),
    re.compile(r'(?i)("?token_v2"?\s*[:=]\s*"?)([^"\s,}]+)'),
    re.compile(r"\bkey_[A-Za-z0-9_]{8,}\b"),
    re.compile(r"(?i)\bbearer\s+[A-Za-z0-9\-._~+/]+=*"),
)


def redact_har_text(text: str) -> str:
    cleaned = str(text or "")
    for pattern in _SECRET_PATTERNS:
        if pattern.groups >= 2:
            cleaned = pattern.sub(lambda match: f"{match.group(1)}[redacted]", cleaned)
        else:
            cleaned = pattern.sub("[redacted]", cleaned)
    return cleaned


def _walk_json(value: Any) -> Any:
    return redact_secret_fields(value)


def extract_cursor_workflows_from_har(har_payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Extract sanitized Cursor workflow evidence from a HAR object."""

    entries = []
    log = har_payload.get("log") if isinstance(har_payload, dict) else None
    if isinstance(log, dict) and isinstance(log.get("entries"), list):
        entries = log["entries"]

    discovered: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        request = entry.get("request") if isinstance(entry.get("request"), dict) else {}
        response = entry.get("response") if isinstance(entry.get("response"), dict) else {}
        url = str(request.get("url") or "")
        interesting = any(
            token in url
            for token in (
                "getAvailableExternalAgents",
                "postWorkflowsCursorAgentConnect",
                "listCursorAgentRepos",
                "externalAgentHarness",
                "cursorAgent",
            )
        )
        content = response.get("content") if isinstance(response.get("content"), dict) else {}
        text = str(content.get("text") or "")
        if not interesting and "cursor_agent" not in text and "externalAgentHarness" not in text:
            continue
        parsed: Any = None
        if text:
            try:
                parsed = json.loads(text)
            except json.JSONDecodeError:
                parsed = {"raw_text": redact_har_text(text)[:4000]}
        sanitized = _walk_json(parsed) if parsed is not None else {}
        workflow_id = ""
        connection_id = ""
        repos: list[str] = []
        if isinstance(sanitized, dict):
            workflow_id = str(
                sanitized.get("workflowId")
                or sanitized.get("workflow_id")
                or sanitized.get("id")
                or ""
            ).strip()
            connection_id = str(
                sanitized.get("connectionId")
                or sanitized.get("connection_id")
                or ""
            ).strip()
            for key in ("repositories", "repos", "allowedRepositories"):
                raw_repos = sanitized.get(key)
                if isinstance(raw_repos, list):
                    repos.extend(str(item).strip() for item in raw_repos if str(item).strip())
            harness = sanitized.get("externalAgentHarness")
            if isinstance(harness, dict):
                if not connection_id:
                    connection_id = str(
                        harness.get("connectionId") or harness.get("id") or ""
                    ).strip()
        discovered.append(
            {
                "source_url": redact_har_text(url)[:500],
                "workflow_id": workflow_id,
                "connection_id": connection_id,
                "allowed_github_repos": repos,
                "payload": sanitized if isinstance(sanitized, dict) else {},
            }
        )
    return discovered


def import_cursor_workflows_from_har_file(
    path: str | Path,
    *,
    account_key: str,
    workspace_id: str,
    registry: CursorAgentRegistry | None = None,
    workspace_key: str = "",
    base_profile_name: str = "",
    cursor_api_key_secret_id: str = "",
) -> list[dict[str, Any]]:
    """Import discovered workflows into the registry without retaining HAR secrets."""

    har_path = Path(path)
    raw = har_path.read_text(encoding="utf-8", errors="replace")
    # Redact before JSON parse where possible, then parse the redacted text.
    redacted_text = redact_har_text(raw)
    payload = json.loads(redacted_text)
    if not isinstance(payload, dict):
        raise ValueError("HAR root must be a JSON object")
    store = registry or CursorAgentRegistry()
    imported: list[dict[str, Any]] = []
    for item in extract_cursor_workflows_from_har(payload):
        workflow_id = str(item.get("workflow_id") or "").strip()
        if not workflow_id:
            continue
        record = store.import_workflow_metadata(
            account_key=account_key,
            workspace_id=workspace_id,
            workspace_key=workspace_key,
            base_profile_name=base_profile_name,
            workflow_id=workflow_id,
            connection_id=str(item.get("connection_id") or ""),
            friendly_name=workflow_id,
            allowed_github_repos=list(item.get("allowed_github_repos") or []),
            cursor_api_key_secret_id=cursor_api_key_secret_id,
            metadata={"source_url": item.get("source_url"), "imported_from_har": True},
        )
        imported.append(record)
    return imported
