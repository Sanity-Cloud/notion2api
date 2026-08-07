"""Account+workspace scoped Cursor custom-agent registry.

Canonical relationship:
`(account_key, workspace_id) -> [cursor_agent_instance, ...]`

Secrets are Bitwarden references only; raw Cursor API keys are never stored.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from typing import Any

from app.bitwarden_secrets import redact_secret_fields


class CursorAgentRegistryError(ValueError):
    """Raised for invalid Cursor agent registry operations."""


def default_cursor_agent_registry_path() -> Path:
    raw = str(os.getenv("CURSOR_AGENT_REGISTRY_DB") or "").strip()
    if raw:
        return Path(raw).expanduser().resolve()
    return Path("data") / "cursor_agent_registry.db"


def _required(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise CursorAgentRegistryError(f"{field_name} is required")
    return text


def _now_ms() -> int:
    return int(time.time() * 1000)


class CursorAgentRegistry:
    def __init__(self, path: str | Path | None = None) -> None:
        self.path = (
            Path(path).expanduser().resolve()
            if path is not None
            else default_cursor_agent_registry_path()
        )
        self._lock = threading.RLock()
        self._ensure_schema()

    def _connect(self) -> sqlite3.Connection:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def _ensure_schema(self) -> None:
        with self._lock:
            with self._connect() as conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS cursor_agents (
                        cursor_agent_key TEXT PRIMARY KEY,
                        account_key TEXT NOT NULL,
                        base_profile_name TEXT NOT NULL DEFAULT '',
                        workspace_key TEXT NOT NULL DEFAULT '',
                        workspace_id TEXT NOT NULL,
                        workflow_id TEXT NOT NULL DEFAULT '',
                        connection_id TEXT NOT NULL DEFAULT '',
                        provider TEXT NOT NULL DEFAULT 'cursor_agent',
                        friendly_name TEXT NOT NULL DEFAULT '',
                        role TEXT NOT NULL DEFAULT '',
                        allowed_repos_json TEXT NOT NULL DEFAULT '[]',
                        enabled INTEGER NOT NULL DEFAULT 1,
                        is_default INTEGER NOT NULL DEFAULT 0,
                        health_state TEXT NOT NULL DEFAULT 'unknown',
                        last_run_id TEXT NOT NULL DEFAULT '',
                        last_activity_at INTEGER NOT NULL DEFAULT 0,
                        last_verified_at INTEGER NOT NULL DEFAULT 0,
                        credential_provider TEXT NOT NULL DEFAULT 'bitwarden_secrets_manager',
                        cursor_api_key_secret_id TEXT NOT NULL DEFAULT '',
                        bitwarden_project_id TEXT NOT NULL DEFAULT '',
                        secret_version TEXT NOT NULL DEFAULT '',
                        secret_last_verified_at INTEGER NOT NULL DEFAULT 0,
                        setup_status TEXT NOT NULL DEFAULT 'unknown',
                        quota_telemetry_json TEXT NOT NULL DEFAULT '{}',
                        metadata_json TEXT NOT NULL DEFAULT '{}',
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_cursor_agents_scope
                        ON cursor_agents(account_key, workspace_id);
                    CREATE INDEX IF NOT EXISTS idx_cursor_agents_workflow
                        ON cursor_agents(workflow_id);
                    """
                )

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict[str, Any]:
        payload = {key: row[key] for key in row.keys()}
        for json_field, default in (
            ("allowed_repos_json", []),
            ("quota_telemetry_json", {}),
            ("metadata_json", {}),
        ):
            raw = payload.pop(json_field)
            try:
                parsed = json.loads(raw or ("[]" if default == [] else "{}"))
            except json.JSONDecodeError:
                parsed = default
            if json_field == "allowed_repos_json":
                payload["allowed_github_repos"] = (
                    list(parsed) if isinstance(parsed, list) else []
                )
            elif json_field == "quota_telemetry_json":
                payload["quota_telemetry"] = (
                    dict(parsed) if isinstance(parsed, dict) else {}
                )
            else:
                payload["metadata"] = dict(parsed) if isinstance(parsed, dict) else {}
        payload["enabled"] = bool(payload.get("enabled"))
        payload["is_default"] = bool(payload.get("is_default"))
        # Never expose raw secrets even if a legacy row somehow contained one.
        return redact_secret_fields(payload)

    def upsert_agent(
        self,
        *,
        account_key: str,
        workspace_id: str,
        cursor_agent_key: str | None = None,
        base_profile_name: str = "",
        workspace_key: str = "",
        workflow_id: str = "",
        connection_id: str = "",
        friendly_name: str = "",
        role: str = "",
        allowed_github_repos: list[str] | None = None,
        enabled: bool = True,
        is_default: bool = False,
        health_state: str = "unknown",
        setup_status: str = "unknown",
        cursor_api_key_secret_id: str = "",
        bitwarden_project_id: str = "",
        secret_version: str = "",
        quota_telemetry: dict[str, Any] | None = None,
        metadata: dict[str, Any] | None = None,
        last_run_id: str = "",
        last_activity_at: int = 0,
        last_verified_at: int = 0,
    ) -> dict[str, Any]:
        account = _required(account_key, "account_key")
        workspace = _required(workspace_id, "workspace_id")
        agent_key = str(cursor_agent_key or "").strip() or f"cursor-{uuid.uuid4().hex[:12]}"
        now = _now_ms()
        repos = [
            str(item).strip()
            for item in (allowed_github_repos or [])
            if str(item).strip()
        ]
        with self._lock:
            with self._connect() as conn:
                if is_default:
                    conn.execute(
                        """
                        UPDATE cursor_agents
                        SET is_default = 0, updated_at = ?
                        WHERE account_key = ? AND workspace_id = ?
                        """,
                        (now, account, workspace),
                    )
                conn.execute(
                    """
                    INSERT INTO cursor_agents(
                        cursor_agent_key, account_key, base_profile_name, workspace_key,
                        workspace_id, workflow_id, connection_id, provider, friendly_name,
                        role, allowed_repos_json, enabled, is_default, health_state,
                        last_run_id, last_activity_at, last_verified_at,
                        credential_provider, cursor_api_key_secret_id, bitwarden_project_id,
                        secret_version, secret_last_verified_at, setup_status,
                        quota_telemetry_json, metadata_json, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'cursor_agent', ?, ?, ?, ?, ?, ?, ?, ?, ?,
                              'bitwarden_secrets_manager', ?, ?, ?, 0, ?, ?, ?, ?, ?)
                    ON CONFLICT(cursor_agent_key) DO UPDATE SET
                        account_key = excluded.account_key,
                        base_profile_name = excluded.base_profile_name,
                        workspace_key = excluded.workspace_key,
                        workspace_id = excluded.workspace_id,
                        workflow_id = excluded.workflow_id,
                        connection_id = excluded.connection_id,
                        friendly_name = excluded.friendly_name,
                        role = excluded.role,
                        allowed_repos_json = excluded.allowed_repos_json,
                        enabled = excluded.enabled,
                        is_default = excluded.is_default,
                        health_state = excluded.health_state,
                        last_run_id = excluded.last_run_id,
                        last_activity_at = excluded.last_activity_at,
                        last_verified_at = excluded.last_verified_at,
                        cursor_api_key_secret_id = excluded.cursor_api_key_secret_id,
                        bitwarden_project_id = excluded.bitwarden_project_id,
                        secret_version = excluded.secret_version,
                        setup_status = excluded.setup_status,
                        quota_telemetry_json = excluded.quota_telemetry_json,
                        metadata_json = excluded.metadata_json,
                        updated_at = excluded.updated_at
                    """,
                    (
                        agent_key,
                        account,
                        str(base_profile_name or "").strip(),
                        str(workspace_key or "").strip(),
                        workspace,
                        str(workflow_id or "").strip(),
                        str(connection_id or "").strip(),
                        str(friendly_name or "").strip() or agent_key,
                        str(role or "").strip(),
                        json.dumps(repos, ensure_ascii=False),
                        1 if enabled else 0,
                        1 if is_default else 0,
                        str(health_state or "unknown").strip() or "unknown",
                        str(last_run_id or "").strip(),
                        int(last_activity_at or 0),
                        int(last_verified_at or 0),
                        str(cursor_api_key_secret_id or "").strip(),
                        str(bitwarden_project_id or "").strip(),
                        str(secret_version or "").strip(),
                        str(setup_status or "unknown").strip() or "unknown",
                        json.dumps(redact_secret_fields(quota_telemetry or {}), ensure_ascii=False),
                        json.dumps(redact_secret_fields(metadata or {}), ensure_ascii=False),
                        now,
                        now,
                    ),
                )
                row = conn.execute(
                    "SELECT * FROM cursor_agents WHERE cursor_agent_key = ?",
                    (agent_key,),
                ).fetchone()
        return self._row_to_dict(row)

    def list_agents(
        self,
        *,
        account_key: str = "",
        workspace_id: str = "",
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[str] = []
        if account_key:
            clauses.append("account_key = ?")
            params.append(str(account_key).strip())
        if workspace_id:
            clauses.append("workspace_id = ?")
            params.append(str(workspace_id).strip())
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        with self._lock:
            with self._connect() as conn:
                rows = conn.execute(
                    f"""
                    SELECT * FROM cursor_agents
                    {where}
                    ORDER BY is_default DESC, friendly_name ASC, cursor_agent_key ASC
                    """,
                    tuple(params),
                ).fetchall()
        return [self._row_to_dict(row) for row in rows]

    def get_agent(self, cursor_agent_key: str) -> dict[str, Any] | None:
        key = str(cursor_agent_key or "").strip()
        if not key:
            return None
        with self._lock:
            with self._connect() as conn:
                row = conn.execute(
                    "SELECT * FROM cursor_agents WHERE cursor_agent_key = ?",
                    (key,),
                ).fetchone()
        return self._row_to_dict(row) if row else None

    def select_agent(
        self,
        *,
        cursor_agent_key: str = "",
        account_key: str = "",
        workspace_id: str = "",
        repository_url: str = "",
    ) -> dict[str, Any]:
        """Selection order from the charter; never cross account/workspace boundaries."""

        if cursor_agent_key:
            agent = self.get_agent(cursor_agent_key)
            if agent is None:
                return {
                    "status": "setup_required",
                    "reason": "explicit_agent_missing",
                    "agent": None,
                }
            if account_key and agent["account_key"] != account_key:
                return {
                    "status": "setup_required",
                    "reason": "explicit_agent_account_mismatch",
                    "agent": None,
                }
            if workspace_id and agent["workspace_id"] != workspace_id:
                return {
                    "status": "setup_required",
                    "reason": "explicit_agent_workspace_mismatch",
                    "agent": None,
                }
            return {"status": "selected", "reason": "explicit_agent", "agent": agent}

        if not account_key or not workspace_id:
            return {
                "status": "setup_required",
                "reason": "account_workspace_required",
                "agent": None,
            }

        scoped = self.list_agents(account_key=account_key, workspace_id=workspace_id)
        if not scoped:
            return {
                "status": "setup_required",
                "reason": "no_agents_for_account_workspace",
                "agent": None,
            }

        healthy = [
            item
            for item in scoped
            if item.get("enabled")
            and str(item.get("health_state") or "").casefold()
            not in {"unhealthy", "disabled", "billing_blocked"}
        ]
        pool = healthy or scoped
        repo = str(repository_url or "").strip().casefold()
        if repo:
            repo_matches = [
                item
                for item in pool
                if any(
                    repo in str(candidate).casefold()
                    for candidate in item.get("allowed_github_repos") or []
                )
            ]
            if repo_matches:
                defaulted = [item for item in repo_matches if item.get("is_default")]
                chosen = defaulted[0] if defaulted else repo_matches[0]
                return {
                    "status": "selected",
                    "reason": "repo_compatible_healthy_agent",
                    "agent": chosen,
                }

        defaulted = [item for item in pool if item.get("is_default")]
        chosen = defaulted[0] if defaulted else pool[0]
        return {
            "status": "selected",
            "reason": "account_workspace_default_or_first",
            "agent": chosen,
        }

    def import_workflow_metadata(
        self,
        *,
        account_key: str,
        workspace_id: str,
        workflow_id: str,
        connection_id: str = "",
        friendly_name: str = "",
        allowed_github_repos: list[str] | None = None,
        workspace_key: str = "",
        base_profile_name: str = "",
        cursor_api_key_secret_id: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Import an existing verified workflow without recreating it or storing secrets."""

        cleaned_metadata = redact_secret_fields(metadata or {})
        if isinstance(cleaned_metadata, dict):
            for blocked in ("cursorApiKey", "cursor_api_key", "authorization"):
                cleaned_metadata.pop(blocked, None)
        return self.upsert_agent(
            account_key=account_key,
            workspace_id=workspace_id,
            workspace_key=workspace_key,
            base_profile_name=base_profile_name,
            workflow_id=workflow_id,
            connection_id=connection_id,
            friendly_name=friendly_name or workflow_id,
            allowed_github_repos=allowed_github_repos,
            cursor_api_key_secret_id=cursor_api_key_secret_id,
            setup_status="imported" if workflow_id else "unknown",
            health_state="unknown",
            metadata=cleaned_metadata if isinstance(cleaned_metadata, dict) else {},
            last_verified_at=_now_ms() if workflow_id and connection_id else 0,
        )


_REGISTRY: CursorAgentRegistry | None = None
_REGISTRY_LOCK = threading.Lock()


def get_cursor_agent_registry() -> CursorAgentRegistry:
    global _REGISTRY
    with _REGISTRY_LOCK:
        if _REGISTRY is None:
            _REGISTRY = CursorAgentRegistry()
        return _REGISTRY
