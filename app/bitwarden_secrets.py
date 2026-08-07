"""Bitwarden Secrets Manager resolver for external-agent credentials.

Runtime must store secret references only. Secret values are resolved ephemerally
immediately before use and never returned through status/MCP/logs.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
from dataclasses import dataclass
from typing import Any


class BitwardenSecretsError(RuntimeError):
    """Raised when Bitwarden secret resolution fails closed."""


@dataclass(frozen=True)
class SecretReference:
    secret_id: str
    project_id: str = ""
    credential_provider: str = "bitwarden_secrets_manager"

    def as_metadata(self) -> dict[str, str]:
        return {
            "credential_provider": self.credential_provider,
            "cursor_api_key_secret_id": self.secret_id,
            "bitwarden_project_id": self.project_id,
        }


def default_bws_executable() -> str:
    configured = str(os.getenv("BWS_EXECUTABLE") or "").strip()
    if configured:
        return configured
    which = shutil.which("bws")
    if which:
        return which
    windows_default = r"X:\Tools\Bitwarden\bin\bws.exe"
    if os.path.exists(windows_default):
        return windows_default
    return "bws"


def access_token_present() -> bool:
    return bool(str(os.getenv("BWS_ACCESS_TOKEN") or "").strip())


def resolve_secret_value(secret_id: str, *, timeout_seconds: float = 20.0) -> str:
    """Resolve one Bitwarden secret value. Fail closed when bootstrap is absent."""

    secret_id = str(secret_id or "").strip()
    if not secret_id:
        raise BitwardenSecretsError("Bitwarden secret_id is required")
    if not access_token_present():
        raise BitwardenSecretsError(
            "BWS_ACCESS_TOKEN is not configured; refusing to resolve secrets"
        )
    executable = default_bws_executable()
    try:
        completed = subprocess.run(
            [executable, "secret", "get", secret_id, "--output", "json"],
            check=False,
            capture_output=True,
            text=True,
            timeout=max(1.0, float(timeout_seconds)),
            env=os.environ.copy(),
        )
    except FileNotFoundError as exc:
        raise BitwardenSecretsError(
            f"Bitwarden CLI not found at {executable!r}"
        ) from exc
    except subprocess.TimeoutExpired as exc:
        raise BitwardenSecretsError("Bitwarden secret resolution timed out") from exc

    if completed.returncode != 0:
        detail = (completed.stderr or completed.stdout or "").strip()[:240]
        raise BitwardenSecretsError(
            f"Bitwarden secret get failed (exit {completed.returncode}): {detail}"
        )
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError as exc:
        raise BitwardenSecretsError(
            "Bitwarden secret get returned non-JSON output"
        ) from exc
    value = ""
    if isinstance(payload, dict):
        value = str(payload.get("value") or payload.get("secret") or "").strip()
    if not value:
        raise BitwardenSecretsError("Bitwarden secret payload did not include a value")
    return value


def suggested_secret_name(
    *,
    workspace_key: str,
    account_alias: str,
    cursor_agent_key: str,
) -> str:
    workspace = str(workspace_key or "workspace").strip().casefold() or "workspace"
    alias = str(account_alias or "account").strip().casefold() or "account"
    agent = str(cursor_agent_key or "agent").strip().casefold() or "agent"
    return f"cursor/{workspace}/{alias}/{agent}/api-key"


def redact_secret_fields(payload: Any) -> Any:
    """Recursively strip known secret value fields from structured payloads."""

    blocked = {
        "cursorapikey",
        "cursor_api_key",
        "api_key",
        "apikey",
        "authorization",
        "token",
        "token_v2",
        "cookie",
        "cookies",
        "secret",
        "secret_value",
        "bws_access_token",
        "access_token",
    }
    if isinstance(payload, dict):
        cleaned: dict[str, Any] = {}
        for key, value in payload.items():
            normalized = str(key or "").strip().casefold().replace("-", "_")
            if normalized in blocked or normalized.endswith("_secret"):
                cleaned[key] = "[redacted]"
            else:
                cleaned[key] = redact_secret_fields(value)
        return cleaned
    if isinstance(payload, list):
        return [redact_secret_fields(item) for item in payload]
    return payload
