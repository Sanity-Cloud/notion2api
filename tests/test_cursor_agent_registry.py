"""Cursor agent registry and HAR secret-redaction tests."""

from __future__ import annotations

import json

import pytest

from app.bitwarden_secrets import redact_secret_fields, resolve_secret_value, BitwardenSecretsError
from app.cursor_agent_discovery import (
    extract_cursor_workflows_from_har,
    import_cursor_workflows_from_har_file,
    redact_har_text,
)
from app.cursor_agent_registry import CursorAgentRegistry


def test_registry_scopes_agents_to_account_workspace(tmp_path) -> None:
    registry = CursorAgentRegistry(tmp_path / "agents.db")
    alpha = registry.upsert_agent(
        account_key="ws:user-1",
        workspace_id="ws",
        friendly_name="Alpha-Core",
        workflow_id="wf-1",
        connection_id="conn-1",
        cursor_api_key_secret_id="00000000-0000-0000-0000-000000000001",
        allowed_github_repos=["https://github.com/Sanity-Cloud/Notion2API-CLI"],
        is_default=True,
        setup_status="verified",
    )
    registry.upsert_agent(
        account_key="ws:user-2",
        workspace_id="ws",
        friendly_name="Beta-Core",
        workflow_id="wf-2",
        connection_id="conn-2",
    )

    selected = registry.select_agent(
        account_key="ws:user-1",
        workspace_id="ws",
        repository_url="https://github.com/Sanity-Cloud/Notion2API-CLI",
    )
    assert selected["status"] == "selected"
    assert selected["agent"]["cursor_agent_key"] == alpha["cursor_agent_key"]
    assert "cursorApiKey" not in str(selected)

    crossed = registry.select_agent(
        cursor_agent_key=alpha["cursor_agent_key"],
        account_key="ws:user-2",
        workspace_id="ws",
    )
    assert crossed["status"] == "setup_required"
    assert crossed["reason"] == "explicit_agent_account_mismatch"


def test_bitwarden_resolve_fails_closed_without_token(monkeypatch) -> None:
    monkeypatch.delenv("BWS_ACCESS_TOKEN", raising=False)
    with pytest.raises(BitwardenSecretsError):
        resolve_secret_value("00000000-0000-0000-0000-000000000001")


def test_har_importer_redacts_secrets_and_never_stores_raw_keys(tmp_path) -> None:
    har = {
        "log": {
            "entries": [
                {
                    "request": {
                        "url": "https://www.notion.so/api/v3/getAvailableExternalAgents"
                    },
                    "response": {
                        "content": {
                            "text": json.dumps(
                                {
                                    "workflowId": "wf-personaltouch",
                                    "connectionId": "conn-pt",
                                    "externalAgentHarness": {
                                        "type": "cursor_agent",
                                        "connectionId": "conn-pt",
                                    },
                                    "cursorApiKey": "key_SHOULD_NOT_LEAK",
                                    "repositories": [
                                        "https://github.com/Sanity-Cloud/Notion2API-CLI"
                                    ],
                                }
                            )
                        }
                    },
                }
            ]
        }
    }
    har_path = tmp_path / "notionai.access.har"
    har_path.write_text(json.dumps(har), encoding="utf-8")

    redacted = redact_har_text(har_path.read_text(encoding="utf-8"))
    assert "key_SHOULD_NOT_LEAK" not in redacted
    assert "[redacted]" in redacted

    registry = CursorAgentRegistry(tmp_path / "agents.db")
    imported = import_cursor_workflows_from_har_file(
        har_path,
        account_key="ws:user-1",
        workspace_id="ws",
        registry=registry,
        cursor_api_key_secret_id="bw-secret-ref-only",
    )
    assert len(imported) == 1
    assert imported[0]["workflow_id"] == "wf-personaltouch"
    assert imported[0]["cursor_api_key_secret_id"] == "bw-secret-ref-only"
    serialized = json.dumps(imported)
    assert "key_SHOULD_NOT_LEAK" not in serialized
    assert "cursorApiKey" not in serialized or "[redacted]" in serialized


def test_extract_workflows_redacts_nested_secret_fields() -> None:
    payload = {
        "log": {
            "entries": [
                {
                    "request": {"url": "https://example/listCursorAgentRepos"},
                    "response": {
                        "content": {
                            "text": json.dumps(
                                {
                                    "id": "wf-2",
                                    "authorization": "Bearer secret-token",
                                    "cookies": {"token_v2": "cookie-secret"},
                                }
                            )
                        }
                    },
                }
            ]
        }
    }
    discovered = extract_cursor_workflows_from_har(payload)
    assert discovered
    cleaned = redact_secret_fields(discovered)
    serialized = json.dumps(cleaned)
    assert "secret-token" not in serialized
    assert "cookie-secret" not in serialized
