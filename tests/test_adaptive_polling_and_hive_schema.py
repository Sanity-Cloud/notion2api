"""MCP polling guidance and hive create-mission schema contract tests."""

from __future__ import annotations

import asyncio

from app import mcp_server
from app.mcp_server import create_server


def test_adaptive_poll_guidance_backs_off_and_preserves_stall_semantics(monkeypatch) -> None:
    monkeypatch.setattr(mcp_server, "_now_ms", lambda: 100_000)
    monkeypatch.setattr(mcp_server, "_configured_chat_stall_seconds", lambda: 15.0)

    healthy = mcp_server._refresh_chat_job_health(
        {
            "status": "pending",
            "created_at": 99_000,
            "last_progress_at": 99_000,
            "poll_count": 1,
        },
        increment_poll=True,
    )
    assert healthy["poll_count"] == 2
    assert healthy["recommended_poll_delay_ms"] == 750
    assert healthy["next_poll_after_ms"] == 100_750
    assert healthy["poll_hint"]

    stalled = mcp_server._refresh_chat_job_health(
        {
            "status": "pending",
            "created_at": 1_000,
            "last_progress_at": 1_000,
            "poll_count": 25,
        },
        increment_poll=True,
    )
    assert stalled["dead_loop_suspected"] is True
    assert stalled["cancel_recommended"] is True
    assert stalled["recommended_poll_delay_ms"] == 5_000


def test_persist_chat_progress_skips_redundant_writes(monkeypatch) -> None:
    writes: list[str] = []
    state = {
        "jobs": {
            "req-1": {
                "request_id": "req-1",
                "status": "running",
                "created_at": 1,
                "updated_at": 1,
                "progress_fingerprint": "",
            }
        }
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(
        mcp_server,
        "_save_chat_job_state",
        lambda value, *args, **kwargs: writes.append("write"),
    )
    monkeypatch.setattr(mcp_server, "_now_ms", lambda: 50)

    mcp_server._persist_chat_progress("req-1", "Now reviewing files", "partial", 3, False)
    assert writes == []
    assert state["jobs"]["req-1"]["progress"]["latest_update"]
    first_fingerprint = state["jobs"]["req-1"]["progress_fingerprint"]
    assert first_fingerprint

    mcp_server._persist_chat_progress("req-1", "Now reviewing files", "partial", 3, False)
    assert writes == []
    assert state["jobs"]["req-1"]["progress_fingerprint"] == first_fingerprint


def test_hive_create_mission_schema_requires_workspace_and_user(monkeypatch) -> None:
    monkeypatch.setenv("MCP_SERVER_NAME", "notion2api")
    monkeypatch.setenv("MCP_TOOL_PREFIX", "")
    server = create_server(
        base_url="http://127.0.0.1:8120",
        api_key="test-key",
        timeout=1,
        host="127.0.0.1",
        port=8130,
        mcp_path="/mcp",
    )
    tools = asyncio.run(server.list_tools())
    by_name = {tool.name: tool for tool in tools}
    schema = by_name["hive_create_mission"].inputSchema
    required = set(schema.get("required") or [])
    properties = schema.get("properties") or {}
    assert "workspace_id" in properties
    assert "user_id" in properties
    assert "workspace_id" in required
    assert "user_id" in required
