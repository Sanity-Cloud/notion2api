import json
import sqlite3

import asyncio
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest

from app import mcp_server
from app.attachments.security import AttachmentPolicy
from app.mcp_server import _extract_chat_content, _extract_responses_text, create_server


def test_extract_chat_content_from_openai_shape():
    data = {"choices": [{"message": {"content": "hello"}}]}
    assert _extract_chat_content(data) == "hello"


def test_extract_responses_text_from_output_text():
    data = {"output_text": "hello responses"}
    assert _extract_responses_text(data) == "hello responses"


def test_create_server_registers_tools():
    server = create_server(
        base_url="http://127.0.0.1:8000",
        api_key=None,
        timeout=1,
        host="127.0.0.1",
        port=8130,
        mcp_path="/mcp",
    )
    assert server is not None


def test_create_server_supports_profile_identity(monkeypatch):
    monkeypatch.setenv("MCP_SERVER_NAME", "AIgentBee")
    monkeypatch.setenv("SANITYCLOUD_TOOL_NAMESPACE", "A!")
    monkeypatch.setenv("SANITYCLOUD_INVOCATION_ALIAS", "A!B")
    server = create_server(
        base_url="http://127.0.0.1:8122",
        api_key="test-key",
        timeout=1,
        host="127.0.0.1",
        port=8132,
        mcp_path="/mcp",
    )
    assert server.name == "AIgentBee"
    assert "Human invocation alias: A!B." in server.instructions
    assert "SanityCloud smart-tool namespace: A!." in server.instructions


def test_aigentbee_profile_exposes_configured_machine_prefix(monkeypatch):
    monkeypatch.setenv("MCP_SERVER_NAME", "AIgentBee")
    monkeypatch.setenv("MCP_TOOL_PREFIX", "aigentbee")
    server = create_server(
        base_url="http://127.0.0.1:8122",
        api_key="test-key",
        timeout=1,
        host="127.0.0.1",
        port=8132,
        mcp_path="/mcp",
    )
    tools = asyncio.run(server.list_tools())
    names = {tool.name for tool in tools}
    assert len(names) == 60
    assert "aigentbee_hive_create_mission" in names
    assert "aigentbee_hive_delegate_tasks" in names
    assert "aigentbee_hive_transition_task" in names
    assert "aigentbee_chat" in names
    assert "aigentbee_chat_history" in names
    assert "aigentbee_health" in names
    assert "aigentbee_get_chat_job" in names
    assert "aigentbee_manage_session_retention" in names
    assert "aigentbee_list_accounts" in names
    assert "aigentbee_list_cursor_agents" in names
    assert "aigentbee_select_cursor_agent" in names
    assert "aigentbee_upsert_cursor_agent" in names
    assert "aigentbee_switch_workspace" in names
    assert "aigentbee_switch_account" in names
    assert "aigentbee_rollback_account_switch" in names
    assert "aigentbee_show_swarm_workbench" in names
    assert "aigentbee_get_swarm_workbench" in names
    assert "aigentbee_send_leader_request" in names
    assert "aigentbee_hive_register_worker" in names
    assert "aigentbee_hive_transition_worker" in names
    assert "aigentbee_hive_list_workers" in names
    assert "aigentbee_hive_route_file_operation" in names
    assert "aigentbee_hive_plan_invocation" in names
    assert "aigentbee_hive_materialize_invocation" in names
    assert "aigentbee_hive_approve_materialization" in names
    assert "aigentbee_hive_get_materialization" in names
    assert "aigentbee_hive_record_dispatch_receipt" in names
    assert "aigentbee_hive_release_materialization_leases" in names
    assert "aigentbee_hive_heartbeat_worker_lease" in names
    assert "aigentbee_hive_reconcile_stale_leases" in names
    assert "aigentbee_hive_audit_workforce" in names
    assert "aigentbee_hive_upsert_execution_adapter" in names
    assert "aigentbee_hive_list_execution_adapters" in names
    assert "aigentbee_hive_execute_dispatch" in names
    assert "aigentbee_hive_get_execution" in names
    assert "aigentbee_hive_cancel_execution" in names
    assert "aigentbee_hive_recover_execution" in names
    assert "aigentbee_hive_review_execution" in names
    assert "aigentbee_hive_certify_external_adapter" in names
    assert "aigentbee_hive_list_external_certifications" in names
    assert "aigentbee_hive_transition_external_certification" in names
    assert "aigentbee_hive_list_external_effects" in names
    assert "aigentbee_hive_rollback_external_effect" in names
    assert all(name.startswith("aigentbee_") for name in names)
    assert not any(name.startswith("notion2api_") for name in names)
    assert all("notion2api_" not in (tool.description or "") for tool in tools)
    assert "aigentbee_health" in server.instructions
    assert "aigentbee_list_models" in server.instructions
    assert "aigentbee_get_chat_job" in server.instructions
    assert "aigentbee_stage_file" in server.instructions
    assert "aigentbee_hive_route_file_operation" in server.instructions
    assert "notion2api_" not in server.instructions


def test_primary_profile_exposes_bare_machine_methods(monkeypatch):
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
    names = {tool.name for tool in tools}
    assert len(names) == 57
    assert "hive_create_mission" in names
    assert "hive_delegate_tasks" in names
    assert "hive_transition_task" in names
    create_mission_tool = next(tool for tool in tools if tool.name == "hive_create_mission")
    create_schema = create_mission_tool.model_dump()["inputSchema"]
    assert create_schema["properties"]["authority_ceiling"]["default"] == "A2"
    assert {"workspace_id", "user_id"}.issubset(set(create_schema["required"]))
    assert "chat" in names
    assert "chat_history" in names
    assert "health" in names
    assert "get_chat_job" in names
    assert "manage_session_retention" in names
    assert "list_accounts" in names
    assert "list_cursor_agents" in names
    assert "select_cursor_agent" in names
    assert "upsert_cursor_agent" in names
    assert "switch_workspace" in names
    assert "switch_account" in names
    assert "rollback_account_switch" in names
    assert "hive_register_worker" in names
    assert "hive_transition_worker" in names
    assert "hive_list_workers" in names
    assert "hive_route_file_operation" in names
    assert "hive_plan_invocation" in names
    assert "hive_materialize_invocation" in names
    assert "hive_approve_materialization" in names
    assert "hive_get_materialization" in names
    assert "hive_record_dispatch_receipt" in names
    assert "hive_release_materialization_leases" in names
    assert "hive_heartbeat_worker_lease" in names
    assert "hive_reconcile_stale_leases" in names
    assert "hive_audit_workforce" in names
    assert "hive_upsert_execution_adapter" in names
    assert "hive_list_execution_adapters" in names
    assert "hive_execute_dispatch" in names
    assert "hive_get_execution" in names
    assert "hive_cancel_execution" in names
    assert "hive_recover_execution" in names
    assert "hive_review_execution" in names
    assert "hive_certify_external_adapter" in names
    assert "hive_list_external_certifications" in names
    assert "hive_transition_external_certification" in names
    assert "hive_list_external_effects" in names
    assert "hive_rollback_external_effect" in names
    assert not any(name.startswith(("notion2api_", "aigentbee_")) for name in names)
    assert all("notion2api_" not in (tool.description or "") for tool in tools)
    assert "notion2api_" not in server.instructions


def test_attachment_manifest_redacts_inline_data():
    manifest = mcp_server._attachment_manifest_from_payload({
        "attachments": [
            {
                "name": "sample.pdf",
                "content_type": "application/pdf",
                "size_bytes": 12,
                "source": "mcp_file",
                "data": "data:application/pdf;base64,JVBERi0xLjQ=",
            }
        ]
    })
    assert manifest == [
        {
            "name": "sample.pdf",
            "content_type": "application/pdf",
            "source": "mcp_file",
            "size_bytes": 12,
        }
    ]
    dumped = json.dumps(manifest)
    assert "JVBER" not in dumped
    assert "data:application/pdf" not in dumped


def test_atomic_write_json_retries_replace_failure(tmp_path, monkeypatch):
    path = tmp_path / "jobs.json"
    real_replace = mcp_server.os.replace
    calls = {"count": 0}

    def flaky_replace(src, dst):
        calls["count"] += 1
        if calls["count"] == 1:
            raise PermissionError("locked")
        return real_replace(src, dst)

    monkeypatch.setattr(mcp_server.os, "replace", flaky_replace)
    mcp_server._atomic_write_json(path, {"jobs": {"a": {"updated_at": 1}}})
    assert calls["count"] == 2
    assert json.loads(path.read_text(encoding="utf-8"))["jobs"]["a"]["updated_at"] == 1


def test_load_chat_job_state_recovers_valid_tmp_file(tmp_path):
    path = tmp_path / ".notion2api_mcp_chat_jobs.json"
    path.write_text(json.dumps({"jobs": {"old": {"request_id": "old", "updated_at": 1}}}), encoding="utf-8")
    tmp = path.with_name(f"{path.name}.abc.tmp")
    tmp.write_text(
        json.dumps({
            "jobs": {
                "old": {"request_id": "old", "updated_at": 2},
                "new": {"request_id": "new", "updated_at": 3},
            }
        }),
        encoding="utf-8",
    )

    state = mcp_server._load_chat_job_state(path)
    assert sorted(state["jobs"]) == ["new", "old"]
    assert state["jobs"]["old"]["updated_at"] == 2
    assert json.loads(path.read_text(encoding="utf-8"))["jobs"]["new"]["updated_at"] == 3
    assert not tmp.exists()


def test_legacy_session_key_remains_readable_for_existing_state():
    assert mcp_server._session_key(None) == "op"
    assert mcp_server._session_key("OP") == "op"


def test_chat_wait_is_always_immediate_for_polling():
    assert mcp_server._bounded_chat_wait_seconds(None) == 0
    assert mcp_server._bounded_chat_wait_seconds(60) == 0


def test_persist_chat_progress_updates_pollable_job(monkeypatch):
    state = {"jobs": {"request-1": {"request_id": "request-1", "status": "running"}}}
    saved = []
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(
        mcp_server,
        "_save_chat_job_state",
        lambda value, *args, **kwargs: saved.append((value, args, kwargs)),
    )

    mcp_server._persist_chat_progress(
        "request-1",
        "Working through the record.\n- [x] Map pages\n- [ ] Apply edits",
        "",
        3,
        False,
    )

    progress = state["jobs"]["request-1"]["progress"]
    assert progress["phase"] == "working"
    assert progress["event_count"] == 3
    assert progress["latest_update"] == "Apply edits (pending)"
    assert progress["checklist"] == [
        {"completed": True, "text": "Map pages"},
        {"completed": False, "text": "Apply edits"},
    ]
    assert saved, "bounded progress should be durably checkpointed"
    assert state["jobs"]["request-1"]["progress_persisted_at"] > 0


def test_chat_job_state_cache_and_compaction_avoid_repeated_full_reads(tmp_path, monkeypatch):
    path = tmp_path / "jobs.json"
    response = {"response_text": "answer", "raw": {"content": "answer"}}
    path.write_text(
        json.dumps(
            {
                "jobs": {
                    "request-1": {
                        "request_id": "request-1",
                        "status": "completed",
                        "response": response,
                        "response_text": "answer",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    calls = {"count": 0}
    real_recover = mcp_server._recover_chat_job_state

    def counted_recover(state_path):
        calls["count"] += 1
        return real_recover(state_path)

    monkeypatch.setattr(mcp_server, "_recover_chat_job_state", counted_recover)
    mcp_server._CHAT_JOB_STATE_CACHE.pop(str(path.resolve()), None)

    state = mcp_server._load_chat_job_state(path)
    assert mcp_server._load_chat_job_state(path) is state
    assert calls["count"] == 1

    mcp_server._save_chat_job_state(state, path)
    saved_job = json.loads(path.read_text(encoding="utf-8"))["jobs"]["request-1"]
    assert "response_text" not in saved_job
    assert saved_job["response"]["response_text"] == "answer"


def test_default_chat_job_ledger_migrates_losslessly_to_compressed_sqlite(
    tmp_path, monkeypatch
):
    json_path = tmp_path / "jobs.json"
    db_path = tmp_path / "jobs.sqlite3"
    dense_text = "dense evidence \u2713 " * 2_000
    original = {
        "jobs": {
            "request-1": {
                "request_id": "request-1",
                "status": "completed",
                "conversation_id": "conversation-1",
                "session_name": "research",
                "model": "terra",
                "created_at": 1,
                "updated_at": 2,
                "response": {
                    "response_text": dense_text,
                    "raw": {"evidence": dense_text, "nested": [1, 2, 3]},
                },
                "response_text": dense_text,
            },
            "request-2": {
                "request_id": "request-2",
                "status": "error",
                "conversation_id": "conversation-2",
                "updated_at": 3,
                "error": "preserved error",
            },
        }
    }
    original_bytes = json.dumps(original, ensure_ascii=False).encode("utf-8")
    json_path.write_bytes(original_bytes)
    monkeypatch.setattr(mcp_server, "DEFAULT_CHAT_JOB_STATE_PATH", json_path)
    monkeypatch.setattr(mcp_server, "DEFAULT_CHAT_JOB_DB_PATH", db_path)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_STATE_CACHE", {})
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_DB_READY", set())

    state = mcp_server._load_chat_job_state(json_path)

    assert json_path.read_bytes() == original_bytes
    assert state["jobs"]["request-1"]["response"]["raw"]["evidence"] == dense_text
    assert "response_text" not in state["jobs"]["request-1"]
    with sqlite3.connect(db_path) as conn:
        row = conn.execute(
            "SELECT payload_bytes, length(payload_zlib) FROM chat_jobs "
            "WHERE request_id = 'request-1'"
        ).fetchone()
        indexes = {
            item[1] for item in conn.execute("PRAGMA index_list(chat_jobs)").fetchall()
        }
    assert row[1] < row[0] // 4
    assert "idx_chat_jobs_conversation_updated" in indexes

    state["jobs"]["request-1"]["status"] = "verified"
    state["jobs"]["request-1"]["updated_at"] = 4
    mcp_server._save_chat_job_state(state, json_path, {"request-1"})
    reloaded = mcp_server._load_chat_job_db(db_path)
    assert reloaded["jobs"]["request-1"]["status"] == "verified"
    assert reloaded["jobs"]["request-2"]["error"] == "preserved error"
    assert json_path.read_bytes() == original_bytes


def test_chat_stream_updates_progress_and_returns_final_content(monkeypatch):
    body = "\n".join([
        'data: {"model":"test-model","choices":[{"delta":{"reasoning_content":"Reviewing records.\\n- [ ] Apply edits"}}]}',
        'data: {"model":"test-model","choices":[{"delta":{"content":"Done"},"finish_reason":"stop"}]}',
        "data: [DONE]",
        "",
    ]).encode()
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={
                "content-type": "text/event-stream",
                "X-Conversation-Id": "conversation-123",
                "X-Notion-Thread-Id": "thread-123",
            },
            content=body,
        )
    )
    real_client = httpx.AsyncClient

    class TestAsyncClient(real_client):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(mcp_server.httpx, "AsyncClient", TestAsyncClient)
    updates = []
    client = mcp_server.Notion2APIClient("http://test")
    result = asyncio.run(
        client.post_chat_stream(
            "/v1/chat/completions",
            {"model": "test-model"},
            lambda *args: updates.append(args),
        )
    )

    assert result["choices"][0]["message"]["content"] == "Done"
    assert "reasoning_content" not in result["choices"][0]["message"]
    assert result["model_metadata"]["conversation_id"] == "conversation-123"
    assert result["model_metadata"]["notion_thread_id"] == "thread-123"
    assert updates[-1] == ("Reviewing records.\n- [ ] Apply edits", "Done", 2, True)


def test_chat_job_recovers_persisted_completion_after_backend_503(monkeypatch):
    class FailedBackend:
        base_url = "http://test"
        timeout = 1

        async def post_chat_stream(self, *_args):
            return {"ok": False, "status_code": 503, "error": {"message": "upstream failed"}}

    monkeypatch.setattr(
        mcp_server,
        "_completed_turn_after_checkpoint",
        lambda *_args: {
            "assistant_message_id": 9,
            "response_text": "Recovered answer",
            "thinking": "",
            "remote_chat_id": "thread-9",
            "actual_model": "terra",
        },
    )

    result = asyncio.run(
        mcp_server._run_chat_completion_job(
            client=FailedBackend(),
            path="/v1/chat/completions",
            payload={"model": "terra"},
            model="terra",
            session_key="legal-research",
            conversation_id="conversation-9",
            session_created=False,
            request_id="request-9",
            wait_seconds=0,
            baseline_message_id=8,
        )
    )

    assert result["status"] == "completed"
    assert result["response_text"] == "Recovered answer"
    assert result["raw"]["completion_source"] == "local_conversation_checkpoint"


def test_session_records_migrate_legacy_and_preserve_remote_ids(tmp_path):
    state_path = tmp_path / "sessions.json"
    state_path.write_text(json.dumps({"sessions": {"Review Work": "conv-1"}}), encoding="utf-8")

    records = mcp_server._load_session_records(state_path)
    assert records == {"review-work": {"conversation_id": "conv-1"}}

    records["review-work"].update(
        remote_chat_id="thread-1",
        notion_thread_id="thread-1",
        last_model="terra",
    )
    mcp_server._save_session_records(records, state_path)

    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["version"] == 2
    assert persisted["sessions"]["review-work"]["remote_chat_id"] == "thread-1"
    assert mcp_server._load_session_state(state_path) == {"review-work": "conv-1"}


def test_continue_from_request_id_reuses_prior_session(monkeypatch):
    records = {}

    def save(updated, path=mcp_server.DEFAULT_SESSION_STATE_PATH):
        snapshot = {key: dict(value) for key, value in updated.items()}
        records.clear()
        records.update(snapshot)

    monkeypatch.setattr(mcp_server, "_load_session_records", lambda path=mcp_server.DEFAULT_SESSION_STATE_PATH: records)
    monkeypatch.setattr(mcp_server, "_save_session_records", save)
    monkeypatch.setattr(
        mcp_server,
        "_load_chat_job",
        lambda request_id: {
            "request_id": request_id,
            "session_name": "repo-ai-review",
            "conversation_id": "conv-review-1",
        },
    )

    conversation_id, session_name, created = mcp_server._conversation_id_for_session(
        "op",
        continue_from_request_id="prior-request",
    )

    assert conversation_id == "conv-review-1"
    assert session_name == "repo-ai-review"
    assert created is False
    assert records["repo-ai-review"]["continued_from_request_id"] == "prior-request"


def test_explicit_conversation_id_infers_existing_session(monkeypatch):
    records = {
        "repo-ai-review": {
            "conversation_id": "conv-review-1",
            "remote_chat_id": "thread-review-1",
        }
    }
    monkeypatch.setattr(mcp_server, "_load_session_records", lambda path=mcp_server.DEFAULT_SESSION_STATE_PATH: records)
    monkeypatch.setattr(mcp_server, "_save_session_records", lambda *args, **kwargs: None)

    conversation_id, session_name, created = mcp_server._conversation_id_for_session(
        "op",
        conversation_id="conv-review-1",
    )

    assert (conversation_id, session_name, created) == (
        "conv-review-1",
        "repo-ai-review",
        False,
    )


def test_remote_chat_id_and_stall_monitoring(monkeypatch):
    assert mcp_server._extract_remote_chat_id(
        {"model_metadata": {"notion_thread_id": "thread-123"}}
    ) == "thread-123"

    monkeypatch.setattr(mcp_server, "_now_ms", lambda: 31_000)
    monkeypatch.setattr(mcp_server, "_configured_chat_stall_seconds", lambda: 15.0)
    job = mcp_server._refresh_chat_job_health(
        {
            "status": "pending",
            "created_at": 1_000,
            "last_progress_at": 1_000,
            "poll_count": 2,
        },
        increment_poll=True,
    )
    assert job["poll_count"] == 3
    assert job["stalled_for_seconds"] == 30.0
    assert job["dead_loop_suspected"] is True
    assert job["cancel_recommended"] is True


def test_poll_health_is_persisted_durably(monkeypatch):
    state = {
        "jobs": {
            "request-1": {
                "request_id": "request-1",
                "status": "pending",
                "created_at": 1_000,
                "last_progress_at": 1_000,
                "poll_count": 2,
            }
        }
    }
    saves = []
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(mcp_server, "_now_ms", lambda: 31_000)
    monkeypatch.setattr(mcp_server, "_configured_chat_stall_seconds", lambda: 15.0)
    monkeypatch.setattr(
        mcp_server,
        "_save_chat_job_state",
        lambda value, *args, **kwargs: saves.append((value, args, kwargs)),
    )

    job = mcp_server._refresh_and_persist_chat_job_health(
        "request-1",
        increment_poll=True,
    )

    assert job is not None
    assert job["poll_count"] == 3
    assert job["dead_loop_suspected"] is True
    assert state["jobs"]["request-1"]["poll_count"] == 3
    assert saves


def test_cancel_chat_job_marks_persisted_job_cancelled(monkeypatch):
    state = {
        "jobs": {
            "request-1": {
                "request_id": "request-1",
                "job_id": "request-1",
                "status": "pending",
                "session_name": "repo-ai-review",
                "conversation_id": "conv-1",
                "created_at": 1,
                "updated_at": 1,
            }
        }
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(
        mcp_server,
        "_save_chat_job_state",
        lambda value, *args, **kwargs: state.update(value),
    )

    result = mcp_server._cancel_chat_job("request-1", "obsolete")

    assert result.status == "cancelled"
    assert result.error == "obsolete"
    assert state["jobs"]["request-1"]["status"] == "cancelled"
    assert state["jobs"]["request-1"]["reconciliation_required"] is True
    assert state["jobs"]["request-1"]["retry_safe"] is False


def test_cancel_chat_job_preserves_stall_evidence_and_upstream_uncertainty(monkeypatch):
    state = {
        "jobs": {
            "request-1": {
                "request_id": "request-1",
                "job_id": "request-1",
                "status": "running",
                "session_name": "review",
                "conversation_id": "conv-1",
                "created_at": 1_000,
                "updated_at": 1_000,
                "last_progress_at": 1_000,
            }
        }
    }

    class HeldTask:
        def __init__(self):
            self.cancelled = False

        def done(self):
            return False

        def cancel(self):
            self.cancelled = True

    task = HeldTask()
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {"request-1": task})
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _request_id: state["jobs"]["request-1"])
    monkeypatch.setattr(mcp_server, "_now_ms", lambda: 31_000)
    monkeypatch.setattr(mcp_server, "_configured_chat_stall_seconds", lambda: 15.0)
    monkeypatch.setattr(mcp_server, "_save_chat_job_state", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        mcp_server,
        "_chat_job_output",
        lambda *args, **kwargs: mcp_server.ChatJobOutput(
            ok=True,
            found=True,
            status="cancelled",
            request_id="request-1",
            job_id="request-1",
        ),
    )

    result = mcp_server._cancel_chat_job("request-1", "stalled")

    cancelled = state["jobs"]["request-1"]
    assert result.status == "cancelled"
    assert task.cancelled is True
    assert cancelled["cancelled_from_status"] == "running"
    assert cancelled["stalled_for_seconds_at_cancel"] == 30.0
    assert cancelled["dead_loop_suspected_at_cancel"] is True
    assert cancelled["cancel_recommended_at_cancel"] is True
    assert cancelled["cancellation_state"] == "local_task_cancel_requested_upstream_unconfirmed"
    assert cancelled["upstream_execution_state"] == "unknown"
    assert cancelled["reconciliation_required"] is True


def test_cancel_reconciles_completed_checkpoint_before_marking_cancelled(monkeypatch):
    job = {
        "request_id": "request-1",
        "job_id": "request-1",
        "status": "running",
        "conversation_id": "conv-1",
        "baseline_message_id": 4,
    }
    completed = []
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _request_id: job)
    monkeypatch.setattr(
        mcp_server,
        "_completed_turn_after_checkpoint",
        lambda _conversation_id, _baseline: {"response_text": "done", "assistant_message_id": 5},
    )
    monkeypatch.setattr(
        mcp_server,
        "_complete_chat_job_from_local_turn",
        lambda request_id, current, turn: completed.append((request_id, current, turn)),
    )
    monkeypatch.setattr(
        mcp_server,
        "_chat_job_output",
        lambda *args, **kwargs: mcp_server.ChatJobOutput(
            ok=True,
            found=True,
            status="completed",
            request_id="request-1",
            job_id="request-1",
        ),
    )

    result = mcp_server._cancel_chat_job("request-1", "obsolete")

    assert result.status == "completed"
    assert completed and completed[0][0] == "request-1"


def test_cancelled_job_output_is_not_authoritative(monkeypatch):
    job = {
        "request_id": "request-1",
        "job_id": "request-1",
        "status": "cancelled",
        "response_text": "late result",
        "reconciliation_required": True,
        "created_at": 1,
        "updated_at": 2,
    }
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _request_id: job)
    monkeypatch.setattr(mcp_server, "_refresh_and_persist_chat_job_health", lambda *args, **kwargs: job)

    result = mcp_server._chat_job_output("request-1", increment_poll=False)

    assert result.status == "cancelled"
    assert result.authoritative is False
    assert result.retry_safe is False
    assert result.reconciliation_required is True


def test_cancelled_late_completion_is_reconciled_without_reviving_job(monkeypatch):
    job = {
        "request_id": "request-1",
        "job_id": "request-1",
        "status": "cancelled",
        "conversation_id": "conv-1",
        "reconciliation_required": True,
        "cancellation_state": "local_task_cancel_requested_upstream_unconfirmed",
    }
    persisted = []
    monkeypatch.setattr(
        mcp_server,
        "_chat_output_from_local_turn",
        lambda _job, _turn: {
            "status": "completed",
            "response_text": "late answer",
            "remote_chat_id": "thread-1",
            "output_integrity": {"quarantine_required": False},
            "quarantined": False,
        },
    )
    monkeypatch.setattr(mcp_server, "_persist_chat_job", lambda value: persisted.append(dict(value)))
    monkeypatch.setattr(mcp_server, "_now_ms", lambda: 50_000)

    reconciled = mcp_server._reconcile_cancelled_chat_job_from_local_turn(
        "request-1",
        job,
        {"assistant_message_id": 7},
    )

    assert reconciled["status"] == "cancelled"
    assert reconciled["late_completion_detected"] is True
    assert reconciled["late_completion_at"] == 50_000
    assert reconciled["late_response_chars"] == len("late answer")
    assert reconciled["upstream_execution_state"] == "terminal"
    assert reconciled["reconciliation_required"] is False
    assert reconciled["cancellation_state"] == "local_cancelled_upstream_terminal_observed"
    assert reconciled["remote_chat_id"] == "thread-1"
    assert persisted and persisted[-1]["status"] == "cancelled"


def test_chat_job_sqlite_journal_records_status_transitions(tmp_path):
    import sqlite3

    db_path = tmp_path / "jobs.sqlite3"
    state = {
        "jobs": {
            "request-1": {
                "request_id": "request-1",
                "status": "running",
                "created_at": 1,
                "updated_at": 1,
            }
        }
    }
    with sqlite3.connect(db_path) as conn:
        conn.row_factory = sqlite3.Row
        mcp_server._ensure_chat_job_db_schema(conn)
        mcp_server._append_chat_job_transition_events(conn, state, {"request-1"})
        mcp_server._upsert_chat_jobs(conn, state, {"request-1"})
        conn.commit()

        state["jobs"]["request-1"].update(
            {
                "status": "cancelled",
                "updated_at": 2,
                "cancellation_state": "local_task_cancel_requested_upstream_unconfirmed",
                "reconciliation_required": True,
            }
        )
        mcp_server._append_chat_job_transition_events(conn, state, {"request-1"})
        mcp_server._upsert_chat_jobs(conn, state, {"request-1"})
        conn.commit()
        rows = conn.execute(
            "SELECT event_type, previous_status, new_status, metadata_json "
            "FROM chat_job_events WHERE request_id = ? ORDER BY event_id",
            ("request-1",),
        ).fetchall()

    assert [(row["event_type"], row["previous_status"], row["new_status"]) for row in rows] == [
        ("job_created", "", "running"),
        ("status_transition", "running", "cancelled"),
    ]
    metadata = json.loads(rows[-1]["metadata_json"])
    assert metadata["reconciliation_required"] is True
    assert metadata["cancellation_state"] == "local_task_cancel_requested_upstream_unconfirmed"


def test_cancel_unknown_chat_job_returns_not_found(monkeypatch):
    state = {"jobs": {}}
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(
        mcp_server,
        "_save_chat_job_state",
        lambda value, *args, **kwargs: state.update(value),
    )

    result = mcp_server._cancel_chat_job("missing-request")

    assert result.found is False
    assert state["jobs"] == {}


def test_pending_job_without_live_task_is_marked_stale(monkeypatch):
    state = {
        "jobs": {
            "request-2": {
                "request_id": "request-2",
                "job_id": "request-2",
                "status": "pending",
                "session_name": "review",
                "conversation_id": "conv-2",
                "created_at": 1,
                "updated_at": 1,
            }
        }
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(
        mcp_server,
        "_save_chat_job_state",
        lambda value, *args, **kwargs: state.update(value),
    )
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    result = mcp_server._chat_job_output("request-2")

    assert result.status == "stale"
    assert "lost the in-memory task" in (result.error or "")


def test_page_upload_uses_versioned_backend_endpoint():
    assert mcp_server.NOTION_PAGE_UPLOAD_ENDPOINT == "/v1/notion/upload_file"


def test_mcp_schema_exposes_continuation_and_cancellation(monkeypatch):
    monkeypatch.setenv("MCP_SERVER_NAME", "notion2api")
    monkeypatch.setenv("MCP_TOOL_PREFIX", "")
    server = create_server(
        base_url="http://127.0.0.1:8120",
        api_key="test-key",
        timeout=30,
        host="127.0.0.1",
        port=8130,
        mcp_path="/mcp",
    )
    tools = asyncio.run(server.list_tools())
    by_name = {tool.name: tool for tool in tools}
    chat_schema = by_name["chat"].inputSchema["properties"]
    history_schema = by_name["chat_history"].inputSchema["properties"]

    assert history_schema["action"]["enum"] == [
        "status",
        "list_threads",
        "get_thread",
        "search",
        "export_markdown",
        "model_stats",
        "sync_from_notion",
        "hydrate_thread",
    ]
    assert history_schema["limit"]["default"] == 50
    assert history_schema["offset"]["default"] == 0
    assert history_schema["message_limit"]["default"] == 100
    assert history_schema["content_limit"]["default"] == 50000
    assert history_schema["account_index"]["default"] == 0
    assert history_schema["max_pages"]["default"] == 5
    assert history_schema["hydrate"]["default"] is False

    assert chat_schema["model"]["default"] == "terra"
    assert "conversation_id" in chat_schema
    assert "continue_from_request_id" in chat_schema
    assert "cancel_chat_job" in by_name

    for tool_name in (
        "chat",
        "chat_with_file",
        "chat_completion",
        "responses",
    ):
        model_schema = by_name[tool_name].inputSchema["properties"]["model"]
        assert model_schema["default"] == "terra"
        assert "Omit this argument to use Terra" in model_schema["description"]
    assert "stage_file" in by_name
    assert "chat_with_file" in by_name
    assert "upload_file_to_page" in by_name
    stage_file_schema = by_name["stage_file"].inputSchema["properties"]
    chat_file_schema = by_name["chat_with_file"].inputSchema["properties"]
    page_file_schema = by_name["upload_file_to_page"].inputSchema["properties"]
    assert stage_file_schema["file"]["format"] == "file"
    assert chat_file_schema["file"]["format"] == "file"
    assert page_file_schema["file"]["format"] == "file"
    assert "staged_file_ids" in chat_schema
    assert "require_attachments" in chat_schema
    assert "Service-host local file paths" in chat_schema["attachments"]["description"]
    for tool_name in (
        "chat",
        "chat_with_file",
        "chat_completion",
    ):
        properties = by_name[tool_name].inputSchema["properties"]
        assert "legacy name 'op'" in properties["session_name"]["description"]
        assert "ignored" in properties["wait_seconds"]["description"]
        assert properties["mode"]["enum"] == ["default", "ask", "research"]
        assert "read-only" in properties["mode"]["description"]
        assert properties["task"]["anyOf"][0]["enum"] == ["visualize", "generate_image", "create_slides", "spreadsheet", "deep_research"]
        assert "google-drive" in properties["sources"]["description"]
        assert "web search" in properties["web_access"]["description"]
        assert properties["persona"]["anyOf"][0]["enum"] == ["sidekick", "minimalist", "analyst"]


def test_mcp_schema_exposes_delegated_task_contract(monkeypatch):
    monkeypatch.setenv("MCP_SERVER_NAME", "notion2api")
    monkeypatch.setenv("MCP_TOOL_PREFIX", "")
    server = create_server(
        base_url="http://127.0.0.1:8120",
        api_key="test-key",
        timeout=30,
        host="127.0.0.1",
        port=8130,
        mcp_path="/mcp",
    )
    by_name = {tool.name: tool for tool in asyncio.run(server.list_tools())}

    delegate = by_name["hive_delegate_tasks"].inputSchema["properties"]
    transition = by_name["hive_transition_task"].inputSchema["properties"]
    assert {"mission_id", "tasks", "expected_mission_revision"} <= delegate.keys()
    assert {
        "mission_id",
        "task_id",
        "status",
        "lease_seconds",
        "handoff_receipt",
    } <= transition.keys()


def test_omitted_session_name_is_descriptive_and_not_shared_op():
    first = mcp_server._infer_session_name(
        None,
        "Add durable Notion2API session continuation and polling",
    )
    second = mcp_server._infer_session_name(
        None,
        "Add durable Notion2API session continuation and polling",
    )
    stable_first = mcp_server._infer_session_name(
        None,
        "Add durable Notion2API session continuation and polling",
        request_id="request-123",
    )
    stable_second = mcp_server._infer_session_name(
        None,
        "Add durable Notion2API session continuation and polling",
        request_id="request-123",
    )

    assert first != second
    assert first != "op"
    assert stable_first == stable_second
    assert first.startswith("add-durable-notion2api-session-continuation-polling-")
    legacy = mcp_server._infer_session_name("op", "unrelated prompt", request_id="legacy-op")
    assert legacy != "op"
    assert legacy == mcp_server._infer_session_name("op", "unrelated prompt", request_id="legacy-op")
    assert mcp_server._infer_session_name("RepoAI Review", "unrelated prompt") == "repoai-review"


def test_explicit_prompt_messages_contain_only_caller_fields():
    system_prompt = "Act as skeptical appellate counsel."
    user_prompt = "Review the attached records."

    messages = mcp_server._explicit_prompt_messages(user_prompt, system_prompt)
    progress = mcp_server._progress_snapshot(
        "Considering file options\n/home/oai/skills/pdfs/SKILL.md\n---FILES---",
        "",
        3,
        False,
    )

    assert messages == [
        {"role": "system", "content": system_prompt},
        {"role": "user", "content": user_prompt},
    ]
    assert "Considering file options" not in repr(messages)
    assert progress["activity_chars"] > 0


def test_explicit_contamination_markers_are_allowed_only_when_caller_supplies_them():
    explicit = (
        "Considering file options\n"
        "/home/oai/skills/pdfs/SKILL.md\n"
        "bash -lc cat /home/oai/skills/pdfs/SKILL.md\n"
        "---FILES---\n-rw-r--r-- record.pdf"
    )

    messages = mcp_server._explicit_prompt_messages("Review records.", explicit)

    assert messages[0]["content"] == explicit


def test_explicit_messages_are_deep_copied_before_async_submission():
    source = [{"role": "user", "content": [{"type": "text", "text": "Original"}]}]

    copied = mcp_server._copy_explicit_messages(source)
    source[0]["content"][0]["text"] = "Mutated later"

    assert copied[0]["content"][0]["text"] == "Original"


def test_explicit_session_name_is_stable_across_request_ids():
    first = mcp_server._infer_session_name(
        "Legal Record Review", "first prompt", request_id="operation-1"
    )
    second = mcp_server._infer_session_name(
        "Legal Record Review", "second prompt", request_id="operation-2"
    )

    assert first == second == "legal-record-review"


def _create_terminalization_db(path):
    import sqlite3

    conn = sqlite3.connect(path)
    conn.executescript(
        """
        CREATE TABLE conversations (
            id TEXT PRIMARY KEY, thread_id TEXT, thread_model TEXT
        );
        CREATE TABLE messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            conversation_id TEXT, role TEXT, content TEXT,
            created_at INTEGER, thinking TEXT DEFAULT ''
        );
        INSERT INTO conversations(id, thread_id, thread_model)
        VALUES ('conv-1', 'thread-1', 'terra-route');
        INSERT INTO messages(conversation_id, role, content, created_at)
        VALUES ('conv-1', 'assistant', 'old reply', 1);
        """
    )
    conn.commit()
    conn.close()


def test_completed_turn_after_checkpoint_detects_persisted_reply(tmp_path, monkeypatch):
    import sqlite3

    db_path = tmp_path / "conversations.db"
    _create_terminalization_db(db_path)
    monkeypatch.setattr(mcp_server, "_local_conversation_db_path", lambda: db_path)
    baseline = mcp_server._conversation_message_checkpoint("conv-1")
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "INSERT INTO messages(conversation_id, role, content, created_at) VALUES (?, ?, ?, ?)",
            ("conv-1", "user", "new request", 2),
        )
        conn.execute(
            "INSERT INTO messages(conversation_id, role, content, created_at) VALUES (?, ?, ?, ?)",
            ("conv-1", "assistant", "completed answer", 2),
        )
        conn.commit()

    turn = mcp_server._completed_turn_after_checkpoint("conv-1", baseline)

    assert turn is not None
    assert turn["response_text"] == "completed answer"
    assert turn["remote_chat_id"] == "thread-1"
    assert turn["actual_model"] == "terra-route"


def test_completed_job_is_not_downgraded_by_cancelled_stream_callback(monkeypatch):
    state = {
        "jobs": {
            "request-1": {
                "request_id": "request-1",
                "job_id": "request-1",
                "status": "completed",
                "response_text": "finished",
            }
        }
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(
        mcp_server,
        "_save_chat_job_state",
        lambda value, *args, **kwargs: state.update(value),
    )

    class CancelledTask:
        def result(self):
            raise asyncio.CancelledError()

    mcp_server._finalize_chat_job("request-1", CancelledTask())

    assert state["jobs"]["request-1"]["status"] == "completed"
    assert state["jobs"]["request-1"]["response_text"] == "finished"


def test_active_job_for_conversation_prevents_parallel_turns(monkeypatch):
    state = {
        "jobs": {
            "request-1": {
                "request_id": "request-1",
                "status": "pending",
                "conversation_id": "conv-1",
            }
        }
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)

    conflict = mcp_server._active_job_for_conversation(
        "conv-1", exclude_request_id="request-2"
    )

    assert conflict is not None
    assert conflict[0] == "request-1"


class _HeldTask:
    def __init__(self):
        self.cancelled = False

    def done(self):
        return False

    def cancel(self):
        self.cancelled = True


def _claim_test_job(request_id: str, conversation_id: str) -> dict:
    return {
        "request_id": request_id,
        "job_id": request_id,
        "status": "running",
        "conversation_id": conversation_id,
        "created_at": 1,
        "updated_at": 1,
        "last_progress_at": 1,
    }


def test_atomic_admission_allows_only_one_simultaneous_turn_per_conversation(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "jobs.json"
    barrier = threading.Barrier(2)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    def claim(request_id):
        barrier.wait(timeout=5)
        return mcp_server._claim_chat_job_task(
            _claim_test_job(request_id, "shared-conversation"),
            _HeldTask,
            path=state_path,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(claim, ["request-a", "request-b"]))

    assert sorted(result[0] for result in results) == ["claimed", "conflict"]
    claimed = next(result for result in results if result[0] == "claimed")
    conflict = next(result for result in results if result[0] == "conflict")
    assert conflict[3] == claimed[1]["request_id"]
    persisted = mcp_server._load_chat_job_state(state_path)
    assert list(persisted["jobs"]) == [claimed[1]["request_id"]]


def test_unreconciled_cancelled_job_fences_replacement_turn(tmp_path, monkeypatch):
    state_path = tmp_path / "jobs.json"
    state_path.write_text(
        json.dumps(
            {
                "jobs": {
                    "request-old": {
                        "request_id": "request-old",
                        "job_id": "request-old",
                        "status": "cancelled",
                        "conversation_id": "shared-conversation",
                        "created_at": 1,
                        "updated_at": 2,
                        "reconciliation_required": True,
                        "upstream_execution_state": "unknown",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    status, record, task, conflict_id = mcp_server._claim_chat_job_task(
        _claim_test_job("request-new", "shared-conversation"),
        _HeldTask,
        path=state_path,
    )

    assert status == "stale_conflict"
    assert conflict_id == "request-old"
    assert record["status"] == "cancelled"
    assert record["reconciliation_required"] is True
    assert task is None
    persisted = mcp_server._load_chat_job_state(state_path)
    assert "request-new" not in persisted["jobs"]


def test_reconciled_cancelled_job_no_longer_fences_replacement_turn(tmp_path, monkeypatch):
    state_path = tmp_path / "jobs.json"
    state_path.write_text(
        json.dumps(
            {
                "jobs": {
                    "request-old": {
                        "request_id": "request-old",
                        "job_id": "request-old",
                        "status": "cancelled",
                        "conversation_id": "shared-conversation",
                        "created_at": 1,
                        "updated_at": 2,
                        "reconciliation_required": False,
                        "late_completion_detected": True,
                        "upstream_execution_state": "terminal",
                    }
                }
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    status, record, task, conflict_id = mcp_server._claim_chat_job_task(
        _claim_test_job("request-new", "shared-conversation"),
        _HeldTask,
        path=state_path,
    )

    assert status == "claimed"
    assert conflict_id == ""
    assert record["request_id"] == "request-new"
    assert task is not None


def test_simultaneous_same_request_id_creates_only_one_task(tmp_path, monkeypatch):
    state_path = tmp_path / "jobs.json"
    barrier = threading.Barrier(2)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})
    created_tasks = []
    task_lock = threading.Lock()

    def task_factory():
        task = _HeldTask()
        with task_lock:
            created_tasks.append(task)
        return task

    def claim():
        barrier.wait(timeout=5)
        return mcp_server._claim_chat_job_task(
            _claim_test_job("request-same", "conversation-same"),
            task_factory,
            path=state_path,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(lambda _item: claim(), range(2)))

    assert sorted(result[0] for result in results) == ["claimed", "existing"]
    assert len(created_tasks) == 1
    claimed = next(result for result in results if result[0] == "claimed")
    existing = next(result for result in results if result[0] == "existing")
    assert existing[2] is claimed[2]
    persisted = mcp_server._load_chat_job_state(state_path)
    assert list(persisted["jobs"]) == ["request-same"]


def test_same_request_id_cannot_claim_a_second_conversation(tmp_path, monkeypatch):
    state_path = tmp_path / "jobs.json"
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    first_status, first_record, first_task, _conflict_id = (
        mcp_server._claim_chat_job_task(
            _claim_test_job("request-shared", "conversation-a"),
            _HeldTask,
            path=state_path,
        )
    )
    second_status, second_record, second_task, conflict_id = (
        mcp_server._claim_chat_job_task(
            _claim_test_job("request-shared", "conversation-b"),
            _HeldTask,
            path=state_path,
        )
    )

    assert first_status == "claimed"
    assert first_record["conversation_id"] == "conversation-a"
    assert first_task is not None
    assert second_status == "request_id_conflict"
    assert second_record["conversation_id"] == "conversation-a"
    assert second_task is first_task
    assert conflict_id == "request-shared"
    persisted = mcp_server._load_chat_job_state(state_path)
    assert persisted["jobs"]["request-shared"]["conversation_id"] == "conversation-a"


def test_submit_rejects_request_id_reused_for_another_conversation(monkeypatch):
    from types import SimpleNamespace

    existing = {
        "request_id": "request-shared",
        "job_id": "request-shared",
        "status": "completed",
        "conversation_id": "conversation-a",
        "response": {"status": "completed", "response_text": "private answer"},
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _request_id: existing)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    result = asyncio.run(
        mcp_server._submit_or_resume_chat_job(
            client=SimpleNamespace(
                base_url="http://127.0.0.1:8120",
                timeout=30.0,
            ),
            path="/v1/chat/completions",
            payload={
                "model": "terra",
                "messages": [{"role": "user", "content": "hello"}],
            },
            model="terra",
            session_key="other-session",
            conversation_id="conversation-b",
            session_created=False,
            request_id="request-shared",
            wait_seconds=0,
        )
    )

    assert result["status_code"] == 409
    assert result["raw"] == {"code": "request_id_conversation_mismatch"}
    assert result["response_text"] == ""
    assert "conversation-a" not in str(result)
    assert "private answer" not in str(result)


def test_atomic_admission_does_not_create_task_when_claim_write_fails(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "jobs.json"
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})
    created = []

    def task_factory():
        task = _HeldTask()
        created.append(task)
        return task

    def fail_save(_state, _path=state_path, _request_ids=None):
        raise OSError("simulated durable ledger failure")

    monkeypatch.setattr(mcp_server, "_save_chat_job_state", fail_save)

    with pytest.raises(OSError, match="durable ledger failure"):
        mcp_server._claim_chat_job_task(
            _claim_test_job("request-failed", "conversation-failed"),
            task_factory,
            path=state_path,
        )

    assert created == []
    assert "request-failed" not in mcp_server._CHAT_JOB_TASKS


def test_task_creation_failure_releases_durable_claim_for_retry(tmp_path, monkeypatch):
    state_path = tmp_path / "jobs.json"
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    def fail_task_creation():
        raise RuntimeError("simulated scheduler failure")

    with pytest.raises(RuntimeError, match="scheduler failure"):
        mcp_server._claim_chat_job_task(
            _claim_test_job("request-failed-task", "conversation-failed-task"),
            fail_task_creation,
            path=state_path,
        )

    persisted = mcp_server._load_chat_job_state(state_path)
    failed = persisted["jobs"]["request-failed-task"]
    assert failed["status"] == "error"
    assert "Task scheduling failed" in failed["error"]
    assert "request-failed-task" not in mcp_server._CHAT_JOB_TASKS

    status, record, task, conflict_id = mcp_server._claim_chat_job_task(
        _claim_test_job("request-retry", "conversation-failed-task"),
        _HeldTask,
        path=state_path,
    )
    assert status == "claimed"
    assert record["status"] == "running"
    assert task is not None
    assert conflict_id == ""


def test_orphaned_active_turn_requires_reconciliation_before_replacement(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "jobs.json"
    mcp_server._save_chat_job_state(
        {
            "jobs": {
                "request-old": _claim_test_job(
                    "request-old", "shared-conversation"
                )
            }
        },
        state_path,
    )
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    status, record, task, conflict_id = mcp_server._claim_chat_job_task(
        _claim_test_job("request-new", "shared-conversation"),
        _HeldTask,
        path=state_path,
    )

    assert status == "stale_conflict"
    assert task is None
    assert conflict_id == "request-old"
    assert record["status"] == "stale"
    persisted = mcp_server._load_chat_job_state(state_path)
    assert persisted["jobs"]["request-old"]["status"] == "stale"
    assert "request-new" not in persisted["jobs"]


def test_atomic_admission_preserves_parallel_different_conversations(
    tmp_path, monkeypatch
):
    state_path = tmp_path / "jobs.json"
    barrier = threading.Barrier(2)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    def claim(item):
        request_id, conversation_id = item
        barrier.wait(timeout=5)
        return mcp_server._claim_chat_job_task(
            _claim_test_job(request_id, conversation_id),
            _HeldTask,
            path=state_path,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(
            pool.map(
                claim,
                [
                    ("request-a", "conversation-a"),
                    ("request-b", "conversation-b"),
                ],
            )
        )

    assert [result[0] for result in results] == ["claimed", "claimed"]
    persisted = mcp_server._load_chat_job_state(state_path)
    assert sorted(persisted["jobs"]) == ["request-a", "request-b"]


def test_concurrent_session_updates_preserve_unrelated_records(tmp_path, monkeypatch):
    state_path = tmp_path / "sessions.json"
    barrier = threading.Barrier(2)
    original_load = mcp_server._load_session_records

    def slow_load(path=state_path):
        records = original_load(path)
        time.sleep(0.05)
        return records

    monkeypatch.setattr(mcp_server, "_load_session_records", slow_load)

    def update(item):
        session_name, conversation_id = item
        barrier.wait(timeout=5)
        mcp_server._update_session_record(
            session_name,
            conversation_id=conversation_id,
            request_id=f"request-{session_name}",
            path=state_path,
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        list(
            pool.map(
                update,
                [
                    ("session-a", "conversation-a"),
                    ("session-b", "conversation-b"),
                ],
            )
        )

    records = original_load(state_path)
    assert records["session-a"]["conversation_id"] == "conversation-a"
    assert records["session-b"]["conversation_id"] == "conversation-b"


def test_startup_reconciliation_closes_orphaned_jobs(monkeypatch):
    state = {
        "jobs": {
            "completed-request": {
                "request_id": "completed-request",
                "job_id": "completed-request",
                "status": "pending",
                "conversation_id": "conv-complete",
                "session_name": "complete",
                "model": "terra",
                "baseline_message_id": 0,
            },
            "stale-request": {
                "request_id": "stale-request",
                "job_id": "stale-request",
                "status": "running",
                "conversation_id": "conv-stale",
                "session_name": "stale",
                "model": "terra",
            },
        }
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(
        mcp_server,
        "_save_chat_job_state",
        lambda value, *args, **kwargs: state.update(value),
    )
    monkeypatch.setattr(mcp_server, "_update_session_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        mcp_server,
        "_completed_turn_after_checkpoint",
        lambda conversation_id, baseline: (
            {
                "assistant_message_id": 2,
                "response_text": "done",
                "thinking": "",
                "created_at": 2,
                "remote_chat_id": "thread-1",
                "actual_model": "terra-route",
            }
            if conversation_id == "conv-complete"
            else None
        ),
    )

    summary = mcp_server._reconcile_orphaned_chat_jobs_on_startup()

    assert summary == {"completed": 1, "stale": 1}
    assert state["jobs"]["completed-request"]["status"] == "completed"
    assert state["jobs"]["completed-request"]["response_text"] == "done"
    assert state["jobs"]["stale-request"]["status"] == "stale"




def test_staged_file_round_trip_uses_opaque_id(monkeypatch, tmp_path):
    source = tmp_path / "source.pdf"
    content = b"%PDF-1.4\nstaged\n%EOF"
    source.write_bytes(content)
    policy = AttachmentPolicy(
        enabled=True,
        local_root=str(tmp_path),
        allowed_mime_types={"application/pdf"},
    )
    monkeypatch.setattr(AttachmentPolicy, "from_env", classmethod(lambda cls: policy))

    staged = mcp_server.stage_mcp_transferred_file(str(source), "evidence.pdf")
    resolved = mcp_server.resolve_mcp_staged_files([staged["staged_file_id"]])
    prepared = mcp_server.prepare_mcp_file_attachments(resolved)

    assert staged["staged_file_id"].startswith("stage-")
    assert staged["content_type"] == "application/pdf"
    assert Path(resolved[0]).read_bytes() == content
    assert prepared[0]["name"] == "evidence.pdf"
    assert prepared[0]["size_bytes"] == len(content)

def test_required_attachments_fail_closed_before_job_submission(monkeypatch):
    from types import SimpleNamespace

    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _rid: None)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})
    result = asyncio.run(
        mcp_server._submit_or_resume_chat_job(
            client=SimpleNamespace(base_url="http://127.0.0.1:8120", timeout=30.0),
            path="/v1/chat/completions",
            payload={
                "model": "terra",
                "messages": [{"role": "user", "content": "analyze the attached document"}],
                "metadata": {"require_attachments": True},
            },
            model="terra",
            session_key="attachment-required",
            conversation_id="conv-attachment-required",
            session_created=True,
            request_id="request-attachment-required",
            wait_seconds=0,
        )
    )

    assert result["status"] == "error"
    assert result["status_code"] == 422
    assert result["raw"]["code"] == "required_attachments_missing"
    assert result["attachment_required"] is True
    assert result["attachment_count"] == 0
    assert result["attachment_transfer_status"] == "missing"
    assert result["attachment_manifest"] == []

def test_submit_passes_message_checkpoint_to_worker(monkeypatch):
    from types import SimpleNamespace

    captured = {}

    async def fake_worker(**kwargs):
        captured.update(kwargs)
        return {
            "ok": True,
            "status": "completed",
            "response_text": "done",
            "request_id": kwargs["request_id"],
            "job_id": kwargs["request_id"],
        }

    monkeypatch.setattr(mcp_server, "_conversation_message_checkpoint", lambda _cid: 17)
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _rid: None)
    monkeypatch.setattr(mcp_server, "_persist_chat_job", lambda _job: None)

    def fake_claim(job, task_factory, **_kwargs):
        task = task_factory()
        mcp_server._CHAT_JOB_TASKS[job["request_id"]] = task
        return "claimed", job, task, ""

    monkeypatch.setattr(mcp_server, "_claim_chat_job_task", fake_claim)
    monkeypatch.setattr(mcp_server, "_update_session_record", lambda *args, **kwargs: None)
    monkeypatch.setattr(mcp_server, "_finalize_chat_job", lambda *args, **kwargs: None)
    monkeypatch.setattr(mcp_server, "_run_chat_completion_job", fake_worker)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    client = SimpleNamespace(base_url="http://127.0.0.1:8120", timeout=30.0)
    result = asyncio.run(
        mcp_server._submit_or_resume_chat_job(
            client=client,
            path="/v1/chat/completions",
            payload={
                "model": "terra",
                "messages": [{"role": "user", "content": "hello"}],
            },
            model="terra",
            session_key="checkpoint-test",
            conversation_id="conv-checkpoint",
            session_created=True,
            request_id="request-checkpoint",
            wait_seconds=1,
        )
    )

    assert result["status"] == "pending"
    assert result["wait_seconds"] == 0
    assert captured["baseline_message_id"] == 17


def test_terminal_job_without_response_recovers_persisted_checkpoint(monkeypatch):
    from types import SimpleNamespace

    captured = {}
    existing = {
        "request_id": "request-checkpoint",
        "job_id": "request-checkpoint",
        "status": "completed",
        "session_name": "checkpoint-test",
        "conversation_id": "conv-checkpoint",
        "baseline_message_id": 17,
    }
    turn = {
        "assistant_message_id": 18,
        "response_text": "Recovered persisted answer",
        "thinking": "",
        "remote_chat_id": "thread-checkpoint",
        "actual_model": "terra-route",
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _rid: existing)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})
    monkeypatch.setattr(
        mcp_server,
        "_completed_turn_after_checkpoint",
        lambda conversation_id, baseline: (
            captured.update(conversation_id=conversation_id, baseline=baseline) or turn
        ),
    )
    monkeypatch.setattr(
        mcp_server,
        "_complete_chat_job_from_local_turn",
        lambda request_id, job, completed_turn: (
            captured.update(
                request_id=request_id,
                job=job,
                completed_turn=completed_turn,
            )
            or {"status": "completed", "response_text": completed_turn["response_text"]}
        ),
    )
    monkeypatch.setattr(
        mcp_server,
        "_chat_pending_output",
        lambda **_kwargs: (_ for _ in ()).throw(
            AssertionError("completed checkpoint should be recovered before pending output")
        ),
    )

    result = asyncio.run(
        mcp_server._submit_or_resume_chat_job(
            client=SimpleNamespace(base_url="http://127.0.0.1:8120", timeout=30.0),
            path="/v1/chat/completions",
            payload={"model": "terra", "messages": [{"role": "user", "content": "hello"}]},
            model="terra",
            session_key="checkpoint-test",
            conversation_id="conv-checkpoint",
            session_created=False,
            request_id="request-checkpoint",
            wait_seconds=0,
        )
    )

    assert result == {
        "status": "completed",
        "response_text": "Recovered persisted answer",
    }
    assert captured["conversation_id"] == "conv-checkpoint"
    assert captured["baseline"] == 17
    assert captured["request_id"] == "request-checkpoint"
    assert captured["completed_turn"] == turn


def test_chat_job_poll_is_bounded_unless_full_response_is_requested(monkeypatch):
    response_text = "x" * 100_000
    job = {
        "request_id": "large-request",
        "job_id": "large-request",
        "status": "completed",
        "response_text": response_text,
        "response": {
            "status": "completed",
            "response_text": response_text,
            "raw": {"content": response_text},
        },
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _rid: job)
    monkeypatch.setattr(mcp_server, "_persist_chat_job", lambda _job: None)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    bounded = mcp_server._chat_job_output("large-request")

    assert bounded.status == "completed"
    assert bounded.response_chars == len(response_text)
    assert bounded.response_truncated is True
    assert len(bounded.response_text) == mcp_server.MAX_CHAT_JOB_RESPONSE_PREVIEW_CHARS
    assert bounded.response is None
    assert "response" not in bounded.raw_job
    assert "response_text" not in bounded.raw_job
    assert len(bounded.model_dump_json()) < 10_000

    complete = mcp_server._chat_job_output("large-request", include_response=True)
    assert complete.response_text == response_text
    assert complete.response_truncated is False
    assert complete.response == job["response"]



def _recursive_contaminated_text() -> str:
    paragraph = (
        "This completion claims successful writes but recursively repeats the same "
        "substantive paragraph without independent evidence or distinct content."
    )
    return "\n\n".join([paragraph] * 5)


def test_backend_contaminated_output_is_quarantined_before_completion():
    client = type("Client", (), {"base_url": "http://127.0.0.1:8120", "timeout": 10})()
    text = _recursive_contaminated_text()
    data = {
        "ok": True,
        "status_code": 200,
        "choices": [{"message": {"content": text}}],
    }

    result = mcp_server._chat_output_from_backend(
        data=data,
        client=client,
        model="terra",
        session_key="session",
        conversation_id="conversation",
        session_created=False,
        request_id="request-contaminated",
        wait_seconds=0,
    )

    assert result["status"] == "indeterminate_output"
    assert result["ok"] is False
    assert result["response_text"] == ""
    assert result["quarantined"] is True
    assert result["retry_safe"] is False
    assert result["quarantined_response_available"] is True
    assert result["raw"]["delivery_state"] == "generated_but_quarantined"
    assert result["raw"]["generated_response_chars"] == len(text)
    assert result["output_integrity"]["response_chars"] == len(text)
    assert "identical_paragraph_repetition" in result["output_integrity"]["reasons"]
    assert result["_quarantined_response"]["response"]["response_text"] == text


def test_clean_backend_output_remains_completed():
    client = type("Client", (), {"base_url": "http://127.0.0.1:8120", "timeout": 10})()
    result = mcp_server._chat_output_from_backend(
        data={
            "ok": True,
            "status_code": 200,
            "choices": [{"message": {"content": "A clean and distinct answer."}}],
        },
        client=client,
        model="terra",
        session_key="session",
        conversation_id="conversation",
        session_created=False,
        request_id="request-clean",
        wait_seconds=0,
    )

    assert result["status"] == "completed"
    assert result["response_text"] == "A clean and distinct answer."
    assert result["quarantined"] is False


def test_legacy_completed_job_is_demoted_and_hidden_on_poll(monkeypatch):
    text = _recursive_contaminated_text()
    job = {
        "request_id": "legacy-contaminated",
        "job_id": "legacy-contaminated",
        "status": "completed",
        "model": "terra",
        "response_text": text,
        "response": {
            "ok": True,
            "status": "completed",
            "response_text": text,
            "raw": {"choices": [{"message": {"content": text}}]},
        },
    }
    persisted = []
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _rid: job)
    monkeypatch.setattr(mcp_server, "_persist_chat_job", lambda value: persisted.append(value))
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    result = mcp_server._chat_job_output(
        "legacy-contaminated",
        include_response=False,
        include_last_response=True,
    )

    assert result.status == "indeterminate_output"
    assert result.response_text == ""
    assert result.authoritative is False
    assert result.quarantined is True
    assert result.quarantined_response_available is True
    assert result.quarantined_response_text == ""
    assert result.response is None
    assert result.response_chars == len(text)
    assert result.response_truncated is True
    assert result.raw_job.get("delivery_state") == "generated_but_quarantined"
    assert result.raw_job.get("quarantined_response_available") is True
    assert "_quarantined_response" not in result.raw_job
    assert persisted[0]["_quarantined_response"]["response"]["response_text"] == text

    preview = mcp_server._chat_job_output(
        "legacy-contaminated",
        include_response=False,
        include_last_response=False,
        include_quarantined=True,
        increment_poll=False,
    )
    assert preview.quarantined_response_text == text
    assert preview.authoritative is False
    assert preview.response_text == ""

    full = mcp_server._chat_job_output(
        "legacy-contaminated",
        include_response=True,
        include_quarantined=True,
        increment_poll=False,
    )
    assert full.quarantined_response_text == text
    assert full.response is not None
    assert full.response["quarantined_response_text"] == text
    assert full.response["authoritative"] is False



def test_responses_endpoint_contaminated_output_is_quarantined():
    client = type("Client", (), {"base_url": "http://127.0.0.1:8120", "timeout": 10})()
    text = _recursive_contaminated_text()
    result = mcp_server._responses_output_from_backend(
        data={
            "ok": True,
            "status_code": 200,
            "output_text": text,
        },
        client=client,
        model="terra",
        provenance={
            "attachment_required": False,
            "attachment_count": 0,
            "attachment_transfer_status": "not_requested",
            "attachment_manifest": [],
        },
    )

    assert result["status"] == "indeterminate_output"
    assert result["ok"] is False
    assert result["response_text"] == ""
    assert result["quarantined"] is True
    assert result["output_integrity"]["response_chars"] == len(text)
    assert result["raw"]["quarantined"] is True
    assert "output_text" not in result["raw"]


def test_responses_endpoint_clean_output_remains_available():
    client = type("Client", (), {"base_url": "http://127.0.0.1:8120", "timeout": 10})()
    result = mcp_server._responses_output_from_backend(
        data={
            "ok": True,
            "status_code": 200,
            "output_text": "A clean responses-endpoint answer.",
        },
        client=client,
        model="terra",
        provenance={
            "attachment_required": False,
            "attachment_count": 0,
            "attachment_transfer_status": "not_requested",
            "attachment_manifest": [],
        },
    )

    assert result["status"] == "completed"
    assert result["response_text"] == "A clean responses-endpoint answer."
    assert result["quarantined"] is False
    assert result["raw"]["output_text"] == "A clean responses-endpoint answer."



def test_chat_stream_decodes_unicode_and_applies_content_replacement(monkeypatch):
    events = [
        {
            "model": "test-model",
            "choices": [{"delta": {"content": "Draft caf? ??"}}],
        },
        {
            "type": "content_replace",
            "content": "Corrected caf? ?? ? ??",
            "choices": [{"delta": {}, "finish_reason": None}],
        },
        {
            "model": "test-model",
            "choices": [{"delta": {}, "finish_reason": "stop"}],
        },
    ]
    body = "\n".join(
        [f"data: {json.dumps(event, ensure_ascii=False)}" for event in events]
        + ["data: [DONE]", ""]
    ).encode("utf-8")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"content-type": "text/event-stream; charset=utf-8"},
            content=body,
        )
    )
    real_client = httpx.AsyncClient

    class TestAsyncClient(real_client):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(mcp_server.httpx, "AsyncClient", TestAsyncClient)
    updates = []
    result = asyncio.run(
        mcp_server.Notion2APIClient("http://test").post_chat_stream(
            "/v1/chat/completions",
            {"model": "test-model"},
            lambda *args: updates.append(args),
        )
    )

    expected = "Corrected caf? ?? ? ??"
    assert result["ok"] is True
    assert result["choices"][0]["message"]["content"] == expected
    assert updates[-1][1] == expected


def test_chat_stream_fails_closed_on_backend_quarantine(monkeypatch):
    integrity = {
        "schema_version": 1,
        "status": "quarantined",
        "contaminated": True,
        "quarantine_required": True,
        "response_chars": 27,
        "response_sha256": "example",
        "reasons": ["visible_output_contamination"],
    }
    events = [
        {
            "type": "output_hygiene",
            "hygiene": {"output_integrity": integrity},
        },
        {
            "model": "test-model",
            "choices": [{"delta": {}, "finish_reason": "content_filter"}],
        },
    ]
    body = "\n".join(
        [f"data: {json.dumps(event)}" for event in events]
        + ["data: [DONE]", ""]
    ).encode("utf-8")
    transport = httpx.MockTransport(
        lambda request: httpx.Response(
            200,
            headers={"content-type": "text/event-stream"},
            content=body,
        )
    )
    real_client = httpx.AsyncClient

    class TestAsyncClient(real_client):
        def __init__(self, *args, **kwargs):
            kwargs["transport"] = transport
            super().__init__(*args, **kwargs)

    monkeypatch.setattr(mcp_server.httpx, "AsyncClient", TestAsyncClient)
    updates = []
    result = asyncio.run(
        mcp_server.Notion2APIClient("http://test").post_chat_stream(
            "/v1/chat/completions",
            {"model": "test-model"},
            lambda *args: updates.append(args),
        )
    )

    assert result["ok"] is False
    assert result["status_code"] == 422
    assert result["status"] == "indeterminate_output"
    assert result["quarantined"] is True
    assert result["choices"][0]["finish_reason"] == "content_filter"
    assert result["choices"][0]["message"]["content"] == ""
    assert result["output_integrity"] == integrity
    assert updates[-1][1] == ""


def test_terminal_normalization_preserves_upstream_quarantine_receipt():
    integrity = {
        "schema_version": 1,
        "status": "quarantined",
        "contaminated": True,
        "quarantine_required": True,
        "response_chars": 19,
        "response_sha256": "upstream",
        "reasons": ["upstream_content_filter"],
    }
    normalized, evidence = mcp_server._normalize_terminal_output(
        {
            "ok": False,
            "status_code": 422,
            "status": "indeterminate_output",
            "response_text": "",
            "output_integrity": integrity,
            "quarantined": True,
        },
        source="test",
    )

    assert normalized["quarantined"] is True
    assert normalized["output_integrity"] == integrity
    assert normalized["response_text"] == ""
    assert normalized["authoritative"] is False
    assert evidence is not None


def test_chat_job_poll_preserves_terra_alias_resolution(monkeypatch):
    alias_resolution = {
        "requested_model": "terra",
        "canonical_model": "terra",
        "resolved_model": "orchid-muffin",
        "public_model": "gpt-5.6-terra",
        "display_name": "GPT-5.6 Terra",
        "resolution_kind": "configured_alias",
    }
    job = {
        "request_id": "terra-request",
        "job_id": "terra-request",
        "status": "completed",
        "model": "terra",
        "requested_model": "terra",
        "resolved_model": "orchid-muffin",
        "alias_resolution": alias_resolution,
        "model_route_disposition": "alias_resolution",
        "prompt": "Design the Mission World console.",
    }
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _rid: job)
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    result = mcp_server._chat_job_output("terra-request")

    assert result.model == "terra"
    assert result.requested_model == "terra"
    assert result.resolved_model == "orchid-muffin"
    assert result.alias_resolution == alias_resolution
    assert result.model_route_disposition == "alias_resolution"
    assert result.raw_job["resolved_model"] == "orchid-muffin"
    assert result.raw_job["prompt"] == "Design the Mission World console."


def test_keyword_dump_response_is_quarantined_by_terminal_normalization():
    text = (
        "Sanity Cloud AI Portal existing product architecture and governance concepts "
        "including departments, Oz roles, authority A0-A4, QuickBind, workflow school "
        "levels, and autonomy maturitySanity Cloud AI Portal governance QuickBind "
        "authority A0 A4 Oz Hollywood White House Government Militaryall_time##"
    )
    normalized, evidence = mcp_server._normalize_terminal_output(
        {
            "ok": True,
            "status_code": 200,
            "status": "completed",
            "response_text": text,
        },
        source="test",
    )

    assert normalized["status"] == "indeterminate_output"
    assert normalized["quarantined"] is True
    assert normalized["authoritative"] is False
    assert normalized["response_text"] == ""
    assert "nonsentence_keyword_dump" in normalized["output_integrity"]["reasons"]
    assert evidence is not None
    assert evidence["response"]["response_text"] == text


def test_messages_fallback_includes_persisted_job_prompt(monkeypatch, tmp_path):
    db_path = tmp_path / "conversations.db"
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        conn.execute(
            "CREATE TABLE conversations (id TEXT PRIMARY KEY, title TEXT, created_at INTEGER, "
            "summary TEXT, next_round_index INTEGER, compress_failed_at INTEGER, "
            "thread_id TEXT, thread_model TEXT)"
        )
        conn.execute(
            "CREATE TABLE messages (id INTEGER PRIMARY KEY, conversation_id TEXT, role TEXT, "
            "content TEXT, created_at INTEGER, thinking TEXT)"
        )
        conn.execute(
            "INSERT INTO conversations (id, title, created_at) VALUES (?, ?, ?)",
            ("mcp-session-1", "session-1", 1),
        )

    monkeypatch.setattr(mcp_server, "_local_conversation_db_path", lambda: db_path)
    monkeypatch.setattr(
        mcp_server,
        "_resolve_session_conversation_id",
        lambda session_name=None, conversation_id=None: (
            "session-1",
            "mcp-session-1",
            None,
        ),
    )
    monkeypatch.setattr(
        mcp_server,
        "_load_chat_job_state",
        lambda: {
            "jobs": {
                "job-1": {
                    "status": "completed",
                    "session_name": "session-1",
                    "conversation_id": "mcp-session-1",
                    "prompt": "Create the Academy page tree.",
                    "response_text": "Created the Academy outline.",
                    "updated_at": 20,
                    "created_at": 10,
                }
            }
        },
    )

    result = mcp_server._read_local_messages(session_name="session-1", limit=10)

    assert result.ok is True
    assert result.count == 2
    assert result.persistence_source == "mcp_job_store"
    assert result.reconciliation_required is True
    assert result.messages[0]["role"] == "user"
    assert result.messages[0]["content"] == "Create the Academy page tree."
    assert result.messages[1]["role"] == "assistant"
    assert result.messages[1]["content"] == "Created the Academy outline."


def test_mcp_chat_surfaces_expose_exact_reasoning_effort_schema(monkeypatch):
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
    tools = {tool.name: tool for tool in asyncio.run(server.list_tools())}

    for name in ("chat", "chat_with_file", "chat_completion", "responses"):
        schema = tools[name].inputSchema
        assert "reasoning_effort" in schema["properties"]
        description = schema["properties"]["reasoning_effort"]["description"]
        assert "model-specific" in description
        assert "silently downgraded" in description


def test_mcp_outputs_promote_reasoning_effort_receipt():
    client = type(
        "Client",
        (),
        {"base_url": "http://127.0.0.1:8120", "timeout": 10},
    )()
    data = {
        "ok": True,
        "status_code": 200,
        "choices": [{"message": {"content": "done"}}],
        "model_metadata": {
            "requested_reasoning_effort": "high",
            "resolved_reasoning_effort": "high",
            "reasoning_effort_source": "explicit",
        },
    }

    result = mcp_server._chat_output_from_backend(
        data=data,
        client=client,
        model="terra",
        session_key="reasoning-test",
        conversation_id="conversation-reasoning",
        session_created=True,
        request_id="request-reasoning",
        wait_seconds=0,
    )

    assert result["requested_reasoning_effort"] == "high"
    assert result["resolved_reasoning_effort"] == "high"
    assert result["reasoning_effort_source"] == "explicit"


def test_runtime_audit_reports_configured_imported_history_path(monkeypatch, tmp_path):
    configured = tmp_path / "isolated-history.db"
    monkeypatch.setenv("CHAT_HISTORY_DB_PATH", str(configured))
    client = type("Client", (), {"base_url": "http://127.0.0.1:8120", "timeout": 10})()

    audit = mcp_server._runtime_audit(client, "terra")

    assert audit["imported_history_db"] == str(configured)
