import json

import asyncio
from pathlib import Path

import httpx

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


def test_default_op_session_is_shared_not_unique():
    assert mcp_server._session_key(None) == "op"
    assert mcp_server._session_key("OP") == "op"


def test_persist_chat_progress_updates_pollable_job(monkeypatch):
    state = {"jobs": {"request-1": {"request_id": "request-1", "status": "running"}}}
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(mcp_server, "_save_chat_job_state", lambda value: state.update(value))

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
    monkeypatch.setattr(mcp_server, "_save_chat_job_state", lambda value: state.update(value))

    result = mcp_server._cancel_chat_job("request-1", "obsolete")

    assert result.status == "cancelled"
    assert result.error == "obsolete"
    assert state["jobs"]["request-1"]["status"] == "cancelled"


def test_cancel_unknown_chat_job_returns_not_found(monkeypatch):
    state = {"jobs": {}}
    monkeypatch.setattr(mcp_server, "_load_chat_job_state", lambda: state)
    monkeypatch.setattr(mcp_server, "_save_chat_job_state", lambda value: state.update(value))

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
    monkeypatch.setattr(mcp_server, "_save_chat_job_state", lambda value: state.update(value))
    monkeypatch.setattr(mcp_server, "_CHAT_JOB_TASKS", {})

    result = mcp_server._chat_job_output("request-2")

    assert result.status == "stale"
    assert "lost the in-memory task" in (result.error or "")


def test_mcp_schema_exposes_continuation_and_cancellation():
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
    chat_schema = by_name["notion2api_chat"].inputSchema["properties"]

    assert chat_schema["model"]["default"] == "terra"
    assert "conversation_id" in chat_schema
    assert "continue_from_request_id" in chat_schema
    assert "notion2api_cancel_chat_job" in by_name
    assert "notion2api_stage_file" in by_name
    assert "notion2api_chat_with_file" in by_name
    assert "notion2api_upload_file_to_page" in by_name
    stage_file_schema = by_name["notion2api_stage_file"].inputSchema["properties"]
    chat_file_schema = by_name["notion2api_chat_with_file"].inputSchema["properties"]
    page_file_schema = by_name["notion2api_upload_file_to_page"].inputSchema["properties"]
    assert stage_file_schema["file"]["format"] == "file"
    assert chat_file_schema["file"]["format"] == "file"
    assert page_file_schema["file"]["format"] == "file"
    assert "staged_file_ids" in chat_schema
    assert "require_attachments" in chat_schema
    assert "Service-host local file paths" in chat_schema["attachments"]["description"]
    assert "/mnt/data" in chat_schema["attachments"]["description"]
    assert "format" not in chat_schema["attachments"]


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
    assert mcp_server._infer_session_name("op", "unrelated prompt") == "op"
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
    monkeypatch.setattr(mcp_server, "_save_chat_job_state", lambda value: state.update(value))

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
    monkeypatch.setattr(mcp_server, "_save_chat_job_state", lambda value: state.update(value))
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
    monkeypatch.setattr(mcp_server, "_active_job_for_conversation", lambda *args, **kwargs: None)
    monkeypatch.setattr(mcp_server, "_load_chat_job", lambda _rid: None)
    monkeypatch.setattr(mcp_server, "_persist_chat_job", lambda _job: None)
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

    assert result["status"] == "completed"
    assert captured["baseline_message_id"] == 17
