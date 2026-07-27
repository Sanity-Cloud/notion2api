from __future__ import annotations

import argparse
import copy
import contextlib
import asyncio
import hashlib
import json
import logging
import os
import re
import sqlite3
import threading
import time
import uuid
import zlib
from pathlib import Path
from typing import Any, Annotated, Literal

import httpx
from pydantic import BaseModel, Field

from app.attachments.normalizer import validate_chat_messages, validate_prompt_text
from app.output_hygiene import detect_visible_output_contamination
from app.output_integrity import assess_output_integrity
from app.hive_runtime import (
    HiveMissionSnapshot,
    HiveRuntimeError,
    HiveWorkUnitSpec,
    default_hive_runtime_db_path,
    get_hive_runtime_store,
)
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

if load_dotenv:
    load_dotenv()

# Keep standalone MCP attachment policy aligned with the FastAPI backend.
# Without this patch, AttachmentPolicy.from_env() defaults to disabled even
# when the local authenticated backend would safely enable attachments.
from app.attachments.runtime_config import apply_attachment_runtime_config

apply_attachment_runtime_config()

DEFAULT_BASE_URL = "http://127.0.0.1:8120"
DEFAULT_MCP_HOST = "127.0.0.1"
DEFAULT_MCP_PORT = 8130
DEFAULT_MCP_PATH = "/mcp"
DEFAULT_TIMEOUT_SECONDS = 900.0
DEFAULT_MODEL = "terra"
NOTION_PAGE_UPLOAD_ENDPOINT = "/v1/notion/upload_file"
LEGACY_SHARED_SESSION_NAME = "op"
DEFAULT_SESSION_NAME = LEGACY_SHARED_SESSION_NAME
AUTO_SESSION_LABEL = "auto-generated"
DEFAULT_SESSION_STATE_PATH = Path(
    os.getenv(
        "MCP_NOTION2API_SESSION_STATE",
        str(Path.cwd() / ".notion2api_mcp_sessions.json"),
    )
)
DEFAULT_CHAT_JOB_STATE_PATH = Path(
    os.getenv(
        "MCP_NOTION2API_CHAT_JOB_STATE",
        str(DEFAULT_SESSION_STATE_PATH.with_name(".notion2api_mcp_chat_jobs.json")),
    )
)
DEFAULT_CHAT_JOB_DB_PATH = Path(
    os.getenv(
        "MCP_NOTION2API_CHAT_JOB_DB",
        str(DEFAULT_CHAT_JOB_STATE_PATH.with_suffix(".sqlite3")),
    )
)
DEFAULT_CHAT_STALL_SECONDS = 180.0
MAX_PROGRESS_REASONING_CHARS = 200_000
MAX_CHAT_JOB_RESPONSE_PREVIEW_CHARS = 4_000
DEFAULT_STAGED_FILE_TTL_SECONDS = 24 * 60 * 60
STAGED_FILE_ID_RE = re.compile(r"^stage-[a-f0-9]{32}$")
SESSION_STATE_VERSION = 2
_SESSION_STATE_MUTEX = threading.RLock()
_CHAT_JOB_STATE_MUTEX = threading.RLock()
_CHAT_JOB_TASKS: dict[str, asyncio.Task[dict[str, Any]]] = {}
_CHAT_JOB_STATE_CACHE: dict[
    str, tuple[tuple[int, int, int, int] | None, dict[str, Any]]
] = {}
_CHAT_JOB_DB_READY: set[str] = set()
logger = logging.getLogger(__name__)
CHAT_JOB_STATE_WRITE_RETRIES = 5
CHAT_JOB_STATE_WRITE_BACKOFF_SECONDS = 0.05
CHAT_JOB_LEDGER_SCHEMA_VERSION = 1


class HealthOutput(BaseModel):
    ok: bool = Field(description="Whether the backend health call succeeded.")
    status_code: int | None = Field(default=None, description="HTTP status code returned by Notion2API.")
    status: str | None = Field(default=None, description="Backend status string, usually ok.")
    accounts: int | None = Field(default=None, description="Ready account count reported by Notion2API.")
    accounts_total: int | None = Field(default=None, description="Total configured account count.")
    accounts_cooling: int | None = Field(default=None, description="Number of accounts currently cooling down.")
    uptime: int | float | None = Field(default=None, description="Backend uptime, if reported.")
    raw: dict[str, Any] = Field(default_factory=dict, description="Raw backend health response.")


class ModelInfo(BaseModel):
    id: str = Field(description="Model id.")
    object: str | None = Field(default=None, description="OpenAI-style object type, usually model.")
    created: int | None = Field(default=None, description="Creation timestamp, if supplied.")
    owned_by: str | None = Field(default=None, description="Provider or owner, if supplied.")


class ListModelsOutput(BaseModel):
    ok: bool = Field(description="Whether the models call succeeded.")
    status_code: int | None = Field(default=None, description="HTTP status code returned by Notion2API.")
    count: int = Field(default=0, description="Number of model entries returned.")
    models: list[ModelInfo] = Field(default_factory=list, description="JSON-safe OpenAI-style model entries.")
    error: str | None = Field(default=None, description="Error summary if the backend did not return models.")


class ChatOutput(BaseModel):
    ok: bool = Field(description="Whether the model call succeeded.")
    status_code: int | None = Field(default=None, description="HTTP status code returned by Notion2API.")
    model: str = Field(description="Requested model id passed through the MCP wrapper.")
    actual_model: str = Field(default="", description="Actual Notion model/provider route used, if returned.")
    model_metadata: dict[str, Any] | None = Field(default=None, description="Notion2API model metadata, if any.")
    requested_model: str = Field(default="", description="Requested model alias originally passed to the MCP wrapper.")
    resolved_model: str = Field(default="", description="Concrete Notion route resolved from the requested alias.")
    verified_model: str = Field(default="", description="Responder model only when supported by authoritative upstream evidence.")
    model_identity_verified: bool = Field(default=False, description="Whether verified_model is backed by authoritative responder evidence.")
    model_identity_source: str = Field(default="", description="Evidence source used for model identity classification.")
    model_identity_confidence: str = Field(default="unverified", description="Model identity confidence: verified, observed, or unverified.")
    model_substitution: dict[str, Any] | None = Field(default=None, description="Requested/resolved/responding route change, when observed.")
    caller_id: str = Field(default="", description="Stable identity of the system or agent that initiated the request.")
    caller_type: str = Field(default="", description="Caller class, such as repoai, chatgpt, or mcp.")
    caller_metadata: dict[str, Any] | None = Field(default=None, description="Bounded caller provenance supplied with the request.")
    backend_base_url: str = Field(default="", description="Canonical Notion2API backend URL used by this MCP wrapper.")
    timeout_seconds: float | None = Field(default=None, description="HTTP timeout used by the MCP wrapper for backend calls.")
    session_state_path: str = Field(default="", description="Path to the MCP session state file.")
    local_conversations_db: str = Field(default="", description="Expected local Notion2API conversations DB path.")
    imported_history_db: str = Field(default="", description="Expected imported Notion history DB path.")
    session_name: str | None = Field(default=None, description="Normalized MCP session name. Omitted and legacy 'op' names are generated.")
    conversation_id: str | None = Field(default=None, description="Stable Notion2API conversation id used for the request.")
    session_created: bool | None = Field(default=None, description="True when the wrapper created a new MCP conversation binding.")
    status: str = Field(default="completed", description="MCP wrapper job status: completed, indeterminate_output, pending, running, error, stale, or cancelled.")
    request_id: str | None = Field(default=None, description="Idempotency key used to deduplicate or poll this MCP chat request.")
    job_id: str | None = Field(default=None, description="Pollable job id. Currently identical to request_id.")
    retry_safe: bool = Field(default=False, description="True when retrying with the same request_id is safe and will not resubmit.")
    wait_seconds: float | None = Field(default=None, description="Compatibility field. MCP chat submissions always return immediately for polling.")
    poll_hint: str = Field(default="", description="Human-readable polling instruction for pending or stale jobs.")
    error: str | None = Field(default=None, description="Error summary if the backend call failed or the job became stale.")
    response_text: str = Field(default="", description="Extracted assistant response text.")
    output_integrity: dict[str, Any] | None = Field(default=None, description="Bounded pre-persistence output-integrity receipt.")
    quarantined: bool = Field(default=False, description="Whether visible output was removed from normal conversation state.")
    attachment_required: bool = Field(default=False, description="Whether this request was required to include at least one verified attachment.")
    attachment_count: int = Field(default=0, description="Number of attachments verified before submission.")
    attachment_transfer_status: str = Field(default="not_requested", description="Attachment provenance state: verified, missing, or not_requested.")
    attachment_manifest: list[dict[str, Any]] = Field(default_factory=list, description="Redacted manifest of attachments actually submitted.")
    progress: dict[str, Any] | None = Field(default=None, description="Bounded public activity snapshot captured while the job runs.")
    remote_chat_id: str = Field(default="", description="Durable remote Notion chat/thread id, when available.")
    notion_thread_id: str = Field(default="", description="Compatibility alias for remote_chat_id.")
    raw: dict[str, Any] = Field(default_factory=dict, description="Raw backend response.")


class UploadPageFileOutput(BaseModel):
    ok: bool = Field(description="Whether the Notion page file upload succeeded.")
    page_id: str = Field(default="", description="Target Notion page id.")
    block_id: str = Field(default="", description="Created Notion file block id.")
    file_url: str = Field(default="", description="Stored Notion file URL, if returned.")
    signed_get_url: str = Field(default="", description="Signed download URL, if returned.")
    filename: str = Field(default="", description="Uploaded filename.")
    content_type: str = Field(default="", description="Validated upload MIME type.")
    size: int = Field(default=0, description="Uploaded file size in bytes.")
    error: str | None = Field(default=None, description="Error summary when the upload fails.")
    raw: dict[str, Any] = Field(default_factory=dict, description="Raw backend response.")


class StageFileOutput(BaseModel):
    ok: bool = Field(description="Whether the connector-transferred file was staged successfully.")
    staged_file_id: str = Field(default="", description="Opaque id used by later chat calls.")
    filename: str = Field(default="", description="Sanitized staged filename.")
    content_type: str = Field(default="", description="Validated MIME type.")
    size_bytes: int = Field(default=0, description="Validated file size.")
    sha256: str = Field(default="", description="SHA-256 digest of the staged bytes.")
    expires_at: int = Field(default=0, description="Unix epoch milliseconds when the staged id expires.")
    error: str | None = Field(default=None, description="Staging error summary.")


class ResponsesOutput(BaseModel):
    ok: bool = Field(description="Whether the responses endpoint call succeeded.")
    status_code: int | None = Field(default=None, description="HTTP status code returned by Notion2API.")
    model: str = Field(description="Requested model id passed through the MCP wrapper.")
    actual_model: str = Field(default="", description="Observed Notion model/provider route, if returned.")
    model_metadata: dict[str, Any] | None = Field(default=None, description="Notion2API model metadata, if any.")
    requested_model: str = Field(default="", description="Requested model alias originally passed to the MCP wrapper.")
    resolved_model: str = Field(default="", description="Concrete Notion route resolved from the requested alias.")
    verified_model: str = Field(default="", description="Responder model only when verified by upstream evidence.")
    model_identity_verified: bool = Field(default=False)
    model_identity_source: str = Field(default="")
    model_identity_confidence: str = Field(default="unverified")
    model_substitution: dict[str, Any] | None = Field(default=None)
    caller_id: str = Field(default="")
    caller_type: str = Field(default="")
    caller_metadata: dict[str, Any] | None = Field(default=None)
    backend_base_url: str = Field(default="", description="Canonical Notion2API backend URL used by this MCP wrapper.")
    timeout_seconds: float | None = Field(default=None, description="HTTP timeout used by the MCP wrapper for backend calls.")
    session_state_path: str = Field(default="", description="Path to the MCP session state file.")
    local_conversations_db: str = Field(default="", description="Expected local Notion2API conversations DB path.")
    imported_history_db: str = Field(default="", description="Expected imported Notion history DB path.")
    status: str = Field(
        default="completed",
        description="Terminal response state: completed, error, or indeterminate_output.",
    )
    response_text: str = Field(default="", description="Extracted response output text.")
    output_integrity: dict[str, Any] | None = Field(
        default=None,
        description="Bounded pre-return output-integrity receipt.",
    )
    quarantined: bool = Field(
        default=False,
        description="Whether visible output was withheld from the normal response projection.",
    )
    error: str | None = Field(default=None, description="Error summary if the responses request failed.")
    attachment_required: bool = Field(default=False, description="Whether at least one attachment was required.")
    attachment_count: int = Field(default=0, description="Number of verified attachments submitted.")
    attachment_transfer_status: str = Field(default="not_requested", description="Attachment provenance state: verified, missing, or not_requested.")
    attachment_manifest: list[dict[str, Any]] = Field(default_factory=list, description="Redacted manifest of submitted attachments.")
    raw: dict[str, Any] = Field(default_factory=dict, description="Raw backend response.")


class ListSessionsOutput(BaseModel):
    ok: bool = Field(default=True, description="Whether the session listing succeeded.")
    count: int = Field(description="Number of known MCP sessions.")
    default_session: str = Field(description="Default session policy. New chats are auto-named; explicit op remains a shared legacy alias.")
    state_path: str = Field(description="Path to the MCP session state file.")
    sessions: list[dict[str, Any]] = Field(default_factory=list, description="Known named MCP session bindings and remote thread metadata.")


class UnsafeUrlContinuationOutput(BaseModel):
    ok: bool = Field(description="Whether Notion accepted and applied the allow-once continuation request.")
    continued: bool = Field(default=False, description="Whether a pending inference was resumed.")
    approved: bool = Field(default=False, description="Whether the requested tool-step confirmation was actually applied.")
    session_name: str = Field(default="", description="Resolved MCP session name, when used.")
    thread_id: str = Field(default="", description="Remote Notion thread id.")
    tool_step_ids: list[str] = Field(default_factory=list, description="Confirmed Notion agent tool-result step ids.")
    urls: list[str] = Field(default_factory=list, description="Pending URLs discovered from Notion sync records.")
    trace_id: str = Field(default="", description="Trace id of the resumed inference.")
    stream_completed: bool = Field(default=False, description="Whether the resumed inference stream completed.")
    event_count: int = Field(default=0, description="Number of resumed inference events received.")
    event_types: list[str] = Field(default_factory=list, description="Distinct resumed inference event types.")
    applied_tool_step_ids: list[str] = Field(default_factory=list, description="Requested step ids observed in an applied state after continuation.")
    unresolved_tool_step_ids: list[str] = Field(default_factory=list, description="Requested step ids that remained confirmation-gated after continuation.")
    reason: str = Field(default="", description="Approval outcome or reason no continuation occurred.")
    raw: dict[str, Any] = Field(default_factory=dict, description="Raw Notion2API endpoint response.")


class SessionActionOutput(BaseModel):
    ok: bool = Field(description="Whether the session operation succeeded.")
    action: str = Field(description="Operation performed: reset or rename.")
    session_name: str = Field(description="Normalized target session name.")
    conversation_id: str = Field(description="Conversation id now bound to the target session.")
    previous_session_name: str | None = Field(default=None, description="Previous session name for rename operations.")
    previous_conversation_id: str | None = Field(default=None, description="Prior conversation id replaced or renamed, if any.")
    overwritten: bool = Field(default=False, description="Whether an existing target session was overwritten.")
    state_path: str = Field(description="Path to the MCP session state file.")


class MessagesOutput(BaseModel):
    ok: bool = Field(description="Whether local conversation messages were read successfully.")
    session_name: str = Field(default="", description="Normalized MCP session name used for lookup.")
    conversation_id: str = Field(default="", description="Resolved conversation id.")
    count: int = Field(default=0, description="Number of returned messages.")
    total_count: int = Field(default=0, description="Total local message count for the conversation.")
    db_path: str = Field(default="", description="Local Notion2API conversations database path.")
    persistence_source: str = Field(default="conversation_db", description="Source of readback: 'conversation_db' or 'mcp_job_store'.")
    durable_persisted: bool = Field(default=True, description="Whether messages are durably persisted in SQLite database.")
    reconciliation_required: bool = Field(default=False, description="Whether SQLite database reconciliation is required.")
    messages: list[dict[str, Any]] = Field(default_factory=list, description="Messages in chronological order.")
    error: str | None = Field(default=None, description="Error summary if messages could not be read.")


class LastResponseOutput(BaseModel):
    ok: bool = Field(description="Whether the local last-response lookup completed.")
    found: bool = Field(default=False, description="Whether an assistant response was found.")
    session_name: str = Field(default="", description="Normalized MCP session name used for lookup.")
    conversation_id: str = Field(default="", description="Resolved conversation id.")
    response_text: str = Field(default="", description="Latest assistant visible response content.")
    message: dict[str, Any] | None = Field(default=None, description="Latest assistant message record, if found.")
    db_path: str = Field(default="", description="Local Notion2API conversations database path.")
    persistence_source: str = Field(default="conversation_db", description="Source of readback: 'conversation_db' or 'mcp_job_store'.")
    durable_persisted: bool = Field(default=True, description="Whether response is durably persisted in SQLite database.")
    reconciliation_required: bool = Field(default=False, description="Whether SQLite database reconciliation is required.")
    error: str | None = Field(default=None, description="Error summary if lookup failed.")


class ChatJobOutput(BaseModel):
    ok: bool = Field(description="Whether the chat job lookup completed.")
    found: bool = Field(default=False, description="Whether a job with this request_id exists.")
    status: str = Field(default="", description="Persisted job status: running, pending, completed, indeterminate_output, error, stale, or cancelled.")
    request_id: str = Field(default="", description="Idempotency key / job id.")
    job_id: str = Field(default="", description="Pollable job id. Currently identical to request_id.")
    session_name: str = Field(default="", description="Normalized MCP session name.")
    conversation_id: str = Field(default="", description="Conversation id associated with the job.")
    model: str = Field(default="", description="Requested model for the job.")
    endpoint: str = Field(default="", description="Backend endpoint used by the job.")
    created_at: int = Field(default=0, description="Unix epoch milliseconds when the job was created.")
    updated_at: int = Field(default=0, description="Unix epoch milliseconds when the job was last updated.")
    response_text: str = Field(default="", description="Bounded completed-response preview unless include_response is true. Empty when quarantined.")
    response_chars: int = Field(default=0, description="Full completed-response character count, including generated-but-quarantined text.")
    response_truncated: bool = Field(default=False, description="Whether response_text is a bounded preview.")
    output_integrity: dict[str, Any] | None = Field(default=None, description="Bounded pre-persistence output-integrity receipt.")
    quarantined: bool = Field(default=False, description="Whether visible output was removed from normal conversation state.")
    authoritative: bool = Field(
        default=True,
        description="Whether response_text is safe to treat as an authoritative assistant answer. False when quarantined.",
    )
    quarantined_response_available: bool = Field(
        default=False,
        description="Whether generated text was retained as quarantined evidence even though it was withheld from response_text.",
    )
    quarantined_response_text: str = Field(
        default="",
        description="Generated-but-quarantined text when include_quarantined/include_response is true. Not authoritative.",
    )
    attachment_required: bool = Field(default=False, description="Whether the request required one or more verified attachments.")
    attachment_count: int = Field(default=0, description="Number of verified attachments submitted.")
    attachment_transfer_status: str = Field(default="not_requested", description="Attachment provenance state: verified, missing, or not_requested.")
    attachment_manifest: list[dict[str, Any]] = Field(default_factory=list, description="Redacted manifest of submitted attachments.")
    progress: dict[str, Any] | None = Field(default=None, description="Latest bounded activity/checklist snapshot.")
    remote_chat_id: str = Field(default="", description="Durable remote Notion chat/thread id, when available.")
    notion_thread_id: str = Field(default="", description="Compatibility alias for remote_chat_id.")
    poll_count: int = Field(default=0, description="Number of explicit status polls recorded for this job.")
    stalled_for_seconds: float = Field(default=0.0, description="Seconds since meaningful public progress changed.")
    dead_loop_suspected: bool = Field(default=False, description="Whether the job appears stalled and may require cancellation.")
    cancel_recommended: bool = Field(default=False, description="Whether cancellation should be considered before further polling.")
    response: dict[str, Any] | None = Field(default=None, description="Persisted ChatOutput-compatible response, if available.")
    error: str | None = Field(default=None, description="Persisted error summary, if any.")
    raw_job: dict[str, Any] = Field(default_factory=dict, description="Raw persisted job state.")
    last_response: dict[str, Any] | None = Field(default=None, description="Optional latest local assistant response lookup.")



def prepare_mcp_file_attachments(
    files: list[str] | None,
) -> list[dict[str, Any]]:
    if not files:
        return []

    from app.attachments.errors import AttachmentError
    from app.attachments.loader import infer_content_type
    from app.attachments.security import AttachmentPolicy, validate_attachment_count, validate_content_type, validate_size
    import base64
    from pathlib import Path

    policy = AttachmentPolicy.from_env()
    if not policy.enabled:
        raise AttachmentError(
            "Attachments are disabled for this server.",
            code="attachments_disabled",
            param="attachments",
        )

    validate_attachment_count(len(files), policy)

    prepared = []
    for file_path in files:
        path = Path(file_path)
        if not path.exists():
            raise AttachmentError(
                f"Attachment path does not exist: {file_path}",
                code="attachment_not_found",
                param="attachments",
            )
        if not path.is_file():
            raise AttachmentError(
                f"Attachment path is not a file: {file_path}",
                code="invalid_attachment_type",
                param="attachments",
            )

        size = path.stat().st_size
        validate_size(size, policy)

        mime_type = validate_content_type(infer_content_type(path.name), policy)

        data = path.read_bytes()
        encoded = base64.b64encode(data).decode("utf-8")
        prepared.append({
            "name": path.name,
            "content_type": mime_type,
            "size_bytes": size,
            "source": "mcp_file",
            "data": f"data:{mime_type};base64,{encoded}",
        })

    return prepared


TransferredFile = Annotated[
    str,
    Field(
        description=(
            "A file supplied by the MCP client. This must be a top-level file argument so "
            "ChatGPT can transfer /mnt/data uploads into the connector runtime."
        ),
        json_schema_extra={"format": "file"},
    ),
]


def stage_mcp_file_for_page(file_path: str, filename: str | None = None) -> tuple[Path, bool]:
    """Stage a connector-transferred file under the backend's allowed local root."""

    from app.attachments.errors import AttachmentError
    from app.attachments.security import AttachmentPolicy, validate_size
    import shutil

    policy = AttachmentPolicy.from_env()
    if not policy.enabled:
        raise AttachmentError(
            "Attachments are disabled for this server.",
            code="attachments_disabled",
            param="file",
        )

    source = Path(str(file_path or "")).expanduser()
    if not source.exists() or not source.is_file():
        raise AttachmentError(
            f"Transferred file path does not exist: {file_path}",
            code="attachment_not_found",
            param="file",
        )
    validate_size(source.stat().st_size, policy)

    requested_name = Path(str(filename or source.name)).name.strip()
    if not requested_name or requested_name in {".", ".."}:
        raise AttachmentError(
            "A valid upload filename is required.",
            code="invalid_upload_filename",
            param="filename",
        )

    allowed_root = Path(policy.local_root).expanduser().resolve()
    resolved_source = source.resolve()
    try:
        resolved_source.relative_to(allowed_root)
        return resolved_source, False
    except ValueError:
        pass

    staging_dir = allowed_root / "chatgpt-file-uploads" / uuid.uuid4().hex
    staging_dir.mkdir(parents=True, exist_ok=False)
    staged = staging_dir / requested_name
    shutil.copy2(resolved_source, staged)
    return staged, True


def cleanup_staged_mcp_file(path: Path, staged: bool) -> None:
    if not staged:
        return
    import shutil

    with contextlib.suppress(OSError):
        shutil.rmtree(path.parent)


def _staged_file_ttl_seconds() -> int:
    raw = os.getenv("MCP_NOTION2API_STAGED_FILE_TTL_SECONDS", "").strip()
    try:
        value = int(raw) if raw else DEFAULT_STAGED_FILE_TTL_SECONDS
    except ValueError:
        value = DEFAULT_STAGED_FILE_TTL_SECONDS
    return max(60, min(value, 7 * 24 * 60 * 60))


def _staged_file_root() -> Path:
    from app.attachments.security import AttachmentPolicy

    policy = AttachmentPolicy.from_env()
    root = Path(policy.local_root).expanduser().resolve() / "chatgpt-file-uploads"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _cleanup_expired_staged_files(root: Path | None = None) -> None:
    import shutil

    root = root or _staged_file_root()
    now = _now_ms()
    for candidate in root.glob("stage-*"):
        if not candidate.is_dir() or not STAGED_FILE_ID_RE.fullmatch(candidate.name):
            continue
        metadata_path = candidate / "stage.json"
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
            expires_at = int(metadata.get("expires_at") or 0)
        except Exception:
            expires_at = 0
        if expires_at and expires_at > now:
            continue
        with contextlib.suppress(OSError):
            shutil.rmtree(candidate)


def stage_mcp_transferred_file(file_path: str, filename: str | None = None) -> dict[str, Any]:
    """Persist one connector-transferred file and return an opaque, expiring id."""

    import hashlib
    import shutil
    from app.attachments.errors import AttachmentError
    from app.attachments.loader import infer_content_type
    from app.attachments.security import AttachmentPolicy, validate_content_type, validate_size

    policy = AttachmentPolicy.from_env()
    if not policy.enabled:
        raise AttachmentError("Attachments are disabled for this server.", code="attachments_disabled", param="file")

    source = Path(str(file_path or "")).expanduser().resolve()
    if not source.exists() or not source.is_file():
        raise AttachmentError(
            f"Transferred file path does not exist: {file_path}",
            code="attachment_not_found",
            param="file",
        )
    validate_size(source.stat().st_size, policy)
    safe_name = Path(str(filename or source.name)).name.strip()
    if not safe_name or safe_name in {".", ".."}:
        raise AttachmentError("A valid staged filename is required.", code="invalid_upload_filename", param="filename")

    content_type = validate_content_type(infer_content_type(safe_name), policy)

    root = _staged_file_root()
    _cleanup_expired_staged_files(root)
    staged_file_id = f"stage-{uuid.uuid4().hex}"
    stage_dir = root / staged_file_id
    stage_dir.mkdir(parents=False, exist_ok=False)
    staged_path = stage_dir / safe_name
    shutil.copy2(source, staged_path)
    data = staged_path.read_bytes()
    now = _now_ms()
    metadata = {
        "staged_file_id": staged_file_id,
        "filename": safe_name,
        "content_type": content_type,
        "size_bytes": len(data),
        "sha256": hashlib.sha256(data).hexdigest(),
        "created_at": now,
        "expires_at": now + (_staged_file_ttl_seconds() * 1000),
    }
    (stage_dir / "stage.json").write_text(json.dumps(metadata, indent=2, sort_keys=True), encoding="utf-8")
    return {**metadata, "path": str(staged_path)}


def resolve_mcp_staged_files(staged_file_ids: list[str] | None) -> list[str]:
    """Resolve opaque staged ids to validated service-host paths."""

    import shutil
    from app.attachments.errors import AttachmentError
    from app.attachments.security import AttachmentPolicy, validate_attachment_count, validate_size

    if not staged_file_ids:
        return []
    policy = AttachmentPolicy.from_env()
    if not policy.enabled:
        raise AttachmentError("Attachments are disabled for this server.", code="attachments_disabled", param="staged_file_ids")
    validate_attachment_count(len(staged_file_ids), policy)
    root = _staged_file_root()
    _cleanup_expired_staged_files(root)
    resolved: list[str] = []
    now = _now_ms()
    for raw_id in staged_file_ids:
        staged_file_id = str(raw_id or "").strip().lower()
        if not STAGED_FILE_ID_RE.fullmatch(staged_file_id):
            raise AttachmentError("Invalid staged file id.", code="invalid_staged_file_id", param="staged_file_ids")
        stage_dir = (root / staged_file_id).resolve()
        try:
            stage_dir.relative_to(root)
        except ValueError as exc:
            raise AttachmentError("Invalid staged file path.", code="invalid_staged_file_id", param="staged_file_ids") from exc
        metadata_path = stage_dir / "stage.json"
        try:
            metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise AttachmentError("Staged file metadata was not found.", code="staged_file_not_found", param="staged_file_ids") from exc
        if int(metadata.get("expires_at") or 0) <= now:
            with contextlib.suppress(OSError):
                shutil.rmtree(stage_dir)
            raise AttachmentError("Staged file id has expired.", code="staged_file_expired", param="staged_file_ids")
        safe_name = Path(str(metadata.get("filename") or "")).name
        staged_path = (stage_dir / safe_name).resolve()
        try:
            staged_path.relative_to(stage_dir)
        except ValueError as exc:
            raise AttachmentError("Invalid staged file metadata.", code="invalid_staged_file_id", param="staged_file_ids") from exc
        if not staged_path.exists() or not staged_path.is_file():
            raise AttachmentError("Staged file bytes were not found.", code="staged_file_not_found", param="staged_file_ids")
        validate_size(staged_path.stat().st_size, policy)
        resolved.append(str(staged_path))
    return resolved


FileAttachments = Annotated[
    list[str] | None,
    Field(
        default=None,
        description=(
            "Service-host local file paths visible to the Notion2API process. Do not pass "
            "ChatGPT /mnt/data paths here; stage each ChatGPT upload with notion2api_stage_file "
            "or use notion2api_chat_with_file for one file."
        ),
    ),
]

StagedFileIds = Annotated[
    list[str] | None,
    Field(
        default=None,
        description=(
            "Opaque ids returned by notion2api_stage_file. Use one staging call per ChatGPT "
            "upload, then pass all returned ids here for a multi-file request."
        ),
    ),
]

MCPModel = Annotated[
    str,
    Field(
        description=(
            "Model id. Omit this argument to use Terra. Only pass another model when the "
            "user explicitly requests that model."
        )
    ),
]

MCPSessionName = Annotated[
    str | None,
    Field(
        description=(
            "Session name. Omit this argument to generate a task-specific session. The legacy "
            "name 'op' is also normalized to a generated session."
        )
    ),
]

MCPWaitSeconds = Annotated[
    float | None,
    Field(
        description=(
            "Deprecated compatibility argument; it is ignored. Chat submissions return "
            "immediately with a request_id to poll via notion2api_get_chat_job."
        )
    ),
]

MCPNotionMode = Annotated[
    Literal["default", "ask", "research"],
    Field(description="Notion AI mode: default can search and edit; ask is read-only; research enables deeper research."),
]
MCPNotionTask = Annotated[
    Literal["visualize", "create_slides", "spreadsheet", "deep_research"] | None,
    Field(description="Optional Notion AI task preset for visualizations, slide decks, spreadsheets, or deep research."),
]
MCPNotionSources = Annotated[
    list[str] | None,
    Field(
        description=(
            "Notion AI source scopes. Common values: all, notion, web, notion-help-center, github, "
            "gmail, google-calendar, and google-drive. Omit to use Notion's defaults."
        )
    ),
]
MCPWebAccess = Annotated[
    bool | None,
    Field(description="Explicitly enable or disable Notion AI web search. Omit to use the mode/source default."),
]
MCPNotionPersona = Annotated[
    Literal["sidekick", "minimalist", "analyst"] | None,
    Field(description="Optional response-style preset: warm, concise, or structured and logical."),
]
MCPNotionInstructions = Annotated[
    str | None,
    Field(description="Optional per-request Notion AI instructions, applied in addition to the selected task/persona."),
]


class Notion2APIClient:
    """Small HTTP client used by MCP tools to call the existing Notion2API API."""

    def __init__(self, base_url: str, api_key: str | None = None, timeout: float = DEFAULT_TIMEOUT_SECONDS) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = (api_key or "").strip()
        self.timeout = timeout

    def _headers(self) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def get(self, path: str) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(f"{self.base_url}{path}", headers=self._headers())
        return _json_or_error(response)

    async def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(f"{self.base_url}{path}", headers=self._headers(), json=payload)
        return _json_or_error(response)

    async def post_chat_stream(self, path: str, payload: dict[str, Any], on_progress: Any) -> dict[str, Any]:
        stream_payload = dict(payload)
        stream_payload["stream"] = True
        content_parts: list[str] = []
        reasoning_buffer = ""
        model_metadata: dict[str, Any] = {}
        response_model = str(payload.get("model") or "")
        event_count = 0
        last_update = 0.0

        headers = self._headers()
        headers["Accept"] = "text/event-stream"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream("POST", f"{self.base_url}{path}", headers=headers, json=stream_payload) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    return _json_or_error(httpx.Response(response.status_code, headers=response.headers, content=body))
                remote_conversation_id = str(response.headers.get("X-Conversation-Id") or "").strip()
                remote_chat_id = str(response.headers.get("X-Notion-Thread-Id") or "").strip()
                if remote_conversation_id:
                    model_metadata.setdefault("conversation_id", remote_conversation_id)
                if remote_chat_id:
                    model_metadata.setdefault("notion_thread_id", remote_chat_id)
                    model_metadata.setdefault("remote_chat_id", remote_chat_id)
                async for line in response.aiter_lines():
                    if not line.startswith("data:"):
                        continue
                    raw = line[5:].strip()
                    if not raw or raw == "[DONE]":
                        continue
                    try:
                        event = json.loads(raw)
                    except ValueError:
                        continue
                    if not isinstance(event, dict):
                        continue
                    response_model = str(event.get("model") or response_model)
                    if isinstance(event.get("model_metadata"), dict):
                        model_metadata.update(event["model_metadata"])
                    choices = event.get("choices")
                    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
                        continue
                    delta = choices[0].get("delta")
                    if not isinstance(delta, dict):
                        continue
                    content = delta.get("content")
                    reasoning = delta.get("reasoning_content") or delta.get("thinking")
                    if isinstance(content, str) and content:
                        content_parts.append(content)
                    if isinstance(reasoning, str) and reasoning:
                        reasoning_buffer = (reasoning_buffer + reasoning)[
                            -MAX_PROGRESS_REASONING_CHARS:
                        ]
                    if content or reasoning:
                        event_count += 1
                        now = time.monotonic()
                        if now - last_update >= 0.75:
                            on_progress(
                                reasoning_buffer,
                                "".join(content_parts),
                                event_count,
                                False,
                            )
                            last_update = now

        on_progress(reasoning_buffer, "".join(content_parts), event_count, True)
        # Persist only the final visible answer. Raw reasoning is used transiently
        # to derive the bounded progress snapshot and is never stored in MCP jobs.
        message: dict[str, Any] = {"role": "assistant", "content": "".join(content_parts)}
        return {
            "ok": True,
            "status_code": 200,
            "model": response_model,
            "actual_model": str(model_metadata.get("actual_model") or ""),
            "model_metadata": model_metadata or None,
            "choices": [{"index": 0, "message": message, "finish_reason": "stop"}],
        }


def _json_or_error(response: httpx.Response) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "")
    try:
        data: Any = response.json() if "json" in content_type.lower() or response.content else {}
    except ValueError:
        data = {"text": response.text[:4000]}

    if response.status_code >= 400:
        return {
            "ok": False,
            "status_code": response.status_code,
            "error": data,
        }
    if isinstance(data, dict):
        data.setdefault("ok", True)
        data.setdefault("status_code", response.status_code)
        conversation_id = str(response.headers.get("X-Conversation-Id") or "").strip()
        remote_chat_id = str(response.headers.get("X-Notion-Thread-Id") or "").strip()
        if conversation_id or remote_chat_id:
            metadata = data.get("model_metadata") if isinstance(data.get("model_metadata"), dict) else {}
            if conversation_id:
                metadata.setdefault("conversation_id", conversation_id)
                data.setdefault("conversation_id", conversation_id)
            if remote_chat_id:
                metadata.setdefault("notion_thread_id", remote_chat_id)
                metadata.setdefault("remote_chat_id", remote_chat_id)
                data.setdefault("notion_thread_id", remote_chat_id)
            data["model_metadata"] = metadata
        return data
    return {"ok": True, "status_code": response.status_code, "data": data}


def _string_or_none(value: Any) -> str | None:
    if isinstance(value, str) and value.strip():
        return value.strip()
    return None


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    return None


def _model_info_from_entry(entry: Any) -> ModelInfo | None:
    if not isinstance(entry, dict):
        return None
    model_id = _string_or_none(entry.get("id"))
    if not model_id:
        return None
    return ModelInfo(
        id=model_id,
        object=_string_or_none(entry.get("object")),
        created=_int_or_none(entry.get("created")),
        owned_by=_string_or_none(entry.get("owned_by")),
    )


def _error_summary(data: dict[str, Any]) -> str | None:
    error = data.get("error") if isinstance(data, dict) else None
    if error is None:
        return None
    if isinstance(error, str):
        return error[:1000]
    if isinstance(error, dict):
        message = error.get("message")
        if isinstance(message, str) and message.strip():
            return message.strip()[:1000]
        try:
            return json.dumps(error, ensure_ascii=False)[:1000]
        except Exception:
            return str(error)[:1000]
    return str(error)[:1000]


def _extract_actual_model(data: dict[str, Any]) -> str:
    metadata = data.get("model_metadata") if isinstance(data.get("model_metadata"), dict) else {}
    actual = metadata.get("actual_model") if isinstance(metadata, dict) else None
    if isinstance(actual, str) and actual.strip():
        return actual.strip()
    direct = data.get("actual_model")
    return direct.strip() if isinstance(direct, str) and direct.strip() else ""


def _caller_trace(metadata: dict[str, Any] | None) -> dict[str, Any]:
    raw = metadata.get("caller") if isinstance(metadata, dict) else None
    caller = dict(raw) if isinstance(raw, dict) else {}
    caller_id = str(caller.get("id") or caller.get("caller_id") or "").strip()
    caller_type = str(caller.get("type") or caller.get("caller_type") or "").strip()
    bounded = {
        str(key): value
        for key, value in caller.items()
        if str(key) in {
            "id", "caller_id", "type", "caller_type", "project_id", "run_id",
            "job_id", "review_instance_id", "team_id", "manager_id", "request_origin",
        }
        and value not in (None, "", [], {})
    }
    return {
        "caller_id": caller_id,
        "caller_type": caller_type,
        "caller_metadata": bounded or None,
    }


def _model_identity_trace(
    data: dict[str, Any], requested_model: str
) -> dict[str, Any]:
    metadata = (
        dict(data.get("model_metadata"))
        if isinstance(data.get("model_metadata"), dict)
        else {}
    )
    requested = str(metadata.get("requested_model") or requested_model or "").strip()
    resolved = str(
        metadata.get("notion_requested_model")
        or metadata.get("resolved_model")
        or requested
    ).strip()
    observed = _extract_actual_model(data)
    verified = bool(metadata.get("actual_model_verified") is True and observed)
    source = str(
        metadata.get("actual_model_source")
        or metadata.get("model_identity_source")
        or ("authoritative_upstream_metadata" if verified else "")
    ).strip()
    if verified:
        confidence = "verified"
        verified_model = observed
    elif observed:
        confidence = "observed"
        verified_model = ""
        source = source or "unverified_upstream_observation"
    else:
        confidence = "unverified"
        verified_model = ""
        source = source or "no_responder_identity_evidence"
    substitution = None
    comparison_model = verified_model or observed
    if comparison_model and resolved and comparison_model != resolved:
        substitution = {
            "requested_model": requested,
            "resolved_model": resolved,
            "responding_model": comparison_model,
            "verified": verified,
        }
    return {
        "requested_model": requested,
        "resolved_model": resolved,
        "verified_model": verified_model,
        "model_identity_verified": verified,
        "model_identity_source": source,
        "model_identity_confidence": confidence,
        "model_substitution": substitution,
    }


def _extract_remote_chat_id(data: dict[str, Any]) -> str:
    metadata = data.get("model_metadata") if isinstance(data.get("model_metadata"), dict) else {}
    for key in ("notion_thread_id", "remote_chat_id", "thread_id", "chat_id"):
        value = metadata.get(key) if isinstance(metadata, dict) else None
        if isinstance(value, str) and value.strip():
            return value.strip()
    for key in ("notion_thread_id", "remote_chat_id", "thread_id", "chat_id"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _extract_chat_content(data: dict[str, Any]) -> str:
    choices = data.get("choices")
    if isinstance(choices, list) and choices:
        message = choices[0].get("message") if isinstance(choices[0], dict) else None
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                return content
            if isinstance(content, list):
                parts: list[str] = []
                for item in content:
                    if isinstance(item, dict):
                        text = item.get("text") or item.get("content")
                        if isinstance(text, str):
                            parts.append(text)
                return "\n".join(parts)
    return ""


def _extract_responses_text(data: dict[str, Any]) -> str:
    direct = data.get("output_text")
    if isinstance(direct, str):
        return direct
    output = data.get("output")
    if not isinstance(output, list):
        return ""
    parts: list[str] = []
    for item in output:
        if not isinstance(item, dict):
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, dict) and isinstance(block.get("text"), str):
                parts.append(block["text"])
    return "\n".join(parts)


def _responses_output_from_backend(
    *,
    data: dict[str, Any],
    client: Notion2APIClient,
    model: str,
    provenance: dict[str, Any],
) -> dict[str, Any]:
    """Build a fail-closed MCP responses projection from backend data."""

    ok = bool(data.get("ok", False))
    base = {
        "ok": ok,
        "status_code": data.get("status_code"),
        "status": "completed" if ok else "error",
        "model": _extract_actual_model(data) or data.get("model") or model,
        "actual_model": _extract_actual_model(data),
        "model_metadata": (
            data.get("model_metadata")
            if isinstance(data.get("model_metadata"), dict)
            else None
        ),
        **_model_identity_trace(data, model),
        **_caller_trace(
            data.get("request_metadata")
            if isinstance(data.get("request_metadata"), dict)
            else None
        ),
        **_runtime_audit(client, model),
        "response_text": _extract_responses_text(data),
        "error": _error_summary(data),
        **provenance,
        "raw": {**data, "attachment_provenance": provenance},
    }
    normalized, evidence = _normalize_terminal_output(
        base,
        source="responses_endpoint",
    )
    if evidence is None:
        return normalized

    receipt = normalized.get("output_integrity")
    logger.error(
        "Quarantined contaminated responses-endpoint output",
        extra={
            "request_info": {
                "event": "output_quarantined",
                "error_code": "OUTPUT_CONTAMINATED",
                "source": "responses_endpoint",
                "output_integrity": receipt,
                "normal_projection_blocked": True,
            }
        },
    )
    normalized["raw"] = {
        "quarantined": True,
        "source": "responses_endpoint",
        "output_integrity": receipt,
        "attachment_provenance": provenance,
        "generated_response_available": bool(
            isinstance(receipt, dict) and int(receipt.get("response_chars") or 0) > 0
        ),
        "generated_response_chars": int(
            receipt.get("response_chars") or 0
        ) if isinstance(receipt, dict) else 0,
        "delivery_state": "generated_but_quarantined",
    }
    return normalized


def _local_conversation_db_path() -> Path:
    root = Path(__file__).resolve().parents[1]
    configured = os.getenv("DB_PATH", "").strip()
    if configured:
        path = Path(configured)
        return path if path.is_absolute() else root / path
    return root / "data" / "conversations.db"


def _runtime_audit(client: Notion2APIClient, requested_model: str) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[1]
    return {
        "requested_model": requested_model,
        "backend_base_url": client.base_url,
        "timeout_seconds": client.timeout,
        "session_state_path": str(DEFAULT_SESSION_STATE_PATH),
        "local_conversations_db": str(_local_conversation_db_path()),
        "imported_history_db": str(root / "data" / "chat_history.db"),
    }


def _now_ms() -> int:
    return int(time.time() * 1000)


def _atomic_write_json(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.{uuid.uuid4().hex}.tmp")
    last_error: OSError | None = None
    try:
        with tmp.open("w", encoding="utf-8") as handle:
            json.dump(data, handle, indent=2, sort_keys=True, ensure_ascii=False)
            handle.flush()
            try:
                os.fsync(handle.fileno())
            except OSError:
                # Some network/sandbox filesystems do not support fsync. The
                # replace retry below is still the important Windows hardening.
                pass

        for attempt in range(CHAT_JOB_STATE_WRITE_RETRIES):
            try:
                os.replace(tmp, path)
                return
            except OSError as exc:
                last_error = exc
                if attempt >= CHAT_JOB_STATE_WRITE_RETRIES - 1:
                    break
                time.sleep(CHAT_JOB_STATE_WRITE_BACKOFF_SECONDS * (2 ** attempt))

        assert last_error is not None
        logger.warning(
            "Failed to atomically replace chat job state after retries",
            extra={
                "request_info": {
                    "event": "chat_job_state_replace_failed",
                    "path": str(path),
                    "tmp_path": str(tmp),
                    "error": f"{type(last_error).__name__}: {last_error}",
                }
            },
        )
        raise last_error
    finally:
        if tmp.exists():
            try:
                tmp.unlink()
            except OSError:
                pass


def _safe_float(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _configured_chat_stall_seconds() -> float:
    raw = os.getenv("MCP_NOTION2API_STALL_SECONDS", "")
    configured = _safe_float(raw, DEFAULT_CHAT_STALL_SECONDS) if raw.strip() else DEFAULT_CHAT_STALL_SECONDS
    return max(15.0, configured)


def _bounded_chat_wait_seconds(wait_seconds: float | None) -> float:
    # Retain the argument for compatibility with older MCP clients, but never
    # hold an MCP request open while the backend job runs.
    return 0.0


def _normalize_request_id(request_id: str | None = None) -> str:
    raw = (request_id or "").strip()
    if not raw:
        return f"mcp-chat-{uuid.uuid4().hex}"
    normalized = re.sub(r"[^A-Za-z0-9_.:-]+", "-", raw).strip("-._:")
    return (normalized or f"mcp-chat-{uuid.uuid4().hex}")[:160]


def _session_key(session_name: str | None) -> str:
    raw = (session_name or DEFAULT_SESSION_NAME).strip().lower()
    key = re.sub(r"[^a-z0-9_.-]+", "-", raw).strip("-._")
    return key or DEFAULT_SESSION_NAME


def _infer_session_name(
    session_name: str | None,
    prompt: str,
    *,
    request_id: str | None = None,
) -> str:
    explicit = str(session_name or "").strip()
    if explicit and explicit.lower() not in {
        "auto",
        "inferred",
        "new",
        LEGACY_SHARED_SESSION_NAME,
    }:
        return _session_key(explicit)

    normalized = re.sub(r"\s+", " ", str(prompt or "")).strip().lower()
    words = re.findall(r"[a-z0-9]+", normalized)
    ignored = {
        "a", "an", "and", "are", "be", "by", "for", "from", "in", "is", "it",
        "of", "on", "or", "please", "the", "this", "to", "with", "you",
    }
    meaningful = [word for word in words if word not in ignored][:8]
    base = "-".join(meaningful)[:56].strip("-") or "chat"
    stable_request_id = str(request_id or "").strip()
    digest = (
        uuid.uuid5(uuid.NAMESPACE_URL, f"{normalized}|{stable_request_id}").hex[:8]
        if stable_request_id
        else uuid.uuid4().hex[:8]
    )
    return _session_key(f"{base}-{digest}")


def _explicit_prompt_messages(
    prompt: Any, system_prompt: Any = None
) -> list[dict[str, str]]:
    """Build messages only from explicit caller prompt fields."""

    user_text = validate_prompt_text(prompt, param="prompt")
    system_text = validate_prompt_text(
        system_prompt, param="system_prompt", allow_none=True
    )
    messages: list[dict[str, str]] = []
    if system_text:
        messages.append({"role": "system", "content": system_text})
    messages.append({"role": "user", "content": str(user_text or "")})
    validate_chat_messages(messages)
    return messages


def _copy_explicit_messages(messages: Any) -> list[dict[str, Any]]:
    """Validate and isolate caller messages from later polling/job mutations."""

    validate_chat_messages(messages)
    return copy.deepcopy(messages)


def _prompt_text_from_messages(messages: list[dict[str, Any]]) -> str:
    for message in reversed(messages):
        if not isinstance(message, dict) or str(message.get("role") or "").lower() != "user":
            continue
        content = message.get("content")
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict):
                    value = item.get("text") or item.get("content")
                    if isinstance(value, str):
                        parts.append(value)
            if parts:
                return "\n".join(parts)
    return ""


def _valid_chat_job_state(data: Any) -> dict[str, Any] | None:
    if isinstance(data, dict) and isinstance(data.get("jobs"), dict):
        return data
    return None


def _load_chat_job_state_file(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists() or not path.is_file():
            return None
        return _valid_chat_job_state(json.loads(path.read_text(encoding="utf-8")))
    except Exception:
        return None


def _chat_job_state_signature(path: Path) -> tuple[int, int, int, int] | None:
    try:
        stat = path.stat()
    except OSError:
        return None
    wal_path = path.with_name(f"{path.name}-wal")
    try:
        wal_stat = wal_path.stat()
        wal_signature = (wal_stat.st_mtime_ns, wal_stat.st_size)
    except OSError:
        wal_signature = (0, 0)
    return stat.st_mtime_ns, stat.st_size, *wal_signature


def _compact_chat_job_state(state: dict[str, Any]) -> None:
    jobs = state.get("jobs")
    if not isinstance(jobs, dict):
        return
    for job in jobs.values():
        if not isinstance(job, dict):
            continue
        response = job.get("response")
        response_text = job.get("response_text")
        if (
            isinstance(response, dict)
            and isinstance(response_text, str)
            and response.get("response_text") == response_text
        ):
            job.pop("response_text", None)


def _uses_sqlite_chat_job_ledger(path: Path) -> bool:
    return path.resolve() == DEFAULT_CHAT_JOB_STATE_PATH.resolve()


def _chat_job_storage_path(path: Path) -> Path:
    return DEFAULT_CHAT_JOB_DB_PATH if _uses_sqlite_chat_job_ledger(path) else path


@contextlib.contextmanager
def _chat_job_db(path: Path):
    conn = sqlite3.connect(str(path), timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    try:
        yield conn
    finally:
        conn.close()


def _ensure_chat_job_db_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS ledger_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS chat_jobs (
            request_id TEXT PRIMARY KEY,
            status TEXT NOT NULL DEFAULT '',
            conversation_id TEXT NOT NULL DEFAULT '',
            session_name TEXT NOT NULL DEFAULT '',
            model TEXT NOT NULL DEFAULT '',
            created_at INTEGER NOT NULL DEFAULT 0,
            updated_at INTEGER NOT NULL DEFAULT 0,
            payload_zlib BLOB NOT NULL,
            payload_sha256 TEXT NOT NULL,
            payload_bytes INTEGER NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_chat_jobs_status_updated
            ON chat_jobs(status, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_jobs_conversation_updated
            ON chat_jobs(conversation_id, updated_at DESC);
        CREATE INDEX IF NOT EXISTS idx_chat_jobs_session_updated
            ON chat_jobs(session_name, updated_at DESC);
        """
    )
    conn.execute(
        "INSERT OR REPLACE INTO ledger_metadata(key, value) VALUES ('schema_version', ?)",
        (str(CHAT_JOB_LEDGER_SCHEMA_VERSION),),
    )


def _encoded_chat_job(job: dict[str, Any]) -> tuple[bytes, str, int]:
    raw = json.dumps(
        job,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return zlib.compress(raw, level=6), hashlib.sha256(raw).hexdigest(), len(raw)


def _upsert_chat_jobs(
    conn: sqlite3.Connection,
    state: dict[str, Any],
    request_ids: set[str] | None = None,
) -> None:
    jobs = state.get("jobs") if isinstance(state.get("jobs"), dict) else {}
    ids = request_ids if request_ids is not None else {str(key) for key in jobs}
    for request_id in ids:
        job = jobs.get(request_id)
        if not isinstance(job, dict):
            continue
        payload, payload_hash, payload_bytes = _encoded_chat_job(job)
        conn.execute(
            """
            INSERT INTO chat_jobs(
                request_id, status, conversation_id, session_name, model,
                created_at, updated_at, payload_zlib, payload_sha256, payload_bytes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(request_id) DO UPDATE SET
                status = excluded.status,
                conversation_id = excluded.conversation_id,
                session_name = excluded.session_name,
                model = excluded.model,
                created_at = excluded.created_at,
                updated_at = excluded.updated_at,
                payload_zlib = excluded.payload_zlib,
                payload_sha256 = excluded.payload_sha256,
                payload_bytes = excluded.payload_bytes
            """,
            (
                request_id,
                str(job.get("status") or ""),
                str(job.get("conversation_id") or ""),
                str(job.get("session_name") or ""),
                str(job.get("model") or ""),
                int(job.get("created_at") or 0),
                int(job.get("updated_at") or 0),
                sqlite3.Binary(payload),
                payload_hash,
                payload_bytes,
            ),
        )


def _load_chat_job_db(path: Path) -> dict[str, Any]:
    jobs: dict[str, Any] = {}
    with _chat_job_db(path) as conn:
        rows = conn.execute(
            "SELECT request_id, payload_zlib, payload_sha256 FROM chat_jobs"
        ).fetchall()
    for row in rows:
        raw = zlib.decompress(bytes(row["payload_zlib"]))
        if hashlib.sha256(raw).hexdigest() != str(row["payload_sha256"]):
            raise ValueError(f"Chat job ledger checksum mismatch for {row['request_id']}")
        job = json.loads(raw)
        request_id = str(row["request_id"])
        if not isinstance(job, dict) or str(job.get("request_id") or "") != request_id:
            raise ValueError(f"Invalid chat job ledger payload for {request_id}")
        jobs[request_id] = job
    return {"jobs": jobs}


def _migrate_chat_job_json_to_sqlite(source_path: Path, db_path: Path) -> None:
    if db_path.exists():
        return
    state = _recover_chat_job_state(source_path)
    _compact_chat_job_state(state)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = db_path.with_name(f"{db_path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with _chat_job_db(tmp_path) as conn:
            conn.execute("PRAGMA journal_mode = DELETE")
            conn.execute("PRAGMA synchronous = FULL")
            _ensure_chat_job_db_schema(conn)
            _upsert_chat_jobs(conn, state)
            source_hash = (
                hashlib.sha256(source_path.read_bytes()).hexdigest()
                if source_path.exists()
                else ""
            )
            conn.execute(
                "INSERT OR REPLACE INTO ledger_metadata(key, value) VALUES ('source_json_sha256', ?)",
                (source_hash,),
            )
            conn.commit()
        if _load_chat_job_db(tmp_path) != state:
            raise ValueError("Chat job ledger migration verification failed")
        os.replace(tmp_path, db_path)
    finally:
        with contextlib.suppress(OSError):
            tmp_path.unlink()


def _ensure_chat_job_db(source_path: Path, db_path: Path) -> None:
    key = str(db_path.resolve())
    if key in _CHAT_JOB_DB_READY and db_path.exists():
        return
    _migrate_chat_job_json_to_sqlite(source_path, db_path)
    with _chat_job_db(db_path) as conn:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        _ensure_chat_job_db_schema(conn)
        conn.commit()
    _CHAT_JOB_DB_READY.add(key)


def _job_timestamp(job: Any) -> int:
    if not isinstance(job, dict):
        return 0
    for key in ("updated_at", "created_at"):
        value = job.get(key)
        if isinstance(value, int):
            return value
    return 0


def _merge_chat_job_states(base: dict[str, Any], candidate: dict[str, Any]) -> bool:
    changed = False
    base_jobs = base.setdefault("jobs", {})
    candidate_jobs = candidate.get("jobs", {})
    if not isinstance(base_jobs, dict) or not isinstance(candidate_jobs, dict):
        return False
    for request_id, candidate_job in candidate_jobs.items():
        if not isinstance(candidate_job, dict):
            continue
        request_key = str(request_id)
        existing = base_jobs.get(request_key)
        if not isinstance(existing, dict) or _job_timestamp(candidate_job) > _job_timestamp(existing):
            base_jobs[request_key] = candidate_job
            changed = True
    return changed


def _recover_chat_job_state(path: Path) -> dict[str, Any]:
    state = _load_chat_job_state_file(path) or {"jobs": {}}
    tmp_paths = sorted(
        path.parent.glob(f"{path.name}.*.tmp"),
        key=lambda item: item.stat().st_mtime if item.exists() else 0,
    )
    recovered = False
    valid_tmp_paths: list[Path] = []
    for tmp_path in tmp_paths:
        tmp_state = _load_chat_job_state_file(tmp_path)
        if tmp_state is None:
            continue
        valid_tmp_paths.append(tmp_path)
        recovered = _merge_chat_job_states(state, tmp_state) or recovered
    if valid_tmp_paths:
        try:
            _atomic_write_json(path, state)
        except OSError:
            logger.warning(
                "Recovered chat job state from temp files but could not promote canonical ledger",
                extra={
                    "request_info": {
                        "event": "chat_job_state_recovery_unpromoted",
                        "path": str(path),
                        "recovered": recovered,
                        "valid_temp_files": len(valid_tmp_paths),
                    }
                },
            )
        else:
            promoted = _load_chat_job_state_file(path)
            promotion_valid = promoted is not None
            if promotion_valid and promoted is not None:
                # A re-merge must be a no-op: the canonical file must contain
                # every recovered job at an equal or newer timestamp.
                promotion_valid = not _merge_chat_job_states(promoted, state)
            if promotion_valid:
                for tmp_path in valid_tmp_paths:
                    with contextlib.suppress(OSError):
                        tmp_path.unlink()
            else:
                logger.warning(
                    "Recovered chat job state could not be verified after promotion",
                    extra={
                        "request_info": {
                            "event": "chat_job_state_recovery_verification_failed",
                            "path": str(path),
                            "valid_temp_files": len(valid_tmp_paths),
                        }
                    },
                )
    return state


def _load_chat_job_state(path: Path = DEFAULT_CHAT_JOB_STATE_PATH) -> dict[str, Any]:
    storage_path = _chat_job_storage_path(path)
    key = str(storage_path.resolve())
    with _CHAT_JOB_STATE_MUTEX:
        if _uses_sqlite_chat_job_ledger(path):
            _ensure_chat_job_db(path, storage_path)
        signature = _chat_job_state_signature(storage_path)
        cached = _CHAT_JOB_STATE_CACHE.get(key)
        if cached is not None and cached[0] == signature:
            return cached[1]
        state = (
            _load_chat_job_db(storage_path)
            if _uses_sqlite_chat_job_ledger(path)
            else _recover_chat_job_state(path)
        )
        _CHAT_JOB_STATE_CACHE[key] = (
            _chat_job_state_signature(storage_path),
            state,
        )
        return state


def _save_chat_job_state(
    state: dict[str, Any],
    path: Path = DEFAULT_CHAT_JOB_STATE_PATH,
    changed_request_ids: set[str] | None = None,
) -> None:
    if not isinstance(state.get("jobs"), dict):
        state["jobs"] = {}
    _compact_chat_job_state(state)
    storage_path = _chat_job_storage_path(path)
    key = str(storage_path.resolve())
    try:
        if _uses_sqlite_chat_job_ledger(path):
            _ensure_chat_job_db(path, storage_path)
            with _chat_job_db(storage_path) as conn:
                conn.execute("PRAGMA synchronous = FULL")
                _upsert_chat_jobs(conn, state, changed_request_ids)
                conn.commit()
        else:
            _atomic_write_json(path, state)
    except Exception:
        _CHAT_JOB_STATE_CACHE.pop(key, None)
        raise
    _CHAT_JOB_STATE_CACHE[key] = (
        _chat_job_state_signature(storage_path),
        state,
    )


def _job_response_text(response: dict[str, Any] | None) -> str:
    if isinstance(response, dict):
        text = response.get("response_text")
        if isinstance(text, str):
            return text
    return ""


def _normalize_terminal_output(
    response: dict[str, Any],
    *,
    source: str,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Demote contaminated output before it enters normal job projections."""

    original = copy.deepcopy(response)
    text = _job_response_text(original)
    legacy_contamination = detect_visible_output_contamination(text)
    additional_reasons = (
        ("visible_output_contamination",) if legacy_contamination else ()
    )
    receipt = assess_output_integrity(
        text,
        additional_reasons=additional_reasons,
    )
    normalized = dict(response)
    normalized["output_integrity"] = receipt
    normalized["quarantined"] = bool(receipt["quarantine_required"])
    if not receipt["quarantine_required"]:
        return normalized, None

    evidence = {
        "schema_version": 1,
        "source": source,
        "output_integrity": receipt,
        "response": original,
    }
    generated_chars = int(receipt.get("response_chars") or len(text) or 0)
    normalized.update(
        {
            "ok": False,
            "status_code": 422,
            "status": "indeterminate_output",
            "retry_safe": False,
            "poll_hint": (
                "Generated text was quarantined. Poll notion2api_get_chat_job with "
                "include_quarantined=true to inspect generated-but-quarantined evidence; "
                "do not treat it as authoritative or submit a new semantic request blindly."
            ),
            "error": "OUTPUT_CONTAMINATED: assistant output was quarantined.",
            "response_text": "",
            "authoritative": False,
            "quarantined_response_available": generated_chars > 0,
            "raw": {
                "quarantined": True,
                "source": source,
                "output_integrity": receipt,
                "generated_response_available": generated_chars > 0,
                "generated_response_chars": generated_chars,
                "delivery_state": "generated_but_quarantined",
            },
        }
    )
    return normalized, evidence


def _demote_legacy_completed_job(job: dict[str, Any]) -> dict[str, Any]:
    """Fail closed when polling a completed job created before containment."""

    if str(job.get("status") or "") != "completed":
        return job
    response = (
        dict(job["response"])
        if isinstance(job.get("response"), dict)
        else {
            "ok": True,
            "status": "completed",
            "response_text": str(job.get("response_text") or ""),
        }
    )
    normalized, evidence = _normalize_terminal_output(
        response,
        source="legacy_completed_job_poll",
    )
    if evidence is None:
        return job

    updated = dict(job)
    updated["status"] = "indeterminate_output"
    updated["response"] = normalized
    updated["response_text"] = ""
    updated["output_integrity"] = normalized["output_integrity"]
    updated["quarantined"] = True
    updated["error"] = normalized["error"]
    updated["retry_safe"] = False
    updated["_quarantined_response"] = evidence
    updated["updated_at"] = _now_ms()
    _persist_chat_job(updated)
    return updated


_CHECKLIST_RE = re.compile(r"(?m)^\s*(?:[-*]\s*)?\[([ xX])\]\s+(.+?)\s*$")
_PUBLIC_ACTIVITY_RE = re.compile(
    r"^(?:now\s+)?(?:i(?:'m| am|â€™m)\s+)?"
    r"(?:reviewing|reading|searching|checking|mapping|updating|creating|executing|"
    r"applying|merging|writing|verifying|polling|waiting|uploading|downloading|"
    r"extracting|running|preparing|finalizing|completed|finished)\b",
    re.I,
)


def _progress_snapshot(reasoning: str, content: str, event_count: int, complete: bool) -> dict[str, Any]:
    checklist = [
        {"completed": mark.lower() == "x", "text": re.sub(r"\s+", " ", text).strip()[:300]}
        for mark, text in _CHECKLIST_RE.findall(reasoning)
    ][-20:]
    latest = ""
    for candidate in reversed([part.strip() for part in re.split(r"[\r\n]+", reasoning) if part.strip()]):
        normalized = re.sub(r"\s+", " ", candidate)
        if _PUBLIC_ACTIVITY_RE.match(normalized):
            latest = normalized[:500]
            break
    if not latest and checklist:
        last = checklist[-1]
        latest = f"{last['text']} ({'completed' if last['completed'] else 'pending'})"
    return {
        "phase": "completed" if complete else "working",
        "event_count": event_count,
        "latest_update": latest,
        "checklist": checklist,
        "visible_output_chars": len(content),
        "activity_chars": len(reasoning),
        "updated_at": _now_ms(),
    }


def _persist_chat_job(job: dict[str, Any]) -> None:
    with _CHAT_JOB_STATE_MUTEX:
        state = _load_chat_job_state()
        jobs = state.setdefault("jobs", {})
        request_id = str(job["request_id"])
        jobs[request_id] = job
        _save_chat_job_state(state, changed_request_ids={request_id})


def _progress_fingerprint(snapshot: dict[str, Any]) -> str:
    public_state = {
        "phase": snapshot.get("phase"),
        "latest_update": snapshot.get("latest_update"),
        "checklist": snapshot.get("checklist"),
        "visible_output_chars": snapshot.get("visible_output_chars"),
    }
    return json.dumps(public_state, ensure_ascii=False, sort_keys=True)


def _refresh_chat_job_health(job: dict[str, Any], *, increment_poll: bool = False) -> dict[str, Any]:
    updated = dict(job)
    now = _now_ms()
    if increment_poll:
        updated["poll_count"] = int(updated.get("poll_count") or 0) + 1
    active = str(updated.get("status") or "") in {"running", "pending"}
    last_progress_at = int(
        updated.get("last_progress_at")
        or updated.get("created_at")
        or updated.get("updated_at")
        or now
    )
    stalled_for_seconds = max(0.0, (now - last_progress_at) / 1000.0) if active else 0.0
    dead_loop_suspected = active and stalled_for_seconds >= _configured_chat_stall_seconds()
    updated["stalled_for_seconds"] = round(stalled_for_seconds, 3)
    updated["dead_loop_suspected"] = dead_loop_suspected
    updated["cancel_recommended"] = dead_loop_suspected
    progress = updated.get("progress") if isinstance(updated.get("progress"), dict) else None
    if progress is not None:
        progress = dict(progress)
        progress["monitoring"] = {
            "stalled_for_seconds": updated["stalled_for_seconds"],
            "dead_loop_suspected": dead_loop_suspected,
            "cancel_recommended": dead_loop_suspected,
        }
        updated["progress"] = progress
    return updated


def _persist_chat_progress(request_id: str, reasoning: str, content: str, event_count: int, complete: bool) -> None:
    with _CHAT_JOB_STATE_MUTEX:
        state = _load_chat_job_state()
        jobs = state.setdefault("jobs", {})
        current = jobs.get(request_id)
        if not isinstance(current, dict):
            return
        now = _now_ms()
        job = dict(current)
        snapshot = _progress_snapshot(reasoning, content, event_count, complete)
        fingerprint = _progress_fingerprint(snapshot)
        if fingerprint != str(job.get("progress_fingerprint") or "") or complete:
            job["last_progress_at"] = now
        job["progress_fingerprint"] = fingerprint
        job["progress"] = snapshot
        job["updated_at"] = now
        job = _refresh_chat_job_health(job)
        jobs[request_id] = job
        # ponytail: progress stays process-local until terminal persistence;
        # add a small append-only journal if sub-second crash recovery matters.


def _load_chat_job(request_id: str) -> dict[str, Any] | None:
    with _CHAT_JOB_STATE_MUTEX:
        state = _load_chat_job_state()
        job = state.get("jobs", {}).get(request_id)
        return job if isinstance(job, dict) else None


def _mark_chat_job_stale(job: dict[str, Any]) -> dict[str, Any]:
    updated = dict(job)
    updated["status"] = "stale"
    updated["updated_at"] = _now_ms()
    updated["error"] = "The MCP wrapper restarted or lost the in-memory task before this job completed. Check the local conversation by conversation_id before retrying."
    updated = _refresh_chat_job_health(updated)
    _persist_chat_job(updated)
    return updated


def _cancel_chat_job(request_id: str, reason: str = "Cancelled by caller.") -> ChatJobOutput:
    normalized_id = _normalize_request_id(request_id)
    task = _CHAT_JOB_TASKS.get(normalized_id)
    with _CHAT_JOB_STATE_MUTEX:
        state = _load_chat_job_state()
        jobs = state.setdefault("jobs", {})
        current = jobs.get(normalized_id)
        if not isinstance(current, dict) and task is None:
            return ChatJobOutput(
                ok=True,
                found=False,
                request_id=normalized_id,
                job_id=normalized_id,
            )
        job = dict(current) if isinstance(current, dict) else {
            "request_id": normalized_id,
            "job_id": normalized_id,
        }
        job["status"] = "cancelled"
        job["updated_at"] = _now_ms()
        job["error"] = str(reason or "Cancelled by caller.")[:1000]
        job["dead_loop_suspected"] = False
        job["cancel_recommended"] = False
        jobs[normalized_id] = job
        _save_chat_job_state(state, changed_request_ids={normalized_id})
    if task is not None and not task.done():
        task.cancel()
    _CHAT_JOB_TASKS.pop(normalized_id, None)
    return _chat_job_output(normalized_id, increment_poll=False)


def _chat_output_from_backend(
    *,
    data: dict[str, Any],
    client: Notion2APIClient,
    model: str,
    session_key: str,
    conversation_id: str,
    session_created: bool,
    request_id: str,
    wait_seconds: float,
) -> dict[str, Any]:
    ok = bool(data.get("ok", False))
    status = "completed" if ok else "error"
    remote_chat_id = _extract_remote_chat_id(data)
    response = {
        "ok": ok,
        "status_code": data.get("status_code"),
        "model": _extract_actual_model(data) or data.get("model") or model,
        "actual_model": _extract_actual_model(data),
        "model_metadata": (
            data.get("model_metadata")
            if isinstance(data.get("model_metadata"), dict)
            else None
        ),
        **_model_identity_trace(data, model),
        **_caller_trace(
            data.get("request_metadata")
            if isinstance(data.get("request_metadata"), dict)
            else None
        ),
        **_runtime_audit(client, model),
        "session_name": session_key,
        "conversation_id": conversation_id,
        "session_created": session_created,
        "status": status,
        "request_id": request_id,
        "job_id": request_id,
        "retry_safe": status != "completed",
        "wait_seconds": wait_seconds,
        "poll_hint": (
            ""
            if status == "completed"
            else (
                f"Retry with request_id={request_id} or call "
                "notion2api_get_chat_job."
            )
        ),
        "error": _error_summary(data),
        "response_text": _extract_chat_content(data),
        "remote_chat_id": remote_chat_id,
        "notion_thread_id": remote_chat_id,
        "raw": data,
    }
    normalized, evidence = _normalize_terminal_output(
        response,
        source="backend_response",
    )
    if evidence is not None:
        normalized["_quarantined_response"] = evidence
    return normalized

def _chat_pending_output(
    *,
    job: dict[str, Any],
    client: Notion2APIClient,
    model: str,
    session_key: str,
    conversation_id: str,
    session_created: bool,
    request_id: str,
    wait_seconds: float,
) -> dict[str, Any]:
    job = _refresh_chat_job_health(job)
    remote_chat_id = str(job.get("remote_chat_id") or job.get("notion_thread_id") or "")
    return {
        "ok": False,
        "status_code": None,
        "model": model,
        "actual_model": str(job.get("actual_model") or ""),
        "model_metadata": job.get("model_metadata") if isinstance(job.get("model_metadata"), dict) else None,
        **_model_identity_trace(
            {
                "actual_model": job.get("actual_model"),
                "model_metadata": job.get("model_metadata"),
            },
            str(job.get("requested_model") or model),
        ),
        **_caller_trace(
            {"caller": job.get("caller")}
            if isinstance(job.get("caller"), dict)
            else None
        ),
        **_runtime_audit(client, model),
        "session_name": session_key,
        "conversation_id": conversation_id,
        "session_created": session_created,
        "status": str(job.get("status") or "pending"),
        "request_id": request_id,
        "job_id": request_id,
        "retry_safe": str(job.get("status") or "") in {"running", "pending", "stale"},
        "wait_seconds": wait_seconds,
        "poll_hint": (
            f"Call notion2api_get_chat_job(request_id='{request_id}') or retry the same chat tool with the same request_id."
            if str(job.get("status") or "") in {"running", "pending", "stale"}
            else "This request id is terminal; use a new request_id for new work."
        ),
        "error": job.get("error") if isinstance(job.get("error"), str) else None,
        "response_text": _job_response_text(job.get("response") if isinstance(job.get("response"), dict) else None),
        **_attachment_provenance_from_job(job),
        "progress": job.get("progress") if isinstance(job.get("progress"), dict) else None,
        "remote_chat_id": remote_chat_id,
        "notion_thread_id": remote_chat_id,
        "raw": {"job": job, "job_state_path": str(DEFAULT_CHAT_JOB_STATE_PATH)},
    }


def _manifest_int(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and value >= 0:
        return value
    return None


def _attachment_provenance(required: bool, manifest: list[dict[str, Any]] | None) -> dict[str, Any]:
    clean_manifest = list(manifest or [])
    count = len(clean_manifest)
    status = "verified" if count else ("missing" if required else "not_requested")
    return {
        "attachment_required": bool(required),
        "attachment_count": count,
        "attachment_transfer_status": status,
        "attachment_manifest": clean_manifest,
    }


def _attachment_provenance_from_job(job: dict[str, Any]) -> dict[str, Any]:
    manifest = job.get("attachment_manifest") if isinstance(job.get("attachment_manifest"), list) else []
    return _attachment_provenance(bool(job.get("attachment_required")), manifest)


def _required_attachment_error(
    *,
    client: Notion2APIClient,
    model: str,
    session_key: str,
    conversation_id: str,
    session_created: bool,
    request_id: str,
    wait_seconds: float,
) -> dict[str, Any]:
    provenance = _attachment_provenance(True, [])
    return {
        "ok": False,
        "status_code": 422,
        "model": model,
        "actual_model": "",
        "model_metadata": None,
        **_runtime_audit(client, model),
        "session_name": session_key,
        "conversation_id": conversation_id,
        "session_created": session_created,
        "status": "error",
        "request_id": request_id,
        "job_id": request_id,
        "retry_safe": False,
        "wait_seconds": wait_seconds,
        "poll_hint": "Stage or attach at least one file, then submit with a new request_id.",
        "error": "This request required attachments, but no file bytes were verified before submission.",
        "response_text": "",
        **provenance,
        "remote_chat_id": "",
        "notion_thread_id": "",
        "raw": {
            "code": "required_attachments_missing",
            "attachment_provenance": provenance,
        },
    }


def _attachment_manifest_from_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    raw_attachments = payload.get("attachments") if isinstance(payload, dict) else None
    if not isinstance(raw_attachments, list):
        return []
    manifest: list[dict[str, Any]] = []
    for item in raw_attachments:
        if not isinstance(item, dict):
            continue
        data_value = item.get("data") or item.get("file_data") or ""
        source = str(item.get("source") or "").strip()
        if not source and isinstance(data_value, str) and data_value.startswith("data:"):
            source = "inline_data"
        entry: dict[str, Any] = {
            "name": str(item.get("name") or item.get("filename") or item.get("file_name") or ""),
            "content_type": str(item.get("content_type") or item.get("mime_type") or ""),
            "source": source,
        }
        size = _manifest_int(item.get("size_bytes"))
        if size is not None:
            entry["size_bytes"] = size
        manifest.append({key: value for key, value in entry.items() if value != ""})
    return manifest


def _conversation_message_checkpoint(conversation_id: str) -> int:
    """Return the latest persisted message id for a conversation."""

    clean_id = str(conversation_id or "").strip()
    db_path = _local_conversation_db_path()
    if not clean_id or not db_path.exists():
        return 0
    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) FROM messages WHERE conversation_id = ?",
                (clean_id,),
            ).fetchone()
        return int(row[0] or 0) if row else 0
    except Exception:
        return 0


def _completed_turn_after_checkpoint(
    conversation_id: str, baseline_message_id: int
) -> dict[str, Any] | None:
    """Return the first complete user/assistant turn persisted after a job checkpoint."""

    clean_id = str(conversation_id or "").strip()
    db_path = _local_conversation_db_path()
    if not clean_id or not db_path.exists():
        return None
    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                """
                SELECT id, role, content, COALESCE(thinking, '') AS thinking, created_at
                FROM messages
                WHERE conversation_id = ? AND id > ?
                ORDER BY id ASC
                """,
                (clean_id, max(0, int(baseline_message_id or 0))),
            ).fetchall()
            conversation = conn.execute(
                "SELECT COALESCE(thread_id, ''), COALESCE(thread_model, '') "
                "FROM conversations WHERE id = ?",
                (clean_id,),
            ).fetchone()
    except Exception:
        return None

    saw_user = False
    for row in rows:
        role = str(row["role"] or "").strip().lower()
        if role == "user":
            saw_user = True
            continue
        if role != "assistant" or not saw_user:
            continue
        content = str(row["content"] or "")
        thinking = str(row["thinking"] or "")
        if not content.strip() and not thinking.strip():
            continue
        return {
            "assistant_message_id": int(row["id"]),
            "response_text": content,
            "thinking": thinking,
            "created_at": int(row["created_at"] or 0),
            "remote_chat_id": str(conversation[0] or "") if conversation else "",
            "actual_model": str(conversation[1] or "") if conversation else "",
        }
    return None


def _chat_output_from_local_turn(
    job: dict[str, Any], turn: dict[str, Any]
) -> dict[str, Any]:
    """Construct a terminal ChatOutput when local persistence beats SSE closure."""

    persisted_job = _load_chat_job(str(job.get("request_id") or "")) or job
    requested_model = str(job.get("model") or persisted_job.get("model") or "")
    actual_model = str(turn.get("actual_model") or "")
    remote_chat_id = str(turn.get("remote_chat_id") or "")
    model_metadata: dict[str, Any] = {
        "completion_source": "local_conversation_checkpoint",
        "assistant_message_id": int(turn.get("assistant_message_id") or 0),
    }
    if remote_chat_id:
        model_metadata["remote_chat_id"] = remote_chat_id
        model_metadata["notion_thread_id"] = remote_chat_id
    if actual_model:
        model_metadata["actual_model"] = actual_model
    response = {
        "ok": True,
        "status_code": 200,
        "model": actual_model or requested_model,
        "actual_model": actual_model,
        "model_metadata": model_metadata,
        "requested_model": requested_model,
        "backend_base_url": str(job.get("backend_base_url") or ""),
        "timeout_seconds": job.get("timeout_seconds"),
        "session_state_path": str(
            job.get("session_state_path") or DEFAULT_SESSION_STATE_PATH
        ),
        "local_conversations_db": str(
            job.get("local_conversations_db") or _local_conversation_db_path()
        ),
        "imported_history_db": str(job.get("imported_history_db") or ""),
        "session_name": str(job.get("session_name") or DEFAULT_SESSION_NAME),
        "conversation_id": str(job.get("conversation_id") or ""),
        "session_created": bool(job.get("session_created")),
        "status": "completed",
        "request_id": str(job.get("request_id") or ""),
        "job_id": str(job.get("job_id") or job.get("request_id") or ""),
        "retry_safe": False,
        "wait_seconds": job.get("wait_seconds"),
        "poll_hint": "",
        "error": None,
        "response_text": str(turn.get("response_text") or ""),
        **_attachment_provenance_from_job(persisted_job),
        "progress": (
            persisted_job.get("progress")
            if isinstance(persisted_job.get("progress"), dict)
            else None
        ),
        "remote_chat_id": remote_chat_id,
        "notion_thread_id": remote_chat_id,
        "raw": {
            "completion_source": "local_conversation_checkpoint",
            "assistant_message_id": int(turn.get("assistant_message_id") or 0),
        },
    }
    normalized, evidence = _normalize_terminal_output(
        response,
        source="local_conversation_checkpoint",
    )
    if evidence is not None:
        normalized["_quarantined_response"] = evidence
    return normalized

def _complete_chat_job_from_local_turn(
    request_id: str, job: dict[str, Any], turn: dict[str, Any]
) -> dict[str, Any]:
    """Persist a monotonic terminal state and stop any hanging stream task."""

    normalized_id = _normalize_request_id(request_id)
    response = _chat_output_from_local_turn(job, turn)
    completed = dict(job)
    completed["status"] = str(response.get("status") or "error")
    completed["updated_at"] = _now_ms()
    completed["response"] = {
        key: value
        for key, value in response.items()
        if key != "_quarantined_response"
    }
    completed["response_text"] = str(response.get("response_text") or "")
    completed["output_integrity"] = response.get("output_integrity")
    completed["quarantined"] = bool(response.get("quarantined"))
    if isinstance(response.get("_quarantined_response"), dict):
        completed["_quarantined_response"] = response["_quarantined_response"]
    completed["remote_chat_id"] = str(response.get("remote_chat_id") or "")
    completed["notion_thread_id"] = str(response.get("notion_thread_id") or "")
    completed["completion_source"] = "local_conversation_checkpoint"
    completed["assistant_message_id"] = int(turn.get("assistant_message_id") or 0)
    completed["dead_loop_suspected"] = False
    completed["cancel_recommended"] = False
    if response.get("error"):
        completed["error"] = str(response["error"])
    _persist_chat_job(completed)
    _update_session_record(
        str(completed.get("session_name") or DEFAULT_SESSION_NAME),
        conversation_id=str(completed.get("conversation_id") or ""),
        remote_chat_id=str(completed.get("remote_chat_id") or ""),
        model=str(completed.get("model") or ""),
        request_id=normalized_id,
    )
    task = _CHAT_JOB_TASKS.get(normalized_id)
    if task is not None and not task.done():
        task.cancel()
    return completed["response"]

def _active_job_for_conversation(
    conversation_id: str, *, exclude_request_id: str = ""
) -> tuple[str, dict[str, Any]] | None:
    """Read-only inspection helper; never use this as admission authority."""

    state = _load_chat_job_state()
    jobs = state.get("jobs", {}) if isinstance(state, dict) else {}
    if not isinstance(jobs, dict):
        return None
    for request_id, raw_job in jobs.items():
        if not isinstance(raw_job, dict) or str(request_id) == exclude_request_id:
            continue
        if str(raw_job.get("conversation_id") or "") != conversation_id:
            continue
        if str(raw_job.get("status") or "") in {"running", "pending"}:
            return str(request_id), raw_job
    return None


def _claim_chat_job_task(
    job: dict[str, Any],
    task_factory: Any,
    *,
    path: Path = DEFAULT_CHAT_JOB_STATE_PATH,
) -> tuple[str, dict[str, Any], Any, str]:
    """Atomically reserve one active turn for a conversation in this MCP process.

    Returns ``(status, record, task, conflict_request_id)`` where status is one
    of ``claimed``, ``existing``, ``request_id_conflict``, ``conflict``, or
    ``stale_conflict``. A request ID remains bound to its original conversation.
    The durable job claim is persisted before task creation, and admission
    remains inside one process-local critical section.
    """

    request_id = _normalize_request_id(str(job.get("request_id") or ""))
    conversation_id = str(job.get("conversation_id") or "").strip()
    if not conversation_id:
        raise ValueError("conversation_id is required for chat job admission")

    with _CHAT_JOB_STATE_MUTEX:
        state = _load_chat_job_state(path)
        jobs = state.setdefault("jobs", {})
        existing = jobs.get(request_id)
        if isinstance(existing, dict):
            existing_conversation_id = str(
                existing.get("conversation_id") or ""
            ).strip()
            if existing_conversation_id and existing_conversation_id != conversation_id:
                return (
                    "request_id_conflict",
                    dict(existing),
                    _CHAT_JOB_TASKS.get(request_id),
                    request_id,
                )
            return "existing", dict(existing), _CHAT_JOB_TASKS.get(request_id), ""

        for other_request_id, raw_job in list(jobs.items()):
            if not isinstance(raw_job, dict) or str(other_request_id) == request_id:
                continue
            if str(raw_job.get("conversation_id") or "") != conversation_id:
                continue
            if str(raw_job.get("status") or "") not in {"running", "pending"}:
                continue

            other_id = str(other_request_id)
            other_task = _CHAT_JOB_TASKS.get(other_id)
            if other_task is not None and not other_task.done():
                return "conflict", dict(raw_job), other_task, other_id

            stale = dict(raw_job)
            stale["status"] = "stale"
            stale["updated_at"] = _now_ms()
            stale["error"] = (
                "The MCP wrapper restarted or lost the in-memory task before this job "
                "completed. Check the local conversation by conversation_id before retrying."
            )
            jobs[other_id] = _refresh_chat_job_health(stale)
            _save_chat_job_state(state, path, {other_id})
            return "stale_conflict", dict(stale), None, other_id

        durable_job = dict(job)
        durable_job["request_id"] = request_id
        durable_job["job_id"] = request_id
        durable_job["status"] = "pending"
        durable_job["updated_at"] = _now_ms()
        jobs[request_id] = durable_job
        _save_chat_job_state(state, path, {request_id})

        try:
            task = task_factory()
        except Exception as exc:
            failed_job = dict(durable_job)
            failed_job["status"] = "error"
            failed_job["updated_at"] = _now_ms()
            failed_job["error"] = f"Task scheduling failed: {type(exc).__name__}: {exc}"
            jobs[request_id] = _refresh_chat_job_health(failed_job)
            try:
                _save_chat_job_state(state, path, {request_id})
            except OSError:
                logger.exception(
                    "Could not persist chat task scheduling failure",
                    extra={
                        "request_info": {
                            "event": "chat_job_task_scheduling_failure_unpersisted",
                            "request_id": request_id,
                            "conversation_id": conversation_id,
                        }
                    },
                )
            raise

        running_job = dict(durable_job)
        running_job["status"] = "running"
        running_job["updated_at"] = _now_ms()
        jobs[request_id] = running_job
        try:
            _save_chat_job_state(state, path, {request_id})
        except Exception:
            if task is not None and not task.done():
                task.cancel()
            # The prior durable pending claim remains authoritative and blocks
            # replacement until reconciliation.
            raise
        _CHAT_JOB_TASKS[request_id] = task
        return "claimed", running_job, task, ""


async def _run_chat_completion_job(
    *,
    client: Notion2APIClient,
    path: str,
    payload: dict[str, Any],
    model: str,
    session_key: str,
    conversation_id: str,
    session_created: bool,
    request_id: str,
    wait_seconds: float,
    baseline_message_id: int,
) -> dict[str, Any]:
    stream_task = asyncio.create_task(
        client.post_chat_stream(
            path,
            payload,
            lambda reasoning, content, event_count, complete: _persist_chat_progress(
                request_id, reasoning, content, event_count, complete
            ),
        )
    )
    try:
        while True:
            done, _pending = await asyncio.wait({stream_task}, timeout=0.75)
            if done:
                try:
                    data = stream_task.result()
                except Exception:
                    turn = await asyncio.to_thread(
                        _completed_turn_after_checkpoint,
                        conversation_id,
                        baseline_message_id,
                    )
                    if turn is not None:
                        return _chat_output_from_local_turn(
                            {
                                **_runtime_audit(client, model),
                                "model": model,
                                "session_name": session_key,
                                "conversation_id": conversation_id,
                                "session_created": session_created,
                                "request_id": request_id,
                                "job_id": request_id,
                                "wait_seconds": wait_seconds,
                            },
                            turn,
                        )
                    raise
                if not data.get("ok", False):
                    turn = await asyncio.to_thread(
                        _completed_turn_after_checkpoint,
                        conversation_id,
                        baseline_message_id,
                    )
                    if turn is not None:
                        return _chat_output_from_local_turn(
                            {
                                **_runtime_audit(client, model),
                                "model": model,
                                "session_name": session_key,
                                "conversation_id": conversation_id,
                                "session_created": session_created,
                                "request_id": request_id,
                                "job_id": request_id,
                                "wait_seconds": wait_seconds,
                            },
                            turn,
                        )
                data = dict(data)
                data["request_metadata"] = dict(payload.get("metadata") or {})
                return _chat_output_from_backend(
                    data=data,
                    client=client,
                    model=model,
                    session_key=session_key,
                    conversation_id=conversation_id,
                    session_created=session_created,
                    request_id=request_id,
                    wait_seconds=wait_seconds,
                )

            turn = await asyncio.to_thread(
                _completed_turn_after_checkpoint,
                conversation_id,
                baseline_message_id,
            )
            if turn is None:
                continue
            response = _chat_output_from_local_turn(
                {
                    **_runtime_audit(client, model),
                    "model": model,
                    "session_name": session_key,
                    "conversation_id": conversation_id,
                    "session_created": session_created,
                    "request_id": request_id,
                    "job_id": request_id,
                    "wait_seconds": wait_seconds,
                },
                turn,
            )
            stream_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await stream_task
            return response
    finally:
        if not stream_task.done():
            stream_task.cancel()


def _finalize_chat_job(request_id: str, task: asyncio.Task[dict[str, Any]]) -> None:
    quarantine_evidence: dict[str, Any] | None = None
    try:
        task_response = task.result()
        response, generated_evidence = _normalize_terminal_output(
            task_response,
            source="chat_job_finalizer",
        )
        supplied_evidence = task_response.get("_quarantined_response")
        quarantine_evidence = (
            dict(supplied_evidence)
            if isinstance(supplied_evidence, dict)
            else generated_evidence
        )
        response.pop("_quarantined_response", None)
        status = str(
            response.get("status")
            or ("completed" if response.get("ok") else "error")
        )
        error = (
            response.get("error")
            if isinstance(response.get("error"), str)
            else None
        )
    except asyncio.CancelledError:
        response = None
        status = "cancelled"
        error = "Cancelled by caller."
    except Exception as exc:
        response = None
        status = "error"
        error = f"{type(exc).__name__}: {exc}"

    with _CHAT_JOB_STATE_MUTEX:
        state = _load_chat_job_state()
        jobs = state.setdefault("jobs", {})
        existing = jobs.get(request_id)
        job = (
            dict(existing)
            if isinstance(existing, dict)
            else {"request_id": request_id, "job_id": request_id}
        )
        existing_status = str(job.get("status") or "")
        if existing_status in {
            "completed",
            "indeterminate_output",
            "error",
            "cancelled",
        }:
            _CHAT_JOB_TASKS.pop(request_id, None)
            return
        if existing_status == "cancelled":
            status = "cancelled"

        job["status"] = status
        job["updated_at"] = _now_ms()
        if response is not None:
            provenance = _attachment_provenance_from_job(job)
            response = {**response, **provenance}
            if not response.get("caller_id"):
                response["caller_id"] = str((job.get("caller") or {}).get("id") or "")
            if not response.get("caller_type"):
                response["caller_type"] = str((job.get("caller") or {}).get("type") or "")
            if not response.get("caller_metadata"):
                response["caller_metadata"] = dict(job.get("caller") or {}) or None
            if not response.get("requested_model"):
                response["requested_model"] = str(
                    job.get("requested_model") or job.get("model") or ""
                )
            if not response.get("resolved_model"):
                response["resolved_model"] = str(
                    job.get("resolved_model") or job.get("model") or ""
                )
            if isinstance(response.get("model_metadata"), dict):
                job["model_metadata"] = dict(response["model_metadata"])
            if response.get("actual_model"):
                job["actual_model"] = str(response.get("actual_model") or "")

            raw_response = (
                dict(response.get("raw") or {})
                if isinstance(response.get("raw"), dict)
                else {}
            )
            raw_response["attachment_provenance"] = provenance
            response["raw"] = raw_response
            job["response"] = response
            job["response_text"] = _job_response_text(response)
            job["output_integrity"] = response.get("output_integrity")
            job["quarantined"] = bool(response.get("quarantined"))
            if quarantine_evidence is not None:
                job["_quarantined_response"] = quarantine_evidence
            remote_chat_id = str(
                response.get("remote_chat_id")
                or response.get("notion_thread_id")
                or ""
            ).strip()
            if not remote_chat_id and isinstance(response.get("model_metadata"), dict):
                mm = response["model_metadata"]
                remote_chat_id = str(mm.get("notion_thread_id") or mm.get("remote_chat_id") or "").strip()
            if not remote_chat_id:
                conv_id = str(job.get("conversation_id") or "").strip()
                if conv_id:
                    db_path = _local_conversation_db_path()
                    if db_path.exists():
                        try:
                            with sqlite3.connect(str(db_path), timeout=5) as conn:
                                row = conn.execute("SELECT thread_id FROM conversations WHERE id = ?", (conv_id,)).fetchone()
                                if row and row[0]:
                                    remote_chat_id = str(row[0]).strip()
                        except Exception:
                            pass
            if remote_chat_id:
                job["remote_chat_id"] = remote_chat_id
                job["notion_thread_id"] = remote_chat_id
                response["remote_chat_id"] = remote_chat_id
                response["notion_thread_id"] = remote_chat_id
                if isinstance(response.get("model_metadata"), dict):
                    response["model_metadata"]["remote_chat_id"] = remote_chat_id
                    response["model_metadata"]["notion_thread_id"] = remote_chat_id
        if error:
            job["error"] = error
        job = _refresh_chat_job_health(job)
        jobs[request_id] = job
        _save_chat_job_state(state, changed_request_ids={request_id})

    _update_session_record(
        str(job.get("session_name") or DEFAULT_SESSION_NAME),
        conversation_id=str(job.get("conversation_id") or ""),
        remote_chat_id=str(job.get("remote_chat_id") or ""),
        model=str(job.get("model") or ""),
        request_id=request_id,
    )
    _CHAT_JOB_TASKS.pop(request_id, None)

def _request_id_conversation_conflict_output(
    *,
    client: Notion2APIClient,
    model: str,
    session_key: str,
    conversation_id: str,
    session_created: bool,
    request_id: str,
    wait_seconds: float,
) -> dict[str, Any]:
    """Reject cross-conversation reuse without exposing the prior job."""

    return {
        "ok": False,
        "status_code": 409,
        "model": model,
        "actual_model": "",
        "model_metadata": None,
        **_runtime_audit(client, model),
        "session_name": session_key,
        "conversation_id": conversation_id,
        "session_created": session_created,
        "status": "error",
        "request_id": request_id,
        "job_id": request_id,
        "retry_safe": False,
        "wait_seconds": wait_seconds,
        "poll_hint": "Use a new request_id for a different conversation.",
        "error": "This request_id is already bound to another conversation.",
        "response_text": "",
        "remote_chat_id": "",
        "notion_thread_id": "",
        "raw": {"code": "request_id_conversation_mismatch"},
    }


async def _submit_or_resume_chat_job(
    *,
    client: Notion2APIClient,
    path: str,
    payload: dict[str, Any],
    model: str,
    session_key: str,
    conversation_id: str,
    session_created: bool,
    request_id: str | None,
    wait_seconds: float | None,
) -> dict[str, Any]:
    normalized_id = _normalize_request_id(request_id)
    bounded_wait = _bounded_chat_wait_seconds(wait_seconds)
    metadata = payload.setdefault("metadata", {})
    if not isinstance(metadata, dict):
        metadata = {}
        payload["metadata"] = metadata
    metadata.setdefault("mcp_request_id", normalized_id)
    caller = metadata.get("caller") if isinstance(metadata.get("caller"), dict) else {}
    caller = {
        "id": str(caller.get("id") or caller.get("caller_id") or "notion2api-mcp").strip(),
        "type": str(caller.get("type") or caller.get("caller_type") or "mcp").strip(),
        **{
            str(key): value
            for key, value in caller.items()
            if str(key) not in {"id", "caller_id", "type", "caller_type"}
            and value not in (None, "", [], {})
        },
    }
    caller.setdefault("request_origin", "notion2api_mcp")
    metadata["caller"] = caller
    attachment_manifest = _attachment_manifest_from_payload(payload)
    attachment_required = bool(metadata.get("require_attachments"))

    existing = _load_chat_job(normalized_id)
    task = _CHAT_JOB_TASKS.get(normalized_id)
    existing_conversation_id = (
        str(existing.get("conversation_id") or "").strip()
        if isinstance(existing, dict)
        else ""
    )
    if existing_conversation_id and existing_conversation_id != conversation_id:
        return _request_id_conversation_conflict_output(
            client=client,
            model=model,
            session_key=session_key,
            conversation_id=conversation_id,
            session_created=session_created,
            request_id=normalized_id,
            wait_seconds=bounded_wait,
        )
    if existing:
        baseline_message_id = int(existing.get("baseline_message_id") or 0)
        status = str(existing.get("status") or "")
        response = existing.get("response") if isinstance(existing.get("response"), dict) else None
        if status in {"completed", "indeterminate_output", "error", "cancelled"}:
            if response:
                return response
            if status == "completed" and "baseline_message_id" in existing:
                turn = await asyncio.to_thread(
                    _completed_turn_after_checkpoint,
                    str(existing.get("conversation_id") or conversation_id),
                    baseline_message_id,
                )
                if turn is not None:
                    return _complete_chat_job_from_local_turn(normalized_id, existing, turn)
            return _chat_pending_output(
                job=existing,
                client=client,
                model=model,
                session_key=str(existing.get("session_name") or session_key),
                conversation_id=str(existing.get("conversation_id") or conversation_id),
                session_created=False,
                request_id=normalized_id,
                wait_seconds=bounded_wait,
            )
        if status == "running" and (task is None or task.done()):
            if task and task.done():
                _finalize_chat_job(normalized_id, task)
                refreshed = _load_chat_job(normalized_id)
                response = refreshed.get("response") if isinstance(refreshed, dict) and isinstance(refreshed.get("response"), dict) else None
                if response:
                    return response
                if refreshed:
                    existing = refreshed
            else:
                existing = _mark_chat_job_stale(existing)
                return _chat_pending_output(
                    job=existing,
                    client=client,
                    model=model,
                    session_key=session_key,
                    conversation_id=str(existing.get("conversation_id") or conversation_id),
                    session_created=False,
                    request_id=normalized_id,
                    wait_seconds=bounded_wait,
                )
        elif task is None and status in {"pending", "stale"}:
            return _chat_pending_output(
                job=existing,
                client=client,
                model=model,
                session_key=session_key,
                conversation_id=str(existing.get("conversation_id") or conversation_id),
                session_created=False,
                request_id=normalized_id,
                wait_seconds=bounded_wait,
            )

    if task is None and attachment_required and not attachment_manifest:
        return _required_attachment_error(
            client=client,
            model=model,
            session_key=session_key,
            conversation_id=conversation_id,
            session_created=session_created,
            request_id=normalized_id,
            wait_seconds=bounded_wait,
        )

    if task is None:
        baseline_message_id = _conversation_message_checkpoint(conversation_id)
        now = _now_ms()
        job = {
            "request_id": normalized_id,
            "job_id": normalized_id,
            "status": "running",
            "endpoint": path,
            "model": model,
            "requested_model": model,
            "resolved_model": model,
            "caller": caller,
            "session_name": session_key,
            "conversation_id": conversation_id,
            "session_created": session_created,
            "created_at": now,
            "updated_at": now,
            "last_progress_at": now,
            "poll_count": 0,
            "wait_seconds": bounded_wait,
            "baseline_message_id": baseline_message_id,
            **_runtime_audit(client, model),
            **_attachment_provenance(attachment_required, attachment_manifest),
        }

        def create_task() -> asyncio.Task[dict[str, Any]]:
            return asyncio.create_task(
                _run_chat_completion_job(
                    client=client,
                    path=path,
                    payload=payload,
                    model=model,
                    session_key=session_key,
                    conversation_id=conversation_id,
                    session_created=session_created,
                    request_id=normalized_id,
                    wait_seconds=bounded_wait,
                    baseline_message_id=baseline_message_id,
                )
            )

        claim_status, claimed_job, task, conflict_id = _claim_chat_job_task(
            job,
            create_task,
        )
        if claim_status == "request_id_conflict":
            return _request_id_conversation_conflict_output(
                client=client,
                model=model,
                session_key=session_key,
                conversation_id=conversation_id,
                session_created=session_created,
                request_id=normalized_id,
                wait_seconds=bounded_wait,
            )
        if claim_status in {"conflict", "stale_conflict"}:
            stale_conflict = claim_status == "stale_conflict"
            return {
                "ok": False,
                "status_code": 409,
                "model": model,
                "actual_model": "",
                "model_metadata": None,
                **_runtime_audit(client, model),
                "session_name": session_key,
                "conversation_id": conversation_id,
                "session_created": session_created,
                "status": "error",
                "request_id": normalized_id,
                "job_id": normalized_id,
                "retry_safe": False,
                "wait_seconds": bounded_wait,
                "poll_hint": (
                    f"Inspect request_id={conflict_id} and reconcile its outcome before retrying."
                    if stale_conflict
                    else f"Poll active request_id={conflict_id} before sending another turn."
                ),
                "error": (
                    "A prior request lost its local task; reconcile its durable outcome before "
                    "starting another turn for this conversation."
                    if stale_conflict
                    else "Another request is already active for this conversation."
                ),
                "response_text": "",
                "remote_chat_id": "",
                "notion_thread_id": "",
                "raw": {
                    "code": (
                        "conversation_reconciliation_required"
                        if stale_conflict
                        else "conversation_busy"
                    ),
                    "active_request_id": conflict_id,
                },
            }
        if claim_status == "existing":
            existing_response = (
                claimed_job.get("response")
                if isinstance(claimed_job.get("response"), dict)
                else None
            )
            if existing_response:
                return existing_response
            return _chat_pending_output(
                job=claimed_job,
                client=client,
                model=model,
                session_key=str(claimed_job.get("session_name") or session_key),
                conversation_id=str(claimed_job.get("conversation_id") or conversation_id),
                session_created=False,
                request_id=normalized_id,
                wait_seconds=bounded_wait,
            )

        _update_session_record(
            session_key,
            conversation_id=conversation_id,
            model=model,
            request_id=normalized_id,
        )
        task.add_done_callback(
            lambda done_task, rid=normalized_id: _finalize_chat_job(rid, done_task)
        )

    if bounded_wait > 0:
        done, _pending = await asyncio.wait({task}, timeout=bounded_wait)
        if done:
            result = task.result()
            _finalize_chat_job(normalized_id, task)
            finalized = _load_chat_job(normalized_id)
            if isinstance(finalized, dict) and isinstance(finalized.get("response"), dict):
                return finalized["response"]
            return {**result, **_attachment_provenance(attachment_required, attachment_manifest)}

    current = _load_chat_job(normalized_id) or {
        "request_id": normalized_id,
        "job_id": normalized_id,
        "status": "running",
        "model": model,
        "session_name": session_key,
        "conversation_id": conversation_id,
    }
    if str(current.get("status") or "") == "running":
        current["status"] = "pending"
        current["updated_at"] = _now_ms()
        _persist_chat_job(current)
    return _chat_pending_output(
        job=current,
        client=client,
        model=model,
        session_key=session_key,
        conversation_id=conversation_id,
        session_created=session_created,
        request_id=normalized_id,
        wait_seconds=bounded_wait,
    )


def _extract_quarantined_response_text(job: dict[str, Any]) -> str:
    """Return generated text retained under quarantine evidence, if any."""

    evidence = job.get("_quarantined_response")
    if not isinstance(evidence, dict):
        return ""
    nested = evidence.get("response")
    if isinstance(nested, dict):
        text = _job_response_text(nested)
        if text:
            return text
        nested_text = nested.get("response_text")
        if isinstance(nested_text, str) and nested_text.strip():
            return nested_text
    integrity = evidence.get("output_integrity")
    if isinstance(integrity, dict):
        # Integrity receipts do not store text; fall through.
        pass
    return ""


def _chat_job_output(
    request_id: str,
    include_last_response: bool = False,
    include_response: bool = False,
    include_quarantined: bool = False,
    *,
    increment_poll: bool = True,
) -> ChatJobOutput:
    normalized_id = _normalize_request_id(request_id)
    task = _CHAT_JOB_TASKS.get(normalized_id)
    if task and task.done():
        _finalize_chat_job(normalized_id, task)
    job = _load_chat_job(normalized_id)
    if not job:
        return ChatJobOutput(
            ok=True,
            found=False,
            request_id=normalized_id,
            job_id=normalized_id,
        )
    job = _demote_legacy_completed_job(job)

    if str(job.get("status") or "") in {"running", "pending"}:
        baseline_message_id = int(job.get("baseline_message_id") or 0)
        if "baseline_message_id" in job:
            turn = _completed_turn_after_checkpoint(
                str(job.get("conversation_id") or ""), baseline_message_id
            )
            if turn is not None:
                _complete_chat_job_from_local_turn(normalized_id, job, turn)
                job = _load_chat_job(normalized_id) or job

    if (
        str(job.get("status") or "") in {"running", "pending"}
        and normalized_id not in _CHAT_JOB_TASKS
    ):
        job = _mark_chat_job_stale(job)

    job = _refresh_chat_job_health(job, increment_poll=increment_poll)
    response = job.get("response") if isinstance(job.get("response"), dict) else None
    integrity = (
        dict(job["output_integrity"])
        if isinstance(job.get("output_integrity"), dict)
        else (
            dict(response["output_integrity"])
            if isinstance(response, dict)
            and isinstance(response.get("output_integrity"), dict)
            else None
        )
    )
    quarantined = bool(
        job.get("quarantined")
        or (isinstance(response, dict) and response.get("quarantined"))
        or (isinstance(integrity, dict) and integrity.get("quarantine_required"))
    )
    quarantined_full_text = _extract_quarantined_response_text(job) if quarantined else ""
    quarantined_available = bool(quarantined and quarantined_full_text)
    expose_quarantined = bool(include_quarantined or include_response) and quarantined_available
    full_response_text = "" if quarantined else str(
        job.get("response_text") or _job_response_text(response)
    )
    response_text = (
        full_response_text
        if include_response
        else full_response_text[:MAX_CHAT_JOB_RESPONSE_PREVIEW_CHARS]
    )
    quarantined_response_text = (
        quarantined_full_text
        if expose_quarantined and include_response
        else (
            quarantined_full_text[:MAX_CHAT_JOB_RESPONSE_PREVIEW_CHARS]
            if expose_quarantined
            else ""
        )
    )
    response_chars = (
        int(integrity.get("response_chars") or len(quarantined_full_text) or 0)
        if quarantined and isinstance(integrity, dict)
        else (
            len(quarantined_full_text)
            if quarantined
            else len(full_response_text)
        )
    )
    raw_job = {
        key: value
        for key, value in job.items()
        if key not in {"response", "response_text", "_quarantined_response"}
    }
    if quarantined:
        raw_job["delivery_state"] = "generated_but_quarantined"
        raw_job["quarantined_response_available"] = quarantined_available
        raw_job["generated_response_chars"] = response_chars
    last_response = None
    if include_last_response and not quarantined:
        last = _read_last_local_response(
            session_name=str(job.get("session_name") or DEFAULT_SESSION_NAME),
            conversation_id=str(job.get("conversation_id") or ""),
        )
        last_response = last.model_dump() if hasattr(last, "model_dump") else dict(last)

    projected_response = None
    if include_response and not quarantined:
        projected_response = response
    elif include_response and quarantined and isinstance(response, dict):
        projected_response = {
            **{
                key: value
                for key, value in response.items()
                if key not in {"response_text", "raw"}
            },
            "response_text": "",
            "authoritative": False,
            "quarantined_response_available": quarantined_available,
            "quarantined_response_text": quarantined_full_text if expose_quarantined else "",
            "raw": {
                "quarantined": True,
                "delivery_state": "generated_but_quarantined",
                "generated_response_chars": response_chars,
                "include_quarantined": bool(include_quarantined or include_response),
            },
        }

    return ChatJobOutput(
        ok=True,
        found=True,
        status=str(job.get("status") or ""),
        request_id=normalized_id,
        job_id=str(job.get("job_id") or normalized_id),
        session_name=str(job.get("session_name") or ""),
        conversation_id=str(job.get("conversation_id") or ""),
        model=str(job.get("model") or ""),
        endpoint=str(job.get("endpoint") or ""),
        created_at=int(job.get("created_at") or 0),
        updated_at=int(job.get("updated_at") or 0),
        response_text=response_text,
        response_chars=response_chars,
        response_truncated=(
            (quarantined and expose_quarantined and len(quarantined_response_text) < len(quarantined_full_text))
            or (not quarantined and len(response_text) < len(full_response_text))
            or (quarantined and not expose_quarantined)
        ),
        output_integrity=integrity,
        quarantined=quarantined,
        authoritative=not quarantined,
        quarantined_response_available=quarantined_available,
        quarantined_response_text=quarantined_response_text,
        **_attachment_provenance_from_job(job),
        progress=job.get("progress") if isinstance(job.get("progress"), dict) else None,
        remote_chat_id=str(job.get("remote_chat_id") or job.get("notion_thread_id") or ""),
        notion_thread_id=str(job.get("notion_thread_id") or job.get("remote_chat_id") or ""),
        poll_count=int(job.get("poll_count") or 0),
        stalled_for_seconds=float(job.get("stalled_for_seconds") or 0.0),
        dead_loop_suspected=bool(job.get("dead_loop_suspected")),
        cancel_recommended=bool(job.get("cancel_recommended")),
        response=projected_response,
        error=job.get("error") if isinstance(job.get("error"), str) else None,
        raw_job=raw_job,
        last_response=last_response,
    )


def _reconcile_orphaned_chat_jobs_on_startup() -> dict[str, int]:
    """Close persisted active jobs that cannot have a live task after restart."""

    state = _load_chat_job_state()
    jobs = state.get("jobs", {}) if isinstance(state, dict) else {}
    summary = {"completed": 0, "stale": 0}
    if not isinstance(jobs, dict):
        return summary

    for request_id, raw_job in list(jobs.items()):
        if not isinstance(raw_job, dict):
            continue
        job = dict(raw_job)
        if str(job.get("status") or "") not in {"running", "pending"}:
            continue

        if "baseline_message_id" in job:
            turn = _completed_turn_after_checkpoint(
                str(job.get("conversation_id") or ""),
                int(job.get("baseline_message_id") or 0),
            )
            if turn is not None:
                _complete_chat_job_from_local_turn(str(request_id), job, turn)
                summary["completed"] += 1
                continue

        _mark_chat_job_stale(job)
        summary["stale"] += 1

    return summary


def _load_session_records(path: Path = DEFAULT_SESSION_STATE_PATH) -> dict[str, dict[str, Any]]:
    try:
        if not path.exists():
            return {}
        data = json.loads(path.read_text(encoding="utf-8"))
        sessions = data.get("sessions") if isinstance(data, dict) else None
        if not isinstance(sessions, dict):
            return {}
        records: dict[str, dict[str, Any]] = {}
        for raw_name, raw_value in sessions.items():
            name = _session_key(str(raw_name))
            if isinstance(raw_value, str):
                conversation_id = raw_value.strip()
                if conversation_id:
                    records[name] = {"conversation_id": conversation_id}
                continue
            if not isinstance(raw_value, dict):
                continue
            conversation_id = str(raw_value.get("conversation_id") or "").strip()
            if not conversation_id:
                continue
            record = {str(key): value for key, value in raw_value.items()}
            record["conversation_id"] = conversation_id
            records[name] = record
        return records
    except Exception:
        return {}


def _save_session_records(
    records: dict[str, dict[str, Any]],
    path: Path = DEFAULT_SESSION_STATE_PATH,
) -> None:
    with _SESSION_STATE_MUTEX:
        clean: dict[str, dict[str, Any]] = {}
        for raw_name, raw_record in records.items():
            if not isinstance(raw_record, dict):
                continue
            conversation_id = str(raw_record.get("conversation_id") or "").strip()
            if not conversation_id:
                continue
            record = {
                str(key): value
                for key, value in raw_record.items()
                if value not in (None, "")
            }
            record["conversation_id"] = conversation_id
            clean[_session_key(raw_name)] = record
        try:
            _atomic_write_json(
                path,
                {"version": SESSION_STATE_VERSION, "sessions": clean},
            )
        except Exception:
            # Session continuity is helpful but should not break model calls.
            return


def _load_session_state(path: Path = DEFAULT_SESSION_STATE_PATH) -> dict[str, str]:
    return {
        name: str(record.get("conversation_id") or "")
        for name, record in _load_session_records(path).items()
        if str(record.get("conversation_id") or "").strip()
    }


def _save_session_state(sessions: dict[str, str], path: Path = DEFAULT_SESSION_STATE_PATH) -> None:
    with _SESSION_STATE_MUTEX:
        existing = _load_session_records(path)
        updated: dict[str, dict[str, Any]] = {}
        for raw_name, raw_conversation_id in sessions.items():
            name = _session_key(raw_name)
            conversation_id = str(raw_conversation_id or "").strip()
            if not conversation_id:
                continue
            record = dict(existing.get(name) or {})
            record["conversation_id"] = conversation_id
            record["updated_at"] = _now_ms()
            updated[name] = record
        _save_session_records(updated, path)


def _update_session_record(
    session_name: str,
    *,
    conversation_id: str | None = None,
    remote_chat_id: str | None = None,
    model: str | None = None,
    request_id: str | None = None,
    path: Path = DEFAULT_SESSION_STATE_PATH,
) -> None:
    with _SESSION_STATE_MUTEX:
        key = _session_key(session_name)
        records = _load_session_records(path)
        record = dict(records.get(key) or {})
        clean_conversation_id = str(
            conversation_id or record.get("conversation_id") or ""
        ).strip()
        if not clean_conversation_id:
            return
        record["conversation_id"] = clean_conversation_id
        clean_remote_id = str(remote_chat_id or "").strip()
        if clean_remote_id:
            record["remote_chat_id"] = clean_remote_id
            record["notion_thread_id"] = clean_remote_id
        clean_model = str(model or "").strip()
        if clean_model:
            record["last_model"] = clean_model
        clean_request_id = str(request_id or "").strip()
        if clean_request_id:
            record["last_request_id"] = clean_request_id
        record["updated_at"] = _now_ms()
        records[key] = record
        _save_session_records(records, path)


def _conversation_id_for_session(
    session_name: str | None = None,
    *,
    start_new_chat: bool = False,
    conversation_id: str | None = None,
    continue_from_request_id: str | None = None,
    path: Path = DEFAULT_SESSION_STATE_PATH,
) -> tuple[str, str, bool]:
    requested_key = _session_key(session_name)
    continuation_id = str(continue_from_request_id or "").strip()
    prior_job = (
        _load_chat_job(_normalize_request_id(continuation_id))
        if continuation_id and not start_new_chat
        else None
    )

    with _SESSION_STATE_MUTEX:
        records = _load_session_records(path)
        if isinstance(prior_job, dict):
            prior_conversation_id = str(prior_job.get("conversation_id") or "").strip()
            prior_session_name = str(prior_job.get("session_name") or requested_key).strip()
            if prior_conversation_id:
                key = _session_key(prior_session_name)
                record = dict(records.get(key) or {})
                record["conversation_id"] = prior_conversation_id
                record["continued_from_request_id"] = continuation_id
                record["updated_at"] = _now_ms()
                records[key] = record
                _save_session_records(records, path)
                return prior_conversation_id, key, False

        explicit_conversation_id = str(conversation_id or "").strip()
        if explicit_conversation_id and not start_new_chat:
            matching_key = next(
                (
                    name
                    for name, record in records.items()
                    if str(record.get("conversation_id") or "").strip()
                    == explicit_conversation_id
                ),
                requested_key,
            )
            record = dict(records.get(matching_key) or {})
            record["conversation_id"] = explicit_conversation_id
            record["updated_at"] = _now_ms()
            records[matching_key] = record
            _save_session_records(records, path)
            return explicit_conversation_id, matching_key, False

        current = records.get(requested_key) or {}
        current_conversation_id = str(current.get("conversation_id") or "").strip()
        created = False
        if start_new_chat or not current_conversation_id:
            current_conversation_id = f"mcp-{requested_key}-{uuid.uuid4().hex}"
            current = {
                "conversation_id": current_conversation_id,
                "created_at": _now_ms(),
            }
            records[requested_key] = current
            _save_session_records(records, path)
            created = True
        return current_conversation_id, requested_key, created


def _extract_conversation_id(data: dict[str, Any]) -> str:
    for key in ("conversation_id", "conversationId"):
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    headers = data.get("headers")
    if isinstance(headers, dict):
        for key in ("x-conversation-id", "X-Conversation-Id"):
            value = headers.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _resolve_session_conversation_id(session_name: str | None = None, conversation_id: str | None = None) -> tuple[str, str, str | None]:
    key = _session_key(session_name)
    explicit = (conversation_id or "").strip()
    if explicit:
        return key, explicit, None
    sessions = _load_session_state()
    resolved = sessions.get(key, "").strip()
    if not resolved:
        return key, "", f"No conversation id is bound to MCP session '{key}'."
    return key, resolved, None


def _read_local_messages(session_name: str | None = None, conversation_id: str | None = None, limit: int = 10) -> MessagesOutput:
    key, resolved_id, error = _resolve_session_conversation_id(session_name, conversation_id)
    db_path = _local_conversation_db_path()
    if error:
        return MessagesOutput(ok=False, session_name=key, conversation_id=resolved_id, db_path=str(db_path), error=error)
    if not db_path.exists():
        return MessagesOutput(ok=False, session_name=key, conversation_id=resolved_id, db_path=str(db_path), error="Local conversations database does not exist.")
    safe_limit = max(1, min(int(limit or 10), 100))
    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            total_row = conn.execute("SELECT COUNT(1) AS cnt FROM messages WHERE conversation_id = ?", (resolved_id,)).fetchone()
            total = int(total_row["cnt"] or 0) if total_row else 0
            rows = conn.execute(
                """
                SELECT id, role, content, COALESCE(thinking, '') AS thinking, created_at
                FROM messages
                WHERE conversation_id = ?
                ORDER BY created_at DESC, id DESC
                LIMIT ?
                """,
                (resolved_id, safe_limit),
            ).fetchall()
        messages = [
            {
                "id": int(row["id"]),
                "role": str(row["role"] or ""),
                "content": str(row["content"] or ""),
                "thinking": str(row["thinking"] or ""),
                "created_at": int(row["created_at"] or 0),
            }
            for row in rows
        ]
        messages.reverse()
        fallback_used = False
        if not messages:
            jobs_state = _load_chat_job_state()
            all_jobs = jobs_state.get("jobs", {})
            for job in sorted(all_jobs.values(), key=lambda j: j.get("updated_at") or j.get("created_at") or 0, reverse=True):
                if str(job.get("status")) == "completed" and not job.get("quarantined"):
                    if str(job.get("session_name")) == key or str(job.get("conversation_id")) == resolved_id:
                        text = _job_response_text(job)
                        if text:
                            created_ts = int(job.get("updated_at") or job.get("created_at") or 0)
                            prompt_text = str(job.get("prompt") or "")
                            if prompt_text:
                                messages.append({"id": 1, "role": "user", "content": prompt_text, "thinking": "", "created_at": created_ts - 1})
                            messages.append({"id": 2, "role": "assistant", "content": text, "thinking": "", "created_at": created_ts})
                            total = len(messages)
                            fallback_used = True
                            break
        return MessagesOutput(
            ok=True,
            session_name=key,
            conversation_id=resolved_id,
            count=len(messages),
            total_count=total,
            db_path=str(db_path),
            persistence_source="mcp_job_store" if fallback_used else "conversation_db",
            durable_persisted=not fallback_used,
            reconciliation_required=fallback_used,
            messages=messages,
        )
    except Exception as exc:
        return MessagesOutput(ok=False, session_name=key, conversation_id=resolved_id, db_path=str(db_path), error=f"{type(exc).__name__}: {exc}")


def _read_last_local_response(session_name: str | None = None, conversation_id: str | None = None) -> LastResponseOutput:
    key, resolved_id, error = _resolve_session_conversation_id(session_name, conversation_id)
    db_path = _local_conversation_db_path()
    if error:
        return LastResponseOutput(ok=False, session_name=key, conversation_id=resolved_id, db_path=str(db_path), error=error)
    if not db_path.exists():
        return LastResponseOutput(ok=False, session_name=key, conversation_id=resolved_id, db_path=str(db_path), error="Local conversations database does not exist.")
    try:
        with sqlite3.connect(str(db_path), timeout=10) as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                """
                SELECT id, role, content, COALESCE(thinking, '') AS thinking, created_at
                FROM messages
                WHERE conversation_id = ? AND role = 'assistant'
                ORDER BY created_at DESC, id DESC
                LIMIT 1
                """,
                (resolved_id,),
            ).fetchone()
        if not row:
            # Fallback to completed MCP job records for this session/conversation
            completed_job = None
            jobs_state = _load_chat_job_state()
            all_jobs = jobs_state.get("jobs", {})
            for job in sorted(all_jobs.values(), key=lambda j: j.get("updated_at") or j.get("created_at") or 0, reverse=True):
                if str(job.get("status")) == "completed" and not job.get("quarantined"):
                    if str(job.get("session_name")) == key or str(job.get("conversation_id")) == resolved_id:
                        text = _job_response_text(job)
                        if text:
                            completed_job = (job, text)
                            break
            if completed_job:
                c_job, c_text = completed_job
                message = {
                    "id": 0,
                    "role": "assistant",
                    "content": c_text,
                    "thinking": "",
                    "created_at": int(c_job.get("updated_at") or c_job.get("created_at") or 0),
                }
                return LastResponseOutput(
                    ok=True,
                    found=True,
                    session_name=key,
                    conversation_id=resolved_id,
                    response_text=c_text,
                    message=message,
                    db_path=str(db_path),
                    persistence_source="mcp_job_store",
                    durable_persisted=False,
                    reconciliation_required=True,
                )
            return LastResponseOutput(ok=True, found=False, session_name=key, conversation_id=resolved_id, db_path=str(db_path))
        message = {
            "id": int(row["id"]),
            "role": str(row["role"] or ""),
            "content": str(row["content"] or ""),
            "thinking": str(row["thinking"] or ""),
            "created_at": int(row["created_at"] or 0),
        }
        return LastResponseOutput(
            ok=True,
            found=True,
            session_name=key,
            conversation_id=resolved_id,
            response_text=message["content"],
            message=message,
            db_path=str(db_path),
            persistence_source="conversation_db",
            durable_persisted=True,
            reconciliation_required=False,
        )
    except Exception as exc:
        return LastResponseOutput(ok=False, found=False, session_name=key, conversation_id=resolved_id, db_path=str(db_path), error=f"{type(exc).__name__}: {exc}")


PROMPT_PACK_DIR = Path(
    os.getenv(
        "MCP_NOTION2API_PROMPT_DIR",
        str(Path(__file__).resolve().parents[1] / "prompts" / "notion2api-mcp"),
    )
)
PROMPT_INDEX_PATH = Path(os.getenv("MCP_NOTION2API_PROMPT_INDEX", str(PROMPT_PACK_DIR / "index.json")))


def _load_prompt_index() -> dict[str, Any]:
    try:
        data = json.loads(PROMPT_INDEX_PATH.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {"prompts": []}
    except Exception:
        return {"prompts": []}


def _prompt_metadata(name: str) -> dict[str, Any]:
    prompts = _load_prompt_index().get("prompts", [])
    if isinstance(prompts, list):
        for item in prompts:
            if isinstance(item, dict) and item.get("name") == name:
                return item
    return {"name": name, "title": name, "description": "Notion2API MCP prompt.", "file": ""}


def _load_prompt_body(file_name: str) -> str:
    safe_name = Path(str(file_name or "")).name
    if not safe_name:
        return "Prompt body is unavailable: no file was configured for this prompt."
    path = PROMPT_PACK_DIR / safe_name
    try:
        return path.read_text(encoding="utf-8")
    except Exception as exc:
        return f"Prompt body is unavailable: {type(exc).__name__}: {exc}"


def _format_prompt_arguments(arguments: dict[str, Any]) -> str:
    clean = {key: value for key, value in arguments.items() if value not in (None, "")}
    if not clean:
        return ""
    lines = ["", "## Invocation arguments"]
    for key, value in clean.items():
        lines.append(f"- {key}: {value}")
    return "\n".join(lines)


def _prompt_messages(name: str, arguments: dict[str, Any] | None = None) -> list[dict[str, str]]:
    meta = _prompt_metadata(name)
    body = _load_prompt_body(str(meta.get("file") or ""))
    rendered = body + _format_prompt_arguments(arguments or {})
    return [{"role": "user", "content": rendered}]


def register_notion2api_prompts(server: FastMCP) -> None:
    """Register prompt-pack entries with FastMCP so clients expose prompts/list and prompts/get."""

    meta = _prompt_metadata("notion2api_operator")

    @server.prompt(name="notion2api_operator", title=meta.get("title"), description=meta.get("description"))
    def notion2api_operator() -> list[dict[str, str]]:
        return _prompt_messages("notion2api_operator")

    meta = _prompt_metadata("notion2api_tool_router")

    @server.prompt(name="notion2api_tool_router", title=meta.get("title"), description=meta.get("description"))
    def notion2api_tool_router(user_request: str) -> list[dict[str, str]]:
        return _prompt_messages("notion2api_tool_router", {"user_request": user_request})

    meta = _prompt_metadata("notion2api_output_schema_writer")

    @server.prompt(name="notion2api_output_schema_writer", title=meta.get("title"), description=meta.get("description"))
    def notion2api_output_schema_writer(operation_name: str, current_schema: str = "") -> list[dict[str, str]]:
        return _prompt_messages(
            "notion2api_output_schema_writer",
            {"operation_name": operation_name, "current_schema": current_schema},
        )

    meta = _prompt_metadata("notion2api_provider_debugger")

    @server.prompt(name="notion2api_provider_debugger", title=meta.get("title"), description=meta.get("description"))
    def notion2api_provider_debugger(error_log: str = "", operation: str = "") -> list[dict[str, str]]:
        return _prompt_messages(
            "notion2api_provider_debugger",
            {"error_log": error_log, "operation": operation},
        )

    meta = _prompt_metadata("notion2api_content_sync")

    @server.prompt(name="notion2api_content_sync", title=meta.get("title"), description=meta.get("description"))
    def notion2api_content_sync(content: str, target: str = "") -> list[dict[str, str]]:
        return _prompt_messages("notion2api_content_sync", {"content": content, "target": target})

    meta = _prompt_metadata("notion2api_regression_validation")

    @server.prompt(name="notion2api_regression_validation", title=meta.get("title"), description=meta.get("description"))
    def notion2api_regression_validation(change_summary: str) -> list[dict[str, str]]:
        return _prompt_messages("notion2api_regression_validation", {"change_summary": change_summary})

    meta = _prompt_metadata("notion2api_security_redaction")

    @server.prompt(name="notion2api_security_redaction", title=meta.get("title"), description=meta.get("description"))
    def notion2api_security_redaction(raw_text: str) -> list[dict[str, str]]:
        return _prompt_messages("notion2api_security_redaction", {"raw_text": raw_text})


def create_server(
    *,
    base_url: str,
    api_key: str | None,
    timeout: float,
    host: str,
    port: int,
    mcp_path: str,
    stateless_http: bool = True,
) -> FastMCP:
    client = Notion2APIClient(base_url=base_url, api_key=api_key, timeout=timeout)
    transport_security = _transport_security_settings(host=host)
    server_name = os.getenv("MCP_SERVER_NAME", "notion2api").strip() or "notion2api"
    tool_namespace = os.getenv("SANITYCLOUD_TOOL_NAMESPACE", "").strip()
    invocation_alias = os.getenv("SANITYCLOUD_INVOCATION_ALIAS", "").strip()
    tool_prefix = re.sub(
        r"[^a-z0-9_]+",
        "_",
        os.getenv("MCP_TOOL_PREFIX", "notion2api").strip().lower(),
    ).strip("_") or "notion2api"

    def _tool_name(internal_name: str) -> str:
        suffix = internal_name.removeprefix("notion2api_")
        return f"{tool_prefix}_{suffix}"

    def _tool_description(description: str) -> str:
        return description.replace("Notion2API", server_name).replace(
            "notion2api_", f"{tool_prefix}_"
        )
    identity_instruction = ""
    if invocation_alias:
        identity_instruction = f"Human invocation alias: {invocation_alias}. "
    if tool_namespace:
        identity_instruction += f"SanityCloud smart-tool namespace: {tool_namespace}. "
    server = FastMCP(
        name=server_name,
        instructions=(
            identity_instruction
            + f"Use these tools to call the user's private local {server_name} service. "
            "Start with notion2api_health or notion2api_list_models if service status or model IDs are uncertain. "
            "Omit session_name to generate a descriptive unique session for new work; legacy 'op' values are also auto-generated. "
            "Chat submissions return immediately. Poll notion2api_get_chat_job and report its progress snapshot without exposing raw private reasoning. "
            "For ChatGPT uploads, stage one top-level file at a time with notion2api_stage_file; never pass /mnt/data paths through attachments. "
            "Do not claim document-grounded completion unless attachment_transfer_status is verified and attachment_count is nonzero."
        ),
        host=host,
        port=port,
        streamable_http_path=mcp_path,
        stateless_http=stateless_http,
        json_response=True,
        transport_security=transport_security,
    )
    register_notion2api_prompts(server)

    @server.tool(name=_tool_name("notion2api_health"), description=_tool_description("Check whether the configured Notion2API backend is reachable and healthy."), structured_output=True)
    async def notion2api_health() -> HealthOutput:
        data = await client.get("/health")
        return HealthOutput(
            ok=bool(data.get("ok", False)),
            status_code=data.get("status_code"),
            status=data.get("status"),
            accounts=data.get("accounts"),
            accounts_total=data.get("accounts_total"),
            accounts_cooling=data.get("accounts_cooling"),
            uptime=data.get("uptime"),
            raw=data,
        )

    @server.tool(name=_tool_name("notion2api_list_models"), description=_tool_description("List Notion2API models from the configured backend."), structured_output=True)
    async def notion2api_list_models() -> ListModelsOutput:
        data = await client.get("/v1/models")
        raw_models = data.get("data") if isinstance(data, dict) else None
        model_list = []
        if isinstance(raw_models, list):
            for entry in raw_models:
                info = _model_info_from_entry(entry)
                if info is not None:
                    model_list.append(info)
        return ListModelsOutput(
            ok=bool(data.get("ok", False)),
            status_code=data.get("status_code"),
            count=len(model_list),
            models=model_list,
            error=_error_summary(data),
        )

    @server.tool(name=_tool_name("notion2api_chat"), description=_tool_description("Submit a prompt to Notion2API using a durable session and return immediately with a pollable request_id. Terra is the default; omit model unless the user explicitly requests another. Omit session_name to generate one. Continue by session_name, conversation_id, or continue_from_request_id."), structured_output=True)
    async def notion2api_chat(
        prompt: str,
        model: MCPModel = DEFAULT_MODEL,
        system_prompt: str | None = None,
        persist_remote_chat: bool = True,
        session_name: MCPSessionName = None,
        conversation_id: str | None = None,
        continue_from_request_id: str | None = None,
        start_new_chat: bool = False,
        request_id: str | None = None,
        wait_seconds: MCPWaitSeconds = None,
        attachments: FileAttachments = None,
        staged_file_ids: StagedFileIds = None,
        require_attachments: bool = False,
        mode: MCPNotionMode = "default",
        task: MCPNotionTask = None,
        sources: MCPNotionSources = None,
        web_access: MCPWebAccess = None,
        persona: MCPNotionPersona = None,
        notion_instructions: MCPNotionInstructions = None,
        caller_id: str = "notion2api-mcp",
        caller_type: str = "mcp",
        caller_metadata: dict[str, Any] | None = None,
    ) -> ChatOutput:
        resolved_session_name = _infer_session_name(
            session_name,
            prompt,
            request_id=request_id or continue_from_request_id,
        )
        resolved_conversation_id, session_key, session_created = _conversation_id_for_session(
            resolved_session_name,
            start_new_chat=start_new_chat,
            conversation_id=conversation_id,
            continue_from_request_id=continue_from_request_id,
        )
        messages = _explicit_prompt_messages(prompt, system_prompt)
        local_paths = [*(attachments or []), *resolve_mcp_staged_files(staged_file_ids)]
        prepared = prepare_mcp_file_attachments(local_paths)
        payload = {
            "model": model,
            "messages": messages,
            "stream": False,
            "conversation_id": resolved_conversation_id,
            "session_name": session_key,
            "notion_mode": mode,
            "notion_task": task,
            "notion_sources": sources,
            "web_access": web_access,
            "notion_persona": persona,
            "notion_instructions": notion_instructions,
            "metadata": {
                "persist_remote_chat": persist_remote_chat,
                "mcp_session_name": session_key,
                "continue_from_request_id": continue_from_request_id,
                "require_attachments": require_attachments,
                "caller": {
                    "id": caller_id,
                    "type": caller_type,
                    **dict(caller_metadata or {}),
                },
            },
        }
        if prepared:
            payload["attachments"] = prepared
        return await _submit_or_resume_chat_job(
            client=client,
            path="/v1/chat/completions",
            payload=payload,
            model=model,
            session_key=session_key,
            conversation_id=resolved_conversation_id,
            session_created=session_created,
            request_id=request_id,
            wait_seconds=wait_seconds,
        )

    @server.tool(name=_tool_name("notion2api_stage_file"),
        description=(
            _tool_description("Stage one ChatGPT-uploaded file and return an opaque staged_file_id. Call this once "
            "per uploaded file, then pass all ids to notion2api_chat or notion2api_chat_completion.")
        ),
        structured_output=True,
    )
    async def notion2api_stage_file(
        file: TransferredFile,
        filename: str | None = None,
    ) -> StageFileOutput:
        try:
            staged = stage_mcp_transferred_file(file, filename)
            return StageFileOutput(
                ok=True,
                staged_file_id=str(staged.get("staged_file_id") or ""),
                filename=str(staged.get("filename") or ""),
                content_type=str(staged.get("content_type") or ""),
                size_bytes=int(staged.get("size_bytes") or 0),
                sha256=str(staged.get("sha256") or ""),
                expires_at=int(staged.get("expires_at") or 0),
            )
        except Exception as exc:
            return StageFileOutput(ok=False, error=f"{type(exc).__name__}: {exc}")

    @server.tool(name=_tool_name("notion2api_chat_with_file"),
        description=(
            _tool_description("Send one ChatGPT-uploaded file with a prompt to Notion2API. Use this tool for "
            "files located in ChatGPT /mnt/data; the top-level file argument triggers connector transfer. "
            "Terra is the default; omit model unless the user explicitly requests another.")
        ),
        structured_output=True,
    )
    async def notion2api_chat_with_file(
        file: TransferredFile,
        prompt: str,
        model: MCPModel = DEFAULT_MODEL,
        system_prompt: str | None = None,
        persist_remote_chat: bool = True,
        session_name: MCPSessionName = None,
        conversation_id: str | None = None,
        continue_from_request_id: str | None = None,
        start_new_chat: bool = False,
        request_id: str | None = None,
        wait_seconds: MCPWaitSeconds = None,
        mode: MCPNotionMode = "default",
        task: MCPNotionTask = None,
        sources: MCPNotionSources = None,
        web_access: MCPWebAccess = None,
        persona: MCPNotionPersona = None,
        notion_instructions: MCPNotionInstructions = None,
        caller_id: str = "notion2api-mcp",
        caller_type: str = "mcp",
        caller_metadata: dict[str, Any] | None = None,
    ) -> ChatOutput:
        return await notion2api_chat(
            prompt=prompt,
            model=model,
            system_prompt=system_prompt,
            persist_remote_chat=persist_remote_chat,
            session_name=session_name,
            conversation_id=conversation_id,
            continue_from_request_id=continue_from_request_id,
            start_new_chat=start_new_chat,
            request_id=request_id,
            wait_seconds=wait_seconds,
            attachments=[file],
            require_attachments=True,
            mode=mode,
            task=task,
            sources=sources,
            web_access=web_access,
            persona=persona,
            notion_instructions=notion_instructions,
            caller_id=caller_id,
            caller_type=caller_type,
            caller_metadata=caller_metadata,
        )

    @server.tool(name=_tool_name("notion2api_upload_file_to_page"),
        description=(
            _tool_description("Upload one ChatGPT-supplied file directly to a Notion page. The top-level file "
            "argument supports ChatGPT /mnt/data files and stages them safely for the local backend.")
        ),
        structured_output=True,
    )
    async def notion2api_upload_file_to_page(
        file: TransferredFile,
        page_id: str,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> UploadPageFileOutput:
        staged_path: Path | None = None
        was_staged = False
        try:
            staged_path, was_staged = stage_mcp_file_for_page(file, filename)
            data = await client.post(
                NOTION_PAGE_UPLOAD_ENDPOINT,
                {
                    "page_id": page_id,
                    "file_path": str(staged_path),
                    "filename": filename,
                    "content_type": content_type,
                },
            )
            return UploadPageFileOutput(
                ok=bool(data.get("ok", False)),
                page_id=str(data.get("page_id") or page_id),
                block_id=str(data.get("block_id") or ""),
                file_url=str(data.get("file_url") or ""),
                signed_get_url=str(data.get("signed_get_url") or ""),
                filename=str(data.get("filename") or filename or staged_path.name),
                content_type=str(data.get("content_type") or content_type or ""),
                size=int(data.get("size") or 0),
                error=_error_summary(data),
                raw=data,
            )
        finally:
            if staged_path is not None:
                cleanup_staged_mcp_file(staged_path, was_staged)

    @server.tool(name=_tool_name("notion2api_chat_completion"), description=_tool_description("Submit explicit messages to Notion2API using a durable session and return immediately with a pollable request_id. Terra is the default; omit model unless the user explicitly requests another. Omit session_name to generate one. Continue by session_name, conversation_id, or continue_from_request_id."), structured_output=True)
    async def notion2api_chat_completion(
        messages: list[dict[str, Any]],
        model: MCPModel = DEFAULT_MODEL,
        persist_remote_chat: bool = True,
        session_name: MCPSessionName = None,
        conversation_id: str | None = None,
        continue_from_request_id: str | None = None,
        start_new_chat: bool = False,
        request_id: str | None = None,
        wait_seconds: MCPWaitSeconds = None,
        attachments: FileAttachments = None,
        staged_file_ids: StagedFileIds = None,
        require_attachments: bool = False,
        mode: MCPNotionMode = "default",
        task: MCPNotionTask = None,
        sources: MCPNotionSources = None,
        web_access: MCPWebAccess = None,
        persona: MCPNotionPersona = None,
        notion_instructions: MCPNotionInstructions = None,
        caller_id: str = "notion2api-mcp",
        caller_type: str = "mcp",
        caller_metadata: dict[str, Any] | None = None,
    ) -> ChatOutput:
        explicit_messages = _copy_explicit_messages(messages)
        inferred_prompt = _prompt_text_from_messages(explicit_messages)
        resolved_session_name = _infer_session_name(
            session_name,
            inferred_prompt,
            request_id=request_id or continue_from_request_id,
        )
        resolved_conversation_id, session_key, session_created = _conversation_id_for_session(
            resolved_session_name,
            start_new_chat=start_new_chat,
            conversation_id=conversation_id,
            continue_from_request_id=continue_from_request_id,
        )
        local_paths = [*(attachments or []), *resolve_mcp_staged_files(staged_file_ids)]
        prepared = prepare_mcp_file_attachments(local_paths)
        payload = {
            "model": model,
            "messages": explicit_messages,
            "stream": False,
            "conversation_id": resolved_conversation_id,
            "session_name": session_key,
            "notion_mode": mode,
            "notion_task": task,
            "notion_sources": sources,
            "web_access": web_access,
            "notion_persona": persona,
            "notion_instructions": notion_instructions,
            "metadata": {
                "persist_remote_chat": persist_remote_chat,
                "mcp_session_name": session_key,
                "continue_from_request_id": continue_from_request_id,
                "require_attachments": require_attachments,
                "caller": {
                    "id": caller_id,
                    "type": caller_type,
                    **dict(caller_metadata or {}),
                },
            },
        }
        if prepared:
            payload["attachments"] = prepared
        return await _submit_or_resume_chat_job(
            client=client,
            path="/v1/chat/completions",
            payload=payload,
            model=model,
            session_key=session_key,
            conversation_id=resolved_conversation_id,
            session_created=session_created,
            request_id=request_id,
            wait_seconds=wait_seconds,
        )

    @server.tool(name=_tool_name("notion2api_responses"), description=_tool_description("Call Notion2API /v1/responses and return extracted output text plus the raw response. Terra is the default; omit model unless the user explicitly requests another."), structured_output=True)
    async def notion2api_responses(
        input_text: str,
        model: MCPModel = DEFAULT_MODEL,
        instructions: str | None = None,
        persist_remote_chat: bool = True,
        attachments: FileAttachments = None,
        staged_file_ids: StagedFileIds = None,
        require_attachments: bool = False,
        caller_id: str = "notion2api-mcp",
        caller_type: str = "mcp",
        caller_metadata: dict[str, Any] | None = None,
    ) -> ResponsesOutput:
        validated_input = validate_prompt_text(input_text, param="input_text")
        validated_instructions = validate_prompt_text(
            instructions, param="instructions", allow_none=True
        )
        local_paths = [*(attachments or []), *resolve_mcp_staged_files(staged_file_ids)]
        prepared = prepare_mcp_file_attachments(local_paths)
        manifest = _attachment_manifest_from_payload({"attachments": prepared})
        provenance = _attachment_provenance(require_attachments, manifest)
        if require_attachments and not manifest:
            return ResponsesOutput(
                ok=False,
                status_code=422,
                model=model,
                requested_model=model,
                **_runtime_audit(client, model),
                error="This request required attachments, but no file bytes were verified before submission.",
                **provenance,
                raw={"code": "required_attachments_missing", "attachment_provenance": provenance},
            )
        payload: dict[str, Any] = {
            "model": model,
            "input": validated_input,
            "metadata": {
                "persist_remote_chat": persist_remote_chat,
                "require_attachments": require_attachments,
                "caller": {
                    "id": caller_id,
                    "type": caller_type,
                    **dict(caller_metadata or {}),
                },
            },
        }
        if validated_instructions:
            payload["instructions"] = validated_instructions
        if prepared:
            payload["attachments"] = prepared
        data = await client.post("/v1/responses", payload)
        return _responses_output_from_backend(
            data=data,
            client=client,
            model=model,
            provenance=provenance,
        )

    def _hive_error_snapshot(exc: Exception, mission_id: str = "") -> HiveMissionSnapshot:
        return HiveMissionSnapshot(
            ok=False,
            found=False,
            db_path=str(default_hive_runtime_db_path()),
            mission_id=mission_id,
            error=str(exc),
        )

    @server.tool(name=_tool_name("notion2api_hive_create_mission"), description=_tool_description("Create a durable Hive mission with parallel worker lanes and conversation bindings."), structured_output=True)
    async def notion2api_hive_create_mission(
        title: str,
        objective: str,
        lifecycle_stage: str,
        work_units: list[dict[str, Any]] | None = None,
        authority_ceiling: str = "A3",
        parent_context_id: str = "",
        mission_id: str | None = None,
        idempotency_key: str | None = None,
        actor: str = "notion2api",
    ) -> HiveMissionSnapshot:
        try:
            specs = [HiveWorkUnitSpec.model_validate(item) for item in (work_units or [])]
            return get_hive_runtime_store().create_mission(
                title=title,
                objective=objective,
                lifecycle_stage=lifecycle_stage,
                work_units=specs,
                authority_ceiling=authority_ceiling,
                parent_context_id=parent_context_id,
                mission_id=mission_id,
                idempotency_key=idempotency_key,
                actor=actor,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _hive_error_snapshot(exc, mission_id or "")

    @server.tool(name=_tool_name("notion2api_hive_status"), description=_tool_description("Read one durable Hive mission, its worker lanes, events, and latest fan-in decision."), structured_output=True)
    async def notion2api_hive_status(
        mission_id: str,
        event_limit: int = 200,
        action_limit: int = 200,
    ) -> HiveMissionSnapshot:
        try:
            return get_hive_runtime_store().get_mission(
                mission_id, event_limit=event_limit, action_limit=action_limit
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _hive_error_snapshot(exc, mission_id)

    @server.tool(name=_tool_name("notion2api_hive_append_event"), description=_tool_description("Append a typed Hive event or SyncPulse and optionally update one worker lane state."), structured_output=True)
    async def notion2api_hive_append_event(
        mission_id: str,
        event_type: str,
        sender: str,
        payload: dict[str, Any] | None = None,
        recipient: str = "swarm",
        work_unit_id: str = "",
        context_version: int = 0,
        expected_mission_revision: int | None = None,
        work_unit_status: str | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        try:
            return get_hive_runtime_store().append_event(
                mission_id=mission_id,
                event_type=event_type,
                sender=sender,
                payload=payload,
                recipient=recipient,
                work_unit_id=work_unit_id,
                context_version=context_version,
                expected_mission_revision=expected_mission_revision,
                work_unit_status=work_unit_status,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _hive_error_snapshot(exc, mission_id)

    @server.tool(name=_tool_name("notion2api_hive_cancel"), description=_tool_description("Cancel a Hive mission cooperatively while preserving completed work and its evidence ledger."), structured_output=True)
    async def notion2api_hive_cancel(
        mission_id: str,
        reason: str,
        actor: str = "notion2api",
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        try:
            return get_hive_runtime_store().cancel_mission(
                mission_id=mission_id,
                reason=reason,
                actor=actor,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _hive_error_snapshot(exc, mission_id)

    @server.tool(name=_tool_name("notion2api_hive_fan_in"), description=_tool_description("Record an Emerald City fan-in decision with evidence and preserved dissent."), structured_output=True)
    async def notion2api_hive_fan_in(
        mission_id: str,
        status: str,
        summary: str,
        dissent: list[dict[str, Any]] | None = None,
        evidence: list[dict[str, Any]] | None = None,
        actor: str = "emerald-city",
        close_mission: bool = False,
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        try:
            return get_hive_runtime_store().fan_in(
                mission_id=mission_id,
                status=status,
                summary=summary,
                dissent=dissent,
                evidence=evidence,
                actor=actor,
                close_mission=close_mission,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _hive_error_snapshot(exc, mission_id)

    @server.tool(name=_tool_name("notion2api_list_sessions"), description=_tool_description("List named persistent Notion2API MCP chat sessions with local and remote identifiers."), structured_output=True)
    async def notion2api_list_sessions() -> ListSessionsOutput:
        records = _load_session_records()
        items = [
            {"session_name": name, **record}
            for name, record in sorted(records.items())
        ]
        return ListSessionsOutput(
            ok=True,
            count=len(items),
            default_session=AUTO_SESSION_LABEL,
            state_path=str(DEFAULT_SESSION_STATE_PATH),
            sessions=items,
        )

    @server.tool(name=_tool_name("notion2api_allow_unsafe_url_once"), description=_tool_description("Grant Notion's Allow once confirmation for pending connections.web.loadPage calls and resume the interrupted inference through runInferenceTranscript. Resolves the remote thread from a named MCP session unless notion_thread_id is provided."), structured_output=True)
    async def notion2api_allow_unsafe_url_once(
        session_name: str = DEFAULT_SESSION_NAME,
        notion_thread_id: str | None = None,
        tool_step_ids: list[str] | None = None,
    ) -> UnsafeUrlContinuationOutput:
        key = _session_key(session_name)
        record = _load_session_records().get(key) or {}
        thread_id = str(
            notion_thread_id
            or record.get("remote_chat_id")
            or record.get("notion_thread_id")
            or ""
        ).strip()
        if not thread_id:
            return UnsafeUrlContinuationOutput(
                ok=False,
                session_name=key,
                reason="session_has_no_remote_notion_thread",
            )
        data = await client.post(
            "/v1/notion/unsafe_url/allow_once",
            {
                "thread_id": thread_id,
                "tool_step_ids": list(tool_step_ids or []),
            },
        )
        return UnsafeUrlContinuationOutput(
            ok=bool(data.get("ok")),
            continued=bool(data.get("continued")),
            approved=bool(data.get("approved")),
            session_name=key,
            thread_id=str(data.get("thread_id") or thread_id),
            tool_step_ids=list(data.get("tool_step_ids") or []),
            urls=list(data.get("urls") or []),
            trace_id=str(data.get("trace_id") or ""),
            stream_completed=bool(data.get("stream_completed")),
            event_count=int(data.get("event_count") or 0),
            event_types=list(data.get("event_types") or []),
            applied_tool_step_ids=list(data.get("applied_tool_step_ids") or []),
            unresolved_tool_step_ids=list(data.get("unresolved_tool_step_ids") or []),
            reason=str(data.get("reason") or ""),
            raw=data,
        )

    @server.tool(name=_tool_name("notion2api_get_messages"), description=_tool_description("Read recent locally persisted messages for a persistent Notion2API MCP session without sending a new chat message. Useful after a client-side timeout."), structured_output=True)
    async def notion2api_get_messages(
        session_name: str = DEFAULT_SESSION_NAME,
        limit: int = 10,
        conversation_id: str | None = None,
    ) -> MessagesOutput:
        return _read_local_messages(session_name=session_name, conversation_id=conversation_id, limit=limit)

    @server.tool(name=_tool_name("notion2api_get_last_response"), description=_tool_description("Read the latest locally persisted assistant response for a persistent Notion2API MCP session without sending a new chat message. Useful after a client-side timeout."), structured_output=True)
    async def notion2api_get_last_response(
        session_name: str = DEFAULT_SESSION_NAME,
        conversation_id: str | None = None,
    ) -> LastResponseOutput:
        return _read_last_local_response(session_name=session_name, conversation_id=conversation_id)

    @server.tool(name=_tool_name("notion2api_get_chat_job"), description=_tool_description("Inspect a retry-safe Notion2API MCP chat job. Returns bounded activity/tasks, remote chat id, and stall detection without exposing raw private reasoning. When status is indeterminate_output/quarantined, set include_quarantined=true to read generated-but-quarantined text; it is not authoritative."), structured_output=True)
    async def notion2api_get_chat_job(
        request_id: str,
        include_last_response: bool = False,
        include_response: bool = False,
        include_quarantined: bool = False,
        cancel_if_stalled: bool = False,
    ) -> ChatJobOutput:
        result = _chat_job_output(
            request_id=request_id,
            include_last_response=include_last_response,
            include_response=include_response,
            include_quarantined=include_quarantined,
        )
        if (
            cancel_if_stalled
            and result.found
            and result.dead_loop_suspected
            and result.status in {"running", "pending"}
        ):
            return _cancel_chat_job(
                request_id,
                reason="Cancelled after polling detected no meaningful public progress.",
            )
        return result

    @server.tool(name=_tool_name("notion2api_cancel_chat_job"), description=_tool_description("Cancel a pending or running Notion2API MCP chat job to stop a suspected dead loop or obsolete request."), structured_output=True)
    async def notion2api_cancel_chat_job(
        request_id: str,
        reason: str = "Cancelled by caller.",
    ) -> ChatJobOutput:
        return _cancel_chat_job(request_id=request_id, reason=reason)

    @server.tool(name=_tool_name("notion2api_reset_session"), description=_tool_description("Start a fresh persistent Notion2API MCP chat for a named session."), structured_output=True)
    async def notion2api_reset_session(session_name: str = DEFAULT_SESSION_NAME) -> SessionActionOutput:
        with _SESSION_STATE_MUTEX:
            key = _session_key(session_name)
            sessions = _load_session_state()
            previous = sessions.get(key)
            conversation_id, session_key, _created = _conversation_id_for_session(
                key, start_new_chat=True
            )
        return SessionActionOutput(
            ok=True,
            action="reset",
            session_name=session_key,
            conversation_id=conversation_id,
            previous_conversation_id=previous,
            state_path=str(DEFAULT_SESSION_STATE_PATH),
        )

    @server.tool(name=_tool_name("notion2api_rename_session"), description=_tool_description("Rename a persistent Notion2API MCP chat session without changing its conversation binding."), structured_output=True)
    async def notion2api_rename_session(
        old_session_name: str,
        new_session_name: str,
        overwrite: bool = False,
    ) -> SessionActionOutput:
        with _SESSION_STATE_MUTEX:
            old_key = _session_key(old_session_name)
            new_key = _session_key(new_session_name)
            records = _load_session_records()
            if old_key not in records:
                return SessionActionOutput(
                    ok=False,
                    action="rename",
                    session_name=new_key,
                    conversation_id="",
                    previous_session_name=old_key,
                    state_path=str(DEFAULT_SESSION_STATE_PATH),
                )
            if new_key in records and not overwrite and new_key != old_key:
                return SessionActionOutput(
                    ok=False,
                    action="rename",
                    session_name=new_key,
                    conversation_id=str(records[new_key].get("conversation_id") or ""),
                    previous_session_name=old_key,
                    previous_conversation_id=str(records[old_key].get("conversation_id") or ""),
                    overwritten=False,
                    state_path=str(DEFAULT_SESSION_STATE_PATH),
                )
            source_record = dict(records[old_key])
            conversation_id = str(source_record.get("conversation_id") or "")
            previous_record = records.get(new_key) or {}
            previous_target = str(previous_record.get("conversation_id") or "") or None
            if old_key != new_key:
                source_record["updated_at"] = _now_ms()
                records[new_key] = source_record
                records.pop(old_key, None)
                _save_session_records(records)
        return SessionActionOutput(
            ok=True,
            action="rename",
            session_name=new_key,
            conversation_id=conversation_id,
            previous_session_name=old_key,
            previous_conversation_id=previous_target,
            overwritten=bool(previous_target and previous_target != conversation_id),
            state_path=str(DEFAULT_SESSION_STATE_PATH),
        )

    return server


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_csv(name: str, default: list[str]) -> list[str]:
    raw = os.getenv(name, "")
    values = [item.strip() for item in raw.split(",") if item.strip()]
    return values or default


def _transport_security_settings(host: str) -> TransportSecuritySettings:
    default_hosts = [
        "127.0.0.1:*",
        "localhost:*",
        "[::1]:*",
        "0.0.0.0:*",
        "notion2api-mcp.ptelectronics.net",
        "notion2api-mcp.ptelectronics.net:*",
    ]
    if host and host not in {"0.0.0.0", "::"}:
        default_hosts.append(host if ":" in host else f"{host}:*")

    default_origins = [
        "http://127.0.0.1:*",
        "http://localhost:*",
        "http://[::1]:*",
        "https://notion2api-mcp.ptelectronics.net",
        "http://notion2api-mcp.ptelectronics.net",
        "https://chatgpt.com",
        "https://chat.openai.com",
    ]
    return TransportSecuritySettings(
        enable_dns_rebinding_protection=_env_bool("MCP_ENABLE_DNS_REBINDING_PROTECTION", True),
        allowed_hosts=_env_csv("MCP_ALLOWED_HOSTS", default_hosts),
        allowed_origins=_env_csv("MCP_ALLOWED_ORIGINS", default_origins),
    )


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Run the Notion2API MCP wrapper.")
    parser.add_argument("--transport", choices=("streamable-http", "stdio", "sse"), default=os.getenv("MCP_TRANSPORT", "streamable-http"))
    parser.add_argument("--base-url", default=os.getenv("MCP_NOTION2API_BASE_URL", os.getenv("NOTION2API_BASE_URL", DEFAULT_BASE_URL)))
    parser.add_argument(
        "--api-key",
        default=os.getenv(
            "MCP_NOTION2API_API_KEY",
            os.getenv("NOTION2API_API_KEY", os.getenv("NOTION2API_KEY", os.getenv("API_KEY", ""))),
        ),
    )
    parser.add_argument("--timeout", type=float, default=_env_float("MCP_NOTION2API_TIMEOUT", DEFAULT_TIMEOUT_SECONDS))
    parser.add_argument("--host", default=os.getenv("MCP_HOST", DEFAULT_MCP_HOST))
    parser.add_argument("--port", type=int, default=_env_int("MCP_PORT", DEFAULT_MCP_PORT))
    parser.add_argument("--mcp-path", default=os.getenv("MCP_PATH", DEFAULT_MCP_PATH))
    args = parser.parse_args(argv)

    reconciliation = _reconcile_orphaned_chat_jobs_on_startup()
    if reconciliation["completed"] or reconciliation["stale"]:
        logger.info(
            "Reconciled orphaned MCP chat jobs on startup",
            extra={
                "request_info": {
                    "event": "mcp_job_startup_reconciliation",
                    **reconciliation,
                }
            },
        )

    server = create_server(
        base_url=args.base_url,
        api_key=args.api_key,
        timeout=args.timeout,
        host=args.host,
        port=args.port,
        mcp_path=args.mcp_path,
    )
    server.run(transport=args.transport)


if __name__ == "__main__":
    main()
