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
from app.model_registry import get_model_route_resolution
from app.file_discovery_routing import (
    FileRoutingDecision,
    route_file_operation,
)
from app.aigentbee_workbench import (
    LeaderRequestReceipt,
    MAX_HISTORY_LIMIT,
    SWARM_WIDGET_URI,
    SwarmWorkbenchOutput,
    build_leader_prompt,
    build_swarm_workbench,
    leader_session_name,
    load_swarm_widget_html,
    validate_leader_request,
    validate_prerequisite_progression,
)
from app.mcp_observability import (
    install_mcp_noise_filter,
    mcp_observability_snapshot,
    record_mcp_http_error,
)
from app.output_hygiene import detect_visible_output_contamination
from app.output_integrity import assess_output_integrity
from app.session_retention import (
    archive_and_filter_sessions,
    build_session_retention_plan,
)
from app.hive_runtime import (
    HiveDelegatedTaskSpec,
    HiveHandoffReceipt,
    HiveMissionSnapshot,
    HiveProjectContract,
    HiveRuntimeError,
    HiveWorkUnitSpec,
    default_hive_runtime_db_path,
    get_hive_runtime_store,
)
from app.hive_multithread import leader_conversation_id
from app.hive_dispatcher import (
    HiveAdapterSnapshot,
    HiveExecutionSnapshot,
    get_hive_execution_dispatcher_store,
)
from app.hive_external_effects import (
    ExternalEffectSnapshot,
    get_hive_external_effect_store,
)
from app.hive_materialization import (
    HiveMaterializationSnapshot,
    get_hive_materialization_store,
)
from app.hive_workforce import (
    HiveInvocationPlan,
    WorkforceSnapshot,
    get_hive_workforce_store,
)
from app.hive_workforce_lifecycle import (
    LeaseReconciliationSnapshot,
    RecruitmentMode,
    WorkforceAuditSnapshot,
)
from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations

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
DEFAULT_CHAT_JOB_WATCHDOG_SECONDS = 5.0
MAX_PROGRESS_REASONING_CHARS = 200_000
MAX_CHAT_JOB_RESPONSE_PREVIEW_CHARS = 4_000
MAX_CHAT_JOB_PROMPT_CHARS = 20_000
DEFAULT_STAGED_FILE_TTL_SECONDS = 24 * 60 * 60
STAGED_FILE_ID_RE = re.compile(r"^stage-[a-f0-9]{32}$")
SESSION_STATE_VERSION = 2
_SESSION_STATE_MUTEX = threading.RLock()
_CHAT_JOB_STATE_MUTEX = threading.RLock()
_CHAT_JOB_TASKS: dict[str, asyncio.Task[dict[str, Any]]] = {}
_CHAT_JOB_WATCHDOG_TASK: asyncio.Task[None] | None = None
_CHAT_JOB_STATE_CACHE: dict[
    str, tuple[tuple[int, int, int, int] | None, dict[str, Any]]
] = {}
_CHAT_JOB_DB_READY: set[str] = set()
logger = logging.getLogger(__name__)
CHAT_JOB_STATE_WRITE_RETRIES = 5
CHAT_JOB_STATE_WRITE_BACKOFF_SECONDS = 0.05
CHAT_JOB_LEDGER_SCHEMA_VERSION = 2


class HealthOutput(BaseModel):
    ok: bool = Field(description="Whether the backend health call succeeded.")
    status_code: int | None = Field(default=None, description="HTTP status code returned by Notion2API.")
    status: str | None = Field(default=None, description="Backend status string, usually ok.")
    accounts: int | None = Field(default=None, description="Ready account count reported by Notion2API.")
    accounts_total: int | None = Field(default=None, description="Total configured account count.")
    accounts_cooling: int | None = Field(default=None, description="Number of accounts currently cooling down.")
    uptime: int | float | None = Field(default=None, description="Backend uptime, if reported.")
    account_selection: dict[str, Any] = Field(default_factory=dict, description="Safe Auto/Pinned account-selection summary.")
    governance: dict[str, Any] = Field(default_factory=dict, description="Canonical governance/teamspace receipt.")
    notion_admission: dict[str, Any] = Field(
        default_factory=dict,
        description="Shared Notion admission queue, throttling, and idempotency receipt.",
    )
    conversation_compression: dict[str, Any] = Field(
        default_factory=dict,
        description="Compression backend, warning-coalescing, and context telemetry.",
    )
    mcp_runtime: dict[str, Any] = Field(
        default_factory=dict,
        description="MCP transport correlation and routine-log coalescing telemetry.",
    )
    raw: dict[str, Any] = Field(default_factory=dict, description="Raw backend health response plus local MCP telemetry.")


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
    alias_resolution: dict[str, Any] | None = Field(default=None, description="Configured logical alias to concrete Notion route mapping.")
    model_route_disposition: str = Field(default="direct_route", description="Route classification such as alias_resolution or verified_substitution.")
    caller_id: str = Field(default="", description="Stable identity of the system or agent that initiated the request.")
    caller_type: str = Field(default="", description="Caller class, such as repoai, chatgpt, or mcp.")
    caller_metadata: dict[str, Any] | None = Field(default=None, description="Bounded caller provenance supplied with the request.")
    governance: dict[str, Any] = Field(default_factory=dict, description="Canonical governance/teamspace receipt applied by the backend.")
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
    alias_resolution: dict[str, Any] | None = Field(default=None)
    model_route_disposition: str = Field(default="direct_route")
    caller_id: str = Field(default="")
    caller_type: str = Field(default="")
    caller_metadata: dict[str, Any] | None = Field(default=None)
    governance: dict[str, Any] = Field(default_factory=dict)
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
    retention: dict[str, Any] = Field(
        default_factory=dict,
        description="Preview-only retention plan; no session is removed by listing.",
    )


class SessionRetentionOutput(BaseModel):
    ok: bool = Field(description="Whether retention planning or application succeeded.")
    applied: bool = Field(default=False, description="Whether eligible bindings were archived and removed from the active index.")
    state_path: str = Field(default="", description="Active MCP session-state path.")
    archive_path: str = Field(default="", description="Append-only JSONL archive receipt path.")
    policy: dict[str, Any] = Field(default_factory=dict)
    counts: dict[str, int] = Field(default_factory=dict)
    protected: list[dict[str, Any]] = Field(default_factory=list)
    candidates: list[dict[str, Any]] = Field(default_factory=list)
    archived: int = Field(default=0)
    retained: int = Field(default=0)
    error: str | None = Field(default=None)


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
    requested_model: str = Field(default="", description="Logical model alias requested for the job.")
    resolved_model: str = Field(default="", description="Concrete Notion route resolved for the job.")
    alias_resolution: dict[str, Any] | None = Field(default=None, description="Configured alias resolution for the job.")
    model_route_disposition: str = Field(default="direct_route", description="Requested-to-route classification.")
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
    retry_safe: bool = Field(
        default=False,
        description="Whether retrying this same request_id is safe without first reconciling an unknown upstream outcome.",
    )
    reconciliation_required: bool = Field(
        default=False,
        description="Whether the local tracker lacks a confirmed terminal upstream outcome and must be reconciled before replacement work.",
    )
    cancellation_state: str = Field(
        default="",
        description="Cancellation lifecycle state. Local cancellation is not treated as upstream cancellation acknowledgement.",
    )
    upstream_execution_state: str = Field(
        default="",
        description="Observed upstream execution state such as unknown, active, terminal, or not_started.",
    )
    cancel_requested_at: int = Field(default=0, description="Unix epoch milliseconds when cancellation was requested.")
    cancelled_from_status: str = Field(default="", description="Job status immediately before local cancellation.")
    stalled_for_seconds_at_cancel: float = Field(
        default=0.0,
        description="Immutable stall duration captured immediately before cancellation.",
    )
    dead_loop_suspected_at_cancel: bool = Field(
        default=False,
        description="Whether the stall detector was active immediately before cancellation.",
    )
    late_completion_detected: bool = Field(
        default=False,
        description="Whether a terminal local conversation checkpoint was observed after local cancellation.",
    )
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
            "ChatGPT /mnt/data paths here; stage each ChatGPT upload with stage_file "
            "or use chat_with_file for one file."
        ),
    ),
]

StagedFileIds = Annotated[
    list[str] | None,
    Field(
        default=None,
        description=(
            "Opaque ids returned by stage_file. Use one staging call per ChatGPT "
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
            "immediately with a request_id to poll via get_chat_job."
        )
    ),
]

MCPNotionMode = Annotated[
    Literal["default", "ask", "research"],
    Field(description="Notion AI mode: default can search and edit; ask is read-only; research enables deeper research."),
]
MCPNotionTask = Annotated[
    Literal["visualize", "generate_image", "create_slides", "spreadsheet", "deep_research"] | None,
    Field(description="Optional Notion AI task preset for data/HTML visualizations, image generation, slide decks, spreadsheets, or deep research."),
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

    def _headers(self, request_id: str | None = None) -> dict[str, str]:
        headers = {"Accept": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        if request_id:
            headers["X-Request-ID"] = request_id
        return headers

    async def get(self, path: str) -> dict[str, Any]:
        request_id = f"mcp-{uuid.uuid4().hex}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.get(
                f"{self.base_url}{path}",
                headers=self._headers(request_id),
            )
        return _json_or_error(
            response,
            correlation_id=request_id,
            method="GET",
            path=path,
        )

    async def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        request_id = f"mcp-{uuid.uuid4().hex}"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(
                f"{self.base_url}{path}",
                headers=self._headers(request_id),
                json=payload,
            )
        return _json_or_error(
            response,
            correlation_id=request_id,
            method="POST",
            path=path,
        )

    async def post_chat_stream(self, path: str, payload: dict[str, Any], on_progress: Any) -> dict[str, Any]:
        stream_payload = dict(payload)
        stream_payload["stream"] = True
        content_parts: list[str] = []
        reasoning_buffer = ""
        model_metadata: dict[str, Any] = {}
        response_model = str(payload.get("model") or "")
        event_count = 0
        last_update = 0.0
        hygiene: dict[str, Any] = {}
        quarantined = False
        terminal_finish_reason = ""
        done_received = False
        stream_error: dict[str, Any] | None = None

        request_id = f"mcp-{uuid.uuid4().hex}"
        headers = self._headers(request_id)
        headers["Accept"] = "text/event-stream"
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            async with client.stream("POST", f"{self.base_url}{path}", headers=headers, json=stream_payload) as response:
                if response.status_code >= 400:
                    body = await response.aread()
                    return _json_or_error(
                        httpx.Response(
                            response.status_code,
                            headers=response.headers,
                            content=body,
                        ),
                        correlation_id=request_id,
                        method="POST",
                        path=path,
                    )
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
                    if not raw:
                        continue
                    if raw == "[DONE]":
                        done_received = True
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

                    event_type = str(event.get("type") or "").strip().lower()
                    error_payload = event.get("error")
                    if event_type in {"error", "stream_error", "stream-error"} or error_payload is not None:
                        if isinstance(error_payload, dict):
                            error_detail = dict(error_payload)
                        elif error_payload not in (None, ""):
                            error_detail = {"message": str(error_payload)}
                        else:
                            error_detail = dict(event)
                        raw_status = (
                            event.get("status_code")
                            or event.get("status")
                            or error_detail.get("status_code")
                            or error_detail.get("status")
                        )
                        try:
                            status_code = int(raw_status)
                        except (TypeError, ValueError):
                            status_code = 0
                        fingerprint = json.dumps(error_detail, ensure_ascii=False).lower()
                        if not 100 <= status_code <= 599:
                            status_code = 429 if any(
                                marker in fingerprint
                                for marker in ("429", "too many requests", "too_many_requests", "rate limit", "rate_limit")
                            ) else 502
                        stream_error = {
                            "ok": False,
                            "status_code": status_code,
                            "status": "upstream_rate_limit" if status_code == 429 else "upstream_error",
                            "model": response_model,
                            "actual_model": str(model_metadata.get("actual_model") or ""),
                            "model_metadata": model_metadata or None,
                            "error": error_detail,
                        }
                        break
                    if event_type == "output_hygiene":
                        candidate = event.get("hygiene")
                        if isinstance(candidate, dict):
                            hygiene = dict(candidate)
                            integrity = hygiene.get("output_integrity")
                            if isinstance(integrity, dict) and integrity.get("quarantine_required"):
                                quarantined = True
                        continue
                    if event_type == "content_replace":
                        replacement = event.get("content")
                        if isinstance(replacement, str):
                            content_parts[:] = [replacement]
                            event_count += 1
                        continue
                    if event_type == "thinking_replace":
                        replacement = event.get("text") or event.get("thinking")
                        if isinstance(replacement, str):
                            reasoning_buffer = replacement[-MAX_PROGRESS_REASONING_CHARS:]
                            event_count += 1
                        continue

                    choices = event.get("choices")
                    if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
                        continue
                    choice = choices[0]
                    finish_reason = str(choice.get("finish_reason") or "").strip().lower()
                    if finish_reason:
                        terminal_finish_reason = finish_reason
                    if finish_reason == "content_filter":
                        quarantined = True
                    delta = choice.get("delta")
                    if not isinstance(delta, dict):
                        continue
                    content = delta.get("content")
                    reasoning = delta.get("reasoning_content") or delta.get("thinking")
                    if isinstance(content, str) and content:
                        content_parts.append(content)
                    if isinstance(reasoning, str) and reasoning:
                        reasoning_buffer = (reasoning_buffer + reasoning)[-MAX_PROGRESS_REASONING_CHARS:]
                    if content or reasoning:
                        event_count += 1
                        now = time.monotonic()
                        if now - last_update >= 0.75:
                            on_progress(reasoning_buffer, "".join(content_parts), event_count, False)
                            last_update = now

        visible_content = "".join(content_parts)
        if stream_error is not None:
            on_progress(reasoning_buffer, "", event_count, True)
            return stream_error
        if not terminal_finish_reason and not done_received:
            on_progress(reasoning_buffer, "", event_count, True)
            return {
                "ok": False,
                "status_code": 502,
                "status": "stream_incomplete",
                "model": response_model,
                "actual_model": str(model_metadata.get("actual_model") or ""),
                "model_metadata": model_metadata or None,
                "error": {
                    "code": "STREAM_INCOMPLETE",
                    "message": "Backend stream ended without a terminal finish event.",
                    "partial_content_chars": len(visible_content),
                },
            }
        if quarantined:
            visible_content = ""
        on_progress(reasoning_buffer, visible_content, event_count, True)

        upstream_integrity = hygiene.get("output_integrity") if isinstance(hygiene, dict) else None
        if quarantined:
            if not isinstance(upstream_integrity, dict):
                upstream_integrity = assess_output_integrity(
                    "",
                    additional_reasons=("upstream_content_filter",),
                )
            return {
                "ok": False,
                "status_code": 422,
                "status": "indeterminate_output",
                "model": response_model,
                "actual_model": str(model_metadata.get("actual_model") or ""),
                "model_metadata": model_metadata or None,
                "choices": [{
                    "index": 0,
                    "message": {"role": "assistant", "content": ""},
                    "finish_reason": terminal_finish_reason or "content_filter",
                }],
                "output_integrity": upstream_integrity,
                "hygiene": hygiene or {"output_integrity": upstream_integrity},
                "quarantined": True,
                "error": {
                    "code": "OUTPUT_CONTAMINATED",
                    "message": "Assistant output was quarantined by the backend stream integrity guard.",
                },
            }

        message: dict[str, Any] = {"role": "assistant", "content": visible_content}
        result = {
            "ok": True,
            "status_code": 200,
            "model": response_model,
            "actual_model": str(model_metadata.get("actual_model") or ""),
            "model_metadata": model_metadata or None,
            "choices": [{"index": 0, "message": message, "finish_reason": terminal_finish_reason or "stop"}],
        }
        if isinstance(upstream_integrity, dict):
            result["output_integrity"] = upstream_integrity
        if hygiene:
            result["hygiene"] = hygiene
        return result



def _json_or_error(
    response: httpx.Response,
    *,
    correlation_id: str = "",
    method: str = "",
    path: str = "",
) -> dict[str, Any]:
    content_type = response.headers.get("content-type", "")
    try:
        data: Any = response.json() if "json" in content_type.lower() or response.content else {}
    except ValueError:
        data = {"text": response.text[:4000]}

    if response.status_code >= 400:
        response_request_id = str(
            response.headers.get("X-Request-ID")
            or response.headers.get("X-Correlation-ID")
            or response.headers.get("traceparent")
            or ""
        ).strip()
        correlation = {
            "request_id": str(correlation_id or response_request_id),
            "response_request_id": response_request_id,
            "method": str(method).upper(),
            "path": str(path),
        }
        record_mcp_http_error(
            status_code=response.status_code,
            request_id=correlation["request_id"],
            response_request_id=response_request_id,
            method=correlation["method"],
            path=correlation["path"],
        )
        logger.warning(
            "MCP backend HTTP request failed",
            extra={
                "request_info": {
                    "event": "mcp_backend_http_error",
                    "status_code": response.status_code,
                    **correlation,
                }
            },
        )
        return {
            "ok": False,
            "status_code": response.status_code,
            "error": data,
            "correlation": correlation,
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


def _governance_trace(data: dict[str, Any]) -> dict[str, Any]:
    model_metadata = (
        data.get("model_metadata")
        if isinstance(data.get("model_metadata"), dict)
        else {}
    )
    governance = (
        model_metadata.get("governance")
        if isinstance(model_metadata.get("governance"), dict)
        else data.get("governance")
    )
    return {
        "governance": dict(governance)
        if isinstance(governance, dict)
        else {}
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
    route_resolution = get_model_route_resolution(requested)
    resolved = str(
        metadata.get("notion_requested_model")
        or metadata.get("resolved_model")
        or route_resolution.get("resolved_model")
        or requested
    ).strip()
    alias_resolution = None
    supplied_alias = metadata.get("alias_resolution")
    if (
        isinstance(supplied_alias, dict)
        and str(supplied_alias.get("resolved_model") or "").strip() == resolved
    ):
        alias_resolution = dict(supplied_alias)
    elif (
        route_resolution.get("resolution_kind") == "configured_alias"
        and resolved == str(route_resolution.get("resolved_model") or "")
    ):
        alias_resolution = {
            key: route_resolution[key]
            for key in (
                "requested_model",
                "canonical_model",
                "resolved_model",
                "public_model",
                "display_name",
                "resolution_kind",
            )
        }
    route_disposition = str(metadata.get("model_route_disposition") or "").strip()
    if not route_disposition:
        route_disposition = "alias_resolution" if alias_resolution else "direct_route"

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
        route_disposition = (
            "verified_substitution" if verified else "unverified_route_mismatch"
        )
    return {
        "requested_model": requested,
        "resolved_model": resolved,
        "verified_model": verified_model,
        "model_identity_verified": verified,
        "model_identity_source": source,
        "model_identity_confidence": confidence,
        "model_substitution": substitution,
        "alias_resolution": alias_resolution,
        "model_route_disposition": route_disposition,
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
        **_governance_trace(data),
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


def _configured_chat_job_watchdog_seconds() -> float:
    raw = os.getenv("MCP_NOTION2API_JOB_WATCHDOG_SECONDS", "")
    configured = (
        _safe_float(raw, DEFAULT_CHAT_JOB_WATCHDOG_SECONDS)
        if raw.strip()
        else DEFAULT_CHAT_JOB_WATCHDOG_SECONDS
    )
    return min(60.0, max(1.0, configured))


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
        CREATE TABLE IF NOT EXISTS chat_job_events (
            event_id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id TEXT NOT NULL,
            event_at INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            previous_status TEXT NOT NULL DEFAULT '',
            new_status TEXT NOT NULL DEFAULT '',
            metadata_json TEXT NOT NULL DEFAULT '{}'
        );
        CREATE INDEX IF NOT EXISTS idx_chat_job_events_request_time
            ON chat_job_events(request_id, event_at, event_id);
        """
    )
    conn.execute(
        "INSERT OR REPLACE INTO ledger_metadata(key, value) VALUES ('schema_version', ?)",
        (str(CHAT_JOB_LEDGER_SCHEMA_VERSION),),
    )


def _chat_job_event_metadata(job: dict[str, Any]) -> dict[str, Any]:
    progress = job.get("progress") if isinstance(job.get("progress"), dict) else {}
    return {
        "last_progress_at": int(job.get("last_progress_at") or 0),
        "stalled_for_seconds": float(job.get("stalled_for_seconds") or 0.0),
        "dead_loop_suspected": bool(job.get("dead_loop_suspected")),
        "cancel_recommended": bool(job.get("cancel_recommended")),
        "cancel_requested_at": int(job.get("cancel_requested_at") or 0),
        "cancellation_state": str(job.get("cancellation_state") or ""),
        "upstream_execution_state": str(job.get("upstream_execution_state") or ""),
        "reconciliation_required": bool(job.get("reconciliation_required")),
        "progress_phase": str(progress.get("phase") or ""),
        "quarantined": bool(job.get("quarantined")),
    }


def _append_chat_job_transition_events(
    conn: sqlite3.Connection,
    state: dict[str, Any],
    request_ids: set[str] | None,
) -> None:
    jobs = state.get("jobs") if isinstance(state.get("jobs"), dict) else {}
    ids = request_ids if request_ids is not None else {str(key) for key in jobs}
    for request_id in ids:
        job = jobs.get(request_id)
        if not isinstance(job, dict):
            continue
        row = conn.execute(
            "SELECT status FROM chat_jobs WHERE request_id = ?",
            (request_id,),
        ).fetchone()
        previous_status = str(row["status"] if row is not None else "")
        new_status = str(job.get("status") or "")
        if previous_status == new_status:
            continue
        event_type = "job_created" if not previous_status else "status_transition"
        conn.execute(
            """
            INSERT INTO chat_job_events(
                request_id, event_at, event_type, previous_status, new_status, metadata_json
            ) VALUES (?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                int(job.get("updated_at") or _now_ms()),
                event_type,
                previous_status,
                new_status,
                json.dumps(_chat_job_event_metadata(job), ensure_ascii=False, sort_keys=True),
            ),
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
                _append_chat_job_transition_events(conn, state, changed_request_ids)
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
    upstream_receipt = original.get("output_integrity")
    if not isinstance(upstream_receipt, dict):
        hygiene = original.get("hygiene")
        if isinstance(hygiene, dict) and isinstance(hygiene.get("output_integrity"), dict):
            upstream_receipt = hygiene["output_integrity"]
    upstream_quarantined = bool(
        original.get("quarantined")
        or (isinstance(upstream_receipt, dict) and upstream_receipt.get("quarantine_required"))
    )
    legacy_contamination = detect_visible_output_contamination(text)
    additional_reasons = (
        ("visible_output_contamination",) if legacy_contamination else ()
    )
    if upstream_quarantined and isinstance(upstream_receipt, dict):
        receipt = dict(upstream_receipt)
    else:
        if upstream_quarantined:
            additional_reasons = tuple(additional_reasons) + ("upstream_quarantine",)
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
                "Generated text was quarantined. Poll get_chat_job with "
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


def _refresh_and_persist_chat_job_health(
    request_id: str,
    *,
    increment_poll: bool = False,
) -> dict[str, Any] | None:
    """Refresh monitoring fields against the latest persisted state.

    Polling previously refreshed only a process-local copy. Persisting from a
    stale caller copy could also overwrite a concurrently terminalized job, so
    this helper always reloads the latest record while holding the job mutex.
    """

    normalized_id = _normalize_request_id(request_id)
    with _CHAT_JOB_STATE_MUTEX:
        state = _load_chat_job_state()
        jobs = state.setdefault("jobs", {})
        current = jobs.get(normalized_id)
        if not isinstance(current, dict):
            return None
        updated = _refresh_chat_job_health(current, increment_poll=increment_poll)
        jobs[normalized_id] = updated
        _save_chat_job_state(state, changed_request_ids={normalized_id})
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
        last_persisted_at = int(job.get("progress_persisted_at") or 0)
        if complete or now - last_persisted_at >= 5_000:
            job["progress_persisted_at"] = now
            jobs[request_id] = job
            _save_chat_job_state(state, changed_request_ids={request_id})


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
    updated["retry_safe"] = False
    updated["reconciliation_required"] = True
    updated["upstream_execution_state"] = "unknown"
    updated = _refresh_chat_job_health(updated)
    _persist_chat_job(updated)
    return updated


def _cancel_chat_job(request_id: str, reason: str = "Cancelled by caller.") -> ChatJobOutput:
    normalized_id = _normalize_request_id(request_id)
    task = _CHAT_JOB_TASKS.get(normalized_id)

    # Reconcile a result that already reached the local conversation store
    # before recording cancellation. This closes the race observed in live
    # AIgentBee jobs where the backend had completed successfully but the MCP
    # tracker was still marked active.
    if task is not None and task.done():
        _finalize_chat_job(normalized_id, task)
        task = _CHAT_JOB_TASKS.get(normalized_id)
    current = _load_chat_job(normalized_id)
    if isinstance(current, dict):
        current_status = str(current.get("status") or "")
        if current_status in {"completed", "indeterminate_output", "error", "cancelled"}:
            return _chat_job_output(normalized_id, increment_poll=False)
        if current_status in {"running", "pending"} and "baseline_message_id" in current:
            turn = _completed_turn_after_checkpoint(
                str(current.get("conversation_id") or ""),
                int(current.get("baseline_message_id") or 0),
            )
            if turn is not None:
                _complete_chat_job_from_local_turn(normalized_id, current, turn)
                return _chat_job_output(normalized_id, increment_poll=False)

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
        pre_cancel = _refresh_chat_job_health(job)
        previous_status = str(pre_cancel.get("status") or "")
        now = _now_ms()
        job["status"] = "cancelled"
        job["updated_at"] = now
        job["error"] = str(reason or "Cancelled by caller.")[:1000]
        job["dead_loop_suspected"] = False
        job["cancel_recommended"] = False
        job["cancel_requested_at"] = now
        job["cancelled_from_status"] = previous_status
        job["stalled_for_seconds_at_cancel"] = float(
            pre_cancel.get("stalled_for_seconds") or 0.0
        )
        job["dead_loop_suspected_at_cancel"] = bool(
            pre_cancel.get("dead_loop_suspected")
        )
        job["cancel_recommended_at_cancel"] = bool(
            pre_cancel.get("cancel_recommended")
        )
        job["last_progress_at_at_cancel"] = int(
            pre_cancel.get("last_progress_at") or 0
        )
        job["retry_safe"] = False
        job["reconciliation_required"] = True
        job["upstream_execution_state"] = "unknown"
        job["cancellation_state"] = (
            "local_task_cancel_requested_upstream_unconfirmed"
            if task is not None and not task.done()
            else "local_tracker_cancelled_upstream_unconfirmed"
        )
        jobs[normalized_id] = job
        _save_chat_job_state(state, changed_request_ids={normalized_id})
    if task is not None and not task.done():
        task.cancel()
    elif task is None:
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
    upstream_hygiene = data.get("hygiene") if isinstance(data.get("hygiene"), dict) else {}
    upstream_integrity = data.get("output_integrity")
    if not isinstance(upstream_integrity, dict):
        upstream_integrity = upstream_hygiene.get("output_integrity")
    upstream_quarantined = bool(
        data.get("quarantined")
        or (isinstance(upstream_integrity, dict) and upstream_integrity.get("quarantine_required"))
    )
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
        **_governance_trace(data),
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
                "get_chat_job."
            )
        ),
        "error": _error_summary(data),
        "response_text": _extract_chat_content(data),
        "output_integrity": upstream_integrity if isinstance(upstream_integrity, dict) else None,
        "hygiene": upstream_hygiene or None,
        "quarantined": upstream_quarantined,
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
    status = str(job.get("status") or "pending")
    reconciliation_required = bool(
        job.get("reconciliation_required") or status in {"stale", "cancelled"}
    )
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
        "status": status,
        "request_id": request_id,
        "job_id": request_id,
        "retry_safe": status in {"running", "pending"} and not reconciliation_required,
        "reconciliation_required": reconciliation_required,
        "cancellation_state": str(job.get("cancellation_state") or ""),
        "upstream_execution_state": str(job.get("upstream_execution_state") or ""),
        "wait_seconds": wait_seconds,
        "poll_hint": (
            f"Call get_chat_job(request_id='{request_id}') or retry the same chat tool with the same request_id."
            if status in {"running", "pending"}
            else (
                "Reconcile this request_id before starting replacement work; the upstream outcome is not confirmed."
                if reconciliation_required
                else "This request id is terminal; use a new request_id for new work."
            )
        ),
        "error": job.get("error") if isinstance(job.get("error"), str) else None,
        "response_text": (
            _job_response_text(job.get("response") if isinstance(job.get("response"), dict) else None)
            if status == "completed"
            else ""
        ),
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
    completed["retry_safe"] = False
    completed["reconciliation_required"] = False
    completed["upstream_execution_state"] = "terminal"
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


def _reconcile_cancelled_chat_job_from_local_turn(
    request_id: str,
    job: dict[str, Any],
    turn: dict[str, Any],
) -> dict[str, Any]:
    """Record a late upstream terminal observation without reviving cancellation.

    A local cancellation is a tracker decision, not proof that Notion stopped.
    If the conversation DB later contains the assistant turn, retain the
    cancellation as terminal while closing the reconciliation gap explicitly.
    """

    response = _chat_output_from_local_turn(job, turn)
    updated = dict(job)
    updated["updated_at"] = _now_ms()
    updated["late_completion_detected"] = True
    updated["late_completion_at"] = updated["updated_at"]
    updated["late_completion_status"] = str(response.get("status") or "")
    updated["late_response_chars"] = len(str(response.get("response_text") or ""))
    updated["late_output_integrity"] = response.get("output_integrity")
    updated["late_quarantined"] = bool(response.get("quarantined"))
    updated["upstream_execution_state"] = "terminal"
    updated["reconciliation_required"] = False
    updated["retry_safe"] = False
    updated["cancellation_state"] = "local_cancelled_upstream_terminal_observed"
    remote_chat_id = str(response.get("remote_chat_id") or "")
    if remote_chat_id:
        updated["remote_chat_id"] = remote_chat_id
        updated["notion_thread_id"] = remote_chat_id
    updated["assistant_message_id"] = int(turn.get("assistant_message_id") or 0)
    _persist_chat_job(updated)
    return updated


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

            # A locally cancelled/stale request can still be executing upstream.
            # Until its outcome is reconciled, it remains a mutation fence for
            # this conversation even though its local status is terminal.
            if bool(raw_job.get("reconciliation_required")):
                other_id = str(other_request_id)
                return (
                    "stale_conflict",
                    dict(raw_job),
                    _CHAT_JOB_TASKS.get(other_id),
                    other_id,
                )
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
            with contextlib.suppress(asyncio.CancelledError):
                await stream_task


async def _chat_job_watchdog_loop() -> None:
    """Reconcile and persist job health even when no client is polling.

    This is intentionally a monitor, not an auto-canceller. A stalled request
    can have an indeterminate upstream side effect, so replacement work remains
    blocked until an operator/caller explicitly reconciles or cancels it.
    """

    while True:
        await asyncio.sleep(_configured_chat_job_watchdog_seconds())
        try:
            state = _load_chat_job_state()
            jobs = state.get("jobs", {}) if isinstance(state, dict) else {}
            request_ids = [
                str(request_id)
                for request_id, raw_job in list(jobs.items())
                if isinstance(raw_job, dict)
                and (
                    str(raw_job.get("status") or "") in {"running", "pending"}
                    or (
                        str(raw_job.get("status") or "") == "cancelled"
                        and not bool(raw_job.get("late_completion_detected"))
                    )
                )
            ]
            for request_id in request_ids:
                try:
                    _chat_job_output(request_id, increment_poll=False)
                except Exception:
                    logger.exception(
                        "Chat job watchdog reconciliation failed",
                        extra={
                            "request_info": {
                                "event": "chat_job_watchdog_reconciliation_failed",
                                "request_id": request_id,
                            }
                        },
                    )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception(
                "Chat job watchdog iteration failed",
                extra={"request_info": {"event": "chat_job_watchdog_iteration_failed"}},
            )


def _ensure_chat_job_watchdog() -> asyncio.Task[None]:
    global _CHAT_JOB_WATCHDOG_TASK
    task = _CHAT_JOB_WATCHDOG_TASK
    if task is None or task.done():
        _CHAT_JOB_WATCHDOG_TASK = asyncio.create_task(
            _chat_job_watchdog_loop(),
            name="notion2api-chat-job-watchdog",
        )
    return _CHAT_JOB_WATCHDOG_TASK


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
        if status in {"completed", "indeterminate_output", "error"}:
            job["retry_safe"] = False
            job["reconciliation_required"] = False
            job["upstream_execution_state"] = "terminal"
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
    _ensure_chat_job_watchdog()
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
            if response and status != "cancelled":
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
        route_resolution = get_model_route_resolution(model)
        resolved_model = str(route_resolution.get("resolved_model") or model)
        alias_resolution = None
        route_disposition = "direct_route"
        if route_resolution.get("resolution_kind") == "configured_alias":
            alias_resolution = {
                key: route_resolution[key]
                for key in (
                    "requested_model",
                    "canonical_model",
                    "resolved_model",
                    "public_model",
                    "display_name",
                    "resolution_kind",
                )
            }
            route_disposition = "alias_resolution"
        prompt_text = _prompt_text_from_messages(
            payload.get("messages") if isinstance(payload.get("messages"), list) else []
        )
        if not prompt_text:
            prompt_text = str(payload.get("prompt") or payload.get("input") or "")
        if len(prompt_text) > MAX_CHAT_JOB_PROMPT_CHARS:
            prompt_text = prompt_text[:MAX_CHAT_JOB_PROMPT_CHARS]
        job = {
            "request_id": normalized_id,
            "job_id": normalized_id,
            "status": "running",
            "endpoint": path,
            "model": model,
            "requested_model": model,
            "resolved_model": resolved_model,
            "alias_resolution": alias_resolution,
            "model_route_disposition": route_disposition,
            "prompt": prompt_text,
            "caller": caller,
            "session_name": session_key,
            "conversation_id": conversation_id,
            "session_created": session_created,
            "created_at": now,
            "updated_at": now,
            "last_progress_at": now,
            "poll_count": 0,
            "retry_safe": True,
            "reconciliation_required": False,
            "upstream_execution_state": "active",
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
        str(job.get("status") or "") == "cancelled"
        and "baseline_message_id" in job
        and not bool(job.get("late_completion_detected"))
    ):
        turn = _completed_turn_after_checkpoint(
            str(job.get("conversation_id") or ""),
            int(job.get("baseline_message_id") or 0),
        )
        if turn is not None:
            job = _reconcile_cancelled_chat_job_from_local_turn(
                normalized_id,
                job,
                turn,
            )

    if (
        str(job.get("status") or "") in {"running", "pending"}
        and normalized_id not in _CHAT_JOB_TASKS
    ):
        job = _mark_chat_job_stale(job)

    persisted_health = _refresh_and_persist_chat_job_health(
        normalized_id,
        increment_poll=increment_poll,
    )
    if persisted_health is not None:
        job = persisted_health
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

    requested_model = str(job.get("requested_model") or job.get("model") or "")
    resolved_model = str(job.get("resolved_model") or "")
    alias_resolution = (
        dict(job["alias_resolution"])
        if isinstance(job.get("alias_resolution"), dict)
        else None
    )
    route_disposition = str(job.get("model_route_disposition") or "").strip()
    if not resolved_model or not route_disposition:
        identity = _model_identity_trace(
            {
                "model_metadata": (
                    dict(job["model_metadata"])
                    if isinstance(job.get("model_metadata"), dict)
                    else {}
                )
            },
            requested_model,
        )
        resolved_model = resolved_model or str(identity.get("resolved_model") or "")
        alias_resolution = alias_resolution or (
            dict(identity["alias_resolution"])
            if isinstance(identity.get("alias_resolution"), dict)
            else None
        )
        route_disposition = route_disposition or str(
            identity.get("model_route_disposition") or "direct_route"
        )
    if alias_resolution and not route_disposition:
        route_disposition = "alias_resolution"
    if not route_disposition:
        route_disposition = "direct_route"

    status = str(job.get("status") or "")
    reconciliation_required = bool(
        job.get("reconciliation_required")
        or status == "stale"
        or (
            status == "cancelled"
            and not bool(job.get("late_completion_detected"))
        )
    )
    retry_safe = bool(job.get("retry_safe")) if "retry_safe" in job else status in {
        "running",
        "pending",
    }

    return ChatJobOutput(
        ok=True,
        found=True,
        status=status,
        request_id=normalized_id,
        job_id=str(job.get("job_id") or normalized_id),
        session_name=str(job.get("session_name") or ""),
        conversation_id=str(job.get("conversation_id") or ""),
        model=str(job.get("model") or ""),
        requested_model=requested_model,
        resolved_model=resolved_model,
        alias_resolution=alias_resolution,
        model_route_disposition=route_disposition,
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
        authoritative=(status == "completed" and not quarantined),
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
        retry_safe=retry_safe and not reconciliation_required,
        reconciliation_required=reconciliation_required,
        cancellation_state=str(job.get("cancellation_state") or ""),
        upstream_execution_state=str(job.get("upstream_execution_state") or ""),
        cancel_requested_at=int(job.get("cancel_requested_at") or 0),
        cancelled_from_status=str(job.get("cancelled_from_status") or ""),
        stalled_for_seconds_at_cancel=float(
            job.get("stalled_for_seconds_at_cancel") or 0.0
        ),
        dead_loop_suspected_at_cancel=bool(
            job.get("dead_loop_suspected_at_cancel")
        ),
        late_completion_detected=bool(job.get("late_completion_detected")),
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
    *,
    strict: bool = False,
) -> bool:
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
            return True
        except Exception:
            if strict:
                raise
            # Session continuity is helpful but should not break model calls.
            return False


def _load_session_state(path: Path = DEFAULT_SESSION_STATE_PATH) -> dict[str, str]:
    return {
        name: str(record.get("conversation_id") or "")
        for name, record in _load_session_records(path).items()
        if str(record.get("conversation_id") or "").strip()
    }


def _session_archive_path(path: Path = DEFAULT_SESSION_STATE_PATH) -> Path:
    return path.with_name(f"{path.stem}.archive.jsonl")


def _session_job_bindings() -> tuple[set[str], set[str]]:
    protected_names: set[str] = set()
    protected_conversations: set[str] = set()
    state = _load_chat_job_state()
    jobs = state.get("jobs", {}) if isinstance(state, dict) else {}
    if not isinstance(jobs, dict):
        return protected_names, protected_conversations
    for raw_job in jobs.values():
        if not isinstance(raw_job, dict):
            continue
        session_name = str(raw_job.get("session_name") or "").strip()
        conversation_id = str(raw_job.get("conversation_id") or "").strip()
        if session_name:
            protected_names.add(_session_key(session_name))
        if conversation_id:
            protected_conversations.add(conversation_id)
    return protected_names, protected_conversations


def _build_session_retention_plan(
    records: dict[str, dict[str, Any]],
    *,
    retention_days: int | None = None,
    max_records: int | None = None,
    now_ms: int | None = None,
) -> dict[str, Any]:
    protected_names, protected_conversations = _session_job_bindings()
    return build_session_retention_plan(
        records,
        protected_session_names=protected_names,
        protected_conversation_ids=protected_conversations,
        retention_days=retention_days,
        max_records=max_records,
        now_ms=now_ms,
    )


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
    install_mcp_noise_filter()
    client = Notion2APIClient(base_url=base_url, api_key=api_key, timeout=timeout)
    transport_security = _transport_security_settings(host=host)
    server_name = os.getenv("MCP_SERVER_NAME", "notion2api").strip() or "notion2api"
    tool_namespace = os.getenv("SANITYCLOUD_TOOL_NAMESPACE", "").strip()
    invocation_alias = os.getenv("SANITYCLOUD_INVOCATION_ALIAS", "").strip()
    tool_prefix = os.getenv("MCP_TOOL_PREFIX", "").strip().lower().strip("_")

    def _tool_name(internal_name: str) -> str:
        suffix = internal_name.removeprefix("notion2api_")
        return f"{tool_prefix}_{suffix}" if tool_prefix else suffix

    def _tool_description(description: str) -> str:
        replacement = f"{tool_prefix}_" if tool_prefix else ""
        return description.replace("Notion2API", server_name).replace("notion2api_", replacement)
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
            + f"Start with {_tool_name('notion2api_health')} or {_tool_name('notion2api_list_models')} "
            "if service status or model IDs are uncertain. "
            "Omit session_name to generate a descriptive unique session for new work; legacy 'op' values are also auto-generated. "
            + f"Chat submissions return immediately. Poll {_tool_name('notion2api_get_chat_job')} and report its progress snapshot "
            "without exposing raw private reasoning. "
            + f"For ChatGPT uploads, stage one top-level file at a time with {_tool_name('notion2api_stage_file')}; "
            "never pass /mnt/data paths through attachments. "
            + f"Before filesystem discovery, filename search, enumeration, or path resolution, call {_tool_name('notion2api_hive_route_file_operation')}. "
            "Use Everything_MCP for discovery; use DesktopCommander only for known-file access, process inspection, or an explicitly authorized degraded fallback. "
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

    if tool_prefix == "aigentbee":
        widget_meta = {
            "ui": {"resourceUri": SWARM_WIDGET_URI},
            "openai/outputTemplate": SWARM_WIDGET_URI,
            "openai/toolInvocation/invoking": "Loading the AIgentBee Swarm Workbench.",
            "openai/toolInvocation/invoked": "AIgentBee Swarm Workbench ready",
        }
        widget_access_meta = {"openai/widgetAccessible": True}

        @server.resource(
            SWARM_WIDGET_URI,
            name="aigentbee-swarm-workbench",
            title="AIgentBee Swarm Workbench",
            description=(
                "A governance- and plan-governed view of one AIgentBee Hive mission, its swarm members, "
                "leader Notion chat history, and bounded leader-routed requests."
            ),
            mime_type="text/html;profile=mcp-app",
            meta={
                "ui": {
                    "csp": {"connectDomains": [], "resourceDomains": []},
                    "prefersBorder": True,
                },
                "openai/widgetDescription": (
                    "AIgentBee Swarm Workbench for mission evidence, member roles and tasks, "
                    "leader chat history, and bounded requests to the accountable leader."
                ),
                "openai/widgetPrefersBorder": True,
            },
        )
        def aigentbee_swarm_workbench_resource() -> str:
            return load_swarm_widget_html()

        def _swarm_workbench_output(
            mission_id: str,
            history_limit: int = 30,
        ) -> SwarmWorkbenchOutput:
            try:
                bounded_limit = max(1, min(int(history_limit), MAX_HISTORY_LIMIT))
                snapshot = get_hive_runtime_store().get_mission(
                    mission_id,
                    event_limit=50,
                    action_limit=50,
                )
                session_name = leader_session_name(mission_id)
                session_record = dict(
                    _load_session_records().get(_session_key(session_name)) or {}
                )
                conversation_id = str(session_record.get("conversation_id") or "").strip()
                history: MessagesOutput | dict[str, Any]
                if conversation_id:
                    history = _read_local_messages(
                        session_name=session_name,
                        conversation_id=conversation_id,
                        limit=bounded_limit,
                    )
                else:
                    history = {}
                return build_swarm_workbench(
                    snapshot,
                    session_record=session_record,
                    messages_output=history,
                    history_limit=bounded_limit,
                )
            except (HiveRuntimeError, ValueError, TypeError) as exc:
                return SwarmWorkbenchOutput(
                    ok=False,
                    generated_at=_now_ms(),
                    error=str(exc),
                    governance={
                        "authorityCeiling": "observe_only",
                        "directWorkerExecution": False,
                        "arbitraryShellExecution": False,
                        "leaderRoutingAvailable": False,
                    },
                )

        @server.tool(
            name=_tool_name("notion2api_show_swarm_workbench"),
            description=_tool_description(
                "Use this when the user wants a visual AIgentBee Hive mission view with swarm members, "
                "roles, tasks, status, leader chat history, and governance boundaries."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            meta={**widget_meta, **widget_access_meta},
            structured_output=True,
        )
        async def notion2api_show_swarm_workbench(
            mission_id: str,
            history_limit: int = 30,
        ) -> SwarmWorkbenchOutput:
            return _swarm_workbench_output(mission_id, history_limit)

        @server.tool(
            name=_tool_name("notion2api_get_swarm_workbench"),
            description=_tool_description(
                "Use this to read one AIgentBee Hive mission, swarm-member role and task state, "
                "leader session metadata, recent Hive events, and bounded leader chat history without opening the widget."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=True,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            meta=widget_access_meta,
            structured_output=True,
        )
        async def notion2api_get_swarm_workbench(
            mission_id: str,
            history_limit: int = 30,
        ) -> SwarmWorkbenchOutput:
            return _swarm_workbench_output(mission_id, history_limit)

        @server.tool(
            name=_tool_name("notion2api_send_leader_request"),
            description=_tool_description(
                "Use this to submit one bounded instruction, question, review, status check, or priority proposal "
                "for a named Hive worker lane to the mission's accountable AIgentBee leader Notion chat. "
                "This does not directly command a worker or execute a shell action."
            ),
            annotations=ToolAnnotations(
                readOnlyHint=False,
                destructiveHint=False,
                idempotentHint=True,
                openWorldHint=False,
            ),
            meta=widget_access_meta,
            structured_output=True,
        )
        async def notion2api_send_leader_request(
            mission_id: str,
            member_id: str,
            request: str,
            idempotency_key: str,
            request_type: str = "instruction",
            requested_by: str = "ChatGPT user",
        ) -> LeaderRequestReceipt:
            submitted_at = _now_ms()
            session_name = ""
            member_name = ""
            member_role = ""
            lane_title = ""
            mission_revision = 0
            request_fingerprint = ""
            normalized_request_id = ""
            try:
                if not str(idempotency_key or "").strip():
                    raise ValueError("idempotency_key is required")
                normalized_request_id = _normalize_request_id(idempotency_key)
                snapshot = get_hive_runtime_store().get_mission(
                    mission_id,
                    event_limit=50,
                    action_limit=50,
                )
                if not snapshot.ok or not snapshot.found:
                    raise ValueError(snapshot.error or "Hive mission was not found.")
                if str(snapshot.status).upper() in {"CLOSED", "CANCELLED"}:
                    raise ValueError(
                        f"Mission status {snapshot.status} does not accept new leader requests."
                    )
                member = next(
                    (unit for unit in snapshot.work_units if unit.work_unit_id == member_id),
                    None,
                )
                if member is None:
                    raise ValueError(
                        "The selected swarm member does not exist in the current mission."
                    )
                clean_request, clean_type, clean_requester = validate_leader_request(
                    request,
                    request_type,
                    requested_by,
                )
                validate_prerequisite_progression(
                    snapshot,
                    clean_request,
                    clean_type,
                )
                member_name = member.title
                member_role = member.role
                lane_title = member.title
                session_name = leader_session_name(mission_id)

                existing_job = _load_chat_job(normalized_request_id)
                existing_caller = (
                    existing_job.get("caller")
                    if isinstance(existing_job, dict)
                    and isinstance(existing_job.get("caller"), dict)
                    else {}
                )
                existing_intent = next(
                    (
                        event
                        for event in snapshot.events
                        if event.event_type == "LEADER_REQUEST_INTENT"
                        and str((event.payload or {}).get("request_id") or "")
                        == normalized_request_id
                    ),
                    None,
                )
                existing_intent_payload = (
                    dict(existing_intent.payload or {}) if existing_intent else {}
                )
                mission_revision = int(
                    existing_caller.get("mission_revision")
                    or existing_intent_payload.get("mission_revision_at_submission")
                    or snapshot.revision
                )
                prompt, member_name = build_leader_prompt(
                    snapshot,
                    member_id,
                    clean_request,
                    clean_type,
                    clean_requester,
                    mission_revision_at_submission=mission_revision,
                )
                request_identity = {
                    "mission_id": mission_id,
                    "mission_revision_at_submission": mission_revision,
                    "member_id": member_id,
                    "member_name": member_name,
                    "member_role": member_role,
                    "lane_title": lane_title,
                    "request_type": clean_type,
                    "request_text": clean_request,
                    "leader_session": session_name,
                }
                request_fingerprint = hashlib.sha256(
                    json.dumps(
                        request_identity,
                        ensure_ascii=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest()
                existing_fingerprint = str(
                    existing_caller.get("request_fingerprint")
                    or existing_intent_payload.get("request_fingerprint")
                    or ""
                )
                if (existing_job or existing_intent) and (
                    existing_fingerprint != request_fingerprint
                ):
                    raise ValueError(
                        "idempotency_key is already bound to a different leader request"
                    )

                hive_store = get_hive_runtime_store()
                intent_snapshot = hive_store.append_event(
                    mission_id=mission_id,
                    event_type="LEADER_REQUEST_INTENT",
                    sender="aigentbee-swarm-workbench",
                    recipient="leader",
                    work_unit_id=member_id,
                    payload={
                        "request_id": normalized_request_id,
                        "request_fingerprint": request_fingerprint,
                        "request_type": clean_type,
                        "member_name": member_name,
                        "member_role": member_role,
                        "lane_title": lane_title,
                        "mission_revision_at_submission": mission_revision,
                        "requested_by_display": clean_requester,
                        "request_preview": clean_request[:240],
                        "leader_session": session_name,
                    },
                    context_version=mission_revision,
                    idempotency_key=f"leader-request-intent:{normalized_request_id}",
                )

                deduplicated = bool(existing_job)

                conversation_id, session_key, session_created = _conversation_id_for_session(
                    session_name,
                    conversation_id=leader_conversation_id(mission_id),
                )
                payload = {
                    "model": DEFAULT_MODEL,
                    "messages": _explicit_prompt_messages(prompt, None),
                    "stream": False,
                    "conversation_id": conversation_id,
                    "session_name": session_key,
                    "notion_mode": "default",
                    "notion_task": None,
                    "notion_sources": ["notion"],
                    "web_access": False,
                    "notion_persona": "analyst",
                    "notion_instructions": (
                        "Act as the accountable AIgentBee leader for the named Hive mission. "
                        "Evaluate the request against current mission evidence and authority. "
                        "Accept, revise, defer, or reject it; route accepted work through the existing Hive lane "
                        "and record later execution as separate evidence. Never treat the request itself as proof of execution."
                    ),
                    "metadata": {
                        "persist_remote_chat": True,
                        "mcp_session_name": session_key,
                        "caller": {
                            "id": "aigentbee-swarm-workbench",
                            "type": "widget",
                            "mission_id": mission_id,
                            "mission_revision": mission_revision,
                            "work_unit_id": member_id,
                            "member_role": member_role,
                            "lane_title": lane_title,
                            "request_type": clean_type,
                            "request_fingerprint": request_fingerprint,
                        },
                    },
                }
                result = await _submit_or_resume_chat_job(
                    client=client,
                    path="/v1/chat/completions",
                    payload=payload,
                    model=DEFAULT_MODEL,
                    session_key=session_key,
                    conversation_id=conversation_id,
                    session_created=session_created,
                    request_id=normalized_request_id,
                    wait_seconds=None,
                )
                result_data = dict(result or {})
                result_request_id = str(
                    result_data.get("request_id") or normalized_request_id
                )
                status = str(result_data.get("status") or "").lower()
                accepted = bool(
                    result_request_id
                    and status
                    in {
                        "pending",
                        "running",
                        "stale",
                        "completed",
                        "indeterminate_output",
                    }
                )
                if not accepted:
                    request_status = "failed_to_submit"
                elif deduplicated:
                    request_status = "deduplicated"
                elif status in {"pending", "running", "stale"}:
                    request_status = "queued"
                elif status == "indeterminate_output":
                    request_status = "indeterminate_reconciliation_required"
                else:
                    request_status = status or "accepted"

                ledger_recorded = False
                if accepted:
                    try:
                        hive_store.append_event(
                            mission_id=mission_id,
                            event_type="LEADER_REQUEST_SUBMITTED",
                            sender="aigentbee-swarm-workbench",
                            recipient="leader",
                            work_unit_id=member_id,
                            payload={
                                "request_id": result_request_id,
                                "request_fingerprint": request_fingerprint,
                                "request_status": request_status,
                                "request_type": clean_type,
                                "member_name": member_name,
                                "member_role": member_role,
                                "lane_title": lane_title,
                                "mission_revision_at_submission": mission_revision,
                                "requested_by_display": clean_requester,
                                "request_preview": clean_request[:240],
                                "leader_session": session_key,
                            },
                            context_version=int(intent_snapshot.revision),
                            idempotency_key=f"leader-request-submitted:{result_request_id}",
                        )
                        ledger_recorded = True
                    except (HiveRuntimeError, ValueError):
                        ledger_recorded = False

                return LeaderRequestReceipt(
                    ok=accepted,
                    accepted=accepted,
                    mission_id=mission_id,
                    member_id=member_id,
                    member_name=member_name,
                    member_role=member_role,
                    lane_title=lane_title,
                    mission_revision=mission_revision,
                    request_type=clean_type,
                    session_name=session_key,
                    conversation_id=conversation_id,
                    request_id=result_request_id,
                    job_id=str(result_data.get("job_id") or result_request_id),
                    status=status,
                    request_status=request_status,
                    deduplicated=deduplicated,
                    request_fingerprint=request_fingerprint,
                    submitted_at=submitted_at,
                    ledger_recorded=ledger_recorded,
                    request_preview=clean_request[:240],
                    error=str(result_data.get("error") or ""),
                )
            except (HiveRuntimeError, ValueError, TypeError) as exc:
                return LeaderRequestReceipt(
                    ok=False,
                    accepted=False,
                    mission_id=mission_id,
                    member_id=member_id,
                    member_name=member_name,
                    member_role=member_role,
                    lane_title=lane_title,
                    mission_revision=mission_revision,
                    request_type=request_type,
                    session_name=session_name,
                    request_id=normalized_request_id,
                    request_status="rejected",
                    request_fingerprint=request_fingerprint,
                    submitted_at=submitted_at,
                    error=str(exc),
                )


    @server.tool(name=_tool_name("notion2api_health"), description=_tool_description("Check whether the configured Notion2API backend is reachable and healthy."), structured_output=True)
    async def notion2api_health() -> HealthOutput:
        data = await client.get("/health")
        mcp_runtime = mcp_observability_snapshot()
        raw = dict(data)
        raw["mcp_runtime"] = mcp_runtime
        return HealthOutput(
            ok=bool(data.get("ok", False)),
            status_code=data.get("status_code"),
            status=data.get("status"),
            accounts=data.get("accounts"),
            accounts_total=data.get("accounts_total"),
            accounts_cooling=data.get("accounts_cooling"),
            uptime=data.get("uptime"),
            account_selection=dict(data.get("account_selection") or {}),
            governance=dict(data.get("governance") or {}),
            notion_admission=dict(data.get("notion_admission") or {}),
            conversation_compression=dict(
                data.get("conversation_compression") or {}
            ),
            mcp_runtime=mcp_runtime,
            raw=raw,
        )

    @server.tool(
        name=_tool_name("notion2api_list_accounts"),
        description=_tool_description(
            "List configured Notion account profiles and show whether Notion2API is in automatic rotation or pinned mode. Raw credentials are never returned."
        ),
        structured_output=True,
    )
    async def notion2api_list_accounts() -> dict[str, Any]:
        return await client.get("/v1/notion/accounts")

    @server.tool(
        name=_tool_name("notion2api_switch_workspace"),
        description=_tool_description(
            "Select one Notion workspace explicitly. Account rotation remains automatic "
            "inside that workspace. Existing persistent chats retain their original "
            "workspace/account binding and are not migrated."
        ),
        structured_output=True,
    )
    async def notion2api_switch_workspace(selector: str) -> dict[str, Any]:
        return await client.post(
            "/v1/notion/workspaces/switch",
            {"selector": selector},
        )

    @server.tool(
        name=_tool_name("notion2api_switch_account"),
        description=_tool_description(
            "Switch Notion2API to a named account profile. Use mode='pinned' with a profile name, email, user id, or account number; use mode='auto' to restore rotation and failover. Start a new remote chat after changing accounts because existing thread bindings are not migrated."
        ),
        structured_output=True,
    )
    async def notion2api_switch_account(
        selector: str | None = None,
        mode: Literal["pinned", "auto"] = "pinned",
    ) -> dict[str, Any]:
        return await client.post(
            "/v1/notion/accounts/switch",
            {"selector": selector, "mode": mode},
        )

    @server.tool(
        name=_tool_name("notion2api_rollback_account_switch"),
        description=_tool_description(
            "Roll back the most recent Notion2API account-selection change. This affects new requests only; existing remote thread bindings are unchanged."
        ),
        structured_output=True,
    )
    async def notion2api_rollback_account_switch() -> dict[str, Any]:
        return await client.post("/v1/notion/accounts/rollback", {})

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
        workspace_id: str,
        user_id: str,
        work_units: list[dict[str, Any]] | None = None,
        authority_ceiling: str = "A2",
        parent_context_id: str = "",
        mission_id: str | None = None,
        idempotency_key: str | None = None,
        actor: str = "notion2api",
        account_key: str = "",
        profile_name: str = "",
        account_profile: str = "",
        account_selector: str = "",
        project_contract: dict[str, Any] | None = None,
    ) -> HiveMissionSnapshot:
        try:
            from app.conversation import ConversationManager
            from app.hive_lane_scope import ensure_mission_lane_conversation_scopes

            specs = [HiveWorkUnitSpec.model_validate(item) for item in (work_units or [])]
            snapshot = get_hive_runtime_store().create_mission(
                title=title,
                objective=objective,
                lifecycle_stage=lifecycle_stage,
                work_units=specs,
                authority_ceiling=authority_ceiling,
                parent_context_id=parent_context_id,
                mission_id=mission_id,
                idempotency_key=idempotency_key,
                actor=actor,
                account_key=account_key,
                workspace_id=workspace_id,
                user_id=user_id,
                profile_name=profile_name,
                account_profile=account_profile,
                account_selector=account_selector,
                project_contract=(
                    HiveProjectContract.model_validate(project_contract)
                    if project_contract is not None
                    else None
                ),
            )
            ensure_mission_lane_conversation_scopes(
                ConversationManager(),
                mission_id=snapshot.mission_id,
                account_key=snapshot.account_key,
                workspace_id=snapshot.workspace_id,
                user_id=snapshot.user_id,
                profile_name=snapshot.profile_name,
                work_unit_conversation_ids=[
                    item.conversation_id for item in snapshot.work_units
                ],
            )
            return snapshot
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

    @server.tool(
        name=_tool_name("notion2api_hive_delegate_tasks"),
        description=_tool_description(
            "Create a validated, lane-local delegated-task DAG with bounded authority, "
            "sources, writable domains, and durable task events."
        ),
        structured_output=True,
    )
    async def notion2api_hive_delegate_tasks(
        mission_id: str,
        tasks: list[dict[str, Any]],
        actor: str = "notion2api",
        expected_mission_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        try:
            return get_hive_runtime_store().delegate_tasks(
                mission_id=mission_id,
                tasks=[HiveDelegatedTaskSpec.model_validate(item) for item in tasks],
                actor=actor,
                expected_mission_revision=expected_mission_revision,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _hive_error_snapshot(exc, mission_id)

    @server.tool(
        name=_tool_name("notion2api_hive_transition_task"),
        description=_tool_description(
            "Accept, lease, execute, block, hand off, or terminalize one delegated "
            "task with dependency and writable-domain enforcement."
        ),
        structured_output=True,
    )
    async def notion2api_hive_transition_task(
        mission_id: str,
        task_id: str,
        status: str,
        actor: str,
        worker_binding: str = "",
        lease_seconds: int = 900,
        evidence: list[dict[str, Any]] | None = None,
        handoff_receipt: dict[str, Any] | None = None,
        expected_mission_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        try:
            return get_hive_runtime_store().transition_delegated_task(
                mission_id=mission_id,
                task_id=task_id,
                status=status,
                actor=actor,
                worker_binding=worker_binding,
                lease_seconds=lease_seconds,
                evidence=evidence,
                handoff_receipt=(
                    HiveHandoffReceipt.model_validate(handoff_receipt)
                    if handoff_receipt is not None
                    else None
                ),
                expected_mission_revision=expected_mission_revision,
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

    def _workforce_error_snapshot(exc: Exception) -> WorkforceSnapshot:
        return WorkforceSnapshot(
            ok=False,
            db_path=str(default_hive_runtime_db_path()),
            error=str(exc),
        )

    def _invocation_error_plan(exc: Exception, objective: str = "") -> HiveInvocationPlan:
        return HiveInvocationPlan(
            ok=False,
            db_path=str(default_hive_runtime_db_path()),
            objective=objective,
            error=str(exc),
        )

    @server.tool(name=_tool_name("notion2api_hive_register_worker"), description=_tool_description("Create a durable governed worker requisition. New workers begin in REQUISITIONED state and receive no execution authority."), structured_output=True)
    async def notion2api_hive_register_worker(
        display_name: str,
        worker_class: str,
        role: str,
        accountable_owner: str,
        competencies: list[str] | None = None,
        writable_domains: list[str] | None = None,
        authority_ceiling: str = "A0",
        source_boundary: str = "",
        appointment_scope: str = "",
        worker_id: str | None = None,
        actor: str = "notion2api",
        idempotency_key: str | None = None,
    ) -> WorkforceSnapshot:
        try:
            return get_hive_workforce_store().register_worker(
                display_name=display_name,
                worker_class=worker_class,
                role=role,
                accountable_owner=accountable_owner,
                competencies=competencies,
                writable_domains=writable_domains,
                authority_ceiling=authority_ceiling,
                source_boundary=source_boundary,
                appointment_scope=appointment_scope,
                worker_id=worker_id,
                actor=actor,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _workforce_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_transition_worker"), description=_tool_description("Move a governed worker through shadow, probation, appointment, suspension, rejection, or offboarding. Probation and appointment require a governance-plan authorization receipt; the legacy human_approval flag is accepted only for compatibility."), structured_output=True)
    async def notion2api_hive_transition_worker(
        worker_id: str,
        target_stage: str,
        actor: str,
        reason: str,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        expected_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> WorkforceSnapshot:
        try:
            return get_hive_workforce_store().transition_worker(
                worker_id=worker_id,
                target_stage=target_stage,
                actor=actor,
                reason=reason,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                expected_revision=expected_revision,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _workforce_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_list_workers"), description=_tool_description("List durable AIgentBee worker requisitions and appointments, optionally filtered by lifecycle stage or worker class."), structured_output=True)
    async def notion2api_hive_list_workers(
        stage: str | None = None,
        worker_class: str | None = None,
        limit: int = 100,
    ) -> WorkforceSnapshot:
        try:
            return get_hive_workforce_store().list_workers(
                stage=stage,
                worker_class=worker_class,
                limit=limit,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _workforce_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_route_file_operation"), description=_tool_description("Route filesystem discovery, path resolution, known-file access, and bounded content search through the canonical AIgentBee policy. Discovery uses Everything_MCP; DesktopCommander search is denied unless an explicit governed degraded-mode gate is satisfied."), structured_output=True)
    async def notion2api_hive_route_file_operation(
        intent: str,
        search_text: str = "",
        requested_roots: list[str] | None = None,
        requested_extensions: list[str] | None = None,
        everything_available: bool = True,
        degraded_mode_authorized: bool = False,
        authority_ceiling: str = "A0",
    ) -> FileRoutingDecision:
        try:
            return route_file_operation(
                intent=intent,
                search_text=search_text,
                requested_roots=requested_roots,
                requested_extensions=requested_extensions,
                everything_available=everything_available,
                degraded_mode_authorized=degraded_mode_authorized,
                authority_ceiling=authority_ceiling,
            )
        except ValueError as exc:
            return FileRoutingDecision(
                ok=False,
                intent=str(intent or "").strip().lower(),
                allowed=False,
                error=str(exc),
                reasons=["The requested filesystem route violates the configured coverage profile."],
            )

    @server.tool(name=_tool_name("notion2api_hive_plan_invocation"), description=_tool_description("Plan whether a request should use one bounded agent or a multi-lane Hive. This tool is read-only and does not spawn workers, grant credentials, or execute external effects."), structured_output=True)
    async def notion2api_hive_plan_invocation(
        objective: str,
        required_competencies: list[str] | None = None,
        writable_domains: list[str] | None = None,
        dependency_count: int = 0,
        parallelizable_workstreams: int = 1,
        risk_level: str = "low",
        authority_ceiling: str = "A0",
        independent_review_required: bool = False,
        external_effects: bool = False,
        preferred_worker_ids: list[str] | None = None,
        file_operation_intent: str = "discover",
        file_search_text: str = "",
        file_search_roots: list[str] | None = None,
        file_types: list[str] | None = None,
        everything_available: bool = True,
        degraded_search_authorized: bool = False,
    ) -> HiveInvocationPlan:
        try:
            return get_hive_workforce_store().plan_invocation(
                objective=objective,
                required_competencies=required_competencies,
                writable_domains=writable_domains,
                dependency_count=dependency_count,
                parallelizable_workstreams=parallelizable_workstreams,
                risk_level=risk_level,
                authority_ceiling=authority_ceiling,
                independent_review_required=independent_review_required,
                external_effects=external_effects,
                preferred_worker_ids=preferred_worker_ids,
                file_operation_intent=file_operation_intent,
                file_search_text=file_search_text,
                file_search_roots=file_search_roots,
                file_types=file_types,
                everything_available=everything_available,
                degraded_search_authorized=degraded_search_authorized,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _invocation_error_plan(exc, objective)
    def _materialization_error_snapshot(
        exc: Exception,
        plan_id: str = "",
        mission_id: str = "",
    ) -> HiveMaterializationSnapshot:
        return HiveMaterializationSnapshot(
            ok=False,
            found=False,
            db_path=str(default_hive_runtime_db_path()),
            plan_id=plan_id,
            mission_id=mission_id,
            error=str(exc),
        )

    @server.tool(name=_tool_name("notion2api_hive_materialize_invocation"), description=_tool_description("Persist an invocation plan and, when coverage and governance-plan authorization gates pass, create a durable Hive mission with appointed-worker bindings, bounded leases, conversation bindings, and READY dispatch receipts."), structured_output=True)
    async def notion2api_hive_materialize_invocation(
        objective: str,
        workspace_id: str,
        user_id: str,
        required_competencies: list[str] | None = None,
        writable_domains: list[str] | None = None,
        dependency_count: int = 0,
        parallelizable_workstreams: int = 1,
        risk_level: str = "low",
        authority_ceiling: str = "A0",
        independent_review_required: bool = False,
        external_effects: bool = False,
        preferred_worker_ids: list[str] | None = None,
        file_operation_intent: str = "discover",
        file_search_text: str = "",
        file_search_roots: list[str] | None = None,
        file_types: list[str] | None = None,
        everything_available: bool = True,
        degraded_search_authorized: bool = False,
        recruitment_mode: str = RecruitmentMode.DISABLED.value,
        lease_ttl_seconds: int = 24 * 60 * 60,
        parent_context_id: str = "",
        lifecycle_stage: str = "Build",
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        actor: str = "notion2api",
        plan_id: str | None = None,
        mission_id: str | None = None,
        idempotency_key: str | None = None,
        account_key: str = "",
        profile_name: str = "",
        account_profile: str = "",
        account_selector: str = "",
    ) -> HiveMaterializationSnapshot:
        try:
            from app.conversation import ConversationManager

            return get_hive_materialization_store().materialize_invocation(
                objective=objective,
                required_competencies=required_competencies,
                writable_domains=writable_domains,
                dependency_count=dependency_count,
                parallelizable_workstreams=parallelizable_workstreams,
                risk_level=risk_level,
                authority_ceiling=authority_ceiling,
                independent_review_required=independent_review_required,
                external_effects=external_effects,
                preferred_worker_ids=preferred_worker_ids,
                file_operation_intent=file_operation_intent,
                file_search_text=file_search_text,
                file_search_roots=file_search_roots,
                file_types=file_types,
                everything_available=everything_available,
                degraded_search_authorized=degraded_search_authorized,
                recruitment_mode=recruitment_mode,
                lease_ttl_seconds=lease_ttl_seconds,
                parent_context_id=parent_context_id,
                lifecycle_stage=lifecycle_stage,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                actor=actor,
                plan_id=plan_id,
                mission_id=mission_id,
                idempotency_key=idempotency_key,
                account_key=account_key,
                workspace_id=workspace_id,
                user_id=user_id,
                profile_name=profile_name,
                account_profile=account_profile,
                account_selector=account_selector,
                conversation_manager=ConversationManager(),
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _materialization_error_snapshot(
                exc,
                plan_id or "",
                mission_id or "",
            )

    @server.tool(name=_tool_name("notion2api_hive_approve_materialization"), description=_tool_description("Approve one AWAITING_APPROVAL invocation plan and materialize it only if the selected workers remain appointed and all authority, domain, competency, and review constraints still pass."), structured_output=True)
    async def notion2api_hive_approve_materialization(
        plan_id: str,
        actor: str,
        reason: str,
        idempotency_key: str | None = None,
    ) -> HiveMaterializationSnapshot:
        try:
            return get_hive_materialization_store().approve_materialization(
                plan_id=plan_id,
                actor=actor,
                reason=reason,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _materialization_error_snapshot(exc, plan_id)

    @server.tool(name=_tool_name("notion2api_hive_get_materialization"), description=_tool_description("Read a durable invocation plan, its materialized mission, worker leases, conversation bindings, and dispatch receipts by plan or mission id."), structured_output=True)
    async def notion2api_hive_get_materialization(
        plan_id: str = "",
        mission_id: str = "",
    ) -> HiveMaterializationSnapshot:
        try:
            return get_hive_materialization_store().get_materialization(
                plan_id=plan_id,
                mission_id=mission_id,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _materialization_error_snapshot(
                exc,
                plan_id,
                mission_id,
            )

    @server.tool(name=_tool_name("notion2api_hive_record_dispatch_receipt"), description=_tool_description("Record ACKNOWLEDGED, COMPLETED, FAILED, or CANCELLED execution evidence for one materialized worker lane. When all lanes become terminal, active leases are released and the plan enters fan-in or failure review."), structured_output=True)
    async def notion2api_hive_record_dispatch_receipt(
        plan_id: str,
        work_unit_id: str,
        status: str,
        actor: str,
        evidence: dict[str, Any] | None = None,
        expected_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMaterializationSnapshot:
        try:
            return get_hive_materialization_store().record_dispatch_receipt(
                plan_id=plan_id,
                work_unit_id=work_unit_id,
                status=status,
                actor=actor,
                evidence=evidence,
                expected_revision=expected_revision,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _materialization_error_snapshot(exc, plan_id)

    @server.tool(name=_tool_name("notion2api_hive_release_materialization_leases"), description=_tool_description("Release or revoke all active worker leases for one materialization plan while preserving its mission, bindings, receipts, and evidence ledger."), structured_output=True)
    async def notion2api_hive_release_materialization_leases(
        plan_id: str,
        actor: str,
        reason: str,
        revoke: bool = False,
        idempotency_key: str | None = None,
    ) -> HiveMaterializationSnapshot:
        try:
            return get_hive_materialization_store().release_leases(
                plan_id=plan_id,
                actor=actor,
                reason=reason,
                revoke=revoke,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _materialization_error_snapshot(exc, plan_id)


    def _lease_reconciliation_error_snapshot(
        exc: Exception,
        *,
        dry_run: bool,
    ) -> LeaseReconciliationSnapshot:
        return LeaseReconciliationSnapshot(
            ok=False,
            db_path=str(default_hive_runtime_db_path()),
            dry_run=dry_run,
            error=str(exc),
        )

    def _workforce_audit_error_snapshot(
        exc: Exception,
        *,
        dry_run: bool,
    ) -> WorkforceAuditSnapshot:
        return WorkforceAuditSnapshot(
            ok=False,
            db_path=str(default_hive_runtime_db_path()),
            dry_run=dry_run,
            error=str(exc),
        )

    @server.tool(
        name=_tool_name("notion2api_hive_heartbeat_worker_lease"),
        description=_tool_description(
            "Record bounded execution-liveness evidence for one ACTIVE worker lease and "
            "renew its expiry without granting additional authority or writable domains."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=False,
            idempotentHint=False,
            openWorldHint=False,
        ),
        structured_output=True,
    )
    async def notion2api_hive_heartbeat_worker_lease(
        lease_id: str,
        actor: str,
        heartbeat_status: str = "RUNNING",
        extend_seconds: int = 60 * 60,
        evidence: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> LeaseReconciliationSnapshot:
        try:
            return get_hive_materialization_store().record_lease_heartbeat(
                lease_id=lease_id,
                actor=actor,
                heartbeat_status=heartbeat_status,
                extend_seconds=extend_seconds,
                evidence=evidence,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _lease_reconciliation_error_snapshot(exc, dry_run=False)

    @server.tool(
        name=_tool_name("notion2api_hive_reconcile_stale_leases"),
        description=_tool_description(
            "Inspect or reconcile objectively stale Hive worker leases using expiry and "
            "heartbeat freshness. Dry-run is the default. Applying expiry is local and "
            "audited; revocation requires governed A2 authorization."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=True,
            openWorldHint=False,
        ),
        structured_output=True,
    )
    async def notion2api_hive_reconcile_stale_leases(
        actor: str,
        plan_id: str = "",
        dry_run: bool = True,
        heartbeat_stale_after_seconds: int = 30 * 60,
        no_heartbeat_grace_seconds: int = 6 * 60 * 60,
        revoke: bool = False,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> LeaseReconciliationSnapshot:
        try:
            return get_hive_materialization_store().reconcile_stale_leases(
                actor=actor,
                plan_id=plan_id,
                dry_run=dry_run,
                heartbeat_stale_after_seconds=heartbeat_stale_after_seconds,
                no_heartbeat_grace_seconds=no_heartbeat_grace_seconds,
                revoke=revoke,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _lease_reconciliation_error_snapshot(exc, dry_run=dry_run)

    @server.tool(
        name=_tool_name("notion2api_hive_audit_workforce"),
        description=_tool_description(
            "Audit Hive requisitions and appointments for placeholders, abandoned "
            "requisitions, stale suspensions, chronic inactivity, and duplicates. "
            "Dry-run is the default; applying offboarding requires governed authorization "
            "and protects leaders and reviewers unless A3 protected-role scope is explicit."
        ),
        annotations=ToolAnnotations(
            readOnlyHint=False,
            destructiveHint=True,
            idempotentHint=False,
            openWorldHint=False,
        ),
        structured_output=True,
    )
    async def notion2api_hive_audit_workforce(
        actor: str,
        dry_run: bool = True,
        stale_after_days: int = 30,
        include_protected: bool = False,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> WorkforceAuditSnapshot:
        try:
            return get_hive_materialization_store().audit_workforce(
                actor=actor,
                dry_run=dry_run,
                stale_after_days=stale_after_days,
                include_protected=include_protected,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _workforce_audit_error_snapshot(exc, dry_run=dry_run)


    def _adapter_error_snapshot(exc: Exception) -> HiveAdapterSnapshot:
        return HiveAdapterSnapshot(
            ok=False,
            db_path=str(default_hive_runtime_db_path()),
            error=str(exc),
        )

    def _execution_error_snapshot(
        exc: Exception,
        execution_id: str = "",
    ) -> HiveExecutionSnapshot:
        return HiveExecutionSnapshot(
            ok=False,
            found=False,
            db_path=str(default_hive_runtime_db_path()),
            error=str(exc),
            executions=[],
            reviews=[],
        )

    @server.tool(name=_tool_name("notion2api_hive_upsert_execution_adapter"), description=_tool_description("Register or update one compiled safe execution adapter. Unknown implementations are rejected, adapters remain disabled unless authorized by a governance-plan decision receipt, and compiled capability, domain, timeout, payload, review, and approval limits cannot be weakened."), structured_output=True)
    async def notion2api_hive_upsert_execution_adapter(
        adapter_id: str,
        implementation_id: str,
        display_name: str,
        capabilities: list[str] | None = None,
        writable_domains: list[str] | None = None,
        required_authority: str = "A0",
        max_timeout_ms: int = 1000,
        max_payload_bytes: int = 4096,
        requires_human_approval: bool = False,
        requires_independent_review: bool = False,
        enabled: bool = False,
        actor: str = "notion2api",
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        expected_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> HiveAdapterSnapshot:
        try:
            return get_hive_execution_dispatcher_store().upsert_adapter(
                adapter_id=adapter_id,
                implementation_id=implementation_id,
                display_name=display_name,
                capabilities=capabilities,
                writable_domains=writable_domains,
                required_authority=required_authority,
                max_timeout_ms=max_timeout_ms,
                max_payload_bytes=max_payload_bytes,
                requires_human_approval=requires_human_approval,
                requires_independent_review=requires_independent_review,
                enabled=enabled,
                actor=actor,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                expected_revision=expected_revision,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _adapter_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_list_execution_adapters"), description=_tool_description("List the durable AIgentBee execution-adapter allowlist. Built-in adapters are statically compiled and initially disabled; this operation is read-only."), structured_output=True)
    async def notion2api_hive_list_execution_adapters(
        adapter_id: str = "",
        status: str = "",
        limit: int = 100,
    ) -> HiveAdapterSnapshot:
        try:
            return get_hive_execution_dispatcher_store().list_adapters(
                adapter_id=adapter_id,
                status=status,
                limit=limit,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _adapter_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_execute_dispatch"), description=_tool_description("Execute one READY materialized lane through an enabled compiled adapter after verifying the active lease, appointed worker, capability, writable-domain allowlists, authority ceiling, payload boundary, timeout, and any governance-plan authorization requirement. No shell, browser, filesystem, credential, network, or arbitrary code adapter is available."), structured_output=True)
    async def notion2api_hive_execute_dispatch(
        plan_id: str,
        work_unit_id: str,
        adapter_id: str,
        requested_capability: str,
        payload: dict[str, Any],
        requested_writable_domains: list[str] | None = None,
        timeout_ms: int = 1000,
        actor: str = "notion2api",
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        execution_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> HiveExecutionSnapshot:
        try:
            return get_hive_execution_dispatcher_store().execute_dispatch(
                plan_id=plan_id,
                work_unit_id=work_unit_id,
                adapter_id=adapter_id,
                requested_capability=requested_capability,
                payload=payload,
                requested_writable_domains=requested_writable_domains,
                timeout_ms=timeout_ms,
                actor=actor,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                execution_id=execution_id,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _execution_error_snapshot(exc, execution_id or "")

    @server.tool(name=_tool_name("notion2api_hive_get_execution"), description=_tool_description("Read durable guarded-dispatch executions and independent-review receipts by execution, plan, or work-unit id. With no identifier, returns the most recent bounded set."), structured_output=True)
    async def notion2api_hive_get_execution(
        execution_id: str = "",
        plan_id: str = "",
        work_unit_id: str = "",
        limit: int = 100,
    ) -> HiveExecutionSnapshot:
        try:
            return get_hive_execution_dispatcher_store().get_execution(
                execution_id=execution_id,
                plan_id=plan_id,
                work_unit_id=work_unit_id,
                limit=limit,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _execution_error_snapshot(exc, execution_id)

    @server.tool(name=_tool_name("notion2api_hive_cancel_execution"), description=_tool_description("Request cooperative cancellation of a RUNNING guarded execution or terminally cancel a claimed or review-pending execution while preserving evidence and synchronizing the Phase 2 dispatch receipt."), structured_output=True)
    async def notion2api_hive_cancel_execution(
        execution_id: str,
        actor: str,
        reason: str,
        idempotency_key: str | None = None,
    ) -> HiveExecutionSnapshot:
        try:
            return get_hive_execution_dispatcher_store().cancel_execution(
                execution_id=execution_id,
                actor=actor,
                reason=reason,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _execution_error_snapshot(exc, execution_id)

    @server.tool(name=_tool_name("notion2api_hive_recover_execution"), description=_tool_description("Governance-plan-authorized recovery for a stale CLAIMED or RUNNING guarded execution. Recovery reuses the persisted bounded request, increments the attempt ledger, remains idempotent, and finalizes a pending cancellation instead of rerunning it."), structured_output=True)
    async def notion2api_hive_recover_execution(
        execution_id: str,
        actor: str,
        reason: str,
        stale_after_ms: int = 30000,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> HiveExecutionSnapshot:
        try:
            return get_hive_execution_dispatcher_store().recover_execution(
                execution_id=execution_id,
                actor=actor,
                reason=reason,
                stale_after_ms=stale_after_ms,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _execution_error_snapshot(exc, execution_id)

    @server.tool(name=_tool_name("notion2api_hive_review_execution"), description=_tool_description("Submit the independent review for a REVIEW_REQUIRED execution. The reviewer must be a distinct APPOINTED GOVERNANCE_REVIEWER with sufficient authority; approval completes the lane and rejection fails it."), structured_output=True)
    async def notion2api_hive_review_execution(
        execution_id: str,
        reviewer_worker_id: str,
        approved: bool,
        actor: str,
        findings: dict[str, Any] | None = None,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> HiveExecutionSnapshot:
        try:
            return get_hive_execution_dispatcher_store().review_execution(
                execution_id=execution_id,
                reviewer_worker_id=reviewer_worker_id,
                approved=approved,
                actor=actor,
                findings=findings,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _execution_error_snapshot(exc, execution_id)

    def _external_effect_error_snapshot(exc: Exception) -> ExternalEffectSnapshot:
        return ExternalEffectSnapshot(
            ok=False,
            found=False,
            db_path=str(default_hive_runtime_db_path()),
            error=str(exc),
            certifications=[],
            effects=[],
        )

    @server.tool(name=_tool_name("notion2api_hive_certify_external_adapter"), description=_tool_description("Certify the compiled reversible sandbox-artifact adapter with a threat model, credential boundary, rollback contract, independent reviewer, sandbox allowlist, and a governance-plan authorization receipt."), structured_output=True)
    async def notion2api_hive_certify_external_adapter(
        adapter_id: str,
        implementation_id: str,
        sandbox_name: str,
        threat_model: dict[str, Any],
        rollback_contract: dict[str, Any],
        reviewer_worker_id: str,
        actor: str,
        allowed_extensions: list[str] | None = None,
        max_effect_bytes: int = 65536,
        credential_boundary: str = "none",
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        certification_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> ExternalEffectSnapshot:
        try:
            return get_hive_external_effect_store().certify_adapter(
                adapter_id=adapter_id,
                implementation_id=implementation_id,
                sandbox_name=sandbox_name,
                allowed_extensions=allowed_extensions,
                max_effect_bytes=max_effect_bytes,
                threat_model=threat_model,
                credential_boundary=credential_boundary,
                rollback_contract=rollback_contract,
                reviewer_worker_id=reviewer_worker_id,
                actor=actor,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                certification_id=certification_id,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _external_effect_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_list_external_certifications"), description=_tool_description("Read durable external-effect adapter certifications and their current certified, suspended, or revoked state."), structured_output=True)
    async def notion2api_hive_list_external_certifications(
        certification_id: str = "",
        status: str = "",
        limit: int = 100,
    ) -> ExternalEffectSnapshot:
        try:
            return get_hive_external_effect_store().get_certifications(
                certification_id=certification_id,
                status=status,
                limit=limit,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _external_effect_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_transition_external_certification"), description=_tool_description("Governance-plan-authorized certification control for suspending, re-certifying, or permanently revoking one external-effect adapter certification."), structured_output=True)
    async def notion2api_hive_transition_external_certification(
        certification_id: str,
        target_status: str,
        actor: str,
        reason: str,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        expected_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> ExternalEffectSnapshot:
        try:
            return get_hive_external_effect_store().transition_certification(
                certification_id=certification_id,
                target_status=target_status,
                actor=actor,
                reason=reason,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                expected_revision=expected_revision,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _external_effect_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_list_external_effects"), description=_tool_description("Read Phase 4 dry-run, applied, rolled-back, compensation-failed, and tamper-detected effect receipts."), structured_output=True)
    async def notion2api_hive_list_external_effects(
        effect_id: str = "",
        execution_id: str = "",
        status: str = "",
        limit: int = 100,
    ) -> ExternalEffectSnapshot:
        try:
            return get_hive_external_effect_store().get_effects(
                effect_id=effect_id,
                execution_id=execution_id,
                status=status,
                limit=limit,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _external_effect_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_hive_rollback_external_effect"), description=_tool_description("Restore the certified preimage for one applied sandbox effect after token, target-integrity, reviewer, and governance-plan authorization checks."), structured_output=True)
    async def notion2api_hive_rollback_external_effect(
        effect_id: str,
        rollback_token: str,
        reviewer_worker_id: str,
        actor: str,
        reason: str,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> ExternalEffectSnapshot:
        try:
            return get_hive_external_effect_store().rollback_effect(
                effect_id=effect_id,
                rollback_token=rollback_token,
                reviewer_worker_id=reviewer_worker_id,
                actor=actor,
                reason=reason,
                human_approval=human_approval,
                governance_authorization=governance_authorization,
                idempotency_key=idempotency_key,
            )
        except (HiveRuntimeError, ValueError) as exc:
            return _external_effect_error_snapshot(exc)

    @server.tool(name=_tool_name("notion2api_list_sessions"), description=_tool_description("List named persistent Notion2API MCP chat sessions with local and remote identifiers and a preview-only retention plan."), structured_output=True)
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
            retention=_build_session_retention_plan(records),
        )

    @server.tool(
        name=_tool_name("notion2api_manage_session_retention"),
        description=_tool_description(
            "Preview session-retention candidates or, only when apply=true, archive eligible bindings to append-only JSONL before removing them from the active session index. Active chat-job bindings, governance leader sessions, evidence-bound sessions, and records without timestamps are protected."
        ),
        structured_output=True,
    )
    async def notion2api_manage_session_retention(
        apply: bool = False,
        retention_days: int | None = None,
        max_records: int | None = None,
        applied_by: str = "ChatGPT user",
    ) -> SessionRetentionOutput:
        try:
            with _SESSION_STATE_MUTEX:
                records = _load_session_records()
                plan = _build_session_retention_plan(
                    records,
                    retention_days=retention_days,
                    max_records=max_records,
                )
                archive_path = _session_archive_path()
                archive_receipt = {
                    "archived": 0,
                    "archive_path": str(archive_path),
                }
                retained = records
                if apply and plan.get("candidates"):
                    retained, archive_receipt = archive_and_filter_sessions(
                        records,
                        plan,
                        archive_path=archive_path,
                        applied_by=applied_by,
                    )
                    _save_session_records(retained, strict=True)
            counts = dict(plan.get("counts") or {})
            counts["retained"] = len(retained)
            return SessionRetentionOutput(
                ok=True,
                applied=bool(apply and int(archive_receipt.get("archived") or 0)),
                state_path=str(DEFAULT_SESSION_STATE_PATH),
                archive_path=str(archive_receipt.get("archive_path") or archive_path),
                policy=dict(plan.get("policy") or {}),
                counts=counts,
                protected=list(plan.get("protected") or []),
                candidates=list(plan.get("candidates") or []),
                archived=int(archive_receipt.get("archived") or 0),
                retained=len(retained),
            )
        except Exception as exc:
            return SessionRetentionOutput(
                ok=False,
                state_path=str(DEFAULT_SESSION_STATE_PATH),
                archive_path=str(_session_archive_path()),
                error=f"{type(exc).__name__}: {exc}",
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
