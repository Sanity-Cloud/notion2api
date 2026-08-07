# pylint: disable=broad-exception-caught, protected-access
import asyncio
import json
import os
import re
import time
import uuid
from difflib import SequenceMatcher
from typing import Any, Dict, Generator, Iterable, List, Tuple

from fastapi import APIRouter, BackgroundTasks, HTTPException, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from app.core.errors import openai_error
from app.core.internal_callers import is_repo_ai_internal_request
from app.core.models import normalize_model_id
from app.governance import (
    combine_governance_instructions,
    governance_receipt_from_client,
    resolve_governed_context_page_id,
)
from app.conversation import (
    apply_notion_ai_options,
    compress_round_if_needed,
    compress_sliding_window_round,
    build_lite_transcript,
)
from app.config import is_lite_mode
from app.logger import logger
from app.model_registry import (
    get_model_route_resolution,
    is_supported_model,
    list_available_models,
)
from app.notion_client import NotionUpstreamError
from app.attachments.normalizer import (
    PromptValidationError,
    normalize_chat_messages,
    validate_chat_messages,
    validate_inline_attachment_data,
)
from app.attachments.security import AttachmentPolicy
from app.attachments.errors import AttachmentError
from app.output_integrity import assess_output_integrity
from app.retry_policy import bounded_provider_attempts
from app.output_hygiene import (
    detect_visible_output_contamination,
    finalize_visible_output,
    prepare_visible_stream_chunk,
)
from app.schemas import (
    ChatCompletionRequest,
    ChatCompletionResponse,
    ChatMessage,
    ChatMessageResponseChoice,
)
from app.thread_title import resolve_requested_thread_title
from app.api.chat_resume_thread_binding import _resolve_persistent_thread_id
from app.account_scope import account_key_from_client
from app.chat_history.live_recorder import infer_source_system, record_live_chat_turn
from app.hive_bee_call import validate_bee_notion_call
from app.hive_multithread import MultithreadContractError

router = APIRouter()


def _enforce_bee_notion_call_contract(
    *,
    manager: Any,
    conversation_id: str,
    client: Any,
    req_body: Any,
) -> None:
    """Reject AIgentBee lane chats that borrow another account/conversation/thread."""
    metadata = req_body.metadata if isinstance(getattr(req_body, "metadata", None), dict) else {}
    try:
        scope = (
            manager.get_conversation_scope(conversation_id)
            if manager and conversation_id and hasattr(manager, "get_conversation_scope")
            else {}
        )
        validate_bee_notion_call(
            metadata=metadata,
            conversation_id=str(conversation_id or ""),
            client=client,
            conversation_scope=scope,
            bound_thread_id=str((scope or {}).get("thread_id") or ""),
        )
    except MultithreadContractError as exc:
        openai_error(
            str(exc),
            "bee_call_contract_mismatch",
            status_code=409,
            param="metadata.caller",
        )
        raise AssertionError("openai_error must raise") from exc


def _record_live_chat_history_turn(
    *,
    client: Any,
    thread_id: str | None,
    conversation_id: str,
    user_prompt: str,
    assistant_reply: str,
    requested_model: str = "",
    model_metadata: dict[str, Any] | None = None,
    request_metadata: dict[str, Any] | None = None,
) -> None:
    """Mirror completed turns into the selected account's history shard."""
    notion_thread_id = str(thread_id or "").strip()
    if not notion_thread_id:
        return
    if not str(user_prompt or "").strip() and not str(assistant_reply or "").strip():
        return
    try:
        record_live_chat_turn(
            thread_id=notion_thread_id,
            conversation_id=conversation_id,
            account_key=account_key_from_client(client),
            user_prompt=user_prompt,
            assistant_reply=assistant_reply,
            requested_model=requested_model,
            model_metadata=model_metadata,
            request_metadata=request_metadata,
        )
    except Exception:
        source_system = infer_source_system(request_metadata)
        logger.warning(
            "Failed to record live chat turn into chat history archive",
            exc_info=True,
            extra={
                "request_info": {
                    "event": "chat_history_live_turn_failed",
                    "conversation_id": conversation_id,
                    "thread_id": notion_thread_id,
                }
            },
        )
        if source_system in {"aigentbee", "repoai"}:
            raise


def _apply_notion_request_options(
    transcript: list[dict[str, Any]],
    req_body: ChatCompletionRequest,
    client: Any | None = None,
) -> list[dict[str, Any]]:
    instructions = (
        combine_governance_instructions(client, req_body.notion_instructions)
        if client is not None
        else req_body.notion_instructions
    )
    return apply_notion_ai_options(
        transcript,
        mode=req_body.notion_mode,
        task=req_body.notion_task,
        sources=req_body.notion_sources,
        web_access=req_body.web_access,
        persona=req_body.notion_persona,
        instructions=instructions,
    )


def _bind_governance_request_metadata(
    req_body: ChatCompletionRequest, client: Any
) -> dict[str, Any]:
    metadata = dict(req_body.metadata or {}) if isinstance(req_body.metadata, dict) else {}
    receipt = governance_receipt_from_client(client)
    supplied = metadata.get("governance")
    if isinstance(supplied, dict):
        for key in (
            "contract_version",
            "teamspace_id",
            "authority_page_id",
            "documented_output_parent_page_id",
            "procedural_feedback_parent_page_id",
        ):
            requested = str(supplied.get(key) or "").strip()
            canonical = str(receipt.get(key) or "").strip()
            if requested and requested != canonical:
                openai_error(
                    f"Request governance {key} conflicts with the canonical SanityCloud contract.",
                    "governance_contract_mismatch",
                    status_code=409,
                    param=f"metadata.governance.{key}",
                )
    metadata["governance"] = receipt
    caller = metadata.get("caller") if isinstance(metadata.get("caller"), dict) else {}
    explicit_idempotency = str(
        metadata.get("idempotency_key")
        or metadata.get("request_fingerprint")
        or metadata.get("mcp_request_id")
        or caller.get("request_fingerprint")
        or ""
    ).strip()
    session_name = str(
        metadata.get("mcp_session_name")
        or metadata.get("session_name")
        or ""
    ).strip()
    caller_type = str(caller.get("type") or caller.get("caller_type") or "").strip()
    if not explicit_idempotency and session_name:
        explicit_idempotency = f"leader-session:{caller_type or 'unknown'}:{session_name}"
    trace_id = str(
        metadata.get("trace_id")
        or metadata.get("mcp_request_id")
        or metadata.get("request_fingerprint")
        or caller.get("request_fingerprint")
        or ""
    ).strip()
    request_context_id = str(
        metadata.get("request_context_id")
        or metadata.get("repo_ai_run_id")
        or metadata.get("run_id")
        or metadata.get("mcp_session_name")
        or metadata.get("session_name")
        or getattr(req_body, "conversation_id", "")
        or ""
    ).strip()
    client.request_idempotency_key = explicit_idempotency
    client.request_trace_id = trace_id
    client.request_context_id = request_context_id
    client.request_model_id = normalize_model_id(req_body.model) or str(req_body.model or "")
    metadata["request_context_id"] = request_context_id
    metadata["trace_id"] = trace_id
    req_body.metadata = metadata
    return receipt


def _requested_thread_title(req_body: ChatCompletionRequest) -> str | None:
    return resolve_requested_thread_title(
        chat_title=req_body.chat_title,
        title=req_body.title,
        session_name=req_body.session_name,
        metadata=req_body.metadata,
    )


def _persist_local_thread_title(
    request: Request, req_body: ChatCompletionRequest, title: str | None
) -> None:
    conversation_id = str(req_body.conversation_id or "").strip()
    manager = getattr(request.app.state, "conversation_manager", None)
    if (
        title
        and conversation_id
        and manager
        and manager.conversation_exists(conversation_id)
    ):
        manager.set_conversation_title(conversation_id, title)


# Structured error responses
def _classify_upstream_error(exc: NotionUpstreamError) -> dict[str, Any]:
    """Classify NotionUpstreamError into frontend-safe structured error details."""
    sc = exc.status_code
    excerpt = str(exc.response_excerpt or "")

    if sc == 409 or "BOUND_THREAD_HISTORY_REPLAY" in excerpt:
        return {
            "code": "BOUND_THREAD_HISTORY_REPLAY",
            "type": "conversation_integrity_error",
            "message": "Send only the new user turn for the existing bound thread. Do not replay prior assistant or historical dialog.",
            "suggestion": "Send only the new user turn for the existing bound thread. Do not replay prior assistant or historical dialog.",
            "status_code": 409,
        }
    if sc == 401:
        return {
            "code": "NOTION_401",
            "type": "upstream_auth_error",
            "message": "Notion authentication failed (HTTP 401). The saved session may be expired.",
            "suggestion": "Refresh the local login session and update configuration.",
            "status_code": 401,
        }
    if sc == 403:
        return {
            "code": "NOTION_403",
            "type": "upstream_forbidden",
            "message": "Notion denied access (HTTP 403). Cloudflare or account restrictions may be involved.",
            "suggestion": "Check server network access or retry later.",
            "status_code": 403,
        }
    if sc == 429:
        return {
            "code": "NOTION_429",
            "type": "upstream_rate_limit",
            "message": "Notion request rate is too high (HTTP 429).",
            "suggestion": "Wait briefly before retrying, or configure multiple accounts to spread requests.",
            "status_code": 429,
        }
    if sc and sc >= 500:
        msg = f"Notion is temporarily unavailable (HTTP {sc})."
        if "missing_finishedAt" in excerpt:
            msg = "Notion stream ended before completion metadata (missing_finishedAt)."
        return {
            "code": f"NOTION_{sc}",
            "type": "upstream_server_error",
            "message": msg,
            "suggestion": "The Notion upstream service failed. Inspect diagnostic metadata; do not resubmit blindly.",
            "status_code": sc,
        }
    if "timed out" in str(exc).lower():
        return {
            "code": "NETWORK_TIMEOUT",
            "type": "network_timeout",
            "message": "Connection to Notion timed out.",
            "suggestion": "Check network connectivity from the server to notion.so.",
            "status_code": 504,
        }
    if "failed" in str(exc).lower() and not sc:
        return {
            "code": "NETWORK_ERROR",
            "type": "network_error",
            "message": "Unable to connect to the Notion service.",
            "suggestion": "Check server network and DNS configuration.",
            "status_code": 503,
        }
    if "empty" in str(exc).lower():
        return {
            "code": "NOTION_EMPTY",
            "type": "upstream_empty_response",
            "message": "Notion returned empty content.",
            "suggestion": "Send the message again.",
            "status_code": 502,
        }
    if sc == 400 and not exc.retriable:
        return {
            "code": "UPSTREAM_PROTOCOL_REJECTED",
            "type": "upstream_invalid_request",
            "message": str(exc),
            "suggestion": "Inspect the rejected Notion request stage and payload before retrying.",
            "status_code": 400,
        }
    return {
        "code": "UPSTREAM_UNKNOWN",
        "type": "upstream_error",
        "message": str(exc),
        "suggestion": "Retry later.",
        "status_code": sc or 503,
    }


def _build_error_response(
    status_code: int,
    *,
    code: str,
    message: str,
    error_type: str = "server_error",
    param: str | None = None,
    suggestion: str = "",
    detail: str = "",
) -> JSONResponse:
    """Build a unified JSON error response that the frontend can parse."""
    content: dict[str, Any] = {
        "error": {
            "message": message,
            "type": error_type,
            "param": param,
            "code": code,
        }
    }
    if suggestion:
        content["error"]["suggestion"] = suggestion
    if detail:
        content["error"]["detail"] = detail
    return JSONResponse(status_code=status_code, content=content)


def _upstream_error_response(exc: NotionUpstreamError) -> JSONResponse:
    """Convert NotionUpstreamError to a unified JSON response."""
    excerpt = str(exc.response_excerpt or "")
    if (
        exc.status_code == 422
        or "OUTPUT_CONTAMINATED" in excerpt
        or "INTERNAL_TOOL_SYNTAX_EXPOSED" in excerpt
    ):
        return _build_error_response(
            422,
            code="OUTPUT_CONTAMINATED"
            if "INTERNAL_TOOL_SYNTAX" not in excerpt
            else "INTERNAL_TOOL_SYNTAX_EXPOSED",
            message="Assistant output failed integrity validation and was quarantined.",
            error_type="indeterminate_output",
            suggestion="Inspect the original request and quarantined evidence; do not resubmit blindly.",
            detail=excerpt,
        )
    info = _classify_upstream_error(exc)
    resp_status = info.get("status_code") or exc.status_code or 503
    return _build_error_response(
        resp_status,
        code=info["code"],
        message=info["message"],
        error_type=info["type"],
        suggestion=info.get("suggestion", ""),
        detail=excerpt,
    )


def _raise_output_contaminated(hygiene: dict[str, Any]) -> None:
    integrity = hygiene.get("output_integrity")
    evidence = {
        "error_code": "OUTPUT_CONTAMINATED",
        "output_integrity": integrity if isinstance(integrity, dict) else {},
    }
    raise NotionUpstreamError(
        "Assistant output failed integrity validation.",
        status_code=422,
        retriable=False,
        response_excerpt=json.dumps(evidence, sort_keys=True),
    )


def _bound_thread_history_replay_response(
    *,
    conversation_id: str,
    thread_id: str,
    history_message_count: int,
) -> JSONResponse:
    evidence = {
        "error_code": "BOUND_THREAD_HISTORY_REPLAY",
        "conversation_id": conversation_id,
        "thread_id": thread_id,
        "history_message_count": history_message_count,
    }
    return _build_error_response(
        409,
        code="BOUND_THREAD_HISTORY_REPLAY",
        message=(
            "A persistent Notion thread may receive only the new user message; "
            "client-supplied historical dialog is prohibited."
        ),
        error_type="conversation_integrity_error",
        suggestion=(
            "Send only the new user turn for the existing bound thread. "
            "Do not replay prior assistant or historical dialog."
        ),
        detail=json.dumps(evidence, sort_keys=True),
    )


def _request_workspace_selector(req_body: ChatCompletionRequest) -> str:
    metadata = req_body.metadata if isinstance(req_body.metadata, dict) else {}
    return str(
        metadata.get("workspace_key")
        or metadata.get("workspace")
        or metadata.get("workspace_id")
        or ""
    ).strip()


def _resolve_request_model(
    request: Request,
    model: str | None,
    workspace_selector: str = "",
) -> str:
    normalized_model = normalize_model_id(model)
    if not normalized_model:
        openai_error("The 'model' field is required.", "model_required")

    restricted = set()
    try:
        pool = request.app.state.account_pool
        client = (
            pool.get_metadata_client_for_workspace(workspace_selector)
            if workspace_selector
            else pool.get_metadata_client()
        )
        from app.model_registry import get_restricted_models_for_space, get_notion_model

        restricted = get_restricted_models_for_space(client)
        notion_model = get_notion_model(normalized_model)
        if notion_model in restricted or normalized_model in restricted:
            openai_error(
                f"Model '{normalized_model}' is unavailable for the current account due to restriction (e.g. trial_not_allowed).",
                "model_restricted",
                status_code=400,
            )
    except Exception as e:
        if hasattr(e, "status_code"):
            raise e

    if not is_supported_model(normalized_model):
        try:
            available_models = [
                m
                for m in list_available_models()
                if get_notion_model(m) not in restricted and m not in restricted
            ]
        except Exception:
            available_models = list_available_models()
        openai_error(
            f"Unsupported model '{normalized_model}'. Available models: {', '.join(available_models)}",
            "model_not_found",
        )
    return normalized_model


def _client_type_from_request(request: Request) -> str:
    return request.headers.get("X-Client-Type", "").strip().lower()


def _emit_search_metadata_for_client(client_type: str) -> bool:
    return client_type == "web"


def _request_context_page_id(req_body: ChatCompletionRequest, client: Any) -> str:
    """Resolve the canonical governance authority and reject context drift."""
    metadata = req_body.metadata if isinstance(req_body.metadata, dict) else {}
    requested = str(metadata.get("context_page_id") or "").strip()
    try:
        return resolve_governed_context_page_id(client, requested)
    except ValueError as exc:
        openai_error(
            str(exc),
            "governance_context_mismatch",
            status_code=409,
            param="metadata.context_page_id",
        )
        raise AssertionError("openai_error must raise") from exc


def _conversation_scope_for_client(client: Any) -> dict[str, str]:
    return {
        "workspace_id": str(getattr(client, "space_id", "") or "").strip(),
        "teamspace_id": str(
            getattr(client, "governance_teamspace_id", "") or ""
        ).strip(),
        "user_id": str(getattr(client, "user_id", "") or "").strip(),
        "profile_name": str(
            getattr(client, "profile_name", "")
            or getattr(client, "base_profile_name", "")
            or ""
        ).strip(),
        "publication_parent_page_id": str(
            getattr(client, "documented_output_parent_page_id", "") or ""
        ).strip(),
        "governance_contract_version": str(
            getattr(client, "governance_contract_version", "") or ""
        ).strip(),
    }


def _bind_or_verify_conversation_scope(
    manager: Any,
    conversation_id: str,
    client: Any,
) -> dict[str, str]:
    try:
        return manager.bind_conversation_scope(
            conversation_id,
            **_conversation_scope_for_client(client),
        )
    except ValueError as exc:
        openai_error(
            str(exc),
            "conversation_scope_mismatch",
            status_code=409,
            param="conversation_id",
        )
        raise AssertionError("openai_error must raise") from exc


def _request_account_selector(req_body: ChatCompletionRequest) -> str:
    metadata = req_body.metadata if isinstance(req_body.metadata, dict) else {}
    explicit = str(
        metadata.get("account_profile")
        or metadata.get("profile_name")
        or metadata.get("account_selector")
        or metadata.get("notion_profile")
        or ""
    ).strip()
    if explicit:
        return explicit
    if _request_computer_use_review(req_body):
        return str(os.getenv("NOTION_COMPUTER_USE_PROFILE") or "").strip()
    return ""


def _client_for_requested_workspace(pool: Any, req_body: ChatCompletionRequest) -> Any:
    workspace_selector = _request_workspace_selector(req_body)
    account_selector = _request_account_selector(req_body)
    if account_selector:
        return pool.get_client_for_workspace_account(
            workspace_selector, account_selector
        )
    return (
        pool.get_client_for_workspace(workspace_selector)
        if workspace_selector
        else pool.get_client()
    )


def _outer_retry_limit(
    pool: Any,
    req_body: ChatCompletionRequest,
    *,
    persistent_thread: bool = False,
) -> int:
    metadata = req_body.metadata if isinstance(req_body.metadata, dict) else {}
    if (
        persistent_thread
        or bool(metadata.get("persist_remote_chat"))
        or bool(metadata.get("computer_use_review"))
    ):
        # A durable remote thread is an externally visible side effect. Replaying
        # the request after an indeterminate stream can duplicate threads, uploads,
        # and model work, so reconciliation must precede any retry.
        return 1
    selector = _request_workspace_selector(req_body)
    try:
        account_count = max(1, int(pool.get_workspace_account_count(selector)))
    except Exception:
        account_count = 1
    return bounded_provider_attempts(account_count)


def _client_for_conversation(
    pool: Any,
    manager: Any,
    conversation_id: str,
    req_body: ChatCompletionRequest,
) -> Any:
    stored_scope = manager.get_conversation_scope(conversation_id)
    stored_workspace = str(stored_scope.get("workspace_id") or "").strip()
    stored_user = str(stored_scope.get("user_id") or "").strip()
    requested_workspace = _request_workspace_selector(req_body)

    if stored_workspace and stored_user:
        if requested_workspace:
            requested_client = pool.get_metadata_client_for_workspace(requested_workspace)
            if (
                requested_client.space_id.replace("-", "").casefold()
                != stored_workspace.replace("-", "").casefold()
            ):
                openai_error(
                    "Requested workspace conflicts with the persistent chat binding.",
                    "conversation_workspace_mismatch",
                    status_code=409,
                    param="metadata.workspace_key",
                )
        client = pool.get_client_for_binding(
            workspace_id=stored_workspace,
            user_id=stored_user,
        )
    else:
        client = _client_for_requested_workspace(pool, req_body)

    _bind_or_verify_conversation_scope(manager, conversation_id, client)
    return client


def _request_computer_use_review(req_body: ChatCompletionRequest) -> bool:
    metadata = req_body.metadata if isinstance(req_body.metadata, dict) else {}
    if "computer_use_review" in metadata:
        return bool(metadata.get("computer_use_review"))
    return str(metadata.get("review_mode") or "").strip().lower() == "computer_use"


def _strict_model_requested(req_body: ChatCompletionRequest) -> bool:
    metadata = req_body.metadata if isinstance(req_body.metadata, dict) else {}
    return bool(metadata.get("strict_model"))


def _is_model_mismatch(response_obj: ChatCompletionResponse) -> bool:
    requested = str(
        getattr(response_obj, "notion_requested_model", "")
        or getattr(response_obj, "requested_model", "")
        or ""
    ).strip()
    actual = str(getattr(response_obj, "actual_model", "") or "").strip()
    return bool(requested and actual and requested != actual)


def _model_mismatch_response(response_obj: ChatCompletionResponse) -> JSONResponse:
    return _build_error_response(
        409,
        code="MODEL_MISMATCH",
        message="Requested model was not the model that answered.",
        error_type="model_mismatch",
        suggestion="Use an available model or disable strict model enforcement for this request.",
        detail=json.dumps(
            {
                "requested_model": getattr(response_obj, "requested_model", None),
                "notion_requested_model": getattr(
                    response_obj, "notion_requested_model", None
                ),
                "actual_model": getattr(response_obj, "actual_model", None),
            },
            ensure_ascii=False,
        ),
    )


def _message_content_to_text(content: Any) -> str:
    """Helper to safely coerce content string/list into a single string."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                item_type = str(item.get("type") or "").lower()
                if item_type in {"input_text", "output_text", "text"} or "text" in item:
                    text = str(item.get("text") or "")
                    if text:
                        parts.append(text)
            elif hasattr(item, "model_dump") and callable(item.model_dump):
                dct = item.model_dump()
                item_type = str(dct.get("type") or "").lower()
                if item_type in {"input_text", "output_text", "text"} or "text" in dct:
                    text = str(dct.get("text") or "")
                    if text:
                        parts.append(text)
        return "\n".join(parts)
    return str(content)


def _last_user_message_content(messages: Iterable[Any]) -> Any:
    """Return the content field from the last user message, if any."""
    for message in reversed(list(messages)):
        role = getattr(message, "role", None)
        if role is None and isinstance(message, dict):
            role = message.get("role")
        if role != "user":
            continue
        content = getattr(message, "content", None)
        if content is None and isinstance(message, dict):
            content = message.get("content")
        return content
    return ""


_LOCAL_PROBE_OK = frozenset(
    {
        "reply with ok.",
        "reply with ok",
        "respond with ok.",
        "respond with ok",
    }
)

_LOCAL_PROBE_PONG = frozenset(
    {
        "ping! respond with exactly 'pong' to verify connection.",
        "reply with exactly: pong",
        "reply with exactly pong",
    }
)


def _normalize_probe_text(content: Any) -> str:
    text = _message_content_to_text(content)
    if not text:
        return ""
    return " ".join(text.strip().split()).lower()


def _strip_probe_markdown_prefix(text: str) -> str:
    stripped = text.strip()
    while stripped.startswith("#"):
        stripped = stripped.lstrip("#").strip()
    return stripped


def _probe_match_candidates(content: Any) -> list[str]:
    """Collect normalized strings that may be bare or wrapped probe prompts."""
    text = _message_content_to_text(content)
    if not text:
        return []

    candidates: list[str] = []

    def _add(candidate: str) -> None:
        normalized = " ".join(candidate.strip().split()).lower()
        if not normalized:
            return
        if normalized not in candidates:
            candidates.append(normalized)
        markdown_stripped = _strip_probe_markdown_prefix(normalized)
        if markdown_stripped and markdown_stripped not in candidates:
            candidates.append(markdown_stripped)

    _add(text)

    for match in re.finditer(r"(?im)^\s*question:\s*(.+?)\s*$", text):
        _add(match.group(1))

    for match in re.finditer(
        r"(?i)\[current user request\]\s*(.+)$", text, flags=re.DOTALL
    ):
        tail = match.group(1).strip()
        if not tail:
            continue
        _add(tail)
        for question_match in re.finditer(r"(?im)^\s*question:\s*(.+?)\s*$", tail):
            _add(question_match.group(1))

    return candidates


def _local_probe_response_text(content: Any) -> str:
    """Return a local response for health/preflight prompts that must not persist."""
    for normalized in _probe_match_candidates(content):
        if normalized in _LOCAL_PROBE_OK:
            return "OK"
        if normalized in _LOCAL_PROBE_PONG:
            return "pong"
    return ""


def _sandbox_remote_request_authorized(
    request: Request,
    req_body: ChatCompletionRequest,
) -> bool:
    """Require explicit opt-in before a sandbox may call the remote provider."""
    required = os.getenv("NOTION_SANDBOX_REQUIRE_EXPLICIT_REMOTE", "").strip().lower()
    if required not in {"1", "true", "yes", "on"}:
        return True

    header_value = request.headers.get("x-sandbox-allow-remote", "").strip().lower()
    if header_value in {"1", "true", "yes", "on"}:
        return True

    metadata = req_body.metadata if isinstance(req_body.metadata, dict) else {}
    metadata_value = metadata.get("sandbox_allow_remote")
    if isinstance(metadata_value, bool):
        return metadata_value
    return str(metadata_value or "").strip().lower() in {"1", "true", "yes", "on"}


RECALL_INTENT_KEYWORDS = [
    "history",
    "history",
    "history",
    "history",
    "history",
    "earlier",
    "before",
    "recall",
    "remember",
    "history",
    "history",
    "history",
    "history",
]


def _strip_visible_stream_chunk(text: str) -> str:
    """Remove hidden-reasoning markup from a single streamed visible chunk."""

    return prepare_visible_stream_chunk("", text)


def _prepare_visible_stream_chunk(accumulator: str, raw_chunk: Any) -> str:
    """Normalize a streamed chunk and preserve missing word boundaries."""

    return prepare_visible_stream_chunk(accumulator, raw_chunk)


def _finalize_visible_reply(
    streamed_text: str,
    authoritative_final: str,
    authoritative_source: str,
) -> tuple[str, str, dict[str, Any]]:
    raw_reply, decision = _select_best_final_reply(
        streamed_text,
        authoritative_final,
        authoritative_source,
    )
    sanitized, hygiene = finalize_visible_output(raw_reply)
    additional_reasons = (
        ("visible_output_contamination",)
        if hygiene.get("visible_contamination_detected")
        else ()
    )
    hygiene["output_integrity"] = assess_output_integrity(
        raw_reply,
        additional_reasons=additional_reasons,
    )
    return sanitized, decision, hygiene


def _output_requires_quarantine(hygiene: dict[str, Any] | None) -> bool:
    if not isinstance(hygiene, dict):
        return False
    integrity = hygiene.get("output_integrity")
    return bool(isinstance(integrity, dict) and integrity.get("quarantine_required"))


def _build_hygiene_metadata_event(hygiene: dict[str, Any]) -> str:
    if not (
        hygiene.get("hidden_thinking_removed")
        or hygiene.get("visible_contamination_detected")
        or hygiene.get("retry_recommended")
        or _output_requires_quarantine(hygiene)
    ):
        return ""
    event = {"type": "output_hygiene", "hygiene": hygiene}
    return f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


MAX_GUARDED_STREAM_BUFFER_CHARS = 500_000


def _parse_sse_json(chunk: str) -> dict[str, Any] | None:
    """Parse one JSON SSE frame; non-JSON and ``[DONE]`` frames return ``None``."""
    stripped = str(chunk or "").strip()
    if not stripped.startswith("data:"):
        return None
    payload = stripped[5:].strip()
    if not payload or payload == "[DONE]":
        return None
    try:
        value = json.loads(payload)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _guard_stream_until_integrity(
    source: Iterable[str],
    *,
    response_id: str,
    model: str,
) -> Generator[str, None, None]:
    """Buffer provider SSE until final integrity classification is known.

    A final-only quarantine cannot retract deltas already delivered to clients.
    This guard therefore withholds provider output until the stream reaches a
    terminal integrity decision. Local probe streams do not use this wrapper.
    """
    buffered: list[str] = []
    metadata_events: list[str] = []
    hygiene_events: list[str] = []
    total_chars = 0
    quarantined = False
    forced_limit = False

    for raw_chunk in source:
        chunk = str(raw_chunk)
        total_chars += len(chunk)
        if not forced_limit and total_chars <= MAX_GUARDED_STREAM_BUFFER_CHARS:
            buffered.append(chunk)
        else:
            forced_limit = True
            buffered.clear()

        payload = _parse_sse_json(chunk)
        if payload is None:
            continue
        event_type = str(payload.get("type") or "")
        if event_type == "model_metadata":
            metadata_events.append(chunk)
        if event_type == "output_hygiene":
            hygiene_events.append(chunk)
            hygiene = payload.get("hygiene")
            if isinstance(hygiene, dict) and _output_requires_quarantine(hygiene):
                quarantined = True

        choices = payload.get("choices")
        if isinstance(choices, list) and choices:
            choice = choices[0] if isinstance(choices[0], dict) else {}
            if choice.get("finish_reason") == "content_filter":
                quarantined = True

    if forced_limit:
        hygiene = {
            "output_integrity": assess_output_integrity(
                "",
                additional_reasons=("guarded_stream_buffer_limit_exceeded",),
            )
        }
        yield from metadata_events
        yield _build_hygiene_metadata_event(hygiene)
        yield _build_stream_chunk(response_id, model, finish_reason="content_filter")
        yield "data: [DONE]\n\n"
        return

    if quarantined:
        yield from metadata_events
        yield from hygiene_events
        yield _build_stream_chunk(response_id, model, finish_reason="content_filter")
        yield "data: [DONE]\n\n"
        return

    yield from buffered


def _emit_visible_stream_correction(
    response_id: str,
    model_name: str,
    *,
    assistant_started: bool,
    streamed_text: str,
    sanitized_text: str,
    client_type: str = "",
) -> tuple[bool, str, list[str]]:
    """Emit best-effort stream corrections when sanitized text differs."""

    chunks: list[str] = []
    if sanitized_text == streamed_text:
        return assistant_started, streamed_text, chunks

    if sanitized_text.startswith(streamed_text):
        suffix = sanitized_text[len(streamed_text) :]
        if suffix:
            if not assistant_started:
                assistant_started = True
                chunks.append(
                    _build_stream_chunk(
                        response_id,
                        model_name,
                        role="assistant",
                        content=suffix,
                    )
                )
            else:
                chunks.append(
                    _build_stream_chunk(response_id, model_name, content=suffix)
                )
            streamed_text += suffix
        return assistant_started, streamed_text, chunks

    if not streamed_text and sanitized_text:
        if not assistant_started:
            assistant_started = True
            chunks.append(
                _build_stream_chunk(
                    response_id,
                    model_name,
                    role="assistant",
                    content=sanitized_text,
                )
            )
        else:
            chunks.append(
                _build_stream_chunk(response_id, model_name, content=sanitized_text)
            )
        return assistant_started, sanitized_text, chunks

    if client_type == "web":
        chunks.append(
            _build_local_ui_chunk(
                response_id,
                model_name,
                "content_replace",
                content=sanitized_text,
                reason="output_hygiene",
            )
        )
        return assistant_started, sanitized_text, chunks

    return assistant_started, streamed_text, chunks


def _attach_response_hygiene(
    response_obj: ChatCompletionResponse,
    hygiene: dict[str, Any] | None,
) -> None:
    if hygiene and (
        any(value for key, value in hygiene.items() if key != "output_integrity")
        or _output_requires_quarantine(hygiene)
    ):
        response_obj.hygiene = hygiene


def _build_stream_chunk(
    response_id: str,
    model: str,
    *,
    content: str = "",
    thinking: str = "",
    role: str = "",
    finish_reason=None,
    usage: dict[str, Any] | None = None,
) -> str:
    delta: Dict[str, Any] = {}
    if role:
        delta["role"] = role
    if content:
        delta["content"] = content
    if thinking:
        delta["reasoning_content"] = thinking

    payload = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": delta, "finish_reason": finish_reason}],
    }
    if usage:
        payload["usage"] = usage
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _build_local_ui_chunk(
    response_id: str,
    model: str,
    event_type: str,
    **payload_fields: Any,
) -> str:
    payload: Dict[str, Any] = {
        "id": response_id,
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": model,
        "choices": [{"index": 0, "delta": {}, "finish_reason": None}],
        "type": event_type,
    }
    payload.update(payload_fields)
    return f"data: {json.dumps(payload, ensure_ascii=False)}\n\n"


def _format_search_results_md(search_data: dict[str, Any]) -> str:
    """Format search data as Markdown for standard clients."""
    lines = []
    queries = search_data.get("queries", [])
    if queries:
        lines.append(f"> 🔍 **Searched:** {', '.join(queries)}")

    sources = search_data.get("sources", [])
    if sources:
        lines.append("> 🌐 **Searched:**")
        for i, src in enumerate(sources[:5], 1):  # text5text
            title = src.get("title") or src.get("url") or "Unknown source"
            url = src.get("url")
            if url:
                lines.append(f"> {i}. [{title}]({url})")
            else:
                lines.append(f"> {i}. {title}")

    if lines:
        return "\n".join(lines) + "\n\n"
    return ""


def _normalize_stream_item(item: Any) -> dict[str, Any]:
    if isinstance(item, str):
        return {"type": "content", "text": item}

    if isinstance(item, dict):
        item_type = str(item.get("type", "") or "").lower()
        if item_type == "content":
            return {
                "type": "content",
                "text": str(item.get("text") or item.get("history", "") or ""),
            }
        if item_type == "search":
            payload = item.get("data")
            return {
                "type": "search",
                "data": payload if isinstance(payload, dict) else {},
            }
        if item_type == "thinking":
            return {
                "type": "thinking",
                "text": str(item.get("text") or item.get("history", "") or ""),
            }
        if item_type == "final_content":
            return {
                "type": "final_content",
                "text": str(item.get("text") or item.get("history", "") or ""),
                "source_type": str(item.get("source_type", "") or ""),
                "source_length": item.get("source_length"),
                "model_metadata": item.get("model_metadata")
                if isinstance(item.get("model_metadata"), dict)
                else {},
            }
        if item_type == "model_metadata":
            payload = item.get("data")
            return {
                "type": "model_metadata",
                "data": payload if isinstance(payload, dict) else {},
            }

    return {"type": "unknown"}


def _iter_stream_items(
    first_item: Any, stream_gen: Iterable[Any]
) -> Generator[Any, None, None]:
    if first_item is not None:
        yield first_item
    for item in stream_gen:
        yield item


def _merge_model_metadata(
    current: dict[str, Any] | None, item: dict[str, Any]
) -> dict[str, Any]:
    merged: dict[str, Any] = dict(current or {})
    payload: Any = None
    item_type = str(item.get("type", "") or "")
    if item_type == "model_metadata":
        payload = item.get("data")
    elif item_type == "final_content":
        payload = item.get("model_metadata")
    if not isinstance(payload, dict):
        return merged
    for key, value in payload.items():
        if value not in (None, "", [], {}):
            merged[str(key)] = value
    return merged


def _response_model_metadata(
    requested_model: str,
    model_metadata: dict[str, Any] | None,
    request_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = dict(model_metadata or {})
    requested = normalize_model_id(requested_model) or requested_model
    notion_requested = ""
    route_resolution: dict[str, object] = {}
    if requested:
        payload.setdefault("requested_model", requested)
        try:
            route_resolution = get_model_route_resolution(requested)
            notion_requested = str(route_resolution["resolved_model"])
            payload.setdefault("notion_requested_model", notion_requested)
            payload.setdefault(
                "resolved_model",
                str(payload.get("notion_requested_model") or notion_requested),
            )
        except Exception:
            payload.setdefault(
                "resolved_model",
                str(payload.get("notion_requested_model") or requested),
            )
    resolved_route = str(
        payload.get("resolved_model")
        or payload.get("notion_requested_model")
        or notion_requested
        or requested
        or ""
    ).strip()
    if (
        route_resolution.get("resolution_kind") == "configured_alias"
        and resolved_route == str(route_resolution.get("resolved_model") or "")
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
        payload.setdefault("alias_resolution", alias_resolution)
        payload.setdefault("model_route_disposition", "alias_resolution")
    elif resolved_route:
        payload.setdefault("model_route_disposition", "direct_route")

    governance = (
        request_metadata.get("governance")
        if isinstance(request_metadata, dict)
        else None
    )
    if isinstance(governance, dict):
        payload["governance"] = {
            str(key): value
            for key, value in governance.items()
            if str(key)
            in {
                "contract_version",
                "teamspace_id",
                "authority_page_id",
                "documented_output_parent_page_id",
                "procedural_feedback_parent_page_id",
                "aligned",
            }
        }

    caller = (
        request_metadata.get("caller") if isinstance(request_metadata, dict) else None
    )
    if isinstance(caller, dict):
        payload["caller"] = {
            str(key): value
            for key, value in caller.items()
            if str(key)
            in {
                "id",
                "caller_id",
                "type",
                "caller_type",
                "project_id",
                "run_id",
                "job_id",
                "review_instance_id",
                "team_id",
                "manager_id",
                "request_origin",
            }
            and value not in (None, "", [], {})
        }

    actual = payload.get("actual_model") or payload.get("notion_model_name")
    step_model = payload.get("notion_step_model") or ""
    authoritative_sources = {
        "authoritative_upstream_metadata",
        "provider_response_header",
        "signed_provider_receipt",
    }
    source = str(payload.get("actual_model_source") or "").strip()
    authoritative_verification = bool(
        payload.get("actual_model_verified") is True
        and source in authoritative_sources
    )
    if actual:
        payload["actual_model"] = actual
        payload["actual_model_verified"] = authoritative_verification
        if not authoritative_verification:
            payload.setdefault(
                "actual_model_unverified_reason",
                "The responder model was observed but lacks authoritative verification.",
            )
            payload["actual_model_source"] = source or "notion_model_name_observation"
    elif step_model:
        payload["actual_model"] = step_model
        payload["actual_model_verified"] = False
        payload.setdefault(
            "actual_model_unverified_reason",
            "notion_step_model is an observation, not authoritative responder identity.",
        )
        payload["actual_model_source"] = source or "notion_step_model_observation"

    observed = str(payload.get("actual_model") or "").strip()
    verified = bool(payload.get("actual_model_verified") is True and observed)
    payload["route_alias"] = requested
    payload["resolved_route_model"] = resolved_route
    payload["observed_step_model"] = step_model
    payload["observed_responder_model"] = observed
    payload["model_identity_verified"] = verified
    payload["model_identity_confidence"] = (
        "verified" if verified else ("observed" if observed else "unverified")
    )
    payload["model_identity_source"] = str(
        payload.get("actual_model_source")
        or (
            "authoritative_upstream_metadata"
            if verified
            else "no_responder_identity_evidence"
        )
    )
    if verified:
        payload["verified_model"] = observed
    else:
        payload.pop("verified_model", None)
    if not verified:
        payload["model_identity_warning"] = (
            "The responding model identity was not independently verified."
        )

    resolved = resolved_route
    comparison_model = str(payload.get("verified_model") or observed or "").strip()
    if resolved and comparison_model and comparison_model != resolved:
        payload["model_substitution"] = {
            "requested_model": requested,
            "resolved_model": resolved,
            "responding_model": comparison_model,
            "verified": verified,
        }
        payload["model_route_disposition"] = (
            "verified_substitution" if verified else "unverified_route_mismatch"
        )
    else:
        payload.pop("model_substitution", None)
    return {k: v for k, v in payload.items() if v not in (None, "", [], {})}


def _attach_response_model_metadata(
    response_obj: ChatCompletionResponse,
    requested_model: str,
    model_metadata: dict[str, Any] | None,
    request_metadata: dict[str, Any] | None = None,
) -> None:
    payload = _response_model_metadata(
        requested_model, model_metadata, request_metadata=request_metadata
    )
    if not payload:
        return
    response_obj.requested_model = payload.get("requested_model")
    response_obj.notion_requested_model = payload.get("notion_requested_model")
    response_obj.actual_model = payload.get("actual_model")
    response_obj.model_metadata = payload

    # Never promote a request echo or unverified observation into the response
    # model field. Only authoritative responder evidence may replace the route.
    verified_model = payload.get("verified_model")
    if isinstance(verified_model, str) and verified_model.strip():
        response_obj.model = verified_model.strip()
    else:
        resolved = payload.get("resolved_model")
        if isinstance(resolved, str) and resolved.strip():
            response_obj.model = resolved.strip()


def _build_model_metadata_event(
    requested_model: str,
    model_metadata: dict[str, Any] | None,
    request_metadata: dict[str, Any] | None = None,
) -> str:
    payload = _response_model_metadata(
        requested_model, model_metadata, request_metadata=request_metadata
    )
    if not payload:
        return ""
    display_model = payload.get("verified_model") or payload.get("resolved_model")
    if isinstance(display_model, str) and display_model.strip():
        payload["display_model"] = display_model.strip()
    event = {"type": "model_metadata", "model_metadata": payload}
    return f"data: {json.dumps(event, ensure_ascii=False)}\n\n"


def _public_admission_receipt(client: Any) -> dict[str, Any]:
    receipt = getattr(client, "last_admission_receipt", None)
    if not isinstance(receipt, dict) or not receipt:
        return {}
    public_keys = (
        "disposition",
        "queue_depth",
        "account_queue_depth",
        "waited_seconds",
        "throttled_seconds",
        "admitted_at",
        "operation",
        "retry_count",
        "retry_after_seconds",
        "attempt_id",
        "workload_class",
        "admission_weight",
        "trace_id",
        "request_context_id",
        "model_id",
        "request_bytes",
        "estimated_input_tokens",
    )
    projected = {key: receipt[key] for key in public_keys if key in receipt}
    completion = receipt.get("completion")
    if isinstance(completion, dict) and completion:
        completion_keys = (
            "completed_at",
            "duration_seconds",
            "outcome",
            "status_code",
            "response_bytes",
            "estimated_output_tokens",
            "actual_input_tokens",
            "actual_output_tokens",
            "actual_total_tokens",
            "retry_count",
            "retry_after_seconds",
            "error_class",
        )
        projected["completion"] = {
            key: completion[key] for key in completion_keys if key in completion
        }
    return projected


def _attach_notion_thread_metadata(
    *,
    response: Response | None,
    client: Any,
    model_metadata: dict[str, Any] | None,
) -> dict[str, Any]:
    metadata = dict(model_metadata or {})
    notion_thread_id = str(getattr(client, "current_thread_id", "") or "").strip()
    if notion_thread_id:
        metadata["notion_thread_id"] = notion_thread_id
        if response is not None:
            response.headers["X-Notion-Thread-Id"] = notion_thread_id
    admission = _public_admission_receipt(client)
    if admission:
        metadata["notion_admission"] = admission
    return metadata


def _compute_missing_suffix(current_text: str, final_text: str) -> str:
    if not final_text:
        return ""
    if not current_text:
        return final_text
    if final_text.startswith(current_text):
        return final_text[len(current_text) :]
    return ""


def _select_best_final_reply(
    streamed_text: str,
    final_text: str,
    final_source_type: str,
) -> tuple[str, str]:
    streamed = streamed_text or ""
    final = final_text or ""
    streamed_stripped = streamed.strip()
    final_stripped = final.strip()
    source = (final_source_type or "").strip().lower()

    if not final_stripped:
        return streamed, "streamed_only"
    if not streamed_stripped:
        return final, "final_only"
    if detect_visible_output_contamination(
        streamed_stripped
    ) and not detect_visible_output_contamination(final_stripped):
        return final, "final_preferred_over_contaminated_stream"
    if final.startswith(streamed):
        return final, "final_extends_streamed"
    if streamed.startswith(final):
        if source == "title" or len(final_stripped) <= max(
            32, int(len(streamed_stripped) * 0.35)
        ):
            return streamed, "streamed_beats_short_final"
        return final, "final_prefix_of_streamed"

    # When the token sequence is identical and only whitespace differs, preserve
    # the exact streamed body. That is what the downstream client received, and
    # persisting a differently spaced final event creates a split-brain record.
    if "".join(streamed_stripped.split()) == "".join(final_stripped.split()):
        return streamed, "streamed_whitespace_equivalent"

    # Diverged content: usually prefer richer non-title final content.
    if source == "title" and len(final_stripped) < max(
        48, int(len(streamed_stripped) * 0.6)
    ):
        return streamed, "streamed_beats_title"
    if len(final_stripped) >= max(48, int(len(streamed_stripped) * 0.6)):
        return final, "final_diverged_preferred"
    return streamed, "streamed_diverged_preferred"


def _normalize_overlap_text(text: str) -> str:
    normalized = str(text or "").strip().lower()
    if not normalized:
        return ""
    normalized = re.sub(r"```.*?```", " ", normalized, flags=re.DOTALL)
    normalized = re.sub(r"\s+", "", normalized)
    return normalized


def _trim_redundant_thinking(
    thinking_text: str, final_reply: str
) -> tuple[str, str, float]:
    thinking = str(thinking_text or "").strip()
    final = str(final_reply or "").strip()
    if not thinking or not final:
        return thinking, "missing_text", 0.0

    normalized_thinking = _normalize_overlap_text(thinking)
    normalized_final = _normalize_overlap_text(final)
    if not normalized_thinking or not normalized_final:
        return thinking, "missing_normalized_text", 0.0

    overlap_ratio = SequenceMatcher(None, normalized_thinking, normalized_final).ratio()
    if normalized_thinking == normalized_final:
        return "", "identical", overlap_ratio

    if thinking.endswith(final):
        prefix = thinking[: -len(final)].rstrip()
        if len(_normalize_overlap_text(prefix)) >= 10:
            return prefix, "suffix_trimmed", overlap_ratio
        return "", "suffix_cleared", overlap_ratio

    if overlap_ratio >= 0.92 and (
        normalized_thinking in normalized_final
        or normalized_final in normalized_thinking
    ):
        return "", "high_overlap_cleared", overlap_ratio

    return thinking, "kept", overlap_ratio


def _build_thinking_replacement(
    streamed_content_text: str,
    thinking_text: str,
    final_reply: str,
    final_source_type: str,
) -> dict[str, Any] | None:
    source = str(final_source_type or "").strip().lower()

    # Relax constraint: Allow replacement for more source types to fix Sonnet thinking leakage
    # But still require minimal validation for non-inference sources
    if source not in ("agent-inference", "history", "markdown-chat", ""):
        # Only skip for clearly non-thinking source types
        return None

    normalized_final = _normalize_overlap_text(final_reply)
    normalized_streamed = _normalize_overlap_text(streamed_content_text)

    # Require at least some thinking content to process
    if not _normalize_overlap_text(thinking_text):
        return None

    # For non-agent-inference sources, be more conservative but still check for obvious duplication
    if source != "agent-inference":
        # Only process if there's clear overlap or thinking is redundant
        if not normalized_final:
            return None

        # Check for obvious duplication (thinking appears in final reply)
        if thinking_text.strip() in final_reply or final_reply in thinking_text:
            # Clear case of duplication - trim it
            replacement, decision, overlap_ratio = _trim_redundant_thinking(
                thinking_text, final_reply
            )
            if replacement != str(thinking_text or "").strip():
                logger.debug(
                    "Non-agent-inference thinking replacement applied",
                    extra={
                        "request_info": {
                            "event": "thinking_replacement_non_agent",
                            "source_type": source,
                            "overlap_ratio": round(overlap_ratio, 4),
                            "decision": f"{decision}_non_agent_inference",
                        }
                    },
                )
                return {
                    "thinking": replacement,
                    "decision": f"{decision}_non_agent_inference",
                    "overlap_ratio": round(overlap_ratio, 4),
                    "source_type": source,
                }
        return None

    # Original agent-inference logic continues
    if not normalized_final:
        return None

    # text
    if normalized_streamed and len(normalized_streamed) >= max(
        10, int(len(normalized_final) * 0.35)
    ):
        return None

    replacement, decision, overlap_ratio = _trim_redundant_thinking(
        thinking_text, final_reply
    )
    if replacement == str(thinking_text or "").strip():
        return None

    return {
        "thinking": replacement,
        "decision": decision,
        "overlap_ratio": round(overlap_ratio, 4),
        "source_type": source,
    }


def _contains_recall_intent(text: str) -> bool:
    lowered = text.lower()
    for keyword in RECALL_INTENT_KEYWORDS:
        if keyword.isascii():
            if keyword.lower() in lowered:
                return True
            continue
        if keyword in text:
            return True
    return False


def _extract_recall_query(text: str) -> str:
    cleaned = text
    for keyword in RECALL_INTENT_KEYWORDS:
        if keyword.isascii():
            cleaned = re.sub(
                rf"\b{re.escape(keyword)}\b", " ", cleaned, flags=re.IGNORECASE
            )
        else:
            cleaned = cleaned.replace(keyword, " ")
    cleaned = re.sub(r"[\stext,.!?;:text]+", " ", cleaned).strip()
    return cleaned or text.strip()


def _prepare_messages(
    req_body: ChatCompletionRequest,
) -> Tuple[str, List[Tuple[str, str, str]], str]:
    system_messages = []
    dialogue_messages = []

    for msg in req_body.messages:
        content_text = _message_content_to_text(msg.content)
        if msg.role == "system":
            if content_text.strip():
                system_messages.append(content_text.strip())
            continue
        dialogue_messages.append((msg.role, content_text, msg.thinking or ""))

    if not dialogue_messages:
        raise HTTPException(
            status_code=400,
            detail="The messages list must contain at least one user message.",
        )

    last_role, user_prompt, _ = dialogue_messages[-1]
    raw_user_prompt = user_prompt
    history_messages = dialogue_messages[:-1]

    if last_role != "user":
        raise HTTPException(
            status_code=400, detail="The last message must be from role 'user'."
        )
    if not user_prompt.strip():
        raise HTTPException(
            status_code=400, detail="The last user message cannot be empty."
        )

    return user_prompt, history_messages, raw_user_prompt


def _apply_request_system_instructions(
    transcript: list[dict[str, Any]], req_body: ChatCompletionRequest
) -> list[dict[str, Any]]:
    instructions = [
        _message_content_to_text(message.content).strip()
        for message in req_body.messages
        if message.role == "system"
        and _message_content_to_text(message.content).strip()
    ]
    if not instructions:
        return transcript
    merged = "\n".join(instructions)
    updated: list[dict[str, Any]] = []
    for block in transcript:
        copied = dict(block)
        value = block.get("value")
        if block.get("type") == "config" and isinstance(value, dict):
            copied_value = dict(value)
            existing = str(copied_value.get("ephemeralInstructions") or "").strip()
            copied_value["ephemeralInstructions"] = "\n".join(
                part for part in (existing, merged) if part
            )
            copied["value"] = copied_value
        updated.append(copied)
    return updated


def _prepare_messages_lite(req_body: ChatCompletionRequest) -> str:
    """Lite text user text system text"""
    system_messages = []
    user_prompt = ""

    for msg in req_body.messages:
        content_text = _message_content_to_text(msg.content)
        if msg.role == "system" and content_text.strip():
            system_messages.append(content_text.strip())
        elif msg.role == "user":
            user_prompt = content_text

    if not user_prompt.strip():
        raise HTTPException(
            status_code=400,
            detail="The messages list must contain at least one user message.",
        )

    if system_messages:
        user_prompt = (
            f"[System Instructions: {' '.join(system_messages)}]\n\n{user_prompt}"
        )

    return user_prompt


def _create_lite_stream_generator(
    response_id: str,
    model_name: str,
    first_item: Any,
    stream_gen: Iterable[Any],
    *,
    request_metadata: dict[str, Any] | None = None,
) -> Generator[str, None, None]:
    """Lite text contenttext thinking text search"""
    streamed_content_accumulator = ""
    authoritative_final_content = ""
    authoritative_final_source_type = ""
    model_metadata: dict[str, Any] = {}
    assistant_started = False

    try:
        for raw_item in _iter_stream_items(first_item, stream_gen):
            item = _normalize_stream_item(raw_item)
            item_type = item.get("type")

            model_metadata = _merge_model_metadata(model_metadata, item)
            if item_type == "model_metadata":
                continue

            if item_type == "final_content":
                final_text = _strip_visible_stream_chunk(
                    str(item.get("text", "") or "").strip()
                )
                if final_text:
                    authoritative_final_content = final_text
                    authoritative_final_source_type = str(
                        item.get("source_type", "") or ""
                    )
                continue

            # Lite text thinking text search
            if item_type in ("thinking", "search"):
                continue

            if item_type != "content":
                continue

            chunk_text = _prepare_visible_stream_chunk(
                streamed_content_accumulator,
                item.get("text", ""),
            )
            if not chunk_text:
                continue

            streamed_content_accumulator += chunk_text
            if not assistant_started:
                assistant_started = True
                yield _build_stream_chunk(
                    response_id,
                    model_name,
                    role="assistant",
                    content=chunk_text,
                )
            else:
                yield _build_stream_chunk(response_id, model_name, content=chunk_text)
    except asyncio.CancelledError:
        logger.info(
            "Lite streaming cancelled by client",
            extra={"request_info": {"event": "lite_stream_cancelled"}},
        )
        raise
    except Exception as exc:
        if _is_client_disconnect_error(exc):
            logger.info(
                "Lite streaming connection closed by client",
                extra={"request_info": {"event": "lite_stream_client_disconnected"}},
            )
            return
        logger.error(
            "Lite streaming interrupted",
            exc_info=True,
            extra={"request_info": {"event": "lite_stream_interrupted"}},
        )
        raise
    # text
    final_reply, _, hygiene_meta = _finalize_visible_reply(
        streamed_content_accumulator,
        authoritative_final_content,
        authoritative_final_source_type,
    )

    assistant_started, streamed_content_accumulator, correction_chunks = (
        _emit_visible_stream_correction(
            response_id,
            model_name,
            assistant_started=assistant_started,
            streamed_text=streamed_content_accumulator,
            sanitized_text=final_reply,
        )
    )
    for chunk in correction_chunks:
        yield chunk

    metadata_event = _build_model_metadata_event(
        model_name, model_metadata, request_metadata
    )
    if metadata_event:
        yield metadata_event

    hygiene_event = _build_hygiene_metadata_event(hygiene_meta)
    if hygiene_event:
        yield hygiene_event

    yield _build_stream_chunk(response_id, model_name, finish_reason="stop")
    yield "data: [DONE]\n\n"


def _create_standard_stream_generator(
    response_id: str,
    model_name: str,
    first_item: Any,
    stream_gen: Iterable[Any],
    *,
    client_type: str = "",
    request_metadata: dict[str, Any] | None = None,
) -> Generator[str, None, None]:
    """
    Standard text SSE text

    text
    - thinking_chunk: text
    - thinking_replace: text
    - search_metadata: text
    - choices[0].delta.content: text
    """
    streamed_content_accumulator = ""
    streamed_thinking_accumulator = ""
    collected_search_sources = []
    collected_search_queries = []
    authoritative_final_content = ""
    authoritative_final_source_type = ""
    model_metadata: dict[str, Any] = {}
    assistant_started = False

    try:
        for raw_item in _iter_stream_items(first_item, stream_gen):
            item = _normalize_stream_item(raw_item)
            item_type = item.get("type")

            model_metadata = _merge_model_metadata(model_metadata, item)
            if item_type == "model_metadata":
                continue

            if item_type == "final_content":
                final_text = _strip_visible_stream_chunk(
                    str(item.get("text", "") or "").strip()
                )
                if final_text:
                    authoritative_final_content = final_text
                    authoritative_final_source_type = str(
                        item.get("source_type", "") or ""
                    )
                continue

            # Standard text thinkingtext thinking_chunk text
            if item_type == "thinking":
                thinking_text = item.get("text", "")
                if thinking_text:
                    streamed_thinking_accumulator += thinking_text
                    # Custom local UI thinking_chunk event
                    yield f"data: {json.dumps({'type': 'thinking_chunk', 'text': thinking_text}, ensure_ascii=False)}\n\n"
                    # Standard OpenAI reasoning_content chunk
                    if not assistant_started:
                        assistant_started = True
                        yield _build_stream_chunk(
                            response_id,
                            model_name,
                            role="assistant",
                            thinking=thinking_text,
                        )
                    else:
                        yield _build_stream_chunk(
                            response_id,
                            model_name,
                            thinking=thinking_text,
                        )
                continue

            # Standard text searchtext
            if item_type == "search":
                search_data = item.get("data", {})
                if isinstance(search_data, dict):
                    # text queries text sources
                    queries = search_data.get("queries", [])
                    sources = search_data.get("sources", [])

                    if queries:
                        collected_search_queries.extend(queries)
                    if sources:
                        collected_search_sources.extend(sources)
                continue

            if item_type != "content":
                continue

            chunk_text = _prepare_visible_stream_chunk(
                streamed_content_accumulator,
                item.get("text", ""),
            )
            if not chunk_text:
                continue

            streamed_content_accumulator += chunk_text

            # text OpenAI text delta
            if not assistant_started:
                assistant_started = True
                yield _build_stream_chunk(
                    response_id,
                    model_name,
                    role="assistant",
                    content=chunk_text,
                )
            else:
                yield _build_stream_chunk(response_id, model_name, content=chunk_text)
    except asyncio.CancelledError:
        logger.info(
            "Standard streaming cancelled by client",
            extra={"request_info": {"event": "standard_stream_cancelled"}},
        )
        raise
    except Exception as exc:
        if _is_client_disconnect_error(exc):
            logger.info(
                "Standard streaming connection closed by client",
                extra={
                    "request_info": {"event": "standard_stream_client_disconnected"}
                },
            )
            return
        logger.error(
            "Standard streaming interrupted",
            exc_info=True,
            extra={"request_info": {"event": "standard_stream_interrupted"}},
        )
        raise
    # text
    final_reply, _, hygiene_meta = _finalize_visible_reply(
        streamed_content_accumulator,
        authoritative_final_content,
        authoritative_final_source_type,
    )

    assistant_started, streamed_content_accumulator, correction_chunks = (
        _emit_visible_stream_correction(
            response_id,
            model_name,
            assistant_started=assistant_started,
            streamed_text=streamed_content_accumulator,
            sanitized_text=final_reply,
            client_type=client_type,
        )
    )
    for chunk in correction_chunks:
        yield chunk

    # 输出搜索结果（使用前端定义的 search_metadata 类型；仅 web UI 客户端）
    if _emit_search_metadata_for_client(client_type) and (
        collected_search_sources or collected_search_queries
    ):
        search_metadata = {
            "type": "search_metadata",
            "searches": {
                "queries": collected_search_queries,
                "sources": collected_search_sources,
            },
        }
        yield f"data: {json.dumps(search_metadata, ensure_ascii=False)}\n\n"

    metadata_event = _build_model_metadata_event(
        model_name, model_metadata, request_metadata
    )
    if metadata_event:
        yield metadata_event

    hygiene_event = _build_hygiene_metadata_event(hygiene_meta)
    if hygiene_event:
        yield hygiene_event

    yield _build_stream_chunk(response_id, model_name, finish_reason="stop")
    yield "data: [DONE]\n\n"


def _persist_round(
    manager,
    background_tasks: BackgroundTasks,
    conversation_id: str,
    user_prompt: str,
    assistant_reply: str,
    assistant_thinking: str = "",
) -> None:
    """
    text

    text
    - text round >= WINDOW_ROUNDS//2 text
    - text BackgroundTasks text
    """
    round_index = manager.persist_round(
        conversation_id,
        user_prompt,
        assistant_reply,
        assistant_thinking=assistant_thinking,
    )

    # text
    window_rounds = 8  # text conversation.py text
    precompress_threshold = window_rounds // 2  # text 4 text

    if round_index >= precompress_threshold:
        # text
        round_to_compress = round_index - window_rounds + 1
        if round_to_compress >= 0:
            background_tasks.add_task(
                compress_sliding_window_round,
                manager=manager,
                conversation_id=conversation_id,
                round_number=round_to_compress,
            )
            logger.info(
                "Triggered async pre-compression",
                extra={
                    "request_info": {
                        "event": "async_precompress_triggered",
                        "conversation_id": conversation_id,
                        "current_round": round_index,
                        "compress_round": round_to_compress,
                    }
                },
            )

    # text
    background_tasks.add_task(
        compress_round_if_needed,
        manager=manager,
        conversation_id=conversation_id,
    )


def _persist_history_messages(
    manager, conversation_id: str, history_messages: List[Tuple[str, str, str]]
) -> None:
    for role, content, thinking in history_messages:
        manager.add_message(conversation_id, role, content, thinking)


def _is_client_disconnect_error(exc: BaseException) -> bool:
    if isinstance(exc, asyncio.CancelledError):
        return True
    if isinstance(exc, (BrokenPipeError, ConnectionResetError)):
        return True
    if isinstance(exc, OSError):
        return exc.errno in {32, 54, 104, 10053, 10054}
    return False


def _request_state_attachments(request: Request) -> list[Any]:
    attachments = getattr(request.state, "attachments", None)
    if attachments is None:
        attachments = getattr(request.state, "_attachments", None)
    return attachments if isinstance(attachments, list) else []


def _attachments_enabled_for_request(
    request: Request, policy: AttachmentPolicy
) -> bool:
    return policy.enabled or is_repo_ai_internal_request(request)


def _attachment_error_response(
    exc: AttachmentError | PromptValidationError,
) -> JSONResponse:
    return _build_error_response(
        getattr(exc, "status_code", 400) or 400,
        code=getattr(exc, "code", "invalid_attachment") or "invalid_attachment",
        message=str(exc),
        error_type="invalid_request_error",
        param=getattr(exc, "param", "attachments") or "attachments",
    )


def _handle_lite_request(
    request: Request,
    req_body: ChatCompletionRequest,
    response: Response | None = None,
) -> JSONResponse | StreamingResponse | ChatCompletionResponse:
    """text Lite text"""
    pool = request.app.state.account_pool

    req_body.model = _resolve_request_model(
        request,
        req_body.model,
        _request_workspace_selector(req_body),
    )
    assert req_body.model is not None

    # text
    cleaned_msgs, attachments = normalize_chat_messages(
        [m.model_dump() for m in req_body.messages],
        getattr(req_body, "attachments", None),
    )
    state_attachments = _request_state_attachments(request)
    if state_attachments:
        attachments = state_attachments
    # Gate feature flag
    policy = AttachmentPolicy.from_env()
    if attachments and not _attachments_enabled_for_request(request, policy):
        openai_error(
            "Attachments are disabled for this server.", "attachments_disabled"
        )

    # text
    req_body.messages = [ChatMessage(**m) for m in cleaned_msgs]
    # text
    user_prompt = _prepare_messages_lite(req_body)

    response_id = f"chatcmpl-{uuid.uuid4().hex}"
    max_retries = _outer_retry_limit(pool, req_body)

    for attempt in range(1, max_retries + 1):
        client = None
        try:
            client = _client_for_requested_workspace(pool, req_body)
            _bind_governance_request_metadata(req_body, client)

            # Read poll configuration from headers if available
            poll_interval_hdr = request.headers.get("x-notion-poll-interval")
            poll_timeout_hdr = request.headers.get("x-notion-poll-timeout")
            if poll_interval_hdr:
                try:
                    client.poll_interval = float(poll_interval_hdr)
                except ValueError:
                    pass
            if poll_timeout_hdr:
                try:
                    client.poll_timeout = float(poll_timeout_hdr)
                except ValueError:
                    pass

            # text Lite transcripttext
            transcript = _apply_notion_request_options(
                build_lite_transcript(user_prompt, req_body.model), req_body, client
            )

            # text Notion APItext thread_idtext
            persist_remote_chat = None
            if req_body.metadata and isinstance(req_body.metadata, dict):
                persist_remote_chat = req_body.metadata.get("persist_remote_chat")
            computer_use_review = _request_computer_use_review(req_body)
            thread_title = _requested_thread_title(req_body)
            _persist_local_thread_title(request, req_body, thread_title)

            stream_gen = client.stream_response(
                transcript,
                thread_id=None,
                attachments=attachments if attachments else None,
                persist_remote_chat=persist_remote_chat,
                computer_use_review=computer_use_review,
                thread_title=thread_title,
            )
            first_item = next(stream_gen, None)

            if first_item is None:
                raise NotionUpstreamError(
                    "Notion upstream returned empty content.", retriable=True
                )

            # text
            if req_body.stream:
                stream_headers = {
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                }
                return StreamingResponse(
                    _guard_stream_until_integrity(
                        _create_lite_stream_generator(
                            response_id,
                            req_body.model,
                            first_item,
                            stream_gen,
                            request_metadata=req_body.metadata,
                        ),
                        response_id=response_id,
                        model=req_body.model,
                    ),
                    media_type="text/event-stream",
                    headers=stream_headers,
                )

            # text
            content_parts: list[str] = []
            authoritative_final_content = ""
            authoritative_final_source_type = ""
            model_metadata: dict[str, Any] = {}

            for raw_item in _iter_stream_items(first_item, stream_gen):
                item = _normalize_stream_item(raw_item)
                item_type = item.get("type")

                model_metadata = _merge_model_metadata(model_metadata, item)
                if item_type == "model_metadata":
                    continue

                if item_type == "final_content":
                    final_text = _strip_visible_stream_chunk(
                        str(item.get("text", "") or "").strip()
                    )
                    if final_text:
                        authoritative_final_content = final_text
                        authoritative_final_source_type = str(
                            item.get("source_type", "") or ""
                        )
                    continue

                # Lite text thinking text search
                if item_type in ("thinking", "search"):
                    continue

                if item_type != "content":
                    continue

                chunk_text = _prepare_visible_stream_chunk(
                    "".join(content_parts),
                    item.get("text", ""),
                )
                if chunk_text:
                    content_parts.append(chunk_text)

            full_text, _, hygiene_meta = _finalize_visible_reply(
                "".join(content_parts),
                authoritative_final_content,
                authoritative_final_source_type,
            )

            if not full_text.strip():
                raise NotionUpstreamError(
                    "Notion upstream returned empty content.", retriable=True
                )

            response_text = (
                full_text if full_text.strip() else "[assistant_no_visible_content]"
            )
            response_obj = ChatCompletionResponse(
                id=response_id,
                model=req_body.model,
                choices=[
                    ChatMessageResponseChoice(
                        message=ChatMessage(role="assistant", content=response_text)
                    )
                ],
            )
            model_metadata = _attach_notion_thread_metadata(
                response=response,
                client=client,
                model_metadata=model_metadata,
            )
            _attach_response_model_metadata(
                response_obj, req_body.model, model_metadata, req_body.metadata
            )
            _attach_response_hygiene(response_obj, hygiene_meta)
            lite_thread_id = str(
                (model_metadata or {}).get("notion_thread_id")
                or getattr(client, "current_thread_id", "")
                or ""
            ).strip()
            lite_user_prompt = ""
            for message in reversed(list(req_body.messages or [])):
                if str(getattr(message, "role", "") or "").strip().lower() == "user":
                    lite_user_prompt = str(getattr(message, "content", "") or "")
                    break
            _record_live_chat_history_turn(
                client=client,
                thread_id=lite_thread_id,
                conversation_id=str(getattr(req_body, "conversation_id", "") or ""),
                user_prompt=lite_user_prompt,
                assistant_reply=full_text,
                requested_model=str(req_body.model or ""),
                model_metadata=model_metadata,
                request_metadata=req_body.metadata
                if isinstance(req_body.metadata, dict)
                else None,
            )
            if _strict_model_requested(req_body) and _is_model_mismatch(response_obj):
                return _model_mismatch_response(response_obj)
            return response_obj

        except NotionUpstreamError as exc:
            if client is not None and exc.retriable:
                pool.mark_failed(client)
            logger.warning(
                "Lite mode: Notion upstream failed",
                extra={
                    "request_info": {
                        "event": "lite_notion_upstream_failed",
                        "attempt": attempt,
                        "max_retries": max_retries,
                        "status_code": exc.status_code,
                        "retriable": exc.retriable,
                        "response_excerpt": exc.response_excerpt,
                    }
                },
            )
            if attempt == max_retries or not exc.retriable:
                return _upstream_error_response(exc)
        except RuntimeError as exc:
            logger.error(
                "Lite mode: No available client in account pool",
                extra={
                    "request_info": {
                        "event": "lite_account_pool_unavailable",
                        "detail": str(exc),
                    }
                },
            )
            return _build_error_response(
                503,
                code="POOL_COOLING",
                message=str(exc),
                error_type="account_pool_cooling",
                suggestion="Retry later.",
            )
        except AttachmentError as exc:
            logger.warning(
                "Lite mode: Invalid attachment input",
                extra={
                    "request_info": {
                        "event": "lite_invalid_attachment",
                        "code": getattr(exc, "code", "invalid_attachment"),
                        "param": getattr(exc, "param", "attachments"),
                    }
                },
            )
            return _attachment_error_response(exc)
        except HTTPException:
            raise
        except Exception:
            if client is not None:
                pool.mark_failed(client)
            logger.error(
                "Lite mode: Unhandled error",
                exc_info=True,
                extra={
                    "request_info": {
                        "event": "lite_unhandled_exception",
                        "attempt": attempt,
                    }
                },
            )
            if attempt == max_retries:
                return _build_error_response(
                    500,
                    code="INTERNAL_ERROR",
                    message="Service error.",
                    error_type="internal_error",
                    suggestion="Retry later.",
                )

    return _build_error_response(
        503,
        code="RETRIES_EXHAUSTED",
        message="Service error.",
        error_type="upstream_error",
        suggestion="Notion text",
    )


def _handle_standard_request(
    request: Request,
    req_body: ChatCompletionRequest,
    response: Response | None = None,
) -> JSONResponse | StreamingResponse | ChatCompletionResponse:
    """
    text Standard text thinking text

    text Lite text
    1. text messages text
    2. text thinking text
    3. text
    """
    from app.conversation import build_standard_transcript

    manager = getattr(request.app.state, "conversation_manager", None)
    conversation_id = str(req_body.conversation_id or "").strip()
    user_prompt, history_messages, raw_user_prompt = _prepare_messages(req_body)

    bound_thread_id = (
        _resolve_persistent_thread_id(manager, conversation_id)
        if manager and conversation_id
        else ""
    )
    persist_remote_chat = bool(
        req_body.metadata.get("persist_remote_chat", True)
        if isinstance(req_body.metadata, dict)
        else True
    )
    trusted_import = bool(
        (
            req_body.metadata.get("trusted_import")
            or req_body.metadata.get("import_mode")
        )
        if isinstance(req_body.metadata, dict)
        else False
    )
    has_assistant_or_multi_user_history = any(
        role == "assistant" for role, *_ in history_messages
    ) or (sum(1 for role, *_ in history_messages if role == "user") > 0)

    if history_messages and (
        bound_thread_id
        or (
            persist_remote_chat
            and not trusted_import
            and has_assistant_or_multi_user_history
        )
    ):
        logger.warning(
            "Rejected client history for persistent Notion thread before account acquisition",
            extra={
                "request_info": {
                    "event": "bound_thread_history_replay_rejected",
                    "error_code": "BOUND_THREAD_HISTORY_REPLAY",
                    "conversation_id": conversation_id,
                    "thread_id": bound_thread_id or "",
                    "history_message_count": len(history_messages),
                    "persist_remote_chat": persist_remote_chat,
                    "trusted_import": trusted_import,
                }
            },
        )
        return _bound_thread_history_replay_response(
            conversation_id=conversation_id,
            thread_id=bound_thread_id or "",
            history_message_count=len(history_messages),
        )

    req_body.model = _resolve_request_model(
        request,
        req_body.model,
        _request_workspace_selector(req_body),
    )
    assert req_body.model is not None

    pool = request.app.state.account_pool
    response_id = f"chatcmpl-{uuid.uuid4().hex}"
    max_retries = _outer_retry_limit(
        pool,
        req_body,
        persistent_thread=bool(bound_thread_id),
    )
    client_type = _client_type_from_request(request)

    for attempt in range(1, max_retries + 1):
        client = None
        try:
            if manager and conversation_id:
                client = _client_for_conversation(
                    pool, manager, conversation_id, req_body
                )
            else:
                client = _client_for_requested_workspace(pool, req_body)
            _bind_governance_request_metadata(req_body, client)
            if manager and conversation_id:
                _enforce_bee_notion_call_contract(
                    manager=manager,
                    conversation_id=conversation_id,
                    client=client,
                    req_body=req_body,
                )

            # Read poll configuration from headers if available
            poll_interval_hdr = request.headers.get("x-notion-poll-interval")
            poll_timeout_hdr = request.headers.get("x-notion-poll-timeout")
            if poll_interval_hdr:
                try:
                    client.poll_interval = float(poll_interval_hdr)
                except ValueError:
                    pass
            if poll_timeout_hdr:
                try:
                    client.poll_timeout = float(poll_timeout_hdr)
                except ValueError:
                    pass

            # text
            cleaned_msgs, attachments = normalize_chat_messages(
                [m.model_dump() for m in req_body.messages],
                getattr(req_body, "attachments", None),
            )
            state_attachments = _request_state_attachments(request)
            if state_attachments:
                attachments = state_attachments
            policy = AttachmentPolicy.from_env()
            if attachments and not _attachments_enabled_for_request(request, policy):
                openai_error(
                    "Attachments are disabled for this server.", "attachments_disabled"
                )

            # text Standard transcripttext
            # text client text
            account = {
                "user_id": client.user_id,
                "space_id": client.space_id,
                "timezone": getattr(client, "timezone", "America/Chicago"),
                "context_page_id": _request_context_page_id(req_body, client),
            }
            messages = cleaned_msgs
            transcript = _apply_notion_request_options(
                build_standard_transcript(messages, req_body.model, account), req_body, client
            )

            # text Notion APItext thread_idtext Notion text
            persist_remote_chat = None
            if req_body.metadata and isinstance(req_body.metadata, dict):
                persist_remote_chat = req_body.metadata.get("persist_remote_chat")
            computer_use_review = _request_computer_use_review(req_body)
            thread_title = _requested_thread_title(req_body)
            _persist_local_thread_title(request, req_body, thread_title)

            stream_gen = client.stream_response(
                transcript,
                thread_id=None,
                attachments=attachments if attachments else None,
                persist_remote_chat=persist_remote_chat,
                computer_use_review=computer_use_review,
                thread_title=thread_title,
            )
            first_item = next(stream_gen, None)

            if first_item is None:
                raise NotionUpstreamError(
                    "Notion upstream returned empty content.", retriable=True
                )

            # text
            if req_body.stream:
                stream_headers = {
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                }
                return StreamingResponse(
                    _guard_stream_until_integrity(
                        _create_standard_stream_generator(
                            response_id,
                            req_body.model,
                            first_item,
                            stream_gen,
                            client_type=client_type,
                            request_metadata=req_body.metadata,
                        ),
                        response_id=response_id,
                        model=req_body.model,
                    ),
                    media_type="text/event-stream",
                    headers=stream_headers,
                )

            # text
            content_parts: list[str] = []
            thinking_parts: list[str] = []
            search_results: list[dict] = []
            authoritative_final_content = ""
            authoritative_final_source_type = ""
            model_metadata: dict[str, Any] = {}

            for raw_item in _iter_stream_items(first_item, stream_gen):
                item = _normalize_stream_item(raw_item)
                item_type = item.get("type")

                model_metadata = _merge_model_metadata(model_metadata, item)
                if item_type == "model_metadata":
                    continue

                if item_type == "final_content":
                    final_text = _strip_visible_stream_chunk(
                        str(item.get("text", "") or "").strip()
                    )
                    if final_text:
                        authoritative_final_content = final_text
                        authoritative_final_source_type = str(
                            item.get("source_type", "") or ""
                        )
                    continue

                # Standard text thinking
                if item_type == "thinking":
                    thinking_text = item.get("text", "")
                    if thinking_text:
                        thinking_parts.append(thinking_text)
                    continue

                # Standard text search
                if item_type == "search":
                    search_data = item.get("data", {})
                    if search_data:
                        search_results.append(search_data)
                    continue

                if item_type != "content":
                    continue

                chunk_text = _prepare_visible_stream_chunk(
                    "".join(content_parts),
                    item.get("text", ""),
                )
                if chunk_text:
                    content_parts.append(chunk_text)

            full_text, _, hygiene_meta = _finalize_visible_reply(
                "".join(content_parts),
                authoritative_final_content,
                authoritative_final_source_type,
            )

            if not full_text.strip():
                raise NotionUpstreamError(
                    "Notion upstream returned empty content.", retriable=True
                )

            response_text = (
                full_text if full_text.strip() else "[assistant_no_visible_content]"
            )

            # text
            response_message = ChatMessage(role="assistant", content=response_text)

            # text thinkingtext
            if thinking_parts:
                thinking_val = "".join(thinking_parts)
                response_message.thinking = thinking_val
                response_message.reasoning_content = thinking_val

            # text
            response_obj = ChatCompletionResponse(
                id=response_id,
                model=req_body.model,
                choices=[ChatMessageResponseChoice(message=response_message)],
            )
            model_metadata = _attach_notion_thread_metadata(
                response=response,
                client=client,
                model_metadata=model_metadata,
            )
            _attach_response_model_metadata(
                response_obj, req_body.model, model_metadata, req_body.metadata
            )
            _attach_response_hygiene(response_obj, hygiene_meta)

            # text
            if search_results:
                # text queries text sources
                all_queries = []
                all_sources = []
                for result in search_results:
                    if isinstance(result, dict):
                        all_queries.extend(result.get("queries", []))
                        all_sources.extend(result.get("sources", []))

                if all_queries or all_sources:
                    # text
                    response_obj.search_metadata = {
                        "queries": all_queries,
                        "sources": all_sources,
                    }

            if _strict_model_requested(req_body) and _is_model_mismatch(response_obj):
                return _model_mismatch_response(response_obj)
            return response_obj

        except NotionUpstreamError as exc:
            if client is not None and exc.retriable:
                pool.mark_failed(client)
            logger.warning(
                "Standard mode: Notion upstream failed",
                extra={
                    "request_info": {
                        "event": "standard_notion_upstream_failed",
                        "attempt": attempt,
                        "max_retries": max_retries,
                        "status_code": exc.status_code,
                        "retriable": exc.retriable,
                        "response_excerpt": exc.response_excerpt,
                    }
                },
            )
            if attempt == max_retries or not exc.retriable:
                return _upstream_error_response(exc)
        except RuntimeError as exc:
            logger.error(
                "Standard mode: No available client in account pool",
                extra={
                    "request_info": {
                        "event": "standard_account_pool_unavailable",
                        "detail": str(exc),
                    }
                },
            )
            return _build_error_response(
                503,
                code="POOL_COOLING",
                message=str(exc),
                error_type="account_pool_cooling",
                suggestion="Retry later.",
            )
        except AttachmentError as exc:
            logger.warning(
                "Standard mode: Invalid attachment input",
                extra={
                    "request_info": {
                        "event": "standard_invalid_attachment",
                        "code": getattr(exc, "code", "invalid_attachment"),
                        "param": getattr(exc, "param", "attachments"),
                    }
                },
            )
            return _attachment_error_response(exc)
        except HTTPException:
            raise
        except Exception:
            if client is not None:
                pool.mark_failed(client)
            logger.error(
                "Standard mode: Unhandled error",
                exc_info=True,
                extra={
                    "request_info": {
                        "event": "standard_unhandled_exception",
                        "attempt": attempt,
                    }
                },
            )
            if attempt == max_retries:
                return _build_error_response(
                    500,
                    code="INTERNAL_ERROR",
                    message="Service error.",
                    error_type="internal_error",
                    suggestion="Retry later.",
                )

    return _build_error_response(
        503,
        code="RETRIES_EXHAUSTED",
        message="Service error.",
        error_type="upstream_error",
        suggestion="Notion text",
    )


@router.post("/chat/completions", tags=["chat"])
async def create_chat_completion(
    request: Request,
    req_body: ChatCompletionRequest,
    background_tasks: BackgroundTasks,
    response: Response,
):
    """
    text OpenAI APItext

    text
    - Lite text30/text
    - Standard text25/text thinking text
    - Heavy text20/text
    """
    from app.config import is_standard_mode

    try:
        validate_chat_messages(
            [message.model_dump() for message in req_body.messages]
        )
    except PromptValidationError as exc:
        return _attachment_error_response(exc)

    # Resolve attachment policy and inline-data validity before selecting a
    # mode, acquiring an account, or creating a remote Notion thread.
    try:
        _preflight_messages, preflight_attachments = normalize_chat_messages(
            [message.model_dump() for message in req_body.messages],
            getattr(req_body, "attachments", None),
        )
        state_attachments = _request_state_attachments(request)
        if state_attachments:
            preflight_attachments = state_attachments
        policy = AttachmentPolicy.from_env()
        if preflight_attachments and not _attachments_enabled_for_request(
            request, policy
        ):
            openai_error(
                "Attachments are disabled for this server.",
                "attachments_disabled",
            )
        validate_inline_attachment_data(preflight_attachments)
        if preflight_attachments:
            request.state.attachments = preflight_attachments
    except PromptValidationError as exc:
        return _attachment_error_response(exc)

    # Check if this is an OpenCode call
    user_agent = request.headers.get("user-agent", "").lower()
    x_client_name = request.headers.get("x-client-name", "").lower()
    is_opencode = "opencode" in user_agent or x_client_name == "opencode"

    if is_opencode:
        custom_instructions = (
            "If concrete artifacts are provided:\n"
            "\tAnalyze the supplied artifacts.\n"
            "\tIdentify the likely root cause.\n"
            "\tProvide a minimal fix or patch.\n"
            "\tInclude verification steps.\n"
            "\n"
            "If artifacts are incomplete:\n"
            "\tState the most likely interpretation.\n"
            "\tExplain what cannot be determined.\n"
            "\tRequest the smallest missing input needed.\n"
            "\n"
            "If the user asks what the assistant can do:\n"
            "\tDescribe capabilities in terms of the API/client workflow.\n"
            "\tDo not mention missing native access unless asked.\n"
            "\n"
            "If tools are exposed by the client:\n"
            "\tUse the provided tool protocol.\n"
            "\tDo not claim independent access outside that protocol."
        )

        # Check if there is already a system message
        system_msg = None
        for msg in req_body.messages:
            if msg.role == "system":
                system_msg = msg
                break

        if system_msg:
            # Prefix the system message content
            if isinstance(system_msg.content, str):
                system_msg.content = f"{custom_instructions}\n\n{system_msg.content}"
            elif isinstance(system_msg.content, list):
                system_msg.content.insert(
                    0, {"type": "text", "text": custom_instructions + "\n\n"}
                )
            else:
                system_msg.content = custom_instructions
        else:
            # No system message exists, insert one at the beginning
            new_system_msg = ChatMessage(role="system", content=custom_instructions)
            req_body.messages.insert(0, new_system_msg)

    req_body.model = _resolve_request_model(
        request,
        req_body.model,
        _request_workspace_selector(req_body),
    )
    assert req_body.model is not None

    # Check for local smoke/preflight messages to avoid creating new chats in Notion.
    if req_body.messages:
        last_user_content = _last_user_message_content(req_body.messages)
        probe_response = _local_probe_response_text(last_user_content)
        if probe_response:
            response_id = f"chatcmpl-{uuid.uuid4().hex}"
            if req_body.stream:

                def ping_stream_generator() -> Generator[str, None, None]:
                    yield _build_stream_chunk(
                        response_id, req_body.model, role="assistant"
                    )
                    yield _build_stream_chunk(
                        response_id, req_body.model, content=probe_response
                    )
                    yield _build_stream_chunk(
                        response_id, req_body.model, finish_reason="stop"
                    )
                    yield "data: [DONE]\n\n"

                stream_headers = {
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                }
                return StreamingResponse(
                    ping_stream_generator(),
                    media_type="text/event-stream",
                    headers=stream_headers,
                )
            else:
                return ChatCompletionResponse(
                    id=response_id,
                    model=req_body.model,
                    choices=[
                        ChatMessageResponseChoice(
                            message=ChatMessage(
                                role="assistant", content=probe_response
                            )
                        )
                    ],
                )

    if not _sandbox_remote_request_authorized(request, req_body):
        return _build_error_response(
            403,
            code="SANDBOX_REMOTE_AUTH_REQUIRED",
            message="Sandbox remote-provider access requires explicit request opt-in.",
            error_type="sandbox_remote_access_denied",
            suggestion=(
                "Set X-Sandbox-Allow-Remote: true or metadata.sandbox_allow_remote=true "
                "for an intentional credentialed integration test."
            ),
        )

    # Lite text
    if is_lite_mode():
        import anyio

        return await anyio.to_thread.run_sync(
            _handle_lite_request, request, req_body, response
        )

    # Standard text thinking text
    if is_standard_mode():
        import anyio

        return await anyio.to_thread.run_sync(
            _handle_standard_request, request, req_body, response
        )

    # Heavy text
    pool = request.app.state.account_pool
    manager = request.app.state.conversation_manager

    user_prompt, history_messages, raw_user_prompt = _prepare_messages(req_body)
    recall_query = (
        _extract_recall_query(raw_user_prompt)
        if _contains_recall_intent(raw_user_prompt)
        else None
    )

    conversation_id = (
        req_body.conversation_id.strip() if req_body.conversation_id else ""
    )
    restore_history = False
    if not conversation_id:
        conversation_id = manager.new_conversation(
            title=_requested_thread_title(req_body)
        )
        restore_history = True
    elif not manager.conversation_exists(conversation_id):
        logger.warning(
            "Conversation id not found, creating a fresh conversation",
            extra={
                "request_info": {
                    "event": "conversation_id_not_found",
                    "provided_conversation_id": conversation_id,
                }
            },
        )
        conversation_id = manager.new_conversation(
            title=_requested_thread_title(req_body),
            conversation_id=conversation_id,
        )
    bound_thread_id = _resolve_persistent_thread_id(manager, conversation_id)

    persist_remote_chat = bool(
        req_body.metadata.get("persist_remote_chat", True)
        if isinstance(req_body.metadata, dict)
        else True
    )
    trusted_import = bool(
        (
            req_body.metadata.get("trusted_import")
            or req_body.metadata.get("import_mode")
        )
        if isinstance(req_body.metadata, dict)
        else False
    )

    has_assistant_or_multi_user_history = any(
        role == "assistant" for role, *_ in history_messages
    ) or (sum(1 for role, *_ in history_messages if role == "user") > 0)

    # Persistent remote-chat workflows prohibit client-supplied historical dialog
    # before provider dispatch (whether the thread is already bound or newly created),
    # unless an explicit trusted import mode is enabled.
    if history_messages and (
        bound_thread_id
        or (
            persist_remote_chat
            and not trusted_import
            and has_assistant_or_multi_user_history
        )
    ):
        logger.warning(
            "Rejected client history for persistent Notion thread",
            extra={
                "request_info": {
                    "event": "bound_thread_history_replay_rejected",
                    "error_code": "BOUND_THREAD_HISTORY_REPLAY",
                    "conversation_id": conversation_id,
                    "thread_id": bound_thread_id or "",
                    "history_message_count": len(history_messages),
                    "persist_remote_chat": persist_remote_chat,
                    "trusted_import": trusted_import,
                }
            },
        )
        return _bound_thread_history_replay_response(
            conversation_id=conversation_id,
            thread_id=bound_thread_id or "",
            history_message_count=len(history_messages),
        )
    elif history_messages:
        with manager._get_conn() as conn:
            existing_count = manager._count_messages(conn, conversation_id)
            history_count = len(history_messages)
            if history_count > existing_count:
                _persist_history_messages(manager, conversation_id, history_messages)
                restored_user_count = sum(
                    1 for role, *_ in history_messages if role == "user"
                )
                restored_assistant_count = sum(
                    1 for role, *_ in history_messages if role == "assistant"
                )

                logger.info(
                    "Restored history into unbound conversation",
                    extra={
                        "request_info": {
                            "event": "conversation_history_restored",
                            "conversation_id": conversation_id,
                            "restore_history_flag": restore_history,
                            "existing_count": existing_count,
                            "history_count": history_count,
                            "restored_total": len(history_messages),
                            "restored_user_count": restored_user_count,
                            "restored_assistant_count": restored_assistant_count,
                        }
                    },
                )

    response_id = f"chatcmpl-{uuid.uuid4().hex}"
    max_retries = _outer_retry_limit(
        pool,
        req_body,
        persistent_thread=bool(bound_thread_id),
    )

    for attempt in range(1, max_retries + 1):
        client = None
        try:
            client = _client_for_conversation(
                pool, manager, conversation_id, req_body
            )
            _bind_governance_request_metadata(req_body, client)
            _enforce_bee_notion_call_contract(
                manager=manager,
                conversation_id=conversation_id,
                client=client,
                req_body=req_body,
            )
            transcript_payload = manager.get_transcript_payload(
                notion_client=client,
                conversation_id=conversation_id,
                new_prompt=user_prompt,
                model_name=req_body.model,
                recall_query=None if bound_thread_id else recall_query,
                context_page_id=_request_context_page_id(req_body, client),
            )
            transcript = transcript_payload["transcript"]
            transcript = _apply_request_system_instructions(transcript, req_body)
            transcript = _apply_notion_request_options(transcript, req_body, client)
            memory_degraded = bool(transcript_payload.get("memory_degraded"))
            memory_headers = {"X-Memory-Status": "degraded"} if memory_degraded else {}

            # Bound conversations always continue the existing remote-native thread.
            thread_id = bound_thread_id

            # Pass attachments when present
            _cleaned_msgs, attachments = normalize_chat_messages(
                [m.model_dump() for m in req_body.messages],
                getattr(req_body, "attachments", None),
            )
            state_attachments = _request_state_attachments(request)
            if state_attachments:
                attachments = state_attachments
            if attachments and not _attachments_enabled_for_request(
                request, AttachmentPolicy.from_env()
            ):
                openai_error(
                    "Attachments are disabled for this server.", "attachments_disabled"
                )

            persist_remote_chat = None
            if req_body.metadata and isinstance(req_body.metadata, dict):
                persist_remote_chat = req_body.metadata.get("persist_remote_chat")
            computer_use_review = _request_computer_use_review(req_body)
            thread_title = _requested_thread_title(req_body)
            if thread_title:
                manager.set_conversation_title(conversation_id, thread_title)

            stream_gen = client.stream_response(
                transcript,
                thread_id=thread_id,
                attachments=attachments if attachments else None,
                persist_remote_chat=persist_remote_chat,
                computer_use_review=computer_use_review,
                thread_title=thread_title,
            )
            first_item = next(stream_gen, None)

            # text thread_idtext
            if not thread_id and hasattr(client, "current_thread_id"):
                manager.set_conversation_thread_id(
                    conversation_id,
                    client.current_thread_id,
                    model_name=req_body.model,
                )

            if first_item is None:
                raise NotionUpstreamError(
                    "Notion upstream returned empty content.", retriable=True
                )

            first_stream_item = first_item
            active_stream_gen = stream_gen
            attempt_no = attempt
            active_client = client

            def openai_stream_generator(
                first_stream_item: Any = first_stream_item,
                active_stream_gen: Any = active_stream_gen,
                attempt_no: int = attempt_no,
                active_client: Any = active_client,
            ) -> Generator[str, None, None]:
                streamed_content_accumulator = ""
                thinking_accumulator = ""
                authoritative_final_content = ""
                authoritative_final_source_type = ""
                assistant_started = False
                pending_search_md = ""
                client_type = request.headers.get("X-Client-Type", "").lower()
                recent_thinking_buffer: list[str] = []
                model_metadata: dict[str, Any] = {}

                try:
                    for raw_item in _iter_stream_items(
                        first_stream_item, active_stream_gen
                    ):
                        item = _normalize_stream_item(raw_item)
                        item_type = item.get("type")

                        model_metadata = _merge_model_metadata(model_metadata, item)
                        if item_type == "model_metadata":
                            continue

                        if item_type == "search":
                            search_data = item.get("data")
                            if isinstance(search_data, dict) and search_data:
                                pending_search_md += _format_search_results_md(
                                    search_data
                                )
                                if client_type == "web":
                                    yield _build_local_ui_chunk(
                                        response_id,
                                        req_body.model,
                                        "search_metadata",
                                        searches=search_data,
                                    )
                            continue

                        if item_type == "final_content":
                            final_text = _strip_visible_stream_chunk(
                                str(item.get("text", "") or "").strip()
                            )
                            if final_text:
                                authoritative_final_content = final_text
                                authoritative_final_source_type = str(
                                    item.get("source_type", "") or ""
                                )
                            continue

                        if item_type == "thinking":
                            thinking_text = item.get("text", "")
                            if thinking_text:
                                thinking_accumulator += thinking_text
                                # Track recent thinking for overlap detection
                                recent_thinking_buffer.append(thinking_text)
                                # Keep buffer manageable (max 40 recent chunks)
                                if len(recent_thinking_buffer) > 40:
                                    recent_thinking_buffer.pop(0)

                                if not assistant_started:
                                    assistant_started = True
                                    yield _build_stream_chunk(
                                        response_id,
                                        req_body.model,
                                        role="assistant",
                                        thinking=thinking_text,
                                    )
                                else:
                                    yield _build_stream_chunk(
                                        response_id,
                                        req_body.model,
                                        thinking=thinking_text,
                                    )
                            continue

                        if item_type != "content":
                            continue

                        chunk_text = _prepare_visible_stream_chunk(
                            streamed_content_accumulator,
                            item.get("text", ""),
                        )
                        if not chunk_text and not pending_search_md:
                            continue

                        # Check if content overlaps with recent thinking (prevents thinking leakage)
                        if recent_thinking_buffer and chunk_text.strip():
                            combined_recent_thinking = "".join(recent_thinking_buffer)
                            chunk_normalized = chunk_text.strip()

                            # Use normalized text without spaces for robust comparison
                            combined_norm = re.sub(r"\s+", "", combined_recent_thinking)
                            chunk_norm = re.sub(r"\s+", "", chunk_normalized)

                            # Check for significant overlap - skip duplicate content
                            # We only skip if a sufficiently long chunk matches to avoid swallowing short common characters.
                            if (
                                chunk_norm
                                and len(chunk_norm) > 3
                                and (
                                    chunk_norm in combined_norm
                                    or (
                                        len(chunk_norm) > 10
                                        and chunk_norm[:10] in combined_norm
                                    )
                                )
                            ):
                                # Skip this chunk as it's likely duplicated thinking content
                                logger.debug(
                                    "Skipping duplicate content chunk that overlaps with thinking",
                                    extra={
                                        "request_info": {
                                            "event": "content_overlap_with_thinking",
                                            "chunk_length": len(chunk_text),
                                            "overlap_detected": True,
                                        }
                                    },
                                )
                                continue

                        # text
                        if pending_search_md and client_type != "web":
                            chunk_text = pending_search_md + chunk_text

                        if pending_search_md:
                            pending_search_md = ""

                        streamed_content_accumulator += chunk_text
                        if not assistant_started:
                            assistant_started = True
                            yield _build_stream_chunk(
                                response_id,
                                req_body.model,
                                role="assistant",
                                content=chunk_text,
                            )
                        else:
                            yield _build_stream_chunk(
                                response_id, req_body.model, content=chunk_text
                            )
                except asyncio.CancelledError:
                    logger.info(
                        "Streaming response cancelled by downstream client",
                        extra={
                            "request_info": {
                                "event": "stream_cancelled_by_client",
                                "conversation_id": conversation_id,
                                "attempt": attempt_no,
                            }
                        },
                    )
                    raise
                except Exception as exc:
                    if _is_client_disconnect_error(exc):
                        logger.info(
                            "Streaming connection closed by downstream client",
                            extra={
                                "request_info": {
                                    "event": "stream_client_disconnected",
                                    "conversation_id": conversation_id,
                                    "attempt": attempt_no,
                                }
                            },
                        )
                        return
                    if (
                        isinstance(exc, NotionUpstreamError)
                        and active_client is not None
                        and getattr(exc, "retriable", False)
                    ):
                        pool.mark_failed(active_client)
                    log_method = (
                        logger.warning
                        if isinstance(exc, NotionUpstreamError)
                        else logger.error
                    )
                    log_method(
                        "Streaming response interrupted",
                        exc_info=True,
                        extra={
                            "request_info": {
                                "event": "stream_interrupted",
                                "conversation_id": conversation_id,
                                "attempt": attempt_no,
                                "is_upstream_error": isinstance(
                                    exc, NotionUpstreamError
                                ),
                            }
                        },
                    )
                    error_hint = "\n\n[Upstream connection interrupted. Retry later.]"
                    streamed_content_accumulator += error_hint
                    if not assistant_started:
                        assistant_started = True
                        yield _build_stream_chunk(
                            response_id,
                            req_body.model,
                            role="assistant",
                            content=error_hint,
                        )
                    else:
                        yield _build_stream_chunk(
                            response_id, req_body.model, content=error_hint
                        )
                finally:
                    final_reply, reply_decision, hygiene_meta = _finalize_visible_reply(
                        streamed_content_accumulator,
                        authoritative_final_content,
                        authoritative_final_source_type,
                    )

                    missing_suffix = _compute_missing_suffix(
                        streamed_content_accumulator, final_reply
                    )
                    if missing_suffix:
                        suffix_to_emit = missing_suffix
                        if (
                            pending_search_md
                            and client_type != "web"
                            and not streamed_content_accumulator
                        ):
                            suffix_to_emit = pending_search_md + suffix_to_emit
                            pending_search_md = ""
                        if not assistant_started:
                            assistant_started = True
                            yield _build_stream_chunk(
                                response_id,
                                req_body.model,
                                role="assistant",
                                content=suffix_to_emit,
                            )
                        else:
                            yield _build_stream_chunk(
                                response_id, req_body.model, content=suffix_to_emit
                            )
                        streamed_content_accumulator += suffix_to_emit
                    elif final_reply != streamed_content_accumulator:
                        # Diverged bodies cannot be safely "patched" in plain OpenAI deltas.
                        # Web client supports replace event to keep rendered body aligned with persisted final reply.
                        if client_type == "web":
                            yield _build_local_ui_chunk(
                                response_id,
                                req_body.model,
                                "content_replace",
                                content=final_reply,
                                source_type=authoritative_final_source_type,
                                decision=reply_decision,
                            )
                            streamed_content_accumulator = final_reply
                        elif not streamed_content_accumulator and final_reply:
                            # Non-web fallback when nothing has been shown yet.
                            emit_text = final_reply
                            if pending_search_md and client_type != "web":
                                emit_text = pending_search_md + emit_text
                                pending_search_md = ""
                            if not assistant_started:
                                assistant_started = True
                                yield _build_stream_chunk(
                                    response_id,
                                    req_body.model,
                                    role="assistant",
                                    content=emit_text,
                                )
                            else:
                                yield _build_stream_chunk(
                                    response_id, req_body.model, content=emit_text
                                )
                            streamed_content_accumulator = final_reply

                    (
                        assistant_started,
                        streamed_content_accumulator,
                        correction_chunks,
                    ) = _emit_visible_stream_correction(
                        response_id,
                        req_body.model,
                        assistant_started=assistant_started,
                        streamed_text=streamed_content_accumulator,
                        sanitized_text=final_reply,
                        client_type=client_type,
                    )
                    for chunk in correction_chunks:
                        yield chunk

                    thinking_replacement = _build_thinking_replacement(
                        streamed_content_accumulator,
                        thinking_accumulator,
                        final_reply,
                        authoritative_final_source_type,
                    )
                    if client_type == "web" and thinking_replacement is not None:
                        yield _build_local_ui_chunk(
                            response_id,
                            req_body.model,
                            "thinking_replace",
                            thinking=thinking_replacement["thinking"],
                            decision=thinking_replacement["decision"],
                            overlap_ratio=thinking_replacement["overlap_ratio"],
                            source_type=thinking_replacement["source_type"],
                            reply_decision=reply_decision,
                        )

                    persisted_thinking = (
                        str(thinking_replacement["thinking"])
                        if thinking_replacement is not None
                        else thinking_accumulator
                    )
                    quarantined = _output_requires_quarantine(hygiene_meta)
                    if quarantined:
                        logger.error(
                            "Quarantined contaminated streaming response",
                            extra={
                                "request_info": {
                                    "event": "output_quarantined",
                                    "error_code": "OUTPUT_CONTAMINATED",
                                    "conversation_id": conversation_id,
                                    "output_integrity": hygiene_meta.get(
                                        "output_integrity"
                                    ),
                                    "normal_persistence_blocked": True,
                                }
                            },
                        )
                    elif final_reply.strip() or persisted_thinking.strip():
                        try:
                            _persist_round(
                                manager,
                                background_tasks,
                                conversation_id,
                                user_prompt,
                                final_reply,
                                persisted_thinking,
                            )
                        except Exception:
                            logger.error(
                                "Failed to persist conversation round",
                                exc_info=True,
                                extra={
                                    "request_info": {
                                        "event": "conversation_persist_failed",
                                        "conversation_id": conversation_id,
                                    }
                                },
                            )
                    active_thread_id = str(
                        getattr(client, "current_thread_id", "") or thread_id or ""
                    ).strip()
                    if active_thread_id:
                        model_metadata = dict(model_metadata or {})
                        model_metadata["notion_thread_id"] = active_thread_id
                        model_metadata["remote_chat_id"] = active_thread_id
                    if (final_reply.strip() or persisted_thinking.strip()) and not quarantined:
                        _record_live_chat_history_turn(
                            client=client,
                            thread_id=active_thread_id,
                            conversation_id=conversation_id,
                            user_prompt=user_prompt,
                            assistant_reply=final_reply,
                            requested_model=str(req_body.model or ""),
                            model_metadata=model_metadata,
                            request_metadata=req_body.metadata
                            if isinstance(req_body.metadata, dict)
                            else None,
                        )

                    metadata_event = _build_model_metadata_event(
                        req_body.model, model_metadata, req_body.metadata
                    )
                    if metadata_event:
                        yield metadata_event

                    hygiene_event = _build_hygiene_metadata_event(hygiene_meta)
                    if hygiene_event:
                        yield hygiene_event

                    yield _build_stream_chunk(
                        response_id,
                        req_body.model,
                        finish_reason="content_filter" if quarantined else "stop",
                    )
                    yield "data: [DONE]\n\n"

            if req_body.stream:
                active_thread_id = str(
                    thread_id or getattr(client, "current_thread_id", None) or ""
                ).strip()
                stream_headers = {
                    "Cache-Control": "no-cache",
                    "Connection": "keep-alive",
                    "X-Accel-Buffering": "no",
                    "X-Conversation-Id": conversation_id,
                    **memory_headers,
                }
                if active_thread_id:
                    stream_headers["X-Notion-Thread-Id"] = active_thread_id
                return StreamingResponse(
                    _guard_stream_until_integrity(
                        openai_stream_generator(),
                        response_id=response_id,
                        model=req_body.model,
                    ),
                    media_type="text/event-stream",
                    headers=stream_headers,
                )

            content_parts: list[str] = []
            thinking_parts: list[str] = []
            authoritative_final_content = ""
            authoritative_final_source_type = ""
            model_metadata: dict[str, Any] = {}
            for raw_item in _iter_stream_items(first_item, stream_gen):
                item = _normalize_stream_item(raw_item)
                item_type = item.get("type")
                model_metadata = _merge_model_metadata(model_metadata, item)
                if item_type == "model_metadata":
                    continue
                if item_type == "final_content":
                    final_text = _strip_visible_stream_chunk(
                        str(item.get("text", "") or "").strip()
                    )
                    if final_text:
                        authoritative_final_content = final_text
                        authoritative_final_source_type = str(
                            item.get("source_type", "") or ""
                        )
                    continue
                if item_type == "thinking":
                    thinking_text = str(item.get("text", "") or "")
                    if thinking_text:
                        thinking_parts.append(thinking_text)
                    continue
                if item_type != "content":
                    continue
                chunk_text = _prepare_visible_stream_chunk(
                    "".join(content_parts),
                    item.get("text", ""),
                )
                if chunk_text:
                    content_parts.append(chunk_text)

            full_text, _, hygiene_meta = _finalize_visible_reply(
                "".join(content_parts),
                authoritative_final_content,
                authoritative_final_source_type,
            )
            merged_thinking = "".join(thinking_parts).strip()
            if not full_text.strip() and not merged_thinking:
                raise NotionUpstreamError(
                    "Notion upstream returned empty content.", retriable=True
                )
            if _output_requires_quarantine(hygiene_meta):
                _raise_output_contaminated(hygiene_meta)

            _persist_round(
                manager,
                background_tasks,
                conversation_id,
                user_prompt,
                full_text,
                merged_thinking,
            )
            response.headers["X-Conversation-Id"] = conversation_id
            if memory_degraded:
                response.headers["X-Memory-Status"] = "degraded"

            notion_thread_id = str(
                getattr(active_client, "current_thread_id", "") or ""
            ).strip()
            if notion_thread_id:
                model_metadata = dict(model_metadata or {})
                model_metadata["notion_thread_id"] = notion_thread_id
                response.headers["X-Notion-Thread-Id"] = notion_thread_id
            _record_live_chat_history_turn(
                client=active_client,
                thread_id=notion_thread_id,
                conversation_id=conversation_id,
                user_prompt=user_prompt,
                assistant_reply=full_text,
                requested_model=str(req_body.model or ""),
                model_metadata=model_metadata,
                request_metadata=req_body.metadata
                if isinstance(req_body.metadata, dict)
                else None,
            )

            response_text = (
                full_text if full_text.strip() else "[assistant_no_visible_content]"
            )
            response_message = ChatMessage(role="assistant", content=response_text)
            if merged_thinking:
                response_message.thinking = merged_thinking
                response_message.reasoning_content = merged_thinking
            response_obj = ChatCompletionResponse(
                id=response_id,
                model=req_body.model,
                choices=[ChatMessageResponseChoice(message=response_message)],
            )
            _attach_response_model_metadata(
                response_obj, req_body.model, model_metadata, req_body.metadata
            )
            _attach_response_hygiene(response_obj, hygiene_meta)
            return response_obj
        except NotionUpstreamError as exc:
            if client is not None and exc.retriable:
                pool.mark_failed(client)
            logger.warning(
                "Notion upstream failed",
                extra={
                    "request_info": {
                        "event": "notion_upstream_failed",
                        "attempt": attempt,
                        "max_retries": max_retries,
                        "conversation_id": conversation_id,
                        "status_code": exc.status_code,
                        "retriable": exc.retriable,
                        "response_excerpt": exc.response_excerpt,
                    }
                },
            )
            if attempt == max_retries or not exc.retriable:
                return _upstream_error_response(exc)
        except RuntimeError as exc:
            logger.error(
                "No available client in account pool",
                extra={
                    "request_info": {
                        "event": "account_pool_unavailable",
                        "detail": str(exc),
                    }
                },
            )
            return _build_error_response(
                503,
                code="POOL_COOLING",
                message=str(exc),
                error_type="account_pool_cooling",
                suggestion="Retry later.",
            )
        except AttachmentError as exc:
            logger.warning(
                "Invalid attachment input",
                extra={
                    "request_info": {
                        "event": "chat_completion_invalid_attachment",
                        "code": getattr(exc, "code", "invalid_attachment"),
                        "param": getattr(exc, "param", "attachments"),
                    }
                },
            )
            return _attachment_error_response(exc)
        except HTTPException:
            raise
        except Exception:
            if client is not None:
                pool.mark_failed(client)
            logger.error(
                "Unhandled chat completion error",
                exc_info=True,
                extra={
                    "request_info": {
                        "event": "chat_completion_unhandled_exception",
                        "attempt": attempt,
                        "conversation_id": conversation_id,
                    }
                },
            )
            if attempt == max_retries:
                return _build_error_response(
                    500,
                    code="INTERNAL_ERROR",
                    message="Service error.",
                    error_type="internal_error",
                    suggestion="Retry later.",
                )

    return _build_error_response(
        503,
        code="RETRIES_EXHAUSTED",
        message="Service error.",
        error_type="upstream_error",
        suggestion="Notion text",
    )


@router.delete("/conversations/{conversation_id}", tags=["chat"])
async def delete_conversation(conversation_id: str, request: Request):
    """
    Delete a conversation by its ID.
    """
    manager = request.app.state.conversation_manager
    deleted = manager.delete_conversation(conversation_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Conversation not found.")
    return {"id": conversation_id, "deleted": True}
