from __future__ import annotations

from typing import Any, TYPE_CHECKING
import os
import threading
import time
import uuid

from app.chat_history.extractor import collect_hydration_message_ids
from app.chat_history.har_importer import import_chat_object
from app.chat_history.notion_sync import HYDRATE_ENDPOINT, _post_json
from app.logger import logger

if TYPE_CHECKING:
    from app.notion_client import NotionOpusAPI


_PENDING_TTL_SECONDS = 15 * 60
_PENDING_LOCK = threading.RLock()
_PENDING_BY_THREAD: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
_PENDING_UPDATED_AT: dict[tuple[str, str], float] = {}

# Deduplicate auto-approval submissions per account/thread/tool-step.
_APPROVAL_LOCK = threading.RLock()
_APPROVAL_STATE: dict[tuple[str, str, str], dict[str, Any]] = {}
_APPROVAL_UPDATED_AT: dict[tuple[str, str, str], float] = {}
_AUDIT_RECEIPTS: list[dict[str, Any]] = []
_AUDIT_RECEIPTS_LIMIT = 200


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def external_url_approval_policy() -> str:
    raw = (
        os.getenv("EXTERNAL_URL_APPROVAL_POLICY")
        or os.getenv("Notion__ExternalUrlApprovalPolicy")
        or "allow_all"
    )
    policy = str(raw).strip().lower()
    if policy in {"allow_all", "allow-all", "all"}:
        return "allow_all"
    if policy in {"manual", "prompt", "ask"}:
        return "manual"
    return "allow_all"


def auto_continue_external_url_confirmations_enabled() -> bool:
    if os.getenv("AUTO_CONTINUE_EXTERNAL_URL_CONFIRMATIONS") is not None:
        return _env_flag("AUTO_CONTINUE_EXTERNAL_URL_CONFIRMATIONS", default=True)
    if os.getenv("Notion__AutoContinueExternalUrlConfirmations") is not None:
        return _env_flag("Notion__AutoContinueExternalUrlConfirmations", default=True)
    return external_url_approval_policy() == "allow_all"


def external_url_auto_approve_max_retries() -> int:
    raw = os.getenv("EXTERNAL_URL_AUTO_APPROVE_MAX_RETRIES", "2")
    try:
        return max(0, int(raw or "2"))
    except (TypeError, ValueError):
        return 2


def should_auto_continue_external_url_confirmations() -> bool:
    return (
        external_url_approval_policy() == "allow_all"
        and auto_continue_external_url_confirmations_enabled()
    )


def _registry_key(client: "NotionOpusAPI", thread_id: str) -> tuple[str, str]:
    account = str(getattr(client, "account_key", "") or getattr(client, "user_id", "") or "default")
    return account, str(thread_id or "").strip()


def _approval_key(client: "NotionOpusAPI", thread_id: str, tool_step_id: str) -> tuple[str, str, str]:
    account, clean_thread = _registry_key(client, thread_id)
    return account, clean_thread, str(tool_step_id or "").strip()


def _prune_pending_locked(now: float | None = None) -> None:
    current = time.monotonic() if now is None else now
    stale = [key for key, updated in _PENDING_UPDATED_AT.items() if current - updated > _PENDING_TTL_SECONDS]
    for key in stale:
        _PENDING_UPDATED_AT.pop(key, None)
        _PENDING_BY_THREAD.pop(key, None)


def _prune_approvals_locked(now: float | None = None) -> None:
    current = time.monotonic() if now is None else now
    stale = [
        key
        for key, updated in _APPROVAL_UPDATED_AT.items()
        if current - updated > _PENDING_TTL_SECONDS
    ]
    for key in stale:
        _APPROVAL_UPDATED_AT.pop(key, None)
        _APPROVAL_STATE.pop(key, None)


def remember_pending_unsafe_url_steps(
    client: "NotionOpusAPI",
    thread_id: str,
    steps: list[dict[str, Any]],
) -> None:
    """Retain transient confirmation step IDs captured from the live NDJSON stream."""
    key = _registry_key(client, thread_id)
    if not key[1]:
        return
    normalized: dict[str, dict[str, Any]] = {}
    for step in steps:
        if not isinstance(step, dict):
            continue
        step_id = str(step.get("tool_step_id") or "").strip()
        if not step_id:
            continue
        urls = list(dict.fromkeys(str(url).strip() for url in step.get("urls") or [] if str(url).strip()))
        normalized[step_id] = {"tool_step_id": step_id, "urls": urls}
    if not normalized:
        return
    with _PENDING_LOCK:
        _prune_pending_locked()
        bucket = _PENDING_BY_THREAD.setdefault(key, {})
        bucket.update(normalized)
        _PENDING_UPDATED_AT[key] = time.monotonic()


def get_remembered_unsafe_url_steps(
    client: "NotionOpusAPI",
    thread_id: str,
) -> list[dict[str, Any]]:
    key = _registry_key(client, thread_id)
    with _PENDING_LOCK:
        _prune_pending_locked()
        return [dict(item) for item in _PENDING_BY_THREAD.get(key, {}).values()]


def clear_remembered_unsafe_url_steps(
    client: "NotionOpusAPI",
    thread_id: str,
    tool_step_ids: list[str],
) -> None:
    key = _registry_key(client, thread_id)
    with _PENDING_LOCK:
        bucket = _PENDING_BY_THREAD.get(key)
        if not bucket:
            return
        for step_id in tool_step_ids:
            bucket.pop(str(step_id or "").strip(), None)
        if bucket:
            _PENDING_UPDATED_AT[key] = time.monotonic()
        else:
            _PENDING_BY_THREAD.pop(key, None)
            _PENDING_UPDATED_AT.pop(key, None)


def _record_audit_receipt(receipt: dict[str, Any]) -> dict[str, Any]:
    with _APPROVAL_LOCK:
        _AUDIT_RECEIPTS.append(dict(receipt))
        overflow = len(_AUDIT_RECEIPTS) - _AUDIT_RECEIPTS_LIMIT
        if overflow > 0:
            del _AUDIT_RECEIPTS[:overflow]
    logger.info(
        "External URL confirmation audit receipt",
        extra={"request_info": {"event": "external_url_confirmation_audit", **receipt}},
    )
    return dict(receipt)


def get_external_url_audit_receipts(*, limit: int = 50) -> list[dict[str, Any]]:
    clean_limit = max(1, min(int(limit or 50), _AUDIT_RECEIPTS_LIMIT))
    with _APPROVAL_LOCK:
        return [dict(item) for item in _AUDIT_RECEIPTS[-clean_limit:]]


def reset_external_url_approval_state_for_tests() -> None:
    """Clear in-memory approval/dedupe/audit state (test helper)."""
    with _PENDING_LOCK:
        _PENDING_BY_THREAD.clear()
        _PENDING_UPDATED_AT.clear()
    with _APPROVAL_LOCK:
        _APPROVAL_STATE.clear()
        _APPROVAL_UPDATED_AT.clear()
        _AUDIT_RECEIPTS.clear()


def claim_external_url_auto_approval(
    client: "NotionOpusAPI",
    *,
    thread_id: str,
    tool_step_id: str,
    urls: list[str] | None = None,
) -> dict[str, Any]:
    """Claim a one-shot auto-approval for a tool-step on this account/thread."""
    clean_thread = str(thread_id or "").strip()
    clean_step = str(tool_step_id or "").strip()
    clean_urls = list(dict.fromkeys(str(url).strip() for url in (urls or []) if str(url).strip()))
    account = str(getattr(client, "account_key", "") or getattr(client, "user_id", "") or "default")
    if not clean_thread:
        return {
            "claimed": False,
            "duplicate": False,
            "reason": "missing_thread_id",
            "account": account,
            "thread_id": clean_thread,
            "tool_step_id": clean_step,
            "urls": clean_urls,
            "url_count": len(clean_urls),
        }
    if not clean_step:
        return {
            "claimed": False,
            "duplicate": False,
            "reason": "missing_tool_step_id",
            "account": account,
            "thread_id": clean_thread,
            "tool_step_id": clean_step,
            "urls": clean_urls,
            "url_count": len(clean_urls),
        }

    key = _approval_key(client, clean_thread, clean_step)
    with _APPROVAL_LOCK:
        _prune_approvals_locked()
        existing = _APPROVAL_STATE.get(key)
        if existing is not None:
            status = str(existing.get("status") or "")
            return {
                "claimed": False,
                "duplicate": True,
                "reason": f"duplicate_{status}" if status else "duplicate",
                "account": account,
                "thread_id": clean_thread,
                "tool_step_id": clean_step,
                "urls": list(existing.get("urls") or clean_urls),
                "url_count": int(existing.get("url_count") or len(clean_urls)),
                "receipt_id": existing.get("receipt_id"),
                "status": status,
            }
        receipt_id = str(uuid.uuid4())
        _APPROVAL_STATE[key] = {
            "status": "in_flight",
            "receipt_id": receipt_id,
            "urls": clean_urls,
            "url_count": len(clean_urls),
            "account": account,
            "thread_id": clean_thread,
            "tool_step_id": clean_step,
        }
        _APPROVAL_UPDATED_AT[key] = time.monotonic()
        return {
            "claimed": True,
            "duplicate": False,
            "reason": "claimed",
            "account": account,
            "thread_id": clean_thread,
            "tool_step_id": clean_step,
            "urls": clean_urls,
            "url_count": len(clean_urls),
            "receipt_id": receipt_id,
            "status": "in_flight",
        }


def complete_external_url_auto_approval(
    client: "NotionOpusAPI",
    *,
    thread_id: str,
    tool_step_id: str,
    approved: bool,
    continuation_result: dict[str, Any] | None = None,
    error: str | None = None,
) -> dict[str, Any]:
    """Finalize approval state and emit an audit receipt."""
    key = _approval_key(client, thread_id, tool_step_id)
    continuation = dict(continuation_result or {})
    with _APPROVAL_LOCK:
        _prune_approvals_locked()
        current = dict(_APPROVAL_STATE.get(key) or {})
        status = "approved" if approved else "failed"
        current["status"] = status
        current["approved"] = bool(approved)
        current["continuation_result"] = {
            "approved": bool(continuation.get("approved")),
            "reason": continuation.get("reason"),
            "stream_completed": bool(continuation.get("stream_completed")),
            "applied_tool_step_ids": list(continuation.get("applied_tool_step_ids") or []),
            "unresolved_tool_step_ids": list(continuation.get("unresolved_tool_step_ids") or []),
            "event_count": int(continuation.get("event_count") or 0),
            "trace_id": continuation.get("trace_id"),
        }
        if error:
            current["error"] = str(error)[:500]
        _APPROVAL_STATE[key] = current
        _APPROVAL_UPDATED_AT[key] = time.monotonic()

    receipt = {
        "receipt_id": current.get("receipt_id") or str(uuid.uuid4()),
        "account": current.get("account")
        or str(getattr(client, "account_key", "") or getattr(client, "user_id", "") or "default"),
        "thread_id": str(thread_id or "").strip(),
        "tool_step_id": str(tool_step_id or "").strip(),
        "url_count": int(current.get("url_count") or len(current.get("urls") or [])),
        "urls": list(current.get("urls") or []),
        "approval_result": status,
        "approved": bool(approved),
        "continuation_result": current.get("continuation_result") or {},
        "error": current.get("error"),
        "policy": external_url_approval_policy(),
    }
    return _record_audit_receipt(receipt)


def _is_pending_confirmation(step: dict[str, Any]) -> bool:
    state = str(step.get("state") or "").strip().lower()
    legacy = str(step.get("userConfirmation") or "").strip().lower()
    if state == "confirmation:requested" or legacy == "requested":
        return True
    return bool(
        step.get("requestedConfirmation")
        and state not in {"applied", "confirmation:confirmed", "confirmation:rejected"}
        and not step.get("result")
    )


def _unsafe_url_step(step: dict[str, Any]) -> dict[str, Any] | None:
    if step.get("type") != "agent-tool-result" or not _is_pending_confirmation(step):
        return None
    tool_input = step.get("input")
    if not isinstance(tool_input, dict):
        return None
    if tool_input.get("function") != "connections.web.loadPage":
        return None

    urls: list[str] = []
    for confirmation in step.get("pendingConfirmations") or []:
        if not isinstance(confirmation, dict) or confirmation.get("type") != "urlSafety":
            continue
        urls.extend(str(url).strip() for url in confirmation.get("urls") or [] if str(url).strip())
    if not urls:
        args = tool_input.get("args")
        if isinstance(args, dict) and str(args.get("url") or "").strip():
            urls.append(str(args["url"]).strip())

    step_id = str(step.get("id") or "").strip()
    if not step_id:
        return None
    return {"tool_step_id": step_id, "urls": list(dict.fromkeys(urls))}


def find_pending_unsafe_url_steps(value: Any) -> list[dict[str, Any]]:
    """Extract unique pending web load confirmations from a Notion sync payload."""
    found: dict[str, dict[str, Any]] = {}

    def visit(item: Any) -> None:
        if isinstance(item, dict):
            candidate = _unsafe_url_step(item)
            if candidate:
                found[candidate["tool_step_id"]] = candidate
            for child in item.values():
                visit(child)
        elif isinstance(item, list):
            for child in item:
                visit(child)

    visit(value)
    return list(found.values())


def discover_pending_unsafe_url_steps(
    client: "NotionOpusAPI",
    thread_id: str,
    *,
    batch_size: int = 50,
) -> list[dict[str, Any]]:
    """Hydrate raw Notion records and retain confirmation-only tool steps."""
    thread_payload = {
        "requests": [
            {
                "pointer": {
                    "table": "thread",
                    "id": thread_id,
                    "spaceId": client.space_id,
                },
                "version": -1,
            }
        ]
    }
    thread_obj = _post_json(client, HYDRATE_ENDPOINT, thread_payload)
    found = {item["tool_step_id"]: item for item in find_pending_unsafe_url_steps(thread_obj)}
    message_ids = collect_hydration_message_ids(thread_obj)
    thread_bundle = import_chat_object(thread_obj)
    for thread in thread_bundle.get("threads", {}).values():
        message_ids.extend(str(item) for item in thread.get("message_ids") or [] if str(item).strip())
    message_ids = list(dict.fromkeys(message_ids))
    for start in range(0, len(message_ids), batch_size):
        batch = message_ids[start:start + batch_size]
        message_obj = _post_json(
            client,
            HYDRATE_ENDPOINT,
            {
                "requests": [
                    {
                        "pointer": {
                            "table": "thread_message",
                            "id": message_id,
                            "spaceId": client.space_id,
                        },
                        "version": -1,
                    }
                    for message_id in batch
                ]
            },
        )
        for item in find_pending_unsafe_url_steps(message_obj):
            found[item["tool_step_id"]] = item
    return list(found.values())


def allow_pending_unsafe_urls_once(
    client: "NotionOpusAPI",
    *,
    thread_id: str,
    tool_step_ids: list[str] | None = None,
) -> dict[str, Any]:
    clean_thread_id = str(thread_id or "").strip()
    if not clean_thread_id:
        raise ValueError("thread_id is required")

    pending: list[dict[str, Any]] = []
    clean_ids = list(dict.fromkeys(str(item).strip() for item in (tool_step_ids or []) if str(item).strip()))
    if not clean_ids:
        pending = get_remembered_unsafe_url_steps(client, clean_thread_id)
        clean_ids = [item["tool_step_id"] for item in pending]
    if not clean_ids:
        # Hydration is only a fallback. Confirmation-requested tool steps are
        # normally transient and are captured from the live NDJSON stream.
        pending = discover_pending_unsafe_url_steps(client, clean_thread_id)
        clean_ids = [item["tool_step_id"] for item in pending]
    if not clean_ids:
        return {
            "ok": False,
            "continued": False,
            "thread_id": clean_thread_id,
            "tool_step_ids": [],
            "urls": [],
            "reason": "no_pending_unsafe_url_confirmation",
        }

    if not pending:
        remembered = {
            item["tool_step_id"]: item
            for item in get_remembered_unsafe_url_steps(client, clean_thread_id)
        }
        pending = [remembered[step_id] for step_id in clean_ids if step_id in remembered]
        for step_id in clean_ids:
            if step_id not in {item["tool_step_id"] for item in pending}:
                pending.append({"tool_step_id": step_id, "urls": []})

    result = client.continue_confirmed_tool_steps(
        thread_id=clean_thread_id,
        tool_step_ids=clean_ids,
    )
    if result.get("approved"):
        clear_remembered_unsafe_url_steps(client, clean_thread_id, clean_ids)
    return {
        "ok": bool(result.get("approved")),
        "continued": bool(result.get("approved")),
        "thread_id": clean_thread_id,
        "tool_step_ids": clean_ids,
        "urls": list(dict.fromkeys(url for item in pending for url in item["urls"])),
        **result,
    }


def auto_approve_external_url_confirmation(
    client: "NotionOpusAPI",
    *,
    thread_id: str,
    tool_step_id: str,
    urls: list[str] | None = None,
) -> dict[str, Any]:
    """Immediately approve one captured external-URL confirmation via allow-once continuation."""
    claim = claim_external_url_auto_approval(
        client,
        thread_id=thread_id,
        tool_step_id=tool_step_id,
        urls=urls,
    )
    if claim.get("duplicate"):
        return {
            "ok": True,
            "duplicate": True,
            "approved": str(claim.get("status") or "") == "approved",
            "continued": False,
            "reason": claim.get("reason") or "duplicate",
            "thread_id": claim.get("thread_id"),
            "tool_step_id": claim.get("tool_step_id"),
            "urls": list(claim.get("urls") or []),
            "url_count": int(claim.get("url_count") or 0),
            "receipt_id": claim.get("receipt_id"),
            "state": "external_url_auto_approved"
            if str(claim.get("status") or "") == "approved"
            else "external_url_confirmation_received",
        }
    if not claim.get("claimed"):
        receipt = complete_external_url_auto_approval(
            client,
            thread_id=thread_id,
            tool_step_id=tool_step_id,
            approved=False,
            error=str(claim.get("reason") or "claim_failed"),
        )
        return {
            "ok": False,
            "duplicate": False,
            "approved": False,
            "continued": False,
            "reason": claim.get("reason") or "claim_failed",
            "thread_id": thread_id,
            "tool_step_id": tool_step_id,
            "urls": list(urls or []),
            "url_count": len(list(urls or [])),
            "receipt": receipt,
            "state": "external_url_approval_failed",
        }

    remember_pending_unsafe_url_steps(
        client,
        thread_id,
        [{"tool_step_id": tool_step_id, "urls": list(urls or [])}],
    )

    max_retries = external_url_auto_approve_max_retries()
    attempt = 0
    last_error: Exception | None = None
    result: dict[str, Any] = {}
    while attempt <= max_retries:
        attempt += 1
        try:
            result = allow_pending_unsafe_urls_once(
                client,
                thread_id=thread_id,
                tool_step_ids=[tool_step_id],
            )
            last_error = None
            break
        except Exception as exc:  # noqa: BLE001 - operational boundary; classified below
            last_error = exc
            retriable = bool(getattr(exc, "retriable", False))
            if not retriable or attempt > max_retries:
                receipt = complete_external_url_auto_approval(
                    client,
                    thread_id=thread_id,
                    tool_step_id=tool_step_id,
                    approved=False,
                    error=str(exc),
                )
                return {
                    "ok": False,
                    "duplicate": False,
                    "approved": False,
                    "continued": False,
                    "reason": "continuation_failed",
                    "error": str(exc),
                    "error_type": "operational_error",
                    "thread_id": thread_id,
                    "tool_step_id": tool_step_id,
                    "urls": list(urls or []),
                    "url_count": len(list(urls or [])),
                    "attempts": attempt,
                    "receipt": receipt,
                    "state": "external_url_approval_failed",
                }

    if last_error is not None:
        raise last_error

    approved = bool(result.get("approved"))
    receipt = complete_external_url_auto_approval(
        client,
        thread_id=thread_id,
        tool_step_id=tool_step_id,
        approved=approved,
        continuation_result=result,
        error=None if approved else str(result.get("reason") or "confirmation_not_applied"),
    )
    return {
        "ok": approved,
        "duplicate": False,
        "approved": approved,
        "continued": approved,
        "reason": result.get("reason") or ("approved" if approved else "confirmation_not_applied"),
        "thread_id": thread_id,
        "tool_step_id": tool_step_id,
        "tool_step_ids": list(result.get("tool_step_ids") or [tool_step_id]),
        "urls": list(result.get("urls") or urls or []),
        "url_count": len(list(result.get("urls") or urls or [])),
        "attempts": attempt,
        "receipt": receipt,
        "continuation": result,
        "state": "external_url_auto_approved" if approved else "external_url_approval_failed",
        "error_type": None if approved else "operational_error",
    }
