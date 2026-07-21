from __future__ import annotations

from typing import Any, TYPE_CHECKING
import threading
import time

from app.chat_history.extractor import collect_hydration_message_ids
from app.chat_history.har_importer import import_chat_object
from app.chat_history.notion_sync import HYDRATE_ENDPOINT, _post_json

if TYPE_CHECKING:
    from app.notion_client import NotionOpusAPI


_PENDING_TTL_SECONDS = 15 * 60
_PENDING_LOCK = threading.RLock()
_PENDING_BY_THREAD: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
_PENDING_UPDATED_AT: dict[tuple[str, str], float] = {}


def _registry_key(client: "NotionOpusAPI", thread_id: str) -> tuple[str, str]:
    account = str(getattr(client, "account_key", "") or getattr(client, "user_id", "") or "default")
    return account, str(thread_id or "").strip()


def _prune_pending_locked(now: float | None = None) -> None:
    current = time.monotonic() if now is None else now
    stale = [key for key, updated in _PENDING_UPDATED_AT.items() if current - updated > _PENDING_TTL_SECONDS]
    for key in stale:
        _PENDING_UPDATED_AT.pop(key, None)
        _PENDING_BY_THREAD.pop(key, None)


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
