from __future__ import annotations

import asyncio
import csv
import threading
import hashlib
import io
import re
from datetime import datetime
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import PlainTextResponse

from app.account_scope import AccountScope
from app.chat_history.har_importer import import_har_object
from app.chat_history.notion_sync import hydrate_thread_from_notion, sync_chat_history_from_notion
from app.chat_history.migrate import migrate_shared_chat_history
from app.chat_history.store import (
    ChatHistoryStore,
    get_chat_history_db_root,
    get_default_chat_history_db_path,
)
from app.notion_client import NotionUpstreamError

router = APIRouter(prefix="/chat-history", tags=["chat-history"])

_OPERATION_STATE_LOCK = threading.Lock()
_ACTIVE_OPERATIONS: set[str] = set()


class OperationInProgress(RuntimeError):
    """Raised when a duplicate parent sync or hydrate operation is already running."""


def _claim_operation(key: str) -> None:
    with _OPERATION_STATE_LOCK:
        if key in _ACTIVE_OPERATIONS:
            raise OperationInProgress(key)
        _ACTIVE_OPERATIONS.add(key)


def _release_operation(key: str) -> None:
    with _OPERATION_STATE_LOCK:
        _ACTIVE_OPERATIONS.discard(key)


async def _run_singleflight(key: str, func: Any, /, *args: Any, **kwargs: Any) -> Any:
    """Run one blocking operation without blocking the API loop or duplicating work."""
    _claim_operation(key)
    worker = asyncio.create_task(run_in_threadpool(func, *args, **kwargs))
    release_deferred = False
    try:
        return await asyncio.shield(worker)
    except asyncio.CancelledError:
        release_deferred = True
        worker.add_done_callback(lambda _future: _release_operation(key))
        raise
    finally:
        if not release_deferred:
            _release_operation(key)


def _store() -> ChatHistoryStore:
    """Legacy shard accessor for explicit migration/compatibility endpoints only."""
    return ChatHistoryStore()


def _account_store(client: Any) -> tuple[AccountScope, ChatHistoryStore]:
    scope = AccountScope.from_client(client)
    return scope, ChatHistoryStore(account_key=scope.account_key)


def _bool_payload(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "on", "full", "hydrate"}
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _clean_thread_ids(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    out: list[str] = []
    seen: set[str] = set()
    for item in value:
        if isinstance(item, bool) or item is None or isinstance(item, (dict, list, tuple, set)):
            continue
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        out.append(text)
    return out


def _get_account_client(request: Request, account_index: int):
    pool = request.app.state.account_pool
    clients = getattr(pool, "clients", [])
    if not clients:
        raise HTTPException(status_code=503, detail="No configured Notion accounts are available")
    if account_index < 0 or account_index >= len(clients):
        raise HTTPException(status_code=400, detail="account_index is out of range")
    return clients[account_index]


def _require_account_client(
    request: Request,
    *,
    account_index: Any = None,
    account_profile: Any = None,
):
    pool = request.app.state.account_pool
    clients = list(getattr(pool, "clients", []) or [])
    if not clients:
        raise HTTPException(status_code=503, detail="No configured Notion accounts are available")
    profile = str(account_profile or "").strip()
    if account_index is None and not profile:
        raise HTTPException(
            status_code=400,
            detail="account_index or account_profile is required in multi-account mode",
        )
    if account_index is not None:
        try:
            client = _get_account_client(request, int(account_index))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=400, detail="account_index is invalid") from exc
        if profile:
            names = {
                str(getattr(client, "profile_name", "") or "").casefold(),
                str(getattr(client, "base_profile_name", "") or "").casefold(),
            }
            if profile.casefold() not in names:
                raise HTTPException(
                    status_code=409, detail="account_index and account_profile select different accounts"
                )
        return client
    matches = [
        client
        for client in clients
        if profile.casefold()
        in {
            str(getattr(client, "profile_name", "") or "").casefold(),
            str(getattr(client, "base_profile_name", "") or "").casefold(),
            str(getattr(client, "email", "") or "").casefold(),
            str(getattr(client, "user_id", "") or "").casefold(),
        }
    ]
    if len(matches) != 1:
        raise HTTPException(status_code=400, detail="account_profile did not resolve uniquely")
    return matches[0]


def _normalized_prompt(value: Any) -> str:
    return " ".join(str(value or "").split())


def _group_export_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        prompt = _normalized_prompt(row.get("sent_message"))
        key = hashlib.sha1(prompt.encode("utf-8", errors="replace")).hexdigest()[:12]
        grouped.setdefault(key, []).append(row)
    out: list[dict[str, Any]] = []
    for key, group_rows in sorted(grouped.items(), key=lambda item: (-len(item[1]), _normalized_prompt(item[1][0].get("sent_message")))):
        total = len(group_rows)
        for index, row in enumerate(
            sorted(group_rows, key=lambda item: (str(item.get("actual_model") or ""), str(item.get("received_message_time") or ""))),
            start=1,
        ):
            enriched = dict(row)
            enriched["duplicate_prompt_hash"] = key
            enriched["duplicate_prompt_count"] = total
            enriched["duplicate_prompt_index"] = index
            out.append(enriched)
    return out


def _csv_text(rows: list[dict[str, Any]], *, include_prompt: bool = True, include_response: bool = True) -> str:
    output = io.StringIO()
    fieldnames = [
        "duplicate_prompt_hash",
        "duplicate_prompt_count",
        "duplicate_prompt_index",
        "thread_id",
        "title",
        "sent_message_time",
        "received_message_time",
        "response_role",
        "response_is_error",
        "model_provider",
        "actual_model",
        "requested_model",
        "notion_requested_model",
    ]
    if include_prompt:
        fieldnames.append("sent_message")
    if include_response:
        fieldnames.append("received_message")
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    for row in _group_export_rows(rows):
        writer.writerow(row)
    return output.getvalue()


def _md_escape(value: Any) -> str:
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def _md_model_label(row: dict[str, Any]) -> str:
    display = _md_escape(row.get("display_model"))
    actual = _md_escape(row.get("actual_model"))
    requested = _md_escape(row.get("requested_model"))
    notion_requested = _md_escape(row.get("notion_requested_model"))
    provider = _md_escape(row.get("model_provider"))
    model = display or actual or requested or notion_requested or "[unknown]"
    if provider and provider != "[unknown]":
        return f"{model} / {provider}"
    return model


def _md_anchor(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", str(value or "").lower()).strip("-")
    return text or "section"


def _markdown_text(rows: list[dict[str, Any]], *, include_prompt: bool = True, include_response: bool = True) -> str:
    grouped_rows = _group_export_rows(rows)
    by_hash: dict[str, list[dict[str, Any]]] = {}
    for row in grouped_rows:
        by_hash.setdefault(str(row.get("duplicate_prompt_hash") or ""), []).append(row)

    lines: list[str] = [
        "# Exported Notion Message Threads",
        "",
        "## Navigation Index",
        "",
        "Use the headings, `Model:` labels, and `Thread:` IDs for fast search in Notepad++ or any Markdown renderer.",
        "",
        "| Prompt group | Responses | Models | Errors |",
        "|---|---:|---|---:|",
    ]
    for group_hash, group in by_hash.items():
        models = ", ".join(dict.fromkeys(_md_model_label(row) for row in group)) or "[unknown]"
        errors = sum(1 for row in group if bool(row.get("response_is_error")))
        lines.append(f"| `{group_hash}` | {len(group)} | {models} | {errors} |")
    lines += ["", "---", ""]

    for group_hash, group in by_hash.items():
        prompt = _md_escape(group[0].get("sent_message"))
        anchor = _md_anchor(group_hash)
        lines += [
            f"## Prompt Group: `{group_hash}`",
            "",
            f"Anchor: `{anchor}`",
            f"Responses: `{len(group)}`",
            f"Duplicate prompt hash: `{group_hash}`",
            "",
        ]
        if include_prompt:
            lines += [
                "### Shared Sent Prompt",
                "",
                "The same or normalized-same prompt was used for this group. Each individual thread below still repeats its own sent-message label for search/sort consistency.",
                "",
                "```text",
                prompt,
                "```",
                "",
            ]

        for row in group:
            role = _md_escape(row.get("response_role")) or "response"
            model_label = _md_model_label(row)
            thread_id = _md_escape(row.get("thread_id"))
            title = _md_escape(row.get("title")) or thread_id
            sent_time = _md_escape(row.get("sent_message_time"))
            received_time = _md_escape(row.get("received_message_time"))
            error_flag = "true" if bool(row.get("response_is_error")) else "false"
            response_index = row.get("duplicate_prompt_index")

            lines += [
                f"### Thread Response {response_index}: Model: {model_label}",
                "",
                f"Thread: `{thread_id}`",
                f"Title: {title}",
                f"Response role: `{role}`",
                f"Error response: `{error_flag}`",
                f"Model: {model_label}",
                f"Actual model: `{_md_escape(row.get('actual_model')) or '[unknown]'}`",
                f"Provider: `{_md_escape(row.get('model_provider')) or '[unknown]'}`",
                f"Requested model: `{_md_escape(row.get('requested_model')) or '[unknown]'}`",
                f"Notion requested model: `{_md_escape(row.get('notion_requested_model')) or '[unknown]'}`",
                "",
            ]
            if include_prompt:
                lines += [
                    f"#### Sent Message ? Model: {model_label}",
                    "",
                    f"Sent timestamp: `{sent_time}`",
                    f"Model: {model_label}",
                    "",
                    "```text",
                    _md_escape(row.get("sent_message")),
                    "```",
                    "",
                ]
            if include_response:
                lines += [
                    f"#### Received Message ? Model: {model_label}",
                    "",
                    f"Received timestamp: `{received_time}`",
                    f"Model: {model_label}",
                    f"Role: `{role}`",
                    "",
                    "```text",
                    _md_escape(row.get("received_message")),
                    "```",
                    "",
                ]
            lines += ["---", ""]
    return "\n".join(lines).rstrip() + "\n"

def _delete_known_threads(request: Request, thread_ids: list[str], account_index: int, *, remote: bool = True, local: bool = True) -> dict[str, Any]:
    results: dict[str, Any] = {"success": [], "failed": []}
    if remote:
        client = _get_account_client(request, account_index)
        for thread_id in thread_ids:
            try:
                remote_result = client.delete_threads([thread_id])
            except NotionUpstreamError as exc:
                results["failed"].append({"thread_id": thread_id, "stage": "remote", "error": exc.response_excerpt or str(exc)})
                continue
            accepted = int(remote_result.get("remote_deleted", 0) or remote_result.get("remote_accepted", 0) or 0) > 0
            if accepted:
                results["success"].append(thread_id)
            else:
                results["failed"].append({"thread_id": thread_id, "stage": "remote", "error": "Remote delete transaction was not accepted."})
    else:
        results["success"].extend(thread_ids)

    local_result: dict[str, int] = {"threads_deleted": 0, "messages_deleted": 0, "fts_deleted": 0}
    if local and results["success"]:
        local_result = _store().delete_threads(results["success"])
    return {"results": results, "local_result": local_result}


async def _request_payload(request: Request) -> dict[str, Any]:
    try:
        payload = await request.json()
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Request body must be JSON") from exc
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="Request body must be a JSON object")
    return payload


def _bulk_delete_from_payload(request: Request, payload: dict[str, Any]) -> dict[str, Any]:
    thread_ids = _clean_thread_ids(payload.get("thread_ids") or payload.get("threadIds"))
    if not thread_ids:
        raise HTTPException(status_code=400, detail="thread_ids must contain at least one thread id")
    if len(thread_ids) > 200:
        raise HTTPException(status_code=400, detail="Bulk delete is limited to 200 thread ids per request")

    remote = _bool_payload(payload.get("remote"), default=True)
    local = _bool_payload(payload.get("local"), default=True)
    account_index = payload.get("account_index", 0)
    try:
        account_index = int(account_index)
    except Exception as exc:
        raise HTTPException(status_code=400, detail="Invalid delete parameters") from exc

    store = _store()
    existing_ids = store.existing_thread_ids(thread_ids)
    results: dict[str, Any] = {"success": [], "failed": []}

    for thread_id in thread_ids:
        if thread_id not in existing_ids:
            results["failed"].append(
                {
                    "thread_id": thread_id,
                    "stage": "local_auth",
                    "error": "Thread ID not found in the local archive.",
                }
            )

    known_ids = [thread_id for thread_id in thread_ids if thread_id in existing_ids]

    if remote:
        client = _get_account_client(request, account_index)
        for thread_id in known_ids:
            try:
                remote_result = client.delete_threads([thread_id])
            except NotionUpstreamError as exc:
                results["failed"].append(
                    {
                        "thread_id": thread_id,
                        "stage": "remote",
                        "error": exc.response_excerpt or str(exc),
                    }
                )
                continue
            accepted = int(remote_result.get("remote_deleted", 0) or remote_result.get("remote_accepted", 0) or 0) > 0
            if accepted:
                results["success"].append(thread_id)
            else:
                results["failed"].append(
                    {
                        "thread_id": thread_id,
                        "stage": "remote",
                        "error": "Remote delete transaction was not accepted.",
                    }
                )
    else:
        results["success"].extend(known_ids)

    local_result: dict[str, int] = {"threads_deleted": 0, "messages_deleted": 0, "fts_deleted": 0}
    if local and results["success"]:
        local_result = store.delete_threads(results["success"])

    return {
        "requested": len(thread_ids),
        "remote": remote,
        "local": local,
        "results": results,
        "remote_result": {
            "remote_accepted": len(results["success"]) if remote else 0,
            "remote_failed": len([item for item in results["failed"] if item.get("stage") == "remote"]),
            "failed_ids": [item.get("thread_id") for item in results["failed"] if item.get("stage") == "remote"],
        },
        "local_result": local_result,
        "message": f"Processed {len(thread_ids)} thread(s): {len(results['success'])} succeeded, {len(results['failed'])} failed.",
    }


@router.get("/status")
def status() -> dict[str, Any]:
    return {
        "status": "ok",
        "legacy_db_path": get_default_chat_history_db_path(),
        "account_db_root": get_chat_history_db_root(),
        "capabilities": [
            "har_import",
            "notion_direct_sync",
            "metadata_only_sync",
            "selected_thread_hydration",
            "bulk_remote_delete",
            "bulk_delete_partial_results",
            "bulk_delete_post_fallback",
            "hydration_diagnostics",
            "thread_debug",
            "model_response_stats",
            "local_archive",
            "local_search",
            "markdown_export",
        ],
    }


def _history_upstream_http_exception(
    exc: NotionUpstreamError,
    *,
    action: str,
) -> HTTPException:
    """Preserve Notion rate limits while keeping other history failures unavailable."""
    rate_limited = exc.status_code == 429
    return HTTPException(
        status_code=429 if rate_limited else 503,
        detail={
            "error": {
                "message": (
                    f"Notion rate-limited the chat-history {action}."
                    if rate_limited
                    else f"Unable to {action} chat history from Notion."
                ),
                "type": "upstream_rate_limit" if rate_limited else "upstream_error",
                "param": None,
                "code": "NOTION_429" if rate_limited else "upstream_error",
                "detail": exc.response_excerpt,
                "upstream_status_code": exc.status_code,
                "retriable": exc.retriable,
            }
        },
    )


@router.post("/import/har")
async def import_har(request: Request) -> dict[str, Any]:
    """Import a browser HAR JSON object containing Notion AI chat-history records."""
    payload = await _request_payload(request)

    har = payload.get("har") if "har" in payload else payload
    if not isinstance(har, dict):
        raise HTTPException(status_code=400, detail="HAR payload must be a JSON object")

    bundle = import_har_object(har)
    imported = _store().upsert_bundle(bundle)
    return {"imported": imported, "endpoint_counts": bundle.get("endpoint_counts", {})}


@router.post("/sync/notion")
@router.post("/import/notion")
async def sync_from_notion(request: Request) -> dict[str, Any]:
    """Pull chat-history metadata by default; hydrate full message bodies only when requested."""
    try:
        payload = await request.json()
    except (TypeError, ValueError):
        payload = {}

    if not isinstance(payload, dict):
        payload = {}

    account_index = payload.get("account_index")
    account_profile = payload.get("account_profile") or payload.get("profile_name")
    limit = payload.get("limit", 100)
    max_pages = payload.get("max_pages", 5)
    hydrate = _bool_payload(payload.get("hydrate"), default=False)

    try:
        if account_index is not None:
            account_index = int(account_index)
        limit = max(1, min(int(limit), 500))
        max_pages = max(1, min(int(max_pages), 20))
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Invalid import parameters") from exc

    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    scope, store = _account_store(client)
    operation_key = f"sync:{scope.account_key}"
    try:
        fresh_lookup = None
        if hydrate:
            def fresh_lookup(candidates: dict[str, int | None]) -> set[str]:
                return store.fresh_message_ids(
                    candidates,
                    workspace_id=getattr(client, "space_id", None),
                    account_key=scope.account_key,
                )

        bundle = await _run_singleflight(
            operation_key,
            sync_chat_history_from_notion,
            client,
            limit=limit,
            max_pages=max_pages,
            hydrate=hydrate,
            existing_message_ids_lookup=store.existing_message_ids if hydrate else None,
            fresh_message_ids_lookup=fresh_lookup,
            store=store,
            persist=True,
        )
    except OperationInProgress as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": {
                    "message": "A chat-history sync is already in progress for this account.",
                    "type": "conflict",
                    "param": None,
                    "code": "sync_in_progress",
                }
            },
        ) from exc
    except NotionUpstreamError as exc:
        raise _history_upstream_http_exception(exc, action="sync") from exc

    imported = bundle.get("persist_result") if isinstance(bundle.get("persist_result"), dict) else {}
    summary = dict(bundle.get("sync_summary", {}))
    summary.update(
        {
            "threads_inserted": imported.get("threads_inserted", 0),
            "threads_updated": imported.get("threads_updated", 0),
            "messages_inserted": imported.get("messages_inserted", 0),
            "messages_updated": imported.get("messages_updated", 0),
            "semantic_messages_inserted": imported.get("semantic_messages_inserted", 0),
            "raw_records_inserted": imported.get("raw_records_inserted", 0),
            "checkpoint_advanced": summary.get("checkpoint_advanced", False),
            "sync_run_id": summary.get("sync_run_id"),
        }
    )
    return {
        "imported": imported,
        "endpoint_counts": bundle.get("endpoint_counts", {}),
        "source": "notion_direct_sync",
        "account_index": account_index,
        "account_profile": scope.profile_name,
        "account_key": scope.account_key,
        "db_path": store.db_path,
        "stats": bundle.get("stats", {}),
        "sync_summary": summary,
    }


@router.get("/model-stats")
def model_stats(
    request: Request,
    account_index: int | None = Query(None, ge=0),
    account_profile: str | None = Query(None),
) -> dict[str, Any]:
    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    scope, store = _account_store(client)
    stats = store.model_response_stats()
    return {
        "account_key": scope.account_key,
        "account_profile": scope.profile_name,
        "db_path": store.db_path,
        **(stats if isinstance(stats, dict) else {"stats": stats}),
    }


@router.post("/migrate/accounts")
async def migrate_account_shards(request: Request) -> dict[str, Any]:
    """One-time partition of the shared chat_history.db into account shards."""
    payload = await _request_payload(request)
    known = payload.get("known_thread_accounts") or {}
    if known is not None and not isinstance(known, dict):
        raise HTTPException(status_code=400, detail="known_thread_accounts must be an object")
    result = migrate_shared_chat_history(
        source_db=payload.get("source_db") or None,
        known_thread_accounts={str(k): str(v) for k, v in dict(known or {}).items()},
        dry_run=_bool_payload(payload.get("dry_run"), default=False),
        keep_source=not _bool_payload(payload.get("move_source"), default=False),
    )
    return result.as_dict()


@router.get("/threads")
def list_threads(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
    account_index: int | None = Query(None, ge=0),
    account_profile: str | None = Query(None),
) -> dict[str, Any]:
    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    scope, store = _account_store(client)
    return {
        "account_key": scope.account_key,
        "account_profile": scope.profile_name,
        "db_path": store.db_path,
        "threads": store.list_threads(limit=limit, offset=offset),
    }


@router.post("/threads/cleanup-single-message")
@router.delete("/threads/cleanup-single-message")
async def cleanup_single_message_threads(request: Request) -> dict[str, Any]:
    payload = await _request_payload(request)
    requested_ids = _clean_thread_ids(payload.get("thread_ids") or payload.get("threadIds") or [])
    if not requested_ids:
        raise HTTPException(status_code=400, detail="thread_ids must contain at least one thread id")
    account_index = int(payload.get("account_index", 0) or 0)
    remote = _bool_payload(payload.get("remote"), default=True)
    local = _bool_payload(payload.get("local"), default=True)
    eligible_ids = _store().single_message_thread_ids(requested_ids or None)
    delete_result = (
        _delete_known_threads(request, eligible_ids, account_index, remote=remote, local=local)
        if eligible_ids
        else {"results": {"success": [], "failed": []}, "local_result": {"threads_deleted": 0, "messages_deleted": 0, "fts_deleted": 0}}
    )
    return {
        "requested": len(requested_ids),
        "eligible": len(eligible_ids),
        "thread_ids": eligible_ids,
        **delete_result,
    }


@router.post("/threads/cleanup-error-threads")
async def cleanup_error_threads(request: Request) -> dict[str, Any]:
    payload = await _request_payload(request)
    requested_ids = _clean_thread_ids(payload.get("thread_ids") or payload.get("threadIds") or [])
    if not requested_ids:
        raise HTTPException(status_code=400, detail="thread_ids must contain at least one thread id")
    account_index = int(payload.get("account_index", 0) or 0)
    remote = _bool_payload(payload.get("remote"), default=True)
    local = _bool_payload(payload.get("local"), default=True)
    eligible_ids = _store().errored_thread_ids(requested_ids or None)
    delete_result = _delete_known_threads(request, eligible_ids, account_index, remote=remote, local=local) if eligible_ids else {"results": {"success": [], "failed": []}, "local_result": {"threads_deleted": 0, "messages_deleted": 0, "fts_deleted": 0}}
    return {
        "requested": len(requested_ids),
        "eligible": len(eligible_ids),
        "thread_ids": eligible_ids,
        **delete_result,
    }


@router.post("/threads/export-two-message-responses")
async def export_two_message_responses(request: Request) -> dict[str, Any]:
    payload = await _request_payload(request)
    requested_ids = _clean_thread_ids(payload.get("thread_ids") or payload.get("threadIds") or [])
    if not requested_ids:
        raise HTTPException(status_code=400, detail="thread_ids must contain at least one thread id")
    account_index = int(payload.get("account_index", 0) or 0)
    delete_after_export = _bool_payload(payload.get("delete_after_export") or payload.get("deleteAfterExport"), default=False)
    remote = _bool_payload(payload.get("remote"), default=True) if delete_after_export else False
    local = _bool_payload(payload.get("local"), default=True) if delete_after_export else False
    export_format = str(payload.get("format") or payload.get("export_format") or "csv").strip().lower()
    if export_format in {"excel", "spreadsheet", "xlsx"}:
        export_format = "csv"
    if export_format not in {"csv", "md", "markdown"}:
        raise HTTPException(status_code=400, detail="format must be csv or md")
    if export_format == "markdown":
        export_format = "md"

    include_errors = _bool_payload(payload.get("include_errors") or payload.get("includeErrors"), default=False)
    include_prompt = _bool_payload(payload.get("include_prompt") if "include_prompt" in payload else payload.get("includePrompt"), default=True)
    include_response = _bool_payload(payload.get("include_response") if "include_response" in payload else payload.get("includeResponse"), default=True)
    if not include_prompt and not include_response:
        raise HTTPException(status_code=400, detail="At least one of include_prompt or include_response must be true")

    export_data = _store().two_message_export_rows(requested_ids or None, include_errors=include_errors)
    rows = export_data.get("rows", []) if isinstance(export_data, dict) else []
    grouped_rows = _group_export_rows(rows)
    eligible_ids = export_data.get("thread_ids", []) if isinstance(export_data, dict) else []
    stamp = datetime.utcnow().strftime('%Y%m%d-%H%M%S')
    if export_format == "md":
        content = _markdown_text(rows, include_prompt=include_prompt, include_response=include_response)
        filename = f"notion-two-message-responses-{stamp}.md"
        content_type = "text/markdown"
    else:
        content = _csv_text(rows, include_prompt=include_prompt, include_response=include_response)
        filename = f"notion-two-message-responses-{stamp}.csv"
        content_type = "text/csv"
    if delete_after_export and rows and eligible_ids:
        delete_result = _delete_known_threads(request, eligible_ids, account_index, remote=remote, local=local)
    else:
        delete_result = {"results": {"success": [], "failed": []}, "local_result": {"threads_deleted": 0, "messages_deleted": 0, "fts_deleted": 0}}
    return {
        "requested": len(requested_ids),
        "eligible": len(eligible_ids),
        "exported": len(rows),
        "format": export_format,
        "include_errors": include_errors,
        "include_prompt": include_prompt,
        "include_response": include_response,
        "delete_after_export": delete_after_export,
        "filename": filename,
        "content_type": content_type,
        "content": content,
        "csv": content if export_format == "csv" else "",
        "markdown": content if export_format == "md" else "",
        "rows": grouped_rows,
        "duplicate_prompt_groups": len({row.get("duplicate_prompt_hash") for row in grouped_rows}),
        **delete_result,
    }


@router.delete("/threads")
async def delete_threads(request: Request) -> dict[str, Any]:
    """Bulk-delete remote Notion chat threads and remove confirmed successes from the local archive."""
    return _bulk_delete_from_payload(request, await _request_payload(request))


@router.post("/threads/delete")
@router.post("/threads/bulk-delete")
async def post_delete_threads(request: Request) -> dict[str, Any]:
    """POST fallback for clients/proxies that reject DELETE with a JSON body."""
    return _bulk_delete_from_payload(request, await _request_payload(request))


@router.get("/threads/{thread_id}")
def get_thread(
    thread_id: str,
    request: Request,
    account_index: int | None = Query(None, ge=0),
    account_profile: str | None = Query(None),
) -> dict[str, Any]:
    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    scope, store = _account_store(client)
    thread = store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")
    return {
        **thread,
        "account_key": scope.account_key,
        "account_profile": scope.profile_name,
        "db_path": store.db_path,
    }


@router.post("/threads/{thread_id}/hydrate")
async def hydrate_thread(thread_id: str, request: Request) -> dict[str, Any]:
    """Hydrate full messages for one selected archived thread."""
    try:
        payload = await request.json()
    except (TypeError, ValueError):
        payload = {}
    if not isinstance(payload, dict):
        payload = {}

    account_index = payload.get("account_index")
    account_profile = payload.get("account_profile") or payload.get("profile_name")
    try:
        if account_index is not None:
            account_index = int(account_index)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Invalid hydrate parameters") from exc

    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    scope, store = _account_store(client)
    thread = store.get_thread_hydration_source(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    operation_key = f"hydrate:{scope.account_key}:{thread_id}"
    try:
        def fresh_lookup(candidates: dict[str, int | None]) -> set[str]:
            return store.fresh_message_ids(
                candidates,
                workspace_id=getattr(client, "space_id", None),
                account_key=scope.account_key,
            )

        bundle = await _run_singleflight(
            operation_key,
            hydrate_thread_from_notion,
            client,
            thread,
            existing_message_ids_lookup=store.existing_message_ids,
            fresh_message_ids_lookup=fresh_lookup,
        )
    except OperationInProgress as exc:
        raise HTTPException(
            status_code=409,
            detail={
                "error": {
                    "message": "This chat thread is already being hydrated.",
                    "type": "conflict",
                    "param": None,
                    "code": "hydrate_in_progress",
                }
            },
        ) from exc
    except NotionUpstreamError as exc:
        raise _history_upstream_http_exception(exc, action="hydrate") from exc

    imported = store.upsert_bundle(
        bundle,
        workspace_id=getattr(client, "space_id", None),
        notion_user_id=getattr(client, "user_id", None) or getattr(client, "notion_user_id", None),
        account_key=scope.account_key,
        source_kind="notion_server_hydrate",
        source_endpoint="syncRecordValuesSpaceInitial",
    )
    hydrated = store.get_thread(thread_id)
    return {
        "account_key": scope.account_key,
        "account_profile": scope.profile_name,
        "db_path": store.db_path,
        "imported": imported,
        "endpoint_counts": bundle.get("endpoint_counts", {}),
        "stats": bundle.get("stats", {}),
        "thread": {
            "id": thread_id,
            "message_count": (hydrated or {}).get("message_count", 0),
            "hydrated": bool((hydrated or {}).get("message_count", 0)),
        },
    }


@router.post("/threads/{thread_id}/resume")
async def resume_thread(thread_id: str, request: Request) -> dict[str, Any]:
    """Create a real local conversation from one archived thread (fork or continue)."""
    payload = await _request_payload(request)
    mode = str(payload.get("mode") or "fork").strip().lower()
    if mode not in {"fork", "continue"}:
        raise HTTPException(status_code=400, detail="mode must be either 'fork' or 'continue'")

    account_index = payload.get("account_index")
    account_profile = payload.get("account_profile") or payload.get("profile_name")
    try:
        if account_index is not None:
            account_index = int(account_index)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail="Invalid resume parameters") from exc
    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    scope, store = _account_store(client)
    thread = store.get_thread(thread_id)
    if not thread:
        raise HTTPException(status_code=404, detail="Thread not found")

    manager = request.app.state.conversation_manager
    conversation_id = manager.new_conversation(
        workspace_id=scope.workspace_id,
        user_id=scope.user_id,
        profile_name=scope.profile_name,
    )

    if mode == "continue":
        manager.set_conversation_thread_id(conversation_id, thread_id)

    seeded_messages: list[dict[str, str]] = []
    for message in thread.get("messages") or []:
        role = str(message.get("role") or "").strip().lower()
        if role not in {"user", "assistant", "system"}:
            continue
        content = str(message.get("text") or "").strip()
        if not content:
            continue
        manager.add_message(conversation_id, role, content)
        seeded_messages.append({"role": role, "content": content})

    return {
        "account_key": scope.account_key,
        "account_profile": scope.profile_name,
        "db_path": store.db_path,
        "conversation_id": conversation_id,
        "mode": mode,
        "thread_id": thread_id,
        "seeded_message_count": len(seeded_messages),
        "title": str(thread.get("title") or thread_id),
        "messages": seeded_messages,
    }


@router.get("/threads/{thread_id}/debug")
def debug_thread(
    thread_id: str,
    request: Request,
    account_index: int | None = Query(None, ge=0),
    account_profile: str | None = Query(None),
) -> dict[str, Any]:
    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    scope, store = _account_store(client)
    payload = store.debug_thread(thread_id)
    return {
        "account_key": scope.account_key,
        "account_profile": scope.profile_name,
        "db_path": store.db_path,
        **(payload if isinstance(payload, dict) else {"debug": payload}),
    }


@router.get("/threads/{thread_id}/markdown", response_class=PlainTextResponse)
def export_markdown(
    thread_id: str,
    request: Request,
    account_index: int | None = Query(None, ge=0),
    account_profile: str | None = Query(None),
) -> str:
    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    _scope, store = _account_store(client)
    markdown = store.thread_to_markdown(thread_id)
    if markdown is None:
        raise HTTPException(status_code=404, detail="Thread not found")
    return markdown


@router.get("/search")
def search(
    request: Request,
    q: str = Query(..., min_length=1),
    limit: int = Query(25, ge=1, le=100),
    offset: int = Query(0, ge=0),
    account_index: int | None = Query(None, ge=0),
    account_profile: str | None = Query(None),
) -> dict[str, Any]:
    client = _require_account_client(
        request, account_index=account_index, account_profile=account_profile
    )
    scope, store = _account_store(client)
    try:
        return {
            "account_key": scope.account_key,
            "account_profile": scope.profile_name,
            "db_path": store.db_path,
            "results": store.search(q, limit=limit, offset=offset),
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
