from __future__ import annotations

import uuid
from collections import defaultdict
from collections.abc import Callable, Mapping
from typing import Any

from app.chat_history.extractor import collect_hydration_message_ids, record_versions
from app.chat_history.har_importer import import_chat_object
from app.notion_client import NotionOpusAPI, NotionUpstreamError

ExistingMessageIdsLookup = Callable[[list[str]], set[str]]
FreshMessageIdsLookup = Callable[[dict[str, int | None]], set[str]]


def _resolve_skip_message_ids(
    candidate_ids: set[str] | list[str],
    *,
    skip_message_ids: set[str] | None = None,
    existing_message_ids_lookup: ExistingMessageIdsLookup | None = None,
    candidate_versions: Mapping[str, int | None] | None = None,
    fresh_message_ids_lookup: FreshMessageIdsLookup | None = None,
) -> set[str]:
    """Resolve IDs that do not need hydration.

    Prefer version-aware freshness when available. Presence-only lookups remain
    supported for backward compatibility, but a higher server version forces
    re-hydration even when the ID already exists locally.
    """
    skip = {
        str(message_id).strip()
        for message_id in (skip_message_ids or set())
        if str(message_id or "").strip()
    }
    ordered = sorted(
        {
            str(message_id).strip()
            for message_id in candidate_ids
            if str(message_id or "").strip()
        }
    )
    if not ordered:
        return skip

    versioned: dict[str, int | None] = {}
    for message_id in ordered:
        if candidate_versions and message_id in candidate_versions:
            versioned[message_id] = candidate_versions[message_id]
        else:
            versioned[message_id] = None

    if fresh_message_ids_lookup is not None:
        try:
            skip |= set(fresh_message_ids_lookup(versioned))
        except TypeError:
            skip |= set(fresh_message_ids_lookup(dict(versioned)))
        return skip

    if existing_message_ids_lookup is None:
        return skip

    # Presence-only fallback: never skip IDs with a known positive server version,
    # because local existence may be stale relative to Notion authority.
    presence_candidates = [
        message_id
        for message_id, version in versioned.items()
        if version is None or (isinstance(version, int) and version < 0)
    ]
    if not presence_candidates:
        return skip
    try:
        skip |= set(existing_message_ids_lookup(presence_candidates))
    except TypeError:
        skip |= set(existing_message_ids_lookup(list(presence_candidates)))
    return skip


TRANSCRIPTS_ENDPOINT = "https://www.notion.so/api/v3/getInferenceTranscriptsForUser"
HYDRATE_ENDPOINT = "https://www.notion.so/api/v3/syncRecordValuesSpaceInitial"


def _merge_bundle(target: dict[str, Any], source: dict[str, Any]) -> None:
    target.setdefault("threads", {}).update(source.get("threads", {}))
    target.setdefault("messages", {}).update(source.get("messages", {}))
    target.setdefault("thread_messages", {}).update(source.get("thread_messages", {}))
    if source.get("raw_records"):
        target.setdefault("raw_records", []).extend(list(source.get("raw_records") or []))


def _collect_candidate_versions(bundle: dict[str, Any]) -> dict[str, int | None]:
    versions: dict[str, int | None] = {}
    for message_id, message in (bundle.get("thread_messages") or {}).items():
        if not isinstance(message, dict):
            continue
        version = message.get("version")
        versions[str(message_id)] = int(version) if isinstance(version, int) else version
    for message_id, message in (bundle.get("messages") or {}).items():
        if str(message_id) in versions:
            continue
        if not isinstance(message, dict):
            versions[str(message_id)] = None
            continue
        version = message.get("version")
        if version is None:
            version, _last = record_versions(message.get("raw") or message, message.get("raw") if isinstance(message.get("raw"), dict) else None)
        versions[str(message_id)] = version
    for raw_record in bundle.get("raw_records") or []:
        if not isinstance(raw_record, dict):
            continue
        if str(raw_record.get("table_name") or "") != "thread_message":
            continue
        record_id = str(raw_record.get("record_id") or "").strip()
        if not record_id:
            continue
        if record_id not in versions or versions[record_id] is None:
            versions[record_id] = raw_record.get("version")
    return versions


def _post_json(client: NotionOpusAPI, url: str, payload: dict[str, Any]) -> dict[str, Any]:
    # pylint: disable-next=protected-access
    response = client._scraper.post(  # noqa: SLF001 - reusing the client transport and auth headers
        url,
        # pylint: disable-next=protected-access
        headers=client._build_chat_history_headers(),  # noqa: SLF001 - shared header shape already exists on the client
        json=payload,
        timeout=(15, 60),
    )

    if response.status_code != 200:
        excerpt = (response.text or "").strip().replace("\n", " ")[:300]
        raise NotionUpstreamError(
            f"Notion chat-history sync returned HTTP {response.status_code}.",
            status_code=response.status_code,
            retriable=response.status_code >= 500 or response.status_code == 429,
            response_excerpt=excerpt,
        )

    try:
        body = response.json()
    except Exception as exc:
        raise NotionUpstreamError(
            "Notion chat-history sync returned invalid JSON.",
            status_code=502,
            retriable=True,
            response_excerpt=(response.text or "").strip()[:300],
        ) from exc

    if not isinstance(body, dict):
        raise NotionUpstreamError(
            "Notion chat-history sync returned an unexpected payload.",
            status_code=502,
            retriable=True,
            response_excerpt=(response.text or "").strip()[:300],
        )

    return body


def _threads_without_messages(bundle: dict[str, Any]) -> int:
    messages_by_thread: dict[str, int] = defaultdict(int)
    for message in bundle.get("messages", {}).values():
        thread_id = message.get("thread_id")
        if isinstance(thread_id, str) and thread_id.strip():
            messages_by_thread[thread_id] += 1
    for message in bundle.get("thread_messages", {}).values():
        thread_id = message.get("thread_id") if isinstance(message, dict) else None
        if isinstance(thread_id, str) and thread_id.strip():
            messages_by_thread[thread_id] += 1
    count = 0
    for thread_id, thread in bundle.get("threads", {}).items():
        if not thread.get("message_ids") and not messages_by_thread.get(thread_id):
            count += 1
    return count


def _collect_page_hydration_ids(page_bundle: dict[str, Any]) -> set[str]:
    ids: set[str] = set()
    for thread in page_bundle.get("threads", {}).values():
        ids.update(collect_hydration_message_ids(thread))
        raw = thread.get("raw") if isinstance(thread, dict) else None
        if raw:
            ids.update(collect_hydration_message_ids(raw))
    for message in page_bundle.get("messages", {}).values():
        ids.update(collect_hydration_message_ids(message))
        raw = message.get("raw") if isinstance(message, dict) else None
        if raw:
            ids.update(collect_hydration_message_ids(raw))
    for message_id in (page_bundle.get("thread_messages") or {}):
        if str(message_id).strip():
            ids.add(str(message_id).strip())
    return {message_id for message_id in ids if isinstance(message_id, str) and message_id.strip()}


def hydrate_message_ids_from_notion(
    client: NotionOpusAPI,
    message_ids: list[str] | set[str],
    *,
    fallback_thread_id: str | None = None,
    hydrate_batch_size: int = 50,
    skip_message_ids: set[str] | None = None,
) -> dict[str, Any]:
    """Hydrate specific Notion thread-message IDs into a chat-history bundle."""
    skip_ids = {
        str(message_id).strip()
        for message_id in (skip_message_ids or set())
        if str(message_id or "").strip()
    }
    candidate_ids = sorted(
        {
            str(message_id).strip()
            for message_id in message_ids
            if str(message_id or "").strip()
        }
    )
    clean_ids = [message_id for message_id in candidate_ids if message_id not in skip_ids]
    bundle: dict[str, Any] = {
        "threads": {},
        "messages": {},
        "thread_messages": {},
        "raw_records": [],
        "endpoint_counts": defaultdict(int),
    }
    hydration_batches = 0
    hydrated_messages_seen = 0
    hydration_failed_ids = 0

    for start_index in range(0, len(clean_ids), hydrate_batch_size):
        batch = clean_ids[start_index:start_index + hydrate_batch_size]
        hydrate_payload = {
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
        }
        hydrate_obj = _post_json(client, HYDRATE_ENDPOINT, hydrate_payload)
        bundle["endpoint_counts"]["syncRecordValuesSpaceInitial"] += 1
        hydration_batches += 1
        hydrate_bundle = import_chat_object(hydrate_obj)
        # Annotate provenance on raw records without treating private endpoints as public API.
        for raw_record in hydrate_bundle.get("raw_records") or []:
            if isinstance(raw_record, dict):
                raw_record.setdefault("source_kind", "notion_server_hydrate")
                raw_record.setdefault("source_endpoint", "syncRecordValuesSpaceInitial")
                raw_record.setdefault("workspace_id", getattr(client, "space_id", None))
        if fallback_thread_id:
            for message in hydrate_bundle.get("messages", {}).values():
                if not message.get("thread_id"):
                    message["thread_id"] = fallback_thread_id
            for message in hydrate_bundle.get("thread_messages", {}).values():
                if isinstance(message, dict) and not message.get("thread_id"):
                    message["thread_id"] = fallback_thread_id
        seen_after = set(hydrate_bundle.get("thread_messages", {})) | set(hydrate_bundle.get("messages", {}))
        hydration_failed_ids += sum(1 for message_id in batch if message_id not in seen_after)
        hydrated_messages_seen += len(hydrate_bundle.get("messages", {})) + len(
            {
                message_id
                for message_id in hydrate_bundle.get("thread_messages", {})
                if message_id not in hydrate_bundle.get("messages", {})
            }
        )
        _merge_bundle(bundle, hydrate_bundle)

    bundle["endpoint_counts"] = dict(bundle["endpoint_counts"])
    bundle["stats"] = {
        "hydration_candidate_ids": len(candidate_ids),
        "hydration_skipped_ids": len(candidate_ids) - len(clean_ids),
        "hydrated_message_ids": len(clean_ids),
        "hydration_batches": hydration_batches,
        "hydrated_messages_seen": hydrated_messages_seen,
        "hydration_failed_ids": hydration_failed_ids,
        "messages": len(bundle.get("messages", {})),
        "thread_messages": len(bundle.get("thread_messages", {})),
    }
    return bundle


def hydrate_thread_record_from_notion(client: NotionOpusAPI, thread_id: str) -> dict[str, Any]:
    payload = {
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
    hydrate_obj = _post_json(client, HYDRATE_ENDPOINT, payload)
    bundle = import_chat_object(hydrate_obj)
    for raw_record in bundle.get("raw_records") or []:
        if isinstance(raw_record, dict):
            raw_record.setdefault("source_kind", "notion_server_hydrate")
            raw_record.setdefault("source_endpoint", "syncRecordValuesSpaceInitial")
            raw_record.setdefault("workspace_id", getattr(client, "space_id", None))
    bundle["endpoint_counts"] = {"syncRecordValuesSpaceInitial": 1}
    return bundle


def hydrate_thread_from_notion(
    client: NotionOpusAPI,
    thread: dict[str, Any],
    *,
    hydrate_batch_size: int = 50,
    skip_message_ids: set[str] | None = None,
    existing_message_ids_lookup: ExistingMessageIdsLookup | None = None,
    fresh_message_ids_lookup: FreshMessageIdsLookup | None = None,
    candidate_versions: Mapping[str, int | None] | None = None,
) -> dict[str, Any]:
    """Hydrate only the messages referenced by one selected archived thread."""
    thread_id = str(thread.get("id") or "").strip() or None
    ids: set[str] = set()
    # Prefer explicit ID lists before deep scanning nested thread metadata.
    for key in ("message_ids", "messageIds", "thread_message_ids", "threadMessageIds"):
        value = thread.get(key)
        if isinstance(value, list):
            ids.update(str(item).strip() for item in value if str(item or "").strip())
    ids.update(collect_hydration_message_ids(thread))
    raw = thread.get("raw") if isinstance(thread.get("raw"), dict) else None
    if raw:
        ids.update(collect_hydration_message_ids(raw))
    thread_bundle: dict[str, Any] = {"threads": {}, "messages": {}, "thread_messages": {}, "raw_records": [], "endpoint_counts": {}}
    if thread_id:
        thread_bundle = hydrate_thread_record_from_notion(client, thread_id)
        hydrated_thread = thread_bundle.get("threads", {}).get(thread_id)
        if hydrated_thread:
            for key in ("message_ids", "messageIds", "thread_message_ids", "threadMessageIds"):
                value = hydrated_thread.get(key)
                if isinstance(value, list):
                    ids.update(str(item).strip() for item in value if str(item or "").strip())
            ids.update(collect_hydration_message_ids(hydrated_thread))
            hydrated_raw = hydrated_thread.get("raw") if isinstance(hydrated_thread.get("raw"), dict) else None
            if hydrated_raw:
                ids.update(collect_hydration_message_ids(hydrated_raw))
    # Never treat the thread id itself as a message id; that wastes Notion RPCs and
    # confuses parsers when the archive only has metadata.
    if thread_id:
        ids.discard(thread_id)

    resolved_versions = dict(candidate_versions or {})
    resolved_versions.update(_collect_candidate_versions(thread_bundle))
    skip_ids = _resolve_skip_message_ids(
        ids,
        skip_message_ids=skip_message_ids,
        existing_message_ids_lookup=existing_message_ids_lookup,
        candidate_versions=resolved_versions,
        fresh_message_ids_lookup=fresh_message_ids_lookup,
    )
    bundle = hydrate_message_ids_from_notion(
        client,
        ids,
        fallback_thread_id=thread_id,
        hydrate_batch_size=hydrate_batch_size,
        skip_message_ids=skip_ids,
    )
    if thread_bundle.get("threads") or thread_bundle.get("raw_records") or thread_bundle.get("thread_messages"):
        _merge_bundle(
            bundle,
            {
                "threads": thread_bundle.get("threads", {}),
                "messages": {},
                "thread_messages": thread_bundle.get("thread_messages", {}),
                "raw_records": thread_bundle.get("raw_records", []),
            },
        )
        for key, value in thread_bundle.get("endpoint_counts", {}).items():
            bundle["endpoint_counts"][key] = bundle["endpoint_counts"].get(key, 0) + value
        if "stats" in bundle:
            bundle["stats"]["hydration_batches"] = bundle["stats"].get("hydration_batches", 0) + sum(
                thread_bundle.get("endpoint_counts", {}).values()
            )
    fallback_text = str(thread.get("title") or thread.get("first_message_preview") or thread.get("last_message_preview") or "").strip()
    if thread_id and fallback_text:
        for message in bundle.get("messages", {}).values():
            if not message.get("thread_id"):
                message["thread_id"] = thread_id
            if message.get("thread_id") == thread_id and not str(message.get("text") or "").strip():
                message["text"] = fallback_text
    if "stats" not in bundle:
        bundle["stats"] = {}
    bundle["stats"]["thread_message_candidates"] = len(ids)
    return bundle


def sync_chat_history_from_notion(
    client: NotionOpusAPI,
    *,
    limit: int = 50,
    max_pages: int = 20,
    hydrate: bool = False,
    hydrate_batch_size: int = 50,
    skip_message_ids: set[str] | None = None,
    existing_message_ids_lookup: ExistingMessageIdsLookup | None = None,
    fresh_message_ids_lookup: FreshMessageIdsLookup | None = None,
    store: Any | None = None,
    persist: bool = False,
    advance_checkpoint: bool = True,
    cursor_name: str = "inference_transcripts",
) -> dict[str, Any]:
    """Read-only direct sync from Notion transcript RPCs into the local archive bundle.

    When ``store`` is provided and ``persist`` is True, durable sync_run/cursor state is
    maintained. Checkpoints advance only after successful persistence of fetched data.
    Server omission never deletes archived rows.
    """
    thread_parent_pointer = {
        "table": "space",
        "id": client.space_id,
        "spaceId": client.space_id,
    }
    workspace_id = str(getattr(client, "space_id", "") or "").strip() or "unknown"
    notion_user_id = str(getattr(client, "user_id", "") or getattr(client, "notion_user_id", "") or "").strip() or None
    account_key = str(getattr(client, "account_key", "") or "").strip() or None

    bundle: dict[str, Any] = {
        "threads": {},
        "messages": {},
        "thread_messages": {},
        "raw_records": [],
        "endpoint_counts": defaultdict(int),
        "workspace_id": workspace_id,
        "notion_user_id": notion_user_id,
    }
    seen_message_ids: set[str] = set()
    candidate_versions: dict[str, int | None] = {}
    cursor: str | None = None
    pages_scanned = 0
    stopped_reason = "completed"
    sync_run_id = str(uuid.uuid4())
    sync_error: str | None = None
    hydration_failed_ids = 0
    hydration_batches = 0
    hydrated_messages_seen = 0
    hydration_skipped_ids = 0
    hydrated_message_ids = 0
    message_ids: list[str] = []
    records_persisted = 0
    persisted = False
    checkpoint_advanced = False

    sync_run_started = False
    if store is not None and persist:
        try:
            store.begin_sync_run(
                sync_run_id,
                workspace_id=workspace_id,
                notion_user_id=notion_user_id,
                account_key=account_key,
                source_kind="notion_server",
            )
            sync_run_started = True
            prior_cursor = store.get_sync_cursor(
                cursor_name,
                workspace_id=workspace_id,
                account_key=account_key,
            )
            if prior_cursor:
                cursor = prior_cursor
        except Exception as exc:  # pragma: no cover - defensive around optional store wiring
            sync_error = f"sync_run_init_failed: {exc}"

    try:
        while pages_scanned < max_pages:
            payload: dict[str, Any] = {
                "threadParentPointer": thread_parent_pointer,
                "limit": limit,
                "includeWriterChats": False,
            }
            if cursor:
                payload["cursor"] = cursor

            page_obj = _post_json(client, TRANSCRIPTS_ENDPOINT, payload)
            pages_scanned += 1
            bundle["endpoint_counts"]["getInferenceTranscriptsForUser"] += 1

            page_bundle = import_chat_object(page_obj)
            for raw_record in page_bundle.get("raw_records") or []:
                if isinstance(raw_record, dict):
                    raw_record.setdefault("source_kind", "notion_server_transcripts")
                    raw_record.setdefault("source_endpoint", "getInferenceTranscriptsForUser")
                    raw_record.setdefault("workspace_id", workspace_id)
            _merge_bundle(bundle, page_bundle)
            page_ids = _collect_page_hydration_ids(page_bundle)
            seen_message_ids.update(page_ids)
            candidate_versions.update(_collect_candidate_versions(page_bundle))
            for message_id in page_ids:
                candidate_versions.setdefault(message_id, None)

            if not page_bundle.get("threads") and not page_bundle.get("messages") and not page_bundle.get("thread_messages"):
                stopped_reason = "empty_page"

            next_cursor = page_obj.get("nextCursor") or page_obj.get("next_cursor")
            has_more = bool(page_obj.get("hasMore"))
            if isinstance(next_cursor, str) and next_cursor.strip() and has_more:
                cursor = next_cursor.strip()
                continue
            cursor = next_cursor if isinstance(next_cursor, str) else None
            if stopped_reason != "empty_page":
                stopped_reason = "no_next_cursor" if not cursor else "has_more_false"
            break
        else:
            stopped_reason = "max_pages"

        message_ids = sorted(seen_message_ids)
        if hydrate:
            skip_ids = _resolve_skip_message_ids(
                message_ids,
                skip_message_ids=skip_message_ids,
                existing_message_ids_lookup=existing_message_ids_lookup,
                candidate_versions=candidate_versions,
                fresh_message_ids_lookup=fresh_message_ids_lookup,
            )
            hydrate_bundle = hydrate_message_ids_from_notion(
                client,
                message_ids,
                hydrate_batch_size=hydrate_batch_size,
                skip_message_ids=skip_ids,
            )
            _merge_bundle(bundle, hydrate_bundle)
            for key, value in hydrate_bundle.get("endpoint_counts", {}).items():
                bundle["endpoint_counts"][key] += value
            hydrate_stats = hydrate_bundle.get("stats", {})
            hydration_batches = int(hydrate_stats.get("hydration_batches") or 0)
            hydrated_messages_seen = int(hydrate_stats.get("hydrated_messages_seen") or 0)
            hydration_skipped_ids = int(hydrate_stats.get("hydration_skipped_ids") or 0)
            hydrated_message_ids = int(hydrate_stats.get("hydrated_message_ids") or 0)
            hydration_failed_ids = int(hydrate_stats.get("hydration_failed_ids") or 0)

        records_persisted = 0
        if persist and store is not None:
            imported = store.upsert_bundle(
                bundle,
                workspace_id=workspace_id,
                notion_user_id=notion_user_id,
                account_key=account_key,
                source_kind="notion_server",
                source_endpoint="getInferenceTranscriptsForUser",
            )
            bundle["persist_result"] = dict(imported)
            records_persisted = int(imported.get("messages_inserted", 0)) + int(
                imported.get("semantic_messages_inserted", 0)
            ) + int(imported.get("raw_records_inserted", 0))
            persisted = True
            # Partial hydration must not advance the durable checkpoint.
            allow_advance = bool(advance_checkpoint) and hydration_failed_ids == 0 and sync_error is None
            checkpoint_advanced = bool(
                store.advance_sync_cursor(
                    cursor_name,
                    cursor,
                    sync_run_id=sync_run_id,
                    workspace_id=workspace_id,
                    account_key=account_key,
                    allow_advance=allow_advance,
                )
            )
    except Exception as exc:
        sync_error = str(exc)
        raise
    finally:
        if store is not None and persist and sync_run_started:
            status = "error" if sync_error else ("partial" if hydration_failed_ids else "completed")
            if sync_error is None and persist and not persisted:
                status = "partial"
            try:
                store.finish_sync_run(
                    sync_run_id,
                    status=status,
                    pages_scanned=pages_scanned,
                    records_persisted=records_persisted if persist else 0,
                    hydration_failed=hydration_failed_ids,
                    hydration_partial=1 if hydration_failed_ids else 0,
                    checkpoint_advanced=checkpoint_advanced,
                    metrics={
                        "stopped_reason": stopped_reason,
                        "hydrate": bool(hydrate),
                        "threads": len(bundle.get("threads", {})),
                        "messages": len(bundle.get("messages", {})),
                        "thread_messages": len(bundle.get("thread_messages", {})),
                    },
                    error_text=sync_error,
                )
            except Exception:
                pass

    messages_seen = len(bundle["messages"])
    summary = {
        "pages_scanned": pages_scanned,
        "threads_seen": len(bundle["threads"]),
        "messages_seen": messages_seen,
        "thread_messages_seen": len(bundle.get("thread_messages", {})),
        "threads_without_messages": _threads_without_messages(bundle),
        "next_cursor": cursor,
        "stopped_reason": stopped_reason,
        "hydrate": bool(hydrate),
        "hydration_candidate_ids": len(message_ids),
        "hydration_skipped_ids": hydration_skipped_ids,
        "hydration_batches": hydration_batches,
        "hydrated_messages_seen": hydrated_messages_seen,
        "hydration_failed_ids": hydration_failed_ids,
        "sync_run_id": sync_run_id,
        "checkpoint_advanced": checkpoint_advanced,
        "persisted": persisted,
    }
    bundle["endpoint_counts"] = dict(bundle["endpoint_counts"])
    bundle["sync_summary"] = summary
    bundle["stats"] = {
        "pages_fetched": pages_scanned,
        "pages_scanned": pages_scanned,
        "threads": len(bundle["threads"]),
        "messages": messages_seen,
        "thread_messages": len(bundle.get("thread_messages", {})),
        "hydrated_message_ids": hydrated_message_ids if hydrate else 0,
        "hydration_candidate_ids": len(message_ids),
        "hydration_skipped_ids": hydration_skipped_ids,
        "hydration_batches": hydration_batches,
        "hydrated_messages_seen": hydrated_messages_seen,
        "hydration_failed_ids": hydration_failed_ids,
        "threads_without_messages": summary["threads_without_messages"],
        "next_cursor": cursor,
        "stopped_reason": stopped_reason,
        "hydrate": bool(hydrate),
        "sync_run_id": sync_run_id,
        "checkpoint_advanced": checkpoint_advanced,
    }
    return bundle
