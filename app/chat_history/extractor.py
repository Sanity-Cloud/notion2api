"""Chat-history record extraction and normalization helpers."""

from __future__ import annotations

import hashlib
import re
from typing import Any

THREAD_MESSAGE_FIELDS = (
    "messages",
    "message_ids",
    "thread_message_ids",
    "messageIds",
    "threadMessageIds",
    "conversation_messages",
    "conversationMessages",
    "records",
    "items",
)

MESSAGE_ID_FIELDS = ("id", "message_id", "messageId", "uuid")
MESSAGE_ROLE_FIELDS = ("role", "author_role", "authorRole", "type")
MESSAGE_TEXT_FIELDS = ("content", "text", "markdown", "message", "body")
MESSAGE_TEXT_NESTED_FIELDS = ("data", "properties")
THREAD_ID_FIELDS = ("thread_id", "threadId", "parent_id", "parentId", "conversation_id", "conversationId")
THREAD_UPDATED_FIELDS = ("updated_at", "updatedAt", "updated_time", "updatedTime", "last_edited_time", "lastEditedTime", "last_updated_time", "lastUpdatedTime")
THREAD_CREATED_FIELDS = ("created_time", "createdTime", "created_at", "createdAt", "created_at_ms", "createdAtMs", "startedAt")
SECRET_KEY_FRAGMENTS = ("token", "cookie", "authorization", "api_key", "apikey", "secret", "password", "session")
THREAD_TITLE_FIELDS = ("title", "name", "subject")
HIDDEN_MESSAGE_TYPES = {
    "agent-instruction-state",
    "agent-search-query-generation",
    "agent-tool-result",
    "agent-turn-full-record-map",
    "thinking",
    "title",
}
# Step types observed in Notion server/desktop evidence. Unknown types are retained as-is.
KNOWN_STEP_TYPES = HIDDEN_MESSAGE_TYPES | {
    "agent-inference",
    "user",
    "context",
    "computer-file",
    "config",
    "updated-config",
    "workflow",
}
VERSION_FIELDS = ("version", "last_version", "lastVersion")
HYDRATION_SCAN_FIELDS = (
    "recordMap",
    "body",
    "data",
    "result",
    "results",
    "properties",
    "value",
    "values",
    "transcripts",
    "threads",
    "thread_messages",
    "threadMessages",
    "children",
    "blocks",
)


def record_value(record: Any) -> dict[str, Any]:
    if not isinstance(record, dict):
        return {}
    value = record.get("value")
    return value if isinstance(value, dict) else record


def record_maps(obj: Any):
    if not isinstance(obj, dict):
        return
    record_map = obj.get("recordMap")
    if isinstance(record_map, dict):
        yield record_map
    for key in ("body", "data", "result"):
        nested = obj.get(key)
        if isinstance(nested, dict):
            yield from record_maps(nested)


def _first_str(value: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
    return None


def _first_scalar_text(value: dict[str, Any], keys: tuple[str, ...]) -> str | None:
    for key in keys:
        candidate = value.get(key)
        if isinstance(candidate, str) and candidate.strip():
            return candidate.strip()
        if isinstance(candidate, (int, float)) and not isinstance(candidate, bool):
            return str(candidate)
    return None


def _coerce_text(value: Any) -> str:
    chunks: list[str] = []
    _collect_text(value, chunks)
    unique: list[str] = []
    seen: set[str] = set()
    for chunk in chunks:
        key = " ".join(str(chunk).split())
        if key and key not in seen:
            seen.add(key)
            unique.append(str(chunk).strip())
    return "\n".join(unique)


def _strip_internal_markup(text: str) -> str:
    return re.sub(r"<lang\b[^>]*/>", "", text or "").strip()


def message_model_metadata(value: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    step = value.get("step") if isinstance(value.get("step"), dict) else value
    if not isinstance(step, dict):
        return {}
    values = step.get("value") if isinstance(step.get("value"), list) else []
    notion_step_model = _first_scalar_text(step, ("model",)) or ""
    notion_model_name = _first_scalar_text(step, ("notionModelName",)) or ""
    model_provider = _first_scalar_text(step, ("modelProvider",)) or ""
    for part in values:
        if not isinstance(part, dict):
            continue
        notion_model_name = notion_model_name or _first_scalar_text(part, ("notionModelName",)) or ""
        model_provider = model_provider or _first_scalar_text(part, ("modelProvider",)) or ""
    actual_model = notion_model_name or notion_step_model
    if not any((actual_model, notion_step_model, notion_model_name, model_provider)):
        return {}
    data_value = value.get("data")
    inference_id = data_value.get("inference_id") if isinstance(data_value, dict) else ""
    out = {
        "actual_model": actual_model,
        "notion_step_model": notion_step_model,
        "notion_model_name": notion_model_name,
        "model_provider": model_provider,
        "source_step_type": str(step.get("type") or value.get("type") or ""),
        "trace_id": _first_scalar_text(step, ("traceId",)) or "",
        "inference_id": inference_id or "",
    }
    return {k: v for k, v in out.items() if v not in (None, "", [], {})}


def visible_message_text(value: dict[str, Any]) -> str:
    """Return only the text Notion shows as chat content, excluding thinking/tool bookkeeping."""
    if isinstance(value.get("step"), dict):
        return visible_message_text(value["step"])
    message_type = str(value.get("type") or "").strip()
    if message_type == "agent-inference" and isinstance(value.get("value"), list):
        chunks: list[str] = []
        for part in value["value"]:
            if not isinstance(part, dict):
                continue
            part_type = str(part.get("type") or "").strip()
            content = part.get("content")
            if part_type == "text" and isinstance(content, str):
                text = _strip_internal_markup(content)
                if text:
                    chunks.append(text)
        return "\n\n".join(chunks).strip()
    if message_type in HIDDEN_MESSAGE_TYPES:
        return ""
    text = _coerce_text({key: value.get(key) for key in MESSAGE_TEXT_FIELDS if key in value}) or _coerce_text(value)
    return _strip_internal_markup(text)


def visible_message_role(value: dict[str, Any]) -> str | None:
    if isinstance(value.get("step"), dict):
        return visible_message_role(value["step"])
    message_type = str(value.get("type") or "").strip()
    if message_type == "agent-inference":
        return "assistant"
    if message_type == "user":
        return "user"
    role = _first_str(value, MESSAGE_ROLE_FIELDS)
    if role == "agent-inference":
        return "assistant"
    if role in HIDDEN_MESSAGE_TYPES:
        return None
    return role


def step_payload(value: dict[str, Any]) -> dict[str, Any]:
    """Return the semantic step object (unwrap nested step when present)."""
    if not isinstance(value, dict):
        return {}
    step = value.get("step")
    return step if isinstance(step, dict) else value


def message_step_type(value: dict[str, Any]) -> str:
    step = step_payload(value)
    step_type = str(step.get("type") or value.get("type") or "").strip()
    return step_type or "unknown"


def _coerce_version(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str) and value.strip().lstrip("-").isdigit():
        return int(value.strip())
    return None


def record_versions(raw: Any, value: dict[str, Any] | None = None) -> tuple[int | None, int | None]:
    """Extract (version, last_version) from a Notion record wrapper or value."""
    containers: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        containers.append(raw)
        nested_value = raw.get("value")
        if isinstance(nested_value, dict):
            containers.append(nested_value)
    if isinstance(value, dict):
        containers.append(value)
        step = value.get("step")
        if isinstance(step, dict):
            containers.append(step)
    version: int | None = None
    last_version: int | None = None
    for container in containers:
        if version is None:
            version = _coerce_version(container.get("version"))
        if last_version is None:
            last_version = _coerce_version(container.get("last_version"))
            if last_version is None:
                last_version = _coerce_version(container.get("lastVersion"))
    return version, last_version


def extract_message_parts(value: dict[str, Any]) -> list[dict[str, Any]]:
    """Preserve ordinal + part_type + raw/text for agent-inference.value[] (and similar)."""
    step = step_payload(value)
    values = step.get("value") if isinstance(step.get("value"), list) else None
    if values is None and isinstance(value.get("value"), list):
        values = value.get("value")
    if not isinstance(values, list):
        return []
    parts: list[dict[str, Any]] = []
    for index, part in enumerate(values):
        if not isinstance(part, dict):
            parts.append(
                {
                    "ordinal": index,
                    "part_type": "unknown",
                    "text": str(part) if part is not None else None,
                    "content": part,
                    "raw": {"value": part},
                }
            )
            continue
        part_type = str(part.get("type") or "").strip() or "unknown"
        content = part.get("content")
        text = None
        if isinstance(content, str):
            text = content
        elif content is not None and not isinstance(content, (dict, list)):
            text = str(content)
        parts.append(
            {
                "ordinal": index,
                "part_type": part_type,
                "text": text,
                "content": content,
                "raw": part,
            }
        )
    return parts


def _semantic_role(step_type: str, visible_role: str | None) -> str | None:
    if visible_role:
        return visible_role
    if step_type == "user":
        return "user"
    if step_type == "agent-inference":
        return "assistant"
    if step_type in HIDDEN_MESSAGE_TYPES:
        return None
    return step_type if step_type and step_type != "unknown" else None


def normalize_thread_message_record(
    message_id: str | None,
    raw: dict[str, Any],
    fallback_thread_id: str | None = None,
) -> dict[str, Any] | None:
    """Normalize a thread_message into a durable semantic record.

    Hidden and unknown step types are retained. Visibility is a projection flag and
    never gates whether the record exists.
    """
    value = record_value(raw)
    if not value and not isinstance(raw, dict):
        return None
    step = step_payload(value)
    resolved_id = (
        message_id
        or _first_str(value, MESSAGE_ID_FIELDS)
        or _first_str(step, MESSAGE_ID_FIELDS)
    )
    step_type = message_step_type(value)
    if step_type not in KNOWN_STEP_TYPES and step_type != "unknown":
        # Retain unknown types losslessly under their observed name.
        pass
    text = visible_message_text(value)
    if not text:
        for key in MESSAGE_TEXT_NESTED_FIELDS:
            nested = value.get(key)
            if isinstance(nested, dict):
                text = visible_message_text(nested)
                if text:
                    break
    role = visible_message_role(value)
    visible = bool(text and role is not None)
    if not resolved_id:
        if not (text or step_type != "unknown"):
            return None
        resolved_id = _synthetic_message_id(fallback_thread_id, text or step_type)
    thread_id = (
        _first_str(value, THREAD_ID_FIELDS)
        or _first_str(step, THREAD_ID_FIELDS)
        or fallback_thread_id
    )
    created_at = _first_scalar_text(value, THREAD_CREATED_FIELDS) or _first_scalar_text(
        step, THREAD_CREATED_FIELDS
    )
    version, last_version = record_versions(raw, value)
    model_metadata = message_model_metadata(value)
    data_value = value.get("data") if isinstance(value.get("data"), dict) else {}
    inference_id = ""
    if isinstance(data_value, dict):
        inference_id = str(data_value.get("inference_id") or "")
    inference_id = inference_id or str(model_metadata.get("inference_id") or "")
    trace_id = str(model_metadata.get("trace_id") or "") or (
        _first_scalar_text(step, ("traceId", "trace_id")) or ""
    )
    return {
        "id": str(resolved_id),
        "thread_id": thread_id,
        "step_type": step_type,
        "visible": visible,
        "role": role,
        "semantic_role": _semantic_role(step_type, role),
        "text": text or "",
        "created_time": created_at or value.get("created_time") or value.get("createdTime"),
        "version": version,
        "last_version": last_version,
        "parts": extract_message_parts(value),
        "actual_model": model_metadata.get("actual_model"),
        "model_provider": model_metadata.get("model_provider"),
        "notion_model_name": model_metadata.get("notion_model_name"),
        "model_metadata": model_metadata,
        "inference_id": inference_id or None,
        "trace_id": trace_id or None,
        "raw": value,
        "raw_wrapper": raw if isinstance(raw, dict) else {"value": value},
    }


def visible_transcript_message(record: dict[str, Any] | None) -> dict[str, Any] | None:
    """Project a semantic record into the legacy visible chat_messages shape."""
    if not isinstance(record, dict) or not record.get("visible"):
        return None
    role = record.get("role")
    text = str(record.get("text") or "")
    if not role or not text:
        return None
    return {
        "id": record["id"],
        "thread_id": record.get("thread_id"),
        "role": role,
        "text": text,
        "created_time": record.get("created_time"),
        "actual_model": record.get("actual_model"),
        "model_provider": record.get("model_provider"),
        "notion_model_name": record.get("notion_model_name"),
        "model_metadata": record.get("model_metadata") or {},
        "raw": record.get("raw") or {},
        "version": record.get("version"),
        "last_version": record.get("last_version"),
        "step_type": record.get("step_type"),
    }


def _collect_text(value: Any, out: list[str], depth: int = 0) -> None:
    if depth > 8:
        return
    if isinstance(value, str):
        if value.strip():
            out.append(value.strip())
        return
    if isinstance(value, list):
        for item in value:
            if isinstance(item, list) and item and isinstance(item[0], str):
                if item[0].strip():
                    out.append(item[0].strip())
            else:
                _collect_text(item, out, depth + 1)
        return
    if isinstance(value, dict):
        for key in ("text", "plain_text", "content", "message", "prompt", "response", "markdown", "title", "body"):
            candidate = value.get(key)
            if isinstance(candidate, str) and candidate.strip():
                out.append(candidate.strip())
        props = value.get("properties")
        if isinstance(props, dict):
            for prop in props.values():
                _collect_text(prop, out, depth + 1)
        for key in ("parts", "children", "value", "values", "blocks", "rich_text"):
            candidate = value.get(key)
            if isinstance(candidate, (list, dict)):
                _collect_text(candidate, out, depth + 1)


def _extract_id(candidate: Any) -> str | None:
    if isinstance(candidate, str) and candidate.strip():
        return candidate.strip()
    if isinstance(candidate, dict):
        for key in MESSAGE_ID_FIELDS:
            value = candidate.get(key)
            if isinstance(value, str) and value.strip():
                return value.strip()
        pointer = candidate.get("pointer")
        if isinstance(pointer, dict):
            value = pointer.get("id")
            if isinstance(value, str) and value.strip():
                return value.strip()
        value = candidate.get("value")
        if isinstance(value, dict):
            return _extract_id(value)
    return None


def _extract_ids(candidate: Any, depth: int = 0) -> list[str]:
    if depth > 6:
        return []
    direct = _extract_id(candidate)
    if direct:
        return [direct]
    ids: list[str] = []
    if isinstance(candidate, list):
        for item in candidate:
            ids.extend(_extract_ids(item, depth + 1))
    elif isinstance(candidate, dict):
        thread_message_map = candidate.get("thread_message")
        if isinstance(thread_message_map, dict):
            ids.extend(str(key) for key in thread_message_map.keys() if str(key).strip())
            for record in thread_message_map.values():
                ids.extend(_extract_ids(record, depth + 1))
        record_map = candidate.get("recordMap")
        if isinstance(record_map, dict):
            ids.extend(_extract_ids(record_map, depth + 1))
        for key in THREAD_MESSAGE_FIELDS + ("value", "values"):
            nested = candidate.get(key)
            if isinstance(nested, (list, dict)):
                ids.extend(_extract_ids(nested, depth + 1))
    return _dedupe(ids)


def _dedupe(values: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for value in values:
        value = str(value or "").strip()
        if value and value not in seen:
            seen.add(value)
            out.append(value)
    return out


def _synthetic_message_id(thread_id: str | None, text: str) -> str:
    digest = hashlib.sha256(f"{thread_id or ''}\n{text}".encode("utf-8", errors="replace")).hexdigest()[:24]
    return f"synthetic-{digest}"


def extract_message_ids(value: dict[str, Any]) -> list[str]:
    ids: list[str] = []
    for key in THREAD_MESSAGE_FIELDS:
        if key in value:
            ids.extend(_extract_ids(value.get(key)))
    return _dedupe(ids)


def collect_hydration_message_ids(value: Any, depth: int = 0) -> list[str]:
    """Collect nested Notion thread-message IDs without treating container IDs as messages."""
    if depth > 8:
        return []
    ids: list[str] = []
    if isinstance(value, list):
        for item in value:
            ids.extend(collect_hydration_message_ids(item, depth + 1))
        return _dedupe(ids)
    if not isinstance(value, dict):
        return []

    for key in THREAD_MESSAGE_FIELDS:
        if key in value:
            ids.extend(_extract_ids(value.get(key)))

    record_map = value.get("recordMap")
    if isinstance(record_map, dict):
        thread_message_map = record_map.get("thread_message")
        if isinstance(thread_message_map, dict):
            ids.extend(str(key) for key in thread_message_map.keys() if str(key).strip())

    for key in HYDRATION_SCAN_FIELDS:
        nested = value.get(key)
        if isinstance(nested, (dict, list)):
            ids.extend(collect_hydration_message_ids(nested, depth + 1))

    return _dedupe(ids)


def normalize_thread(thread_id: str | None, raw: dict[str, Any]) -> dict[str, Any] | None:
    value = record_value(raw)
    resolved_id = thread_id or _first_scalar_text(value, ("id", "thread_id", "threadId", "uuid"))
    if not resolved_id:
        return None
    updated_at = _first_scalar_text(value, THREAD_UPDATED_FIELDS)
    created_at = _first_scalar_text(value, THREAD_CREATED_FIELDS)
    data_value = value.get("data")
    data_title = data_value.get("title") if isinstance(data_value, dict) else None
    return {
        "id": str(resolved_id),
        "title": _first_scalar_text(value, THREAD_TITLE_FIELDS) or data_title,
        "created_time": created_at,
        "last_edited_time": updated_at,
        "updated_at": updated_at,
        "alive": value.get("alive") if isinstance(value.get("alive"), bool) else None,
        "message_ids": extract_message_ids(value),
        "raw": value,
    }


def normalize_message(message_id: str | None, raw: dict[str, Any], fallback_thread_id: str | None = None) -> dict[str, Any] | None:
    """Visible-transcript projection. Hidden/unknown steps are excluded here by design.

    Prefer normalize_thread_message_record() for lossless durable storage.
    """
    record = normalize_thread_message_record(message_id, raw, fallback_thread_id=fallback_thread_id)
    return visible_transcript_message(record)


def _iter_collection(obj: Any, names: tuple[str, ...]):
    if not isinstance(obj, dict):
        return
    for name in names:
        collection = obj.get(name)
        if isinstance(collection, list):
            for item in collection:
                if isinstance(item, dict):
                    yield None, item
        elif isinstance(collection, dict):
            for key, item in collection.items():
                if isinstance(item, dict):
                    yield str(key), item
    for key in ("body", "data", "result"):
        nested = obj.get(key)
        if isinstance(nested, dict):
            yield from _iter_collection(nested, names)


def _ensure_bundle_collections(bundle: dict[str, Any]) -> None:
    bundle.setdefault("threads", {})
    bundle.setdefault("messages", {})
    bundle.setdefault("thread_messages", {})
    bundle.setdefault("raw_records", [])


def _append_raw_record(
    bundle: dict[str, Any],
    *,
    table_name: str,
    record_id: str,
    raw: Any,
    source_kind: str = "notion_payload",
) -> None:
    value = record_value(raw) if isinstance(raw, dict) else {}
    version, last_version = record_versions(raw, value if isinstance(value, dict) else None)
    space_id = None
    if isinstance(raw, dict):
        space_id = raw.get("spaceId") or raw.get("space_id")
        if space_id is None and isinstance(raw.get("pointer"), dict):
            space_id = raw["pointer"].get("spaceId") or raw["pointer"].get("space_id")
    if space_id is None and isinstance(value, dict):
        space_id = value.get("spaceId") or value.get("space_id")
    bundle.setdefault("raw_records", []).append(
        {
            "table_name": table_name,
            "record_id": str(record_id),
            "version": version,
            "last_version": last_version,
            "workspace_id": str(space_id).strip() if space_id not in (None, "") else None,
            "raw": raw if isinstance(raw, dict) else {"value": raw},
            "source_kind": source_kind,
        }
    )


def _ingest_thread_message(
    bundle: dict[str, Any],
    message_id: str | None,
    raw: dict[str, Any],
    *,
    fallback_thread_id: str | None = None,
) -> dict[str, Any] | None:
    record = normalize_thread_message_record(message_id, raw, fallback_thread_id=fallback_thread_id)
    if not record:
        return None
    bundle.setdefault("thread_messages", {})[record["id"]] = record
    _append_raw_record(
        bundle,
        table_name="thread_message",
        record_id=record["id"],
        raw=raw if isinstance(raw, dict) else record.get("raw_wrapper") or {"value": record.get("raw")},
    )
    visible = visible_transcript_message(record)
    if visible:
        bundle["messages"][visible["id"]] = visible
    return record


def _merge_thread_candidate(bundle: dict[str, Any], fallback_id: str | None, candidate: dict[str, Any]) -> None:
    _ensure_bundle_collections(bundle)
    thread = normalize_thread(fallback_id, candidate)
    if not thread:
        return
    thread_id = thread["id"]
    direct_message_ids = list(thread.get("message_ids") or [])
    value = record_value(candidate)
    _append_raw_record(bundle, table_name="thread", record_id=thread_id, raw=candidate)
    for field in THREAD_MESSAGE_FIELDS:
        items = value.get(field)
        if not isinstance(items, list):
            continue
        for item in items:
            if not isinstance(item, dict):
                continue
            record = _ingest_thread_message(bundle, None, item, fallback_thread_id=thread_id)
            if record:
                direct_message_ids.append(record["id"])
    thread["message_ids"] = _dedupe(direct_message_ids)
    bundle["threads"][thread_id] = thread


def _has_thread_shape(value: dict[str, Any]) -> bool:
    return any(field in value for field in THREAD_TITLE_FIELDS) and any(field in value for field in ("id", "thread_id", "threadId", "uuid"))


def _has_message_shape(value: dict[str, Any]) -> bool:
    return any(field in value for field in MESSAGE_TEXT_FIELDS + MESSAGE_ROLE_FIELDS + THREAD_ID_FIELDS) or (
        message_step_type(value) != "unknown"
    )


def merge_records_into_bundle(bundle: dict[str, Any], obj: Any) -> None:
    """Extract threads/messages from Notion payloads.

    Prefer structured `recordMap` / transcript collections. Skip the deep tree walk
    when those hit, because Notion hydrate payloads embed huge encrypted blobs that
    make full walks dominate CPU for no additional records.

    Bundle keys:
      - threads / messages: visible-transcript projection (legacy compatible)
      - thread_messages: lossless semantic thread_message records (incl. hidden/unknown)
      - raw_records: version-aware raw Notion record mirrors
    """
    _ensure_bundle_collections(bundle)
    structured_hits = 0
    for record_map in record_maps(obj):
        for thread_id, record in (record_map.get("thread") or {}).items():
            thread = normalize_thread(str(thread_id), record_value(record))
            if thread:
                bundle["threads"][thread["id"]] = thread
                _append_raw_record(bundle, table_name="thread", record_id=thread["id"], raw=record)
                structured_hits += 1
        for message_id, record in (record_map.get("thread_message") or {}).items():
            raw_record = record if isinstance(record, dict) else {"value": record}
            if _ingest_thread_message(bundle, str(message_id), raw_record):
                # Count semantic hits even when the step is hidden from the visible transcript.
                structured_hits += 1
            else:
                # Still mirror raw when normalization cannot resolve an id.
                _append_raw_record(bundle, table_name="thread_message", record_id=str(message_id), raw=raw_record)
                structured_hits += 1

    if isinstance(obj, dict):
        for fallback_id, candidate in _iter_collection(obj, ("transcripts", "threads")):
            before = (
                len(bundle["threads"])
                + len(bundle["messages"])
                + len(bundle.get("thread_messages", {}))
            )
            _merge_thread_candidate(bundle, fallback_id, candidate)
            after = (
                len(bundle["threads"])
                + len(bundle["messages"])
                + len(bundle.get("thread_messages", {}))
            )
            if after > before:
                structured_hits += 1
        for fallback_id, candidate in _iter_collection(obj, ("messages", "thread_messages", "threadMessages")):
            if _ingest_thread_message(bundle, fallback_id, candidate):
                structured_hits += 1

    if structured_hits > 0:
        return

    def walk(value: Any, fallback_thread_id: str | None = None) -> None:
        if isinstance(value, dict):
            next_thread_id = fallback_thread_id
            if _has_thread_shape(value):
                thread = normalize_thread(None, value)
                if thread:
                    _merge_thread_candidate(bundle, thread["id"], value)
                    next_thread_id = thread["id"]
            elif _has_message_shape(value):
                if _ingest_thread_message(bundle, None, value, fallback_thread_id=fallback_thread_id):
                    return

            for nested in value.values():
                if isinstance(nested, (dict, list)):
                    walk(nested, next_thread_id)
        elif isinstance(value, list):
            for item in value:
                walk(item, fallback_thread_id)

    walk(obj)


def extract_chat_bundle(obj: Any) -> dict[str, Any]:
    bundle: dict[str, Any] = {"threads": {}, "messages": {}, "thread_messages": {}, "raw_records": []}
    merge_records_into_bundle(bundle, obj)
    return bundle


def redact_secrets(value: Any, depth: int = 0) -> Any:
    if depth > 6:
        return "[redacted-depth-limit]"
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            lowered = str(key).lower()
            if any(fragment in lowered for fragment in SECRET_KEY_FRAGMENTS):
                out[str(key)] = "[redacted]"
            else:
                out[str(key)] = redact_secrets(item, depth + 1)
        return out
    if isinstance(value, list):
        return [redact_secrets(item, depth + 1) for item in value[:20]]
    return value


def describe_thread_record(thread: dict[str, Any] | None, messages: list[dict[str, Any]] | None = None) -> dict[str, Any]:
    thread = thread or {}
    messages = messages or []
    thread_raw_any = thread.get("raw")
    thread_raw: dict[str, Any] = thread_raw_any if isinstance(thread_raw_any, dict) else {}

    raw_fields = {str(key) for key in thread_raw.keys()}
    for message in messages:
        message_raw_any = message.get("raw")
        message_raw: dict[str, Any] = message_raw_any if isinstance(message_raw_any, dict) else {}
        raw_fields.update(str(key) for key in message_raw.keys())

    known_fields = [field for field in THREAD_MESSAGE_FIELDS if field in thread_raw]
    if messages:
        for field in ("id", "thread_id", "role", "text", "created_time", "raw"):
            if any(message.get(field) not in (None, "", []) for message in messages):
                known_fields.append(field)

    thread_sample = {key: value for key, value in thread.items() if key != "messages"}

    return {
        "thread_exists": bool(thread),
        "message_count": len(messages),
        "hydrated": bool(messages),
        "raw_fields_seen": sorted(raw_fields),
        "known_message_fields_found": _dedupe(known_fields),
        "sample": {
            "thread": redact_secrets(thread_sample),
            "messages": [redact_secrets(message) for message in messages[:3]],
        },
    }
