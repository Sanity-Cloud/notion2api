import json
import re
from typing import Any, Generator

import requests

from app.logger import logger

# text Notion text/text
# 1. text <lang ...>...</lang> text_strip_lang_tags text
_RE_LANG_FULL = re.compile(r"<lang\b[^>]*>(.*?)</lang>", re.DOTALL)
# 2. text <lang ...> text
_RE_LANG_OPEN = re.compile(r"<lang\b[^>]*>")
# 3. text </lang> text
_RE_LANG_CLOSE = re.compile(r"</lang>")
# 4. Notion text primary="zh-CN" text primary="zh" text primary="en"
#    text
_RE_PRIMARY_ATTR = re.compile(r'\bprimary="[a-zA-Z\-]{1,15}"\s*')
# 5. text > text "> text -CN"> text
_RE_ATTR_TAIL = re.compile(r'^-?[a-zA-Z]{0,4}"\s*>\s*')
_RE_PRIMARY_START = re.compile(r"\bprimary\b", re.IGNORECASE)

SEARCH_PATH_KEYWORDS = ("search", "web", "query", "source", "citation", "tool")
SEARCH_TYPE_KEYWORDS = ("search", "web", "tool", "citation")
LINE_DEBUG_KEYWORDS = ("queries", "category", "sources", "citations", "questions")
SEARCH_VALUE_KEYS = (
    "queries",
    "query",
    "questions",
    "category",
    "sources",
    "citations",
    "results",
    "url",
    "urls",
    "href",
    "search",
    "web",
    "tool",
    "toolname",
    "tooltype",
    "internal",
)

# ---- text Notion o:"a" patch text v.type text ----
# text type text
_THINKING_TYPES = ("agent-inference", "thinking", "reasoning", "inference")
# text/text type text
_TOOL_TYPES = ("agent-tool-result", "tool_use", "tool", "search", "web", "citation")

# text
SEG_THINKING = "thinking"
SEG_TOOL = "tool"
SEG_CONTENT = "content"
SEG_META = "meta"

FINAL_STEP_PRIORITIES: dict[str, int] = {
    "markdown-chat": 400,
    "text": 350,
    "agent-inference": 300,
    "title": 50,
}


def _strip_lang_tags(text: str, in_tag: list[bool]) -> str:
    """Strip Notion internal <lang ...> tags, including cross-chunk broken tags."""
    result = []
    i = 0
    while i < len(text):
        if in_tag[0]:
            end = text.find(">", i)
            if end == -1:
                break
            in_tag[0] = False
            i = end + 1
            continue

        lang_start = text.find("<lang", i)
        close_start = text.find("</lang>", i)
        candidates = [(pos, typ) for pos, typ in [(lang_start, "open"), (close_start, "close")] if pos != -1]
        if not candidates:
            result.append(text[i:])
            break

        next_pos, typ = min(candidates, key=lambda x: x[0])
        result.append(text[i:next_pos])

        if typ == "close":
            i = next_pos + len("</lang>")
            continue

        end = text.find(">", next_pos)
        if end == -1:
            in_tag[0] = True
            break
        i = end + 1

    return "".join(result)


def _clean_notion_markup(text: str) -> str:
    """
    text _strip_lang_tags text Notion text

    text
    1. text <lang ...>text</lang>text_strip_lang_tags text
    2. text </lang> text
    3. text <lang ...> text _strip_lang_tags text
    4. primary="zh-CN" text
    5. text -CN"> text en"> text
    """
    # text <lang ...>text</lang>text
    text = _RE_LANG_FULL.sub(r"\1", text)
    # text </lang>
    text = _RE_LANG_CLOSE.sub("", text)
    # text <lang ...> text
    text = _RE_LANG_OPEN.sub("", text)
    # text primary="zh-CN" text
    text = _RE_PRIMARY_ATTR.sub("", text)
    # text -CN"> text "> text en">
    text = _RE_ATTR_TAIL.sub("", text)
    return text



def _strip_primary_attr_fragments(text: str, in_primary_attr: list[bool]) -> str:
    """Strip fragmented `primary=...` attribute pieces leaked by stream chunks."""
    out: list[str] = []
    i = 0

    while i < len(text):
        if in_primary_attr[0]:
            ch = text[i]
            if ch in ">\r\n":
                in_primary_attr[0] = False
                i += 1
                continue
            if ch.isalpha() or ch in '-_="\'/: ':
                i += 1
                continue
            in_primary_attr[0] = False
            continue

        m = _RE_PRIMARY_START.search(text, i)
        if not m:
            out.append(text[i:])
            break

        start = m.start()
        out.append(text[i:start])

        j = m.end()
        while j < len(text) and text[j].isspace():
            j += 1

        if j < len(text) and text[j] not in ("=", "\"", "'"):
            out.append(text[start:m.end()])
            i = m.end()
            continue

        if j < len(text) and text[j] == "=":
            j += 1
            while j < len(text) and text[j].isspace():
                j += 1

        if j < len(text) and text[j] in ("\"", "'"):
            quote = text[j]
            j += 1
            while j < len(text) and (text[j].isalpha() or text[j] in "-_"):
                j += 1
            if j < len(text) and text[j] == quote:
                j += 1
        else:
            while j < len(text) and (text[j].isalpha() or text[j] in "-_"):
                j += 1

        while j < len(text) and text[j] in " />":
            j += 1

        if j >= len(text):
            in_primary_attr[0] = True
            break

        i = j

    return "".join(out)

def _normalize_path(patch: dict[str, Any]) -> str:
    for key in ("path", "p", "pointer", "at"):
        if key not in patch:
            continue
        raw = patch.get(key)
        if isinstance(raw, (list, tuple)):
            return "/".join(str(part) for part in raw)
        return str(raw)
    return ""


def _extract_segment_index(path: str) -> int | None:
    parts = [part for part in path.split("/") if part]
    if len(parts) < 2 or parts[0] != "s":
        return None
    try:
        return int(parts[1])
    except Exception:
        return None


def _extract_value_index(path: str) -> int | None:
    """text /s/N/value/M/... text path text value block text Mtext"""
    parts = [p for p in path.split("/") if p]
    for i, part in enumerate(parts):
        if part == "value" and i + 1 < len(parts):
            try:
                return int(parts[i + 1])
            except ValueError:
                return None
    return None


def _extract_value_add_index(path: str) -> int | None:
    """
    text `o:"a"` text `/s/N/value/<idx|->` text value block text
    text value block text `/content` text
    """
    parts = [p for p in path.split("/") if p]
    if len(parts) != 4:
        return None
    if parts[0] != "s" or parts[2] != "value":
        return None
    idx_raw = parts[3]
    if idx_raw == "-":
        return -1
    try:
        return int(idx_raw)
    except ValueError:
        return None


def _truncate_json(value: Any, max_len: int = 2000) -> str:
    try:
        raw = json.dumps(value, ensure_ascii=False)
    except Exception:
        raw = str(value)
    if len(raw) <= max_len:
        return raw
    return raw[:max_len] + "...(truncated)"


def _contains_search_keys(value: Any) -> bool:
    if isinstance(value, dict):
        for key, val in value.items():
            key_lower = str(key).lower()
            if any(token in key_lower for token in SEARCH_VALUE_KEYS):
                return True
            if _contains_search_keys(val):
                return True
        return False

    if isinstance(value, list):
        return any(_contains_search_keys(item) for item in value)

    return False


def _append_query(out: dict[str, Any], query: str) -> None:
    q = query.strip()
    if q:
        out.setdefault("queries", []).append(q)


def _append_source(out: dict[str, Any], source: dict[str, Any]) -> None:
    title = str(source.get("title", "") or "").strip()
    url = str(source.get("url", "") or "").strip()
    snippet = str(source.get("snippet", "") or "").strip()
    if not title and not url:
        return
    if url.startswith("user://") or title.startswith("user://"):
        return

    entry: dict[str, str] = {}
    if title:
        entry["title"] = title
    if url:
        entry["url"] = url
    if snippet:
        entry["snippet"] = snippet
    out.setdefault("sources", []).append(entry)


def _collect_search_metadata(value: Any, out: dict[str, Any]) -> None:
    if isinstance(value, dict):
        lowered = {str(k).lower(): v for k, v in value.items()}

        if isinstance(lowered.get("queries"), list):
            for item in lowered["queries"]:
                if isinstance(item, str):
                    _append_query(out, item)

        if isinstance(lowered.get("questions"), list):
            for item in lowered["questions"]:
                if isinstance(item, str):
                    _append_query(out, item)

        for single_query_key in ("query", "search_query", "searchquery"):
            query_val = lowered.get(single_query_key)
            if isinstance(query_val, str):
                _append_query(out, query_val)

        category = lowered.get("category")
        if isinstance(category, str) and category.strip():
            out.setdefault("categories", []).append(category.strip())

        for source_key in ("sources", "citations", "results"):
            source_items = lowered.get(source_key)
            if not isinstance(source_items, list):
                continue
            for item in source_items:
                if isinstance(item, dict):
                    _append_source(
                        out,
                        {
                            "title": item.get("title") or item.get("name") or item.get("sourceTitle") or "",
                            "url": item.get("url") or item.get("href") or item.get("link") or item.get("sourceUrl") or "",
                            "snippet": item.get("snippet") or item.get("summary") or item.get("description") or "",
                        },
                    )
                elif isinstance(item, str):
                    _append_source(out, {"title": item, "url": item})

        if isinstance(lowered.get("urls"), list):
            for url_item in lowered["urls"]:
                if isinstance(url_item, str) and url_item.strip():
                    _append_source(out, {"title": url_item.strip(), "url": url_item.strip()})

        url_val = lowered.get("url") or lowered.get("href") or lowered.get("link")
        if isinstance(url_val, str):
            _append_source(
                out,
                {
                    "title": lowered.get("title") or lowered.get("name") or url_val,
                    "url": url_val,
                    "snippet": lowered.get("snippet") or lowered.get("summary") or "",
                },
            )

        for nested in value.values():
            _collect_search_metadata(nested, out)
        return

    if isinstance(value, list):
        for item in value:
            _collect_search_metadata(item, out)


def _dedupe_search_data(data: dict[str, Any]) -> dict[str, Any]:
    queries = data.get("queries", [])
    sources = data.get("sources", [])
    categories = data.get("categories", [])

    deduped_queries: list[str] = []
    for query in queries:
        if query not in deduped_queries:
            deduped_queries.append(query)

    deduped_sources: list[dict[str, str]] = []
    seen_sources: set[tuple[str, str]] = set()
    for source in sources:
        title = str(source.get("title", "") or "")
        url = str(source.get("url", "") or "")
        key = (title, url)
        if key in seen_sources:
            continue
        seen_sources.add(key)
        deduped_sources.append(source)

    deduped_categories: list[str] = []
    for category in categories:
        if category not in deduped_categories:
            deduped_categories.append(category)

    out: dict[str, Any] = {}
    if deduped_queries:
        out["queries"] = deduped_queries
    if deduped_sources:
        out["sources"] = deduped_sources
    if deduped_categories:
        out["categories"] = deduped_categories
    return out


def _looks_like_search_patch(patch: dict[str, Any]) -> bool:
    patch_type = str(patch.get("type", "") or "").lower()
    patch_v = patch.get("v")
    nested_type = ""
    if isinstance(patch_v, dict):
        nested_type = str(patch_v.get("type", "") or "").lower()

    effective_type = patch_type or nested_type
    if effective_type and effective_type != "text" and any(token in effective_type for token in SEARCH_TYPE_KEYWORDS):
        return True

    path = _normalize_path(patch).lower()
    if any(token in path for token in SEARCH_PATH_KEYWORDS):
        return True

    if _contains_search_keys(patch.get("v")):
        return True

    return False


def _extract_search_data_from_patch(patch: dict[str, Any]) -> dict[str, Any]:
    extracted: dict[str, Any] = {"queries": [], "sources": [], "categories": []}
    _collect_search_metadata(patch, extracted)
    return _dedupe_search_data(extracted)


def _extract_text_from_patch(patch: dict[str, Any]) -> str:
    content = ""
    patch_op = patch.get("o")

    if patch_op == "a":
        patch_v = patch.get("v", {})
        if isinstance(patch_v, dict) and "value" in patch_v:
            val = patch_v["value"]
            if isinstance(val, list):
                text_parts = []
                for item in val:
                    if isinstance(item, dict) and isinstance(item.get("content"), str):
                        text_parts.append(str(item.get("content", "")))
                content = "".join(text_parts)
        elif isinstance(patch_v, dict) and "content" in patch_v:
            # text value block texto:"a" /s/N/value/- → {type:"text", content:"..."}
            raw = patch_v.get("content")
            if isinstance(raw, str):
                content = raw

    elif patch_op == "x" and "v" in patch:
        content = patch["v"] if isinstance(patch["v"], str) else ""
    
    elif patch_op == "p" and "v" in patch:
        # text
        path = _normalize_path(patch)
        if "/content" in path or "/text" in path:
            content = patch["v"] if isinstance(patch["v"], str) else ""

    return content


def _looks_like_search_json_fragment(text: str) -> bool:
    stripped = text.strip().lower()
    if not stripped.startswith("{"):
        return False

    if '"default"' in stripped and ('"questions"' in stripped or '"queries"' in stripped):
        return True

    return (
        '"queries"' in stripped
        or '"web"' in stripped
        or '"sources"' in stripped
        or '"citations"' in stripped
        or '"internal"' in stripped
        or '"questions"' in stripped
        or '"urls"' in stripped
    )


def _looks_like_tool_call_json_fragment(text: str) -> bool:
    stripped = text.strip().lower()
    if not stripped.startswith("{"):
        return False
    return (
        '"command"' in stripped
        and ('"replacecontent"' in stripped or '"newstr"' in stripped or '"pageurl"' in stripped or '"block_id"' in stripped)
    )



def _extract_search_data_from_json_text(text: str) -> dict[str, Any]:
    try:
        parsed = json.loads(text)
    except Exception:
        return {}

    extracted: dict[str, Any] = {"queries": [], "sources": [], "categories": []}
    _collect_search_metadata(parsed, extracted)
    return _dedupe_search_data(extracted)


def _clean_extracted_text(text: str) -> str:
    if not text:
        return ""
    in_lang_tag = [False]
    in_primary_attr = [False]
    cleaned = _strip_lang_tags(text, in_lang_tag)
    cleaned = _strip_primary_attr_fragments(cleaned, in_primary_attr)
    cleaned = _clean_notion_markup(cleaned)
    return cleaned.strip()


def _extract_text_from_value_items(value_items: Any) -> str:
    if not isinstance(value_items, list):
        return ""

    parts: list[str] = []
    for item in value_items:
        if not isinstance(item, dict):
            continue
        if str(item.get("type", "") or "").lower() != "text":
            continue
        content = item.get("content")
        if isinstance(content, str) and content:
            parts.append(content)
    return "".join(parts)


def _extract_markdown_chat_text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        parts: list[str] = []
        for item in value:
            if isinstance(item, str):
                parts.append(item)
                continue
            if not isinstance(item, dict):
                continue
            item_type = str(item.get("type", "") or "").lower()
            if item_type == "text" and isinstance(item.get("content"), str):
                parts.append(str(item.get("content", "")))
                continue
            nested_value = item.get("value")
            if nested_value is not None:
                nested_text = _extract_markdown_chat_text(nested_value)
                if nested_text:
                    parts.append(nested_text)
        return "".join(parts)
    if isinstance(value, dict):
        for key in ("value", "content", "text"):
            if key in value:
                nested_text = _extract_markdown_chat_text(value.get(key))
                if nested_text:
                    return nested_text
    return ""


def _extract_markdown_chat_patch_text(patch: dict[str, Any]) -> tuple[str, str] | None:
    patch_op = str(patch.get("o", "") or "")
    patch_v = patch.get("v")

    if (
        patch_op == "a"
        and isinstance(patch_v, dict)
        and str(patch_v.get("type", "") or "").lower() == "markdown-chat"
    ):
        cleaned = _clean_extracted_text(_extract_markdown_chat_text(patch_v.get("value")))
        if cleaned:
            return ("final_content", cleaned)

    return None


def _safe_int(value: Any) -> int:
    try:
        return int(value)
    except Exception:
        return 0


def _non_empty_str(value: Any) -> str:
    if isinstance(value, str):
        return value.strip()
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(value)
    return ""


def _extract_model_metadata_from_step(
    step: dict[str, Any],
    *,
    message_id: str = "",
    outer_value: dict[str, Any] | None = None,
    inner_value: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not isinstance(step, dict):
        return {}
    notion_model_name = _non_empty_str(step.get("notionModelName"))
    notion_step_model = _non_empty_str(step.get("model"))
    model_provider = _non_empty_str(step.get("modelProvider"))
    for part in step.get("value", []) if isinstance(step.get("value"), list) else []:
        if not isinstance(part, dict):
            continue
        notion_model_name = notion_model_name or _non_empty_str(part.get("notionModelName"))
        model_provider = model_provider or _non_empty_str(part.get("modelProvider"))
    # `step.model` is often the requested/route model, not proof of the model
    # that actually produced the response. Only `notionModelName` is treated
    # as observed responder metadata.
    
    if not notion_model_name and isinstance(step.get('value'), dict):
        notion_model_name = _non_empty_str(step.get('value', {}).get('model', ''))
    
    if not notion_model_name and "model" in step.keys():
        notion_model_name = _non_empty_str(step.get('model', ''))

    
    actual_model = notion_model_name
    if not any((actual_model, notion_step_model, notion_model_name, model_provider)):
        return {}
    out: dict[str, Any] = {
        "notion_step_model": notion_step_model,
        "notion_model_name": notion_model_name,
        "model_provider": model_provider,
        "source_step_type": _non_empty_str(step.get("type")),
        "source_message_id": message_id,
        "trace_id": _non_empty_str(step.get("traceId")),
    }
    if actual_model:
        out["actual_model"] = actual_model
        out["actual_model_source"] = "notionModelName"
        # Do not set actual_model_verified here — notionModelName may just
        # echo the requested model.  Verification is deferred to
        # _response_model_metadata which compares against the request.
    elif notion_step_model:
        out["actual_model_verified"] = False
        out["actual_model_unverified_reason"] = "Only step.model was observed; it may be the requested route, not the responder."
    if isinstance(inner_value, dict):
        data = inner_value.get("data")
        if isinstance(data, dict):
            inference_id = _non_empty_str(data.get("inference_id"))
            if inference_id:
                out["inference_id"] = inference_id
        created_time = inner_value.get("created_time")
        if created_time is not None:
            out["message_created_time"] = created_time
    if isinstance(outer_value, dict):
        role = _non_empty_str(outer_value.get("role"))
        if role:
            out["record_role"] = role
    return {k: v for k, v in out.items() if v not in (None, "", [], {})}


def _extract_final_content_from_record_map(data: dict[str, Any]) -> dict[str, Any] | None:
    record_map = data.get("recordMap")
    if not isinstance(record_map, dict):
        return None

    thread_messages = record_map.get("thread_message")
    if not isinstance(thread_messages, dict):
        return None

    candidates: list[dict[str, Any]] = []

    for msg_id, msg_data in thread_messages.items():
        if not isinstance(msg_data, dict):
            continue

        outer_value = msg_data.get("value")
        if not isinstance(outer_value, dict):
            continue
        inner_value = outer_value.get("value")
        if not isinstance(inner_value, dict):
            continue
        step = inner_value.get("step")
        if not isinstance(step, dict):
            continue

        model_metadata = _extract_model_metadata_from_step(
            step,
            message_id=str(msg_id),
            outer_value=outer_value,
            inner_value=inner_value,
        )
        step_type = str(step.get("type", "") or "").lower()
        content = ""

        if step_type == "markdown-chat":
            content = _extract_markdown_chat_text(step.get("value"))
        elif step_type == "agent-inference":
            content = _extract_text_from_value_items(step.get("value"))
        elif step_type in {"text", "title"}:
            raw_value = step.get("value")
            if isinstance(raw_value, str):
                content = raw_value

        cleaned = _clean_extracted_text(content)
        if cleaned:
            candidates.append(
                {
                    "message_id": str(msg_id),
                    "step_type": step_type or "unknown",
                    "priority": FINAL_STEP_PRIORITIES.get(step_type, 100),
                    "created_at": _safe_int(outer_value.get("created_time")),
                    "edited_at": _safe_int(outer_value.get("last_edited_time")),
                    "length": len(cleaned),
                    "text": cleaned,
                    "model_metadata": model_metadata,
                }
            )

    # text text/markdown-chattext agent-inference
    # text Opus/GPT text agent-inference text
    high_priority_types = {"text", "markdown-chat"}
    has_high_priority = any(c["step_type"] in high_priority_types for c in candidates)

    if has_high_priority:
        original_count = len(candidates)
        candidates = [c for c in candidates if c["step_type"] in high_priority_types]
        logger.debug(
            "Final content filtered",
            extra={
                "request_info": {
                    "event": "final_content_filtered",
                    "original_count": original_count,
                    "filtered_count": len(candidates),
                    "removed_types": [c["step_type"] for c in candidates if c["step_type"] not in high_priority_types],
                }
            },
        )

    if not candidates:
        return None

    best = max(
        candidates,
        key=lambda candidate: (
            int(candidate.get("priority", 0)),
            int(candidate.get("edited_at", 0)),
            int(candidate.get("created_at", 0)),
            int(candidate.get("length", 0)),
        ),
    )

    logger.debug(
        "Final content selected",
        extra={
            "request_info": {
                "event": "final_content_selected",
                "step_type": best.get("step_type", "unknown"),
                "priority": best.get("priority", 0),
                "length": len(best.get("text", "")),
                "message_id": best.get("message_id", ""),
            }
        },
    )

    return {
        "text": str(best.get("text", "") or ""),
        "source_type": str(best.get("step_type", "") or "unknown"),
        "source_message_id": str(best.get("message_id", "") or ""),
        "source_length": int(best.get("length", 0)),
        "model_metadata": best.get("model_metadata") if isinstance(best.get("model_metadata"), dict) else {},
    }


def _classify_segment_type(effective_type: str) -> str:
    """
    text o:"a" patch text type text
    text——text Notion text typetext
    """
    if not effective_type:
        return SEG_CONTENT
    if effective_type == "text":
        return SEG_CONTENT
    if effective_type == "title":
        return SEG_META
    if any(kw in effective_type for kw in _THINKING_TYPES):
        return SEG_THINKING
    if any(kw in effective_type for kw in _TOOL_TYPES):
        return SEG_TOOL
    # text
    return SEG_CONTENT


def _bind_pending_segment(
    notion_idx: int,
    pending: list[dict],
    segment_types: dict[int, str],
    value_types: dict[tuple[int, int], str],
    next_val_id: dict[int, int],
    patch_path: str,
) -> None:
    """
    text pending text Notion text indextext

    text agent-inferencetextthinkingtext
    texttooltextmetatextcontenttext
    textmeta/tooltext contenttext
    """
    if not pending:
        return

    # text patch_path text value indextext
    val_idx = _extract_value_index(patch_path)

    # text path text value/0/contenttext thinking value[0] text
    best_idx = 0  # text
    if val_idx is not None:
        for i, seg in enumerate(pending):
            vt = seg.get("value_types", {})
            if vt.get(val_idx) == SEG_THINKING:
                best_idx = i
                break

    chosen = pending.pop(best_idx)
    segment_types[notion_idx] = chosen["seg_class"]
    for vi, vc in chosen["value_types"].items():
        value_types[(notion_idx, vi)] = vc
    next_val_id[notion_idx] = chosen["next_val_id"]

    logger.debug(
        "Pending segment bound to Notion index",
        extra={
            "request_info": {
                "event": "segment_bound",
                "notion_idx": notion_idx,
                "seg_class": chosen["seg_class"],
                "value_types": {str(k): v for k, v in chosen["value_types"].items()},
                "remaining_pending": len(pending),
                "patch_path": patch_path,
            }
        },
    )


def _stream_completion_event(patch: dict[str, Any]) -> dict[str, Any] | None:
    """Return an explicit completion event for Notion's terminal metadata patch."""
    patch_path = _normalize_path(patch)
    if patch_path.rsplit("/", 1)[-1].lower() != "finishedat":
        return None

    finished_at = patch.get("v")
    if finished_at is None:
        return None

    return {
        "type": "stream_complete",
        "finished_at": finished_at,
        "segment_index": _extract_segment_index(patch_path),
    }


def parse_stream(response: requests.Response) -> Generator[dict[str, Any], None, None]:
    """
    text Notion NDJSON text
      - {"type": "content",  "text": "..."}   text
      - {"type": "search",   "data": {...}}    text
      - {"type": "thinking", "text": "..."}    text

    text——textSegment Registrytext
      Notion text o:"a" + path="/s/-" text patch text
      text v.type textagent-inference / agent-tool-result / text text
      text
      text o:"x" + path="/s/N/..." text
      text
    """
    in_lang_tag: list[bool] = [False]
    in_primary_attr: list[bool] = [False]
    search_json_buffer = ""
    search_json_depth = 0
    tool_json_buffer = ""
    tool_json_depth = 0

    # ---- text ----
    # Notion text /s/N text N text indextext config/context/user text
    # text o:"a" /s/- text indextext
    # texto:"a" /s/- text pending dicttext
    # text o:"x" /s/N/... text o:"a" /s/N/... text N text
    # text pending text agent-inference/thinking text
    segment_types: dict[int, str] = {}            # notion_index → SEG_THINKING / SEG_TOOL / SEG_CONTENT
    value_types: dict[tuple[int, int], str] = {}  # (notion_index, val_index) → text
    next_val_id: dict[int, int] = {}              # notion_index → text value block text

    # pendingtexto:"a" /s/- text Notion text index text
    _pending_segments: list[dict] = []  # [{seg_class, value_types_local, next_val_id_local}]

    for line in response.iter_lines(decode_unicode=True):
        if not line:
            continue
        if isinstance(line, bytes):
            line = line.decode("utf-8", errors="ignore")

        # text
        lowered_line = line.lower()
        if any(token in lowered_line for token in LINE_DEBUG_KEYWORDS):
            logger.debug(
                "NDJSON debug line",
                extra={"request_info": {"event": "notion_ndjson_debug_line", "line": line[:4000]}},
            )

        try:
            data = json.loads(line)
        except json.JSONDecodeError:
            continue

        data_type = str(data.get("type", "") or "").lower()

        if data_type == "record-map":
            final_payload = _extract_final_content_from_record_map(data)
            if final_payload and final_payload.get("text"):
                model_metadata = final_payload.get("model_metadata")
                if isinstance(model_metadata, dict) and model_metadata:
                    yield {"type": "model_metadata", "data": model_metadata}
                yield {"type": "final_content", **final_payload}
            continue

        if data_type == "markdown-chat":
            cleaned = _clean_extracted_text(_extract_markdown_chat_text(data.get("value")))
            if cleaned:
                yield {"type": "final_content", "text": cleaned, "source_type": "markdown-chat-event"}
            continue

        if data_type != "patch":
            continue

        patches = data.get("v", [])
        if not isinstance(patches, list):
            continue

        for patch in patches:
            if not isinstance(patch, dict):
                continue

            initial_value_events: list[tuple[str, str]] = []
            patch_op = str(patch.get("o", "") or "")
            patch_v = patch.get("v")
            patch_path = _normalize_path(patch)
            patch_seg = _extract_segment_index(patch_path)

            completion_event = _stream_completion_event(patch)
            if completion_event is not None:
                yield completion_event

            markdown_chat_patch = _extract_markdown_chat_patch_text(patch)
            if markdown_chat_patch is not None:
                event_type, event_text = markdown_chat_patch
                if event_type == "final_content":
                    yield {"type": event_type, "text": event_text, "source_type": "markdown-chat-patch"}
                else:
                    yield {"type": event_type, "text": event_text}
                continue

            # text effective typetext patch.typetext v.typetext
            patch_type = str(patch.get("type", "") or "").lower()
            nested_type = ""
            if isinstance(patch_v, dict):
                nested_type = str(patch_v.get("type", "") or "").lower()
            effective_type = patch_type or nested_type

            # ========== texto:"a" text ==========
            path_stripped = patch_path.strip("/")
            is_new_toplevel_segment = (patch_op == "a" and path_stripped == "s/-")

            # text patch text patch text
            patch_role: str | None = None

            if is_new_toplevel_segment:
                seg_class = _classify_segment_type(effective_type)

                # text value item text
                local_value_types: dict[int, str] = {}
                local_next_val_id = 0
                if isinstance(patch_v, dict) and "value" in patch_v:
                    value_array = patch_v.get("value")
                    if isinstance(value_array, list):
                        for idx, item in enumerate(value_array):
                            if isinstance(item, dict):
                                item_type = str(item.get("type", "") or "").lower()
                                item_class = _classify_segment_type(item_type)
                                local_value_types[idx] = item_class
                                local_next_val_id = idx + 1
                                item_content = item.get("content")
                                if isinstance(item_content, str) and item_content:
                                    initial_value_events.append((item_class, item_content))

                if 0 not in local_value_types:
                    local_value_types[0] = seg_class
                    local_next_val_id = max(local_next_val_id, 1)

                # text pending text o:"x" /s/N text index
                _pending_segments.append({
                    "seg_class": seg_class,
                    "value_types": local_value_types,
                    "next_val_id": local_next_val_id,
                })

                # text patch text value[0] text
                patch_role = local_value_types.get(0, seg_class)
                # patch_seg text Nonetext index
                patch_seg = None

                # Extract model metadata from the initial segment step.
                # The step dict (patch_v) carries `model` which reveals the
                # actual responder, especially for silent model swaps.
                if isinstance(patch_v, dict) and patch_v.get("model"):
                    _seg_meta = _extract_model_metadata_from_step(patch_v, message_id="")
                    if isinstance(_seg_meta, dict) and _seg_meta:
                        yield {"type": "model_metadata", "data": _seg_meta}

                logger.debug(
                    "Segment registered (pending)",
                    extra={
                        "request_info": {
                            "event": "segment_registered",
                            "pending_idx": len(_pending_segments) - 1,
                            "seg_class": seg_class,
                            "effective_type": effective_type,
                            "value_types": local_value_types,
                            "patch_role": patch_role,
                        }
                    },
                )

            elif patch_op == "a" and patch_seg is not None:
                # o:"a" text path text /s/-text /s/2/value/-text
                # text pending text
                if patch_seg not in segment_types and _pending_segments:
                    _bind_pending_segment(patch_seg, _pending_segments, segment_types, value_types, next_val_id, patch_path)

                if patch_seg not in segment_types:
                    segment_types[patch_seg] = _classify_segment_type(effective_type)

                # text /s/N/value/<idx|-> text
                value_add_idx = _extract_value_add_index(patch_path)
                if value_add_idx is not None:
                    vid = next_val_id.get(patch_seg, 0) if value_add_idx < 0 else value_add_idx
                    next_val_id[patch_seg] = max(next_val_id.get(patch_seg, 0), vid + 1)
                    val_class = _classify_segment_type(effective_type)
                    value_types[(patch_seg, vid)] = val_class
                    patch_role = val_class
                    in_lang_tag[0] = False
                    in_primary_attr[0] = False

            # ========== text pending texto:"x" text ==========
            if patch_seg is not None and patch_seg not in segment_types and _pending_segments:
                _bind_pending_segment(patch_seg, _pending_segments, segment_types, value_types, next_val_id, patch_path)

            # ========== text patch text ==========
            if patch_role is not None:
                # text patchtext
                seg_owner = patch_role
            else:
                # o:"x" text value block text segment text
                val_idx = _extract_value_index(patch_path)
                if val_idx is not None and patch_seg is not None and (patch_seg, val_idx) in value_types:
                    seg_owner = value_types[(patch_seg, val_idx)]
                    logger.debug(
                        "Patch role from value_types",
                        extra={
                            "request_info": {
                                "event": "patch_role_value_lookup",
                                "patch_op": patch_op,
                                "patch_seg": patch_seg,
                                "val_idx": val_idx,
                                "seg_owner": seg_owner,
                                "patch_path": patch_path,
                            }
                        },
                    )
                elif patch_seg is not None and patch_seg in segment_types:
                    seg_owner = segment_types[patch_seg]
                    logger.debug(
                        "Patch role from segment_types fallback",
                        extra={
                            "request_info": {
                                "event": "patch_role_segment_fallback",
                                "patch_op": patch_op,
                                "patch_seg": patch_seg,
                                "val_idx": val_idx,
                                "seg_owner": seg_owner,
                                "value_types_missing": (patch_seg, val_idx) not in value_types if val_idx is not None else None,
                                "patch_path": patch_path,
                            }
                        },
                    )
                else:
                    seg_owner = SEG_CONTENT
                    logger.debug(
                        "Patch role defaulting to content",
                        extra={
                            "request_info": {
                                "event": "patch_role_default_content",
                                "patch_op": patch_op,
                                "patch_seg": patch_seg,
                                "val_idx": val_idx,
                                "patch_path": patch_path,
                            }
                        },
                    )

            # ========== text ==========
            is_search_patch = _looks_like_search_patch(patch)
            if is_search_patch:
                search_data = _extract_search_data_from_patch(patch)
                if search_data:
                    yield {"type": "search", "data": search_data}

            # ========== text ==========
            # /content replace patch can finalize broken lang attributes.
            # Reset state here to avoid swallowing following normal text.
            if patch_op == "p" and "/content" in patch_path and isinstance(patch_v, str):
                if ">" in patch_v or "\n" in patch_v or "\r" in patch_v or not patch_v.strip():
                    in_lang_tag[0] = False
                    in_primary_attr[0] = False

            # Initial top-level segments can contain mixed value items, such as
            # a private thinking item followed by the visible text answer. Emit
            # each item under its own classified role instead of concatenating
            # the entire array under the first item's role.
            if initial_value_events:
                for initial_role, initial_text in initial_value_events:
                    cleaned = _strip_lang_tags(initial_text, in_lang_tag)
                    cleaned = _strip_primary_attr_fragments(cleaned, in_primary_attr)
                    cleaned = _clean_notion_markup(cleaned)
                    if not cleaned or initial_role == SEG_META:
                        continue
                    if initial_role in (SEG_THINKING, SEG_TOOL):
                        yield {"type": "thinking", "text": cleaned}
                    else:
                        yield {"type": "content", "text": cleaned}
                continue

            content = _extract_text_from_patch(patch)
            if not content:
                continue

            cleaned = _strip_lang_tags(content, in_lang_tag)
            cleaned = _strip_primary_attr_fragments(cleaned, in_primary_attr)
            cleaned = _clean_notion_markup(cleaned)
            if not cleaned:
                continue

            # ========== text JSON text ==========
            stripped = cleaned.strip()
            if stripped and (search_json_depth > 0 or _looks_like_search_json_fragment(stripped)):
                search_json_buffer += cleaned
                search_json_depth += stripped.count("{") - stripped.count("}")
                if search_json_depth <= 0:
                    sd = _extract_search_data_from_json_text(search_json_buffer)
                    if sd:
                        yield {"type": "search", "data": sd}
                    search_json_buffer = ""
                    search_json_depth = 0
                continue

            # ========== Tool Call JSON text ==========
            if stripped and (tool_json_depth > 0 or _looks_like_tool_call_json_fragment(stripped)):
                tool_json_buffer += cleaned
                tool_json_depth += stripped.count("{") - stripped.count("}")
                if tool_json_depth <= 0:
                    yield {"type": "thinking", "text": tool_json_buffer}
                    tool_json_buffer = ""
                    tool_json_depth = 0
                else:
                    yield {"type": "thinking", "text": cleaned}
                continue

            # text search patch text
            if is_search_patch:
                continue

            # ========== text ==========
            if seg_owner == SEG_META:
                continue
            if seg_owner in (SEG_THINKING, SEG_TOOL):
                yield {"type": "thinking", "text": cleaned}
            else:
                yield {"type": "content", "text": cleaned}




