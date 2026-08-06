"""Record live notion2api chat turns into account-partitioned history shards.

RepoAI and AIgentBee create Notion AI threads through /v1/chat/completions. Each
completed turn is written immediately to the shard bound to workspace_id:user_id.
"""

from __future__ import annotations

import hashlib
import time
from typing import Any

from app.chat_history.store import ChatHistoryStore
from app.logger import logger


def infer_source_system(request_metadata: dict[str, Any] | None) -> str:
    metadata = request_metadata if isinstance(request_metadata, dict) else {}
    caller = metadata.get("caller") if isinstance(metadata.get("caller"), dict) else {}
    caller_type = str(
        caller.get("type") or caller.get("caller_type") or metadata.get("caller_type") or ""
    ).strip().lower()
    caller_id = str(
        caller.get("id") or caller.get("caller_id") or metadata.get("caller_id") or ""
    ).strip().lower()
    session_name = str(
        metadata.get("session_name")
        or metadata.get("mcp_session_name")
        or metadata.get("chat_title")
        or metadata.get("title")
        or ""
    ).strip().lower()

    if (
        caller_type in {"aigentbee", "aigent-bee", "hive"}
        or caller_id.startswith("aigentbee")
        or "aigentbee" in caller_id
        or session_name.startswith("aigentbee-leader")
        or "aigentbee" in session_name
        or bool(caller.get("mission_id") and caller.get("work_unit_id"))
    ):
        return "aigentbee"

    if caller_type in {"repoai", "repo-ai", "repo_ai"} or caller_id.startswith("repo-ai") or "repoai" in caller_id:
        return "repoai"
    if any(
        metadata.get(key)
        for key in (
            "repo_ai_review_instance_id",
            "computer_use_review",
            "repo_ai_thread_title",
            "repo_ai_review_page_id",
        )
    ):
        return "repoai"

    if caller_type:
        return caller_type
    return "notion2api"


def _stable_message_id(thread_id: str, role: str, text: str, *, hint: str = "") -> str:
    hint = str(hint or "").strip()
    if hint and not hint.startswith("live-") and not hint.startswith("synthetic-"):
        return hint
    digest = hashlib.sha256(
        f"{thread_id}\n{role}\n{text}".encode("utf-8", errors="replace")
    ).hexdigest()[:24]
    return f"live-{digest}"


def _thread_title(request_metadata: dict[str, Any] | None, user_prompt: str) -> str:
    metadata = request_metadata if isinstance(request_metadata, dict) else {}
    for key in ("chat_title", "title", "session_name", "repo_ai_thread_title"):
        value = str(metadata.get(key) or "").strip()
        if value:
            return value[:180]
    prompt = " ".join(str(user_prompt or "").split())
    if not prompt:
        return "Untitled chat"
    return prompt[:180]


def build_live_turn_bundle(
    *,
    thread_id: str,
    conversation_id: str,
    account_key: str = "",
    user_prompt: str,
    assistant_reply: str,
    requested_model: str = "",
    model_metadata: dict[str, Any] | None = None,
    request_metadata: dict[str, Any] | None = None,
    created_time: str | None = None,
) -> dict[str, Any]:
    """Build a chat-history bundle for one completed live turn."""
    tid = str(thread_id or "").strip()
    if not tid:
        return {"threads": {}, "messages": {}}

    meta = dict(model_metadata or {})
    req_meta = request_metadata if isinstance(request_metadata, dict) else {}
    source_system = infer_source_system(req_meta)
    stamp = str(created_time or int(time.time() * 1000))
    user_text = str(user_prompt or "").strip()
    assistant_text = str(assistant_reply or "").strip()
    if not user_text and not assistant_text:
        return {"threads": {}, "messages": {}}

    caller = req_meta.get("caller") if isinstance(req_meta.get("caller"), dict) else {}
    provenance = {
        "source": "live_chat",
        "source_system": source_system,
        "account_key": str(account_key or "").strip(),
        "conversation_id": str(conversation_id or "").strip(),
        "session_name": str(req_meta.get("session_name") or "").strip(),
        "caller": {
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
                "request_origin",
            }
            and value not in (None, "", [], {})
        },
    }
    if req_meta.get("repo_ai_review_instance_id"):
        provenance["repo_ai_review_instance_id"] = str(req_meta.get("repo_ai_review_instance_id"))

    message_ids: list[str] = []
    messages: dict[str, Any] = {}

    if user_text:
        user_id = _stable_message_id(tid, "user", user_text)
        message_ids.append(user_id)
        messages[user_id] = {
            "id": user_id,
            "thread_id": tid,
            "role": "user",
            "text": user_text,
            "created_time": stamp,
            "raw": {
                "role": "user",
                "text": user_text,
                "live": provenance,
            },
        }

    if assistant_text:
        assistant_hint = str(
            meta.get("source_message_id") or meta.get("message_id") or ""
        ).strip()
        assistant_id = _stable_message_id(
            tid, "assistant", assistant_text, hint=assistant_hint
        )
        message_ids.append(assistant_id)
        assistant_raw = {
            "role": "assistant",
            "text": assistant_text,
            "live": provenance,
            "requested_model": str(meta.get("requested_model") or requested_model or "").strip(),
            "notion_requested_model": str(meta.get("notion_requested_model") or "").strip(),
            "actual_model": str(
                meta.get("actual_model") or meta.get("notion_model_name") or ""
            ).strip(),
            "model_provider": str(meta.get("model_provider") or "").strip(),
            "notion_model_name": str(meta.get("notion_model_name") or "").strip(),
            "notion_step_model": str(meta.get("notion_step_model") or "").strip(),
            "source_message_id": assistant_hint,
        }
        if meta.get("inference_id"):
            assistant_raw["data"] = {"inference_id": meta.get("inference_id")}
        messages[assistant_id] = {
            "id": assistant_id,
            "thread_id": tid,
            "role": "assistant",
            "text": assistant_text,
            "created_time": str(meta.get("message_created_time") or stamp),
            "actual_model": assistant_raw["actual_model"],
            "model_provider": assistant_raw["model_provider"],
            "raw": {key: value for key, value in assistant_raw.items() if value not in (None, "", [], {})},
        }

    title = _thread_title(req_meta, user_text)
    thread = {
        "id": tid,
        "title": title,
        "created_time": stamp,
        "last_edited_time": stamp,
        "alive": True,
        "message_ids": message_ids,
        "raw": {
            "type": "live_chat",
            "title": title,
            "live": provenance,
        },
    }
    return {"threads": {tid: thread}, "messages": messages}


def record_live_chat_turn(
    *,
    thread_id: str,
    conversation_id: str,
    account_key: str,
    user_prompt: str,
    assistant_reply: str,
    requested_model: str = "",
    model_metadata: dict[str, Any] | None = None,
    request_metadata: dict[str, Any] | None = None,
    store: ChatHistoryStore | None = None,
) -> dict[str, Any]:
    """Write one live chat turn into its mandatory account history shard."""
    key = str(account_key or "").strip()
    if not key:
        raise ValueError("account_key is required for live chat-history writes")
    if store is not None and store.account_key and store.account_key != key:
        raise ValueError("live recorder account_key does not match the supplied store")
    bundle = build_live_turn_bundle(
        thread_id=thread_id,
        conversation_id=conversation_id,
        account_key=key,
        user_prompt=user_prompt,
        assistant_reply=assistant_reply,
        requested_model=requested_model,
        model_metadata=model_metadata,
        request_metadata=request_metadata,
    )
    if not bundle.get("threads"):
        return {"recorded": False, "reason": "empty_turn"}

    history_store = store or ChatHistoryStore(account_key=key)
    imported = history_store.record_live_turn(bundle)
    source_system = infer_source_system(request_metadata)
    logger.info(
        "Recorded live chat turn into chat history archive",
        extra={
            "request_info": {
                "event": "chat_history_live_turn_recorded",
                "thread_id": str(thread_id or "").strip(),
                "conversation_id": str(conversation_id or "").strip(),
                "source_system": source_system,
                "account_key": key,
                "db_path": history_store.db_path,
                "messages": imported.get("messages", 0),
                "messages_inserted": imported.get("messages_inserted", 0),
                "messages_updated": imported.get("messages_updated", 0),
            }
        },
    )
    return {
        "recorded": True,
        "source_system": source_system,
        "account_key": key,
        "db_path": history_store.db_path,
        "imported": imported,
    }
