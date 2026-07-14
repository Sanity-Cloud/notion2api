from __future__ import annotations

import contextvars
from collections.abc import Callable
from functools import wraps
from typing import Any

from app.logger import logger

_PATCHED = False

active_binding = contextvars.ContextVar("active_binding", default=None)


def _conversation_id_from_request(req_body: Any) -> str:
    return str(getattr(req_body, "conversation_id", None) or "").strip()


def _manager_from_request(request: Any) -> Any | None:
    return getattr(getattr(request, "app", None).state, "conversation_manager", None)


def _conversation_exists(manager: Any, conversation_id: str) -> bool:
    try:
        return bool(manager and conversation_id and manager.conversation_exists(conversation_id))
    except Exception:
        return False


def _get_bound_thread_id(manager: Any, conversation_id: str) -> str | None:
    if not _conversation_exists(manager, conversation_id):
        return None
    try:
        thread_id = manager.get_conversation_thread_id(conversation_id)
    except Exception:
        return None
    clean = str(thread_id or "").strip()
    return clean or None


def _resolve_persistent_thread_id(
    manager: Any,
    conversation_id: str,
    explicit_thread_id: str | None = None,
) -> str | None:
    """Prefer an explicit thread, otherwise keep the conversation's bound thread."""
    return explicit_thread_id or _get_bound_thread_id(manager, conversation_id)


def _set_bound_thread_id(manager: Any, conversation_id: str, thread_id: str | None, model_name: str | None = None) -> None:
    clean = str(thread_id or "").strip()
    if not clean or not _conversation_exists(manager, conversation_id):
        return
    try:
        manager.set_conversation_thread_id(conversation_id, clean, model_name=model_name)
    except Exception:
        logger.warning(
            "Unable to persist resumed chat thread binding",
            exc_info=True,
            extra={
                "request_info": {
                    "event": "resume_thread_binding_persist_failed",
                    "conversation_id": conversation_id,
                    "thread_id": clean,
                }
            },
        )


_ORIGINAL_STREAM_RESPONSE = None


def _apply_stream_response_patch() -> None:
    global _ORIGINAL_STREAM_RESPONSE
    if _ORIGINAL_STREAM_RESPONSE is not None:
        return

    from app.notion_client import NotionOpusAPI
    _ORIGINAL_STREAM_RESPONSE = NotionOpusAPI.stream_response

    @wraps(_ORIGINAL_STREAM_RESPONSE)
    def patched_stream_response(
        self: NotionOpusAPI,
        transcript: list,
        thread_id: str | None = None,
        attachments: list | None = None,
        persist_remote_chat: bool | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        binding = active_binding.get()
        if not binding:
            return _ORIGINAL_STREAM_RESPONSE(
                self,
                transcript,
                thread_id=thread_id,
                attachments=attachments,
                persist_remote_chat=persist_remote_chat,
                *args,
                **kwargs,
            )

        conversation_id = binding["conversation_id"]
        model_name = binding["model_name"]
        manager = binding["manager"]

        # A conversation ID owns one durable Notion thread across model changes.
        bound_thread_id = _get_bound_thread_id(manager, conversation_id)

        active_thread_id = _resolve_persistent_thread_id(manager, conversation_id, thread_id)
        stream = _ORIGINAL_STREAM_RESPONSE(
            self,
            transcript,
            thread_id=active_thread_id,
            attachments=attachments,
            persist_remote_chat=persist_remote_chat,
            *args,
            **kwargs,
        )

        def generator_wrapper():
            try:
                for chunk in stream:
                    yield chunk
            finally:
                if not bound_thread_id:
                    created_thread_id = getattr(self, "current_thread_id", None)
                    if created_thread_id:
                        _set_bound_thread_id(manager, conversation_id, created_thread_id, model_name)

        return generator_wrapper()

    NotionOpusAPI.stream_response = patched_stream_response


def _wrap_handler(handler: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(handler)
    def wrapped(request: Any, req_body: Any, *args: Any, **kwargs: Any) -> Any:
        conversation_id = _conversation_id_from_request(req_body)
        model_name = getattr(req_body, "model", None)
        if not conversation_id:
            return handler(request, req_body, *args, **kwargs)

        manager = _manager_from_request(request)
        if manager and not manager.conversation_exists(conversation_id):
            try:
                import datetime
                created_at = int(datetime.datetime.now().timestamp())
                with manager._get_conn() as conn:
                    conn.execute(
                        "INSERT INTO conversations (id, title, created_at, next_round_index) VALUES (?, ?, ?, ?)",
                        (conversation_id, "External Chat", created_at, 0)
                    )
                    conn.commit()
                logger.info(
                    "Auto-created conversation record for standard/lite client",
                    extra={"request_info": {"event": "conversation_created_standard_patch", "conversation_id": conversation_id}},
                )
            except Exception as e:
                logger.warning("Failed to auto-create conversation %s: %s", conversation_id, e)

        token = active_binding.set({
            "conversation_id": conversation_id,
            "model_name": model_name,
            "manager": manager
        })
        try:
            res = handler(request, req_body, *args, **kwargs)
            # Inject X-Conversation-Id header into response if possible
            if res is not None and hasattr(res, "headers"):
                try:
                    res.headers["X-Conversation-Id"] = conversation_id
                except Exception:
                    pass
            for arg in args:
                if hasattr(arg, "headers") and hasattr(arg, "status_code"):
                    try:
                        arg.headers["X-Conversation-Id"] = conversation_id
                    except Exception:
                        pass
            for val in kwargs.values():
                if hasattr(val, "headers") and hasattr(val, "status_code"):
                    try:
                        val.headers["X-Conversation-Id"] = conversation_id
                    except Exception:
                        pass
            return res
        finally:
            active_binding.reset(token)

    return wrapped


def apply_chat_resume_thread_bindings() -> None:
    global _PATCHED
    if _PATCHED:
        return

    _apply_stream_response_patch()

    from app.api import chat as chat_module

    chat_module._handle_standard_request = _wrap_handler(chat_module._handle_standard_request)
    chat_module._handle_lite_request = _wrap_handler(chat_module._handle_lite_request)
    _PATCHED = True

    logger.info(
        "Chat resume thread bindings patched into standard/lite handlers",
        extra={"request_info": {"event": "resume_thread_binding_patch_applied"}},
    )
