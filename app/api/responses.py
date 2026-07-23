from __future__ import annotations

import time
import uuid
from typing import Any, Literal

from fastapi import APIRouter, BackgroundTasks, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from app.api.chat import create_chat_completion
from app.attachments.errors import AttachmentError
from app.core.errors import openai_error
from app.core.models import normalize_model_id
from app.schemas import ChatCompletionRequest, ChatMessage
from app.attachments.normalizer import (
    PromptValidationError,
    normalize_responses_input,
    validate_inline_attachment_data,
)
from app.attachments.security import AttachmentPolicy, validate_content_type

router = APIRouter()


ChatRole = Literal["system", "user", "assistant"]


def _normalize_chat_role(value: Any) -> ChatRole:
    role = str(value or "user").lower()
    if role == "developer":
        return "system"
    if role == "system":
        return "system"
    if role == "assistant":
        return "assistant"
    return "user"


def _content_to_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, str):
                parts.append(item)
            elif isinstance(item, dict):
                item_type = str(item.get("type") or "")
                if item_type in {"input_text", "output_text", "text"}:
                    parts.append(str(item.get("text") or ""))
                elif "text" in item:
                    parts.append(str(item.get("text") or ""))
        return "\n".join(part for part in parts if part)
    return ""


def _responses_input_to_messages(input_value: Any) -> list[ChatMessage]:
    if isinstance(input_value, str):
        return [ChatMessage(role="user", content=input_value)]

    if not isinstance(input_value, list):
        openai_error("Responses API input must be a string or list.", "invalid_input")

    messages: list[ChatMessage] = []
    for item in input_value:
        if isinstance(item, str):
            messages.append(ChatMessage(role="user", content=item))
            continue
        if not isinstance(item, dict):
            continue

        role = _normalize_chat_role(item.get("role"))

        if item.get("type") == "message" and "content" in item:
            text = _content_to_text(item.get("content"))
        elif "content" in item:
            text = _content_to_text(item.get("content"))
        elif item.get("type") in {"input_text", "output_text", "text"}:
            text = str(item.get("text") or "")
        else:
            text = ""

        if text.strip():
            messages.append(ChatMessage(role=role, content=text))

    if not messages:
        openai_error("Responses API input did not contain any text messages.", "invalid_input")
    return messages


def _as_dict(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        return value.model_dump()
    if hasattr(value, "dict"):
        return value.dict()
    return {}


def _chat_completion_to_response(chat_payload: dict[str, Any], requested_model: str) -> dict[str, Any]:
    choice = (chat_payload.get("choices") or [{}])[0] or {}
    message = choice.get("message") or {}
    text = str(message.get("content") or "")
    model = str(chat_payload.get("model") or requested_model)
    created = int(chat_payload.get("created") or time.time())
    response_id = f"resp_{uuid.uuid4().hex}"
    message_id = f"msg_{uuid.uuid4().hex}"

    return {
        "id": response_id,
        "object": "response",
        "created_at": created,
        "status": "completed",
        "model": model,
        "output": [
            {
                "id": message_id,
                "type": "message",
                "status": "completed",
                "role": "assistant",
                "content": [
                    {
                        "type": "output_text",
                        "text": text,
                        "annotations": [],
                    }
                ],
            }
        ],
        "output_text": text,
        "usage": chat_payload.get("usage") or {"input_tokens": 0, "output_tokens": 0, "total_tokens": 0},
    }


@router.post("/responses", tags=["responses"])
async def create_response(
    request: Request,
    payload: dict[str, Any],
    background_tasks: BackgroundTasks,
    response: Response,
):
    """Minimal OpenAI Responses API compatibility shim backed by /v1/chat/completions."""
    model = normalize_model_id(payload.get("model"))
    if not model:
        openai_error("The 'model' field is required.", "model_required")
    try:
        cleaned_msgs, attachments = normalize_responses_input(payload.get("input"), payload.get("attachments"))
    except (AttachmentError, PromptValidationError) as exc:
        openai_error(
            str(exc),
            exc.code or "invalid_attachment",
            status_code=exc.status_code,
            param=exc.param,
        )

    policy = AttachmentPolicy.from_env()
    if attachments and not policy.enabled:
        openai_error("Attachments are disabled.", "attachments_disabled")

    try:
        validate_inline_attachment_data(attachments)
        for att in attachments:
            if att.content_type:
                validate_content_type(att.content_type, policy)
    except (AttachmentError, PromptValidationError) as exc:
        openai_error(
            str(exc),
            exc.code or "invalid_attachment",
            status_code=exc.status_code,
            param=exc.param,
        )

    messages = [
        ChatMessage(
            role=_normalize_chat_role(m.get("role")),
            content=str(m.get("content") or ""),
        )
        for m in cleaned_msgs
    ]
    stream = bool(payload.get("stream", False))

    chat_req = ChatCompletionRequest(
        model=model,
        messages=messages,
        stream=stream,
        temperature=payload.get("temperature"),
        conversation_id=payload.get("conversation_id"),
    )

    # Preserve raw attachment data for the delegated chat handler without logging it.
    request.state.attachments = attachments or None

    if attachments:
        safe_top_level = []
        for att in attachments:
            safe_top_level.append(
                {
                    "name": att.name,
                    "content_type": att.content_type,
                    "source": att.source,
                    "url": att.url,
                    "path": att.path,
                }
            )
        chat_req.attachments = safe_top_level

    chat_result = await create_chat_completion(request, chat_req, background_tasks, response)

    if isinstance(chat_result, (JSONResponse, StreamingResponse)):
        return chat_result

    chat_payload = _as_dict(chat_result)
    return _chat_completion_to_response(chat_payload, model)
