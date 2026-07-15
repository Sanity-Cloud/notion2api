"""Normalize OpenAI-compatible attachment request shapes."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

from app.attachments.models import DEFAULT_ATTACHMENT_PROMPT, InputAttachment

TEXT_PART_TYPES = {"text", "input_text", "output_text"}
ATTACHMENT_PART_TYPES = {"image_url", "input_image", "file", "input_file", "attachment"}

DEFAULT_MAX_PROMPT_FIELD_CHARS = 200_000
DEFAULT_MAX_PROMPT_TOTAL_CHARS = 400_000
_ALLOWED_CONTROL_CHARS = {"\t", "\n", "\r"}


class PromptValidationError(ValueError):
    """Bounded invalid-request error that never echoes prompt content."""

    def __init__(self, message: str, *, code: str, param: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.param = param
        self.status_code = status_code


def _positive_env_int(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default
    return value if value > 0 else default


def validate_prompt_text(
    value: Any,
    *,
    param: str,
    allow_none: bool = False,
    max_chars: int | None = None,
) -> str | None:
    """Validate caller-provided prompt text without coercing or logging its value."""

    if value is None and allow_none:
        return None
    if not isinstance(value, str):
        raise PromptValidationError(
            f"{param} must be a string.",
            code="invalid_prompt_type",
            param=param,
        )

    limit = max_chars or _positive_env_int(
        "MAX_PROMPT_FIELD_CHARS", DEFAULT_MAX_PROMPT_FIELD_CHARS
    )
    if len(value) > limit:
        raise PromptValidationError(
            f"{param} exceeds the maximum length of {limit} characters.",
            code="prompt_too_large",
            param=param,
        )

    for index, character in enumerate(value):
        ordinal = ord(character)
        if (ordinal < 32 and character not in _ALLOWED_CONTROL_CHARS) or ordinal == 127:
            raise PromptValidationError(
                f"{param} contains an unsupported control character at offset {index}.",
                code="invalid_prompt_control_character",
                param=param,
            )
    return value


def _validated_text_part(value: Any, *, param: str) -> str:
    if value is None:
        return ""
    validated = validate_prompt_text(value, param=param)
    return str(validated or "")


def validate_chat_messages(messages: Any) -> int:
    """Validate text-bearing message fields and return their aggregate character count."""

    if not isinstance(messages, list):
        raise PromptValidationError(
            "messages must be a list.",
            code="invalid_messages_type",
            param="messages",
        )

    total = 0
    for message_index, raw_message in enumerate(messages):
        if not isinstance(raw_message, dict):
            raise PromptValidationError(
                f"messages[{message_index}] must be an object.",
                code="invalid_message_type",
                param=f"messages[{message_index}]",
            )
        content = raw_message.get("content")
        param = f"messages[{message_index}].content"
        text, _attachments = _extract_text_and_attachments(content, param=param)
        total += len(text)

    total_limit = _positive_env_int(
        "MAX_PROMPT_TOTAL_CHARS", DEFAULT_MAX_PROMPT_TOTAL_CHARS
    )
    if total > total_limit:
        raise PromptValidationError(
            f"messages contain more than {total_limit} prompt characters in total.",
            code="prompt_total_too_large",
            param="messages",
        )
    return total


def _string(value: Any) -> str:
    return str(value or "").strip()


def _content_type_from_data_url(value: str) -> str:
    if not value.startswith("data:"):
        return ""
    header = value.split(",", 1)[0]
    return header[5:].split(";", 1)[0].strip().lower()


def _looks_like_url(value: str) -> bool:
    parsed = urlparse(value)
    return parsed.scheme.lower() in {"http", "https"}


def _looks_like_windows_path(value: str) -> bool:
    return len(value) >= 3 and value[1] == ":" and value[2] in {"\\", "/"}


def _looks_like_path(value: str) -> bool:
    if not value:
        return False
    if _looks_like_windows_path(value):
        return True
    if value.startswith(("/", "\\\\", ".\\", "./", "..\\", "../")):
        return True

    parsed = urlparse(value)
    if parsed.scheme.lower() == "file":
        return True
    if parsed.scheme:
        return False
    return False


def _image_url_value(part: dict[str, Any]) -> str:
    image_url = part.get("image_url")
    if isinstance(image_url, dict):
        return _string(image_url.get("url"))
    return _string(image_url)


def _attachment_name(part: dict[str, Any], fallback: str = "") -> str:
    return _string(
        part.get("name")
        or part.get("filename")
        or part.get("file_name")
        or part.get("title")
        or fallback
    )


def _attachment_content_type(part: dict[str, Any], inline_value: str = "") -> str:
    explicit = _string(
        part.get("content_type")
        or part.get("mime_type")
        or part.get("media_type")
    ).lower()
    if explicit:
        return explicit
    data_url_type = _content_type_from_data_url(inline_value)
    if data_url_type:
        return data_url_type
    if part.get("type") in {"image_url", "input_image"}:
        return "image/png"
    return ""


def _attachment_from_reference(part: dict[str, Any], ref: str) -> InputAttachment:
    content_type = _attachment_content_type(part, ref)
    name = _attachment_name(part)
    if ref.startswith("data:"):
        return InputAttachment(name=name, content_type=content_type, source="inline_data", data=ref)
    if _looks_like_url(ref):
        return InputAttachment(name=name, content_type=content_type, source="remote_url", url=ref)
    if _looks_like_path(ref):
        return InputAttachment(name=name, content_type=content_type, source="local_path", path=ref)
    return InputAttachment(name=name, content_type=content_type, source="inline_data", data=ref)


def _attachment_from_part(part: dict[str, Any]) -> InputAttachment | None:
    part_type = _string(part.get("type")).lower()

    if part_type in {"image_url", "input_image"}:
        ref = _image_url_value(part) or _string(part.get("url"))
        if ref:
            return _attachment_from_reference(part, ref)

    if part_type in {"file", "input_file", "attachment"} or any(
        key in part for key in ("file_data", "data", "file_url", "url", "path")
    ):
        inline_value = _string(part.get("file_data") or part.get("data"))
        if inline_value:
            return _attachment_from_reference(part, inline_value)

        ref = _string(part.get("file_url") or part.get("url") or part.get("path"))
        if ref:
            return _attachment_from_reference(part, ref)

    image_ref = _image_url_value(part)
    if image_ref:
        return _attachment_from_reference(part, image_ref)

    return None


def _top_level_attachment(item: Any) -> InputAttachment | None:
    if not isinstance(item, dict):
        return None
    normalized = dict(item)
    if not normalized.get("type"):
        content_type = _string(normalized.get("content_type") or normalized.get("mime_type")).lower()
        if content_type.startswith("image/") or normalized.get("image_url"):
            normalized["type"] = "image_url"
        else:
            normalized["type"] = "file"
    return _attachment_from_part(normalized)


def _attachment_signature(attachment: InputAttachment) -> tuple[Any, ...]:
    data_value = attachment.data
    if isinstance(data_value, bytes):
        data_value = data_value.decode("utf-8", errors="ignore")
    return (
        str(attachment.source or ""),
        str(attachment.name or ""),
        str(attachment.content_type or ""),
        str(attachment.url or ""),
        str(attachment.path or ""),
        str(data_value or ""),
    )


def _extract_text_and_attachments(
    content: Any, *, param: str = "content"
) -> tuple[str, list[InputAttachment]]:
    if content is None:
        return "", []
    if isinstance(content, str):
        return _validated_text_part(content, param=param), []
    if not isinstance(content, list):
        raise PromptValidationError(
            f"{param} must be a string or a list of structured content parts.",
            code="invalid_prompt_type",
            param=param,
        )

    text_parts: list[str] = []
    attachments: list[InputAttachment] = []

    for item_index, item in enumerate(content):
        item_param = f"{param}[{item_index}]"
        if isinstance(item, str):
            text = _validated_text_part(item, param=item_param)
            if text:
                text_parts.append(text)
            continue
        if not isinstance(item, dict):
            raise PromptValidationError(
                f"{item_param} must be a string or an object.",
                code="invalid_prompt_part_type",
                param=item_param,
            )

        item_type = _string(item.get("type")).lower()
        if item_type in TEXT_PART_TYPES or (not item_type and "text" in item):
            text = _validated_text_part(item.get("text"), param=f"{item_param}.text")
            if text:
                text_parts.append(text)
            continue

        attachment = _attachment_from_part(item)
        if attachment is not None:
            attachments.append(attachment)
            continue

        if "text" in item:
            text = _validated_text_part(item.get("text"), param=f"{item_param}.text")
            if text:
                text_parts.append(text)

    return "\n".join(part for part in text_parts if part), attachments


def _append_attachment_fallback(messages: list[dict[str, Any]], attachments: list[InputAttachment]) -> None:
    if not attachments:
        return
    for msg in reversed(messages):
        if str(msg.get("role") or "").lower() == "user":
            if not _string(msg.get("content")):
                msg["content"] = DEFAULT_ATTACHMENT_PROMPT
            return
    messages.append({"role": "user", "content": DEFAULT_ATTACHMENT_PROMPT})


def normalize_chat_messages(
    messages: list[dict[str, Any]],
    top_level_attachments: list[Any] | None = None,
) -> tuple[list[dict[str, Any]], list[InputAttachment]]:
    """Return text-clean messages and normalized attachments."""

    validate_chat_messages(messages or [])
    normalized_messages: list[dict[str, Any]] = []
    attachments: list[InputAttachment] = []

    for message_index, raw_message in enumerate(messages or []):
        msg = dict(raw_message)
        text, message_attachments = _extract_text_and_attachments(
            msg.get("content"), param=f"messages[{message_index}].content"
        )
        msg["content"] = text
        normalized_messages.append(msg)
        attachments.extend(message_attachments)

    for item in top_level_attachments or []:
        attachment = _top_level_attachment(item)
        if attachment is not None and _attachment_signature(attachment) not in {
            _attachment_signature(existing) for existing in attachments
        }:
            attachments.append(attachment)

    _append_attachment_fallback(normalized_messages, attachments)
    return normalized_messages, attachments


def normalize_responses_input(
    input_value: Any,
    top_level_attachments: list[Any] | None = None,
) -> tuple[list[dict[str, Any]], list[InputAttachment]]:
    """Return chat-compatible messages and normalized attachments from Responses input."""

    if isinstance(input_value, str):
        return normalize_chat_messages(
            [{"role": "user", "content": input_value}],
            top_level_attachments,
        )

    messages: list[dict[str, Any]] = []
    attachments: list[InputAttachment] = []

    if input_value is not None and not isinstance(input_value, list):
        raise PromptValidationError(
            "input must be a string or a list.",
            code="invalid_prompt_type",
            param="input",
        )

    if isinstance(input_value, list):
        for item in input_value:
            if isinstance(item, str):
                messages.append({"role": "user", "content": item})
                continue
            if not isinstance(item, dict):
                raise PromptValidationError(
                    f"input[{len(messages)}] must be a string or an object.",
                    code="invalid_prompt_part_type",
                    param="input",
                )

            role = _string(item.get("role") or "user").lower()
            if role == "developer":
                role = "system"
            if role not in {"system", "user", "assistant"}:
                role = "user"

            if item.get("type") == "message" and "content" in item:
                text, item_attachments = _extract_text_and_attachments(item.get("content"))
            elif "content" in item:
                text, item_attachments = _extract_text_and_attachments(item.get("content"))
            elif _string(item.get("type")).lower() in TEXT_PART_TYPES:
                text = _string(item.get("text"))
                item_attachments = []
            else:
                attachment = _attachment_from_part(item)
                text = ""
                item_attachments = [attachment] if attachment is not None else []

            if text:
                messages.append({"role": role, "content": text})
            attachments.extend(item_attachments)

    cleaned_messages, top_level = normalize_chat_messages(messages, top_level_attachments)
    attachments = [*attachments, *top_level]
    _append_attachment_fallback(cleaned_messages, attachments)
    return cleaned_messages, attachments
