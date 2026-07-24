import os
import datetime
import json
import threading
import time
import uuid
from urllib.parse import urlparse
from typing import Any, Generator, Optional

try:
    import cloudscraper
except Exception:
    cloudscraper = None
import requests
import urllib3

from app.logger import logger
from app.attachments.notion_upload import NotionAttachmentUploader, NotionAttachmentUploadError
from app.attachments.models import UploadedAttachment
from app.model_registry import get_notion_model
from app.stream_parser_safe import parse_stream

# text SSL text
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# text Notion textNotion text
NOTION_CLIENT_VERSION = os.getenv("NOTION_CLIENT_VERSION", "23.13.20260623.1532")


def _env_timeout_seconds(name: str, default: float) -> float | None:
    """Read a timeout in seconds; zero or negative disables that timeout."""
    raw_value = os.getenv(name)
    if raw_value is None:
        return default
    try:
        parsed = float(raw_value)
    except (TypeError, ValueError):
        return default
    return None if parsed <= 0 else parsed


NOTION_UPSTREAM_CONNECT_TIMEOUT = _env_timeout_seconds(
    "NOTION_UPSTREAM_CONNECT_TIMEOUT_SECONDS", 15.0
)
NOTION_UPSTREAM_READ_TIMEOUT = _env_timeout_seconds(
    "NOTION_UPSTREAM_READ_TIMEOUT_SECONDS", 1200.0
)


def _env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _attachment_descriptor_debug_enabled() -> bool:
    return _env_flag("ATTACHMENT_DESCRIPTOR_DEBUG", False)


def _redact_response_excerpt(text: str) -> str:
    excerpt = (text or "").strip().replace("\n", " ")[:500]
    for marker in ("token_v2=", "cookie:", "Cookie:", "Authorization:"):
        if marker in excerpt:
            excerpt = excerpt.replace(marker, f"{marker}[redacted]")
    return excerpt


def _extract_attachment_file_id(attachment_url: str) -> str:
    clean = str(attachment_url or "").strip()
    if clean.startswith("attachment:"):
        parts = clean.split(":", 2)
        if len(parts) == 3:
            return parts[1].strip()
    try:
        path = urlparse(clean).path
    except Exception:
        return ""
    return path.rstrip("/").split("/")[-1] if path else ""


def _is_zip_upload(name: str, content_type: str) -> bool:
    normalized_type = str(content_type or "").split(";", 1)[0].strip().lower()
    return normalized_type in {"application/zip", "application/x-zip-compressed"} or str(name or "").lower().endswith(".zip")


def _notion_attachment_upload_name(name: str, content_type: str) -> str:
    """Match Notion web: ZIP descriptors use ``{uuid}zip``, not ``source.zip``."""
    if _is_zip_upload(name, content_type):
        return f"{uuid.uuid4()}zip"
    return name


def _notion_upload_content_type(name: str, content_type: str) -> str:
    if _is_zip_upload(name, content_type):
        return "application/x-zip-compressed"
    return content_type


def _default_persist_threads() -> bool:
    """Preserve Notion-visible threads only for stateful local-memory mode by default."""
    app_mode = os.getenv("APP_MODE", "heavy").strip().lower()
    return app_mode == "heavy"


def _resolve_thread_persistence() -> dict[str, bool]:
    """Resolve how upstream Notion threads should be persisted.

    In standard/lite proxy modes, callers normally send the full request context and
    do not need Notion's visible chat list as a backing store. Defaulting those modes
    to ephemeral threads prevents council-style fan-out from flooding Notion AI chat
    history. Heavy mode keeps the previous preserved-thread behavior unless explicitly
    overridden.
    """
    persist = _env_flag("NOTION_PERSIST_THREADS", _default_persist_threads())
    return {
        "persist": persist,
        "generate_title": _env_flag("NOTION_GENERATE_TITLES", persist),
        "save_all_thread_operations": _env_flag("NOTION_SAVE_THREAD_OPERATIONS", persist),
        "set_unread_state": _env_flag("NOTION_SET_UNREAD_STATE", persist),
        "delete_after_stream": _env_flag("NOTION_DELETE_EPHEMERAL_THREADS", not persist),
    }


class NotionUpstreamError(RuntimeError):
    """Notion text"""

    status_code: Optional[int]
    retriable: bool
    response_excerpt: str

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        retriable: bool = True,
        response_excerpt: str = "",
    ):
        super().__init__(message)
        self.status_code = status_code
        self.retriable = retriable
        self.response_excerpt = response_excerpt


BOUND_THREAD_HISTORY_REPLAY = "BOUND_THREAD_HISTORY_REPLAY"
_BOUND_THREAD_ALLOWED_BLOCK_TYPES = frozenset({"config", "context", "user"})
_BOUND_THREAD_HISTORY_MARKERS = (
    "[previous conversation context]",
    "[recalled conversation archive]",
    "[conversation history]",
)


def _transcript_block_text(block: dict[str, Any]) -> str:
    value = block.get("value")
    parts: list[str] = []

    def collect(item: Any) -> None:
        if isinstance(item, str):
            parts.append(item)
        elif isinstance(item, list):
            for child in item:
                collect(child)
        elif isinstance(item, dict):
            for child in item.values():
                collect(child)

    collect(value)
    return "\n".join(parts)


def validate_bound_thread_transcript(transcript: list[dict[str, Any]]) -> None:
    """Reject historical dialog replay when Notion already owns thread history."""

    block_types = [str(block.get("type") or "").strip() for block in transcript]
    unsupported = [kind for kind in block_types if kind not in _BOUND_THREAD_ALLOWED_BLOCK_TYPES]
    user_blocks = [block for block in transcript if block.get("type") == "user"]

    marker_detected = False
    for block in user_blocks:
        lowered = _transcript_block_text(block).casefold()
        if any(marker in lowered for marker in _BOUND_THREAD_HISTORY_MARKERS):
            marker_detected = True
            break

    if unsupported or len(user_blocks) > 1 or marker_detected:
        evidence = {
            "error_code": BOUND_THREAD_HISTORY_REPLAY,
            "block_types": block_types,
            "user_block_count": len(user_blocks),
            "unsupported_block_types": unsupported,
            "history_marker_detected": marker_detected,
        }
        raise NotionUpstreamError(
            "A bound Notion thread may not receive historical dialog or multiple user messages.",
            status_code=409,
            retriable=False,
            response_excerpt=json.dumps(evidence, sort_keys=True),
        )


class NotionOpusAPI:
    def __init__(self, account_config: dict):
        """
        text Notion text
        account_config text token_v2, space_id, user_id, space_view_id, user_name, user_email
        """
        self.token_v2 = account_config.get("token_v2", "")
        self.space_id = account_config.get("space_id", "")
        self.user_id = account_config.get("user_id", "")
        self.space_view_id = account_config.get("space_view_id", "")
        self.user_name = account_config.get("user_name", "user")
        self.user_email = account_config.get("user_email", "")
        self.timezone = str(
            account_config.get("timezone")
            or os.getenv("NOTION_TIMEZONE")
            or "America/Chicago"
        ).strip()
        self.context_page_id = str(
            account_config.get("context_page_id")
            or os.getenv("NOTION_CONTEXT_PAGE_ID")
            or ""
        ).strip()
        self.repo_ai_parent_page_id = str(
            account_config.get("repo_ai_parent_page_id")
            or os.getenv("REPO_AI_NOTION_PARENT_PAGE_ID")
            or ""
        ).strip()
        self.cookies = account_config.get("cookies", {})
        if not isinstance(self.cookies, dict):
            self.cookies = {}
        self.cookies["token_v2"] = self.token_v2

        self.url = "https://www.notion.so/api/v3/runInferenceTranscript"
        self.delete_url = "https://www.notion.so/api/v3/saveTransactions"
        self.thread_title_url = "https://app.notion.com/api/v3/saveTransactionsFanout"
        self.account_key = self.user_email or self.user_id or "unknown-account"

        # Reuse cloudscraper instance when available; otherwise fall back to requests.Session.
        if cloudscraper is not None:
            self._scraper = cloudscraper.create_scraper()
        else:
            self._scraper = requests.Session()
        self._scraper_lock = threading.Lock()

    def get_ai_model_picker_config(self) -> dict[str, Any]:
        """Fetch space AI model picker config from Notion v3 API."""
        endpoint = "https://www.notion.so/api/v3/getAvailableModels"
        payload = {"spaceId": self.space_id}
        resp = self._scraper.post(endpoint, headers=self._build_chat_history_headers(), json=payload, timeout=30)
        resp.raise_for_status()
        return resp.json()

    def _build_cookie_header(self) -> str:
        cookie_jar = self.cookies.copy()
        cookie_jar["notion_user_id"] = self.user_id
        return "; ".join(f"{name}={value}" for name, value in cookie_jar.items() if value)

    def _to_notion_transcript(self, transcript: list[dict[str, Any]]) -> list[dict[str, Any]]:
        converted: list[dict[str, Any]] = []
        for block in transcript:
            if block.get("type") != "config":
                converted.append(block)
                continue

            value = block.get("value")
            if not isinstance(value, dict):
                converted.append(block)
                continue

            notion_block = dict(block)
            notion_value = dict(value)
            notion_value["model"] = get_notion_model(str(value.get("model", "") or ""))
            notion_block["value"] = notion_value
            converted.append(notion_block)
        return converted

    def _resolve_thread_type(self, notion_transcript: list[dict[str, Any]]) -> str:
        for block in notion_transcript:
            if block.get("type") != "config":
                continue
            value = block.get("value")
            if isinstance(value, dict):
                thread_type = str(value.get("type", "") or "").strip()
                if thread_type:
                    return thread_type
        return "workflow"

    def _with_thread_type(
        self,
        notion_transcript: list[dict[str, Any]],
        thread_type: str,
    ) -> list[dict[str, Any]]:
        """Return a transcript copy whose config block uses the requested chat surface."""
        converted: list[dict[str, Any]] = []
        for block in notion_transcript:
            if block.get("type") != "config" or not isinstance(block.get("value"), dict):
                converted.append(block)
                continue
            updated = dict(block)
            updated_value = dict(block["value"])
            updated_value["type"] = thread_type
            updated["value"] = updated_value
            converted.append(updated)
        return converted

    def _with_computer_use_capabilities(
        self,
        notion_transcript: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        converted: list[dict[str, Any]] = []
        for block in notion_transcript:
            updated = dict(block)
            value = block.get("value")
            if block.get("type") == "config" and isinstance(value, dict):
                updated_value = dict(value)
                updated_value.update({
                    "type": "workflow",
                    "enableComputer": True,
                    "enableScriptAgent": True,
                    "enableCsvAttachmentSupport": True,
                    "enableCreateAndRunThread": True,
                    "enableScriptAgentCustomToolCalling": True,
                })
                updated["value"] = updated_value
            elif block.get("type") == "context" and isinstance(value, dict):
                updated_value = dict(value)
                updated_value["surface"] = "ai_module"
                updated["value"] = updated_value
            converted.append(updated)
        return converted

    def _resolve_request_profile(self, thread_type: str) -> dict[str, Any]:
        is_markdown_chat = thread_type == "markdown-chat"
        return {
            "thread_type": thread_type,
            "create_thread": not is_markdown_chat,
            "is_partial_transcript": is_markdown_chat,
            "precreate_thread": is_markdown_chat,
            "include_debug_overrides": True,
        }

    def _build_thread_headers(self) -> dict[str, str]:
        return {
            "content-type": "application/json",
            "cookie": self._build_cookie_header(),
            "x-notion-active-user-header": self.user_id,
            "x-notion-space-id": self.space_id,
        }

    def _build_chat_history_headers(self) -> dict[str, str]:
        return {
            "content-type": "application/json",
            "accept": "application/json",
            "cookie": self._build_cookie_header(),
            "x-notion-active-user-header": self.user_id,
            "x-notion-space-id": self.space_id,
            "notion-client-version": NOTION_CLIENT_VERSION,
            "origin": "https://www.notion.so",
            "referer": "https://www.notion.so/ai",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        }

    def iter_continue_confirmed_tool_steps(
        self,
        *,
        thread_id: str,
        tool_step_ids: list[str],
        summary: dict[str, Any] | None = None,
    ) -> Generator[dict[str, Any], None, None]:
        """Yield NDJSON chunks while resuming inference after tool confirmations."""
        clean_thread_id = str(thread_id or "").strip()
        clean_ids = list(dict.fromkeys(str(item).strip() for item in tool_step_ids if str(item).strip()))
        if not clean_thread_id:
            raise ValueError("thread_id is required")
        if not clean_ids:
            raise ValueError("tool_step_ids must contain at least one id")

        trace_id = str(uuid.uuid4())
        payload = {
            "traceId": trace_id,
            "spaceId": self.space_id,
            "transcript": [],
            "threadId": clean_thread_id,
            "createThread": False,
            "debugOverrides": {
                "cachedInferences": {},
                "annotationInferences": {},
                "emitInferences": False,
            },
            "generateTitle": False,
            "saveAllThreadOperations": True,
            "setUnreadState": True,
            "confirmToolStepIds": clean_ids,
            "createdSource": "workflows",
            "threadType": "workflow",
            "isPartialTranscript": True,
            "asPatchResponse": True,
            "patchResponseVersion": 2,
            "isUserInAnySalesAssistedSpace": False,
            "isSpaceSalesAssisted": False,
            "supportsCustomAgentNudgeTranscriptStep": True,
        }
        headers = self._build_chat_history_headers()
        headers["accept"] = "application/x-ndjson"
        try:
            response = self._scraper.post(
                self.url,
                headers=headers,
                json=payload,
                stream=True,
                timeout=(NOTION_UPSTREAM_CONNECT_TIMEOUT, NOTION_UPSTREAM_READ_TIMEOUT),
            )
        except requests.exceptions.RequestException as exc:
            raise NotionUpstreamError(
                "Request to Notion upstream failed while continuing confirmed tool steps.",
                retriable=True,
                response_excerpt=str(exc),
            ) from exc
        if response.status_code != 200:
            excerpt = _redact_response_excerpt(response.text or "")
            raise NotionUpstreamError(
                f"Notion allow-once continuation returned HTTP {response.status_code}.",
                status_code=response.status_code,
                retriable=response.status_code >= 500 or response.status_code == 429,
                response_excerpt=excerpt,
            )

        event_count = 0
        event_types: list[str] = []
        stream_completed = False
        unresolved_ids: set[str] = set()
        applied_ids: set[str] = set()
        requested_ids = set(clean_ids)
        try:
            for chunk in parse_stream(response):
                if not isinstance(chunk, dict):
                    continue
                chunk_type = str(chunk.get("type") or "").strip()
                if chunk_type == "stream_complete":
                    stream_completed = True
                    continue
                if chunk_type == "tool_confirmation":
                    for step_id in chunk.get("tool_step_ids") or []:
                        clean_step_id = str(step_id or "").strip()
                        if clean_step_id in requested_ids:
                            unresolved_ids.add(clean_step_id)
                elif chunk_type == "tool_result_status":
                    clean_step_id = str(chunk.get("tool_step_id") or "").strip()
                    state = str(chunk.get("state") or "").strip().lower()
                    if clean_step_id in requested_ids and (
                        state in {"applied", "confirmation:confirmed"}
                        or bool(chunk.get("has_result"))
                    ):
                        applied_ids.add(clean_step_id)
                        unresolved_ids.discard(clean_step_id)
                event_count += 1
                if chunk_type and chunk_type not in event_types:
                    event_types.append(chunk_type)
                yield chunk
        finally:
            try:
                response.close()
            except Exception:
                pass

        missing_applied_ids = requested_ids - applied_ids
        unresolved_ids.update(missing_applied_ids)
        approved = stream_completed and not unresolved_ids and applied_ids == requested_ids
        result = {
            "trace_id": trace_id,
            "approved": approved,
            "stream_completed": stream_completed,
            "event_count": event_count,
            "event_types": event_types[:30],
            "applied_tool_step_ids": sorted(applied_ids),
            "unresolved_tool_step_ids": sorted(unresolved_ids),
            "reason": "approved" if approved else "confirmation_not_applied",
            "thread_id": clean_thread_id,
            "tool_step_ids": clean_ids,
        }
        if summary is not None:
            summary.clear()
            summary.update(result)

    def continue_confirmed_tool_steps(
        self,
        *,
        thread_id: str,
        tool_step_ids: list[str],
    ) -> dict[str, Any]:
        """Resume an existing inference after its tool confirmations were granted."""
        summary: dict[str, Any] = {}
        for _chunk in self.iter_continue_confirmed_tool_steps(
            thread_id=thread_id,
            tool_step_ids=tool_step_ids,
            summary=summary,
        ):
            pass
        return summary

    def _normalize_upload_descriptor(self, body: Any) -> dict[str, Any]:
        if not isinstance(body, dict):
            raise NotionUpstreamError("Upload descriptor response malformed", retriable=False, response_excerpt=str(body)[:300])

        upload_url = (
            body.get("upload_url")
            or body.get("uploadUrl")
            or body.get("signedUploadPostUrl")
            or body.get("signed_upload_post_url")
            or body.get("signedUploadUrl")
            or body.get("signed_upload_url")
        )

        fields = body.get("fields") or body.get("formFields") or body.get("postFields") or {}
        if fields is None:
            fields = {}
        if not isinstance(fields, dict):
            raise NotionUpstreamError("Upload descriptor fields malformed", retriable=False, response_excerpt=str(body)[:300])

        attachment_url = body.get("attachment_url") or body.get("attachmentUrl") or body.get("url")
        file_id = body.get("file_id") or body.get("fileId") or body.get("id")
        if not file_id and isinstance(body.get("file"), dict):
            file_id = body["file"].get("id")
        if not file_id:
            file_id = _extract_attachment_file_id(str(attachment_url or ""))

        signed_get_url = body.get("signed_get_url") or body.get("signedGetUrl")
        chat_id = body.get("chat_id") or body.get("chatId")

        metadata = body.get("metadata") or {}
        if metadata is None:
            metadata = {}
        if not isinstance(metadata, dict):
            metadata = {"value": metadata}

        canonical = {
            "upload_url": str(upload_url or ""),
            "fields": fields,
            "file_id": str(file_id or ""),
            "attachment_url": str(attachment_url or ""),
            "signed_get_url": str(signed_get_url or ""),
            "chat_id": str(chat_id or ""),
            "metadata": metadata,
        }

        if not canonical["upload_url"] and not canonical["file_id"] and not canonical["attachment_url"]:
            raise NotionUpstreamError("Upload descriptor missing required fields", retriable=False, response_excerpt=str(body)[:300])

        return canonical

    # --- Attachment upload adapter methods -------------------------------------------------
    def request_upload_descriptor(self, *, name: str, content_type: str, size: int, thread_id: str | None, create_thread: bool) -> dict[str, Any]:
        """Request an upload descriptor from Notion upstream for staging an attachment.

        Returns a dict containing at least one of: upload_url, file_id, attachment_url, and optional fields.
        Raises NotionUpstreamError on HTTP or response failures.
        """
        endpoint = "https://www.notion.so/api/v3/getUploadFileUrlForAssistantChatTranscriptUpload"
        notion_content_type = _notion_upload_content_type(name, content_type)
        upload_name = _notion_attachment_upload_name(name, content_type)
        payload = {
            "name": upload_name,
            "contentType": notion_content_type,
            "assistantChatTranscriptSessionPointer": {
                "spaceId": self.space_id,
                "table": "thread",
                "id": thread_id,
            },
            "contentLength": size,
            "createThread": create_thread,
        }
        if _is_zip_upload(name, notion_content_type):
            payload["allowUnsupportedTypes"] = True
        if _attachment_descriptor_debug_enabled():
            logger.warning(
                "Attachment descriptor request",
                extra={
                    "request_info": {
                        "event": "attachment_descriptor_request",
                        "endpoint": "getUploadFileUrlForAssistantChatTranscriptUpload",
                        "payload_keys": sorted(payload.keys()),
                        "originalFileName": name,
                        "uploadName": upload_name,
                        "contentType": notion_content_type,
                        "contentLength": size,
                        "threadId_present": bool(thread_id),
                        "createThread": create_thread,
                        "spaceId_present": bool(self.space_id),
                        "userId_present": bool(self.user_id),
                    }
                },
            )
        try:
            resp = self._scraper.post(endpoint, headers=self._build_chat_history_headers(), json=payload, timeout=30)
        except Exception as exc:
            raise NotionUpstreamError("Failed to request upload descriptor", status_code=None, retriable=True, response_excerpt=str(exc)) from exc

        if resp.status_code != 200:
            excerpt = _redact_response_excerpt(resp.text or "")
            if _attachment_descriptor_debug_enabled():
                logger.warning(
                    "Attachment descriptor response failure",
                    extra={
                        "request_info": {
                            "event": "attachment_descriptor_response_failure",
                            "status_code": resp.status_code,
                            "retriable": resp.status_code >= 500,
                            "originalFileName": name,
                            "uploadName": upload_name,
                            "contentType": notion_content_type,
                            "contentLength": size,
                            "createThread": create_thread,
                            "threadId_present": bool(thread_id),
                            "spaceId_present": bool(self.space_id),
                            "response_excerpt": excerpt,
                        }
                    },
                )
            raise NotionUpstreamError(
                "Upload descriptor request failed",
                status_code=resp.status_code,
                retriable=resp.status_code >= 500,
                response_excerpt=excerpt,
            )

        try:
            body = resp.json()
        except Exception as exc:
            raise NotionUpstreamError("Upload descriptor response invalid JSON", status_code=resp.status_code, retriable=True, response_excerpt=(resp.text or "")[:300]) from exc

        descriptor = self._normalize_upload_descriptor(body)
        if _attachment_descriptor_debug_enabled():
            logger.warning(
                "Attachment descriptor response ok",
                extra={
                    "request_info": {
                        "event": "attachment_descriptor_response_ok",
                        "descriptor_keys": sorted(descriptor.keys()),
                        "has_upload_url": bool(descriptor.get("upload_url")),
                        "has_file_id": bool(descriptor.get("file_id")),
                        "has_attachment_url": bool(descriptor.get("attachment_url")),
                        "has_chat_id": bool(descriptor.get("chat_id")),
                        "field_keys": sorted((descriptor.get("fields") or {}).keys()),
                    }
                },
            )
        return descriptor

    def perform_multipart_upload(self, *, descriptor: dict[str, Any], name: str, data: bytes, content_type: str) -> None:
        """Perform multipart upload to a signed upload URL using descriptor data.

        Raises NotionUpstreamError on failure.
        """
        upload_url = descriptor.get("upload_url") or descriptor.get("uploadUrl")
        fields = descriptor.get("fields") or descriptor.get("formFields") or {}
        if not upload_url:
            raise NotionUpstreamError("Descriptor missing upload URL", retriable=False)

        # Use requests to POST multipart form data
        import requests

        files = {"file": (name, data, content_type)}
        try:
            resp = requests.post(upload_url, data=fields, files=files, timeout=60)
        except Exception as exc:
            raise NotionUpstreamError("Multipart upload HTTP error", retriable=True, response_excerpt=str(exc)) from exc

        if resp.status_code < 200 or resp.status_code >= 300:
            raise NotionUpstreamError(f"Multipart upload failed with HTTP {resp.status_code}", status_code=resp.status_code, retriable=resp.status_code >= 500, response_excerpt=(resp.text or "")[:300])

    def enqueue_attachment_processing(self, *, attachment_url: str, thread_id: str) -> str:
        """Ask Notion to enqueue processing of a staged attachment and return a task id."""
        endpoint = "https://www.notion.so/api/v3/enqueueTask"
        payload = {
            "task": {
                "eventName": "processAgentAttachment",
                "request": {
                    "url": attachment_url,
                    "spaceId": self.space_id,
                    "aiSessionPointer": {
                        "spaceId": self.space_id,
                        "table": "thread",
                        "id": thread_id,
                    },
                    "source": "user_upload",
                    "clientVersion": NOTION_CLIENT_VERSION,
                },
                "cellRouting": {
                    "spaceIds": [self.space_id],
                },
            },
        }
        try:
            resp = self._scraper.post(endpoint, headers=self._build_chat_history_headers(), json=payload, timeout=30)
        except Exception as exc:
            raise NotionUpstreamError("Failed to enqueue attachment processing", retriable=True, response_excerpt=str(exc)) from exc

        if resp.status_code != 200:
            raise NotionUpstreamError("Enqueue attachment task failed", status_code=resp.status_code, retriable=resp.status_code >= 500, response_excerpt=(resp.text or "")[:300])

        try:
            body = resp.json()
        except Exception as exc:
            raise NotionUpstreamError("Enqueue task response invalid JSON", status_code=resp.status_code, retriable=True, response_excerpt=(resp.text or "")[:300]) from exc

        task_id = body.get("taskId") or body.get("id")
        if not task_id:
            raise NotionUpstreamError("Enqueue task response missing task id", retriable=False, response_excerpt=str(body)[:300])
        return str(task_id)

    def get_task_status(self, task_id: str) -> dict[str, Any]:
        endpoint = "https://www.notion.so/api/v3/getTasks"
        payload = {"taskIds": [task_id]}
        try:
            resp = self._scraper.post(endpoint, headers=self._build_chat_history_headers(), json=payload, timeout=30)
        except Exception as exc:
            raise NotionUpstreamError("Failed to fetch task status", retriable=True, response_excerpt=str(exc)) from exc

        if resp.status_code != 200:
            raise NotionUpstreamError("Get tasks failed", status_code=resp.status_code, retriable=resp.status_code >= 500, response_excerpt=(resp.text or "")[:300])

        try:
            body = resp.json()
        except Exception as exc:
            raise NotionUpstreamError("Get tasks response invalid JSON", status_code=resp.status_code, retriable=True, response_excerpt=(resp.text or "")[:300]) from exc

        results = body.get("results")
        if isinstance(results, list) and results:
            entry = results[0] if isinstance(results[0], dict) else {}
            state = str(entry.get("state") or "").strip()
            status = entry.get("status") if isinstance(entry.get("status"), dict) else {}
            result = status.get("result") if isinstance(status.get("result"), dict) else {}
            result_type = str(result.get("type") or "").strip()
            data = result.get("data") if isinstance(result.get("data"), dict) else {}
            if state == "success" or result_type == "success":
                return {"status": "completed", "success": True, "data": data}
            if state == "error" or result_type == "error":
                return {"status": "failed", "success": False, "data": data}
            return {"status": state or result_type or "pending", "success": False, "data": data}

        items = body.get("tasks") or body
        if isinstance(items, dict):
            return items.get(task_id) or {"status": items.get("status"), "success": items.get("success")}
        if isinstance(items, list):
            for item in items:
                if str(item.get("id")) == str(task_id):
                    return item
        # malformed
        raise NotionUpstreamError("Malformed getTasks response", retriable=False, response_excerpt=str(body)[:300])

    def get_signed_read_url(
        self,
        attachment_url: str,
        thread_id: str = "",
        download_name: str = "",
        *,
        permission_table: str = "thread",
        permission_id: str = "",
    ) -> str:
        endpoint = "https://www.notion.so/api/v3/getSignedFileUrls"
        resolved_permission_id = str(permission_id or thread_id or "").strip()
        if not resolved_permission_id:
            raise ValueError("A permission record id is required for signed file access.")
        payload = {
            "urls": [
                {
                    "url": attachment_url,
                    "download": False,
                    "downloadName": download_name,
                    "permissionRecord": {
                        "table": permission_table,
                        "id": resolved_permission_id,
                        "spaceId": self.space_id,
                    },
                }
            ]
        }
        try:
            resp = self._scraper.post(endpoint, headers=self._build_chat_history_headers(), json=payload, timeout=30)
        except Exception as exc:
            raise NotionUpstreamError("Failed to request signed read URL", retriable=True, response_excerpt=str(exc)) from exc

        if resp.status_code != 200:
            raise NotionUpstreamError("Signed URL request failed", status_code=resp.status_code, retriable=resp.status_code >= 500, response_excerpt=(resp.text or "")[:300])

        try:
            body = resp.json()
        except Exception as exc:
            raise NotionUpstreamError("Signed URL response invalid JSON", status_code=resp.status_code, retriable=True, response_excerpt=(resp.text or "")[:300]) from exc

        if isinstance(body, dict):
            if "signedUrls" in body and isinstance(body["signedUrls"], list):
                for item in body["signedUrls"]:
                    if isinstance(item, str):
                        return item
                    if not isinstance(item, dict):
                        continue
                    if str(item.get("url") or item.get("sourceUrl") or attachment_url) == str(attachment_url):
                        return item.get("url") or ""
                    if item.get("signedUrl"):
                        return item.get("signedUrl") or ""
            if attachment_url in body:
                return body[attachment_url]
        raise NotionUpstreamError("Signed URL not found in response", retriable=False, response_excerpt=str(body)[:300])

    def warm_script_agent_cache(self) -> None:
        """Prime Notion's script-agent module cache before workflow ZIP reviews."""
        endpoint = "https://www.notion.so/api/v3/warmScriptAgentDynamicModuleCache"
        payload = {"spaceId": self.space_id}
        try:
            resp = self._scraper.post(
                endpoint,
                headers=self._build_chat_history_headers(),
                json=payload,
                timeout=20,
            )
            if resp.status_code != 200:
                logger.warning(
                    "Script agent warm-cache request failed",
                    extra={
                        "request_info": {
                            "event": "script_agent_warm_cache_failed",
                            "status_code": resp.status_code,
                        }
                    },
                )
        except Exception:
            logger.warning(
                "Script agent warm-cache request errored",
                exc_info=True,
                extra={"request_info": {"event": "script_agent_warm_cache_error"}},
            )

    def _build_attachment_transcript_steps(
        self,
        uploaded_attachments: list[UploadedAttachment],
        *,
        computer_file: bool = False,
    ) -> list[dict[str, Any]]:
        steps: list[dict[str, Any]] = []
        for uploaded in uploaded_attachments:
            metadata = uploaded.metadata or {}
            if computer_file:
                metadata = {
                    **metadata,
                    "fileSize": uploaded.size_bytes,
                    "attachmentSource": "user_upload",
                }
            steps.append({
                "id": str(uuid.uuid4()),
                "type": "computer-file" if computer_file else "attachment",
                "fileName": uploaded.name,
                "contentType": uploaded.content_type,
                "fileUrl": uploaded.attachment_url,
                "metadata": metadata,
            })
        return steps

    def _build_computer_use_zip_instruction_step(self) -> dict[str, Any]:
        return {
            "id": str(uuid.uuid4()),
            "type": "user",
            "value": [[
                "Use computer-use and script tools to download and extract the attached ZIP. "
                "Inspect the extracted repository files and complete the requested review. "
                "Do not wait for a manual response in the Notion app."
            ]],
            "userId": self.user_id,
            "createdAt": datetime.datetime.now().astimezone().isoformat(),
        }

    def fetch_chat_history(self, limit: int = 100, max_pages: int = 5) -> dict[str, Any]:
        """Best-effort pull of Notion AI chat transcripts for the current user."""
        endpoint = "https://www.notion.so/api/v3/getInferenceTranscriptsForUser"
        collected: dict[str, Any] = {}
        page_cursor: str | None = None

        for page_index in range(max_pages):
            request_id = str(uuid.uuid4())
            candidate_payloads = [
                {"requestId": request_id},
                {"requestId": request_id, "limit": limit},
                {"requestId": request_id, "pageSize": limit},
            ]
            if page_cursor:
                candidate_payloads = [
                    {"requestId": request_id, "cursor": page_cursor, "limit": limit},
                    {"requestId": request_id, "startCursor": page_cursor, "limit": limit},
                    {"requestId": request_id, "cursor": page_cursor, "pageSize": limit},
                ] + candidate_payloads

            response_obj = None
            last_excerpt = ""
            for payload in candidate_payloads:
                try:
                    response_obj = self._scraper.post(
                        endpoint,
                        headers=self._build_chat_history_headers(),
                        json=payload,
                        timeout=(15, 60),
                    )
                except Exception as exc:
                    last_excerpt = str(exc)
                    continue

                if response_obj.status_code == 200:
                    break

                last_excerpt = (response_obj.text or "").strip().replace("\n", " ")[:300]
                response_obj = None

            if response_obj is None:
                raise NotionUpstreamError(
                    "Failed to fetch Notion chat history.",
                    status_code=502,
                    retriable=True,
                    response_excerpt=last_excerpt,
                )

            try:
                page_obj = response_obj.json()
            except Exception as exc:
                raise NotionUpstreamError(
                    "Notion chat history response was not valid JSON.",
                    status_code=response_obj.status_code,
                    retriable=True,
                    response_excerpt=(response_obj.text or "").strip()[:300],
                ) from exc

            if isinstance(page_obj, dict):
                collected.update(page_obj)

                next_cursor = (
                    page_obj.get("nextCursor")
                    or page_obj.get("next_cursor")
                    or page_obj.get("cursor")
                    or page_obj.get("nextPageCursor")
                )
                if isinstance(next_cursor, str) and next_cursor.strip():
                    page_cursor = next_cursor.strip()
                    continue

            break

        return collected

    def _create_thread(self, thread_id: str, thread_type: str) -> bool:
        persisted_thread_type = "markdownChat" if thread_type == "markdown-chat" else thread_type
        payload = {
            "requestId": str(uuid.uuid4()),
            "transactions": [
                {
                    "id": str(uuid.uuid4()),
                    "spaceId": self.space_id,
                    "operations": [
                        {
                            "pointer": {"table": "thread", "id": thread_id, "spaceId": self.space_id},
                            "path": [],
                            "command": "set",
                            "args": {
                                "id": thread_id,
                                "version": 1,
                                "parent_id": self.space_id,
                                "parent_table": "space",
                                "space_id": self.space_id,
                                "created_time": int(time.time() * 1000),
                                "created_by_id": self.user_id,
                                "created_by_table": "notion_user",
                                "messages": [],
                                "data": {},
                                "alive": True,
                                "type": persisted_thread_type,
                            },
                        }
                    ],
                }
            ],
        }
        try:
            resp = requests.post(
                self.delete_url,
                json=payload,
                headers=self._build_thread_headers(),
                timeout=20,
            )
            if resp.status_code == 200:
                return True
            logger.warning(
                "Pre-create thread failed",
                extra={
                    "request_info": {
                        "event": "thread_precreate_failed",
                        "thread_id": thread_id,
                        "thread_type": thread_type,
                        "status": resp.status_code,
                    }
                },
            )
        except Exception:
            logger.warning(
                "Pre-create thread raised exception",
                exc_info=True,
                extra={
                    "request_info": {
                        "event": "thread_precreate_error",
                        "thread_id": thread_id,
                        "thread_type": thread_type,
                    }
                },
            )
        return False

    def delete_threads(self, thread_ids: list[str]) -> dict[str, Any]:
        """Mark multiple remote Notion AI threads inactive using saveTransactions."""
        clean_ids: list[str] = []
        seen: set[str] = set()
        for thread_id in thread_ids:
            value = str(thread_id or "").strip()
            if not value or value in seen:
                continue
            seen.add(value)
            clean_ids.append(value)

        result: dict[str, Any] = {
            "requested": len(thread_ids),
            "valid_ids": len(clean_ids),
            "remote_deleted": 0,
            "remote_failed": 0,
            "failed_ids": [],
        }
        if not clean_ids:
            return result

        transactions = []
        for thread_id in clean_ids:
            transactions.append(
                {
                    "id": str(uuid.uuid4()),
                    "spaceId": self.space_id,
                    "operations": [
                        {
                            "pointer": {
                                "table": "thread",
                                "id": thread_id,
                                "spaceId": self.space_id,
                            },
                            "command": "update",
                            "path": [],
                            "args": {"alive": False},
                        }
                    ],
                }
            )

        payload = {"requestId": str(uuid.uuid4()), "transactions": transactions}
        try:
            resp = requests.post(
                self.delete_url,
                json=payload,
                headers=self._build_thread_headers(),
                timeout=30,
            )
        except requests.exceptions.RequestException as exc:
            raise NotionUpstreamError(
                "Request to Notion upstream failed while deleting remote threads.",
                retriable=True,
                response_excerpt=str(exc),
            ) from exc

        if resp.status_code != 200:
            excerpt = (resp.text or "").strip().replace("\n", " ")[:300]
            raise NotionUpstreamError(
                f"Notion remote thread delete returned HTTP {resp.status_code}.",
                status_code=resp.status_code,
                retriable=resp.status_code >= 500 or resp.status_code == 429,
                response_excerpt=excerpt,
            )

        result["remote_deleted"] = len(clean_ids)
        logger.info(
            "Bulk remote Notion threads marked inactive",
            extra={
                "request_info": {
                    "event": "threads_bulk_deleted",
                    "count": len(clean_ids),
                    "account": self.account_key,
                }
            },
        )
        return result

    def delete_thread(self, thread_id: str) -> None:
        """Mark one remote Notion AI thread inactive."""
        self.delete_threads([thread_id])

    def set_thread_title(self, thread_id: str, title: str) -> bool:
        """Set the exact title of a persisted remote Notion AI thread."""
        from app.thread_title import normalize_thread_title

        clean_thread_id = str(thread_id or "").strip()
        clean_title = normalize_thread_title(title)
        if not clean_thread_id or not clean_title:
            return False

        payload = {
            "requestId": str(uuid.uuid4()),
            "transactions": [
                {
                    "id": str(uuid.uuid4()),
                    "spaceId": self.space_id,
                    "debug": {
                        "userAction": "agentChat.threadPersistenceActions.updateThreadTitle",
                    },
                    "operations": [
                        {
                            "pointer": {
                                "table": "thread",
                                "id": clean_thread_id,
                                "spaceId": self.space_id,
                            },
                            "command": "update",
                            "path": ["data"],
                            "args": {
                                "title": clean_title,
                                "user_set_title": True,
                            },
                        }
                    ],
                }
            ],
        }
        try:
            response = requests.post(
                self.thread_title_url,
                json=payload,
                headers=self._build_thread_headers(),
                timeout=20,
            )
        except requests.exceptions.RequestException as exc:
            raise NotionUpstreamError(
                "Request to Notion upstream failed while setting the thread title.",
                retriable=True,
                response_excerpt=str(exc),
            ) from exc
        if response.status_code != 200:
            excerpt = (response.text or "").strip().replace("\n", " ")[:300]
            raise NotionUpstreamError(
                f"Notion thread title update returned HTTP {response.status_code}.",
                status_code=response.status_code,
                retriable=response.status_code >= 500 or response.status_code == 429,
                response_excerpt=excerpt,
            )
        return True

    def _auto_continue_external_url_confirmation(
        self,
        *,
        thread_id: str,
        confirmation_chunk: dict[str, Any],
    ) -> Generator[dict[str, Any], None, None]:
        """Auto-approve a Notion urlSafety confirmation and resume the same thread."""
        from app.unsafe_url_continuation import (
            claim_external_url_auto_approval,
            complete_external_url_auto_approval,
            external_url_auto_approve_max_retries,
            remember_pending_unsafe_url_steps,
        )

        step_id = str(confirmation_chunk.get("tool_step_id") or "").strip()
        urls = [
            str(url).strip()
            for url in (confirmation_chunk.get("urls") or [])
            if str(url).strip()
        ]
        remember_pending_unsafe_url_steps(
            self,
            thread_id,
            [{"tool_step_id": step_id, "urls": urls}],
        )
        yield {
            "type": "external_url_confirmation_received",
            "thread_id": thread_id,
            "account": self.account_key,
            "tool_step_id": step_id,
            "tool_step_ids": list(
                confirmation_chunk.get("tool_step_ids") or ([step_id] if step_id else [])
            ),
            "urls": urls,
            "url_count": len(urls),
        }
        yield confirmation_chunk

        claim = claim_external_url_auto_approval(
            self,
            thread_id=thread_id,
            tool_step_id=step_id,
            urls=urls,
        )
        if claim.get("duplicate"):
            yield {
                "type": "external_url_auto_approved",
                "duplicate": True,
                "thread_id": thread_id,
                "tool_step_id": step_id,
                "urls": urls,
                "url_count": len(urls),
                "reason": claim.get("reason"),
                "receipt_id": claim.get("receipt_id"),
                "continuation": {"approved": True, "stream_completed": False},
            }
            return
        if not claim.get("claimed"):
            receipt = complete_external_url_auto_approval(
                self,
                thread_id=thread_id,
                tool_step_id=step_id,
                approved=False,
                error=str(claim.get("reason") or "claim_failed"),
            )
            yield {
                "type": "external_url_approval_failed",
                "thread_id": thread_id,
                "tool_step_id": step_id,
                "urls": urls,
                "url_count": len(urls),
                "reason": claim.get("reason") or "claim_failed",
                "error_type": "operational_error",
                "receipt": receipt,
            }
            raise NotionUpstreamError(
                "Notion external URL auto-approval continuation failed.",
                status_code=502,
                retriable=False,
                response_excerpt=(
                    f"state=external_url_approval_failed;"
                    f"tool_step_id={step_id};"
                    f"reason={claim.get('reason') or 'claim_failed'}"
                ),
            )

        summary: dict[str, Any] = {}
        last_error: Exception | None = None
        nested_confirmations: list[dict[str, Any]] = []
        max_retries = external_url_auto_approve_max_retries()
        for attempt in range(1, max_retries + 2):
            nested_confirmations = []
            try:
                for cont_chunk in self.iter_continue_confirmed_tool_steps(
                    thread_id=thread_id,
                    tool_step_ids=[step_id],
                    summary=summary,
                ):
                    if isinstance(cont_chunk, dict) and cont_chunk.get("type") == "tool_confirmation":
                        nested_confirmations.append(cont_chunk)
                        continue
                    yield cont_chunk
                last_error = None
                break
            except NotionUpstreamError as exc:
                last_error = exc
                if not exc.retriable or attempt > max_retries:
                    break
            except requests.exceptions.RequestException as exc:
                last_error = NotionUpstreamError(
                    "Request to Notion upstream failed while continuing confirmed tool steps.",
                    retriable=True,
                    response_excerpt=str(exc),
                )
                if attempt > max_retries:
                    break

        if last_error is not None:
            receipt = complete_external_url_auto_approval(
                self,
                thread_id=thread_id,
                tool_step_id=step_id,
                approved=False,
                error=str(last_error),
            )
            yield {
                "type": "external_url_approval_failed",
                "thread_id": thread_id,
                "tool_step_id": step_id,
                "urls": urls,
                "url_count": len(urls),
                "reason": "continuation_failed",
                "error_type": "operational_error",
                "receipt": receipt,
            }
            raise NotionUpstreamError(
                "Notion external URL auto-approval continuation failed.",
                status_code=getattr(last_error, "status_code", 502) or 502,
                retriable=False,
                response_excerpt=(
                    f"state=external_url_approval_failed;"
                    f"tool_step_id={step_id};"
                    f"reason=continuation_failed"
                ),
            ) from last_error

        # If Notion requested another URL mid-continuation, the current step is
        # considered applied once its id is present even if finishedAt arrives later.
        approved = bool(summary.get("approved")) or (
            step_id in set(summary.get("applied_tool_step_ids") or [])
            and bool(nested_confirmations)
        )
        if nested_confirmations and step_id in set(summary.get("applied_tool_step_ids") or []):
            summary = {
                **summary,
                "approved": True,
                "reason": "approved",
                "stream_completed": bool(summary.get("stream_completed")),
            }
            approved = True

        receipt = complete_external_url_auto_approval(
            self,
            thread_id=thread_id,
            tool_step_id=step_id,
            approved=approved,
            continuation_result=summary,
            error=None if approved else str(summary.get("reason") or "confirmation_not_applied"),
        )
        if not approved:
            yield {
                "type": "external_url_approval_failed",
                "thread_id": thread_id,
                "tool_step_id": step_id,
                "urls": urls,
                "url_count": len(urls),
                "reason": summary.get("reason") or "confirmation_not_applied",
                "error_type": "operational_error",
                "receipt": receipt,
            }
            raise NotionUpstreamError(
                "Notion external URL auto-approval continuation failed.",
                status_code=502,
                retriable=False,
                response_excerpt=(
                    f"state=external_url_approval_failed;"
                    f"tool_step_id={step_id};"
                    f"reason={summary.get('reason') or 'confirmation_not_applied'}"
                ),
            )

        yield {
            "type": "external_url_auto_approved",
            "duplicate": False,
            "thread_id": thread_id,
            "account": self.account_key,
            "tool_step_id": step_id,
            "urls": urls,
            "url_count": len(urls),
            "reason": summary.get("reason") or "approved",
            "receipt": receipt,
            "continuation": {
                "approved": True,
                "stream_completed": bool(summary.get("stream_completed")) and not nested_confirmations,
                "trace_id": summary.get("trace_id"),
                "event_count": int(summary.get("event_count") or 0),
            },
        }

        nested_completed = False
        for nested in nested_confirmations:
            for event in self._auto_continue_external_url_confirmation(
                thread_id=thread_id,
                confirmation_chunk=nested,
            ):
                if (
                    isinstance(event, dict)
                    and event.get("type") == "external_url_auto_approved"
                    and event.get("continuation", {}).get("stream_completed")
                ):
                    nested_completed = True
                yield event
        if nested_completed:
            yield {
                "type": "external_url_auto_approved",
                "duplicate": True,
                "thread_id": thread_id,
                "tool_step_id": step_id,
                "urls": urls,
                "url_count": len(urls),
                "reason": "nested_continuation_completed",
                "continuation": {"approved": True, "stream_completed": True},
            }

    def stream_response(
        self,
        transcript: list,
        thread_id: Optional[str] = None,
        attachments: list | None = None,
        persist_remote_chat: Optional[bool] = None,
        computer_use_review: Optional[bool] = None,
        thread_title: Optional[str] = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        text Notion API text
        text transcript text

        Args:
            transcript: text
            thread_id: text thread_idtext
            persist_remote_chat: text Notion text
            computer_use_review: keep workflow thread + script agent for ZIP extraction
        """
        if not isinstance(transcript, list) or not transcript:
            raise ValueError("Invalid transcript payload: transcript must be a non-empty list.")

        notion_transcript = self._to_notion_transcript(transcript)
        if str(thread_id or "").strip():
            validate_bound_thread_transcript(notion_transcript)
        thread_type = self._resolve_thread_type(notion_transcript)
        if computer_use_review:
            notion_transcript = self._with_computer_use_capabilities(notion_transcript)
            thread_type = "workflow"
        if attachments and not computer_use_review:
            # Native uploads belong to an ordinary Notion AI chat. Attachment
            # transport must not silently reclassify the persisted thread as a workflow.
            thread_type = "markdown-chat"
            notion_transcript = self._with_thread_type(notion_transcript, thread_type)
        request_profile = self._resolve_request_profile(thread_type)
        thread_persistence = _resolve_thread_persistence()

        from app.thread_title import normalize_thread_title
        requested_thread_title = normalize_thread_title(thread_title)

        if persist_remote_chat is not None:
            if persist_remote_chat:
                thread_persistence["persist"] = True
                thread_persistence["delete_after_stream"] = False
                thread_persistence["generate_title"] = True
                thread_persistence["save_all_thread_operations"] = True
                thread_persistence["set_unread_state"] = True
            else:
                thread_persistence["persist"] = False
                thread_persistence["delete_after_stream"] = True

        if requested_thread_title and thread_persistence["persist"]:
            thread_persistence["generate_title"] = False

        if not thread_persistence["persist"]:
            request_profile["precreate_thread"] = False

        # text thread_idtext
        should_create_thread = thread_id is None
        thread_id = thread_id or str(uuid.uuid4())
        trace_id = str(uuid.uuid4())
        response = None
        scraper = None

        # text thread_id text
        self.current_thread_id = thread_id

        if attachments and should_create_thread and not computer_use_review:
            # A normal file chat must exist as a markdown-chat thread before
            # requesting an assistant-chat upload descriptor. Letting the
            # descriptor create an implicit session and then continuing it as
            # markdown-chat produces an invalid runInferenceTranscript payload.
            if not self._create_thread(thread_id, thread_type):
                raise NotionUpstreamError(
                    "Failed to create an ordinary Notion AI chat for the attachment request.",
                    status_code=502,
                    retriable=True,
                    response_excerpt="attachment_chat_precreate_failed",
                )
            should_create_thread = False
            request_profile["create_thread"] = False
            request_profile["precreate_thread"] = False
            request_profile["is_partial_transcript"] = True

        if (
            requested_thread_title
            and thread_persistence["persist"]
            and not should_create_thread
        ):
            self.set_thread_title(thread_id, requested_thread_title)

        uploaded_attachments: list[UploadedAttachment] = []
        if attachments:
            try:
                uploader = NotionAttachmentUploader(self)
                attachment_create_thread = bool(request_profile["create_thread"] and should_create_thread)
                uploaded_attachments, resolved_thread_id = uploader.upload_attachments(
                    thread_id=thread_id,
                    attachments=list(attachments),
                    create_thread=attachment_create_thread,
                )
                if resolved_thread_id and resolved_thread_id != thread_id:
                    thread_id = resolved_thread_id
                    self.current_thread_id = thread_id
            except NotionAttachmentUploadError as exc:
                raise NotionUpstreamError(
                    "Attachment upload staging failed.",
                    status_code=502,
                    retriable=True,
                    response_excerpt=str(getattr(exc, "reason", "attachment_upload_failed"))[:300],
                ) from exc

            attachment_steps = self._build_attachment_transcript_steps(
                uploaded_attachments,
                computer_file=bool(computer_use_review),
            )
            if attachment_steps:
                notion_transcript = notion_transcript + attachment_steps
                if computer_use_review and thread_persistence["persist"]:
                    notion_transcript.append(self._build_computer_use_zip_instruction_step())
                    if should_create_thread:
                        # A new workflow needs inference to create its thread after
                        # the assistant-chat upload pointer has been staged.
                        request_profile["create_thread"] = True
                        request_profile["is_partial_transcript"] = False
                    else:
                        # A bound review conversation is a continuation even when
                        # each model pass attaches a fresh source archive.
                        request_profile["create_thread"] = False
                        request_profile["is_partial_transcript"] = True
                else:
                    should_create_thread = False
                    request_profile["create_thread"] = False

        if computer_use_review and (uploaded_attachments or thread_type == "workflow"):
            self.warm_script_agent_cache()

        if request_profile["precreate_thread"] and should_create_thread:
            if not self._create_thread(thread_id, thread_type):
                should_create_thread = True
                request_profile["create_thread"] = True
                request_profile["is_partial_transcript"] = False
        elif not should_create_thread:
            # text
            request_profile["create_thread"] = False
            # text is_partial_transcript=Truetext Notion text
            request_profile["is_partial_transcript"] = True

        # text cookie text headertext cloudscraper text cookie jar
        # textcookie jar text Cloudflare challenge text ASCII text cookietext
        cookie_header = self._build_cookie_header()

        headers = {
            "Content-Type": "application/json",
            "Accept": "application/x-ndjson",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "x-notion-space-id": self.space_id,
            "x-notion-active-user-header": self.user_id,
            "notion-audit-log-platform": "web",
            "notion-client-version": NOTION_CLIENT_VERSION,
            "origin": "https://www.notion.so",
            "referer": "https://www.notion.so/ai",
            "cookie": cookie_header,
        }

        created_source = (
            "ai_module"
            if uploaded_attachments
            else ("workflows" if thread_type == "workflow" else "ai_module")
        )

        payload = {
            "traceId": trace_id,
            "spaceId": self.space_id,
            "threadId": thread_id,
            "threadType": thread_type,
            "createThread": request_profile["create_thread"],
            "generateTitle": thread_persistence["generate_title"],
            "saveAllThreadOperations": thread_persistence["save_all_thread_operations"],
            "setUnreadState": thread_persistence["set_unread_state"],
            "isPartialTranscript": request_profile["is_partial_transcript"],
            "asPatchResponse": True,
            "patchResponseVersion": 2,
            "createdSource": created_source,
            "isUserInAnySalesAssistedSpace": False,
            "isSpaceSalesAssisted": False,
            "threadParentPointer": {
                "table": "space",
                "id": self.space_id,
                "spaceId": self.space_id,
            },
            "transcript": notion_transcript,
        }
        if uploaded_attachments and not payload["createThread"]:
            payload.pop("threadParentPointer", None)
        if request_profile["include_debug_overrides"]:
            payload["debugOverrides"] = {
                "emitAgentSearchExtractedResults": True,
                "cachedInferences": {},
                "annotationInferences": {},
                "emitInferences": False,
            }

        logger.info(
            "Dispatching request to Notion upstream",
            extra={
                "request_info": {
                    "event": "notion_upstream_request",
                    "trace_id": trace_id,
                    "thread_id": thread_id,
                    "thread_type": thread_type,
                    "create_thread": bool(request_profile["create_thread"]),
                    "is_partial_transcript": bool(request_profile["is_partial_transcript"]),
                    "persist_thread": bool(thread_persistence["persist"]),
                    "save_all_thread_operations": bool(thread_persistence["save_all_thread_operations"]),
                    "delete_after_stream": bool(thread_persistence["delete_after_stream"]),
                    "account": self.account_key,
                    "space_id": self.space_id,
                }
            },
        )

        try:
            # Create a fresh, isolated scraper for this request to ensure thread safety.
            # Reusing requests.Session concurrently across threads is not thread-safe.
            if cloudscraper is not None:
                scraper = cloudscraper.create_scraper()
            else:
                scraper = requests.Session()
            scraper.cookies.clear()
            response = scraper.post(
                self.url,
                headers=headers,
                json=payload,
                stream=True,
                timeout=(NOTION_UPSTREAM_CONNECT_TIMEOUT, NOTION_UPSTREAM_READ_TIMEOUT),
            )
            if response.status_code == 403:
                # Cloudflare challenge text scraper text
                response.close()
                logger.warning(
                    "Got 403, rebuilding cloudscraper to refresh Cloudflare challenge",
                    extra={"request_info": {"event": "cloudflare_challenge_refresh", "account": self.account_key}},
                )
                if cloudscraper is not None:
                    scraper = cloudscraper.create_scraper()
                else:
                    scraper = requests.Session()
                response = scraper.post(
                    self.url,
                    headers=headers,
                    json=payload,
                    stream=True,
                    timeout=(NOTION_UPSTREAM_CONNECT_TIMEOUT, NOTION_UPSTREAM_READ_TIMEOUT),
                )
            if response.status_code != 200:
                excerpt = (response.text or "").strip().replace("\n", " ")[:300]
                retriable = response.status_code >= 500 or response.status_code == 429
                if uploaded_attachments:
                    stage = "runInferenceTranscript_after_attachment_staging"
                    excerpt = f"stage={stage}; attachment_count={len(uploaded_attachments)}; {excerpt}"
                    message = (
                        "Notion rejected runInferenceTranscript after attachment staging "
                        f"with HTTP {response.status_code}."
                    )
                else:
                    message = f"Notion upstream returned HTTP {response.status_code}."
                raise NotionUpstreamError(
                    message,
                    status_code=response.status_code,
                    retriable=retriable,
                    response_excerpt=excerpt,
                )

            # The thread exists once inference is accepted. Apply the explicit
            # title before consuming a potentially long model stream so the UI
            # never exposes Notion's prompt-derived placeholder for minutes.
            if requested_thread_title and thread_persistence["persist"]:
                self.set_thread_title(thread_id, requested_thread_title)

            emitted = False
            stream_completed = False
            from app.unsafe_url_continuation import (
                remember_pending_unsafe_url_steps,
                should_auto_continue_external_url_confirmations,
            )

            auto_enabled = should_auto_continue_external_url_confirmations()
            active_response = response
            response = None
            try:
                for chunk in parse_stream(active_response):
                    if isinstance(chunk, dict) and chunk.get("type") == "stream_complete":
                        stream_completed = True
                        continue
                    if isinstance(chunk, dict) and chunk.get("type") == "tool_confirmation":
                        step_id = str(chunk.get("tool_step_id") or "").strip()
                        urls = [
                            str(url).strip()
                            for url in (chunk.get("urls") or [])
                            if str(url).strip()
                        ]
                        remember_pending_unsafe_url_steps(
                            self,
                            thread_id,
                            [{"tool_step_id": step_id, "urls": urls}],
                        )
                        logger.info(
                            "Captured transient unsafe URL confirmation from live stream",
                            extra={
                                "request_info": {
                                    "event": "unsafe_url_confirmation_captured",
                                    "thread_id": thread_id,
                                    "tool_step_id": step_id,
                                    "urls": urls,
                                    "url_count": len(urls),
                                }
                            },
                        )
                        emitted = True
                        if not auto_enabled:
                            yield {
                                "type": "external_url_confirmation_received",
                                "thread_id": thread_id,
                                "account": self.account_key,
                                "tool_step_id": step_id,
                                "tool_step_ids": list(
                                    chunk.get("tool_step_ids") or ([step_id] if step_id else [])
                                ),
                                "urls": urls,
                                "url_count": len(urls),
                            }
                            yield chunk
                            continue

                        try:
                            active_response.close()
                        except Exception:
                            pass
                        for event in self._auto_continue_external_url_confirmation(
                            thread_id=thread_id,
                            confirmation_chunk=chunk,
                        ):
                            if (
                                isinstance(event, dict)
                                and event.get("type") == "external_url_auto_approved"
                                and event.get("continuation", {}).get("stream_completed")
                            ):
                                stream_completed = True
                            emitted = True
                            yield event
                        break

                    emitted = True
                    yield chunk
            finally:
                if active_response is not None:
                    try:
                        active_response.close()
                    except Exception:
                        pass

            if not stream_completed:
                raise NotionUpstreamError(
                    "Notion upstream stream ended before completion metadata.",
                    status_code=502,
                    retriable=True,
                    response_excerpt="missing_finishedAt",
                )

            if not emitted:
                raise NotionUpstreamError(
                    "Notion upstream returned an empty stream.",
                    status_code=502,
                    retriable=True,
                )

            if thread_persistence["delete_after_stream"]:
                try:
                    self.delete_thread(thread_id)
                    logger.info(
                        "Ephemeral Notion thread cleaned up after proxy response",
                        extra={
                            "request_info": {
                                "event": "ephemeral_thread_deleted",
                                "thread_id": thread_id,
                                "was_created_new": should_create_thread,
                            }
                        },
                    )
                except Exception:
                    logger.warning(
                        "Ephemeral Notion thread cleanup failed",
                        exc_info=True,
                        extra={
                            "request_info": {
                                "event": "ephemeral_thread_delete_failed",
                                "thread_id": thread_id,
                            }
                        },
                    )
            else:
                if requested_thread_title:
                    self.set_thread_title(thread_id, requested_thread_title)
                # text thread
                # textNotion API text workflow text
                # text thread textAI text
                # text thread text
                logger.info(
                    "Thread completed and preserved for conversation context",
                    extra={
                        "request_info": {
                            "event": "thread_completed_preserved",
                            "thread_id": thread_id,
                            "was_created_new": should_create_thread,
                        }
                    },
                )
        except requests.exceptions.Timeout as exc:
            logger.error(f"Request timeout: {exc}", exc_info=True)
            raise NotionUpstreamError("Request to Notion upstream timed out.", retriable=True) from exc
        except requests.exceptions.RequestException as exc:
            logger.error(f"Request failed: {exc}", exc_info=True)
            # text
            raise NotionUpstreamError("Request to Notion upstream failed. Please try again later.", retriable=True) from exc
        finally:
            if response is not None:
                response.close()
            if scraper is not None:
                try:
                    scraper.close()
                except Exception:
                    pass

    @staticmethod
    def _normalize_notion_id(value: str, *, field_name: str) -> str:
        """Normalize a Notion block/page identifier to a canonical UUID string."""
        clean = str(value or "").strip().replace("-", "")
        if len(clean) != 32:
            raise ValueError(f"{field_name} must be a 32-character Notion UUID.")
        try:
            return str(uuid.UUID(hex=clean))
        except ValueError as exc:
            raise ValueError(f"{field_name} must be a valid Notion UUID.") from exc

    @staticmethod
    def _resolve_page_upload_file(
        *,
        file_path: str,
        filename: str | None,
        content_type: str | None,
    ) -> tuple[Any, str, str, int]:
        """Validate and resolve a local file using the existing attachment policy."""
        import mimetypes
        from pathlib import Path

        from app.attachments.errors import AttachmentError
        from app.attachments.security import (
            AttachmentPolicy,
            normalize_content_type,
            validate_local_path_allowed,
            validate_size,
        )

        policy = AttachmentPolicy.from_env()
        validate_local_path_allowed(policy)
        if not policy.local_root:
            raise AttachmentError(
                "ATTACHMENT_LOCAL_ROOT must be configured for page uploads.",
                code="attachment_local_root_required",
                param="file_path",
            )

        try:
            path = Path(str(file_path or "")).expanduser().resolve(strict=True)
        except (FileNotFoundError, OSError) as exc:
            raise AttachmentError(
                "Upload file does not exist or cannot be accessed.",
                code="upload_file_not_found",
                param="file_path",
            ) from exc
        if not path.is_file():
            raise AttachmentError(
                "Upload path must reference a file.",
                code="upload_path_not_file",
                param="file_path",
            )

        try:
            root = Path(policy.local_root).expanduser().resolve(strict=True)
        except (FileNotFoundError, OSError) as exc:
            raise AttachmentError(
                "ATTACHMENT_LOCAL_ROOT does not exist or cannot be accessed.",
                code="attachment_local_root_invalid",
                param="file_path",
            ) from exc
        try:
            path.relative_to(root)
        except ValueError as exc:
            raise AttachmentError(
                "Upload path is outside ATTACHMENT_LOCAL_ROOT.",
                code="attachment_path_outside_root",
                param="file_path",
            ) from exc

        file_size = path.stat().st_size
        validate_size(file_size, policy)

        requested_name = str(filename or path.name).strip()
        safe_name = Path(requested_name).name
        if not safe_name or safe_name in {".", ".."} or safe_name != requested_name:
            raise AttachmentError(
                "filename must be a basename without directory components.",
                code="invalid_upload_filename",
                param="filename",
            )

        guessed_type, _ = mimetypes.guess_type(safe_name)
        normalized_type = normalize_content_type(
            content_type or guessed_type or "application/octet-stream"
        )
        if not normalized_type:
            raise AttachmentError(
                "content_type could not be determined.",
                code="upload_content_type_required",
                param="content_type",
            )
        return path, safe_name, normalized_type, file_size

    def check_page_access(self, page_id: str) -> dict[str, Any]:
        """Check whether this Notion account can read a page through the v3 API."""
        normalized_page_id = self._normalize_notion_id(page_id, field_name="page_id")
        endpoint = "https://www.notion.so/api/v3/loadPageChunk"
        payload = {
            "pageId": normalized_page_id,
            "limit": 20,
            "cursor": {"stack": []},
            "chunkNumber": 0,
            "verticalColumns": False,
        }
        try:
            response = self._scraper.post(
                endpoint,
                headers=self._build_chat_history_headers(),
                json=payload,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise NotionUpstreamError(
                "Notion page-access check failed.",
                retriable=True,
                response_excerpt=str(exc)[:300],
            ) from exc

        try:
            body = response.json()
        except ValueError:
            body = {}
        record_map = body.get("recordMap") if isinstance(body, dict) else None
        block_map = record_map.get("block", {}) if isinstance(record_map, dict) else {}
        candidate_ids = {
            normalized_page_id,
            normalized_page_id.replace("-", ""),
        }
        available_ids = {
            str(block_id).replace("-", "")
            for block_id in block_map.keys()
        } if isinstance(block_map, dict) else set()
        accessible = response.status_code == 200 and bool(
            {candidate.replace("-", "") for candidate in candidate_ids} & available_ids
        )
        error_value = ""
        if isinstance(body, dict):
            error_value = str(
                body.get("message")
                or body.get("error")
                or body.get("errorId")
                or ""
            )
        if not error_value and response.status_code != 200:
            error_value = _redact_response_excerpt(response.text)
        return {
            "ok": True,
            "page_id": normalized_page_id,
            "accessible": accessible,
            "status_code": response.status_code,
            "space_id": self.space_id,
            "error": error_value[:500],
        }

    def _post_save_transactions(
        self,
        operations: list[dict[str, Any]],
        *,
        error_message: str,
    ) -> None:
        if not operations:
            return
        payload = {
            "requestId": str(uuid.uuid4()),
            "transactions": [
                {
                    "id": str(uuid.uuid4()),
                    "spaceId": self.space_id,
                    "operations": operations,
                }
            ],
        }
        try:
            resp = self._scraper.post(
                self.delete_url,
                headers=self._build_chat_history_headers(),
                json=payload,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise NotionUpstreamError(
                error_message,
                retriable=True,
                response_excerpt=str(exc)[:300],
            ) from exc
        if resp.status_code != 200:
            raise NotionUpstreamError(
                error_message,
                status_code=resp.status_code,
                retriable=resp.status_code >= 500 or resp.status_code == 429,
                response_excerpt=_redact_response_excerpt(resp.text or ""),
            )

    @staticmethod
    def _page_url(page_id: str) -> str:
        clean = str(page_id or "").replace("-", "")
        return f"https://www.notion.so/{clean}"

    @staticmethod
    def _page_app_url(page_id: str) -> str:
        clean = str(page_id or "").replace("-", "")
        return f"notion://www.notion.so/{clean}"

    def _load_page_chunk(self, page_id: str) -> dict[str, Any]:
        normalized_page_id = self._normalize_notion_id(page_id, field_name="page_id")
        endpoint = "https://www.notion.so/api/v3/loadPageChunk"
        payload = {
            "pageId": normalized_page_id,
            "limit": 100,
            "cursor": {"stack": []},
            "chunkNumber": 0,
            "verticalColumns": False,
        }
        try:
            response = self._scraper.post(
                endpoint,
                headers=self._build_chat_history_headers(),
                json=payload,
                timeout=30,
            )
        except requests.RequestException as exc:
            raise NotionUpstreamError(
                "Notion page load failed.",
                retriable=True,
                response_excerpt=str(exc)[:300],
            ) from exc
        try:
            body = response.json()
        except ValueError as exc:
            raise NotionUpstreamError(
                "Notion page load returned invalid JSON.",
                status_code=response.status_code,
                retriable=True,
                response_excerpt=(response.text or "")[:300],
            ) from exc
        if response.status_code != 200:
            raise NotionUpstreamError(
                "Notion page load returned an error.",
                status_code=response.status_code,
                retriable=response.status_code >= 500 or response.status_code == 429,
                response_excerpt=_redact_response_excerpt(response.text or ""),
            )
        if not isinstance(body, dict):
            return {}
        return body

    def _page_content_ids(self, page_id: str) -> list[str]:
        body = self._load_page_chunk(page_id)
        record_map = body.get("recordMap") if isinstance(body, dict) else {}
        block_map = record_map.get("block", {}) if isinstance(record_map, dict) else {}
        normalized = self._normalize_notion_id(page_id, field_name="page_id")
        candidates = [normalized, normalized.replace("-", "")]
        page_value: dict[str, Any] | None = None
        for candidate in candidates:
            entry = block_map.get(candidate)
            if isinstance(entry, dict):
                value = entry.get("value")
                if isinstance(value, dict):
                    page_value = value
                    break
        if not page_value:
            return []
        content = page_value.get("content")
        if not isinstance(content, list):
            return []
        return [str(item) for item in content if item]

    def create_child_page(
        self,
        *,
        parent_page_id: str,
        title: str,
    ) -> dict[str, Any]:
        """Create a child page owned by this Notion account."""
        normalized_parent_id = self._normalize_notion_id(
            parent_page_id,
            field_name="parent_page_id",
        )
        access = self.check_page_access(normalized_parent_id)
        if not access.get("accessible"):
            raise ValueError(
                "Parent page is not readable by the configured Notion account. "
                f"Set repo_ai_parent_page_id to a page in this workspace. ({access.get('error') or 'no access'})"
            )

        page_id = str(uuid.uuid4())
        now_ms = int(time.time() * 1000)
        clean_title = str(title or "Untitled").strip() or "Untitled"
        page_block = {
            "id": page_id,
            "version": 1,
            "type": "page",
            "properties": {"title": [[clean_title]]},
            "format": {"page_full_width": False, "page_small_text": False},
            "parent_id": normalized_parent_id,
            "parent_table": "block",
            "space_id": self.space_id,
            "created_time": now_ms,
            "last_edited_time": now_ms,
            "created_by_id": self.user_id,
            "created_by_table": "notion_user",
            "last_edited_by_id": self.user_id,
            "last_edited_by_table": "notion_user",
            "alive": True,
        }
        operations = [
            {
                "pointer": {
                    "table": "block",
                    "id": page_id,
                    "spaceId": self.space_id,
                },
                "command": "set",
                "path": [],
                "args": page_block,
            },
            {
                "pointer": {
                    "table": "block",
                    "id": normalized_parent_id,
                    "spaceId": self.space_id,
                },
                "command": "listAfter",
                "path": ["content"],
                "args": {"id": page_id},
            },
        ]
        self._post_save_transactions(
            operations,
            error_message="Failed to create Notion child page.",
        )
        return {
            "ok": True,
            "page_id": page_id,
            "page_url": self._page_url(page_id),
            "page_app_url": self._page_app_url(page_id),
            "parent_page_id": normalized_parent_id,
            "title": clean_title,
        }

    def delete_block_children(
        self,
        page_id: str,
        *,
        preserve_types: set[str] | None = None,
    ) -> int:
        """Soft-delete child blocks on a page, optionally preserving block types."""
        normalized_page_id = self._normalize_notion_id(page_id, field_name="page_id")
        preserved = preserve_types or set()
        body = self._load_page_chunk(normalized_page_id)
        record_map = body.get("recordMap") if isinstance(body, dict) else {}
        block_map = record_map.get("block", {}) if isinstance(record_map, dict) else {}
        deleted = 0
        for child_id in self._page_content_ids(normalized_page_id):
            entry = block_map.get(child_id) or block_map.get(child_id.replace("-", ""))
            value = entry.get("value") if isinstance(entry, dict) else None
            if not isinstance(value, dict):
                continue
            block_type = str(value.get("type") or "")
            if block_type in preserved:
                continue
            self._post_save_transactions(
                [
                    {
                        "pointer": {
                            "table": "block",
                            "id": self._normalize_notion_id(child_id, field_name="block_id"),
                            "spaceId": self.space_id,
                        },
                        "command": "update",
                        "path": [],
                        "args": {"alive": False},
                    }
                ],
                error_message="Failed to delete Notion block children.",
            )
            deleted += 1
        return deleted

    @staticmethod
    def _rich_text_plain(rich_text: list[dict[str, Any]] | None) -> str:
        if not isinstance(rich_text, list):
            return ""
        parts: list[str] = []
        for item in rich_text:
            if not isinstance(item, dict):
                continue
            if item.get("type") == "text":
                text_obj = item.get("text")
                if isinstance(text_obj, dict):
                    parts.append(str(text_obj.get("content") or ""))
        return "".join(parts)

    def _integration_block_to_v3(self, block: dict[str, Any], block_id: str, page_id: str, now_ms: int) -> dict[str, Any]:
        block_type = str(block.get("type") or "paragraph")
        mapping = {
            "paragraph": "text",
            "heading_1": "header",
            "heading_2": "sub_header",
            "heading_3": "sub_sub_header",
            "bulleted_list_item": "bulleted_list",
            "callout": "callout",
            "toggle": "toggle",
        }
        notion_type = mapping.get(block_type, "text")
        payload = block.get(block_type)
        if not isinstance(payload, dict):
            payload = {}
        title = self._rich_text_plain(payload.get("rich_text"))
        properties: dict[str, Any] = {"title": [[title]]}
        if notion_type == "callout":
            icon = payload.get("icon")
            if isinstance(icon, dict) and icon.get("emoji"):
                properties["icon"] = icon["emoji"]
        return {
            "id": block_id,
            "version": 1,
            "type": notion_type,
            "properties": properties,
            "format": {},
            "parent_id": page_id,
            "parent_table": "block",
            "space_id": self.space_id,
            "created_time": now_ms,
            "last_edited_time": now_ms,
            "created_by_id": self.user_id,
            "created_by_table": "notion_user",
            "last_edited_by_id": self.user_id,
            "last_edited_by_table": "notion_user",
            "alive": True,
        }

    def append_integration_blocks(self, page_id: str, children: list[dict[str, Any]]) -> int:
        """Append blocks expressed in Notion public API shape to a page."""
        normalized_page_id = self._normalize_notion_id(page_id, field_name="page_id")
        if not children:
            return 0
        now_ms = int(time.time() * 1000)
        appended = 0
        for block in children:
            if not isinstance(block, dict):
                continue
            block_id = str(uuid.uuid4())
            v3_block = self._integration_block_to_v3(
                block,
                block_id,
                normalized_page_id,
                now_ms,
            )
            self._post_save_transactions(
                [
                    {
                        "pointer": {
                            "table": "block",
                            "id": block_id,
                            "spaceId": self.space_id,
                        },
                        "command": "set",
                        "path": [],
                        "args": v3_block,
                    },
                    {
                        "pointer": {
                            "table": "block",
                            "id": normalized_page_id,
                            "spaceId": self.space_id,
                        },
                        "command": "listAfter",
                        "path": ["content"],
                        "args": {"id": block_id},
                    },
                ],
                error_message="Failed to append blocks to Notion page.",
            )
            appended += 1
        return appended

    def resolve_repo_ai_parent_page_id(self, requested_parent_page_id: str = "") -> str:
        """Resolve the parent page for Repo AI dashboards using account/env config."""
        candidates = [
            str(requested_parent_page_id or "").strip(),
            str(getattr(self, "repo_ai_parent_page_id", "") or "").strip(),
            str(self.context_page_id or "").strip(),
            str(os.getenv("REPO_AI_NOTION_PARENT_PAGE_ID") or "").strip(),
            str(os.getenv("NOTION_CONTEXT_PAGE_ID") or "").strip(),
        ]
        seen: set[str] = set()
        for candidate in candidates:
            if not candidate or candidate in seen:
                continue
            seen.add(candidate)
            try:
                normalized = self._normalize_notion_id(candidate, field_name="parent_page_id")
            except ValueError:
                continue
            access = self.check_page_access(normalized)
            if access.get("accessible"):
                return normalized
        return ""

    def request_general_upload_descriptor(
        self,
        *,
        name: str,
        content_type: str,
        size: int,
        page_id: str,
    ) -> dict[str, Any]:
        """Request an upload descriptor bound to the existing parent page."""
        normalized_page_id = self._normalize_notion_id(page_id, field_name="page_id")
        notion_content_type = _notion_upload_content_type(name, content_type)
        endpoint = "https://www.notion.so/api/v3/getUploadFileUrl"
        payload = {
            "bucket": "secure",
            "name": name,
            "contentType": notion_content_type,
            "record": {
                "table": "block",
                "id": normalized_page_id,
                "spaceId": self.space_id,
            },
            "contentLength": size,
        }
        if _is_zip_upload(name, notion_content_type):
            payload["allowUnsupportedTypes"] = True
        try:
            resp = self._scraper.post(
                endpoint,
                headers=self._build_chat_history_headers(),
                json=payload,
                timeout=30,
            )
        except Exception as exc:
            raise NotionUpstreamError(
                "Failed to request general upload descriptor.",
                status_code=None,
                retriable=True,
                response_excerpt=str(exc),
            ) from exc

        if resp.status_code != 200:
            excerpt = _redact_response_excerpt(resp.text or "")
            raise NotionUpstreamError(
                "General upload descriptor request failed.",
                status_code=resp.status_code,
                retriable=resp.status_code >= 500 or resp.status_code == 429,
                response_excerpt=excerpt,
            )
        try:
            descriptor = self._normalize_upload_descriptor(resp.json())
        except NotionUpstreamError:
            raise
        except Exception as exc:
            raise NotionUpstreamError(
                "General upload descriptor response was invalid JSON.",
                status_code=resp.status_code,
                retriable=True,
                response_excerpt=_redact_response_excerpt(resp.text or ""),
            ) from exc

        if not descriptor.get("attachment_url") and descriptor.get("file_id"):
            descriptor["attachment_url"] = (
                f"attachment:{descriptor['file_id']}:{normalized_page_id}"
            )
        if not descriptor.get("upload_url") or not descriptor.get("attachment_url"):
            raise NotionUpstreamError(
                "General upload descriptor was missing required upload fields.",
                status_code=resp.status_code,
                retriable=False,
            )
        return descriptor

    def perform_multipart_file_upload(
        self,
        *,
        descriptor: dict[str, Any],
        name: str,
        file_path: Any,
        content_type: str,
    ) -> None:
        """Stream a local file to a signed multipart upload URL."""
        upload_url = str(descriptor.get("upload_url") or "").strip()
        fields = descriptor.get("fields") or {}
        if not upload_url:
            raise NotionUpstreamError("Descriptor missing upload URL.", retriable=False)
        if not isinstance(fields, dict):
            raise NotionUpstreamError("Descriptor upload fields were malformed.", retriable=False)

        try:
            with file_path.open("rb") as stream:
                response = requests.post(
                    upload_url,
                    data=fields,
                    files={"file": (name, stream, content_type)},
                    timeout=(15, 300),
                )
        except Exception as exc:
            raise NotionUpstreamError(
                "Multipart file upload failed.",
                retriable=True,
                response_excerpt=str(exc),
            ) from exc
        if response.status_code < 200 or response.status_code >= 300:
            raise NotionUpstreamError(
                "Multipart file upload returned an error.",
                status_code=response.status_code,
                retriable=response.status_code >= 500 or response.status_code == 429,
                response_excerpt=_redact_response_excerpt(response.text or ""),
            )

    def append_file_block_to_page(
        self,
        *,
        page_id: str,
        block_id: str,
        file_url: str,
        filename: str,
        file_size: int,
    ) -> str:
        """Append an uploaded file block to a Notion page using saveTransactions."""
        normalized_page_id = self._normalize_notion_id(page_id, field_name="page_id")
        normalized_block_id = self._normalize_notion_id(block_id, field_name="block_id")
        now_ms = int(time.time() * 1000)
        block = {
            "id": normalized_block_id,
            "version": 1,
            "type": "file",
            "properties": {
                "title": [[filename]],
                "source": [[file_url]],
                "size": [[str(file_size)]],
            },
            "format": {},
            "parent_id": normalized_page_id,
            "parent_table": "block",
            "space_id": self.space_id,
            "created_time": now_ms,
            "last_edited_time": now_ms,
            "created_by_id": self.user_id,
            "created_by_table": "notion_user",
            "last_edited_by_id": self.user_id,
            "last_edited_by_table": "notion_user",
            "alive": True,
        }
        payload = {
            "requestId": str(uuid.uuid4()),
            "transactions": [
                {
                    "id": str(uuid.uuid4()),
                    "spaceId": self.space_id,
                    "operations": [
                        {
                            "pointer": {
                                "table": "block",
                                "id": normalized_block_id,
                                "spaceId": self.space_id,
                            },
                            "command": "set",
                            "path": [],
                            "args": block,
                        },
                        {
                            "pointer": {
                                "table": "block",
                                "id": normalized_page_id,
                                "spaceId": self.space_id,
                            },
                            "command": "listAfter",
                            "path": ["content"],
                            "args": {"id": normalized_block_id},
                        },
                    ],
                }
            ],
        }
        try:
            resp = self._scraper.post(
                "https://www.notion.so/api/v3/saveTransactions",
                headers=self._build_chat_history_headers(),
                json=payload,
                timeout=30,
            )
        except Exception as exc:
            raise NotionUpstreamError(
                "Failed to append file block to page.",
                retriable=True,
                response_excerpt=str(exc),
            ) from exc
        if resp.status_code != 200:
            raise NotionUpstreamError(
                "Appending the file block failed.",
                status_code=resp.status_code,
                retriable=resp.status_code >= 500 or resp.status_code == 429,
                response_excerpt=_redact_response_excerpt(resp.text or ""),
            )
        return normalized_block_id

    def upload_file_to_page(
        self,
        *,
        page_id: str,
        file_path: str,
        filename: str | None = None,
        content_type: str | None = None,
    ) -> dict[str, Any]:
        """Upload a local file and append it as a File block on a Notion page."""
        normalized_page_id = self._normalize_notion_id(page_id, field_name="page_id")
        path, name, normalized_type, file_size = self._resolve_page_upload_file(
            file_path=file_path,
            filename=filename,
            content_type=content_type,
        )
        block_id = str(uuid.uuid4())
        descriptor = self.request_general_upload_descriptor(
            name=name,
            content_type=normalized_type,
            size=file_size,
            page_id=normalized_page_id,
        )
        self.perform_multipart_file_upload(
            descriptor=descriptor,
            name=name,
            file_path=path,
            content_type=normalized_type,
        )
        file_url = str(descriptor["attachment_url"])
        persisted_block_id = self.append_file_block_to_page(
            page_id=normalized_page_id,
            block_id=block_id,
            file_url=file_url,
            filename=name,
            file_size=file_size,
        )
        signed_get_url = self.get_signed_read_url(
            file_url,
            download_name=name,
            permission_table="block",
            permission_id=normalized_page_id,
        )
        return {
            "ok": True,
            "page_id": normalized_page_id,
            "block_id": persisted_block_id,
            "file_url": file_url,
            "signed_get_url": signed_get_url,
            "filename": name,
            "content_type": normalized_type,
            "size": file_size,
        }
