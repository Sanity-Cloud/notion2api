import os
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

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 可通过环境变量覆盖 Notion 客户端版本号（Notion 更新后可能需要同步）
NOTION_CLIENT_VERSION = os.getenv("NOTION_CLIENT_VERSION", "23.13.20260228.0625")


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
    """Notion 上游请求失败或返回异常内容。"""

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


class NotionOpusAPI:
    def __init__(self, account_config: dict):
        """
        从单组账号配置初始化 Notion 客户端。
        account_config 需要包含 token_v2, space_id, user_id, space_view_id, user_name, user_email
        """
        self.token_v2 = account_config.get("token_v2", "")
        self.space_id = account_config.get("space_id", "")
        self.user_id = account_config.get("user_id", "")
        self.space_view_id = account_config.get("space_view_id", "")
        self.user_name = account_config.get("user_name", "user")
        self.user_email = account_config.get("user_email", "")
        self.cookies = account_config.get("cookies", {})
        if not isinstance(self.cookies, dict):
            self.cookies = {}
        self.cookies["token_v2"] = self.token_v2

        self.url = "https://www.notion.so/api/v3/runInferenceTranscript"
        self.delete_url = "https://www.notion.so/api/v3/saveTransactions"
        self.account_key = self.user_email or self.user_id or "unknown-account"

        # Reuse cloudscraper instance when available; otherwise fall back to requests.Session.
        if cloudscraper is not None:
            self._scraper = cloudscraper.create_scraper()
        else:
            self._scraper = requests.Session()
        self._scraper_lock = threading.Lock()

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
        payload = {
            "name": name,
            "contentType": content_type,
            "assistantChatTranscriptSessionPointer": {
                "spaceId": self.space_id,
                "table": "thread",
                "id": thread_id,
            },
            "contentLength": size,
            "createThread": create_thread,
        }
        if _attachment_descriptor_debug_enabled():
            logger.warning(
                "Attachment descriptor request",
                extra={
                    "request_info": {
                        "event": "attachment_descriptor_request",
                        "endpoint": "getUploadFileUrlForAssistantChatTranscriptUpload",
                        "payload_keys": sorted(payload.keys()),
                        "fileName": name,
                        "contentType": content_type,
                        "size": size,
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

    def get_signed_read_url(self, attachment_url: str, thread_id: str = "", download_name: str = "") -> str:
        endpoint = "https://www.notion.so/api/v3/getSignedFileUrls"
        payload = {
            "urls": [
                {
                    "url": attachment_url,
                    "download": False,
                    "downloadName": download_name,
                    "permissionRecord": {
                        "table": "thread",
                        "id": thread_id,
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

    def _build_attachment_transcript_steps(self, uploaded_attachments: list[UploadedAttachment]) -> list[dict[str, Any]]:
        steps: list[dict[str, Any]] = []
        for uploaded in uploaded_attachments:
            steps.append({
                "id": str(uuid.uuid4()),
                "type": "attachment",
                "fileName": uploaded.name,
                "contentType": uploaded.content_type,
                "fileUrl": uploaded.attachment_url,
                "metadata": uploaded.metadata or {},
            })
        return steps

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
                                "type": thread_type,
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

    def stream_response(
        self,
        transcript: list,
        thread_id: Optional[str] = None,
        attachments: list | None = None,
        persist_remote_chat: Optional[bool] = None,
    ) -> Generator[dict[str, Any], None, None]:
        """
        发起 Notion API 请求并返回结构化流生成器。
        接收完整的 transcript 列表作为参数。

        Args:
            transcript: 对话历史记录列表
            thread_id: 可选的已有 thread_id。如果提供，将重用该线程以保持上下文
            persist_remote_chat: 是否持久化 Notion 端的聊天线程（覆盖默认配置）
        """
        if not isinstance(transcript, list) or not transcript:
            raise ValueError("Invalid transcript payload: transcript must be a non-empty list.")

        notion_transcript = self._to_notion_transcript(transcript)
        thread_type = self._resolve_thread_type(notion_transcript)
        request_profile = self._resolve_request_profile(thread_type)
        thread_persistence = _resolve_thread_persistence()

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

        if not thread_persistence["persist"]:
            request_profile["precreate_thread"] = False

        # 如果没有提供 thread_id，创建新的；否则重用已有的
        should_create_thread = thread_id is None
        thread_id = thread_id or str(uuid.uuid4())
        trace_id = str(uuid.uuid4())
        response = None

        # 保存 thread_id 以便外部访问
        self.current_thread_id = thread_id

        uploaded_attachments: list[UploadedAttachment] = []
        if attachments:
            try:
                uploader = NotionAttachmentUploader(self)
                uploaded_attachments, resolved_thread_id = uploader.upload_attachments(
                    thread_id=thread_id,
                    attachments=list(attachments),
                    create_thread=request_profile["create_thread"],
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

            attachment_steps = self._build_attachment_transcript_steps(uploaded_attachments)
            if attachment_steps:
                notion_transcript = notion_transcript + attachment_steps
                should_create_thread = False
                request_profile["create_thread"] = False

        if request_profile["precreate_thread"] and should_create_thread:
            if not self._create_thread(thread_id, thread_type):
                should_create_thread = True
                request_profile["create_thread"] = True
                request_profile["is_partial_transcript"] = False
        elif not should_create_thread:
            # 如果重用已有线程，不要创建新线程
            request_profile["create_thread"] = False
            # 关键修复：设置 is_partial_transcript=True，让 Notion 接受客户端的历史消息
            request_profile["is_partial_transcript"] = True

        # 把 cookie 直接放进 header，绕过 cloudscraper 的 cookie jar
        # （cookie jar 可能被 Cloudflare challenge 写入含非 ASCII 字符的 cookie，导致编码错误）
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
            "createdSource": "workflows" if uploaded_attachments else "ai_module",
            "isUserInAnySalesAssistedSpace": False,
            "isSpaceSalesAssisted": False,
            "threadParentPointer": {
                "table": "space",
                "id": self.space_id,
                "spaceId": self.space_id,
            },
            "transcript": notion_transcript,
        }
        if uploaded_attachments:
            if not payload["createThread"]:
                payload.pop("threadParentPointer", None)
            payload["attachments"] = [
                {
                    "type": "attachment",
                    "fileName": uploaded.name,
                    "contentType": uploaded.content_type,
                    "fileUrl": uploaded.attachment_url,
                }
                for uploaded in uploaded_attachments
            ]
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
            with self._scraper_lock:
                scraper = self._scraper
                scraper.cookies.clear()
            response = scraper.post(
                self.url,
                headers=headers,
                json=payload,
                stream=True,
                timeout=(15, 120),
            )
            if response.status_code == 403:
                # Cloudflare challenge 可能过期，重建 scraper 后重试一次
                response.close()
                logger.warning(
                    "Got 403, rebuilding cloudscraper to refresh Cloudflare challenge",
                    extra={"request_info": {"event": "cloudflare_challenge_refresh", "account": self.account_key}},
                )
                new_scraper = cloudscraper.create_scraper()
                with self._scraper_lock:
                    self._scraper = new_scraper
                response = new_scraper.post(
                    self.url,
                    headers=headers,
                    json=payload,
                    stream=True,
                    timeout=(15, 120),
                )
            if response.status_code != 200:
                excerpt = (response.text or "").strip().replace("\n", " ")[:300]
                # 429 和 5xx 都允许重试（换账号或等待后重试）
                retriable = response.status_code >= 500 or response.status_code == 429
                raise NotionUpstreamError(
                    f"Notion upstream returned HTTP {response.status_code}.",
                    status_code=response.status_code,
                    retriable=retriable,
                    response_excerpt=excerpt,
                )

            emitted = False
            for chunk in parse_stream(response):
                emitted = True
                yield chunk

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
                # 流结束后，不再自动删除 thread
                # 原因：Notion API 的 workflow 模式依赖于服务器端保存的对话历史
                # 删除 thread 会导致后续请求无法获取历史消息（AI 失忆）
                # 保持 thread 存活可以维持对话上下文
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
            # 不暴露原始异常细节给用户
            raise NotionUpstreamError("Request to Notion upstream failed. Please try again later.", retriable=True) from exc
        finally:
            if response is not None:
                response.close()
