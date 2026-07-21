from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from app.attachments.errors import AttachmentError
from app.logger import logger
from app.notion_client import NotionUpstreamError
from app.unsafe_url_continuation import (
    allow_pending_unsafe_urls_once,
    get_remembered_unsafe_url_steps,
)

router = APIRouter(tags=["notion"])


class UploadPageFileRequest(BaseModel):
    page_id: str = Field(min_length=1)
    file_path: str = Field(min_length=1)
    filename: str | None = None
    content_type: str | None = None


class UploadPageFileResponse(BaseModel):
    ok: bool
    page_id: str
    block_id: str
    file_url: str
    signed_get_url: str
    filename: str
    content_type: str
    size: int


class CheckPageAccessRequest(BaseModel):
    page_id: str = Field(min_length=1)


class CheckPageAccessResponse(BaseModel):
    ok: bool
    page_id: str
    accessible: bool
    status_code: int
    space_id: str
    error: str


class CreatePageRequest(BaseModel):
    title: str = Field(min_length=1)
    parent_page_id: str | None = None


class CreatePageResponse(BaseModel):
    ok: bool
    page_id: str
    page_url: str
    parent_page_id: str
    title: str


class DeleteBlockChildrenRequest(BaseModel):
    page_id: str = Field(min_length=1)
    preserve_types: list[str] = Field(default_factory=list)


class DeleteBlockChildrenResponse(BaseModel):
    ok: bool
    page_id: str
    deleted_count: int


class AppendBlocksRequest(BaseModel):
    page_id: str = Field(min_length=1)
    children: list[dict[str, Any]] = Field(default_factory=list)


class AppendBlocksResponse(BaseModel):
    ok: bool
    page_id: str
    appended_count: int


class AccountInfoResponse(BaseModel):
    ok: bool
    space_id: str
    user_id: str
    repo_ai_parent_page_id: str
    parent_page_accessible: bool
    context_page_id: str


class AllowUnsafeUrlOnceRequest(BaseModel):
    thread_id: str = Field(min_length=1)
    tool_step_ids: list[str] = Field(default_factory=list)


class AllowUnsafeUrlOnceResponse(BaseModel):
    ok: bool
    continued: bool
    approved: bool = False
    thread_id: str
    tool_step_ids: list[str] = Field(default_factory=list)
    urls: list[str] = Field(default_factory=list)
    trace_id: str = ""
    stream_completed: bool = False
    event_count: int = 0
    event_types: list[str] = Field(default_factory=list)
    applied_tool_step_ids: list[str] = Field(default_factory=list)
    unresolved_tool_step_ids: list[str] = Field(default_factory=list)
    reason: str = ""


def _error_detail(*, message: str, code: str, error_type: str, param: str | None = None, detail: str = "") -> dict:
    payload = {
        "message": message,
        "type": error_type,
        "code": code,
    }
    if param:
        payload["param"] = param
    if detail:
        payload["detail"] = detail
    return {"error": payload}


@router.post("/notion/upload_file", response_model=UploadPageFileResponse)
async def upload_page_file(request: Request, body: UploadPageFileRequest) -> UploadPageFileResponse:
    """Upload a local file into a page File block using the configured Notion account."""
    try:
        client = request.app.state.account_pool.get_client()
        result = await run_in_threadpool(
            client.upload_file_to_page,
            page_id=body.page_id.strip(),
            file_path=body.file_path,
            filename=body.filename,
            content_type=body.content_type,
        )
        return UploadPageFileResponse(**result)
    except AttachmentError as exc:
        raise HTTPException(
            status_code=exc.status_code,
            detail=_error_detail(
                message=str(exc),
                code=exc.code,
                error_type="invalid_request_error",
                param=exc.param,
            ),
        ) from exc
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                message=str(exc),
                code="invalid_notion_page_upload",
                error_type="invalid_request_error",
            ),
        ) from exc
    except NotionUpstreamError as exc:
        raise HTTPException(
            status_code=503 if exc.retriable else 502,
            detail=_error_detail(
                message=str(exc),
                code="notion_page_upload_failed",
                error_type="upstream_error",
                detail=exc.response_excerpt,
            ),
        ) from exc
    except Exception as exc:
        logger.error(
            "Failed to upload file to Notion page",
            exc_info=True,
            extra={"request_info": {"event": "notion_page_upload_failed"}},
        )
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                message="Internal server error while uploading the file.",
                code="notion_page_upload_internal_error",
                error_type="server_error",
            ),
        ) from exc


@router.post("/notion/check_page_access", response_model=CheckPageAccessResponse)
async def check_page_access(
    request: Request,
    body: CheckPageAccessRequest,
) -> CheckPageAccessResponse:
    """Check whether the configured Notion account can read a page."""
    try:
        client = request.app.state.account_pool.get_client()
        result = await run_in_threadpool(client.check_page_access, body.page_id.strip())
        return CheckPageAccessResponse(**result)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                message=str(exc),
                code="invalid_notion_page_id",
                error_type="invalid_request_error",
                param="page_id",
            ),
        ) from exc
    except NotionUpstreamError as exc:
        raise HTTPException(
            status_code=503 if exc.retriable else 502,
            detail=_error_detail(
                message=str(exc),
                code="notion_page_access_check_failed",
                error_type="upstream_error",
                detail=exc.response_excerpt,
            ),
        ) from exc


@router.get("/notion/account_info", response_model=AccountInfoResponse)
async def account_info(request: Request) -> AccountInfoResponse:
    """Return the active Notion account and resolved Repo AI parent page metadata."""
    client = request.app.state.account_pool.get_client()
    parent_page_id = client.resolve_repo_ai_parent_page_id()
    parent_accessible = False
    if parent_page_id:
        access = client.check_page_access(parent_page_id)
        parent_accessible = bool(access.get("accessible"))
    return AccountInfoResponse(
        ok=True,
        space_id=client.space_id,
        user_id=client.user_id,
        repo_ai_parent_page_id=parent_page_id,
        parent_page_accessible=parent_accessible,
        context_page_id=client.context_page_id,
    )


@router.post("/notion/unsafe_url/allow_once", response_model=AllowUnsafeUrlOnceResponse)
async def allow_unsafe_url_once(
    request: Request,
    body: AllowUnsafeUrlOnceRequest,
) -> AllowUnsafeUrlOnceResponse:
    """Grant pending web load confirmations and resume the existing Notion inference."""
    try:
        pool = request.app.state.account_pool
        thread_id = body.thread_id.strip()
        # Confirmation step IDs are account-scoped. Prefer the exact client
        # that captured this transient step instead of round-robin routing.
        client = next(
            (
                candidate
                for candidate in getattr(pool, "clients", [])
                if get_remembered_unsafe_url_steps(candidate, thread_id)
            ),
            None,
        ) or pool.get_client()
        result = await run_in_threadpool(
            allow_pending_unsafe_urls_once,
            client,
            thread_id=thread_id,
            tool_step_ids=body.tool_step_ids,
        )
        return AllowUnsafeUrlOnceResponse(**result)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                message=str(exc),
                code="invalid_unsafe_url_continuation",
                error_type="invalid_request_error",
            ),
        ) from exc
    except NotionUpstreamError as exc:
        raise HTTPException(
            status_code=503 if exc.retriable else 502,
            detail=_error_detail(
                message=str(exc),
                code="notion_unsafe_url_continuation_failed",
                error_type="upstream_error",
                detail=exc.response_excerpt,
            ),
        ) from exc


@router.post("/notion/create_page", response_model=CreatePageResponse)
async def create_page(request: Request, body: CreatePageRequest) -> CreatePageResponse:
    """Create a child page using the configured Notion account."""
    try:
        client = request.app.state.account_pool.get_client()
        requested_parent = str(body.parent_page_id or "").strip()
        if requested_parent:
            parent_page_id = client._normalize_notion_id(
                requested_parent,
                field_name="parent_page_id",
            )
            access = client.check_page_access(parent_page_id)
            if not access.get("accessible"):
                raise ValueError(
                    "Parent page is not readable by the configured Notion account. "
                    f"({access.get('error') or 'no access'})"
                )
        else:
            parent_page_id = client.resolve_repo_ai_parent_page_id()
            if not parent_page_id:
                raise ValueError(
                    "No accessible Repo AI parent page is configured. Set repo_ai_parent_page_id "
                    "in accounts.json or REPO_AI_NOTION_PARENT_PAGE_ID to a page in this workspace."
                )
        result = await run_in_threadpool(
            client.create_child_page,
            parent_page_id=parent_page_id,
            title=body.title.strip(),
        )
        return CreatePageResponse(**result)
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                message=str(exc),
                code="invalid_notion_page_request",
                error_type="invalid_request_error",
            ),
        ) from exc
    except NotionUpstreamError as exc:
        raise HTTPException(
            status_code=503 if exc.retriable else 502,
            detail=_error_detail(
                message=str(exc),
                code="notion_page_create_failed",
                error_type="upstream_error",
                detail=exc.response_excerpt,
            ),
        ) from exc


@router.post("/notion/delete_block_children", response_model=DeleteBlockChildrenResponse)
async def delete_block_children(
    request: Request,
    body: DeleteBlockChildrenRequest,
) -> DeleteBlockChildrenResponse:
    """Delete child blocks from a page, preserving selected block types."""
    try:
        client = request.app.state.account_pool.get_client()
        deleted_count = await run_in_threadpool(
            client.delete_block_children,
            body.page_id.strip(),
            preserve_types=set(body.preserve_types),
        )
        return DeleteBlockChildrenResponse(
            ok=True,
            page_id=body.page_id.strip(),
            deleted_count=deleted_count,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                message=str(exc),
                code="invalid_notion_page_id",
                error_type="invalid_request_error",
                param="page_id",
            ),
        ) from exc
    except NotionUpstreamError as exc:
        raise HTTPException(
            status_code=503 if exc.retriable else 502,
            detail=_error_detail(
                message=str(exc),
                code="notion_block_delete_failed",
                error_type="upstream_error",
                detail=exc.response_excerpt,
            ),
        ) from exc


@router.post("/notion/append_blocks", response_model=AppendBlocksResponse)
async def append_blocks(request: Request, body: AppendBlocksRequest) -> AppendBlocksResponse:
    """Append public-API-shaped blocks to a page."""
    try:
        client = request.app.state.account_pool.get_client()
        appended_count = await run_in_threadpool(
            client.append_integration_blocks,
            body.page_id.strip(),
            body.children,
        )
        return AppendBlocksResponse(
            ok=True,
            page_id=body.page_id.strip(),
            appended_count=appended_count,
        )
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                message=str(exc),
                code="invalid_notion_page_id",
                error_type="invalid_request_error",
                param="page_id",
            ),
        ) from exc
    except NotionUpstreamError as exc:
        raise HTTPException(
            status_code=503 if exc.retriable else 502,
            detail=_error_detail(
                message=str(exc),
                code="notion_block_append_failed",
                error_type="upstream_error",
                detail=exc.response_excerpt,
            ),
        ) from exc
