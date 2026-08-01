from __future__ import annotations

import inspect

from typing import Any, Literal

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from app.attachments.errors import AttachmentError
from app.logger import logger
from app.governance import governance_receipt_from_client
from app.mutation_policy import (
    MutationPolicy,
    MutationPolicyError,
    require_mutation_capability,
)
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
    page_app_url: str
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
    governance: dict[str, Any] = Field(default_factory=dict)


class AccountSummaryResponse(BaseModel):
    account_number: int
    profile_name: str
    user_name: str
    user_email: str
    space_id: str
    user_id: str
    selected: bool
    next_in_rotation: bool
    available: bool
    cooldown_remaining_seconds: float
    governance_aligned: bool


class AccountSelectionResponse(BaseModel):
    ok: bool
    mode: Literal["auto", "pinned"]
    selected_account_number: int | None = None
    selected_profile_name: str | None = None
    next_account_number: int | None = None
    previous_selection: dict[str, Any] = Field(default_factory=dict)
    effective_for_new_requests: bool = True
    persistence_enabled: bool = False
    accounts: list[AccountSummaryResponse] = Field(default_factory=list)
    governance: dict[str, Any] = Field(default_factory=dict)


class AccountSwitchRequest(BaseModel):
    mode: Literal["auto", "pinned"] = "pinned"
    selector: str | None = None


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


def _error_detail(
    *,
    message: str,
    code: str,
    error_type: str,
    param: str | None = None,
    detail: str = "",
) -> dict:
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

async def _resolve_plan_authorization(
    request: Request,
    client: Any,
    capability: str,
    governance: dict[str, Any],
) -> dict[str, Any]:
    decision_engine = getattr(
        request.app.state, "mutation_decision_engine", None
    )
    if callable(decision_engine):
        decision = decision_engine(
            request=request,
            client=client,
            capability=capability,
            governance=governance,
        )
        if inspect.isawaitable(decision):
            decision = await decision
        return dict(decision or {}) if isinstance(decision, dict) else {}

    trusted_state_decision = getattr(request.state, "plan_authorization", None)
    if isinstance(trusted_state_decision, dict):
        return dict(trusted_state_decision)

    configured_decision = getattr(request.app.state, "plan_authorization", None)
    if isinstance(configured_decision, dict):
        return dict(configured_decision)
    return {}


async def _require_notion_mutation(
    request: Request,
    client: Any,
    capability: str,
) -> dict[str, object]:
    policy = getattr(request.app.state, "mutation_policy", None)
    if policy is not None and not isinstance(policy, MutationPolicy):
        raise HTTPException(
            status_code=500,
            detail=_error_detail(
                message="The runtime mutation policy is invalid.",
                code="invalid_mutation_policy",
                error_type="server_error",
            ),
        )
    governance = governance_receipt_from_client(client)
    plan_authorization = await _resolve_plan_authorization(
        request, client, capability, governance
    )
    try:
        return require_mutation_capability(
            capability,
            governance_aligned=bool(governance.get("aligned")),
            plan_authorization=plan_authorization,
            policy=policy,
        )
    except MutationPolicyError as exc:
        raise HTTPException(
            status_code=403,
            detail=_error_detail(
                message=str(exc),
                code="notion_mutation_not_authorized",
                error_type="permission_error",
                param="capability",
            ),
        ) from exc



@router.post("/notion/upload_file", response_model=UploadPageFileResponse)
async def upload_page_file(
    request: Request, body: UploadPageFileRequest
) -> UploadPageFileResponse:
    """Upload a local file into a page File block using the configured Notion account."""
    try:
        client = request.app.state.account_pool.get_client()
        await _require_notion_mutation(request, client, "page.upload")
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


def _account_selection_response(pool: Any) -> AccountSelectionResponse:
    summary = pool.get_selection_summary()
    return AccountSelectionResponse(
        ok=True,
        mode=summary["mode"],
        selected_account_number=summary.get("selected_account_number"),
        selected_profile_name=summary.get("selected_profile_name"),
        next_account_number=summary.get("next_account_number"),
        previous_selection=dict(summary.get("previous_selection") or {}),
        effective_for_new_requests=bool(
            summary.get("effective_for_new_requests", True)
        ),
        persistence_enabled=bool(summary.get("persistence_enabled", False)),
        accounts=[
            AccountSummaryResponse(**item) for item in summary.get("accounts", [])
        ],
        governance=pool.get_governance_summary(),
    )


@router.get("/notion/accounts", response_model=AccountSelectionResponse)
async def list_accounts(request: Request) -> AccountSelectionResponse:
    """List safe account metadata and the current AIgentBee selection mode."""
    return _account_selection_response(request.app.state.account_pool)


@router.post("/notion/accounts/switch", response_model=AccountSelectionResponse)
async def switch_account(
    request: Request,
    body: AccountSwitchRequest,
) -> AccountSelectionResponse:
    """Pin AIgentBee to a named account or restore automatic rotation."""
    pool = request.app.state.account_pool
    try:
        if body.mode == "pinned":
            selector = str(body.selector or "").strip()
            if not selector:
                raise ValueError("selector is required when mode is pinned")
            candidate = pool.get_client_for_selector(selector)
            governance = governance_receipt_from_client(candidate)
            if not governance.get("aligned"):
                raise HTTPException(
                    status_code=409,
                    detail=_error_detail(
                        message="Selected account is not aligned with the canonical governance contract.",
                        code="notion_account_governance_mismatch",
                        error_type="conflict_error",
                    ),
                )
            authority_page_id = str(governance.get("authority_page_id") or "").strip()
            if authority_page_id:
                access = await run_in_threadpool(
                    candidate.check_page_access, authority_page_id
                )
                if not access.get("accessible"):
                    raise HTTPException(
                        status_code=409,
                        detail=_error_detail(
                            message="Selected account cannot access the canonical governance authority page.",
                            code="notion_account_authority_unavailable",
                            error_type="conflict_error",
                        ),
                    )
        pool.switch_account(mode=body.mode, selector=body.selector)
        return _account_selection_response(pool)
    except HTTPException:
        raise
    except ValueError as exc:
        raise HTTPException(
            status_code=400,
            detail=_error_detail(
                message=str(exc),
                code="invalid_notion_account_selector",
                error_type="invalid_request_error",
                param="selector",
            ),
        ) from exc
    except RuntimeError as exc:
        raise HTTPException(
            status_code=409,
            detail=_error_detail(
                message=str(exc),
                code="notion_account_unavailable",
                error_type="conflict_error",
            ),
        ) from exc
    except NotionUpstreamError as exc:
        raise HTTPException(
            status_code=503 if exc.retriable else 502,
            detail=_error_detail(
                message=str(exc),
                code="notion_account_validation_failed",
                error_type="upstream_error",
                detail=exc.response_excerpt,
            ),
        ) from exc


@router.post("/notion/accounts/rollback", response_model=AccountSelectionResponse)
async def rollback_account_switch(request: Request) -> AccountSelectionResponse:
    """Restore the selection state that existed immediately before the last switch."""
    pool = request.app.state.account_pool
    pool.rollback_account_switch()
    return _account_selection_response(pool)


@router.get("/notion/account_info", response_model=AccountInfoResponse)
async def account_info(request: Request) -> AccountInfoResponse:
    """Return the selected Notion account and resolved Repo AI parent page metadata."""
    client = request.app.state.account_pool.get_metadata_client()
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
        governance=governance_receipt_from_client(client),
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
        client = (
            next(
                (
                    candidate
                    for candidate in getattr(pool, "clients", [])
                    if get_remembered_unsafe_url_steps(candidate, thread_id)
                ),
                None,
            )
            or pool.get_client()
        )
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
        await _require_notion_mutation(request, client, "page.create")
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


@router.post(
    "/notion/delete_block_children", response_model=DeleteBlockChildrenResponse
)
async def delete_block_children(
    request: Request,
    body: DeleteBlockChildrenRequest,
) -> DeleteBlockChildrenResponse:
    """Delete child blocks from a page, preserving selected block types."""
    try:
        client = request.app.state.account_pool.get_client()
        await _require_notion_mutation(request, client, "page.delete_children")
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
async def append_blocks(
    request: Request, body: AppendBlocksRequest
) -> AppendBlocksResponse:
    """Append public-API-shaped blocks to a page."""
    try:
        client = request.app.state.account_pool.get_client()
        await _require_notion_mutation(request, client, "page.append")
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
