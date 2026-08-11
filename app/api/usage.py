from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from starlette.concurrency import run_in_threadpool

from app.notion_admission import get_notion_usage_store
from app.notion_usage import normalize_notion_ai_usage


router = APIRouter(prefix="/usage", tags=["usage"])

_QUOTA_FIELDS = {
    "scope",
    "account_key",
    "workload_class",
    "window_seconds",
    "max_requests",
    "max_tokens",
    "max_request_bytes",
    "enabled",
}


def _contract(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        **payload,
        "account_identifiers": "opaque_sha256_prefix",
        "token_basis": "actual_total_tokens_when_available_else_estimate",
        "billing_grade": False,
        "notion_allowance": {
            "rolling_window_seconds": 21600,
            "monthly_window": "provider_reported_plan_cycle",
            "excluded_products": ["custom_agents", "workers"],
            "observations_are_authoritative_for_enforcement": False,
        },
    }


@router.get("/summary")
def usage_summary(
    window_seconds: int = Query(default=3600, ge=60, le=31 * 86400),
    account_key: str = Query(default="", max_length=160),
    workload_class: str = Query(default="", max_length=32),
) -> dict[str, Any]:
    summary = get_notion_usage_store().usage_summary(
        window_seconds=window_seconds,
        account_key=account_key,
        workload_class=workload_class,
    )
    return _contract({"usage": summary})


@router.get("/quotas")
def list_quotas(include_disabled: bool = True) -> dict[str, Any]:
    quotas = get_notion_usage_store().list_quotas(include_disabled=include_disabled)
    return _contract({"quotas": quotas, "count": len(quotas)})


@router.get("/allowance")
def latest_allowance(
    account_key: str = Query(min_length=1, max_length=160),
) -> dict[str, Any]:
    observation = get_notion_usage_store().latest_allowance_observation(
        account_key=account_key
    )
    return _contract({"allowance": observation})


@router.get("/provider")
async def provider_usage(
    request: Request,
    profile_name: str = Query(min_length=1, max_length=160),
) -> dict[str, Any]:
    try:
        client = request.app.state.account_pool.get_client_for_selector(profile_name)
        payload = await run_in_threadpool(client.get_ai_usage_eligibility)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _contract({"provider_usage": normalize_notion_ai_usage(payload)})


@router.put("/allowance")
async def put_allowance(request: Request) -> dict[str, Any]:
    try:
        parsed = await request.json()
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="Request body must be JSON"
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="Request body must be an object")
    allowed = {
        "account_key",
        "rolling_used_percent",
        "rolling_resets_at",
        "monthly_used_percent",
        "monthly_resets_at",
        "observed_at",
        "source",
    }
    unknown = sorted(set(parsed) - allowed)
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported allowance fields: {', '.join(unknown)}",
        )
    try:
        observation = get_notion_usage_store().record_allowance_observation(**parsed)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return _contract({"allowance": observation})


@router.get("/quotas/status")
def quota_status(
    account_key: str = Query(default="", max_length=160),
    workload_class: str = Query(default="legacy", max_length=32),
    projected_requests: int = Query(default=0, ge=0),
    projected_tokens: int = Query(default=0, ge=0),
    projected_request_bytes: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    statuses = get_notion_usage_store().quota_status(
        account_key=account_key,
        workload_class=workload_class,
        projected_requests=projected_requests,
        projected_tokens=projected_tokens,
        projected_request_bytes=projected_request_bytes,
    )
    return _contract({"quotas": statuses, "count": len(statuses)})


@router.put("/quotas/{quota_id}")
async def put_quota(quota_id: str, request: Request) -> dict[str, Any]:
    try:
        parsed = await request.json()
    except Exception as exc:
        raise HTTPException(
            status_code=400, detail="Request body must be JSON"
        ) from exc
    if not isinstance(parsed, dict):
        raise HTTPException(status_code=400, detail="Request body must be an object")
    unknown = sorted(set(parsed) - _QUOTA_FIELDS)
    if unknown:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported quota fields: {', '.join(unknown)}",
        )
    try:
        get_notion_usage_store().upsert_quota(quota_id, **parsed)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    quota = get_notion_usage_store().get_quota(quota_id, public=True)
    return _contract({"quota": quota})
