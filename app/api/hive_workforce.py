from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel, Field
from starlette.concurrency import run_in_threadpool

from app.hive_materialization import get_hive_materialization_store
from app.hive_runtime import HiveRuntimeError
from app.hive_workforce_control_plane import (
    RecruitmentPolicy,
    WorkforceControlPlaneSnapshot,
)

router = APIRouter(prefix="/hive/workforce", tags=["hive-workforce"])


class PolicyUpdateRequest(BaseModel):
    actor: str
    policy: RecruitmentPolicy
    governance_authorization: dict[str, Any] | None = None
    human_approval: bool = False
    expected_revision: int | None = None


class LeaseHeartbeatRequest(BaseModel):
    lease_id: str
    actor: str
    heartbeat_status: str = "RUNNING"
    extend_seconds: int | None = 3600
    evidence: dict[str, Any] = Field(default_factory=dict)
    idempotency_key: str | None = None


class LeaseReconcileRequest(BaseModel):
    actor: str = "portal-operator"
    dry_run: bool = True
    stale_after_seconds: int | None = None
    no_heartbeat_grace_seconds: int | None = None
    revoke_stale: bool = False
    governance_authorization: dict[str, Any] | None = None
    human_approval: bool = False
    idempotency_key: str | None = None


class RecruitmentProcessRequest(BaseModel):
    actor: str = "portal-operator"
    governance_authorization: dict[str, Any] | None = None
    human_approval: bool = False
    limit: int = 10


class WorkforceAuditRequest(BaseModel):
    actor: str = "portal-operator"
    dry_run: bool = True
    stale_after_days: int = 30
    include_protected: bool = False
    governance_authorization: dict[str, Any] | None = None
    human_approval: bool = False
    idempotency_key: str | None = None


def _store():
    return get_hive_materialization_store()


def _raise_api_error(exc: Exception) -> None:
    status_code = 409 if isinstance(exc, HiveRuntimeError) else 422
    raise HTTPException(status_code=status_code, detail=str(exc)) from exc


@router.get("/overview", response_model=WorkforceControlPlaneSnapshot)
async def get_workforce_overview(
    limit: int = Query(default=250, ge=1, le=1000),
) -> WorkforceControlPlaneSnapshot:
    try:
        return await run_in_threadpool(_store().control_plane.overview, limit=limit)
    except Exception as exc:
        _raise_api_error(exc)


@router.get("/registry")
async def get_workforce_registry(
    limit: int = Query(default=250, ge=1, le=1000),
) -> dict[str, Any]:
    snapshot = await get_workforce_overview(limit)
    return {
        "generated_at": snapshot.generated_at,
        "count": len(snapshot.registry),
        "workers": [item.model_dump(mode="json") for item in snapshot.registry],
    }


@router.get("/requisitions")
async def get_requisition_queue(
    limit: int = Query(default=250, ge=1, le=1000),
) -> dict[str, Any]:
    snapshot = await get_workforce_overview(limit)
    return {
        "generated_at": snapshot.generated_at,
        "count": len(snapshot.requisitions),
        "requisitions": [
            item.model_dump(mode="json") for item in snapshot.requisitions
        ],
    }


@router.get("/leases")
async def get_lease_monitor(
    limit: int = Query(default=500, ge=1, le=2000),
) -> dict[str, Any]:
    snapshot = await get_workforce_overview(max(1, limit // 2))
    leases = snapshot.leases[:limit]
    return {
        "generated_at": snapshot.generated_at,
        "count": len(leases),
        "leases": [item.model_dump(mode="json") for item in leases],
    }


@router.get("/policy", response_model=RecruitmentPolicy)
async def get_recruitment_policy() -> RecruitmentPolicy:
    try:
        return await run_in_threadpool(_store().control_plane.get_policy)
    except Exception as exc:
        _raise_api_error(exc)


@router.put("/policy", response_model=RecruitmentPolicy)
async def update_recruitment_policy(
    request: PolicyUpdateRequest,
) -> RecruitmentPolicy:
    try:
        return await run_in_threadpool(
            _store().control_plane.update_policy,
            actor=request.actor,
            policy=request.policy,
            governance_authorization=request.governance_authorization,
            human_approval=request.human_approval,
            expected_revision=request.expected_revision,
        )
    except Exception as exc:
        _raise_api_error(exc)


@router.post("/lease/heartbeat")
async def record_worker_lease_heartbeat(
    request: LeaseHeartbeatRequest,
) -> dict[str, Any]:
    try:
        result = await run_in_threadpool(
            _store().record_lease_heartbeat,
            lease_id=request.lease_id,
            actor=request.actor,
            heartbeat_status=request.heartbeat_status,
            extend_seconds=request.extend_seconds,
            evidence=request.evidence,
            idempotency_key=request.idempotency_key,
        )
        return result.model_dump(mode="json")
    except Exception as exc:
        _raise_api_error(exc)


@router.post("/leases/reconcile")
async def reconcile_stale_leases(
    request: LeaseReconcileRequest,
) -> dict[str, Any]:
    try:
        policy = await run_in_threadpool(_store().control_plane.get_policy)
        result = await run_in_threadpool(
            _store().reconcile_stale_leases,
            actor=request.actor,
            dry_run=request.dry_run,
            heartbeat_stale_after_seconds=(
                request.stale_after_seconds or policy.stale_heartbeat_seconds
            ),
            no_heartbeat_grace_seconds=(
                request.no_heartbeat_grace_seconds
                or policy.no_heartbeat_grace_seconds
            ),
            revoke=request.revoke_stale,
            governance_authorization=request.governance_authorization,
            human_approval=request.human_approval,
            idempotency_key=request.idempotency_key,
        )
        return result.model_dump(mode="json")
    except Exception as exc:
        _raise_api_error(exc)


@router.post("/recruitment/process")
async def process_recruitment_queue(
    request: RecruitmentProcessRequest,
) -> dict[str, Any]:
    try:
        return await run_in_threadpool(
            _store().control_plane.process_recruitment_queue,
            actor=request.actor,
            governance_authorization=request.governance_authorization,
            human_approval=request.human_approval,
            limit=request.limit,
        )
    except Exception as exc:
        _raise_api_error(exc)


@router.post("/audits")
async def run_workforce_audit(
    request: WorkforceAuditRequest,
) -> dict[str, Any]:
    try:
        result = await run_in_threadpool(
            _store().audit_workforce,
            actor=request.actor,
            dry_run=request.dry_run,
            stale_after_days=request.stale_after_days,
            include_protected=request.include_protected,
            governance_authorization=request.governance_authorization,
            human_approval=request.human_approval,
            idempotency_key=request.idempotency_key,
        )
        return result.model_dump(mode="json")
    except Exception as exc:
        _raise_api_error(exc)
