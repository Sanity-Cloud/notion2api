from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel, Field

from app.governed_authorization import (
    GovernedAuthorizationError,
    require_governed_authorization,
)
from app.hive_runtime import HiveIdempotencyConflict, HiveTransitionError
from app.hive_workforce import HiveWorkforceStore, WorkerStage
from app.hive_workforce_lifecycle import (
    DEFAULT_HEARTBEAT_STALE_SECONDS,
    DEFAULT_NO_HEARTBEAT_GRACE_SECONDS,
    GapRecruitmentSnapshot,
    HiveWorkforceLifecycleStore,
)


class RecruitmentPolicy(BaseModel):
    policy_id: str = "default"
    automatic_hiring_enabled: bool = False
    recruitment_mode: str = "requisition_only"
    max_workers: int = 64
    allowed_models: list[str] = Field(default_factory=lambda: ["terra", "glm-5.2"])
    default_model: str = "terra"
    default_account_profile: str = "auto"
    minimum_evaluation_score: float = 0.85
    monthly_budget_limit: float = 0.0
    per_worker_budget_limit: float = 0.0
    quarantine_rules: dict[str, Any] = Field(
        default_factory=lambda: {
            "new_workers": True,
            "failed_evaluations": True,
            "external_effects": True,
        }
    )
    stale_heartbeat_seconds: int = DEFAULT_HEARTBEAT_STALE_SECONDS
    no_heartbeat_grace_seconds: int = DEFAULT_NO_HEARTBEAT_GRACE_SECONDS
    lease_reconcile_interval_seconds: int = 300
    workforce_audit_interval_seconds: int = 86400
    auto_offboard_enabled: bool = False
    updated_by: str = "system"
    updated_at: int = 0
    revision: int = 1


class WorkerRegistryItem(BaseModel):
    worker_id: str
    display_name: str
    worker_class: str
    role: str
    competencies: list[str] = Field(default_factory=list)
    writable_domains: list[str] = Field(default_factory=list)
    model_id: str = ""
    account_profile: str = ""
    appointment_state: str
    runtime_state: str = "UNASSIGNED"
    quarantine_state: str = "NONE"
    current_assignment: dict[str, Any] | None = None
    authority_ceiling: str = "A0"
    accountable_owner: str = ""
    created_at: int = 0
    updated_at: int = 0


class CandidateEvaluationItem(BaseModel):
    evaluation_id: str
    requisition_id: str
    worker_id: str
    candidate_kind: str
    model_id: str = ""
    account_profile: str = ""
    competency_score: float = 0.0
    domain_score: float = 0.0
    authority_score: float = 0.0
    total_score: float = 0.0
    threshold: float = 0.0
    outcome: str = "PENDING"
    evidence: dict[str, Any] = Field(default_factory=dict)
    created_at: int = 0


class RequisitionQueueItem(BaseModel):
    requisition_id: str
    plan_id: str
    objective: str = ""
    requested_competencies: list[str] = Field(default_factory=list)
    requested_writable_domains: list[str] = Field(default_factory=list)
    urgency: str = "NORMAL"
    status: str = "OPEN"
    matching_attempts: int = 0
    candidate_count: int = 0
    evaluations: list[CandidateEvaluationItem] = Field(default_factory=list)
    appointed_worker_id: str = ""
    appointment_outcome: str = "PENDING"
    created_at: int = 0
    updated_at: int = 0


class LeaseMonitorItem(BaseModel):
    lease_id: str
    plan_id: str
    mission_id: str
    work_unit_id: str
    worker_id: str
    status: str
    liveness_status: str
    execution_live: bool = False
    issued_at: int = 0
    expires_at: int = 0
    last_renewal_at: int = 0
    last_heartbeat_at: int = 0
    heartbeat_age_seconds: int | None = None
    stale: bool = False
    automatic_cleanup_action: str = "NONE"
    release_reason: str = ""
    renewal_count: int = 0


class WorkforceMetrics(BaseModel):
    generated_at: int = 0
    time_to_fill_average_seconds: float | None = None
    blocked_plans_competency_gaps: int = 0
    stale_leases_removed: int = 0
    successful_appointments: int = 0
    worker_utilization: float = 0.0
    failed_evaluations: int = 0
    appointed_workers: int = 0
    active_assignments: int = 0
    open_requisitions: int = 0


class WorkforceControlPlaneSnapshot(BaseModel):
    ok: bool = True
    db_path: str = ""
    generated_at: int = 0
    policy: RecruitmentPolicy = Field(default_factory=RecruitmentPolicy)
    registry: list[WorkerRegistryItem] = Field(default_factory=list)
    requisitions: list[RequisitionQueueItem] = Field(default_factory=list)
    leases: list[LeaseMonitorItem] = Field(default_factory=list)
    audits: list[dict[str, Any]] = Field(default_factory=list)
    offboarding_history: list[dict[str, Any]] = Field(default_factory=list)
    metrics: WorkforceMetrics = Field(default_factory=WorkforceMetrics)
    governor_status: dict[str, Any] = Field(default_factory=dict)
    backend_responsibilities: list[str] = Field(
        default_factory=lambda: [
            "heartbeat ingestion",
            "lease expiry and reconciliation",
            "worker creation",
            "candidate evaluation",
            "worker process restart",
            "stale assignment cleanup",
        ]
    )
    error: str = ""


class HiveWorkforceControlPlaneStore:
    """Durable read/control contract for portal workforce observability."""

    def __init__(self, path: str | Path, workforce: HiveWorkforceStore):
        self.path = Path(path).expanduser().resolve()
        self.workforce = workforce
        self.lifecycle = HiveWorkforceLifecycleStore(self.path, workforce)
        self._ensure_schema()

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True)

    @classmethod
    def _fingerprint(cls, value: Any) -> str:
        return hashlib.sha256(cls._json(value).encode("utf-8")).hexdigest()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        now = self._now_ms()
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS hive_recruitment_policies (
                    policy_id TEXT PRIMARY KEY,
                    automatic_hiring_enabled INTEGER NOT NULL DEFAULT 0,
                    recruitment_mode TEXT NOT NULL DEFAULT 'requisition_only',
                    max_workers INTEGER NOT NULL DEFAULT 64,
                    allowed_models_json TEXT NOT NULL DEFAULT '["terra","glm-5.2"]',
                    default_model TEXT NOT NULL DEFAULT 'terra',
                    default_account_profile TEXT NOT NULL DEFAULT 'auto',
                    minimum_evaluation_score REAL NOT NULL DEFAULT 0.85,
                    monthly_budget_limit REAL NOT NULL DEFAULT 0,
                    per_worker_budget_limit REAL NOT NULL DEFAULT 0,
                    quarantine_rules_json TEXT NOT NULL DEFAULT '{}',
                    stale_heartbeat_seconds INTEGER NOT NULL DEFAULT 1800,
                    no_heartbeat_grace_seconds INTEGER NOT NULL DEFAULT 21600,
                    lease_reconcile_interval_seconds INTEGER NOT NULL DEFAULT 300,
                    workforce_audit_interval_seconds INTEGER NOT NULL DEFAULT 86400,
                    auto_offboard_enabled INTEGER NOT NULL DEFAULT 0,
                    updated_by TEXT NOT NULL DEFAULT 'system',
                    updated_at INTEGER NOT NULL,
                    revision INTEGER NOT NULL DEFAULT 1
                );
                CREATE TABLE IF NOT EXISTS hive_worker_profiles (
                    worker_id TEXT PRIMARY KEY REFERENCES hive_workers(worker_id) ON DELETE CASCADE,
                    model_id TEXT NOT NULL DEFAULT '',
                    account_profile TEXT NOT NULL DEFAULT '',
                    runtime_state TEXT NOT NULL DEFAULT 'UNASSIGNED',
                    runtime_instance_id TEXT NOT NULL DEFAULT '',
                    quarantine_state TEXT NOT NULL DEFAULT 'NONE',
                    metadata_json TEXT NOT NULL DEFAULT '{}',
                    last_runtime_event_at INTEGER NOT NULL DEFAULT 0,
                    updated_at INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS hive_requisitions (
                    requisition_id TEXT PRIMARY KEY,
                    plan_id TEXT NOT NULL,
                    objective TEXT NOT NULL DEFAULT '',
                    requested_competencies_json TEXT NOT NULL DEFAULT '[]',
                    requested_writable_domains_json TEXT NOT NULL DEFAULT '[]',
                    urgency TEXT NOT NULL DEFAULT 'NORMAL',
                    status TEXT NOT NULL DEFAULT 'OPEN',
                    matching_attempts INTEGER NOT NULL DEFAULT 0,
                    candidate_count INTEGER NOT NULL DEFAULT 0,
                    appointed_worker_id TEXT NOT NULL DEFAULT '',
                    appointment_outcome TEXT NOT NULL DEFAULT 'PENDING',
                    idempotency_key TEXT UNIQUE,
                    request_sha256 TEXT NOT NULL,
                    created_at INTEGER NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_hive_requisitions_status_updated
                    ON hive_requisitions(status, updated_at DESC);
                CREATE TABLE IF NOT EXISTS hive_candidate_evaluations (
                    evaluation_id TEXT PRIMARY KEY,
                    requisition_id TEXT NOT NULL REFERENCES hive_requisitions(requisition_id) ON DELETE CASCADE,
                    worker_id TEXT NOT NULL,
                    candidate_kind TEXT NOT NULL,
                    model_id TEXT NOT NULL DEFAULT '',
                    account_profile TEXT NOT NULL DEFAULT '',
                    competency_score REAL NOT NULL DEFAULT 0,
                    domain_score REAL NOT NULL DEFAULT 0,
                    authority_score REAL NOT NULL DEFAULT 0,
                    total_score REAL NOT NULL DEFAULT 0,
                    threshold REAL NOT NULL DEFAULT 0,
                    outcome TEXT NOT NULL DEFAULT 'PENDING',
                    evidence_json TEXT NOT NULL DEFAULT '{}',
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_hive_candidate_evaluations_req_created
                    ON hive_candidate_evaluations(requisition_id, created_at DESC);
                CREATE TABLE IF NOT EXISTS hive_workforce_governor_runs (
                    run_id TEXT PRIMARY KEY,
                    started_at INTEGER NOT NULL,
                    finished_at INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    lease_changes INTEGER NOT NULL DEFAULT 0,
                    audit_findings INTEGER NOT NULL DEFAULT 0,
                    audit_actions INTEGER NOT NULL DEFAULT 0,
                    error TEXT NOT NULL DEFAULT ''
                );
                """
            )
            conn.execute(
                """
                INSERT OR IGNORE INTO hive_recruitment_policies(
                    policy_id, automatic_hiring_enabled, recruitment_mode,
                    max_workers, allowed_models_json, default_model,
                    default_account_profile, minimum_evaluation_score,
                    monthly_budget_limit, per_worker_budget_limit,
                    quarantine_rules_json, stale_heartbeat_seconds,
                    no_heartbeat_grace_seconds, lease_reconcile_interval_seconds,
                    workforce_audit_interval_seconds, auto_offboard_enabled,
                    updated_by, updated_at, revision
                ) VALUES ('default', 0, 'requisition_only', 64, ?, 'terra', 'auto',
                          0.85, 0, 0, ?, ?, ?, 300, 86400, 0, 'system', ?, 1)
                """,
                (
                    self._json(["terra", "glm-5.2"]),
                    self._json(
                        {
                            "new_workers": True,
                            "failed_evaluations": True,
                            "external_effects": True,
                        }
                    ),
                    DEFAULT_HEARTBEAT_STALE_SECONDS,
                    DEFAULT_NO_HEARTBEAT_GRACE_SECONDS,
                    now,
                ),
            )

    @staticmethod
    def _policy_from_row(row: sqlite3.Row) -> RecruitmentPolicy:
        return RecruitmentPolicy(
            policy_id=str(row["policy_id"]),
            automatic_hiring_enabled=bool(row["automatic_hiring_enabled"]),
            recruitment_mode=str(row["recruitment_mode"]),
            max_workers=int(row["max_workers"]),
            allowed_models=json.loads(str(row["allowed_models_json"])),
            default_model=str(row["default_model"]),
            default_account_profile=str(row["default_account_profile"]),
            minimum_evaluation_score=float(row["minimum_evaluation_score"]),
            monthly_budget_limit=float(row["monthly_budget_limit"]),
            per_worker_budget_limit=float(row["per_worker_budget_limit"]),
            quarantine_rules=json.loads(str(row["quarantine_rules_json"])),
            stale_heartbeat_seconds=int(row["stale_heartbeat_seconds"]),
            no_heartbeat_grace_seconds=int(row["no_heartbeat_grace_seconds"]),
            lease_reconcile_interval_seconds=int(row["lease_reconcile_interval_seconds"]),
            workforce_audit_interval_seconds=int(row["workforce_audit_interval_seconds"]),
            auto_offboard_enabled=bool(row["auto_offboard_enabled"]),
            updated_by=str(row["updated_by"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    def get_policy(self) -> RecruitmentPolicy:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM hive_recruitment_policies WHERE policy_id='default'"
            ).fetchone()
            if not row:
                raise RuntimeError("Default Hive recruitment policy is missing.")
            return self._policy_from_row(row)

    def update_policy(
        self,
        *,
        actor: str,
        policy: RecruitmentPolicy,
        governance_authorization: dict[str, Any] | None = None,
        human_approval: bool = False,
        expected_revision: int | None = None,
    ) -> RecruitmentPolicy:
        actor_name = str(actor or "").strip()
        if not actor_name:
            raise ValueError("actor is required")
        required_authority = "A3" if policy.automatic_hiring_enabled else "A2"
        try:
            require_governed_authorization(
                governance_authorization,
                required_authority=required_authority,
                legacy_human_approval=human_approval,
            )
        except GovernedAuthorizationError as exc:
            raise HiveTransitionError(str(exc)) from exc
        if policy.recruitment_mode not in {"disabled", "requisition_only", "auto_appoint"}:
            raise ValueError(f"Unsupported recruitment mode: {policy.recruitment_mode}")
        models = sorted({str(item).strip() for item in policy.allowed_models if str(item).strip()})
        if not models or policy.default_model not in models:
            raise ValueError("default_model must be present in allowed_models")
        if not 0 <= float(policy.minimum_evaluation_score) <= 1:
            raise ValueError("minimum_evaluation_score must be between 0 and 1")
        now = self._now_ms()
        with self._write() as conn:
            row = conn.execute(
                "SELECT revision FROM hive_recruitment_policies WHERE policy_id='default'"
            ).fetchone()
            current_revision = int(row["revision"])
            if expected_revision is not None and current_revision != int(expected_revision):
                raise HiveTransitionError(
                    f"Stale policy revision: expected {expected_revision}, current {current_revision}."
                )
            conn.execute(
                """
                UPDATE hive_recruitment_policies SET
                    automatic_hiring_enabled=?, recruitment_mode=?, max_workers=?,
                    allowed_models_json=?, default_model=?, default_account_profile=?,
                    minimum_evaluation_score=?, monthly_budget_limit=?,
                    per_worker_budget_limit=?, quarantine_rules_json=?,
                    stale_heartbeat_seconds=?, no_heartbeat_grace_seconds=?,
                    lease_reconcile_interval_seconds=?, workforce_audit_interval_seconds=?,
                    auto_offboard_enabled=?, updated_by=?, updated_at=?, revision=revision+1
                WHERE policy_id='default'
                """,
                (
                    int(policy.automatic_hiring_enabled),
                    policy.recruitment_mode,
                    max(1, min(int(policy.max_workers), 1000)),
                    self._json(models),
                    policy.default_model,
                    str(policy.default_account_profile or "auto"),
                    float(policy.minimum_evaluation_score),
                    max(0.0, float(policy.monthly_budget_limit)),
                    max(0.0, float(policy.per_worker_budget_limit)),
                    self._json(policy.quarantine_rules),
                    max(60, int(policy.stale_heartbeat_seconds)),
                    max(60, int(policy.no_heartbeat_grace_seconds)),
                    max(60, int(policy.lease_reconcile_interval_seconds)),
                    max(300, int(policy.workforce_audit_interval_seconds)),
                    int(policy.auto_offboard_enabled),
                    actor_name,
                    now,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM hive_recruitment_policies WHERE policy_id='default'"
            ).fetchone()
            return self._policy_from_row(updated)

    def open_requisition(
        self,
        *,
        plan_id: str,
        objective: str,
        requested_competencies: list[str],
        requested_writable_domains: list[str],
        urgency: str,
    ) -> str:
        request = {
            "plan_id": str(plan_id).strip(),
            "objective": str(objective or "").strip(),
            "requested_competencies": sorted(
                {str(item).strip().lower() for item in requested_competencies if str(item).strip()}
            ),
            "requested_writable_domains": sorted(
                {str(item).strip().lower() for item in requested_writable_domains if str(item).strip()}
            ),
            "urgency": str(urgency or "NORMAL").strip().upper(),
        }
        digest = self._fingerprint(request)
        requisition_id = f"req-{digest[:24]}"
        now = self._now_ms()
        with self._write() as conn:
            existing = conn.execute(
                "SELECT request_sha256 FROM hive_requisitions WHERE requisition_id=?",
                (requisition_id,),
            ).fetchone()
            if existing:
                if str(existing["request_sha256"]) != digest:
                    raise HiveIdempotencyConflict(
                        "Requisition identifier was reused with different content."
                    )
                return requisition_id
            conn.execute(
                """
                INSERT INTO hive_requisitions(
                    requisition_id, plan_id, objective,
                    requested_competencies_json, requested_writable_domains_json,
                    urgency, status, matching_attempts, candidate_count,
                    appointed_worker_id, appointment_outcome,
                    idempotency_key, request_sha256, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, 'OPEN', 0, 0, '', 'PENDING', ?, ?, ?, ?)
                """,
                (
                    requisition_id,
                    request["plan_id"],
                    request["objective"],
                    self._json(request["requested_competencies"]),
                    self._json(request["requested_writable_domains"]),
                    request["urgency"],
                    f"requisition:{digest}",
                    digest,
                    now,
                    now,
                ),
            )
        return requisition_id

    def record_recruitment_result(
        self,
        *,
        requisition_id: str,
        result: GapRecruitmentSnapshot,
        requested_authority: str,
    ) -> None:
        policy = self.get_policy()
        workers = {
            item.worker_id: item for item in self.workforce.list_workers(limit=1000).workers
        }
        requested_competencies = set(result.requested_competencies)
        requested_domains = set(result.requested_writable_domains)
        worker_ids = list(dict.fromkeys(result.recruited_worker_ids + result.appointed_worker_ids))
        now = self._now_ms()
        with self._write() as conn:
            row = conn.execute(
                "SELECT * FROM hive_requisitions WHERE requisition_id=?",
                (requisition_id,),
            ).fetchone()
            if not row:
                raise ValueError(f"Requisition not found: {requisition_id}")
            for worker_id in worker_ids:
                worker = workers.get(worker_id)
                if not worker:
                    continue
                competency_score = (
                    1.0
                    if not requested_competencies
                    else len(requested_competencies.intersection(worker.competencies))
                    / len(requested_competencies)
                )
                domain_score = (
                    1.0
                    if not requested_domains
                    else len(requested_domains.intersection(worker.writable_domains))
                    / len(requested_domains)
                )
                authority_score = 1.0 if worker.authority_ceiling >= requested_authority else 0.5
                total_score = round(
                    competency_score * 0.5 + domain_score * 0.35 + authority_score * 0.15,
                    4,
                )
                outcome = (
                    "APPOINTED"
                    if worker_id in result.appointed_worker_ids
                    else "REQUISITIONED"
                )
                evaluation_id = f"eval-{self._fingerprint({'r': requisition_id, 'w': worker_id})[:24]}"
                conn.execute(
                    """
                    INSERT OR REPLACE INTO hive_candidate_evaluations(
                        evaluation_id, requisition_id, worker_id, candidate_kind,
                        model_id, account_profile, competency_score, domain_score,
                        authority_score, total_score, threshold, outcome,
                        evidence_json, created_at
                    ) VALUES (?, ?, ?, 'AUTO_GENERATED', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        evaluation_id,
                        requisition_id,
                        worker_id,
                        policy.default_model,
                        policy.default_account_profile,
                        competency_score,
                        domain_score,
                        authority_score,
                        total_score,
                        policy.minimum_evaluation_score,
                        outcome,
                        self._json(
                            {
                                "recruitment_status": result.status,
                                "authorization_basis": result.authorization.get(
                                    "authorization_basis", ""
                                ),
                                "policy_revision": policy.revision,
                            }
                        ),
                        now,
                    ),
                )
                quarantine = (
                    "QUARANTINED"
                    if policy.quarantine_rules.get("new_workers", True)
                    and outcome != "APPOINTED"
                    else "CLEARED"
                )
                conn.execute(
                    """
                    INSERT INTO hive_worker_profiles(
                        worker_id, model_id, account_profile, runtime_state,
                        runtime_instance_id, quarantine_state, metadata_json,
                        last_runtime_event_at, updated_at
                    ) VALUES (?, ?, ?, 'UNASSIGNED', '', ?, ?, 0, ?)
                    ON CONFLICT(worker_id) DO UPDATE SET
                        model_id=excluded.model_id,
                        account_profile=excluded.account_profile,
                        quarantine_state=excluded.quarantine_state,
                        metadata_json=excluded.metadata_json,
                        updated_at=excluded.updated_at
                    """,
                    (
                        worker_id,
                        policy.default_model,
                        policy.default_account_profile,
                        quarantine,
                        self._json({"originating_requisition_id": requisition_id}),
                        now,
                    ),
                )
            appointed = result.appointed_worker_ids[0] if result.appointed_worker_ids else ""
            status = (
                "APPOINTED"
                if appointed
                else "FAILED"
                if not result.ok
                else "REQUISITIONED"
            )
            conn.execute(
                """
                UPDATE hive_requisitions SET
                    status=?, matching_attempts=matching_attempts+1,
                    candidate_count=(SELECT COUNT(*) FROM hive_candidate_evaluations WHERE requisition_id=?),
                    appointed_worker_id=?, appointment_outcome=?, updated_at=?
                WHERE requisition_id=?
                """,
                (
                    status,
                    requisition_id,
                    appointed,
                    result.status if result.status else status,
                    now,
                    requisition_id,
                ),
            )

    def process_recruitment_queue(
        self,
        *,
        actor: str,
        governance_authorization: dict[str, Any] | None = None,
        human_approval: bool = False,
        limit: int = 10,
    ) -> dict[str, Any]:
        policy = self.get_policy()
        result: dict[str, Any] = {
            "enabled": policy.automatic_hiring_enabled,
            "policy_revision": policy.revision,
            "inspected": 0,
            "appointed": [],
            "failed": [],
            "deferred": [],
        }
        if not policy.automatic_hiring_enabled:
            return result
        try:
            authorization = require_governed_authorization(
                governance_authorization,
                required_authority="A2",
                legacy_human_approval=human_approval,
            )
        except GovernedAuthorizationError as exc:
            raise HiveTransitionError(str(exc)) from exc
        with self._connect() as conn:
            rows = conn.execute(
                """
                SELECT r.requisition_id, r.plan_id, r.status,
                       e.evaluation_id, e.worker_id, e.model_id,
                       e.total_score, e.threshold, e.outcome
                FROM hive_requisitions r
                JOIN hive_candidate_evaluations e
                  ON e.requisition_id=r.requisition_id
                WHERE r.status IN ('OPEN','REQUISITIONED','MATCHING')
                  AND e.outcome IN ('PENDING','REQUISITIONED')
                ORDER BY r.urgency DESC, r.created_at, e.total_score DESC
                LIMIT ?
                """,
                (max(1, min(int(limit), 100)),),
            ).fetchall()
        for row in rows:
            result["inspected"] += 1
            worker_id = str(row["worker_id"])
            requisition_id = str(row["requisition_id"])
            if str(row["model_id"]) not in policy.allowed_models:
                result["failed"].append(
                    {"requisition_id": requisition_id, "worker_id": worker_id, "reason": "MODEL_NOT_ALLOWED"}
                )
                self._set_evaluation_outcome(
                    requisition_id=requisition_id,
                    evaluation_id=str(row["evaluation_id"]),
                    worker_id=worker_id,
                    outcome="FAILED",
                    appointment_outcome="MODEL_NOT_ALLOWED",
                    quarantine_state="QUARANTINED",
                )
                continue
            if float(row["total_score"]) < max(
                float(row["threshold"]), policy.minimum_evaluation_score
            ):
                result["failed"].append(
                    {"requisition_id": requisition_id, "worker_id": worker_id, "reason": "BELOW_THRESHOLD"}
                )
                self._set_evaluation_outcome(
                    requisition_id=requisition_id,
                    evaluation_id=str(row["evaluation_id"]),
                    worker_id=worker_id,
                    outcome="FAILED",
                    appointment_outcome="BELOW_THRESHOLD",
                    quarantine_state="QUARANTINED",
                )
                continue
            workers = {
                item.worker_id: item
                for item in self.workforce.list_workers(limit=1000).workers
            }
            worker = workers.get(worker_id)
            if not worker:
                result["failed"].append(
                    {"requisition_id": requisition_id, "worker_id": worker_id, "reason": "WORKER_MISSING"}
                )
                continue
            try:
                if worker.stage == WorkerStage.REQUISITIONED.value:
                    worker = self.workforce.transition_worker(
                        worker_id=worker_id,
                        target_stage=WorkerStage.SHADOW.value,
                        actor=actor,
                        reason=f"Recruitment service shadow evaluation for {requisition_id}.",
                        idempotency_key=f"governor-shadow:{requisition_id}:{worker_id}",
                    ).workers[0]
                if worker.stage == WorkerStage.SHADOW.value:
                    worker = self.workforce.transition_worker(
                        worker_id=worker_id,
                        target_stage=WorkerStage.PROBATION.value,
                        actor=actor,
                        reason=f"Recruitment service probation for {requisition_id}.",
                        governance_authorization=governance_authorization,
                        human_approval=human_approval,
                        expected_revision=worker.revision,
                        idempotency_key=f"governor-probation:{requisition_id}:{worker_id}",
                    ).workers[0]
                if worker.stage == WorkerStage.PROBATION.value:
                    worker = self.workforce.transition_worker(
                        worker_id=worker_id,
                        target_stage=WorkerStage.APPOINTED.value,
                        actor=actor,
                        reason=f"Recruitment service appointment for {requisition_id}.",
                        governance_authorization=governance_authorization,
                        human_approval=human_approval,
                        expected_revision=worker.revision,
                        idempotency_key=f"governor-appoint:{requisition_id}:{worker_id}",
                    ).workers[0]
            except Exception as exc:
                result["deferred"].append(
                    {"requisition_id": requisition_id, "worker_id": worker_id, "reason": str(exc)}
                )
                continue
            if worker.stage == WorkerStage.APPOINTED.value:
                result["appointed"].append(
                    {"requisition_id": requisition_id, "worker_id": worker_id}
                )
                self._set_evaluation_outcome(
                    requisition_id=requisition_id,
                    evaluation_id=str(row["evaluation_id"]),
                    worker_id=worker_id,
                    outcome="APPOINTED",
                    appointment_outcome="APPOINTED",
                    quarantine_state="CLEARED",
                    authorization=authorization,
                )
        return result

    def _set_evaluation_outcome(
        self,
        *,
        requisition_id: str,
        evaluation_id: str,
        worker_id: str,
        outcome: str,
        appointment_outcome: str,
        quarantine_state: str,
        authorization: dict[str, Any] | None = None,
    ) -> None:
        now = self._now_ms()
        with self._write() as conn:
            conn.execute(
                "UPDATE hive_candidate_evaluations SET outcome=? WHERE evaluation_id=?",
                (outcome, evaluation_id),
            )
            conn.execute(
                """
                UPDATE hive_requisitions SET status=?, appointed_worker_id=?,
                    appointment_outcome=?, updated_at=? WHERE requisition_id=?
                """,
                (
                    "APPOINTED" if outcome == "APPOINTED" else "FAILED",
                    worker_id if outcome == "APPOINTED" else "",
                    appointment_outcome,
                    now,
                    requisition_id,
                ),
            )
            conn.execute(
                """
                UPDATE hive_worker_profiles SET quarantine_state=?,
                    metadata_json=?, updated_at=? WHERE worker_id=?
                """,
                (
                    quarantine_state,
                    self._json({"last_recruitment_authorization": authorization or {}}),
                    now,
                    worker_id,
                ),
            )

    def set_worker_runtime_profile(
        self,
        *,
        worker_id: str,
        model_id: str = "",
        account_profile: str = "",
        runtime_state: str = "",
        runtime_instance_id: str = "",
        quarantine_state: str = "",
        metadata: dict[str, Any] | None = None,
    ) -> None:
        now = self._now_ms()
        with self._write() as conn:
            conn.execute(
                """
                INSERT INTO hive_worker_profiles(
                    worker_id, model_id, account_profile, runtime_state,
                    runtime_instance_id, quarantine_state, metadata_json,
                    last_runtime_event_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(worker_id) DO UPDATE SET
                    model_id=CASE WHEN excluded.model_id='' THEN hive_worker_profiles.model_id ELSE excluded.model_id END,
                    account_profile=CASE WHEN excluded.account_profile='' THEN hive_worker_profiles.account_profile ELSE excluded.account_profile END,
                    runtime_state=CASE WHEN excluded.runtime_state='' THEN hive_worker_profiles.runtime_state ELSE excluded.runtime_state END,
                    runtime_instance_id=CASE WHEN excluded.runtime_instance_id='' THEN hive_worker_profiles.runtime_instance_id ELSE excluded.runtime_instance_id END,
                    quarantine_state=CASE WHEN excluded.quarantine_state='' THEN hive_worker_profiles.quarantine_state ELSE excluded.quarantine_state END,
                    metadata_json=CASE WHEN excluded.metadata_json='{}' THEN hive_worker_profiles.metadata_json ELSE excluded.metadata_json END,
                    last_runtime_event_at=CASE WHEN excluded.runtime_state='' THEN hive_worker_profiles.last_runtime_event_at ELSE excluded.last_runtime_event_at END,
                    updated_at=excluded.updated_at
                """,
                (
                    worker_id,
                    model_id,
                    account_profile,
                    runtime_state,
                    runtime_instance_id,
                    quarantine_state,
                    self._json(metadata or {}),
                    now if runtime_state else 0,
                    now,
                ),
            )

    def _registry(self, conn: sqlite3.Connection, limit: int) -> list[WorkerRegistryItem]:
        workers = conn.execute(
            "SELECT * FROM hive_workers ORDER BY updated_at DESC, worker_id LIMIT ?",
            (max(1, min(int(limit), 1000)),),
        ).fetchall()
        profiles = {
            str(row["worker_id"]): row
            for row in conn.execute("SELECT * FROM hive_worker_profiles").fetchall()
        }
        lease_rows = conn.execute(
            """
            SELECT l.*, m.objective, d.status AS dispatch_status
            FROM hive_worker_leases l
            LEFT JOIN hive_invocation_materializations m ON m.plan_id=l.plan_id
            LEFT JOIN hive_dispatch_receipts d
              ON d.plan_id=l.plan_id AND d.work_unit_id=l.work_unit_id
            WHERE l.status='ACTIVE'
            ORDER BY l.updated_at DESC, l.lease_id DESC
            """
        ).fetchall()
        active_by_worker: dict[str, sqlite3.Row] = {}
        for row in lease_rows:
            active_by_worker.setdefault(str(row["worker_id"]), row)
        now = self._now_ms()
        items: list[WorkerRegistryItem] = []
        for row in workers:
            worker_id = str(row["worker_id"])
            profile = profiles.get(worker_id)
            lease = active_by_worker.get(worker_id)
            runtime_state = str(profile["runtime_state"]) if profile else "UNASSIGNED"
            assignment: dict[str, Any] | None = None
            if lease:
                liveness, execution_live = self.lifecycle.lease_liveness(
                    status=str(lease["status"]),
                    expires_at=int(lease["expires_at"] or 0),
                    last_heartbeat_at=int(lease["last_heartbeat_at"] or 0),
                    heartbeat_status=str(lease["heartbeat_status"]),
                    now_ms=now,
                    stale_after_seconds=self.get_policy().stale_heartbeat_seconds,
                )
                runtime_state = liveness
                assignment = {
                    "plan_id": str(lease["plan_id"]),
                    "mission_id": str(lease["mission_id"]),
                    "work_unit_id": str(lease["work_unit_id"]),
                    "objective": str(lease["objective"] or ""),
                    "dispatch_status": str(lease["dispatch_status"] or ""),
                    "execution_live": execution_live,
                }
            items.append(
                WorkerRegistryItem(
                    worker_id=worker_id,
                    display_name=str(row["display_name"]),
                    worker_class=str(row["worker_class"]),
                    role=str(row["role"]),
                    competencies=json.loads(str(row["competencies_json"])),
                    writable_domains=json.loads(str(row["writable_domains_json"])),
                    model_id=str(profile["model_id"]) if profile else "",
                    account_profile=str(profile["account_profile"]) if profile else "",
                    appointment_state=str(row["stage"]),
                    runtime_state=runtime_state,
                    quarantine_state=str(profile["quarantine_state"]) if profile else "NONE",
                    current_assignment=assignment,
                    authority_ceiling=str(row["authority_ceiling"]),
                    accountable_owner=str(row["accountable_owner"]),
                    created_at=int(row["created_at"]),
                    updated_at=int(row["updated_at"]),
                )
            )
        return items

    def _requisitions(self, conn: sqlite3.Connection, limit: int) -> list[RequisitionQueueItem]:
        rows = conn.execute(
            "SELECT * FROM hive_requisitions ORDER BY updated_at DESC LIMIT ?",
            (max(1, min(int(limit), 1000)),),
        ).fetchall()
        items: list[RequisitionQueueItem] = []
        for row in rows:
            evaluations = [
                CandidateEvaluationItem(
                    evaluation_id=str(item["evaluation_id"]),
                    requisition_id=str(item["requisition_id"]),
                    worker_id=str(item["worker_id"]),
                    candidate_kind=str(item["candidate_kind"]),
                    model_id=str(item["model_id"]),
                    account_profile=str(item["account_profile"]),
                    competency_score=float(item["competency_score"]),
                    domain_score=float(item["domain_score"]),
                    authority_score=float(item["authority_score"]),
                    total_score=float(item["total_score"]),
                    threshold=float(item["threshold"]),
                    outcome=str(item["outcome"]),
                    evidence=json.loads(str(item["evidence_json"])),
                    created_at=int(item["created_at"]),
                )
                for item in conn.execute(
                    """
                    SELECT * FROM hive_candidate_evaluations
                    WHERE requisition_id=? ORDER BY created_at DESC
                    """,
                    (str(row["requisition_id"]),),
                ).fetchall()
            ]
            items.append(
                RequisitionQueueItem(
                    requisition_id=str(row["requisition_id"]),
                    plan_id=str(row["plan_id"]),
                    objective=str(row["objective"]),
                    requested_competencies=json.loads(
                        str(row["requested_competencies_json"])
                    ),
                    requested_writable_domains=json.loads(
                        str(row["requested_writable_domains_json"])
                    ),
                    urgency=str(row["urgency"]),
                    status=str(row["status"]),
                    matching_attempts=int(row["matching_attempts"]),
                    candidate_count=int(row["candidate_count"]),
                    evaluations=evaluations,
                    appointed_worker_id=str(row["appointed_worker_id"]),
                    appointment_outcome=str(row["appointment_outcome"]),
                    created_at=int(row["created_at"]),
                    updated_at=int(row["updated_at"]),
                )
            )
        return items

    def _leases(self, conn: sqlite3.Connection, limit: int) -> list[LeaseMonitorItem]:
        rows = conn.execute(
            "SELECT * FROM hive_worker_leases ORDER BY updated_at DESC LIMIT ?",
            (max(1, min(int(limit), 2000)),),
        ).fetchall()
        policy = self.get_policy()
        now = self._now_ms()
        items: list[LeaseMonitorItem] = []
        for row in rows:
            last_heartbeat = int(row["last_heartbeat_at"] or 0)
            liveness, execution_live = self.lifecycle.lease_liveness(
                status=str(row["status"]),
                expires_at=int(row["expires_at"] or 0),
                last_heartbeat_at=last_heartbeat,
                heartbeat_status=str(row["heartbeat_status"]),
                now_ms=now,
                stale_after_seconds=policy.stale_heartbeat_seconds,
            )
            stale = liveness in {"STALE", "OFFLINE", "EXPIRED"}
            cleanup = (
                "EXPIRE"
                if liveness == "EXPIRED"
                else "REVOKE"
                if liveness in {"STALE", "OFFLINE"}
                else "NONE"
            )
            items.append(
                LeaseMonitorItem(
                    lease_id=str(row["lease_id"]),
                    plan_id=str(row["plan_id"]),
                    mission_id=str(row["mission_id"]),
                    work_unit_id=str(row["work_unit_id"]),
                    worker_id=str(row["worker_id"]),
                    status=str(row["status"]),
                    liveness_status=liveness,
                    execution_live=execution_live,
                    issued_at=int(row["issued_at"] or row["created_at"]),
                    expires_at=int(row["expires_at"] or 0),
                    last_renewal_at=int(row["updated_at"]),
                    last_heartbeat_at=last_heartbeat,
                    heartbeat_age_seconds=(
                        max(0, (now - last_heartbeat) // 1000) if last_heartbeat else None
                    ),
                    stale=stale,
                    automatic_cleanup_action=cleanup,
                    release_reason=str(row["release_reason"]),
                    renewal_count=int(row["renewal_count"] or 0),
                )
            )
        return items

    def _audit_history(self, conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
        return [
            {
                "audit_id": str(row["audit_id"]),
                "actor": str(row["actor"]),
                "dry_run": bool(row["dry_run"]),
                "findings": json.loads(str(row["findings_json"])),
                "actions": json.loads(str(row["actions_json"])),
                "authorization": json.loads(str(row["authorization_json"])),
                "created_at": int(row["created_at"]),
            }
            for row in conn.execute(
                "SELECT * FROM hive_workforce_audits ORDER BY created_at DESC LIMIT ?",
                (max(1, min(int(limit), 500)),),
            ).fetchall()
        ]

    def _offboarding_history(self, conn: sqlite3.Connection, limit: int) -> list[dict[str, Any]]:
        rows = conn.execute(
            """
            SELECT e.*, w.display_name, w.role,
                   (SELECT COUNT(*) FROM hive_worker_leases l WHERE l.worker_id=e.worker_id) AS retained_lease_count,
                   (SELECT COUNT(*) FROM hive_dispatch_receipts d WHERE d.worker_id=e.worker_id) AS retained_receipt_count
            FROM hive_worker_events e
            JOIN hive_workers w ON w.worker_id=e.worker_id
            WHERE e.to_stage IN ('OFFBOARDED','SUSPENDED','REJECTED')
            ORDER BY e.created_at DESC LIMIT ?
            """,
            (max(1, min(int(limit), 500)),),
        ).fetchall()
        return [
            {
                "event_id": str(row["event_id"]),
                "worker_id": str(row["worker_id"]),
                "display_name": str(row["display_name"]),
                "role": str(row["role"]),
                "from_stage": str(row["from_stage"]),
                "to_stage": str(row["to_stage"]),
                "reason": str(row["reason"]),
                "actor": str(row["actor"]),
                "retained_artifacts": {
                    "leases": int(row["retained_lease_count"] or 0),
                    "dispatch_receipts": int(row["retained_receipt_count"] or 0),
                },
                "created_at": int(row["created_at"]),
            }
            for row in rows
        ]

    def _metrics(self, conn: sqlite3.Connection) -> WorkforceMetrics:
        now = self._now_ms()
        filled = conn.execute(
            """
            SELECT AVG(updated_at-created_at) AS average_ms
            FROM hive_requisitions WHERE status='APPOINTED'
            """
        ).fetchone()
        blocked = conn.execute(
            """
            SELECT COUNT(*) AS count FROM hive_invocation_materializations
            WHERE status IN ('BLOCKED','RECRUITING')
              AND (missing_competencies_json != '[]' OR missing_writable_domains_json != '[]')
            """
        ).fetchone()
        stale_removed = conn.execute(
            """
            SELECT COUNT(*) AS count FROM hive_lease_events
            WHERE event_type IN ('LEASE_AUTO_EXPIRED','LEASE_AUTO_REVOKED')
            """
        ).fetchone()
        appointments = conn.execute(
            "SELECT COUNT(*) AS count FROM hive_requisitions WHERE status='APPOINTED'"
        ).fetchone()
        failed = conn.execute(
            "SELECT COUNT(*) AS count FROM hive_candidate_evaluations WHERE outcome IN ('FAILED','REJECTED')"
        ).fetchone()
        appointed = conn.execute(
            "SELECT COUNT(*) AS count FROM hive_workers WHERE stage=?",
            (WorkerStage.APPOINTED.value,),
        ).fetchone()
        active = conn.execute(
            "SELECT COUNT(DISTINCT worker_id) AS count FROM hive_worker_leases WHERE status='ACTIVE'"
        ).fetchone()
        open_req = conn.execute(
            "SELECT COUNT(*) AS count FROM hive_requisitions WHERE status IN ('OPEN','MATCHING','REQUISITIONED')"
        ).fetchone()
        appointed_count = int(appointed["count"] or 0)
        active_count = int(active["count"] or 0)
        average_ms = filled["average_ms"] if filled else None
        return WorkforceMetrics(
            generated_at=now,
            time_to_fill_average_seconds=(
                round(float(average_ms) / 1000, 3) if average_ms is not None else None
            ),
            blocked_plans_competency_gaps=int(blocked["count"] or 0),
            stale_leases_removed=int(stale_removed["count"] or 0),
            successful_appointments=int(appointments["count"] or 0),
            worker_utilization=(
                round(active_count / appointed_count, 4) if appointed_count else 0.0
            ),
            failed_evaluations=int(failed["count"] or 0),
            appointed_workers=appointed_count,
            active_assignments=active_count,
            open_requisitions=int(open_req["count"] or 0),
        )

    def _governor_status(self, conn: sqlite3.Connection) -> dict[str, Any]:
        row = conn.execute(
            """
            SELECT * FROM hive_workforce_governor_runs
            ORDER BY finished_at DESC, run_id DESC LIMIT 1
            """
        ).fetchone()
        if not row:
            return {"status": "NEVER_RUN"}
        return {
            "run_id": str(row["run_id"]),
            "started_at": int(row["started_at"]),
            "finished_at": int(row["finished_at"]),
            "status": str(row["status"]),
            "lease_changes": int(row["lease_changes"]),
            "audit_findings": int(row["audit_findings"]),
            "audit_actions": int(row["audit_actions"]),
            "error": str(row["error"]),
        }

    def overview(self, *, limit: int = 250) -> WorkforceControlPlaneSnapshot:
        with self._connect() as conn:
            return WorkforceControlPlaneSnapshot(
                db_path=str(self.path),
                generated_at=self._now_ms(),
                policy=self.get_policy(),
                registry=self._registry(conn, limit),
                requisitions=self._requisitions(conn, limit),
                leases=self._leases(conn, limit * 2),
                audits=self._audit_history(conn, limit),
                offboarding_history=self._offboarding_history(conn, limit),
                metrics=self._metrics(conn),
                governor_status=self._governor_status(conn),
            )

    def record_governor_run(
        self,
        *,
        started_at: int,
        status: str,
        lease_changes: int,
        audit_findings: int,
        audit_actions: int,
        error: str = "",
    ) -> str:
        run_id = f"governor-{uuid.uuid4()}"
        with self._write() as conn:
            conn.execute(
                """
                INSERT INTO hive_workforce_governor_runs(
                    run_id, started_at, finished_at, status,
                    lease_changes, audit_findings, audit_actions, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    int(started_at),
                    self._now_ms(),
                    str(status),
                    int(lease_changes),
                    int(audit_findings),
                    int(audit_actions),
                    str(error or "")[:4000],
                ),
            )
        return run_id
