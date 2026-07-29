from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel, Field

from app.hive_runtime import (
    HiveIdempotencyConflict,
    HiveNotFoundError,
    HiveTransitionError,
    HiveWorkUnitSpec,
    default_hive_runtime_db_path,
    get_hive_runtime_store,
)
from app.hive_workforce import (
    AUTHORITY_RANK,
    HiveInvocationPlan,
    HiveWorker,
    WorkerClass,
    WorkerStage,
    get_hive_workforce_store,
)


class MaterializationStatus(str, Enum):
    BLOCKED = "BLOCKED"
    AWAITING_APPROVAL = "AWAITING_APPROVAL"
    PREPARING = "PREPARING"
    MATERIALIZED = "MATERIALIZED"
    READY_FOR_FAN_IN = "READY_FOR_FAN_IN"
    CLOSED_WITH_FAILURE = "CLOSED_WITH_FAILURE"
    CANCELLED = "CANCELLED"


class LeaseStatus(str, Enum):
    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"
    REVOKED = "REVOKED"


class DispatchStatus(str, Enum):
    READY = "READY"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


DISPATCH_TRANSITIONS: dict[str, set[str]] = {
    DispatchStatus.READY.value: {
        DispatchStatus.READY.value,
        DispatchStatus.ACKNOWLEDGED.value,
        DispatchStatus.COMPLETED.value,
        DispatchStatus.FAILED.value,
        DispatchStatus.CANCELLED.value,
    },
    DispatchStatus.ACKNOWLEDGED.value: {
        DispatchStatus.ACKNOWLEDGED.value,
        DispatchStatus.COMPLETED.value,
        DispatchStatus.FAILED.value,
        DispatchStatus.CANCELLED.value,
    },
    DispatchStatus.COMPLETED.value: {DispatchStatus.COMPLETED.value},
    DispatchStatus.FAILED.value: {DispatchStatus.FAILED.value},
    DispatchStatus.CANCELLED.value: {DispatchStatus.CANCELLED.value},
}
TERMINAL_DISPATCH = {
    DispatchStatus.COMPLETED.value,
    DispatchStatus.FAILED.value,
    DispatchStatus.CANCELLED.value,
}


class HiveWorkerLease(BaseModel):
    lease_id: str
    plan_id: str
    mission_id: str
    work_unit_id: str
    worker_id: str
    status: str
    authority_ceiling: str
    writable_domains: list[str] = Field(default_factory=list)
    source_boundary: str = ""
    release_reason: str = ""
    created_at: int
    updated_at: int
    revision: int


class HiveDispatchReceipt(BaseModel):
    receipt_id: str
    plan_id: str
    mission_id: str
    work_unit_id: str
    worker_id: str
    conversation_id: str
    status: str
    actor: str
    evidence: dict[str, Any] = Field(default_factory=dict)
    created_at: int
    updated_at: int
    revision: int


class HiveMaterializationSnapshot(BaseModel):
    ok: bool = True
    found: bool = True
    db_path: str = ""
    error: str = ""
    plan_id: str = ""
    objective: str = ""
    mode: str = ""
    status: str = ""
    requested_authority: str = "A0"
    required_competencies: list[str] = Field(default_factory=list)
    writable_domains: list[str] = Field(default_factory=list)
    reasons: list[str] = Field(default_factory=list)
    selected_worker_ids: list[str] = Field(default_factory=list)
    missing_competencies: list[str] = Field(default_factory=list)
    missing_writable_domains: list[str] = Field(default_factory=list)
    parent_context_id: str = ""
    lifecycle_stage: str = ""
    mission_id: str = ""
    human_gate_required: bool = False
    human_approval: bool = False
    approved_by: str = ""
    work_unit_ids: list[str] = Field(default_factory=list)
    leases: list[HiveWorkerLease] = Field(default_factory=list)
    dispatch_receipts: list[HiveDispatchReceipt] = Field(default_factory=list)
    created_at: int = 0
    updated_at: int = 0
    revision: int = 0


class HiveMaterializationStore:
    """Durable plan approval, mission materialization, leases, and receipts."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self._schema_lock = threading.RLock()
        self.workforce = get_hive_workforce_store(self.path)
        self.runtime = get_hive_runtime_store(self.path)
        self._ensure_schema()

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )

    @classmethod
    def _fingerprint(cls, value: Any) -> str:
        return hashlib.sha256(cls._json(value).encode("utf-8")).hexdigest()

    @staticmethod
    def _required(value: str, field_name: str) -> str:
        clean = str(value or "").strip()
        if not clean:
            raise ValueError(f"{field_name} is required")
        return clean

    @staticmethod
    def _normalized_list(values: list[str] | None) -> list[str]:
        return sorted(
            {
                str(value).strip().lower()
                for value in (values or [])
                if str(value).strip()
            }
        )

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
        with self._schema_lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS hive_invocation_materializations (
                        plan_id TEXT PRIMARY KEY,
                        objective TEXT NOT NULL,
                        mode TEXT NOT NULL,
                        status TEXT NOT NULL,
                        requested_authority TEXT NOT NULL,
                        required_competencies_json TEXT NOT NULL DEFAULT '[]',
                        writable_domains_json TEXT NOT NULL DEFAULT '[]',
                        reasons_json TEXT NOT NULL DEFAULT '[]',
                        selected_worker_ids_json TEXT NOT NULL DEFAULT '[]',
                        missing_competencies_json TEXT NOT NULL DEFAULT '[]',
                        missing_writable_domains_json TEXT NOT NULL DEFAULT '[]',
                        plan_json TEXT NOT NULL,
                        request_json TEXT NOT NULL,
                        parent_context_id TEXT NOT NULL DEFAULT '',
                        lifecycle_stage TEXT NOT NULL,
                        mission_id TEXT NOT NULL DEFAULT '',
                        human_gate_required INTEGER NOT NULL DEFAULT 0,
                        human_approval INTEGER NOT NULL DEFAULT 0,
                        approved_by TEXT NOT NULL DEFAULT '',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_materialization_events (
                        event_id TEXT PRIMARY KEY,
                        plan_id TEXT NOT NULL REFERENCES
                            hive_invocation_materializations(plan_id)
                            ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_worker_leases (
                        lease_id TEXT PRIMARY KEY,
                        plan_id TEXT NOT NULL REFERENCES
                            hive_invocation_materializations(plan_id)
                            ON DELETE CASCADE,
                        mission_id TEXT NOT NULL,
                        work_unit_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL REFERENCES hive_workers(worker_id),
                        status TEXT NOT NULL,
                        authority_ceiling TEXT NOT NULL,
                        writable_domains_json TEXT NOT NULL DEFAULT '[]',
                        source_boundary TEXT NOT NULL DEFAULT '',
                        release_reason TEXT NOT NULL DEFAULT '',
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL,
                        UNIQUE(plan_id, work_unit_id)
                    );
                    CREATE TABLE IF NOT EXISTS hive_dispatch_receipts (
                        receipt_id TEXT PRIMARY KEY,
                        plan_id TEXT NOT NULL REFERENCES
                            hive_invocation_materializations(plan_id)
                            ON DELETE CASCADE,
                        mission_id TEXT NOT NULL,
                        work_unit_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL REFERENCES hive_workers(worker_id),
                        conversation_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        evidence_json TEXT NOT NULL DEFAULT '{}',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL,
                        UNIQUE(plan_id, work_unit_id)
                    );
                    CREATE INDEX IF NOT EXISTS idx_materializations_status_updated
                        ON hive_invocation_materializations(
                            status, updated_at DESC
                        );
                    CREATE INDEX IF NOT EXISTS idx_leases_mission_status
                        ON hive_worker_leases(
                            mission_id, status, updated_at DESC
                        );
                    CREATE INDEX IF NOT EXISTS idx_dispatch_mission_status
                        ON hive_dispatch_receipts(
                            mission_id, status, updated_at DESC
                        );
                    """
                )

    @staticmethod
    def _bounded_authority(worker: str, requested: str) -> str:
        rank = min(
            AUTHORITY_RANK.get(worker, 0),
            AUTHORITY_RANK.get(requested, 0),
        )
        return f"A{rank}"

    @staticmethod
    def _lease_from_row(row: sqlite3.Row) -> HiveWorkerLease:
        return HiveWorkerLease(
            lease_id=str(row["lease_id"]),
            plan_id=str(row["plan_id"]),
            mission_id=str(row["mission_id"]),
            work_unit_id=str(row["work_unit_id"]),
            worker_id=str(row["worker_id"]),
            status=str(row["status"]),
            authority_ceiling=str(row["authority_ceiling"]),
            writable_domains=json.loads(str(row["writable_domains_json"])),
            source_boundary=str(row["source_boundary"]),
            release_reason=str(row["release_reason"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    @staticmethod
    def _receipt_from_row(row: sqlite3.Row) -> HiveDispatchReceipt:
        return HiveDispatchReceipt(
            receipt_id=str(row["receipt_id"]),
            plan_id=str(row["plan_id"]),
            mission_id=str(row["mission_id"]),
            work_unit_id=str(row["work_unit_id"]),
            worker_id=str(row["worker_id"]),
            conversation_id=str(row["conversation_id"]),
            status=str(row["status"]),
            actor=str(row["actor"]),
            evidence=json.loads(str(row["evidence_json"])),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    def _snapshot(
        self,
        conn: sqlite3.Connection,
        plan_id: str,
    ) -> HiveMaterializationSnapshot:
        row = conn.execute(
            "SELECT * FROM hive_invocation_materializations WHERE plan_id = ?",
            (plan_id,),
        ).fetchone()
        if not row:
            return HiveMaterializationSnapshot(
                ok=False,
                found=False,
                db_path=str(self.path),
                plan_id=plan_id,
            )
        lease_rows = conn.execute(
            """
            SELECT * FROM hive_worker_leases
            WHERE plan_id = ? ORDER BY created_at, lease_id
            """,
            (plan_id,),
        ).fetchall()
        receipt_rows = conn.execute(
            """
            SELECT * FROM hive_dispatch_receipts
            WHERE plan_id = ? ORDER BY created_at, receipt_id
            """,
            (plan_id,),
        ).fetchall()
        receipts = [self._receipt_from_row(item) for item in receipt_rows]
        return HiveMaterializationSnapshot(
            db_path=str(self.path),
            plan_id=str(row["plan_id"]),
            objective=str(row["objective"]),
            mode=str(row["mode"]),
            status=str(row["status"]),
            requested_authority=str(row["requested_authority"]),
            required_competencies=json.loads(str(row["required_competencies_json"])),
            writable_domains=json.loads(str(row["writable_domains_json"])),
            reasons=json.loads(str(row["reasons_json"])),
            selected_worker_ids=json.loads(str(row["selected_worker_ids_json"])),
            missing_competencies=json.loads(str(row["missing_competencies_json"])),
            missing_writable_domains=json.loads(
                str(row["missing_writable_domains_json"])
            ),
            parent_context_id=str(row["parent_context_id"]),
            lifecycle_stage=str(row["lifecycle_stage"]),
            mission_id=str(row["mission_id"]),
            human_gate_required=bool(row["human_gate_required"]),
            human_approval=bool(row["human_approval"]),
            approved_by=str(row["approved_by"]),
            work_unit_ids=[item.work_unit_id for item in receipts],
            leases=[self._lease_from_row(item) for item in lease_rows],
            dispatch_receipts=receipts,
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    def _event(
        self,
        conn: sqlite3.Connection,
        *,
        plan_id: str,
        event_type: str,
        actor: str,
        payload: dict[str, Any],
        idempotency_key: str | None = None,
        fingerprint: str = "",
        created_at: int | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO hive_materialization_events(
                event_id, plan_id, event_type, actor, payload_json,
                idempotency_key, request_sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                plan_id,
                event_type,
                actor,
                self._json(payload),
                idempotency_key,
                fingerprint,
                created_at or self._now_ms(),
            ),
        )

    def _set_status(
        self,
        conn: sqlite3.Connection,
        plan_id: str,
        status: str,
        *,
        approved_by: str | None = None,
        human_approval: bool | None = None,
    ) -> None:
        fields = ["status = ?", "updated_at = ?", "revision = revision + 1"]
        params: list[Any] = [status, self._now_ms()]
        if approved_by is not None:
            fields.append("approved_by = ?")
            params.append(approved_by)
        if human_approval is not None:
            fields.append("human_approval = ?")
            params.append(int(human_approval))
        params.append(plan_id)
        conn.execute(
            f"UPDATE hive_invocation_materializations SET {', '.join(fields)} WHERE plan_id = ?",
            tuple(params),
        )

    def materialize_invocation(
        self,
        *,
        objective: str,
        required_competencies: list[str] | None = None,
        writable_domains: list[str] | None = None,
        dependency_count: int = 0,
        parallelizable_workstreams: int = 1,
        risk_level: str = "low",
        authority_ceiling: str = "A0",
        independent_review_required: bool = False,
        external_effects: bool = False,
        preferred_worker_ids: list[str] | None = None,
        parent_context_id: str = "",
        lifecycle_stage: str = "Build",
        human_approval: bool = False,
        actor: str = "notion2api",
        plan_id: str | None = None,
        mission_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMaterializationSnapshot:
        stable = (
            hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:20]
            if idempotency_key
            else ""
        )
        plan_key = str(
            plan_id or (f"plan-{stable}" if stable else f"plan-{uuid.uuid4()}")
        ).strip()
        mission_key = str(
            mission_id
            or (
                f"hive-materialized-{stable}"
                if stable
                else f"hive-materialized-{uuid.uuid4()}"
            )
        ).strip()
        request = {
            "objective": self._required(objective, "objective"),
            "required_competencies": self._normalized_list(required_competencies),
            "writable_domains": self._normalized_list(writable_domains),
            "dependency_count": max(0, int(dependency_count)),
            "parallelizable_workstreams": max(1, int(parallelizable_workstreams)),
            "risk_level": str(risk_level or "low").strip().lower(),
            "authority_ceiling": str(authority_ceiling or "A0").strip().upper(),
            "independent_review_required": bool(independent_review_required),
            "external_effects": bool(external_effects),
            "preferred_worker_ids": sorted(
                {
                    str(value).strip()
                    for value in (preferred_worker_ids or [])
                    if str(value).strip()
                }
            ),
            "parent_context_id": str(parent_context_id or "").strip(),
            "lifecycle_stage": self._required(lifecycle_stage, "lifecycle_stage"),
            "mission_id": mission_key,
        }
        fingerprint = self._fingerprint(request)
        replay_plan_id = ""
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    """
                    SELECT plan_id, request_sha256
                    FROM hive_invocation_materializations
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Materialization idempotency key was already "
                            "used with different content."
                        )
                    replay_plan_id = str(existing["plan_id"])
            if (
                not replay_plan_id
                and conn.execute(
                    "SELECT 1 FROM hive_invocation_materializations WHERE plan_id = ?",
                    (plan_key,),
                ).fetchone()
            ):
                raise HiveIdempotencyConflict(
                    f"Materialization plan already exists: {plan_key}"
                )
        if replay_plan_id:
            replay = self.get_materialization(plan_id=replay_plan_id)
            if replay.status == MaterializationStatus.PREPARING.value:
                return self._complete_materialization(replay_plan_id, actor=actor)
            return replay

        plan = self.workforce.plan_invocation(
            objective=request["objective"],
            required_competencies=request["required_competencies"],
            writable_domains=request["writable_domains"],
            dependency_count=request["dependency_count"],
            parallelizable_workstreams=request["parallelizable_workstreams"],
            risk_level=request["risk_level"],
            authority_ceiling=request["authority_ceiling"],
            independent_review_required=request["independent_review_required"],
            external_effects=request["external_effects"],
            preferred_worker_ids=request["preferred_worker_ids"],
        )
        selected_ids = [item.worker_id for item in plan.selected_workers]
        blocked = bool(
            plan.missing_competencies
            or plan.missing_writable_domains
            or not selected_ids
            or (plan.mode == "hive" and len(selected_ids) < plan.suggested_lane_count)
        )
        if blocked:
            status = MaterializationStatus.BLOCKED.value
        elif plan.human_gate_required and not human_approval:
            status = MaterializationStatus.AWAITING_APPROVAL.value
        else:
            status = MaterializationStatus.PREPARING.value
        now = self._now_ms()

        with self._write() as conn:
            conn.execute(
                """
                INSERT INTO hive_invocation_materializations(
                    plan_id, objective, mode, status,
                    requested_authority,
                    required_competencies_json,
                    writable_domains_json, reasons_json,
                    selected_worker_ids_json,
                    missing_competencies_json,
                    missing_writable_domains_json,
                    plan_json, request_json, parent_context_id,
                    lifecycle_stage, mission_id,
                    human_gate_required, human_approval, approved_by,
                    idempotency_key, request_sha256,
                    created_at, updated_at, revision
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?, ?, ?, ?, 1
                )
                """,
                (
                    plan_key,
                    request["objective"],
                    plan.mode,
                    status,
                    plan.requested_authority,
                    self._json(request["required_competencies"]),
                    self._json(request["writable_domains"]),
                    self._json(plan.reasons),
                    self._json(selected_ids),
                    self._json(plan.missing_competencies),
                    self._json(plan.missing_writable_domains),
                    self._json(plan.model_dump(mode="json")),
                    self._json(request),
                    request["parent_context_id"],
                    request["lifecycle_stage"],
                    mission_key,
                    int(plan.human_gate_required),
                    int(human_approval),
                    actor if human_approval else "",
                    idempotency_key,
                    fingerprint,
                    now,
                    now,
                ),
            )
            self._event(
                conn,
                plan_id=plan_key,
                event_type="PLAN_PREPARED",
                actor=actor,
                payload={
                    "status": status,
                    "mode": plan.mode,
                    "selected_worker_ids": selected_ids,
                    "missing_competencies": plan.missing_competencies,
                    "missing_writable_domains": plan.missing_writable_domains,
                },
                fingerprint=fingerprint,
                created_at=now,
            )
        if status != MaterializationStatus.PREPARING.value:
            return self.get_materialization(plan_id=plan_key)
        return self._complete_materialization(plan_key, actor=actor)

    def approve_materialization(
        self,
        *,
        plan_id: str,
        actor: str,
        reason: str,
        idempotency_key: str | None = None,
    ) -> HiveMaterializationSnapshot:
        request = {
            "plan_id": self._required(plan_id, "plan_id"),
            "actor": self._required(actor, "actor"),
            "reason": self._required(reason, "reason"),
        }
        fingerprint = self._fingerprint(request)
        replay_plan_id = ""
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    """
                    SELECT plan_id, request_sha256
                    FROM hive_materialization_events
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Approval idempotency key was already used "
                            "with different content."
                        )
                    replay_plan_id = str(existing["plan_id"])
            if replay_plan_id:
                row = None
            else:
                row = conn.execute(
                    """
                SELECT status FROM hive_invocation_materializations
                WHERE plan_id = ?
                """,
                    (plan_id,),
                ).fetchone()
            if not replay_plan_id and not row:
                raise HiveNotFoundError(f"Materialization plan not found: {plan_id}")
            if replay_plan_id:
                status = ""
            else:
                status = str(row["status"])
            if replay_plan_id:
                pass
            elif status == MaterializationStatus.MATERIALIZED.value:
                return self._snapshot(conn, plan_id)
            elif status != MaterializationStatus.AWAITING_APPROVAL.value:
                raise HiveTransitionError(
                    "Only an AWAITING_APPROVAL plan can be approved; "
                    f"current status is {status}."
                )
            if not replay_plan_id:
                self._set_status(
                    conn,
                    plan_id,
                    MaterializationStatus.PREPARING.value,
                    approved_by=actor,
                    human_approval=True,
                )
                self._event(
                    conn,
                    plan_id=plan_id,
                    event_type="PLAN_APPROVED",
                    actor=actor,
                    payload={"reason": reason},
                    idempotency_key=idempotency_key,
                    fingerprint=fingerprint,
                )
        target_plan_id = replay_plan_id or plan_id
        replay = self.get_materialization(plan_id=target_plan_id)
        if replay.status == MaterializationStatus.PREPARING.value:
            return self._complete_materialization(target_plan_id, actor=actor)
        return replay

    def _current_workers(self, worker_ids: list[str]) -> list[HiveWorker]:
        current = {
            item.worker_id: item
            for item in self.workforce.list_workers(limit=1000).workers
        }
        return [current[item] for item in worker_ids if item in current]

    def _complete_materialization(
        self,
        plan_id: str,
        *,
        actor: str,
    ) -> HiveMaterializationSnapshot:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM hive_invocation_materializations
                WHERE plan_id = ?
                """,
                (plan_id,),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Materialization plan not found: {plan_id}")
            if str(row["status"]) == MaterializationStatus.MATERIALIZED.value:
                return self._snapshot(conn, plan_id)
            plan = HiveInvocationPlan.model_validate_json(str(row["plan_json"]))
            request = json.loads(str(row["request_json"]))
            mission_id = str(row["mission_id"])
            selected_ids = json.loads(str(row["selected_worker_ids_json"]))

        workers = self._current_workers(selected_ids)
        reason = ""
        if len(workers) != len(selected_ids):
            reason = "One or more selected workers no longer exist."
        elif any(item.stage != WorkerStage.APPOINTED.value for item in workers):
            reason = "Only APPOINTED workers can receive execution leases."
        elif request["independent_review_required"] and not any(
            item.worker_class == WorkerClass.GOVERNANCE_REVIEWER.value
            for item in workers
        ):
            reason = "Independent review requires an appointed GOVERNANCE_REVIEWER."
        if reason:
            with self._write() as conn:
                self._set_status(
                    conn,
                    plan_id,
                    MaterializationStatus.BLOCKED.value,
                )
                self._event(
                    conn,
                    plan_id=plan_id,
                    event_type="MATERIALIZATION_BLOCKED",
                    actor=actor,
                    payload={"reason": reason},
                )
                return self._snapshot(conn, plan_id)

        worker_map = {item.worker_id: item for item in workers}
        ordered = [worker_map[item] for item in selected_ids]
        lanes: list[dict[str, Any]] = []
        non_review_ids: list[str] = []
        for index, worker in enumerate(ordered, start=1):
            work_id = f"{mission_id}-lane-{index:02d}-{worker.worker_id}"
            conversation_id = f"aigentbee:{plan_id}:{worker.worker_id}"
            requested_domains = set(request["writable_domains"])
            lease_domains = sorted(
                requested_domains.intersection(worker.writable_domains)
            )
            if not requested_domains:
                lease_domains = list(worker.writable_domains)
            lane = {
                "worker": worker,
                "work_unit_id": work_id,
                "conversation_id": conversation_id,
                "lease_domains": lease_domains,
                "authority": self._bounded_authority(
                    worker.authority_ceiling,
                    plan.requested_authority,
                ),
                "dependencies": [],
            }
            lanes.append(lane)
            if worker.worker_class != WorkerClass.GOVERNANCE_REVIEWER.value:
                non_review_ids.append(work_id)

        previous = ""
        remaining_dependencies = int(request["dependency_count"])
        for lane in lanes:
            worker = lane["worker"]
            if worker.worker_class == WorkerClass.GOVERNANCE_REVIEWER.value:
                lane["dependencies"] = list(non_review_ids)
            elif previous and remaining_dependencies > 0:
                lane["dependencies"] = [previous]
                remaining_dependencies -= 1
            previous = lane["work_unit_id"]

        specs = [
            HiveWorkUnitSpec(
                work_unit_id=lane["work_unit_id"],
                title=(f"{lane['worker'].role} â€” {request['objective'][:80]}"),
                role=lane["worker"].role,
                conversation_id=lane["conversation_id"],
                writable_domain=";".join(lane["lease_domains"]),
                dependencies=lane["dependencies"],
                authority_ceiling=lane["authority"],
            )
            for lane in lanes
        ]
        self.runtime.create_mission(
            title=f"Materialized: {request['objective'][:100]}",
            objective=request["objective"],
            lifecycle_stage=request["lifecycle_stage"],
            work_units=specs,
            authority_ceiling=plan.requested_authority,
            parent_context_id=request["parent_context_id"],
            mission_id=mission_id,
            idempotency_key=f"materialize:{plan_id}",
            actor=actor,
        )

        now = self._now_ms()
        with self._write() as conn:
            for lane in lanes:
                worker = lane["worker"]
                conn.execute(
                    """
                    INSERT OR IGNORE INTO hive_worker_leases(
                        lease_id, plan_id, mission_id, work_unit_id,
                        worker_id, status, authority_ceiling,
                        writable_domains_json, source_boundary,
                        release_reason, created_at, updated_at, revision
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, 1)
                    """,
                    (
                        str(uuid.uuid4()),
                        plan_id,
                        mission_id,
                        lane["work_unit_id"],
                        worker.worker_id,
                        LeaseStatus.ACTIVE.value,
                        lane["authority"],
                        self._json(lane["lease_domains"]),
                        worker.source_boundary,
                        now,
                        now,
                    ),
                )
                receipt_request = {
                    "plan_id": plan_id,
                    "work_unit_id": lane["work_unit_id"],
                    "worker_id": worker.worker_id,
                    "status": DispatchStatus.READY.value,
                }
                conn.execute(
                    """
                    INSERT OR IGNORE INTO hive_dispatch_receipts(
                        receipt_id, plan_id, mission_id, work_unit_id,
                        worker_id, conversation_id, status, actor,
                        evidence_json, request_sha256,
                        created_at, updated_at, revision
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?, ?, 1)
                    """,
                    (
                        str(uuid.uuid4()),
                        plan_id,
                        mission_id,
                        lane["work_unit_id"],
                        worker.worker_id,
                        lane["conversation_id"],
                        DispatchStatus.READY.value,
                        actor,
                        self._fingerprint(receipt_request),
                        now,
                        now,
                    ),
                )
            self._set_status(
                conn,
                plan_id,
                MaterializationStatus.MATERIALIZED.value,
            )
            self._event(
                conn,
                plan_id=plan_id,
                event_type="MISSION_MATERIALIZED",
                actor=actor,
                payload={
                    "mission_id": mission_id,
                    "work_unit_ids": [item["work_unit_id"] for item in lanes],
                    "worker_ids": selected_ids,
                },
            )
            return self._snapshot(conn, plan_id)

    def get_materialization(
        self,
        *,
        plan_id: str = "",
        mission_id: str = "",
    ) -> HiveMaterializationSnapshot:
        if not plan_id and not mission_id:
            raise ValueError("plan_id or mission_id is required")
        conn = self._connect()
        try:
            resolved = plan_id
            if not resolved:
                row = conn.execute(
                    """
                    SELECT plan_id FROM hive_invocation_materializations
                    WHERE mission_id = ?
                    ORDER BY created_at DESC LIMIT 1
                    """,
                    (mission_id,),
                ).fetchone()
                if not row:
                    return HiveMaterializationSnapshot(
                        ok=False,
                        found=False,
                        db_path=str(self.path),
                        mission_id=mission_id,
                    )
                resolved = str(row["plan_id"])
            return self._snapshot(conn, resolved)
        finally:
            conn.close()

    def record_dispatch_receipt(
        self,
        *,
        plan_id: str,
        work_unit_id: str,
        status: str,
        actor: str,
        evidence: dict[str, Any] | None = None,
        expected_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMaterializationSnapshot:
        target = DispatchStatus(str(status).strip().upper()).value
        request = {
            "plan_id": self._required(plan_id, "plan_id"),
            "work_unit_id": self._required(work_unit_id, "work_unit_id"),
            "status": target,
            "actor": self._required(actor, "actor"),
            "evidence": evidence or {},
            "expected_revision": expected_revision,
        }
        fingerprint = self._fingerprint(request)
        conn = self._connect()
        try:
            if idempotency_key:
                existing = conn.execute(
                    """
                    SELECT plan_id, request_sha256
                    FROM hive_materialization_events
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Dispatch idempotency key was already used "
                            "with different content."
                        )
                    return self._snapshot(conn, str(existing["plan_id"]))
            row = conn.execute(
                """
                SELECT mission_id, status, revision
                FROM hive_dispatch_receipts
                WHERE plan_id = ? AND work_unit_id = ?
                """,
                (plan_id, work_unit_id),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Dispatch receipt not found: {work_unit_id}")
            mission_id = str(row["mission_id"])
            current = str(row["status"])
            receipt_revision = int(row["revision"])
            if expected_revision is not None and receipt_revision != int(
                expected_revision
            ):
                raise HiveTransitionError(
                    "Stale dispatch revision: expected "
                    f"{expected_revision}, current {receipt_revision}."
                )
            if target not in DISPATCH_TRANSITIONS.get(current, set()):
                raise HiveTransitionError(
                    f"Illegal dispatch transition: {current} -> {target}"
                )
        finally:
            conn.close()

        terminal_work_status = target if target in TERMINAL_DISPATCH else None
        self.runtime.append_event(
            mission_id=mission_id,
            event_type=f"DISPATCH_{target}",
            sender=request["actor"],
            payload={
                "plan_id": plan_id,
                "dispatch_status": target,
                "evidence": request["evidence"],
            },
            work_unit_id=work_unit_id,
            work_unit_status=terminal_work_status,
            idempotency_key=f"materialization-dispatch:{fingerprint}",
        )

        with self._write() as conn:
            current_row = conn.execute(
                """
                SELECT status, revision
                FROM hive_dispatch_receipts
                WHERE plan_id = ? AND work_unit_id = ?
                """,
                (plan_id, work_unit_id),
            ).fetchone()
            if not current_row:
                raise HiveNotFoundError(f"Dispatch receipt not found: {work_unit_id}")
            if (
                str(current_row["status"]) != current
                or int(current_row["revision"]) != receipt_revision
            ):
                raise HiveTransitionError(
                    "Dispatch receipt changed during runtime synchronization."
                )
            now = self._now_ms()
            conn.execute(
                """
                UPDATE hive_dispatch_receipts
                SET status = ?, actor = ?, evidence_json = ?,
                    request_sha256 = ?, updated_at = ?,
                    revision = revision + 1
                WHERE plan_id = ? AND work_unit_id = ?
                """,
                (
                    target,
                    request["actor"],
                    self._json(request["evidence"]),
                    fingerprint,
                    now,
                    plan_id,
                    work_unit_id,
                ),
            )
            self._event(
                conn,
                plan_id=plan_id,
                event_type="DISPATCH_RECEIPT_RECORDED",
                actor=request["actor"],
                payload={
                    "work_unit_id": work_unit_id,
                    "from_status": current,
                    "to_status": target,
                    "evidence": request["evidence"],
                    "mission_work_status": terminal_work_status or "UNCHANGED",
                },
                idempotency_key=idempotency_key,
                fingerprint=fingerprint,
            )
            rows = conn.execute(
                """
                SELECT status FROM hive_dispatch_receipts
                WHERE plan_id = ?
                """,
                (plan_id,),
            ).fetchall()
            statuses = [str(item["status"]) for item in rows]
            if statuses and all(item in TERMINAL_DISPATCH for item in statuses):
                final = (
                    MaterializationStatus.READY_FOR_FAN_IN.value
                    if all(item == DispatchStatus.COMPLETED.value for item in statuses)
                    else MaterializationStatus.CLOSED_WITH_FAILURE.value
                )
                self._set_status(conn, plan_id, final)
                conn.execute(
                    """
                    UPDATE hive_worker_leases
                    SET status = ?, release_reason = ?,
                        updated_at = ?, revision = revision + 1
                    WHERE plan_id = ? AND status = ?
                    """,
                    (
                        LeaseStatus.RELEASED.value,
                        "All dispatch receipts reached terminal status.",
                        now,
                        plan_id,
                        LeaseStatus.ACTIVE.value,
                    ),
                )
                self._event(
                    conn,
                    plan_id=plan_id,
                    event_type="LEASES_AUTO_RELEASED",
                    actor=request["actor"],
                    payload={
                        "materialization_status": final,
                        "dispatch_statuses": statuses,
                    },
                )
            return self._snapshot(conn, plan_id)

    def release_leases(
        self,
        *,
        plan_id: str,
        actor: str,
        reason: str,
        revoke: bool = False,
        idempotency_key: str | None = None,
    ) -> HiveMaterializationSnapshot:
        request = {
            "plan_id": self._required(plan_id, "plan_id"),
            "actor": self._required(actor, "actor"),
            "reason": self._required(reason, "reason"),
            "revoke": bool(revoke),
        }
        fingerprint = self._fingerprint(request)
        target = LeaseStatus.REVOKED.value if revoke else LeaseStatus.RELEASED.value
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    """
                    SELECT plan_id, request_sha256
                    FROM hive_materialization_events
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Lease-release idempotency key was already "
                            "used with different content."
                        )
                    return self._snapshot(conn, str(existing["plan_id"]))
            if not conn.execute(
                "SELECT 1 FROM hive_invocation_materializations WHERE plan_id = ?",
                (plan_id,),
            ).fetchone():
                raise HiveNotFoundError(f"Materialization plan not found: {plan_id}")
            now = self._now_ms()
            conn.execute(
                """
                UPDATE hive_worker_leases
                SET status = ?, release_reason = ?,
                    updated_at = ?, revision = revision + 1
                WHERE plan_id = ? AND status = ?
                """,
                (
                    target,
                    reason,
                    now,
                    plan_id,
                    LeaseStatus.ACTIVE.value,
                ),
            )
            self._event(
                conn,
                plan_id=plan_id,
                event_type=("LEASES_REVOKED" if revoke else "LEASES_RELEASED"),
                actor=actor,
                payload={"reason": reason},
                idempotency_key=idempotency_key,
                fingerprint=fingerprint,
            )
            return self._snapshot(conn, plan_id)


_MATERIALIZATION_LOCK = threading.RLock()
_MATERIALIZATION_CACHE: dict[str, HiveMaterializationStore] = {}


def get_hive_materialization_store(
    path: str | Path | None = None,
) -> HiveMaterializationStore:
    resolved = Path(path or default_hive_runtime_db_path()).expanduser().resolve()
    key = str(resolved)
    with _MATERIALIZATION_LOCK:
        store = _MATERIALIZATION_CACHE.get(key)
        if store is None:
            store = HiveMaterializationStore(resolved)
            _MATERIALIZATION_CACHE[key] = store
        return store
