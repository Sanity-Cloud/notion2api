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
    default_hive_runtime_db_path,
)


class WorkerClass(str, Enum):
    TEMPORARY_WORKER = "TEMPORARY_WORKER"
    PERSISTENT_MEMBER = "PERSISTENT_MEMBER"
    SPECIALIST_CONTRACTOR = "SPECIALIST_CONTRACTOR"
    ROAMING_SCOUT = "ROAMING_SCOUT"
    HIVE_LEADER = "HIVE_LEADER"
    GOVERNANCE_REVIEWER = "GOVERNANCE_REVIEWER"


class WorkerStage(str, Enum):
    REQUISITIONED = "REQUISITIONED"
    SHADOW = "SHADOW"
    PROBATION = "PROBATION"
    APPOINTED = "APPOINTED"
    SUSPENDED = "SUSPENDED"
    OFFBOARDED = "OFFBOARDED"
    REJECTED = "REJECTED"


WORKER_TRANSITIONS: dict[str, set[str]] = {
    WorkerStage.REQUISITIONED.value: {
        WorkerStage.SHADOW.value,
        WorkerStage.REJECTED.value,
        WorkerStage.OFFBOARDED.value,
    },
    WorkerStage.SHADOW.value: {
        WorkerStage.PROBATION.value,
        WorkerStage.SUSPENDED.value,
        WorkerStage.OFFBOARDED.value,
    },
    WorkerStage.PROBATION.value: {
        WorkerStage.APPOINTED.value,
        WorkerStage.SUSPENDED.value,
        WorkerStage.OFFBOARDED.value,
    },
    WorkerStage.APPOINTED.value: {
        WorkerStage.SUSPENDED.value,
        WorkerStage.OFFBOARDED.value,
    },
    WorkerStage.SUSPENDED.value: {
        WorkerStage.PROBATION.value,
        WorkerStage.APPOINTED.value,
        WorkerStage.OFFBOARDED.value,
    },
    WorkerStage.OFFBOARDED.value: {WorkerStage.OFFBOARDED.value},
    WorkerStage.REJECTED.value: {WorkerStage.REJECTED.value},
}
HUMAN_APPROVAL_STAGES = {
    WorkerStage.PROBATION.value,
    WorkerStage.APPOINTED.value,
}
ACTIVE_EXECUTION_STAGES = {
    WorkerStage.PROBATION.value,
    WorkerStage.APPOINTED.value,
}
AUTHORITY_RANK = {f"A{level}": level for level in range(5)}


class HiveWorker(BaseModel):
    worker_id: str
    display_name: str
    worker_class: str
    stage: str
    role: str
    competencies: list[str] = Field(default_factory=list)
    writable_domains: list[str] = Field(default_factory=list)
    authority_ceiling: str = "A0"
    accountable_owner: str
    source_boundary: str = ""
    appointment_scope: str = ""
    created_at: int
    updated_at: int
    revision: int


class WorkforceSnapshot(BaseModel):
    ok: bool = True
    db_path: str = ""
    count: int = 0
    workers: list[HiveWorker] = Field(default_factory=list)
    error: str = ""


class HiveInvocationPlan(BaseModel):
    ok: bool = True
    db_path: str = ""
    objective: str = ""
    mode: str = "single_agent"
    reasons: list[str] = Field(default_factory=list)
    human_gate_required: bool = False
    requested_authority: str = "A0"
    suggested_lane_count: int = 1
    eligible_worker_count: int = 0
    selected_workers: list[HiveWorker] = Field(default_factory=list)
    missing_competencies: list[str] = Field(default_factory=list)
    missing_writable_domains: list[str] = Field(default_factory=list)
    error: str = ""


class HiveWorkforceStore:
    """Durable workforce registry plus a read-only Hive invocation planner."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self._schema_lock = threading.RLock()
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

    @staticmethod
    def _required(value: str, field_name: str) -> str:
        clean = str(value or "").strip()
        if not clean:
            raise ValueError(f"{field_name} is required")
        return clean

    @staticmethod
    def _normalized_list(values: list[str] | None) -> list[str]:
        return sorted({str(value).strip().lower() for value in (values or []) if str(value).strip()})

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
                    CREATE TABLE IF NOT EXISTS hive_workers (
                        worker_id TEXT PRIMARY KEY,
                        display_name TEXT NOT NULL,
                        worker_class TEXT NOT NULL,
                        stage TEXT NOT NULL,
                        role TEXT NOT NULL,
                        competencies_json TEXT NOT NULL DEFAULT '[]',
                        writable_domains_json TEXT NOT NULL DEFAULT '[]',
                        authority_ceiling TEXT NOT NULL DEFAULT 'A0',
                        accountable_owner TEXT NOT NULL,
                        source_boundary TEXT NOT NULL DEFAULT '',
                        appointment_scope TEXT NOT NULL DEFAULT '',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_worker_events (
                        event_id TEXT PRIMARY KEY,
                        worker_id TEXT NOT NULL REFERENCES hive_workers(worker_id) ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        from_stage TEXT NOT NULL DEFAULT '',
                        to_stage TEXT NOT NULL DEFAULT '',
                        reason TEXT NOT NULL DEFAULT '',
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_hive_workers_stage_class
                        ON hive_workers(stage, worker_class, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_hive_worker_events_worker_created
                        ON hive_worker_events(worker_id, created_at DESC, event_id DESC);
                    """
                )

    @staticmethod
    def _worker_from_row(row: sqlite3.Row) -> HiveWorker:
        return HiveWorker(
            worker_id=str(row["worker_id"]),
            display_name=str(row["display_name"]),
            worker_class=str(row["worker_class"]),
            stage=str(row["stage"]),
            role=str(row["role"]),
            competencies=json.loads(str(row["competencies_json"])),
            writable_domains=json.loads(str(row["writable_domains_json"])),
            authority_ceiling=str(row["authority_ceiling"]),
            accountable_owner=str(row["accountable_owner"]),
            source_boundary=str(row["source_boundary"]),
            appointment_scope=str(row["appointment_scope"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    def _snapshot(self, conn: sqlite3.Connection, worker_id: str | None = None, limit: int = 100) -> WorkforceSnapshot:
        if worker_id:
            rows = conn.execute("SELECT * FROM hive_workers WHERE worker_id = ?", (worker_id,)).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM hive_workers ORDER BY updated_at DESC, worker_id LIMIT ?",
                (max(1, min(int(limit), 1000)),),
            ).fetchall()
        workers = [self._worker_from_row(row) for row in rows]
        return WorkforceSnapshot(db_path=str(self.path), count=len(workers), workers=workers)

    def register_worker(
        self,
        *,
        display_name: str,
        worker_class: str,
        role: str,
        accountable_owner: str,
        competencies: list[str] | None = None,
        writable_domains: list[str] | None = None,
        authority_ceiling: str = "A0",
        source_boundary: str = "",
        appointment_scope: str = "",
        worker_id: str | None = None,
        actor: str = "notion2api",
        idempotency_key: str | None = None,
    ) -> WorkforceSnapshot:
        worker_key = str(worker_id or f"worker-{uuid.uuid4()}").strip()
        worker_type = WorkerClass(str(worker_class).strip().upper()).value
        authority = str(authority_ceiling or "A0").strip().upper()
        if authority not in AUTHORITY_RANK:
            raise ValueError(f"Unsupported authority ceiling: {authority}")
        request = {
            "worker_id": worker_key,
            "display_name": self._required(display_name, "display_name"),
            "worker_class": worker_type,
            "role": self._required(role, "role"),
            "accountable_owner": self._required(accountable_owner, "accountable_owner"),
            "competencies": self._normalized_list(competencies),
            "writable_domains": self._normalized_list(writable_domains),
            "authority_ceiling": authority,
            "source_boundary": str(source_boundary or "").strip(),
            "appointment_scope": str(appointment_scope or "").strip(),
            "stage": WorkerStage.REQUISITIONED.value,
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT worker_id, request_sha256 FROM hive_workers WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Worker idempotency key was already used with different content."
                        )
                    return self._snapshot(conn, str(existing["worker_id"]))
            if conn.execute("SELECT 1 FROM hive_workers WHERE worker_id = ?", (worker_key,)).fetchone():
                raise HiveIdempotencyConflict(f"Worker already exists: {worker_key}")
            now = self._now_ms()
            conn.execute(
                """
                INSERT INTO hive_workers(
                    worker_id, display_name, worker_class, stage, role,
                    competencies_json, writable_domains_json, authority_ceiling,
                    accountable_owner, source_boundary, appointment_scope,
                    idempotency_key, request_sha256, created_at, updated_at, revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    worker_key,
                    request["display_name"],
                    request["worker_class"],
                    request["stage"],
                    request["role"],
                    self._json(request["competencies"]),
                    self._json(request["writable_domains"]),
                    request["authority_ceiling"],
                    request["accountable_owner"],
                    request["source_boundary"],
                    request["appointment_scope"],
                    idempotency_key,
                    fingerprint,
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO hive_worker_events(
                    event_id, worker_id, event_type, actor, from_stage, to_stage,
                    reason, payload_json, request_sha256, created_at
                ) VALUES (?, ?, 'WORKER_REQUISITIONED', ?, '', ?, '', ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()), worker_key, self._required(actor, "actor"),
                    WorkerStage.REQUISITIONED.value, self._json(request), fingerprint, now,
                ),
            )
            return self._snapshot(conn, worker_key)

    def transition_worker(
        self,
        *,
        worker_id: str,
        target_stage: str,
        actor: str,
        reason: str,
        human_approval: bool = False,
        expected_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> WorkforceSnapshot:
        target = WorkerStage(str(target_stage).strip().upper()).value
        request = {
            "worker_id": self._required(worker_id, "worker_id"),
            "target_stage": target,
            "actor": self._required(actor, "actor"),
            "reason": self._required(reason, "reason"),
            "human_approval": bool(human_approval),
            "expected_revision": expected_revision,
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT worker_id, request_sha256 FROM hive_worker_events WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Worker-transition idempotency key was already used with different content."
                        )
                    return self._snapshot(conn, str(existing["worker_id"]))
            row = conn.execute("SELECT stage, revision FROM hive_workers WHERE worker_id = ?", (worker_id,)).fetchone()
            if not row:
                raise HiveNotFoundError(f"Worker not found: {worker_id}")
            current = str(row["stage"])
            if expected_revision is not None and int(row["revision"]) != int(expected_revision):
                raise HiveTransitionError(
                    f"Stale worker revision: expected {expected_revision}, current {int(row['revision'])}."
                )
            if target not in WORKER_TRANSITIONS.get(current, set()):
                raise HiveTransitionError(f"Illegal worker transition: {current} -> {target}")
            if target in HUMAN_APPROVAL_STAGES and not human_approval:
                raise HiveTransitionError(
                    f"Human approval is required for worker transition to {target}."
                )
            now = self._now_ms()
            conn.execute(
                "UPDATE hive_workers SET stage = ?, updated_at = ?, revision = revision + 1 WHERE worker_id = ?",
                (target, now, worker_id),
            )
            conn.execute(
                """
                INSERT INTO hive_worker_events(
                    event_id, worker_id, event_type, actor, from_stage, to_stage,
                    reason, payload_json, idempotency_key, request_sha256, created_at
                ) VALUES (?, ?, 'WORKER_STAGE_CHANGED', ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()), worker_id, request["actor"], current, target,
                    request["reason"], self._json(request), idempotency_key, fingerprint, now,
                ),
            )
            return self._snapshot(conn, worker_id)

    def list_workers(
        self,
        *,
        stage: str | None = None,
        worker_class: str | None = None,
        limit: int = 100,
    ) -> WorkforceSnapshot:
        clauses: list[str] = []
        params: list[Any] = []
        if stage:
            clauses.append("stage = ?")
            params.append(WorkerStage(str(stage).strip().upper()).value)
        if worker_class:
            clauses.append("worker_class = ?")
            params.append(WorkerClass(str(worker_class).strip().upper()).value)
        query = "SELECT * FROM hive_workers"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, worker_id LIMIT ?"
        params.append(max(1, min(int(limit), 1000)))
        conn = self._connect()
        try:
            rows = conn.execute(query, tuple(params)).fetchall()
            workers = [self._worker_from_row(row) for row in rows]
            return WorkforceSnapshot(db_path=str(self.path), count=len(workers), workers=workers)
        finally:
            conn.close()

    @staticmethod
    def _authority_allows(worker_authority: str, requested_authority: str) -> bool:
        return AUTHORITY_RANK.get(worker_authority, -1) >= AUTHORITY_RANK.get(requested_authority, 99)

    def plan_invocation(
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
    ) -> HiveInvocationPlan:
        objective_text = self._required(objective, "objective")
        competencies = self._normalized_list(required_competencies)
        domains = self._normalized_list(writable_domains)
        preferred = {
            str(value).strip()
            for value in (preferred_worker_ids or [])
            if str(value).strip()
        }
        risk = str(risk_level or "low").strip().lower()
        if risk not in {"low", "medium", "high", "critical"}:
            raise ValueError(f"Unsupported risk level: {risk}")
        authority = str(authority_ceiling or "A0").strip().upper()
        if authority not in AUTHORITY_RANK:
            raise ValueError(f"Unsupported authority ceiling: {authority}")

        active = [
            worker
            for worker in self.list_workers(limit=1000).workers
            if worker.stage in ACTIVE_EXECUTION_STAGES
            and (not preferred or worker.worker_id in preferred)
        ]
        single_candidates = [
            worker
            for worker in active
            if self._authority_allows(worker.authority_ceiling, authority)
            and set(competencies).issubset(set(worker.competencies))
            and set(domains).issubset(set(worker.writable_domains))
        ]
        single_candidates.sort(
            key=lambda item: (
                item.stage != WorkerStage.APPOINTED.value,
                item.worker_id,
            )
        )

        reasons: list[str] = []
        force_hive = False
        if int(parallelizable_workstreams) > 1:
            force_hive = True
            reasons.append("Multiple parallel workstreams benefit from bounded worker lanes.")
        if int(dependency_count) > 0:
            force_hive = True
            reasons.append("Task dependencies require explicit orchestration and fan-in.")
        if independent_review_required:
            force_hive = True
            reasons.append("Independent review requires a separate worker lane.")
        if risk in {"high", "critical"}:
            force_hive = True
            reasons.append(f"{risk.title()} risk requires explicit evidence and fan-in.")
        if external_effects:
            force_hive = True
            reasons.append("External effects require a human-controlled execution gate.")

        mode = "hive" if force_hive or not single_candidates else "single_agent"
        if mode == "single_agent":
            selected = single_candidates[:1]
            suggested_lane_count = 1
            reasons.append(
                "One appointed worker covers the required competencies, domains, and authority."
            )
            eligible_count = len(single_candidates)
        else:
            eligible = [
                worker
                for worker in active
                if not competencies
                or set(worker.competencies).intersection(competencies)
            ]
            eligible.sort(
                key=lambda item: (
                    item.stage != WorkerStage.APPOINTED.value,
                    item.worker_class != WorkerClass.GOVERNANCE_REVIEWER.value,
                    -AUTHORITY_RANK.get(item.authority_ceiling, -1),
                    item.worker_id,
                )
            )
            suggested_lane_count = max(
                2,
                int(parallelizable_workstreams),
                2 if independent_review_required else 1,
            )
            suggested_lane_count = min(suggested_lane_count, 5)
            selected = eligible[:suggested_lane_count]
            eligible_count = len(eligible)
            if not force_hive:
                reasons.append(
                    "No single appointed worker covers the full task; bounded lanes are required."
                )
            if len(competencies) > 1:
                reasons.append("Multiple competencies must be covered across the selected lanes.")
            if len(domains) > 1:
                reasons.append("Writable domains must remain explicit across the selected lanes.")

        covered_competencies = {
            item for worker in selected for item in worker.competencies
        }
        covered_domains = {
            item for worker in selected for item in worker.writable_domains
        }
        missing_competencies = sorted(set(competencies) - covered_competencies)
        missing_domains = sorted(set(domains) - covered_domains)
        if missing_competencies:
            reasons.append("The registry does not cover every required competency.")
        if missing_domains:
            reasons.append("The registry does not cover every required writable domain.")
        lower_authority_selected = any(
            not self._authority_allows(worker.authority_ceiling, authority)
            for worker in selected
        )
        if lower_authority_selected:
            reasons.append(
                "Selected workers require bounded sub-lanes below the requested authority ceiling."
            )
        human_gate = (
            external_effects
            or risk in {"high", "critical"}
            or AUTHORITY_RANK[authority] >= AUTHORITY_RANK["A3"]
            or any(
                worker.stage == WorkerStage.PROBATION.value for worker in selected
            )
            or lower_authority_selected
            or bool(missing_competencies)
            or bool(missing_domains)
        )
        return HiveInvocationPlan(
            db_path=str(self.path),
            objective=objective_text,
            mode=mode,
            reasons=reasons,
            human_gate_required=human_gate,
            requested_authority=authority,
            suggested_lane_count=suggested_lane_count,
            eligible_worker_count=eligible_count,
            selected_workers=selected,
            missing_competencies=missing_competencies,
            missing_writable_domains=missing_domains,
        )


_WORKFORCE_LOCK = threading.RLock()
_WORKFORCE_CACHE: dict[str, HiveWorkforceStore] = {}


def get_hive_workforce_store(path: str | Path | None = None) -> HiveWorkforceStore:
    resolved = Path(path or default_hive_runtime_db_path()).expanduser().resolve()
    key = str(resolved)
    with _WORKFORCE_LOCK:
        store = _WORKFORCE_CACHE.get(key)
        if store is None:
            store = HiveWorkforceStore(resolved)
            _WORKFORCE_CACHE[key] = store
        return store