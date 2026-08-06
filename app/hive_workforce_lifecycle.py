from __future__ import annotations

import hashlib
import json
import sqlite3
import time
import uuid
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel, Field

from app.governed_authorization import (
    GovernedAuthorizationError,
    require_governed_authorization,
)
from app.hive_runtime import (
    HiveIdempotencyConflict,
    HiveNotFoundError,
    HiveTransitionError,
)
from app.hive_workforce import (
    AUTHORITY_RANK,
    HiveWorkforceStore,
    WorkerClass,
    WorkerStage,
)

DEFAULT_LEASE_TTL_SECONDS = 24 * 60 * 60
MAX_LEASE_TTL_SECONDS = 7 * 24 * 60 * 60
DEFAULT_HEARTBEAT_STALE_SECONDS = 30 * 60
DEFAULT_NO_HEARTBEAT_GRACE_SECONDS = 6 * 60 * 60


class RecruitmentMode(str, Enum):
    DISABLED = "disabled"
    REQUISITION_ONLY = "requisition_only"
    AUTO_APPOINT = "auto_appoint"


class LeaseLiveness(str, Enum):
    UNKNOWN = "UNKNOWN"
    LIVE = "LIVE"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    OFFLINE = "OFFLINE"
    EXPIRED = "EXPIRED"
    RELEASED = "RELEASED"
    REVOKED = "REVOKED"


class GapRecruitmentSnapshot(BaseModel):
    ok: bool = True
    mode: str = RecruitmentMode.DISABLED.value
    plan_id: str = ""
    status: str = "NOT_REQUIRED"
    requested_competencies: list[str] = Field(default_factory=list)
    requested_writable_domains: list[str] = Field(default_factory=list)
    recruited_worker_ids: list[str] = Field(default_factory=list)
    appointed_worker_ids: list[str] = Field(default_factory=list)
    authorization: dict[str, Any] = Field(default_factory=dict)
    error: str = ""


class LeaseReconciliationItem(BaseModel):
    lease_id: str
    plan_id: str
    mission_id: str
    work_unit_id: str
    worker_id: str
    previous_status: str
    current_status: str
    action: str
    reason: str
    issued_at: int
    expires_at: int
    last_heartbeat_at: int
    heartbeat_status: str
    liveness_status: str


class LeaseReconciliationSnapshot(BaseModel):
    ok: bool = True
    db_path: str = ""
    dry_run: bool = True
    inspected_count: int = 0
    stale_count: int = 0
    changed_count: int = 0
    items: list[LeaseReconciliationItem] = Field(default_factory=list)
    error: str = ""


class WorkforceAuditFinding(BaseModel):
    worker_id: str
    worker_class: str
    stage: str
    severity: str
    finding_type: str
    reason: str
    recommended_action: str
    protected_role: bool = False
    active_lease_count: int = 0
    last_heartbeat_at: int = 0


class WorkforceAuditSnapshot(BaseModel):
    ok: bool = True
    db_path: str = ""
    audit_id: str = ""
    dry_run: bool = True
    finding_count: int = 0
    action_count: int = 0
    findings: list[WorkforceAuditFinding] = Field(default_factory=list)
    acted_worker_ids: list[str] = Field(default_factory=list)
    authorization: dict[str, Any] = Field(default_factory=dict)
    error: str = ""


class HiveWorkforceLifecycleStore:
    """Recruitment, lease-liveness, reconciliation, and workforce-audit controls."""

    def __init__(self, path: str | Path, workforce: HiveWorkforceStore):
        self.path = Path(path).expanduser().resolve()
        self.workforce = workforce
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
    def _bounded_seconds(value: int | None, *, default: int) -> int:
        resolved = default if value is None else int(value)
        return max(60, min(resolved, MAX_LEASE_TTL_SECONDS))

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

    @staticmethod
    def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
        return {str(row["name"]) for row in conn.execute(f"PRAGMA table_info({table})")}

    def _ensure_schema(self) -> None:
        with self._connect() as conn:
            tables = {
                str(row[0])
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
            if "hive_worker_leases" not in tables:
                raise RuntimeError(
                    "Hive materialization schema must exist before lifecycle controls are initialized."
                )
            materialization_columns = self._columns(
                conn, "hive_invocation_materializations"
            )
            materialization_additions = {
                "recruitment_mode": "TEXT NOT NULL DEFAULT 'disabled'",
                "recruited_worker_ids_json": "TEXT NOT NULL DEFAULT '[]'",
            }
            for name, definition in materialization_additions.items():
                if name not in materialization_columns:
                    conn.execute(
                        "ALTER TABLE hive_invocation_materializations "
                        f"ADD COLUMN {name} {definition}"
                    )
            columns = self._columns(conn, "hive_worker_leases")
            additions = {
                "issued_at": "INTEGER NOT NULL DEFAULT 0",
                "expires_at": "INTEGER NOT NULL DEFAULT 0",
                "last_heartbeat_at": "INTEGER NOT NULL DEFAULT 0",
                "heartbeat_status": "TEXT NOT NULL DEFAULT 'UNKNOWN'",
                "renewal_count": "INTEGER NOT NULL DEFAULT 0",
                "liveness_evidence_json": "TEXT NOT NULL DEFAULT '{}'",
            }
            for name, definition in additions.items():
                if name not in columns:
                    conn.execute(
                        f"ALTER TABLE hive_worker_leases ADD COLUMN {name} {definition}"
                    )
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS hive_lease_events (
                    event_id TEXT PRIMARY KEY,
                    lease_id TEXT NOT NULL REFERENCES hive_worker_leases(lease_id) ON DELETE CASCADE,
                    plan_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    payload_json TEXT NOT NULL DEFAULT '{}',
                    idempotency_key TEXT UNIQUE,
                    request_sha256 TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_hive_lease_events_lease_created
                    ON hive_lease_events(lease_id, created_at DESC, event_id DESC);
                CREATE INDEX IF NOT EXISTS idx_hive_worker_leases_expiry
                    ON hive_worker_leases(status, expires_at, last_heartbeat_at);
                CREATE TABLE IF NOT EXISTS hive_workforce_audits (
                    audit_id TEXT PRIMARY KEY,
                    actor TEXT NOT NULL,
                    dry_run INTEGER NOT NULL,
                    findings_json TEXT NOT NULL DEFAULT '[]',
                    actions_json TEXT NOT NULL DEFAULT '[]',
                    authorization_json TEXT NOT NULL DEFAULT '{}',
                    idempotency_key TEXT UNIQUE,
                    request_sha256 TEXT NOT NULL,
                    created_at INTEGER NOT NULL
                );
                """
            )
            ttl_ms = DEFAULT_LEASE_TTL_SECONDS * 1000
            conn.execute(
                "UPDATE hive_worker_leases SET issued_at = created_at WHERE issued_at = 0"
            )
            conn.execute(
                "UPDATE hive_worker_leases SET expires_at = created_at + ? WHERE expires_at = 0",
                (ttl_ms,),
            )

    @staticmethod
    def lease_liveness(
        *,
        status: str,
        expires_at: int,
        last_heartbeat_at: int,
        heartbeat_status: str,
        now_ms: int | None = None,
        stale_after_seconds: int = DEFAULT_HEARTBEAT_STALE_SECONDS,
    ) -> tuple[str, bool]:
        now = int(now_ms or time.time() * 1000)
        state = str(status or "").upper()
        heartbeat = str(heartbeat_status or "UNKNOWN").upper()
        if state == "RELEASED":
            return LeaseLiveness.RELEASED.value, False
        if state == "REVOKED":
            return LeaseLiveness.REVOKED.value, False
        if state == "EXPIRED" or (expires_at and expires_at <= now):
            return LeaseLiveness.EXPIRED.value, False
        if heartbeat == "OFFLINE":
            return LeaseLiveness.OFFLINE.value, False
        if not last_heartbeat_at:
            return LeaseLiveness.UNKNOWN.value, False
        if now - int(last_heartbeat_at) > int(stale_after_seconds) * 1000:
            return LeaseLiveness.STALE.value, False
        if heartbeat == "DEGRADED":
            return LeaseLiveness.DEGRADED.value, True
        return LeaseLiveness.LIVE.value, heartbeat in {"RUNNING", "IDLE", "READY"}

    @staticmethod
    def _proposed_authority(requested_authority: str, worker_class: str) -> str:
        ceiling = "A2" if worker_class == WorkerClass.GOVERNANCE_REVIEWER.value else "A2"
        rank = min(AUTHORITY_RANK.get(requested_authority, 0), AUTHORITY_RANK[ceiling])
        return f"A{rank}"

    def recruit_gaps(
        self,
        *,
        plan_id: str,
        objective: str,
        missing_competencies: list[str],
        missing_writable_domains: list[str],
        requested_authority: str,
        actor: str,
        mode: str = RecruitmentMode.REQUISITION_ONLY.value,
        reviewer_missing: bool = False,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
    ) -> GapRecruitmentSnapshot:
        recruitment_mode = RecruitmentMode(str(mode).strip().lower())
        competencies = sorted({str(item).strip().lower() for item in missing_competencies if str(item).strip()})
        domains = sorted({str(item).strip().lower() for item in missing_writable_domains if str(item).strip()})
        result = GapRecruitmentSnapshot(
            mode=recruitment_mode.value,
            plan_id=str(plan_id),
            requested_competencies=competencies,
            requested_writable_domains=domains,
        )
        if recruitment_mode == RecruitmentMode.DISABLED or not (competencies or domains or reviewer_missing):
            return result

        authorization: dict[str, Any] = {}
        if recruitment_mode == RecruitmentMode.AUTO_APPOINT:
            try:
                authorization = require_governed_authorization(
                    governance_authorization,
                    required_authority=self._proposed_authority(
                        requested_authority,
                        WorkerClass.SPECIALIST_CONTRACTOR.value,
                    ),
                    legacy_human_approval=human_approval,
                )
            except GovernedAuthorizationError as exc:
                result.ok = False
                result.status = "AUTHORIZATION_REQUIRED"
                result.error = str(exc)
                return result
        result.authorization = authorization

        specs: list[dict[str, Any]] = []
        if competencies or domains:
            specs.append(
                {
                    "worker_class": WorkerClass.SPECIALIST_CONTRACTOR.value,
                    "role": "Autonomous Gap Coverage Specialist",
                    "display_name": "Autonomous Gap Coverage Bee",
                    "competencies": competencies,
                    "writable_domains": domains,
                }
            )
        if reviewer_missing:
            specs.append(
                {
                    "worker_class": WorkerClass.GOVERNANCE_REVIEWER.value,
                    "role": "Independent Gap Recruitment Reviewer",
                    "display_name": "Gap Recruitment Review Bee",
                    "competencies": ["independent code review", "governance boundary review"],
                    "writable_domains": [],
                }
            )

        for spec in specs:
            digest = self._fingerprint(
                {
                    "plan_id": plan_id,
                    "worker_class": spec["worker_class"],
                    "competencies": spec["competencies"],
                    "writable_domains": spec["writable_domains"],
                }
            )[:20]
            worker_id = f"auto-recruit-{digest}"
            authority = self._proposed_authority(requested_authority, spec["worker_class"])
            snapshot = self.workforce.register_worker(
                worker_id=worker_id,
                display_name=spec["display_name"],
                worker_class=spec["worker_class"],
                role=spec["role"],
                accountable_owner=actor,
                competencies=spec["competencies"],
                writable_domains=spec["writable_domains"],
                authority_ceiling=authority,
                source_boundary=(
                    "Only sources and writable domains declared by the originating materialization plan. "
                    "No credentials, routing mutation, deployment, or external publication."
                ),
                appointment_scope=(
                    f"Bounded coverage for materialization {plan_id}: {objective[:240]}"
                ),
                actor=actor,
                idempotency_key=f"auto-recruit:{plan_id}:{digest}",
            )
            worker = snapshot.workers[0]
            result.recruited_worker_ids.append(worker.worker_id)
            if recruitment_mode != RecruitmentMode.AUTO_APPOINT:
                continue
            if worker.stage == WorkerStage.REQUISITIONED.value:
                worker = self.workforce.transition_worker(
                    worker_id=worker.worker_id,
                    target_stage=WorkerStage.SHADOW.value,
                    actor=actor,
                    reason=f"Automated evidence-bounded shadow evaluation for {plan_id}.",
                    idempotency_key=f"auto-shadow:{plan_id}:{digest}",
                ).workers[0]
            if worker.stage == WorkerStage.SHADOW.value:
                worker = self.workforce.transition_worker(
                    worker_id=worker.worker_id,
                    target_stage=WorkerStage.PROBATION.value,
                    actor=actor,
                    reason=f"Governed bounded probation for gap coverage in {plan_id}.",
                    human_approval=human_approval,
                    governance_authorization=governance_authorization,
                    expected_revision=worker.revision,
                    idempotency_key=f"auto-probation:{plan_id}:{digest}",
                ).workers[0]
            if worker.stage == WorkerStage.PROBATION.value:
                worker = self.workforce.transition_worker(
                    worker_id=worker.worker_id,
                    target_stage=WorkerStage.APPOINTED.value,
                    actor=actor,
                    reason=f"Governed appointment for bounded gap coverage in {plan_id}.",
                    human_approval=human_approval,
                    governance_authorization=governance_authorization,
                    expected_revision=worker.revision,
                    idempotency_key=f"auto-appointed:{plan_id}:{digest}",
                ).workers[0]
            if worker.stage == WorkerStage.APPOINTED.value:
                result.appointed_worker_ids.append(worker.worker_id)

        result.status = (
            "APPOINTED" if result.appointed_worker_ids else "REQUISITIONED"
        )
        return result

    def record_lease_heartbeat(
        self,
        *,
        lease_id: str,
        actor: str,
        heartbeat_status: str = "RUNNING",
        extend_seconds: int | None = 60 * 60,
        evidence: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> LeaseReconciliationSnapshot:
        heartbeat = str(heartbeat_status or "RUNNING").strip().upper()
        if heartbeat not in {"READY", "RUNNING", "IDLE", "DEGRADED", "OFFLINE"}:
            raise ValueError(f"Unsupported heartbeat status: {heartbeat}")
        request = {
            "lease_id": str(lease_id).strip(),
            "actor": str(actor).strip(),
            "heartbeat_status": heartbeat,
            "extend_seconds": extend_seconds,
            "evidence": dict(evidence or {}),
        }
        if not request["lease_id"] or not request["actor"]:
            raise ValueError("lease_id and actor are required")
        fingerprint = self._fingerprint(request)
        now = self._now_ms()
        extension_ms = self._bounded_seconds(extend_seconds, default=60 * 60) * 1000
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT lease_id, request_sha256 FROM hive_lease_events WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Lease-heartbeat idempotency key was reused with different content."
                        )
                    row = conn.execute(
                        "SELECT * FROM hive_worker_leases WHERE lease_id = ?",
                        (str(existing["lease_id"]),),
                    ).fetchone()
                    return self._lease_snapshot(conn, [row], dry_run=False, action="HEARTBEAT_REPLAY")
            row = conn.execute(
                "SELECT * FROM hive_worker_leases WHERE lease_id = ?",
                (request["lease_id"],),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Worker lease not found: {request['lease_id']}")
            if str(row["status"]) != "ACTIVE":
                raise HiveTransitionError(
                    f"Heartbeat requires ACTIVE lease; current status is {row['status']}."
                )
            if int(row["expires_at"] or 0) <= now:
                conn.execute(
                    "UPDATE hive_worker_leases SET status='EXPIRED', release_reason=?, updated_at=?, revision=revision+1 WHERE lease_id=? AND status='ACTIVE'",
                    ("Lease expired before heartbeat was received.", now, request["lease_id"]),
                )
                updated = conn.execute(
                    "SELECT * FROM hive_worker_leases WHERE lease_id = ?",
                    (request["lease_id"],),
                ).fetchone()
                return self._lease_snapshot(conn, [updated], dry_run=False, action="EXPIRED")
            expires_at = int(row["expires_at"])
            if heartbeat != "OFFLINE":
                expires_at = max(expires_at, now + extension_ms)
            conn.execute(
                """
                UPDATE hive_worker_leases
                SET last_heartbeat_at=?, heartbeat_status=?, expires_at=?,
                    renewal_count=renewal_count+1, liveness_evidence_json=?,
                    updated_at=?, revision=revision+1
                WHERE lease_id=? AND status='ACTIVE'
                """,
                (
                    now,
                    heartbeat,
                    expires_at,
                    self._json(request["evidence"]),
                    now,
                    request["lease_id"],
                ),
            )
            conn.execute(
                """
                INSERT INTO hive_lease_events(
                    event_id, lease_id, plan_id, event_type, actor, payload_json,
                    idempotency_key, request_sha256, created_at
                ) VALUES (?, ?, ?, 'LEASE_HEARTBEAT', ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    request["lease_id"],
                    str(row["plan_id"]),
                    request["actor"],
                    self._json(request),
                    idempotency_key,
                    fingerprint,
                    now,
                ),
            )
            updated = conn.execute(
                "SELECT * FROM hive_worker_leases WHERE lease_id = ?",
                (request["lease_id"],),
            ).fetchone()
            return self._lease_snapshot(conn, [updated], dry_run=False, action="HEARTBEAT")

    def _lease_item(
        self,
        row: sqlite3.Row,
        *,
        action: str,
        reason: str = "",
        previous_status: str | None = None,
        now_ms: int | None = None,
    ) -> LeaseReconciliationItem:
        liveness, _ = self.lease_liveness(
            status=str(row["status"]),
            expires_at=int(row["expires_at"] or 0),
            last_heartbeat_at=int(row["last_heartbeat_at"] or 0),
            heartbeat_status=str(row["heartbeat_status"] or "UNKNOWN"),
            now_ms=now_ms,
        )
        return LeaseReconciliationItem(
            lease_id=str(row["lease_id"]),
            plan_id=str(row["plan_id"]),
            mission_id=str(row["mission_id"]),
            work_unit_id=str(row["work_unit_id"]),
            worker_id=str(row["worker_id"]),
            previous_status=str(previous_status or row["status"]),
            current_status=str(row["status"]),
            action=action,
            reason=reason or str(row["release_reason"] or ""),
            issued_at=int(row["issued_at"] or row["created_at"]),
            expires_at=int(row["expires_at"] or 0),
            last_heartbeat_at=int(row["last_heartbeat_at"] or 0),
            heartbeat_status=str(row["heartbeat_status"] or "UNKNOWN"),
            liveness_status=liveness,
        )

    def _lease_snapshot(
        self,
        conn: sqlite3.Connection,
        rows: list[sqlite3.Row],
        *,
        dry_run: bool,
        action: str,
    ) -> LeaseReconciliationSnapshot:
        items = [self._lease_item(row, action=action) for row in rows if row is not None]
        return LeaseReconciliationSnapshot(
            db_path=str(self.path),
            dry_run=dry_run,
            inspected_count=len(items),
            stale_count=sum(item.liveness_status in {"STALE", "EXPIRED", "OFFLINE"} for item in items),
            changed_count=0 if dry_run else len(items),
            items=items,
        )

    def reconcile_stale_leases(
        self,
        *,
        actor: str,
        plan_id: str = "",
        dry_run: bool = True,
        heartbeat_stale_after_seconds: int = DEFAULT_HEARTBEAT_STALE_SECONDS,
        no_heartbeat_grace_seconds: int = DEFAULT_NO_HEARTBEAT_GRACE_SECONDS,
        revoke: bool = False,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> LeaseReconciliationSnapshot:
        if not str(actor).strip():
            raise ValueError("actor is required")
        authorization: dict[str, Any] = {}
        if revoke:
            try:
                authorization = require_governed_authorization(
                    governance_authorization,
                    required_authority="A2",
                    legacy_human_approval=human_approval,
                )
            except GovernedAuthorizationError as exc:
                raise HiveTransitionError(str(exc)) from exc
        request = {
            "actor": str(actor).strip(),
            "plan_id": str(plan_id or "").strip(),
            "dry_run": bool(dry_run),
            "heartbeat_stale_after_seconds": int(heartbeat_stale_after_seconds),
            "no_heartbeat_grace_seconds": int(no_heartbeat_grace_seconds),
            "revoke": bool(revoke),
            "authorization": authorization,
        }
        fingerprint = self._fingerprint(request)
        now = self._now_ms()
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT payload_json, request_sha256 FROM hive_lease_events WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Lease-reconciliation idempotency key was reused with different content."
                        )
                    payload = json.loads(str(existing["payload_json"]))
                    rows = [
                        conn.execute(
                            "SELECT * FROM hive_worker_leases WHERE lease_id = ?",
                            (lease_id,),
                        ).fetchone()
                        for lease_id in payload.get("lease_ids", [])
                    ]
                    return self._lease_snapshot(conn, rows, dry_run=bool(payload.get("dry_run")), action="RECONCILIATION_REPLAY")
            query = "SELECT * FROM hive_worker_leases WHERE status='ACTIVE'"
            params: list[Any] = []
            if request["plan_id"]:
                query += " AND plan_id = ?"
                params.append(request["plan_id"])
            query += " ORDER BY issued_at, lease_id"
            rows = conn.execute(query, tuple(params)).fetchall()
            candidates: list[tuple[sqlite3.Row, str]] = []
            stale_ms = max(60, int(heartbeat_stale_after_seconds)) * 1000
            grace_ms = max(60, int(no_heartbeat_grace_seconds)) * 1000
            for row in rows:
                expires_at = int(row["expires_at"] or 0)
                heartbeat_at = int(row["last_heartbeat_at"] or 0)
                issued_at = int(row["issued_at"] or row["created_at"])
                heartbeat_status = str(row["heartbeat_status"] or "UNKNOWN").upper()
                reason = ""
                if expires_at and expires_at <= now:
                    reason = "Lease expiry timestamp elapsed."
                elif heartbeat_status == "OFFLINE":
                    reason = "Worker reported OFFLINE."
                elif heartbeat_at and now - heartbeat_at > stale_ms:
                    reason = "Worker heartbeat freshness threshold elapsed."
                elif not heartbeat_at and now - issued_at > grace_ms:
                    reason = "No worker heartbeat was recorded within the grace period."
                if reason:
                    candidates.append((row, reason))
            items: list[LeaseReconciliationItem] = []
            target = "REVOKED" if revoke else "EXPIRED"
            for row, reason in candidates:
                previous = str(row["status"])
                if not dry_run:
                    conn.execute(
                        """
                        UPDATE hive_worker_leases
                        SET status=?, release_reason=?, heartbeat_status='OFFLINE',
                            updated_at=?, revision=revision+1
                        WHERE lease_id=? AND status='ACTIVE'
                        """,
                        (target, reason, now, str(row["lease_id"])),
                    )
                    row = conn.execute(
                        "SELECT * FROM hive_worker_leases WHERE lease_id = ?",
                        (str(row["lease_id"]),),
                    ).fetchone()
                    conn.execute(
                        """
                        INSERT INTO hive_lease_events(
                            event_id, lease_id, plan_id, event_type, actor,
                            payload_json, request_sha256, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            str(row["lease_id"]),
                            str(row["plan_id"]),
                            "LEASE_AUTO_REVOKED" if revoke else "LEASE_AUTO_EXPIRED",
                            request["actor"],
                            self._json({"reason": reason, "target": target}),
                            fingerprint,
                            now,
                        ),
                    )
                items.append(
                    self._lease_item(
                        row,
                        action="WOULD_REVOKE" if dry_run and revoke else (
                            "WOULD_EXPIRE" if dry_run else target
                        ),
                        reason=reason,
                        previous_status=previous,
                        now_ms=now,
                    )
                )
            event_payload = {
                **request,
                "lease_ids": [item.lease_id for item in items],
                "candidate_count": len(items),
            }
            if idempotency_key or items:
                anchor = items[0].lease_id if items else str(rows[0]["lease_id"]) if rows else ""
                if anchor:
                    conn.execute(
                        """
                        INSERT INTO hive_lease_events(
                            event_id, lease_id, plan_id, event_type, actor, payload_json,
                            idempotency_key, request_sha256, created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            str(uuid.uuid4()),
                            anchor,
                            request["plan_id"] or str(rows[0]["plan_id"]),
                            "LEASE_RECONCILIATION_DRY_RUN" if dry_run else "LEASES_RECONCILED",
                            request["actor"],
                            self._json(event_payload),
                            idempotency_key,
                            fingerprint,
                            now,
                        ),
                    )
            return LeaseReconciliationSnapshot(
                db_path=str(self.path),
                dry_run=bool(dry_run),
                inspected_count=len(rows),
                stale_count=len(items),
                changed_count=0 if dry_run else len(items),
                items=items,
            )

    def audit_workforce(
        self,
        *,
        actor: str,
        dry_run: bool = True,
        stale_after_days: int = 30,
        include_protected: bool = False,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> WorkforceAuditSnapshot:
        if not str(actor).strip():
            raise ValueError("actor is required")
        authorization: dict[str, Any] = {}
        if not dry_run:
            required_authority = "A3" if include_protected else "A2"
            try:
                authorization = require_governed_authorization(
                    governance_authorization,
                    required_authority=required_authority,
                    legacy_human_approval=human_approval,
                )
            except GovernedAuthorizationError as exc:
                raise HiveTransitionError(str(exc)) from exc
        request = {
            "actor": str(actor).strip(),
            "dry_run": bool(dry_run),
            "stale_after_days": max(1, int(stale_after_days)),
            "include_protected": bool(include_protected),
            "authorization": authorization,
        }
        fingerprint = self._fingerprint(request)

        def replay(row: sqlite3.Row) -> WorkforceAuditSnapshot:
            findings = [
                WorkforceAuditFinding.model_validate(item)
                for item in json.loads(str(row["findings_json"]))
            ]
            actions = json.loads(str(row["actions_json"]))
            return WorkforceAuditSnapshot(
                db_path=str(self.path),
                audit_id=str(row["audit_id"]),
                dry_run=bool(row["dry_run"]),
                finding_count=len(findings),
                action_count=len(actions),
                findings=findings,
                acted_worker_ids=actions,
                authorization=json.loads(str(row["authorization_json"])),
            )

        with self._connect() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT * FROM hive_workforce_audits WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Workforce-audit idempotency key was reused with different content."
                        )
                    return replay(existing)
            workers = conn.execute(
                "SELECT * FROM hive_workers ORDER BY created_at, worker_id"
            ).fetchall()
            lease_rows = conn.execute(
                """
                SELECT worker_id,
                       SUM(CASE WHEN status='ACTIVE' THEN 1 ELSE 0 END) AS active_count,
                       MAX(last_heartbeat_at) AS last_heartbeat_at
                FROM hive_worker_leases GROUP BY worker_id
                """
            ).fetchall()

        lease_info = {
            str(row["worker_id"]): (
                int(row["active_count"] or 0),
                int(row["last_heartbeat_at"] or 0),
            )
            for row in lease_rows
        }
        now = self._now_ms()
        stale_ms = request["stale_after_days"] * 24 * 60 * 60 * 1000
        findings: list[WorkforceAuditFinding] = []
        fingerprints: dict[tuple[Any, ...], str] = {}
        terminal = {WorkerStage.OFFBOARDED.value, WorkerStage.REJECTED.value}
        for row in workers:
            worker_id = str(row["worker_id"])
            stage = str(row["stage"])
            if stage in terminal:
                continue
            worker_class = str(row["worker_class"])
            protected = worker_class in {
                WorkerClass.HIVE_LEADER.value,
                WorkerClass.GOVERNANCE_REVIEWER.value,
            }
            active_count, last_heartbeat = lease_info.get(worker_id, (0, 0))
            age_ms = now - int(row["updated_at"])
            label = f"{row['display_name']} {row['role']} {worker_id}".lower()
            finding: WorkforceAuditFinding | None = None
            if any(
                marker in label
                for marker in ("placeholder", "do not use", "ignore-", "ignore ")
            ):
                finding = WorkforceAuditFinding(
                    worker_id=worker_id,
                    worker_class=worker_class,
                    stage=stage,
                    severity="HIGH",
                    finding_type="PLACEHOLDER_OR_DISABLED_IDENTITY",
                    reason=(
                        "Worker identity is explicitly marked as placeholder, "
                        "ignore, or do-not-use."
                    ),
                    recommended_action="OFFBOARD",
                    protected_role=protected,
                    active_lease_count=active_count,
                    last_heartbeat_at=last_heartbeat,
                )
            elif stage == WorkerStage.REQUISITIONED.value and age_ms >= stale_ms:
                finding = WorkforceAuditFinding(
                    worker_id=worker_id,
                    worker_class=worker_class,
                    stage=stage,
                    severity="MEDIUM",
                    finding_type="ABANDONED_REQUISITION",
                    reason="Requisition remained unevaluated beyond the audit threshold.",
                    recommended_action="OFFBOARD",
                    protected_role=protected,
                    active_lease_count=active_count,
                    last_heartbeat_at=last_heartbeat,
                )
            elif stage == WorkerStage.SUSPENDED.value and age_ms >= stale_ms:
                finding = WorkforceAuditFinding(
                    worker_id=worker_id,
                    worker_class=worker_class,
                    stage=stage,
                    severity="MEDIUM",
                    finding_type="STALE_SUSPENSION",
                    reason="Suspended worker remained unresolved beyond the audit threshold.",
                    recommended_action="OFFBOARD",
                    protected_role=protected,
                    active_lease_count=active_count,
                    last_heartbeat_at=last_heartbeat,
                )
            elif (
                stage == WorkerStage.APPOINTED.value
                and active_count == 0
                and age_ms >= stale_ms
            ):
                finding = WorkforceAuditFinding(
                    worker_id=worker_id,
                    worker_class=worker_class,
                    stage=stage,
                    severity="MEDIUM",
                    finding_type="CHRONICALLY_INACTIVE_APPOINTMENT",
                    reason=(
                        "Appointed worker has no active assignment and no recent "
                        "registry activity."
                    ),
                    recommended_action="REVIEW" if protected else "OFFBOARD",
                    protected_role=protected,
                    active_lease_count=active_count,
                    last_heartbeat_at=last_heartbeat,
                )
            duplicate_key = (
                worker_class,
                str(row["role"]).strip().lower(),
                str(row["competencies_json"]),
                str(row["writable_domains_json"]),
            )
            if stage == WorkerStage.APPOINTED.value:
                canonical = fingerprints.get(duplicate_key)
                if canonical and active_count == 0 and finding is None:
                    finding = WorkforceAuditFinding(
                        worker_id=worker_id,
                        worker_class=worker_class,
                        stage=stage,
                        severity="MEDIUM",
                        finding_type="DUPLICATE_APPOINTMENT",
                        reason=(
                            "Appointment duplicates the bounded role and capability "
                            f"profile of {canonical}."
                        ),
                        recommended_action="REVIEW" if protected else "OFFBOARD",
                        protected_role=protected,
                        active_lease_count=active_count,
                        last_heartbeat_at=last_heartbeat,
                    )
                else:
                    fingerprints.setdefault(duplicate_key, worker_id)
            if finding:
                findings.append(finding)

        acted: list[str] = []
        if not dry_run:
            for finding in findings:
                if finding.recommended_action != "OFFBOARD":
                    continue
                if finding.protected_role and not include_protected:
                    continue
                snapshot = self.workforce.transition_worker(
                    worker_id=finding.worker_id,
                    target_stage=WorkerStage.OFFBOARDED.value,
                    actor=request["actor"],
                    reason=(
                        "Governed workforce audit: "
                        f"{finding.finding_type}: {finding.reason}"
                    ),
                    idempotency_key=(
                        f"audit-offboard:{fingerprint}:{finding.worker_id}"
                    ),
                )
                if (
                    snapshot.workers
                    and snapshot.workers[0].stage == WorkerStage.OFFBOARDED.value
                ):
                    acted.append(finding.worker_id)

        audit_id = f"audit-{uuid.uuid4()}"
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT * FROM hive_workforce_audits WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Workforce-audit idempotency key was reused with different content."
                        )
                    return replay(existing)
            conn.execute(
                """
                INSERT INTO hive_workforce_audits(
                    audit_id, actor, dry_run, findings_json, actions_json,
                    authorization_json, idempotency_key, request_sha256, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    audit_id,
                    request["actor"],
                    int(dry_run),
                    self._json(
                        [item.model_dump(mode="json") for item in findings]
                    ),
                    self._json(acted),
                    self._json(authorization),
                    idempotency_key,
                    fingerprint,
                    self._now_ms(),
                ),
            )
        return WorkforceAuditSnapshot(
            db_path=str(self.path),
            audit_id=audit_id,
            dry_run=bool(dry_run),
            finding_count=len(findings),
            action_count=len(acted),
            findings=findings,
            acted_worker_ids=acted,
            authorization=authorization,
        )
