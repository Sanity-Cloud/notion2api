from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterator

from pydantic import BaseModel, Field

from app.file_discovery_routing import enforce_dispatch_file_route
from app.governed_authorization import (
    GovernedAuthorizationError,
    require_governed_authorization,
)
from app.hive_external_effects import (
    EXTERNAL_IMPLEMENTATION_ID,
    get_hive_external_effect_store,
)
from app.hive_materialization import (
    DispatchStatus,
    LeaseStatus,
    MaterializationStatus,
    get_hive_materialization_store,
)
from app.hive_runtime import (
    HiveIdempotencyConflict,
    HiveNotFoundError,
    HiveTransitionError,
    default_hive_runtime_db_path,
)
from app.hive_workforce import (
    AUTHORITY_RANK,
    WorkerClass,
    WorkerStage,
    get_hive_workforce_store,
)


class AdapterStatus(str, Enum):
    ENABLED = "ENABLED"
    DISABLED = "DISABLED"


class ExecutionStatus(str, Enum):
    DENIED = "DENIED"
    CLAIMED = "CLAIMED"
    RUNNING = "RUNNING"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELLED = "CANCELLED"


TERMINAL_EXECUTION_STATUSES = {
    ExecutionStatus.DENIED.value,
    ExecutionStatus.COMPLETED.value,
    ExecutionStatus.FAILED.value,
    ExecutionStatus.TIMED_OUT.value,
    ExecutionStatus.CANCELLED.value,
}
ACTIVE_EXECUTION_STATUSES = {
    ExecutionStatus.CLAIMED.value,
    ExecutionStatus.RUNNING.value,
    ExecutionStatus.REVIEW_REQUIRED.value,
}
BLOCKED_PAYLOAD_KEYS = {
    "cmd",
    "command",
    "credential",
    "credentials",
    "env",
    "environment",
    "executable",
    "file",
    "headers",
    "password",
    "path",
    "secret",
    "shell",
    "token",
    "uri",
    "url",
}


class AdapterCancelled(RuntimeError):
    pass


@dataclass(frozen=True)
class BuiltinAdapterSpec:
    implementation_id: str
    display_name: str
    capabilities: tuple[str, ...]
    writable_domains: tuple[str, ...]
    minimum_authority: str
    max_timeout_ms: int
    max_payload_bytes: int
    requires_human_approval: bool
    requires_independent_review: bool


BUILTIN_ADAPTER_SPECS: dict[str, BuiltinAdapterSpec] = {
    "builtin.noop.v1": BuiltinAdapterSpec(
        implementation_id="builtin.noop.v1",
        display_name="Bounded No-Op",
        capabilities=("noop",),
        writable_domains=(),
        minimum_authority="A0",
        max_timeout_ms=1000,
        max_payload_bytes=4096,
        requires_human_approval=False,
        requires_independent_review=False,
    ),
    "builtin.evidence_digest.v1": BuiltinAdapterSpec(
        implementation_id="builtin.evidence_digest.v1",
        display_name="Evidence Digest",
        capabilities=("evidence_digest",),
        writable_domains=("evidence",),
        minimum_authority="A1",
        max_timeout_ms=2000,
        max_payload_bytes=32768,
        requires_human_approval=False,
        requires_independent_review=True,
    ),
    "builtin.bounded_delay.v1": BuiltinAdapterSpec(
        implementation_id="builtin.bounded_delay.v1",
        display_name="Bounded Delay",
        capabilities=("bounded_delay",),
        writable_domains=(),
        minimum_authority="A0",
        max_timeout_ms=5000,
        max_payload_bytes=4096,
        requires_human_approval=True,
        requires_independent_review=False,
    ),
    EXTERNAL_IMPLEMENTATION_ID: BuiltinAdapterSpec(
        implementation_id=EXTERNAL_IMPLEMENTATION_ID,
        display_name="Certified Sandbox Artifact",
        capabilities=("sandbox_artifact",),
        writable_domains=("external_sandbox",),
        minimum_authority="A2",
        max_timeout_ms=5000,
        max_payload_bytes=65536,
        requires_human_approval=True,
        requires_independent_review=True,
    ),
}


class HiveExecutionAdapter(BaseModel):
    adapter_id: str
    implementation_id: str
    display_name: str
    status: str
    capabilities: list[str] = Field(default_factory=list)
    writable_domains: list[str] = Field(default_factory=list)
    required_authority: str = "A0"
    max_timeout_ms: int = 1000
    max_payload_bytes: int = 4096
    requires_human_approval: bool = False
    requires_independent_review: bool = False
    registered_by: str = ""
    approved_by: str = ""
    built_in: bool = True
    created_at: int = 0
    updated_at: int = 0
    revision: int = 0


class HiveExecutionReview(BaseModel):
    review_id: str
    execution_id: str
    reviewer_worker_id: str
    actor: str
    approved: bool
    findings: dict[str, Any] = Field(default_factory=dict)
    created_at: int = 0


class HiveDispatchExecution(BaseModel):
    execution_id: str
    plan_id: str
    mission_id: str
    work_unit_id: str
    receipt_id: str
    lease_id: str
    worker_id: str
    adapter_id: str
    implementation_id: str
    requested_capability: str
    requested_writable_domains: list[str] = Field(default_factory=list)
    status: str
    actor: str
    request_payload: dict[str, Any] = Field(default_factory=dict)
    result: dict[str, Any] = Field(default_factory=dict)
    evidence: dict[str, Any] = Field(default_factory=dict)
    error_code: str = ""
    error_message: str = ""
    timeout_ms: int = 0
    attempt: int = 0
    cancellation_requested: bool = False
    review_required: bool = False
    created_at: int = 0
    started_at: int = 0
    finished_at: int = 0
    updated_at: int = 0
    revision: int = 0


class HiveAdapterSnapshot(BaseModel):
    ok: bool = True
    db_path: str = ""
    count: int = 0
    adapters: list[HiveExecutionAdapter] = Field(default_factory=list)
    error: str = ""


class HiveExecutionSnapshot(BaseModel):
    ok: bool = True
    found: bool = True
    db_path: str = ""
    count: int = 0
    executions: list[HiveDispatchExecution] = Field(default_factory=list)
    reviews: list[HiveExecutionReview] = Field(default_factory=list)
    error: str = ""


class HiveExecutionDispatcherStore:
    """Fail-closed adapter registry and guarded dispatch execution ledger."""

    def __init__(self, path: str | Path):
        self.path = Path(path).expanduser().resolve()
        self._schema_lock = threading.RLock()
        self.materialization = get_hive_materialization_store(self.path)
        self.workforce = get_hive_workforce_store(self.path)
        self._ensure_schema()
        self._seed_builtin_adapters()

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
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
            {str(item).strip().lower() for item in (values or []) if str(item).strip()}
        )

    @staticmethod
    def _authority_allows(ceiling: str, required: str) -> bool:
        return AUTHORITY_RANK.get(str(ceiling).upper(), -1) >= AUTHORITY_RANK.get(
            str(required).upper(), 99
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
                    CREATE TABLE IF NOT EXISTS hive_execution_adapters (
                        adapter_id TEXT PRIMARY KEY,
                        implementation_id TEXT NOT NULL,
                        display_name TEXT NOT NULL,
                        status TEXT NOT NULL,
                        capabilities_json TEXT NOT NULL DEFAULT '[]',
                        writable_domains_json TEXT NOT NULL DEFAULT '[]',
                        required_authority TEXT NOT NULL,
                        max_timeout_ms INTEGER NOT NULL,
                        max_payload_bytes INTEGER NOT NULL,
                        requires_human_approval INTEGER NOT NULL DEFAULT 0,
                        requires_independent_review INTEGER NOT NULL DEFAULT 0,
                        registered_by TEXT NOT NULL,
                        approved_by TEXT NOT NULL DEFAULT '',
                        built_in INTEGER NOT NULL DEFAULT 1,
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_execution_adapter_events (
                        event_id TEXT PRIMARY KEY,
                        adapter_id TEXT NOT NULL REFERENCES hive_execution_adapters(adapter_id)
                            ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_dispatch_executions (
                        execution_id TEXT PRIMARY KEY,
                        plan_id TEXT NOT NULL REFERENCES hive_invocation_materializations(plan_id)
                            ON DELETE CASCADE,
                        mission_id TEXT NOT NULL,
                        work_unit_id TEXT NOT NULL,
                        receipt_id TEXT NOT NULL,
                        lease_id TEXT NOT NULL,
                        worker_id TEXT NOT NULL REFERENCES hive_workers(worker_id),
                        adapter_id TEXT NOT NULL REFERENCES hive_execution_adapters(adapter_id),
                        implementation_id TEXT NOT NULL,
                        requested_capability TEXT NOT NULL,
                        requested_writable_domains_json TEXT NOT NULL DEFAULT '[]',
                        request_payload_json TEXT NOT NULL DEFAULT '{}',
                        status TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        result_json TEXT NOT NULL DEFAULT '{}',
                        evidence_json TEXT NOT NULL DEFAULT '{}',
                        error_code TEXT NOT NULL DEFAULT '',
                        error_message TEXT NOT NULL DEFAULT '',
                        timeout_ms INTEGER NOT NULL,
                        attempt INTEGER NOT NULL,
                        cancellation_requested INTEGER NOT NULL DEFAULT 0,
                        review_required INTEGER NOT NULL DEFAULT 0,
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        started_at INTEGER NOT NULL DEFAULT 0,
                        finished_at INTEGER NOT NULL DEFAULT 0,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_execution_reviews (
                        review_id TEXT PRIMARY KEY,
                        execution_id TEXT NOT NULL REFERENCES hive_dispatch_executions(execution_id)
                            ON DELETE CASCADE,
                        reviewer_worker_id TEXT NOT NULL REFERENCES hive_workers(worker_id),
                        actor TEXT NOT NULL,
                        approved INTEGER NOT NULL,
                        findings_json TEXT NOT NULL DEFAULT '{}',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_execution_events (
                        event_id TEXT PRIMARY KEY,
                        execution_id TEXT NOT NULL REFERENCES hive_dispatch_executions(execution_id)
                            ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_execution_adapters_status
                        ON hive_execution_adapters(status, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_dispatch_executions_lane
                        ON hive_dispatch_executions(plan_id, work_unit_id, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_dispatch_executions_status
                        ON hive_dispatch_executions(status, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_execution_reviews_execution
                        ON hive_execution_reviews(execution_id, created_at DESC);
                    """
                )

    def _seed_builtin_adapters(self) -> None:
        now = self._now_ms()
        with self._write() as conn:
            for adapter_id, spec in BUILTIN_ADAPTER_SPECS.items():
                if conn.execute(
                    "SELECT 1 FROM hive_execution_adapters WHERE adapter_id = ?",
                    (adapter_id,),
                ).fetchone():
                    continue
                request = {
                    "adapter_id": adapter_id,
                    "implementation_id": spec.implementation_id,
                    "display_name": spec.display_name,
                    "status": AdapterStatus.DISABLED.value,
                    "capabilities": list(spec.capabilities),
                    "writable_domains": list(spec.writable_domains),
                    "required_authority": spec.minimum_authority,
                    "max_timeout_ms": spec.max_timeout_ms,
                    "max_payload_bytes": spec.max_payload_bytes,
                    "requires_human_approval": spec.requires_human_approval,
                    "requires_independent_review": spec.requires_independent_review,
                    "registered_by": "sanitycloud-built-in",
                    "approved_by": "",
                    "built_in": True,
                }
                fingerprint = self._fingerprint(request)
                conn.execute(
                    """
                    INSERT INTO hive_execution_adapters(
                        adapter_id, implementation_id, display_name, status,
                        capabilities_json, writable_domains_json, required_authority,
                        max_timeout_ms, max_payload_bytes, requires_human_approval,
                        requires_independent_review, registered_by, approved_by,
                        built_in, request_sha256, created_at, updated_at, revision
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        adapter_id,
                        spec.implementation_id,
                        spec.display_name,
                        AdapterStatus.DISABLED.value,
                        self._json(list(spec.capabilities)),
                        self._json(list(spec.writable_domains)),
                        spec.minimum_authority,
                        spec.max_timeout_ms,
                        spec.max_payload_bytes,
                        int(spec.requires_human_approval),
                        int(spec.requires_independent_review),
                        "sanitycloud-built-in",
                        "",
                        1,
                        fingerprint,
                        now,
                        now,
                    ),
                )
                self._adapter_event(
                    conn,
                    adapter_id=adapter_id,
                    event_type="BUILTIN_ADAPTER_SEEDED",
                    actor="sanitycloud-built-in",
                    payload={"status": AdapterStatus.DISABLED.value},
                    fingerprint=fingerprint,
                )

    def _adapter_event(
        self,
        conn: sqlite3.Connection,
        *,
        adapter_id: str,
        event_type: str,
        actor: str,
        payload: dict[str, Any],
        fingerprint: str,
        idempotency_key: str | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO hive_execution_adapter_events(
                event_id, adapter_id, event_type, actor, payload_json,
                idempotency_key, request_sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                adapter_id,
                event_type,
                actor,
                self._json(payload),
                idempotency_key,
                fingerprint,
                self._now_ms(),
            ),
        )

    def _execution_event(
        self,
        conn: sqlite3.Connection,
        *,
        execution_id: str,
        event_type: str,
        actor: str,
        payload: dict[str, Any],
        fingerprint: str = "",
        idempotency_key: str | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO hive_execution_events(
                event_id, execution_id, event_type, actor, payload_json,
                idempotency_key, request_sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()),
                execution_id,
                event_type,
                actor,
                self._json(payload),
                idempotency_key,
                fingerprint or self._fingerprint(payload),
                self._now_ms(),
            ),
        )

    @staticmethod
    def _adapter_from_row(row: sqlite3.Row) -> HiveExecutionAdapter:
        return HiveExecutionAdapter(
            adapter_id=str(row["adapter_id"]),
            implementation_id=str(row["implementation_id"]),
            display_name=str(row["display_name"]),
            status=str(row["status"]),
            capabilities=json.loads(str(row["capabilities_json"])),
            writable_domains=json.loads(str(row["writable_domains_json"])),
            required_authority=str(row["required_authority"]),
            max_timeout_ms=int(row["max_timeout_ms"]),
            max_payload_bytes=int(row["max_payload_bytes"]),
            requires_human_approval=bool(row["requires_human_approval"]),
            requires_independent_review=bool(row["requires_independent_review"]),
            registered_by=str(row["registered_by"]),
            approved_by=str(row["approved_by"]),
            built_in=bool(row["built_in"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    @staticmethod
    def _execution_from_row(row: sqlite3.Row) -> HiveDispatchExecution:
        return HiveDispatchExecution(
            execution_id=str(row["execution_id"]),
            plan_id=str(row["plan_id"]),
            mission_id=str(row["mission_id"]),
            work_unit_id=str(row["work_unit_id"]),
            receipt_id=str(row["receipt_id"]),
            lease_id=str(row["lease_id"]),
            worker_id=str(row["worker_id"]),
            adapter_id=str(row["adapter_id"]),
            implementation_id=str(row["implementation_id"]),
            requested_capability=str(row["requested_capability"]),
            requested_writable_domains=json.loads(
                str(row["requested_writable_domains_json"])
            ),
            status=str(row["status"]),
            actor=str(row["actor"]),
            request_payload=json.loads(str(row["request_payload_json"])),
            result=json.loads(str(row["result_json"])),
            evidence=json.loads(str(row["evidence_json"])),
            error_code=str(row["error_code"]),
            error_message=str(row["error_message"]),
            timeout_ms=int(row["timeout_ms"]),
            attempt=int(row["attempt"]),
            cancellation_requested=bool(row["cancellation_requested"]),
            review_required=bool(row["review_required"]),
            created_at=int(row["created_at"]),
            started_at=int(row["started_at"]),
            finished_at=int(row["finished_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    @staticmethod
    def _review_from_row(row: sqlite3.Row) -> HiveExecutionReview:
        return HiveExecutionReview(
            review_id=str(row["review_id"]),
            execution_id=str(row["execution_id"]),
            reviewer_worker_id=str(row["reviewer_worker_id"]),
            actor=str(row["actor"]),
            approved=bool(row["approved"]),
            findings=json.loads(str(row["findings_json"])),
            created_at=int(row["created_at"]),
        )

    def _adapter_snapshot(
        self,
        conn: sqlite3.Connection,
        *,
        adapter_id: str = "",
        status: str = "",
        limit: int = 100,
    ) -> HiveAdapterSnapshot:
        clauses: list[str] = []
        params: list[Any] = []
        if adapter_id:
            clauses.append("adapter_id = ?")
            params.append(adapter_id)
        if status:
            clauses.append("status = ?")
            params.append(AdapterStatus(str(status).strip().upper()).value)
        query = "SELECT * FROM hive_execution_adapters"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, adapter_id LIMIT ?"
        params.append(max(1, min(int(limit), 1000)))
        rows = conn.execute(query, tuple(params)).fetchall()
        adapters = [self._adapter_from_row(row) for row in rows]
        return HiveAdapterSnapshot(
            db_path=str(self.path),
            count=len(adapters),
            adapters=adapters,
        )

    def _execution_snapshot(
        self,
        conn: sqlite3.Connection,
        *,
        execution_id: str = "",
        plan_id: str = "",
        work_unit_id: str = "",
        limit: int = 100,
    ) -> HiveExecutionSnapshot:
        clauses: list[str] = []
        params: list[Any] = []
        if execution_id:
            clauses.append("execution_id = ?")
            params.append(execution_id)
        if plan_id:
            clauses.append("plan_id = ?")
            params.append(plan_id)
        if work_unit_id:
            clauses.append("work_unit_id = ?")
            params.append(work_unit_id)
        query = "SELECT * FROM hive_dispatch_executions"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC, execution_id LIMIT ?"
        params.append(max(1, min(int(limit), 1000)))
        rows = conn.execute(query, tuple(params)).fetchall()
        executions = [self._execution_from_row(row) for row in rows]
        reviews: list[HiveExecutionReview] = []
        if executions:
            placeholders = ",".join("?" for _ in executions)
            review_rows = conn.execute(
                f"""
                SELECT * FROM hive_execution_reviews
                WHERE execution_id IN ({placeholders})
                ORDER BY created_at DESC, review_id
                """,
                tuple(item.execution_id for item in executions),
            ).fetchall()
            reviews = [self._review_from_row(row) for row in review_rows]
        return HiveExecutionSnapshot(
            found=bool(executions),
            db_path=str(self.path),
            count=len(executions),
            executions=executions,
            reviews=reviews,
        )

    def list_adapters(
        self,
        *,
        adapter_id: str = "",
        status: str = "",
        limit: int = 100,
    ) -> HiveAdapterSnapshot:
        with self._connect() as conn:
            return self._adapter_snapshot(
                conn,
                adapter_id=str(adapter_id or "").strip(),
                status=str(status or "").strip(),
                limit=limit,
            )

    def get_execution(
        self,
        *,
        execution_id: str = "",
        plan_id: str = "",
        work_unit_id: str = "",
        limit: int = 100,
    ) -> HiveExecutionSnapshot:
        if not any((execution_id, plan_id, work_unit_id)):
            limit = min(limit, 100)
        with self._connect() as conn:
            return self._execution_snapshot(
                conn,
                execution_id=str(execution_id or "").strip(),
                plan_id=str(plan_id or "").strip(),
                work_unit_id=str(work_unit_id or "").strip(),
                limit=limit,
            )

    def upsert_adapter(
        self,
        *,
        adapter_id: str,
        implementation_id: str,
        display_name: str,
        capabilities: list[str] | None,
        writable_domains: list[str] | None,
        required_authority: str,
        max_timeout_ms: int,
        max_payload_bytes: int,
        requires_human_approval: bool,
        requires_independent_review: bool,
        enabled: bool,
        actor: str,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        expected_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> HiveAdapterSnapshot:
        adapter_key = self._required(adapter_id, "adapter_id")
        implementation_key = self._required(implementation_id, "implementation_id")
        spec = BUILTIN_ADAPTER_SPECS.get(implementation_key)
        if not spec:
            raise HiveTransitionError(
                "Unknown adapter implementation. Only compiled SanityCloud built-ins may be registered."
            )
        requested_capabilities = self._normalized_list(capabilities)
        requested_domains = self._normalized_list(writable_domains)
        if not requested_capabilities:
            requested_capabilities = list(spec.capabilities)
        if not set(requested_capabilities).issubset(set(spec.capabilities)):
            raise HiveTransitionError(
                "Adapter capabilities exceed the compiled implementation contract."
            )
        if not set(requested_domains).issubset(set(spec.writable_domains)):
            raise HiveTransitionError(
                "Adapter writable domains exceed the compiled implementation contract."
            )
        authority = str(required_authority or spec.minimum_authority).strip().upper()
        if authority not in AUTHORITY_RANK:
            raise ValueError(f"Unsupported authority ceiling: {authority}")
        if not self._authority_allows(authority, spec.minimum_authority):
            raise HiveTransitionError(
                f"Adapter authority cannot be lower than {spec.minimum_authority}."
            )
        timeout = max(10, int(max_timeout_ms or spec.max_timeout_ms))
        payload_limit = max(256, int(max_payload_bytes or spec.max_payload_bytes))
        if timeout > spec.max_timeout_ms:
            raise HiveTransitionError(
                "Adapter timeout exceeds the compiled implementation limit."
            )
        if payload_limit > spec.max_payload_bytes:
            raise HiveTransitionError(
                "Adapter payload limit exceeds the compiled implementation limit."
            )
        authorization_receipt: dict[str, Any] = {}
        if enabled:
            try:
                authorization_receipt = require_governed_authorization(
                    governance_authorization,
                    required_authority=authority,
                    legacy_human_approval=human_approval,
                )
            except GovernedAuthorizationError as exc:
                raise HiveTransitionError(str(exc)) from exc
        effective_human_approval = bool(
            spec.requires_human_approval or requires_human_approval
        )
        effective_independent_review = bool(
            spec.requires_independent_review or requires_independent_review
        )
        request = {
            "adapter_id": adapter_key,
            "implementation_id": implementation_key,
            "display_name": self._required(display_name, "display_name"),
            "status": AdapterStatus.ENABLED.value
            if enabled
            else AdapterStatus.DISABLED.value,
            "capabilities": requested_capabilities,
            "writable_domains": requested_domains,
            "required_authority": authority,
            "max_timeout_ms": timeout,
            "max_payload_bytes": payload_limit,
            "requires_human_approval": effective_human_approval,
            "requires_independent_review": effective_independent_review,
            "actor": self._required(actor, "actor"),
            "human_approval": bool(human_approval),
            "governance_authorization": authorization_receipt,
            "expected_revision": expected_revision,
        }
        fingerprint = self._fingerprint(request)
        now = self._now_ms()
        with self._write() as conn:
            if idempotency_key:
                event = conn.execute(
                    """
                    SELECT adapter_id, request_sha256
                    FROM hive_execution_adapter_events
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if event:
                    if str(event["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Adapter idempotency key was already used with different content."
                        )
                    return self._adapter_snapshot(
                        conn, adapter_id=str(event["adapter_id"])
                    )
            row = conn.execute(
                "SELECT revision FROM hive_execution_adapters WHERE adapter_id = ?",
                (adapter_key,),
            ).fetchone()
            if (
                row
                and expected_revision is not None
                and int(row["revision"]) != int(expected_revision)
            ):
                raise HiveTransitionError(
                    f"Stale adapter revision: expected {expected_revision}, current {int(row['revision'])}."
                )
            if row:
                conn.execute(
                    """
                    UPDATE hive_execution_adapters
                    SET implementation_id = ?, display_name = ?, status = ?,
                        capabilities_json = ?, writable_domains_json = ?,
                        required_authority = ?, max_timeout_ms = ?, max_payload_bytes = ?,
                        requires_human_approval = ?, requires_independent_review = ?,
                        registered_by = ?, approved_by = ?, request_sha256 = ?,
                        updated_at = ?, revision = revision + 1
                    WHERE adapter_id = ?
                    """,
                    (
                        implementation_key,
                        request["display_name"],
                        request["status"],
                        self._json(requested_capabilities),
                        self._json(requested_domains),
                        authority,
                        timeout,
                        payload_limit,
                        int(effective_human_approval),
                        int(effective_independent_review),
                        request["actor"],
                        request["actor"] if enabled else "",
                        fingerprint,
                        now,
                        adapter_key,
                    ),
                )
                event_type = "ADAPTER_UPDATED"
            else:
                conn.execute(
                    """
                    INSERT INTO hive_execution_adapters(
                        adapter_id, implementation_id, display_name, status,
                        capabilities_json, writable_domains_json, required_authority,
                        max_timeout_ms, max_payload_bytes, requires_human_approval,
                        requires_independent_review, registered_by, approved_by,
                        built_in, idempotency_key, request_sha256,
                        created_at, updated_at, revision
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 1)
                    """,
                    (
                        adapter_key,
                        implementation_key,
                        request["display_name"],
                        request["status"],
                        self._json(requested_capabilities),
                        self._json(requested_domains),
                        authority,
                        timeout,
                        payload_limit,
                        int(effective_human_approval),
                        int(effective_independent_review),
                        request["actor"],
                        request["actor"] if enabled else "",
                        idempotency_key,
                        fingerprint,
                        now,
                        now,
                    ),
                )
                event_type = "ADAPTER_REGISTERED"
            self._adapter_event(
                conn,
                adapter_id=adapter_key,
                event_type=event_type,
                actor=request["actor"],
                payload=request,
                fingerprint=fingerprint,
                idempotency_key=idempotency_key,
            )
            return self._adapter_snapshot(conn, adapter_id=adapter_key)

    def _validate_payload_keys(self, value: Any, *, path: str = "payload") -> None:
        if isinstance(value, dict):
            for key, item in value.items():
                clean = str(key).strip().lower()
                if clean in BLOCKED_PAYLOAD_KEYS:
                    raise HiveTransitionError(
                        f"Blocked execution payload key at {path}.{key}: {clean}"
                    )
                self._validate_payload_keys(item, path=f"{path}.{key}")
        elif isinstance(value, list):
            for index, item in enumerate(value):
                self._validate_payload_keys(item, path=f"{path}[{index}]")

    def _canonical_payload(
        self, payload: dict[str, Any], max_bytes: int
    ) -> tuple[str, str]:
        if not isinstance(payload, dict):
            raise ValueError("payload must be a JSON object")
        self._validate_payload_keys(payload)
        try:
            encoded = self._json(payload).encode("utf-8")
        except (TypeError, ValueError) as exc:
            raise ValueError("payload must be JSON serializable") from exc
        if len(encoded) > int(max_bytes):
            raise HiveTransitionError(
                f"Execution payload exceeds adapter limit of {int(max_bytes)} bytes."
            )
        return encoded.decode("utf-8"), hashlib.sha256(encoded).hexdigest()

    def _adapter_row(self, conn: sqlite3.Connection, adapter_id: str) -> sqlite3.Row:
        row = conn.execute(
            "SELECT * FROM hive_execution_adapters WHERE adapter_id = ?",
            (adapter_id,),
        ).fetchone()
        if not row:
            raise HiveNotFoundError(f"Execution adapter not found: {adapter_id}")
        return row

    def _lane_context(
        self,
        *,
        plan_id: str,
        work_unit_id: str,
        adapter: HiveExecutionAdapter,
        requested_capability: str,
        requested_domains: list[str],
        human_approval: bool,
        governance_authorization: dict[str, Any] | None,
    ) -> tuple[Any, Any, Any, str]:
        materialized = self.materialization.get_materialization(plan_id=plan_id)
        if not materialized.found:
            raise HiveNotFoundError(f"Materialization plan not found: {plan_id}")
        if materialized.status != MaterializationStatus.MATERIALIZED.value:
            raise HiveTransitionError(
                "Dispatch execution requires a MATERIALIZED plan; "
                f"current status is {materialized.status}."
            )
        receipts = {item.work_unit_id: item for item in materialized.dispatch_receipts}
        leases = {item.work_unit_id: item for item in materialized.leases}
        receipt = receipts.get(work_unit_id)
        lease = leases.get(work_unit_id)
        if not receipt:
            raise HiveNotFoundError(f"Dispatch receipt not found: {work_unit_id}")
        if not lease:
            raise HiveNotFoundError(f"Worker lease not found: {work_unit_id}")
        if receipt.status != DispatchStatus.READY.value:
            raise HiveTransitionError(
                "A new adapter execution requires a READY dispatch receipt; "
                f"current status is {receipt.status}."
            )
        if lease.status != LeaseStatus.ACTIVE.value:
            raise HiveTransitionError(
                "Adapter execution requires an ACTIVE worker lease; "
                f"current status is {lease.status}."
            )
        if lease.liveness_status in {"EXPIRED", "STALE", "OFFLINE"}:
            raise HiveTransitionError(
                "Adapter execution requires a fresh worker lease; "
                f"current liveness is {lease.liveness_status}."
            )
        workers = {
            item.worker_id: item
            for item in self.workforce.list_workers(limit=1000).workers
        }
        worker = workers.get(lease.worker_id)
        if not worker:
            raise HiveNotFoundError(f"Worker not found: {lease.worker_id}")
        if worker.stage != WorkerStage.APPOINTED.value:
            raise HiveTransitionError(
                "Adapter execution is restricted to APPOINTED workers; "
                f"current stage is {worker.stage}."
            )
        if adapter.status != AdapterStatus.ENABLED.value:
            raise HiveTransitionError(
                f"Execution adapter is disabled: {adapter.adapter_id}"
            )
        if requested_capability not in adapter.capabilities:
            raise HiveTransitionError(
                f"Adapter does not allow capability: {requested_capability}"
            )
        if not set(requested_domains).issubset(set(adapter.writable_domains)):
            raise HiveTransitionError(
                "Requested writable domains exceed the adapter allowlist."
            )
        if not set(requested_domains).issubset(set(lease.writable_domains)):
            raise HiveTransitionError(
                "Requested writable domains exceed the worker lease."
            )
        if not self._authority_allows(
            lease.authority_ceiling, adapter.required_authority
        ):
            raise HiveTransitionError(
                "Worker lease authority is below the adapter requirement."
            )
        if adapter.requires_human_approval:
            try:
                require_governed_authorization(
                    governance_authorization,
                    required_authority=adapter.required_authority,
                    legacy_human_approval=human_approval,
                )
            except GovernedAuthorizationError as exc:
                raise HiveTransitionError(str(exc)) from exc
        return materialized, receipt, lease, worker

    def _insert_execution(
        self,
        conn: sqlite3.Connection,
        *,
        execution_id: str,
        request: dict[str, Any],
        materialized: Any,
        receipt: Any,
        lease: Any,
        adapter: HiveExecutionAdapter,
        status: str,
        fingerprint: str,
        idempotency_key: str | None,
        error_code: str = "",
        error_message: str = "",
    ) -> None:
        now = self._now_ms()
        conn.execute(
            """
            INSERT INTO hive_dispatch_executions(
                execution_id, plan_id, mission_id, work_unit_id,
                receipt_id, lease_id, worker_id, adapter_id,
                implementation_id, requested_capability,
                requested_writable_domains_json, request_payload_json,
                status, actor, result_json, evidence_json,
                error_code, error_message, timeout_ms, attempt,
                cancellation_requested, review_required,
                idempotency_key, request_sha256,
                created_at, started_at, finished_at, updated_at, revision
            ) VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', '{}',
                ?, ?, ?, 1, 0, ?, ?, ?, ?, ?, ?, ?, 1
            )
            """,
            (
                execution_id,
                request["plan_id"],
                materialized.mission_id,
                request["work_unit_id"],
                receipt.receipt_id,
                lease.lease_id,
                lease.worker_id,
                adapter.adapter_id,
                adapter.implementation_id,
                request["requested_capability"],
                self._json(request["requested_writable_domains"]),
                self._json(request["payload"]),
                status,
                request["actor"],
                error_code,
                error_message,
                request["timeout_ms"],
                int(request["review_required"]),
                idempotency_key,
                fingerprint,
                now,
                now if status == ExecutionStatus.RUNNING.value else 0,
                now if status in TERMINAL_EXECUTION_STATUSES else 0,
                now,
            ),
        )
        self._execution_event(
            conn,
            execution_id=execution_id,
            event_type=(
                "EXECUTION_DENIED"
                if status == ExecutionStatus.DENIED.value
                else "EXECUTION_CLAIMED"
            ),
            actor=request["actor"],
            payload={
                "plan_id": request["plan_id"],
                "work_unit_id": request["work_unit_id"],
                "adapter_id": adapter.adapter_id,
                "status": status,
                "error_code": error_code,
                "error_message": error_message,
            },
            fingerprint=fingerprint,
        )

    def _record_denied_execution(
        self,
        *,
        execution_id: str,
        request: dict[str, Any],
        adapter: HiveExecutionAdapter,
        fingerprint: str,
        idempotency_key: str | None,
        error_message: str,
    ) -> HiveExecutionSnapshot:
        materialized = self.materialization.get_materialization(
            plan_id=request["plan_id"]
        )
        if not materialized.found:
            raise HiveNotFoundError(
                f"Materialization plan not found: {request['plan_id']}"
            )
        receipt = next(
            (
                item
                for item in materialized.dispatch_receipts
                if item.work_unit_id == request["work_unit_id"]
            ),
            None,
        )
        lease = next(
            (
                item
                for item in materialized.leases
                if item.work_unit_id == request["work_unit_id"]
            ),
            None,
        )
        if not receipt or not lease:
            raise HiveNotFoundError(
                f"Materialized lane not found: {request['work_unit_id']}"
            )
        with self._write() as conn:
            self._insert_execution(
                conn,
                execution_id=execution_id,
                request=request,
                materialized=materialized,
                receipt=receipt,
                lease=lease,
                adapter=adapter,
                status=ExecutionStatus.DENIED.value,
                fingerprint=fingerprint,
                idempotency_key=idempotency_key,
                error_code="POLICY_DENIED",
                error_message=error_message,
            )
            return self._execution_snapshot(conn, execution_id=execution_id)

    def _active_lane_execution(
        self,
        conn: sqlite3.Connection,
        *,
        plan_id: str,
        work_unit_id: str,
    ) -> sqlite3.Row | None:
        placeholders = ",".join("?" for _ in ACTIVE_EXECUTION_STATUSES)
        return conn.execute(
            f"""
            SELECT execution_id, status
            FROM hive_dispatch_executions
            WHERE plan_id = ? AND work_unit_id = ?
              AND status IN ({placeholders})
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (plan_id, work_unit_id, *sorted(ACTIVE_EXECUTION_STATUSES)),
        ).fetchone()

    def _cancellation_requested(self, execution_id: str) -> bool:
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT cancellation_requested
                FROM hive_dispatch_executions
                WHERE execution_id = ?
                """,
                (execution_id,),
            ).fetchone()
            return bool(row and row["cancellation_requested"])

    @staticmethod
    def _check_allowed_keys(
        payload: dict[str, Any],
        allowed: set[str],
        implementation_id: str,
    ) -> None:
        extra = sorted(set(payload) - allowed)
        if extra:
            raise HiveTransitionError(
                f"Unsupported payload keys for {implementation_id}: {', '.join(extra)}"
            )

    def _run_noop(
        self,
        payload: dict[str, Any],
        cancelled: Callable[[], bool],
    ) -> dict[str, Any]:
        self._check_allowed_keys(payload, {"message", "metadata"}, "builtin.noop.v1")
        if cancelled():
            raise AdapterCancelled("Execution cancellation was requested.")
        message = str(payload.get("message") or "")
        if len(message) > 500:
            raise HiveTransitionError("No-op message is limited to 500 characters.")
        metadata = payload.get("metadata") or {}
        if not isinstance(metadata, dict):
            raise HiveTransitionError("No-op metadata must be a JSON object.")
        return {
            "adapter": "builtin.noop.v1",
            "message": message,
            "metadata_sha256": self._fingerprint(metadata),
            "performed_external_effect": False,
        }

    def _run_evidence_digest(
        self,
        payload: dict[str, Any],
        cancelled: Callable[[], bool],
    ) -> dict[str, Any]:
        self._check_allowed_keys(
            payload,
            {"items", "label"},
            "builtin.evidence_digest.v1",
        )
        if cancelled():
            raise AdapterCancelled("Execution cancellation was requested.")
        items = payload.get("items")
        if not isinstance(items, list):
            raise HiveTransitionError("Evidence digest items must be a JSON array.")
        if len(items) > 100:
            raise HiveTransitionError("Evidence digest is limited to 100 items.")
        label = str(payload.get("label") or "")
        if len(label) > 200:
            raise HiveTransitionError(
                "Evidence digest label is limited to 200 characters."
            )
        canonical = self._json(items).encode("utf-8")
        if cancelled():
            raise AdapterCancelled("Execution cancellation was requested.")
        return {
            "adapter": "builtin.evidence_digest.v1",
            "label": label,
            "item_count": len(items),
            "digest_sha256": hashlib.sha256(canonical).hexdigest(),
            "performed_external_effect": False,
        }

    def _run_bounded_delay(
        self,
        payload: dict[str, Any],
        cancelled: Callable[[], bool],
    ) -> dict[str, Any]:
        self._check_allowed_keys(
            payload,
            {"delay_ms", "label"},
            "builtin.bounded_delay.v1",
        )
        delay_ms = int(payload.get("delay_ms") or 0)
        if delay_ms < 0 or delay_ms > 5000:
            raise HiveTransitionError(
                "Bounded delay must be between 0 and 5000 milliseconds."
            )
        label = str(payload.get("label") or "")
        if len(label) > 200:
            raise HiveTransitionError(
                "Bounded delay label is limited to 200 characters."
            )
        deadline = time.monotonic() + (delay_ms / 1000)
        while time.monotonic() < deadline:
            if cancelled():
                raise AdapterCancelled("Execution cancellation was requested.")
            time.sleep(min(0.025, max(0.0, deadline - time.monotonic())))
        if cancelled():
            raise AdapterCancelled("Execution cancellation was requested.")
        return {
            "adapter": "builtin.bounded_delay.v1",
            "label": label,
            "delay_ms": delay_ms,
            "performed_external_effect": False,
        }

    def _run_adapter(
        self,
        execution_id: str,
        actor: str,
        implementation_id: str,
        payload: dict[str, Any],
        cancelled: Callable[[], bool],
    ) -> dict[str, Any]:
        if implementation_id == EXTERNAL_IMPLEMENTATION_ID:
            return get_hive_external_effect_store(self.path).execute_sandbox_artifact(
                execution_id=execution_id,
                payload=payload,
                actor=actor,
                cancelled=cancelled,
            )
        runners: dict[
            str,
            Callable[[dict[str, Any], Callable[[], bool]], dict[str, Any]],
        ] = {
            "builtin.noop.v1": self._run_noop,
            "builtin.evidence_digest.v1": self._run_evidence_digest,
            "builtin.bounded_delay.v1": self._run_bounded_delay,
        }
        runner = runners.get(implementation_id)
        if not runner:
            raise HiveTransitionError(
                f"Compiled adapter implementation is unavailable: {implementation_id}"
            )
        return runner(payload, cancelled)

    def _update_execution(
        self,
        conn: sqlite3.Connection,
        *,
        execution_id: str,
        status: str,
        actor: str,
        result: dict[str, Any] | None = None,
        evidence: dict[str, Any] | None = None,
        error_code: str = "",
        error_message: str = "",
        started: bool = False,
        finished: bool = False,
        cancellation_requested: bool | None = None,
        increment_attempt: bool = False,
    ) -> None:
        now = self._now_ms()
        fields = [
            "status = ?",
            "actor = ?",
            "result_json = ?",
            "evidence_json = ?",
            "error_code = ?",
            "error_message = ?",
            "updated_at = ?",
            "revision = revision + 1",
        ]
        params: list[Any] = [
            status,
            actor,
            self._json(result or {}),
            self._json(evidence or {}),
            error_code,
            error_message,
            now,
        ]
        if started:
            fields.append("started_at = ?")
            params.append(now)
        if finished:
            fields.append("finished_at = ?")
            params.append(now)
        if cancellation_requested is not None:
            fields.append("cancellation_requested = ?")
            params.append(int(cancellation_requested))
        if increment_attempt:
            fields.append("attempt = attempt + 1")
        params.append(execution_id)
        conn.execute(
            f"UPDATE hive_dispatch_executions SET {', '.join(fields)} WHERE execution_id = ?",
            tuple(params),
        )

    def _phase2_receipt(
        self,
        *,
        execution_id: str,
        plan_id: str,
        work_unit_id: str,
        status: str,
        actor: str,
        evidence: dict[str, Any],
    ) -> None:
        self.materialization.record_dispatch_receipt(
            plan_id=plan_id,
            work_unit_id=work_unit_id,
            status=status,
            actor=actor,
            evidence=evidence,
            idempotency_key=f"phase3:{execution_id}:{status.lower()}",
        )

    def _reconcile_execution_receipt(self, execution: HiveDispatchExecution) -> None:
        target = {
            ExecutionStatus.COMPLETED.value: DispatchStatus.COMPLETED.value,
            ExecutionStatus.FAILED.value: DispatchStatus.FAILED.value,
            ExecutionStatus.TIMED_OUT.value: DispatchStatus.FAILED.value,
            ExecutionStatus.CANCELLED.value: DispatchStatus.CANCELLED.value,
        }.get(execution.status)
        if not target:
            return
        materialized = self.materialization.get_materialization(
            plan_id=execution.plan_id
        )
        receipt = next(
            (
                item
                for item in materialized.dispatch_receipts
                if item.work_unit_id == execution.work_unit_id
            ),
            None,
        )
        if not receipt or receipt.status == target:
            return
        if receipt.status not in {
            DispatchStatus.READY.value,
            DispatchStatus.ACKNOWLEDGED.value,
        }:
            raise HiveTransitionError(
                "Execution receipt cannot be reconciled from Phase 2 status "
                f"{receipt.status} to {target}."
            )
        self._phase2_receipt(
            execution_id=execution.execution_id,
            plan_id=execution.plan_id,
            work_unit_id=execution.work_unit_id,
            status=target,
            actor=execution.actor,
            evidence={
                "phase3_execution_id": execution.execution_id,
                "adapter_id": execution.adapter_id,
                "execution_status": execution.status,
                "result": execution.result,
                "evidence": execution.evidence,
                "error_code": execution.error_code,
                "error_message": execution.error_message,
            },
        )

    def execute_dispatch(
        self,
        *,
        plan_id: str,
        work_unit_id: str,
        adapter_id: str,
        requested_capability: str,
        payload: dict[str, Any],
        requested_writable_domains: list[str] | None,
        timeout_ms: int,
        actor: str,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        execution_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> HiveExecutionSnapshot:
        adapter_key = self._required(adapter_id, "adapter_id")
        with self._connect() as conn:
            adapter = self._adapter_from_row(self._adapter_row(conn, adapter_key))
        capability = self._required(
            requested_capability, "requested_capability"
        ).lower()
        domains = self._normalized_list(requested_writable_domains)
        timeout = max(10, int(timeout_ms or adapter.max_timeout_ms))
        payload_json, payload_sha = self._canonical_payload(
            payload, adapter.max_payload_bytes
        )
        stable = (
            hashlib.sha256(idempotency_key.encode("utf-8")).hexdigest()[:20]
            if idempotency_key
            else ""
        )
        execution_key = self._required(
            execution_id
            or (f"execution-{stable}" if stable else f"execution-{uuid.uuid4()}"),
            "execution_id",
        )
        plan_key = self._required(plan_id, "plan_id")
        with self._connect() as conn:
            plan_row = conn.execute(
                """
                SELECT request_json FROM hive_invocation_materializations
                WHERE plan_id = ?
                """,
                (plan_key,),
            ).fetchone()
        if not plan_row:
            raise HiveNotFoundError(f"Materialization plan not found: {plan_key}")
        plan_request = json.loads(str(plan_row["request_json"]))
        review_required = bool(
            adapter.requires_independent_review
            or plan_request.get("independent_review_required")
        )
        file_route = enforce_dispatch_file_route(
            plan_request=plan_request,
            adapter_id=adapter.adapter_id,
            implementation_id=adapter.implementation_id,
            display_name=adapter.display_name,
            requested_capability=capability,
            payload=json.loads(payload_json),
        )
        request = {
            "execution_id": execution_key,
            "plan_id": plan_key,
            "work_unit_id": self._required(work_unit_id, "work_unit_id"),
            "adapter_id": adapter_key,
            "requested_capability": capability,
            "requested_writable_domains": domains,
            "payload": json.loads(payload_json),
            "payload_sha256": payload_sha,
            "timeout_ms": timeout,
            "actor": self._required(actor, "actor"),
            "human_approval": bool(human_approval),
            "governance_authorization": dict(governance_authorization or {}),
            "review_required": review_required,
            "file_route_enforcement": file_route.model_dump(mode="json"),
        }
        fingerprint = self._fingerprint(request)
        with self._connect() as conn:
            if idempotency_key:
                existing = conn.execute(
                    """
                    SELECT execution_id, request_sha256
                    FROM hive_dispatch_executions
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Execution idempotency key was already used with different content."
                        )
                    snapshot = self._execution_snapshot(
                        conn, execution_id=str(existing["execution_id"])
                    )
                    if snapshot.executions:
                        self._reconcile_execution_receipt(snapshot.executions[0])
                    return snapshot
        denial = file_route.error if file_route.applies and not file_route.allowed else ""
        if not denial and timeout > adapter.max_timeout_ms:
            denial = (
                f"Requested timeout {timeout}ms exceeds adapter limit "
                f"{adapter.max_timeout_ms}ms."
            )
        materialized = receipt = lease = worker = None
        if not denial:
            try:
                materialized, receipt, lease, worker = self._lane_context(
                    plan_id=request["plan_id"],
                    work_unit_id=request["work_unit_id"],
                    adapter=adapter,
                    requested_capability=capability,
                    requested_domains=domains,
                    human_approval=human_approval,
                    governance_authorization=governance_authorization,
                )
            except HiveTransitionError as exc:
                denial = str(exc)
        if denial:
            return self._record_denied_execution(
                execution_id=execution_key,
                request=request,
                adapter=adapter,
                fingerprint=fingerprint,
                idempotency_key=idempotency_key,
                error_message=denial,
            )
        assert materialized is not None
        assert receipt is not None
        assert lease is not None
        assert worker is not None
        with self._write() as conn:
            active = self._active_lane_execution(
                conn,
                plan_id=request["plan_id"],
                work_unit_id=request["work_unit_id"],
            )
            if active:
                raise HiveTransitionError(
                    "A lane already has an active Phase 3 execution: "
                    f"{active['execution_id']} ({active['status']})."
                )
            self._insert_execution(
                conn,
                execution_id=execution_key,
                request=request,
                materialized=materialized,
                receipt=receipt,
                lease=lease,
                adapter=adapter,
                status=ExecutionStatus.CLAIMED.value,
                fingerprint=fingerprint,
                idempotency_key=idempotency_key,
            )
        try:
            self._phase2_receipt(
                execution_id=execution_key,
                plan_id=request["plan_id"],
                work_unit_id=request["work_unit_id"],
                status=DispatchStatus.ACKNOWLEDGED.value,
                actor=request["actor"],
                evidence={
                    "phase3_execution_id": execution_key,
                    "adapter_id": adapter.adapter_id,
                    "payload_sha256": payload_sha,
                },
            )
        except Exception as exc:
            with self._write() as conn:
                self._update_execution(
                    conn,
                    execution_id=execution_key,
                    status=ExecutionStatus.FAILED.value,
                    actor=request["actor"],
                    error_code="ACKNOWLEDGEMENT_FAILED",
                    error_message=str(exc),
                    finished=True,
                )
                self._execution_event(
                    conn,
                    execution_id=execution_key,
                    event_type="EXECUTION_ACKNOWLEDGEMENT_FAILED",
                    actor=request["actor"],
                    payload={"error": str(exc)},
                )
            with self._connect() as conn:
                return self._execution_snapshot(conn, execution_id=execution_key)
        return self._execute_existing(execution_key, actor=request["actor"])

    def _execute_existing(
        self,
        execution_id: str,
        *,
        actor: str,
    ) -> HiveExecutionSnapshot:
        with self._write() as conn:
            row = conn.execute(
                "SELECT * FROM hive_dispatch_executions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Execution not found: {execution_id}")
            current = self._execution_from_row(row)
            if current.status in TERMINAL_EXECUTION_STATUSES:
                snapshot = self._execution_snapshot(conn, execution_id=execution_id)
                self._reconcile_execution_receipt(current)
                return snapshot
            if current.status == ExecutionStatus.REVIEW_REQUIRED.value:
                return self._execution_snapshot(conn, execution_id=execution_id)
            if current.status not in {
                ExecutionStatus.CLAIMED.value,
                ExecutionStatus.RUNNING.value,
            }:
                raise HiveTransitionError(
                    f"Execution cannot run from status {current.status}."
                )
            adapter = self._adapter_from_row(
                self._adapter_row(conn, current.adapter_id)
            )
            self._update_execution(
                conn,
                execution_id=execution_id,
                status=ExecutionStatus.RUNNING.value,
                actor=actor,
                started=True,
            )
            self._execution_event(
                conn,
                execution_id=execution_id,
                event_type="EXECUTION_STARTED",
                actor=actor,
                payload={
                    "attempt": current.attempt,
                    "adapter_id": current.adapter_id,
                },
            )
        started = time.monotonic()
        executor = ThreadPoolExecutor(
            max_workers=1, thread_name_prefix="aigentbee-adapter"
        )
        future = executor.submit(
            self._run_adapter,
            execution_id,
            actor,
            adapter.implementation_id,
            current.request_payload,
            lambda: self._cancellation_requested(execution_id),
        )
        try:
            result = future.result(timeout=current.timeout_ms / 1000)
            duration_ms = int((time.monotonic() - started) * 1000)
            result_json = self._json(result).encode("utf-8")
            if len(result_json) > adapter.max_payload_bytes:
                raise HiveTransitionError(
                    "Adapter result exceeds the configured output boundary."
                )
            evidence = {
                "adapter_id": adapter.adapter_id,
                "implementation_id": adapter.implementation_id,
                "attempt": current.attempt,
                "duration_ms": duration_ms,
                "input_sha256": self._fingerprint(current.request_payload),
                "result_sha256": hashlib.sha256(result_json).hexdigest(),
                "performed_external_effect": bool(
                    result.get("performed_external_effect", False)
                ),
            }
            if result.get("effect_id"):
                evidence["phase4_effect_id"] = result["effect_id"]
            if result.get("certification_id"):
                evidence["phase4_certification_id"] = result["certification_id"]
            target = (
                ExecutionStatus.REVIEW_REQUIRED.value
                if current.review_required
                else ExecutionStatus.COMPLETED.value
            )
            with self._write() as conn:
                self._update_execution(
                    conn,
                    execution_id=execution_id,
                    status=target,
                    actor=actor,
                    result=result,
                    evidence=evidence,
                    finished=target == ExecutionStatus.COMPLETED.value,
                )
                self._execution_event(
                    conn,
                    execution_id=execution_id,
                    event_type=(
                        "EXECUTION_REVIEW_REQUIRED"
                        if target == ExecutionStatus.REVIEW_REQUIRED.value
                        else "EXECUTION_COMPLETED"
                    ),
                    actor=actor,
                    payload=evidence,
                )
                snapshot = self._execution_snapshot(conn, execution_id=execution_id)
            if target == ExecutionStatus.COMPLETED.value and snapshot.executions:
                self._reconcile_execution_receipt(snapshot.executions[0])
            return snapshot
        except AdapterCancelled as exc:
            return self._finalize_execution_error(
                execution_id=execution_id,
                actor=actor,
                status=ExecutionStatus.CANCELLED.value,
                error_code="CANCELLED",
                error_message=str(exc),
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        except FutureTimeoutError:
            with self._write() as conn:
                conn.execute(
                    """
                    UPDATE hive_dispatch_executions
                    SET cancellation_requested = 1, updated_at = ?,
                        revision = revision + 1
                    WHERE execution_id = ?
                    """,
                    (self._now_ms(), execution_id),
                )
            return self._finalize_execution_error(
                execution_id=execution_id,
                actor=actor,
                status=ExecutionStatus.TIMED_OUT.value,
                error_code="TIMEOUT",
                error_message=f"Execution exceeded {current.timeout_ms}ms timeout.",
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        except Exception as exc:
            return self._finalize_execution_error(
                execution_id=execution_id,
                actor=actor,
                status=ExecutionStatus.FAILED.value,
                error_code="ADAPTER_FAILED",
                error_message=str(exc),
                duration_ms=int((time.monotonic() - started) * 1000),
            )
        finally:
            future.cancel()
            executor.shutdown(wait=False, cancel_futures=True)

    def _finalize_execution_error(
        self,
        *,
        execution_id: str,
        actor: str,
        status: str,
        error_code: str,
        error_message: str,
        duration_ms: int,
    ) -> HiveExecutionSnapshot:
        evidence = {
            "duration_ms": duration_ms,
            "performed_external_effect": False,
        }
        with self._write() as conn:
            self._update_execution(
                conn,
                execution_id=execution_id,
                status=status,
                actor=actor,
                evidence=evidence,
                error_code=error_code,
                error_message=error_message,
                finished=True,
            )
            self._execution_event(
                conn,
                execution_id=execution_id,
                event_type=f"EXECUTION_{status}",
                actor=actor,
                payload={
                    "error_code": error_code,
                    "error_message": error_message,
                    **evidence,
                },
            )
            snapshot = self._execution_snapshot(conn, execution_id=execution_id)
        if snapshot.executions:
            self._reconcile_execution_receipt(snapshot.executions[0])
        return snapshot

    def cancel_execution(
        self,
        *,
        execution_id: str,
        actor: str,
        reason: str,
        idempotency_key: str | None = None,
    ) -> HiveExecutionSnapshot:
        request = {
            "execution_id": self._required(execution_id, "execution_id"),
            "actor": self._required(actor, "actor"),
            "reason": self._required(reason, "reason"),
        }
        fingerprint = self._fingerprint(request)
        terminal_snapshot: HiveExecutionSnapshot | None = None
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    """
                    SELECT execution_id, request_sha256
                    FROM hive_execution_events
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Cancellation idempotency key was already used with different content."
                        )
                    return self._execution_snapshot(
                        conn, execution_id=str(existing["execution_id"])
                    )
            row = conn.execute(
                "SELECT * FROM hive_dispatch_executions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Execution not found: {execution_id}")
            execution = self._execution_from_row(row)
            if execution.status in TERMINAL_EXECUTION_STATUSES:
                terminal_snapshot = self._execution_snapshot(
                    conn, execution_id=execution_id
                )
            elif execution.status == ExecutionStatus.RUNNING.value:
                conn.execute(
                    """
                    UPDATE hive_dispatch_executions
                    SET cancellation_requested = 1, actor = ?, updated_at = ?,
                        revision = revision + 1
                    WHERE execution_id = ?
                    """,
                    (request["actor"], self._now_ms(), execution_id),
                )
                self._execution_event(
                    conn,
                    execution_id=execution_id,
                    event_type="EXECUTION_CANCELLATION_REQUESTED",
                    actor=request["actor"],
                    payload={"reason": request["reason"]},
                    fingerprint=fingerprint,
                    idempotency_key=idempotency_key,
                )
                return self._execution_snapshot(conn, execution_id=execution_id)
            else:
                self._update_execution(
                    conn,
                    execution_id=execution_id,
                    status=ExecutionStatus.CANCELLED.value,
                    actor=request["actor"],
                    evidence={"cancellation_reason": request["reason"]},
                    error_code="CANCELLED",
                    error_message=request["reason"],
                    cancellation_requested=True,
                    finished=True,
                )
                self._execution_event(
                    conn,
                    execution_id=execution_id,
                    event_type="EXECUTION_CANCELLED",
                    actor=request["actor"],
                    payload={"reason": request["reason"]},
                    fingerprint=fingerprint,
                    idempotency_key=idempotency_key,
                )
                terminal_snapshot = self._execution_snapshot(
                    conn, execution_id=execution_id
                )
        if terminal_snapshot and terminal_snapshot.executions:
            self._reconcile_execution_receipt(terminal_snapshot.executions[0])
        return terminal_snapshot or HiveExecutionSnapshot(
            ok=False,
            found=False,
            db_path=str(self.path),
            error="Execution cancellation did not produce a snapshot.",
        )

    def recover_execution(
        self,
        *,
        execution_id: str,
        actor: str,
        reason: str,
        stale_after_ms: int = 30000,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> HiveExecutionSnapshot:
        try:
            authorization_receipt = require_governed_authorization(
                governance_authorization,
                required_authority="A2",
                legacy_human_approval=human_approval,
            )
        except GovernedAuthorizationError as exc:
            raise HiveTransitionError(str(exc)) from exc
        request = {
            "execution_id": self._required(execution_id, "execution_id"),
            "actor": self._required(actor, "actor"),
            "reason": self._required(reason, "reason"),
            "stale_after_ms": max(0, int(stale_after_ms)),
            "human_approval": bool(human_approval),
            "governance_authorization": authorization_receipt,
        }
        fingerprint = self._fingerprint(request)
        cancelled = False
        idempotent_resume = False
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    """
                    SELECT execution_id, request_sha256
                    FROM hive_execution_events
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Recovery idempotency key was already used with different content."
                        )
                    existing_snapshot = self._execution_snapshot(
                        conn, execution_id=str(existing["execution_id"])
                    )
                    if (
                        existing_snapshot.executions
                        and existing_snapshot.executions[0].status
                        == ExecutionStatus.CLAIMED.value
                    ):
                        idempotent_resume = True
                    else:
                        return existing_snapshot
            row = conn.execute(
                "SELECT * FROM hive_dispatch_executions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Execution not found: {execution_id}")
            execution = self._execution_from_row(row)
            if execution.status in TERMINAL_EXECUTION_STATUSES:
                snapshot = self._execution_snapshot(conn, execution_id=execution_id)
                self._reconcile_execution_receipt(execution)
                return snapshot
            if execution.status == ExecutionStatus.REVIEW_REQUIRED.value:
                return self._execution_snapshot(conn, execution_id=execution_id)
            if execution.status not in {
                ExecutionStatus.CLAIMED.value,
                ExecutionStatus.RUNNING.value,
            }:
                raise HiveTransitionError(
                    f"Execution cannot be recovered from status {execution.status}."
                )
            age_ms = max(0, self._now_ms() - execution.updated_at)
            if (
                not idempotent_resume
                and not execution.cancellation_requested
                and age_ms < request["stale_after_ms"]
            ):
                raise HiveTransitionError(
                    f"Execution is not stale: age {age_ms}ms is below "
                    f"{request['stale_after_ms']}ms."
                )
            if execution.cancellation_requested:
                cancelled = True
            elif not idempotent_resume:
                now = self._now_ms()
                conn.execute(
                    """
                    UPDATE hive_dispatch_executions
                    SET status = ?, actor = ?, result_json = '{}',
                        evidence_json = '{}', error_code = '', error_message = '',
                        cancellation_requested = 0, attempt = attempt + 1,
                        started_at = 0, finished_at = 0, updated_at = ?,
                        revision = revision + 1
                    WHERE execution_id = ?
                    """,
                    (
                        ExecutionStatus.CLAIMED.value,
                        request["actor"],
                        now,
                        execution_id,
                    ),
                )
                self._execution_event(
                    conn,
                    execution_id=execution_id,
                    event_type="EXECUTION_RECOVERY_STARTED",
                    actor=request["actor"],
                    payload={
                        "reason": request["reason"],
                        "previous_status": execution.status,
                        "age_ms": age_ms,
                    },
                    fingerprint=fingerprint,
                    idempotency_key=idempotency_key,
                )
        if cancelled:
            return self._finalize_execution_error(
                execution_id=execution_id,
                actor=request["actor"],
                status=ExecutionStatus.CANCELLED.value,
                error_code="CANCELLED",
                error_message="Cancellation request finalized during recovery.",
                duration_ms=0,
            )
        return self._execute_existing(execution_id, actor=request["actor"])

    def review_execution(
        self,
        *,
        execution_id: str,
        reviewer_worker_id: str,
        approved: bool,
        actor: str,
        findings: dict[str, Any] | None = None,
        human_approval: bool = False,
        governance_authorization: dict[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> HiveExecutionSnapshot:
        try:
            authorization_receipt = require_governed_authorization(
                governance_authorization,
                required_authority="A2",
                legacy_human_approval=human_approval,
                require_reversible=False,
            )
        except GovernedAuthorizationError as exc:
            raise HiveTransitionError(str(exc)) from exc
        request = {
            "execution_id": self._required(execution_id, "execution_id"),
            "reviewer_worker_id": self._required(
                reviewer_worker_id, "reviewer_worker_id"
            ),
            "approved": bool(approved),
            "actor": self._required(actor, "actor"),
            "findings": findings or {},
            "human_approval": bool(human_approval),
            "governance_authorization": authorization_receipt,
        }
        self._validate_payload_keys(request["findings"], path="findings")
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    """
                    SELECT execution_id, request_sha256
                    FROM hive_execution_reviews
                    WHERE idempotency_key = ?
                    """,
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Review idempotency key was already used with different content."
                        )
                    return self._execution_snapshot(
                        conn, execution_id=str(existing["execution_id"])
                    )
            row = conn.execute(
                "SELECT * FROM hive_dispatch_executions WHERE execution_id = ?",
                (execution_id,),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Execution not found: {execution_id}")
            execution = self._execution_from_row(row)
            if execution.status != ExecutionStatus.REVIEW_REQUIRED.value:
                raise HiveTransitionError(
                    "Only REVIEW_REQUIRED executions can receive an independent review; "
                    f"current status is {execution.status}."
                )
            if execution.worker_id == request["reviewer_worker_id"]:
                raise HiveTransitionError(
                    "The executing worker cannot independently review its own execution."
                )
            workers = {
                item.worker_id: item
                for item in self.workforce.list_workers(limit=1000).workers
            }
            reviewer = workers.get(request["reviewer_worker_id"])
            if not reviewer:
                raise HiveNotFoundError(
                    f"Reviewer worker not found: {request['reviewer_worker_id']}"
                )
            if reviewer.stage != WorkerStage.APPOINTED.value:
                raise HiveTransitionError(
                    "Independent review requires an APPOINTED reviewer."
                )
            if reviewer.worker_class != WorkerClass.GOVERNANCE_REVIEWER.value:
                raise HiveTransitionError(
                    "Independent review requires a GOVERNANCE_REVIEWER worker."
                )
            adapter = self._adapter_from_row(
                self._adapter_row(conn, execution.adapter_id)
            )
            if not self._authority_allows(
                reviewer.authority_ceiling, adapter.required_authority
            ):
                raise HiveTransitionError(
                    "Reviewer authority is below the adapter requirement."
                )
            now = self._now_ms()
            review_id = f"review-{uuid.uuid4()}"
            conn.execute(
                """
                INSERT INTO hive_execution_reviews(
                    review_id, execution_id, reviewer_worker_id, actor,
                    approved, findings_json, idempotency_key,
                    request_sha256, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    review_id,
                    execution_id,
                    request["reviewer_worker_id"],
                    request["actor"],
                    int(request["approved"]),
                    self._json(request["findings"]),
                    idempotency_key,
                    fingerprint,
                    now,
                ),
            )
            target = (
                ExecutionStatus.COMPLETED.value
                if request["approved"]
                else ExecutionStatus.FAILED.value
            )
            evidence = {
                **execution.evidence,
                "review_id": review_id,
                "reviewer_worker_id": request["reviewer_worker_id"],
                "review_approved": request["approved"],
                "review_findings_sha256": self._fingerprint(request["findings"]),
            }
            self._update_execution(
                conn,
                execution_id=execution_id,
                status=target,
                actor=request["actor"],
                result=execution.result,
                evidence=evidence,
                error_code="" if request["approved"] else "REVIEW_REJECTED",
                error_message=""
                if request["approved"]
                else "Independent review rejected the execution.",
                finished=True,
            )
            self._execution_event(
                conn,
                execution_id=execution_id,
                event_type=(
                    "EXECUTION_REVIEW_APPROVED"
                    if request["approved"]
                    else "EXECUTION_REVIEW_REJECTED"
                ),
                actor=request["actor"],
                payload={
                    "review_id": review_id,
                    "reviewer_worker_id": request["reviewer_worker_id"],
                    "approved": request["approved"],
                },
            )
            snapshot = self._execution_snapshot(conn, execution_id=execution_id)
        if snapshot.executions:
            self._reconcile_execution_receipt(snapshot.executions[0])
        return snapshot


_DISPATCHER_LOCK = threading.RLock()
_DISPATCHER_CACHE: dict[str, HiveExecutionDispatcherStore] = {}


def get_hive_execution_dispatcher_store(
    path: str | Path | None = None,
) -> HiveExecutionDispatcherStore:
    resolved = Path(path or default_hive_runtime_db_path()).expanduser().resolve()
    key = str(resolved)
    with _DISPATCHER_LOCK:
        store = _DISPATCHER_CACHE.get(key)
        if store is None:
            store = HiveExecutionDispatcherStore(resolved)
            _DISPATCHER_CACHE[key] = store
        return store
