from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Iterator

from pydantic import BaseModel, Field

HIVE_RUNTIME_SCHEMA_VERSION = 1
MAX_SNAPSHOT_ITEMS = 1000


class HiveRuntimeError(RuntimeError):
    """Base error for the durable Hive runtime."""


class HiveSchemaVersionError(HiveRuntimeError):
    """Raised before mutation when a database uses a newer schema."""


class HiveNotFoundError(HiveRuntimeError):
    """Raised when a requested mission or work unit does not exist."""


class HiveIdempotencyConflict(HiveRuntimeError):
    """Raised when an idempotency key is reused with different content."""


class HiveTransitionError(HiveRuntimeError):
    """Raised when a state transition or revision check is invalid."""


class MissionStatus(str, Enum):
    ACTIVE = "ACTIVE"
    WAITING = "WAITING"
    BLOCKED = "BLOCKED"
    FAN_IN = "FAN_IN"
    CLOSED = "CLOSED"
    CANCELLED = "CANCELLED"


class WorkUnitStatus(str, Enum):
    ACTIVE = "ACTIVE"
    WAITING = "WAITING"
    BLOCKED = "BLOCKED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_MISSION_STATUSES = {MissionStatus.CLOSED.value, MissionStatus.CANCELLED.value}
TERMINAL_WORK_STATUSES = {
    WorkUnitStatus.COMPLETED.value,
    WorkUnitStatus.FAILED.value,
    WorkUnitStatus.CANCELLED.value,
}
WORK_TRANSITIONS: dict[str, set[str]] = {
    WorkUnitStatus.ACTIVE.value: {
        WorkUnitStatus.ACTIVE.value,
        WorkUnitStatus.WAITING.value,
        WorkUnitStatus.BLOCKED.value,
        WorkUnitStatus.COMPLETED.value,
        WorkUnitStatus.FAILED.value,
        WorkUnitStatus.CANCELLED.value,
    },
    WorkUnitStatus.WAITING.value: {
        WorkUnitStatus.WAITING.value,
        WorkUnitStatus.ACTIVE.value,
        WorkUnitStatus.BLOCKED.value,
        WorkUnitStatus.CANCELLED.value,
    },
    WorkUnitStatus.BLOCKED.value: {
        WorkUnitStatus.BLOCKED.value,
        WorkUnitStatus.ACTIVE.value,
        WorkUnitStatus.WAITING.value,
        WorkUnitStatus.FAILED.value,
        WorkUnitStatus.CANCELLED.value,
    },
    WorkUnitStatus.COMPLETED.value: {WorkUnitStatus.COMPLETED.value},
    WorkUnitStatus.FAILED.value: {WorkUnitStatus.FAILED.value},
    WorkUnitStatus.CANCELLED.value: {WorkUnitStatus.CANCELLED.value},
}


class HiveWorkUnitSpec(BaseModel):
    work_unit_id: str | None = None
    title: str = Field(min_length=1)
    role: str = Field(min_length=1)
    conversation_id: str = ""
    writable_domain: str = ""
    dependencies: list[str] = Field(default_factory=list)
    authority_ceiling: str = "A3"


class HiveWorkUnit(BaseModel):
    work_unit_id: str
    mission_id: str
    title: str
    role: str
    status: str
    conversation_id: str = ""
    writable_domain: str = ""
    dependencies: list[str] = Field(default_factory=list)
    authority_ceiling: str = "A3"
    created_at: int
    updated_at: int
    revision: int


class HiveEvent(BaseModel):
    event_id: str
    mission_id: str
    work_unit_id: str = ""
    event_type: str
    sender: str
    recipient: str = "swarm"
    payload: dict[str, Any] = Field(default_factory=dict)
    context_version: int = 0
    created_at: int


class HiveDecision(BaseModel):
    decision_id: str
    mission_id: str
    status: str
    summary: str
    dissent: list[dict[str, Any]] = Field(default_factory=list)
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    created_at: int


class HiveAction(BaseModel):
    record_id: str
    mission_id: str
    action_type: str
    actor: str
    correlation_id: str = ""
    payload: dict[str, Any] = Field(default_factory=dict)
    created_at: int


class HiveMissionSnapshot(BaseModel):
    ok: bool = True
    found: bool = True
    db_path: str = ""
    error: str = ""
    mission_id: str = ""
    title: str = ""
    objective: str = ""
    lifecycle_stage: str = ""
    status: str = ""
    authority_ceiling: str = "A3"
    parent_context_id: str = ""
    cancellation_reason: str = ""
    created_at: int = 0
    updated_at: int = 0
    revision: int = 0
    work_unit_count: int = 0
    event_count: int = 0
    action_count: int = 0
    work_units: list[HiveWorkUnit] = Field(default_factory=list)
    events: list[HiveEvent] = Field(default_factory=list)
    actions: list[HiveAction] = Field(default_factory=list)
    decision: HiveDecision | None = None


class HiveRuntimeStore:
    """SQLite-backed minimum viable runtime for Hive missions and workers."""

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
    def _bounded_limit(value: int) -> int:
        return max(0, min(int(value), MAX_SNAPSHOT_ITEMS))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn
    def _ensure_schema(self) -> None:
        with self._schema_lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            conn = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
            conn.row_factory = sqlite3.Row
            try:
                version = int(conn.execute("PRAGMA user_version").fetchone()[0])
                if version > HIVE_RUNTIME_SCHEMA_VERSION:
                    raise HiveSchemaVersionError(
                        f"Hive runtime schema {version} is newer than supported "
                        f"version {HIVE_RUNTIME_SCHEMA_VERSION}."
                    )
                if version == HIVE_RUNTIME_SCHEMA_VERSION:
                    self._validate_schema(conn)
                    return
                conn.executescript(
                    """
                    BEGIN IMMEDIATE;
                    CREATE TABLE hive_missions (
                        mission_id TEXT PRIMARY KEY,
                        title TEXT NOT NULL,
                        objective TEXT NOT NULL,
                        lifecycle_stage TEXT NOT NULL,
                        status TEXT NOT NULL,
                        authority_ceiling TEXT NOT NULL,
                        parent_context_id TEXT NOT NULL DEFAULT '',
                        cancellation_reason TEXT NOT NULL DEFAULT '',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
                    );
                    CREATE TABLE hive_work_units (
                        work_unit_id TEXT PRIMARY KEY,
                        mission_id TEXT NOT NULL REFERENCES hive_missions(mission_id) ON DELETE CASCADE,
                        title TEXT NOT NULL,
                        role TEXT NOT NULL,
                        status TEXT NOT NULL,
                        conversation_id TEXT NOT NULL DEFAULT '',
                        writable_domain TEXT NOT NULL DEFAULT '',
                        dependencies_json TEXT NOT NULL DEFAULT '[]',
                        authority_ceiling TEXT NOT NULL DEFAULT 'A3',
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
                    );
                    CREATE TABLE hive_events (
                        event_id TEXT PRIMARY KEY,
                        mission_id TEXT NOT NULL REFERENCES hive_missions(mission_id) ON DELETE CASCADE,
                        work_unit_id TEXT NOT NULL DEFAULT '',
                        event_type TEXT NOT NULL,
                        sender TEXT NOT NULL,
                        recipient TEXT NOT NULL DEFAULT 'swarm',
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        context_version INTEGER NOT NULL DEFAULT 0,
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE TABLE hive_decisions (
                        decision_id TEXT PRIMARY KEY,
                        mission_id TEXT NOT NULL REFERENCES hive_missions(mission_id) ON DELETE CASCADE,
                        status TEXT NOT NULL,
                        summary TEXT NOT NULL,
                        dissent_json TEXT NOT NULL DEFAULT '[]',
                        evidence_json TEXT NOT NULL DEFAULT '[]',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE TABLE hive_actions (
                        record_id TEXT PRIMARY KEY,
                        mission_id TEXT NOT NULL REFERENCES hive_missions(mission_id) ON DELETE CASCADE,
                        action_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        correlation_id TEXT NOT NULL DEFAULT '',
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        created_at INTEGER NOT NULL
                    );
                    CREATE INDEX idx_hive_work_units_mission_status
                        ON hive_work_units(mission_id, status, updated_at DESC);
                    CREATE INDEX idx_hive_events_mission_created
                        ON hive_events(mission_id, created_at DESC, event_id DESC);
                    CREATE INDEX idx_hive_actions_mission_created
                        ON hive_actions(mission_id, created_at DESC, record_id DESC);
                    CREATE INDEX idx_hive_decisions_mission_created
                        ON hive_decisions(mission_id, created_at DESC, decision_id DESC);
                    PRAGMA user_version = 1;
                    COMMIT;
                    """
                )
                conn.execute("PRAGMA journal_mode = WAL")
                conn.execute("PRAGMA synchronous = FULL")
                self._validate_schema(conn)
            except Exception:
                if conn.in_transaction:
                    conn.rollback()
                raise
            finally:
                conn.close()

    @staticmethod
    def _validate_schema(conn: sqlite3.Connection) -> None:
        required = {
            "hive_missions", "hive_work_units", "hive_events",
            "hive_decisions", "hive_actions",
        }
        present = {
            str(row[0])
            for row in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
        missing = required - present
        if missing:
            raise HiveSchemaVersionError(
                "Hive runtime database declares schema version 1 but is "
                f"missing tables: {sorted(missing)}"
            )

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
    def _record_action(
        self,
        conn: sqlite3.Connection,
        *,
        mission_id: str,
        action_type: str,
        actor: str,
        payload: dict[str, Any],
        correlation_id: str = "",
        created_at: int | None = None,
    ) -> None:
        conn.execute(
            """
            INSERT INTO hive_actions(
                record_id, mission_id, action_type, actor, correlation_id,
                payload_json, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                str(uuid.uuid4()), mission_id, action_type, actor,
                correlation_id, self._json(payload), created_at or self._now_ms(),
            ),
        )

    def _insert_event(
        self,
        conn: sqlite3.Connection,
        *,
        mission_id: str,
        event_type: str,
        sender: str,
        recipient: str = "swarm",
        work_unit_id: str = "",
        payload: dict[str, Any] | None = None,
        context_version: int = 0,
        idempotency_key: str | None = None,
        request_sha256: str = "",
        created_at: int | None = None,
    ) -> str:
        event_id = str(uuid.uuid4())
        conn.execute(
            """
            INSERT INTO hive_events(
                event_id, mission_id, work_unit_id, event_type, sender,
                recipient, payload_json, context_version, idempotency_key,
                request_sha256, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event_id, mission_id, work_unit_id, event_type, sender,
                recipient, self._json(payload or {}), int(context_version),
                idempotency_key, request_sha256, created_at or self._now_ms(),
            ),
        )
        return event_id

    def create_mission(
        self,
        *,
        title: str,
        objective: str,
        lifecycle_stage: str,
        work_units: list[HiveWorkUnitSpec] | None = None,
        authority_ceiling: str = "A3",
        parent_context_id: str = "",
        mission_id: str | None = None,
        idempotency_key: str | None = None,
        actor: str = "notion2api",
    ) -> HiveMissionSnapshot:
        specs = [
            item if isinstance(item, HiveWorkUnitSpec)
            else HiveWorkUnitSpec.model_validate(item)
            for item in (work_units or [])
        ]
        mission_key = str(mission_id or f"hive-{uuid.uuid4()}").strip()
        normalized_specs: list[tuple[str, HiveWorkUnitSpec]] = []
        seen_work_ids: set[str] = set()
        for index, spec in enumerate(specs, start=1):
            work_id = str(
                spec.work_unit_id or f"{mission_key}-wu-{index:03d}"
            ).strip()
            if not work_id or work_id in seen_work_ids:
                raise ValueError(f"Duplicate or empty work_unit_id: {work_id!r}")
            seen_work_ids.add(work_id)
            normalized_specs.append((work_id, spec))
        request = {
            "title": self._required(title, "title"),
            "objective": self._required(objective, "objective"),
            "lifecycle_stage": self._required(
                lifecycle_stage, "lifecycle_stage"
            ),
            "work_units": [
                {"work_unit_id": work_id, **spec.model_dump(mode="json")}
                for work_id, spec in normalized_specs
            ],
            "authority_ceiling": str(authority_ceiling or "A3").strip(),
            "parent_context_id": str(parent_context_id or "").strip(),
            "mission_id": mission_key,
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT mission_id, request_sha256 FROM hive_missions "
                    "WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Mission idempotency key was already used "
                            "with different content."
                        )
                    return self._snapshot(conn, str(existing["mission_id"]))
            if conn.execute(
                "SELECT 1 FROM hive_missions WHERE mission_id = ?",
                (mission_key,),
            ).fetchone():
                raise HiveIdempotencyConflict(
                    f"Mission already exists: {mission_key}"
                )
            now = self._now_ms()
            conn.execute(
                """
                INSERT INTO hive_missions(
                    mission_id, title, objective, lifecycle_stage, status,
                    authority_ceiling, parent_context_id, cancellation_reason,
                    idempotency_key, request_sha256, created_at, updated_at, revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, 1)
                """,
                (
                    mission_key, request["title"], request["objective"],
                    request["lifecycle_stage"], MissionStatus.ACTIVE.value,
                    request["authority_ceiling"], request["parent_context_id"],
                    idempotency_key, fingerprint, now, now,
                ),
            )
            for work_id, spec in normalized_specs:
                conn.execute(
                    """
                    INSERT INTO hive_work_units(
                        work_unit_id, mission_id, title, role, status,
                        conversation_id, writable_domain, dependencies_json,
                        authority_ceiling, created_at, updated_at, revision
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1)
                    """,
                    (
                        work_id, mission_key,
                        self._required(spec.title, "work unit title"),
                        self._required(spec.role, "work unit role"),
                        WorkUnitStatus.ACTIVE.value,
                        spec.conversation_id.strip(),
                        spec.writable_domain.strip(),
                        self._json(spec.dependencies),
                        spec.authority_ceiling.strip()
                        or request["authority_ceiling"],
                        now, now,
                    ),
                )
            self._insert_event(
                conn,
                mission_id=mission_key,
                event_type="MISSION_OPENED",
                sender=actor,
                payload={
                    "work_unit_count": len(normalized_specs),
                    "lifecycle_stage": request["lifecycle_stage"],
                },
                request_sha256=fingerprint,
                created_at=now,
            )
            self._record_action(
                conn,
                mission_id=mission_key,
                action_type="MISSION_CREATED",
                actor=actor,
                payload=request,
                correlation_id=idempotency_key or mission_key,
                created_at=now,
            )
            return self._snapshot(conn, mission_key)

    def get_mission(
        self,
        mission_id: str,
        *,
        event_limit: int = 200,
        action_limit: int = 200,
    ) -> HiveMissionSnapshot:
        conn = self._connect()
        try:
            return self._snapshot(
                conn,
                mission_id,
                event_limit=event_limit,
                action_limit=action_limit,
            )
        finally:
            conn.close()

    def append_event(
        self,
        *,
        mission_id: str,
        event_type: str,
        sender: str,
        payload: dict[str, Any] | None = None,
        recipient: str = "swarm",
        work_unit_id: str = "",
        context_version: int = 0,
        expected_mission_revision: int | None = None,
        work_unit_status: str | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        request = {
            "mission_id": mission_id,
            "event_type": self._required(
                event_type, "event_type"
            ).upper(),
            "sender": self._required(sender, "sender"),
            "payload": payload or {},
            "recipient": str(recipient or "swarm").strip(),
            "work_unit_id": str(work_unit_id or "").strip(),
            "context_version": int(context_version),
            "expected_mission_revision": expected_mission_revision,
            "work_unit_status": str(work_unit_status or "").upper(),
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            mission = conn.execute(
                "SELECT status, revision FROM hive_missions "
                "WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()
            if not mission:
                raise HiveNotFoundError(f"Mission not found: {mission_id}")
            if idempotency_key:
                existing = conn.execute(
                    "SELECT request_sha256 FROM hive_events "
                    "WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Event idempotency key was already used "
                            "with different content."
                        )
                    return self._snapshot(conn, mission_id)
            if str(mission["status"]) in TERMINAL_MISSION_STATUSES:
                raise HiveTransitionError(
                    "A terminal mission cannot accept new events."
                )
            if (
                expected_mission_revision is not None
                and int(mission["revision"])
                != int(expected_mission_revision)
            ):
                raise HiveTransitionError(
                    "Stale mission revision: expected "
                    f"{expected_mission_revision}, current "
                    f"{int(mission['revision'])}."
                )
            now = self._now_ms()
            if work_unit_status:
                if not request["work_unit_id"]:
                    raise ValueError(
                        "work_unit_id is required when work_unit_status is set"
                    )
                row = conn.execute(
                    "SELECT status FROM hive_work_units "
                    "WHERE mission_id = ? AND work_unit_id = ?",
                    (mission_id, request["work_unit_id"]),
                ).fetchone()
                if not row:
                    raise HiveNotFoundError(
                        f"Work unit not found: {request['work_unit_id']}"
                    )
                current = str(row["status"])
                target = request["work_unit_status"]
                if target not in WORK_TRANSITIONS.get(current, set()):
                    raise HiveTransitionError(
                        f"Illegal work-unit transition: {current} -> {target}"
                    )
                conn.execute(
                    """
                    UPDATE hive_work_units
                    SET status = ?, updated_at = ?, revision = revision + 1
                    WHERE mission_id = ? AND work_unit_id = ?
                    """,
                    (target, now, mission_id, request["work_unit_id"]),
                )
            self._insert_event(
                conn,
                mission_id=mission_id,
                event_type=request["event_type"],
                sender=request["sender"],
                recipient=request["recipient"],
                work_unit_id=request["work_unit_id"],
                payload=request["payload"],
                context_version=request["context_version"],
                idempotency_key=idempotency_key,
                request_sha256=fingerprint,
                created_at=now,
            )
            conn.execute(
                "UPDATE hive_missions SET updated_at = ?, "
                "revision = revision + 1 WHERE mission_id = ?",
                (now, mission_id),
            )
            self._record_action(
                conn,
                mission_id=mission_id,
                action_type="EVENT_RECORDED",
                actor=request["sender"],
                payload=request,
                correlation_id=idempotency_key or "",
                created_at=now,
            )
            return self._snapshot(conn, mission_id)

    def cancel_mission(
        self,
        *,
        mission_id: str,
        reason: str,
        actor: str = "notion2api",
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        request = {
            "mission_id": mission_id,
            "reason": self._required(reason, "reason"),
            "actor": self._required(actor, "actor"),
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            mission = conn.execute(
                "SELECT status FROM hive_missions WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()
            if not mission:
                raise HiveNotFoundError(f"Mission not found: {mission_id}")
            if idempotency_key:
                existing = conn.execute(
                    "SELECT request_sha256 FROM hive_events "
                    "WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Cancellation idempotency key was already used "
                            "with different content."
                        )
                    return self._snapshot(conn, mission_id)
            current = str(mission["status"])
            if current == MissionStatus.CANCELLED.value:
                return self._snapshot(conn, mission_id)
            if current == MissionStatus.CLOSED.value:
                raise HiveTransitionError(
                    "A closed mission cannot be cancelled."
                )
            now = self._now_ms()
            conn.execute(
                """
                UPDATE hive_missions
                SET status = ?, cancellation_reason = ?,
                    updated_at = ?, revision = revision + 1
                WHERE mission_id = ?
                """,
                (
                    MissionStatus.CANCELLED.value,
                    request["reason"], now, mission_id,
                ),
            )
            conn.execute(
                """
                UPDATE hive_work_units
                SET status = ?, updated_at = ?, revision = revision + 1
                WHERE mission_id = ? AND status NOT IN (?, ?, ?)
                """,
                (
                    WorkUnitStatus.CANCELLED.value, now, mission_id,
                    WorkUnitStatus.COMPLETED.value,
                    WorkUnitStatus.FAILED.value,
                    WorkUnitStatus.CANCELLED.value,
                ),
            )
            self._insert_event(
                conn,
                mission_id=mission_id,
                event_type="CANCEL_REQUESTED",
                sender=request["actor"],
                payload={"reason": request["reason"]},
                idempotency_key=idempotency_key,
                request_sha256=fingerprint,
                created_at=now,
            )
            self._record_action(
                conn,
                mission_id=mission_id,
                action_type="MISSION_CANCELLED",
                actor=request["actor"],
                payload=request,
                correlation_id=idempotency_key or "",
                created_at=now,
            )
            return self._snapshot(conn, mission_id)

    def fan_in(
        self,
        *,
        mission_id: str,
        status: str,
        summary: str,
        dissent: list[dict[str, Any]] | None = None,
        evidence: list[dict[str, Any]] | None = None,
        actor: str = "emerald-city",
        close_mission: bool = False,
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        request = {
            "mission_id": mission_id,
            "status": self._required(status, "status").upper(),
            "summary": self._required(summary, "summary"),
            "dissent": dissent or [],
            "evidence": evidence or [],
            "actor": self._required(actor, "actor"),
            "close_mission": bool(close_mission),
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            mission = conn.execute(
                "SELECT status FROM hive_missions WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()
            if not mission:
                raise HiveNotFoundError(f"Mission not found: {mission_id}")
            if idempotency_key:
                existing = conn.execute(
                    "SELECT request_sha256 FROM hive_decisions "
                    "WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Fan-in idempotency key was already used "
                            "with different content."
                        )
                    return self._snapshot(conn, mission_id)
            if str(mission["status"]) in TERMINAL_MISSION_STATUSES:
                raise HiveTransitionError(
                    "A terminal mission cannot enter fan-in."
                )
            now = self._now_ms()
            decision_id = str(uuid.uuid4())
            conn.execute(
                """
                INSERT INTO hive_decisions(
                    decision_id, mission_id, status, summary, dissent_json,
                    evidence_json, idempotency_key, request_sha256, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    decision_id, mission_id, request["status"],
                    request["summary"], self._json(request["dissent"]),
                    self._json(request["evidence"]), idempotency_key,
                    fingerprint, now,
                ),
            )
            target_status = (
                MissionStatus.CLOSED.value
                if request["close_mission"]
                else MissionStatus.FAN_IN.value
            )
            if request["close_mission"]:
                conn.execute(
                    """
                    UPDATE hive_work_units
                    SET status = ?, updated_at = ?, revision = revision + 1
                    WHERE mission_id = ? AND status NOT IN (?, ?, ?)
                    """,
                    (
                        WorkUnitStatus.CANCELLED.value, now, mission_id,
                        WorkUnitStatus.COMPLETED.value,
                        WorkUnitStatus.FAILED.value,
                        WorkUnitStatus.CANCELLED.value,
                    ),
                )
            conn.execute(
                """
                UPDATE hive_missions
                SET status = ?, updated_at = ?, revision = revision + 1
                WHERE mission_id = ?
                """,
                (target_status, now, mission_id),
            )
            self._insert_event(
                conn,
                mission_id=mission_id,
                event_type="FANIN_SUBMITTED",
                sender=request["actor"],
                payload={
                    "decision_id": decision_id,
                    "status": request["status"],
                    "mission_status": target_status,
                },
                request_sha256=fingerprint,
                created_at=now,
            )
            self._record_action(
                conn,
                mission_id=mission_id,
                action_type="DECISION_RECORDED",
                actor=request["actor"],
                payload=request,
                correlation_id=idempotency_key or decision_id,
                created_at=now,
            )
            return self._snapshot(conn, mission_id)
    def _snapshot(
        self,
        conn: sqlite3.Connection,
        mission_id: str,
        *,
        event_limit: int = 200,
        action_limit: int = 200,
    ) -> HiveMissionSnapshot:
        mission = conn.execute(
            "SELECT * FROM hive_missions WHERE mission_id = ?",
            (mission_id,),
        ).fetchone()
        if not mission:
            return HiveMissionSnapshot(
                ok=False,
                found=False,
                db_path=str(self.path),
                mission_id=mission_id,
            )
        work_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM hive_work_units WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()[0]
        )
        event_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM hive_events WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()[0]
        )
        action_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM hive_actions WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()[0]
        )
        work_rows = conn.execute(
            "SELECT * FROM hive_work_units WHERE mission_id = ? "
            "ORDER BY created_at, work_unit_id",
            (mission_id,),
        ).fetchall()
        event_rows = conn.execute(
            "SELECT * FROM hive_events WHERE mission_id = ? "
            "ORDER BY created_at DESC, event_id DESC LIMIT ?",
            (mission_id, self._bounded_limit(event_limit)),
        ).fetchall()[::-1]
        action_rows = conn.execute(
            "SELECT * FROM hive_actions WHERE mission_id = ? "
            "ORDER BY created_at DESC, record_id DESC LIMIT ?",
            (mission_id, self._bounded_limit(action_limit)),
        ).fetchall()[::-1]
        decision_row = conn.execute(
            "SELECT * FROM hive_decisions WHERE mission_id = ? "
            "ORDER BY created_at DESC, decision_id DESC LIMIT 1",
            (mission_id,),
        ).fetchone()
        work_units = [
            HiveWorkUnit(
                work_unit_id=str(row["work_unit_id"]),
                mission_id=str(row["mission_id"]),
                title=str(row["title"]),
                role=str(row["role"]),
                status=str(row["status"]),
                conversation_id=str(row["conversation_id"]),
                writable_domain=str(row["writable_domain"]),
                dependencies=json.loads(str(row["dependencies_json"])),
                authority_ceiling=str(row["authority_ceiling"]),
                created_at=int(row["created_at"]),
                updated_at=int(row["updated_at"]),
                revision=int(row["revision"]),
            )
            for row in work_rows
        ]
        events = [
            HiveEvent(
                event_id=str(row["event_id"]),
                mission_id=str(row["mission_id"]),
                work_unit_id=str(row["work_unit_id"]),
                event_type=str(row["event_type"]),
                sender=str(row["sender"]),
                recipient=str(row["recipient"]),
                payload=json.loads(str(row["payload_json"])),
                context_version=int(row["context_version"]),
                created_at=int(row["created_at"]),
            )
            for row in event_rows
        ]
        actions = [
            HiveAction(
                record_id=str(row["record_id"]),
                mission_id=str(row["mission_id"]),
                action_type=str(row["action_type"]),
                actor=str(row["actor"]),
                correlation_id=str(row["correlation_id"]),
                payload=json.loads(str(row["payload_json"])),
                created_at=int(row["created_at"]),
            )
            for row in action_rows
        ]
        decision = None
        if decision_row:
            decision = HiveDecision(
                decision_id=str(decision_row["decision_id"]),
                mission_id=str(decision_row["mission_id"]),
                status=str(decision_row["status"]),
                summary=str(decision_row["summary"]),
                dissent=json.loads(str(decision_row["dissent_json"])),
                evidence=json.loads(str(decision_row["evidence_json"])),
                created_at=int(decision_row["created_at"]),
            )
        return HiveMissionSnapshot(
            ok=True,
            found=True,
            db_path=str(self.path),
            mission_id=str(mission["mission_id"]),
            title=str(mission["title"]),
            objective=str(mission["objective"]),
            lifecycle_stage=str(mission["lifecycle_stage"]),
            status=str(mission["status"]),
            authority_ceiling=str(mission["authority_ceiling"]),
            parent_context_id=str(mission["parent_context_id"]),
            cancellation_reason=str(mission["cancellation_reason"]),
            created_at=int(mission["created_at"]),
            updated_at=int(mission["updated_at"]),
            revision=int(mission["revision"]),
            work_unit_count=work_count,
            event_count=event_count,
            action_count=action_count,
            work_units=work_units,
            events=events,
            actions=actions,
            decision=decision,
        )


def default_hive_runtime_db_path() -> Path:
    configured = os.getenv(
        "NOTION2API_HIVE_RUNTIME_DB_PATH", ""
    ).strip()
    if configured:
        return Path(configured).expanduser().resolve()
    return (
        Path(__file__).resolve().parents[1]
        / ".notion2api_hive_runtime.sqlite3"
    )


_STORE_LOCK = threading.RLock()
_STORE_CACHE: dict[str, HiveRuntimeStore] = {}


def get_hive_runtime_store(
    path: str | Path | None = None,
) -> HiveRuntimeStore:
    resolved = Path(
        path or default_hive_runtime_db_path()
    ).expanduser().resolve()
    key = str(resolved)
    with _STORE_LOCK:
        store = _STORE_CACHE.get(key)
        if store is None:
            store = HiveRuntimeStore(resolved)
            _STORE_CACHE[key] = store
        return store
