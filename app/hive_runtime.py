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

from pydantic import BaseModel, Field, field_validator

from app.account_scope import AccountScopeError, canonical_account_key, require_matching_account_key
from app.governed_authorization import AUTHORITY_RANK, authority_label

HIVE_RUNTIME_SCHEMA_VERSION = 3
MAX_SNAPSHOT_ITEMS = 1000
MAX_WORK_UNITS = 64
MAX_DEPENDENCIES_PER_WORK_UNIT = 16
MAX_GRAPH_DEPTH = 16
MAX_GRAPH_FAN_OUT = 16
MAX_DELEGATED_TASKS_PER_LANE = 64
MAX_TASK_LEASE_SECONDS = 24 * 60 * 60


def _account_mission_prefix(account_key: str) -> str:
    digest = hashlib.sha256(str(account_key).encode("utf-8")).hexdigest()[:12]
    return f"acct-{digest}"


class HiveRuntimeError(RuntimeError):
    """Base error for the durable Hive runtime."""


def resolve_mission_account_scope(
    *,
    account_key: str = "",
    workspace_id: str = "",
    user_id: str = "",
    profile_name: str = "",
    account_profile: str = "",
    account_selector: str = "",
) -> dict[str, str]:
    """Require an explicit account identity for mission create/materialize."""
    workspace = str(workspace_id or "").strip()
    user = str(user_id or "").strip()
    profile = str(
        profile_name or account_profile or account_selector or ""
    ).strip()
    supplied_key = str(account_key or "").strip()
    if not workspace or not user:
        raise AccountScopeError(
            "workspace_id and user_id are required to bind a Hive mission account"
        )
    key = canonical_account_key(workspace, user)
    if supplied_key:
        key = require_matching_account_key(
            supplied_key,
            workspace_id=workspace,
            user_id=user,
            profile_name=profile,
        )
    return {
        "account_key": key,
        "workspace_id": workspace,
        "user_id": user,
        "profile_name": profile,
    }


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


class DelegatedTaskStatus(str, Enum):
    DELEGATED = "DELEGATED"
    ACCEPTED = "ACCEPTED"
    ACTIVE = "ACTIVE"
    BLOCKED = "BLOCKED"
    HANDOFF_READY = "HANDOFF_READY"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class ProjectKind(str, Enum):
    CODING = "coding"
    BUSINESS = "business"
    CREATIVE = "creative"
    HYBRID = "hybrid"


class HiveProjectContract(BaseModel):
    project_kind: ProjectKind
    scope: str = Field(min_length=1)
    exclusions: list[str] = Field(min_length=1)
    accountable_human: str = Field(min_length=1)
    source_boundary: list[str] = Field(min_length=1)
    risks: list[dict[str, Any]] = Field(min_length=1)
    acceptance_criteria: list[str] = Field(min_length=1)
    decision_gates: list[str] = Field(min_length=1)
    fan_in_owner: str = Field(min_length=1)
    closure_condition: str = Field(min_length=1)

    @field_validator("scope", "accountable_human", "fan_in_owner", "closure_condition")
    @classmethod
    def _required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("project contract text fields cannot be blank")
        return normalized

    @field_validator(
        "exclusions", "source_boundary", "acceptance_criteria", "decision_gates"
    )
    @classmethod
    def _normalized_text_list(cls, values: list[str]) -> list[str]:
        normalized = [str(item).strip() for item in values if str(item).strip()]
        if not normalized:
            raise ValueError("project contract lists cannot be blank")
        if len(normalized) != len(set(normalized)):
            raise ValueError("project contract lists cannot contain duplicates")
        return normalized


class HiveGraphReceipt(BaseModel):
    schema_version: int = 1
    validated: bool = True
    work_unit_count: int = 0
    dependency_edge_count: int = 0
    mutation_conflicts: list[list[str]] = Field(default_factory=list)
    dependency_waves: list[list[str]] = Field(default_factory=list)
    execution_waves: list[list[str]] = Field(default_factory=list)
    graph_depth: int = 0
    max_parallel_width: int = 0
    authority_ceiling: str = "A2"
    authority_level: str = "Execute bounded work (A2)"


TERMINAL_MISSION_STATUSES = {MissionStatus.CLOSED.value, MissionStatus.CANCELLED.value}
TERMINAL_WORK_STATUSES = {
    WorkUnitStatus.COMPLETED.value,
    WorkUnitStatus.FAILED.value,
    WorkUnitStatus.CANCELLED.value,
}
TERMINAL_TASK_STATUSES = {
    DelegatedTaskStatus.COMPLETED.value,
    DelegatedTaskStatus.FAILED.value,
    DelegatedTaskStatus.CANCELLED.value,
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
TASK_TRANSITIONS: dict[str, set[str]] = {
    DelegatedTaskStatus.DELEGATED.value: {
        DelegatedTaskStatus.ACCEPTED.value,
        DelegatedTaskStatus.CANCELLED.value,
    },
    DelegatedTaskStatus.ACCEPTED.value: {
        DelegatedTaskStatus.ACTIVE.value,
        DelegatedTaskStatus.BLOCKED.value,
        DelegatedTaskStatus.CANCELLED.value,
    },
    DelegatedTaskStatus.ACTIVE.value: {
        DelegatedTaskStatus.BLOCKED.value,
        DelegatedTaskStatus.HANDOFF_READY.value,
        DelegatedTaskStatus.FAILED.value,
        DelegatedTaskStatus.CANCELLED.value,
    },
    DelegatedTaskStatus.BLOCKED.value: {
        DelegatedTaskStatus.ACTIVE.value,
        DelegatedTaskStatus.FAILED.value,
        DelegatedTaskStatus.CANCELLED.value,
    },
    DelegatedTaskStatus.HANDOFF_READY.value: {
        DelegatedTaskStatus.ACTIVE.value,
        DelegatedTaskStatus.COMPLETED.value,
        DelegatedTaskStatus.CANCELLED.value,
    },
    DelegatedTaskStatus.COMPLETED.value: {DelegatedTaskStatus.COMPLETED.value},
    DelegatedTaskStatus.FAILED.value: {DelegatedTaskStatus.FAILED.value},
    DelegatedTaskStatus.CANCELLED.value: {DelegatedTaskStatus.CANCELLED.value},
}


class HiveWorkUnitSpec(BaseModel):
    work_unit_id: str | None = None
    title: str = Field(min_length=1)
    role: str = Field(min_length=1)
    conversation_id: str = ""
    writable_domain: str = ""
    dependencies: list[str] = Field(default_factory=list)
    authority_ceiling: str = "A2"


class HiveWorkUnit(BaseModel):
    work_unit_id: str
    mission_id: str
    title: str
    role: str
    status: str
    conversation_id: str = ""
    writable_domain: str = ""
    dependencies: list[str] = Field(default_factory=list)
    authority_ceiling: str = "A2"
    created_at: int
    updated_at: int
    revision: int


class HiveHandoffReceipt(BaseModel):
    summary: str = Field(min_length=1)
    deliverables: list[dict[str, Any]] = Field(min_length=1)
    evidence: list[dict[str, Any]] = Field(min_length=1)
    dissent: list[dict[str, Any]] = Field(default_factory=list)
    next_owner: str = Field(min_length=1)


class HiveDelegatedTaskSpec(BaseModel):
    task_id: str | None = None
    parent_lane_id: str = Field(min_length=1)
    objective: str = Field(min_length=1)
    scope: str = Field(min_length=1)
    exclusions: list[str] = Field(min_length=1)
    required_context: list[str] = Field(default_factory=list)
    source_boundary: list[str] = Field(default_factory=list)
    writable_domains: list[str] = Field(default_factory=list)
    authority_ceiling: str = "A2"
    dependencies: list[str] = Field(default_factory=list)
    acceptance_criteria: list[str] = Field(min_length=1)
    deliverables: list[str] = Field(min_length=1)
    evidence_requirements: list[str] = Field(min_length=1)
    checkpoint: str = Field(min_length=1)
    fan_in_owner: str = Field(min_length=1)
    closure_condition: str = Field(min_length=1)
    worker_binding: str = ""

    @field_validator(
        "parent_lane_id",
        "objective",
        "scope",
        "checkpoint",
        "fan_in_owner",
        "closure_condition",
    )
    @classmethod
    def _normalize_required_text(cls, value: str) -> str:
        normalized = str(value or "").strip()
        if not normalized:
            raise ValueError("delegated-task text fields cannot be blank")
        return normalized

    @field_validator(
        "exclusions",
        "acceptance_criteria",
        "deliverables",
        "evidence_requirements",
    )
    @classmethod
    def _normalize_required_lists(cls, values: list[str]) -> list[str]:
        normalized = [str(item).strip() for item in values if str(item).strip()]
        if not normalized:
            raise ValueError("delegated-task contract lists cannot be blank")
        if len(normalized) != len(set(normalized)):
            raise ValueError("delegated-task contract lists cannot contain duplicates")
        return normalized

    @field_validator(
        "required_context", "source_boundary", "writable_domains", "dependencies"
    )
    @classmethod
    def _normalize_optional_lists(cls, values: list[str]) -> list[str]:
        normalized = [str(item).strip() for item in values if str(item).strip()]
        if len(normalized) != len(set(normalized)):
            raise ValueError("delegated-task lists cannot contain duplicates")
        return normalized


class HiveDelegatedTask(BaseModel):
    task_id: str
    mission_id: str
    parent_lane_id: str
    objective: str
    scope: str
    exclusions: list[str] = Field(default_factory=list)
    required_context: list[str] = Field(default_factory=list)
    source_boundary: list[str] = Field(default_factory=list)
    writable_domains: list[str] = Field(default_factory=list)
    authority_ceiling: str = "A2"
    authority_level: str = "Execute bounded work (A2)"
    dependencies: list[str] = Field(default_factory=list)
    acceptance_criteria: list[str] = Field(default_factory=list)
    deliverables: list[str] = Field(default_factory=list)
    evidence_requirements: list[str] = Field(default_factory=list)
    checkpoint: str = ""
    fan_in_owner: str = ""
    closure_condition: str = ""
    worker_binding: str = ""
    status: str = DelegatedTaskStatus.DELEGATED.value
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    handoff_receipt: HiveHandoffReceipt | None = None
    execution_lease_owner: str = ""
    execution_lease_expires_at: int = 0
    created_at: int
    updated_at: int
    revision: int


class HiveTaskGraphReceipt(BaseModel):
    parent_lane_id: str
    validated: bool = True
    task_count: int = 0
    dependency_edge_count: int = 0
    mutation_conflicts: list[list[str]] = Field(default_factory=list)
    dependency_waves: list[list[str]] = Field(default_factory=list)
    execution_waves: list[list[str]] = Field(default_factory=list)
    ready_task_ids: list[str] = Field(default_factory=list)
    blocked_task_ids: list[str] = Field(default_factory=list)
    handoff_ready_task_ids: list[str] = Field(default_factory=list)
    fan_in_ready: bool = False


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
    authority_ceiling: str = "A2"
    parent_context_id: str = ""
    cancellation_reason: str = ""
    account_key: str = ""
    workspace_id: str = ""
    user_id: str = ""
    profile_name: str = ""
    created_at: int = 0
    updated_at: int = 0
    revision: int = 0
    work_unit_count: int = 0
    event_count: int = 0
    action_count: int = 0
    delegated_task_count: int = 0
    work_units: list[HiveWorkUnit] = Field(default_factory=list)
    delegated_tasks: list[HiveDelegatedTask] = Field(default_factory=list)
    task_graph_receipts: list[HiveTaskGraphReceipt] = Field(default_factory=list)
    events: list[HiveEvent] = Field(default_factory=list)
    actions: list[HiveAction] = Field(default_factory=list)
    decision: HiveDecision | None = None
    project_contract: HiveProjectContract | None = None
    graph_receipt: HiveGraphReceipt | None = None


def _normalize_domain(value: str) -> str:
    return str(value or "").strip().casefold().replace("\\", "/").rstrip("/")


def _domain_contains(parent: str, child: str) -> bool:
    parent_key = _normalize_domain(parent)
    child_key = _normalize_domain(child)
    return bool(parent_key and child_key) and (
        child_key == parent_key or child_key.startswith(f"{parent_key}/")
    )


def _domain_sets_conflict(left: set[str], right: set[str]) -> bool:
    return any(
        _domain_contains(a, b) or _domain_contains(b, a)
        for a in left
        for b in right
    )


def analyze_work_graph(
    work_units: list[tuple[str, HiveWorkUnitSpec]],
    *,
    authority_ceiling: str,
) -> HiveGraphReceipt:
    """Validate one finite mission graph and derive dependency/conflict waves."""
    if len(work_units) > MAX_WORK_UNITS:
        raise ValueError(f"A mission may contain at most {MAX_WORK_UNITS} work units")
    mission_authority = str(authority_ceiling or "A2").strip().upper()
    if mission_authority not in AUTHORITY_RANK:
        raise ValueError(f"Unknown mission authority ceiling: {mission_authority}")

    ids = {work_id for work_id, _ in work_units}
    dependencies: dict[str, list[str]] = {}
    domains: dict[str, set[str]] = {}
    dependents = {work_id: [] for work_id in ids}
    for work_id, spec in work_units:
        work_authority = (
            mission_authority
            if "authority_ceiling" not in spec.model_fields_set
            else str(spec.authority_ceiling or mission_authority).strip().upper()
        )
        if work_authority not in AUTHORITY_RANK:
            raise ValueError(f"Unknown authority ceiling for {work_id}: {work_authority}")
        if AUTHORITY_RANK[work_authority] > AUTHORITY_RANK[mission_authority]:
            raise ValueError(
                f"Work unit {work_id} authority {work_authority} exceeds mission "
                f"ceiling {mission_authority}"
            )
        deps = [str(item).strip() for item in spec.dependencies]
        if len(deps) != len(set(deps)):
            raise ValueError(f"Work unit {work_id} has duplicate dependencies")
        if len(deps) > MAX_DEPENDENCIES_PER_WORK_UNIT:
            raise ValueError(
                f"Work unit {work_id} exceeds {MAX_DEPENDENCIES_PER_WORK_UNIT} dependencies"
            )
        if work_id in deps:
            raise ValueError(f"Work unit {work_id} cannot depend on itself")
        unknown = sorted(set(deps) - ids)
        if unknown:
            raise ValueError(f"Work unit {work_id} has unknown dependencies: {unknown}")
        dependencies[work_id] = deps
        for dependency in deps:
            dependents[dependency].append(work_id)
        domains[work_id] = {
            _normalize_domain(item)
            for item in spec.writable_domain.split(";")
            if item.strip()
        }

    excessive_fan_out = {
        work_id: len(children)
        for work_id, children in dependents.items()
        if len(children) > MAX_GRAPH_FAN_OUT
    }
    if excessive_fan_out:
        raise ValueError(f"Work-unit fan-out exceeds {MAX_GRAPH_FAN_OUT}: {excessive_fan_out}")

    remaining = {work_id: len(deps) for work_id, deps in dependencies.items()}
    dependency_waves: list[list[str]] = []
    ready = sorted(work_id for work_id, count in remaining.items() if count == 0)
    visited = 0
    while ready:
        dependency_waves.append(ready)
        visited += len(ready)
        next_ready: list[str] = []
        for work_id in ready:
            for child in dependents[work_id]:
                remaining[child] -= 1
                if remaining[child] == 0:
                    next_ready.append(child)
        ready = sorted(next_ready)
    if visited != len(work_units):
        cyclic = sorted(work_id for work_id, count in remaining.items() if count > 0)
        raise ValueError(f"Work-unit dependency graph contains a cycle: {cyclic}")
    if len(dependency_waves) > MAX_GRAPH_DEPTH:
        raise ValueError(f"Work-unit graph exceeds maximum depth {MAX_GRAPH_DEPTH}")

    conflicts: list[list[str]] = []
    conflict_pairs: set[tuple[str, str]] = set()
    ordered_ids = sorted(ids)
    # ponytail: O(n^2) is bounded by MAX_WORK_UNITS; index domains if that cap grows.
    for index, left in enumerate(ordered_ids):
        for right in ordered_ids[index + 1 :]:
            if domains[left] and _domain_sets_conflict(domains[left], domains[right]):
                conflict_pairs.add((left, right))
                conflicts.append([left, right])

    execution_waves: list[list[str]] = []
    for dependency_wave in dependency_waves:
        pending = list(dependency_wave)
        while pending:
            wave: list[str] = []
            deferred: list[str] = []
            for work_id in pending:
                if any(tuple(sorted((work_id, selected))) in conflict_pairs for selected in wave):
                    deferred.append(work_id)
                else:
                    wave.append(work_id)
            execution_waves.append(wave)
            pending = deferred

    return HiveGraphReceipt(
        work_unit_count=len(work_units),
        dependency_edge_count=sum(len(items) for items in dependencies.values()),
        mutation_conflicts=conflicts,
        dependency_waves=dependency_waves,
        execution_waves=execution_waves,
        graph_depth=len(dependency_waves),
        max_parallel_width=max((len(wave) for wave in execution_waves), default=0),
        authority_ceiling=mission_authority,
        authority_level=authority_label(mission_authority),
    )


def analyze_delegated_task_graph(
    tasks: list[HiveDelegatedTask],
    *,
    parent_lane_id: str,
) -> HiveTaskGraphReceipt:
    """Validate one lane-local child DAG and derive conflict-safe execution waves."""
    if len(tasks) > MAX_DELEGATED_TASKS_PER_LANE:
        raise ValueError(
            f"Lane {parent_lane_id} may contain at most "
            f"{MAX_DELEGATED_TASKS_PER_LANE} delegated tasks"
        )
    ids = {task.task_id for task in tasks}
    if len(ids) != len(tasks):
        raise ValueError(f"Lane {parent_lane_id} contains duplicate task IDs")
    dependencies: dict[str, list[str]] = {}
    dependents = {task_id: [] for task_id in ids}
    domains = {
        task.task_id: {
            _normalize_domain(item) for item in task.writable_domains if item.strip()
        }
        for task in tasks
    }
    statuses = {task.task_id: task.status for task in tasks}
    for task in tasks:
        deps = [str(item).strip() for item in task.dependencies]
        if len(deps) != len(set(deps)):
            raise ValueError(f"Task {task.task_id} has duplicate dependencies")
        if len(deps) > MAX_DEPENDENCIES_PER_WORK_UNIT:
            raise ValueError(
                f"Task {task.task_id} exceeds "
                f"{MAX_DEPENDENCIES_PER_WORK_UNIT} dependencies"
            )
        if task.task_id in deps:
            raise ValueError(f"Task {task.task_id} cannot depend on itself")
        unknown = sorted(set(deps) - ids)
        if unknown:
            raise ValueError(f"Task {task.task_id} has unknown dependencies: {unknown}")
        dependencies[task.task_id] = deps
        for dependency in deps:
            dependents[dependency].append(task.task_id)

    remaining = {task_id: len(deps) for task_id, deps in dependencies.items()}
    dependency_waves: list[list[str]] = []
    ready = sorted(task_id for task_id, count in remaining.items() if count == 0)
    visited = 0
    while ready:
        dependency_waves.append(ready)
        visited += len(ready)
        next_ready: list[str] = []
        for task_id in ready:
            for child in dependents[task_id]:
                remaining[child] -= 1
                if remaining[child] == 0:
                    next_ready.append(child)
        ready = sorted(next_ready)
    if visited != len(tasks):
        cyclic = sorted(task_id for task_id, count in remaining.items() if count > 0)
        raise ValueError(f"Delegated-task graph contains a cycle: {cyclic}")
    if len(dependency_waves) > MAX_GRAPH_DEPTH:
        raise ValueError(f"Delegated-task graph exceeds maximum depth {MAX_GRAPH_DEPTH}")

    conflicts: list[list[str]] = []
    conflict_pairs: set[tuple[str, str]] = set()
    ordered_ids = sorted(ids)
    # ponytail: bounded by MAX_DELEGATED_TASKS_PER_LANE; index if that cap grows.
    for index, left in enumerate(ordered_ids):
        for right in ordered_ids[index + 1 :]:
            if domains[left] and _domain_sets_conflict(domains[left], domains[right]):
                conflict_pairs.add((left, right))
                conflicts.append([left, right])

    execution_waves: list[list[str]] = []
    for dependency_wave in dependency_waves:
        pending = list(dependency_wave)
        while pending:
            wave: list[str] = []
            deferred: list[str] = []
            for task_id in pending:
                if any(
                    tuple(sorted((task_id, selected))) in conflict_pairs
                    for selected in wave
                ):
                    deferred.append(task_id)
                else:
                    wave.append(task_id)
            execution_waves.append(wave)
            pending = deferred

    ready_task_ids = sorted(
        task_id
        for task_id, deps in dependencies.items()
        if statuses[task_id] == DelegatedTaskStatus.DELEGATED.value
        and all(
            statuses[dependency] == DelegatedTaskStatus.COMPLETED.value
            for dependency in deps
        )
    )
    return HiveTaskGraphReceipt(
        parent_lane_id=parent_lane_id,
        task_count=len(tasks),
        dependency_edge_count=sum(len(items) for items in dependencies.values()),
        mutation_conflicts=conflicts,
        dependency_waves=dependency_waves,
        execution_waves=execution_waves,
        ready_task_ids=ready_task_ids,
        blocked_task_ids=sorted(
            task_id
            for task_id, status in statuses.items()
            if status == DelegatedTaskStatus.BLOCKED.value
        ),
        handoff_ready_task_ids=sorted(
            task_id
            for task_id, status in statuses.items()
            if status == DelegatedTaskStatus.HANDOFF_READY.value
        ),
        fan_in_ready=bool(tasks)
        and all(status in TERMINAL_TASK_STATUSES for status in statuses.values()),
    )


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
                if version in {1, 2}:
                    conn.execute("BEGIN IMMEDIATE")
                    try:
                        if version == 1:
                            self._migrate_v1_to_v2(conn)
                        self._migrate_v2_to_v3(conn)
                        conn.execute(f"PRAGMA user_version = {HIVE_RUNTIME_SCHEMA_VERSION}")
                        conn.commit()
                    except Exception:
                        conn.rollback()
                        raise
                    self._validate_schema(conn)
                    return
                conn.executescript(
                    f"""
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
                        account_key TEXT NOT NULL DEFAULT '',
                        workspace_id TEXT NOT NULL DEFAULT '',
                        user_id TEXT NOT NULL DEFAULT '',
                        profile_name TEXT NOT NULL DEFAULT '',
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
                        authority_ceiling TEXT NOT NULL DEFAULT 'A2',
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
                        payload_json TEXT NOT NULL DEFAULT '{{}}',
                        context_version INTEGER NOT NULL DEFAULT 0,
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE TABLE hive_delegated_tasks (
                        task_id TEXT PRIMARY KEY,
                        mission_id TEXT NOT NULL REFERENCES hive_missions(mission_id) ON DELETE CASCADE,
                        parent_lane_id TEXT NOT NULL REFERENCES hive_work_units(work_unit_id) ON DELETE CASCADE,
                        objective TEXT NOT NULL,
                        scope TEXT NOT NULL,
                        exclusions_json TEXT NOT NULL DEFAULT '[]',
                        required_context_json TEXT NOT NULL DEFAULT '[]',
                        source_boundary_json TEXT NOT NULL DEFAULT '[]',
                        writable_domains_json TEXT NOT NULL DEFAULT '[]',
                        authority_ceiling TEXT NOT NULL DEFAULT 'A2',
                        dependencies_json TEXT NOT NULL DEFAULT '[]',
                        acceptance_criteria_json TEXT NOT NULL DEFAULT '[]',
                        deliverables_json TEXT NOT NULL DEFAULT '[]',
                        evidence_requirements_json TEXT NOT NULL DEFAULT '[]',
                        checkpoint TEXT NOT NULL,
                        fan_in_owner TEXT NOT NULL,
                        closure_condition TEXT NOT NULL,
                        worker_binding TEXT NOT NULL DEFAULT '',
                        status TEXT NOT NULL,
                        evidence_json TEXT NOT NULL DEFAULT '[]',
                        handoff_json TEXT NOT NULL DEFAULT '{{}}',
                        execution_lease_owner TEXT NOT NULL DEFAULT '',
                        execution_lease_expires_at INTEGER NOT NULL DEFAULT 0,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
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
                        payload_json TEXT NOT NULL DEFAULT '{{}}',
                        created_at INTEGER NOT NULL
                    );
                    CREATE INDEX idx_hive_work_units_mission_status
                        ON hive_work_units(mission_id, status, updated_at DESC);
                    CREATE INDEX idx_hive_tasks_lane_status
                        ON hive_delegated_tasks(mission_id, parent_lane_id, status, updated_at DESC);
                    CREATE INDEX idx_hive_events_mission_created
                        ON hive_events(mission_id, created_at DESC, event_id DESC);
                    CREATE INDEX idx_hive_actions_mission_created
                        ON hive_actions(mission_id, created_at DESC, record_id DESC);
                    CREATE INDEX idx_hive_decisions_mission_created
                        ON hive_decisions(mission_id, created_at DESC, decision_id DESC);
                    CREATE INDEX idx_hive_missions_account_key
                        ON hive_missions(account_key, updated_at DESC);
                    PRAGMA user_version = {HIVE_RUNTIME_SCHEMA_VERSION};
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
    def _migrate_v1_to_v2(conn: sqlite3.Connection) -> None:
        columns = {
            str(row[1]) for row in conn.execute("PRAGMA table_info(hive_missions)").fetchall()
        }
        for name in ("account_key", "workspace_id", "user_id", "profile_name"):
            if name not in columns:
                conn.execute(
                    f"ALTER TABLE hive_missions ADD COLUMN {name} TEXT NOT NULL DEFAULT ''"
                )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_hive_missions_account_key
                ON hive_missions(account_key, updated_at DESC)
            """
        )

    @staticmethod
    def _migrate_v2_to_v3(conn: sqlite3.Connection) -> None:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS hive_delegated_tasks (
                task_id TEXT PRIMARY KEY,
                mission_id TEXT NOT NULL REFERENCES hive_missions(mission_id) ON DELETE CASCADE,
                parent_lane_id TEXT NOT NULL REFERENCES hive_work_units(work_unit_id) ON DELETE CASCADE,
                objective TEXT NOT NULL,
                scope TEXT NOT NULL,
                exclusions_json TEXT NOT NULL DEFAULT '[]',
                required_context_json TEXT NOT NULL DEFAULT '[]',
                source_boundary_json TEXT NOT NULL DEFAULT '[]',
                writable_domains_json TEXT NOT NULL DEFAULT '[]',
                authority_ceiling TEXT NOT NULL DEFAULT 'A2',
                dependencies_json TEXT NOT NULL DEFAULT '[]',
                acceptance_criteria_json TEXT NOT NULL DEFAULT '[]',
                deliverables_json TEXT NOT NULL DEFAULT '[]',
                evidence_requirements_json TEXT NOT NULL DEFAULT '[]',
                checkpoint TEXT NOT NULL,
                fan_in_owner TEXT NOT NULL,
                closure_condition TEXT NOT NULL,
                worker_binding TEXT NOT NULL DEFAULT '',
                status TEXT NOT NULL,
                evidence_json TEXT NOT NULL DEFAULT '[]',
                handoff_json TEXT NOT NULL DEFAULT '{}',
                execution_lease_owner TEXT NOT NULL DEFAULT '',
                execution_lease_expires_at INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                revision INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_hive_tasks_lane_status
                ON hive_delegated_tasks(
                    mission_id, parent_lane_id, status, updated_at DESC
                );
            """
        )

    @staticmethod
    def _validate_schema(conn: sqlite3.Connection) -> None:
        required = {
            "hive_missions", "hive_work_units", "hive_events",
            "hive_decisions", "hive_actions", "hive_delegated_tasks",
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
                "Hive runtime database declares schema version "
                f"{HIVE_RUNTIME_SCHEMA_VERSION} but is missing tables: "
                f"{sorted(missing)}"
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

    @staticmethod
    def _task_from_row(row: sqlite3.Row) -> HiveDelegatedTask:
        handoff = json.loads(str(row["handoff_json"]))
        authority = str(row["authority_ceiling"])
        return HiveDelegatedTask(
            task_id=str(row["task_id"]),
            mission_id=str(row["mission_id"]),
            parent_lane_id=str(row["parent_lane_id"]),
            objective=str(row["objective"]),
            scope=str(row["scope"]),
            exclusions=json.loads(str(row["exclusions_json"])),
            required_context=json.loads(str(row["required_context_json"])),
            source_boundary=json.loads(str(row["source_boundary_json"])),
            writable_domains=json.loads(str(row["writable_domains_json"])),
            authority_ceiling=authority,
            authority_level=authority_label(authority),
            dependencies=json.loads(str(row["dependencies_json"])),
            acceptance_criteria=json.loads(str(row["acceptance_criteria_json"])),
            deliverables=json.loads(str(row["deliverables_json"])),
            evidence_requirements=json.loads(
                str(row["evidence_requirements_json"])
            ),
            checkpoint=str(row["checkpoint"]),
            fan_in_owner=str(row["fan_in_owner"]),
            closure_condition=str(row["closure_condition"]),
            worker_binding=str(row["worker_binding"]),
            status=str(row["status"]),
            evidence=json.loads(str(row["evidence_json"])),
            handoff_receipt=(
                HiveHandoffReceipt.model_validate(handoff) if handoff else None
            ),
            execution_lease_owner=str(row["execution_lease_owner"]),
            execution_lease_expires_at=int(row["execution_lease_expires_at"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    def create_mission(
        self,
        *,
        title: str,
        objective: str,
        lifecycle_stage: str,
        work_units: list[HiveWorkUnitSpec] | None = None,
        authority_ceiling: str = "A2",
        parent_context_id: str = "",
        mission_id: str | None = None,
        idempotency_key: str | None = None,
        actor: str = "notion2api",
        account_key: str = "",
        workspace_id: str = "",
        user_id: str = "",
        profile_name: str = "",
        account_profile: str = "",
        account_selector: str = "",
        project_contract: HiveProjectContract | dict[str, Any] | None = None,
    ) -> HiveMissionSnapshot:
        specs = [
            item if isinstance(item, HiveWorkUnitSpec)
            else HiveWorkUnitSpec.model_validate(item)
            for item in (work_units or [])
        ]
        account = resolve_mission_account_scope(
            account_key=account_key,
            workspace_id=workspace_id,
            user_id=user_id,
            profile_name=profile_name,
            account_profile=account_profile,
            account_selector=account_selector,
        )
        if mission_id is None:
            mission_key = (
                f"{_account_mission_prefix(account['account_key'])}-hive-{uuid.uuid4().hex[:12]}"
            )
        else:
            mission_key = str(mission_id).strip()
        normalized_specs: list[tuple[str, HiveWorkUnitSpec]] = []
        seen_work_ids: set[str] = set()
        for index, spec in enumerate(specs, start=1):
            work_id = str(
                spec.work_unit_id or f"{mission_key}-wu-{index:03d}"
            ).strip()
            if not work_id or work_id in seen_work_ids:
                raise ValueError(f"Duplicate or empty work_unit_id: {work_id!r}")
            seen_work_ids.add(work_id)
            if "authority_ceiling" not in spec.model_fields_set:
                spec = spec.model_copy(
                    update={
                        "authority_ceiling": str(authority_ceiling or "A2")
                        .strip()
                        .upper()
                    }
                )
            normalized_specs.append((work_id, spec))
        contract = (
            project_contract
            if isinstance(project_contract, HiveProjectContract)
            else HiveProjectContract.model_validate(project_contract)
            if project_contract is not None
            else None
        )
        if contract is not None and not str(parent_context_id or "").strip():
            raise ValueError("parent_context_id is required for a governed project contract")
        graph_receipt = analyze_work_graph(
            normalized_specs,
            authority_ceiling=authority_ceiling,
        )
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
            "authority_ceiling": graph_receipt.authority_ceiling,
            "parent_context_id": str(parent_context_id or "").strip(),
            "mission_id": mission_key,
            "account_key": account["account_key"],
            "workspace_id": account["workspace_id"],
            "user_id": account["user_id"],
            "profile_name": account["profile_name"],
            "project_contract": (
                contract.model_dump(mode="json") if contract is not None else None
            ),
            "graph_receipt": graph_receipt.model_dump(mode="json"),
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
                    account_key, workspace_id, user_id, profile_name,
                    idempotency_key, request_sha256, created_at, updated_at, revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    mission_key, request["title"], request["objective"],
                    request["lifecycle_stage"], MissionStatus.ACTIVE.value,
                    request["authority_ceiling"], request["parent_context_id"],
                    request["account_key"], request["workspace_id"],
                    request["user_id"], request["profile_name"],
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
                    "project_contract": request["project_contract"],
                    "graph_receipt": request["graph_receipt"],
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
                if target == WorkUnitStatus.COMPLETED.value:
                    task_statuses = [
                        str(item["status"])
                        for item in conn.execute(
                            "SELECT status FROM hive_delegated_tasks "
                            "WHERE mission_id = ? AND parent_lane_id = ?",
                            (mission_id, request["work_unit_id"]),
                        ).fetchall()
                    ]
                    if task_statuses and not all(
                        item in TERMINAL_TASK_STATUSES for item in task_statuses
                    ):
                        raise HiveTransitionError(
                            "A lane cannot complete before delegated-task fan-in is ready"
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

    def delegate_tasks(
        self,
        *,
        mission_id: str,
        tasks: list[HiveDelegatedTaskSpec | dict[str, Any]],
        actor: str = "notion2api",
        expected_mission_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        specs = [
            item
            if isinstance(item, HiveDelegatedTaskSpec)
            else HiveDelegatedTaskSpec.model_validate(item)
            for item in tasks
        ]
        if not specs:
            raise ValueError("At least one delegated task is required")
        request = {
            "mission_id": self._required(mission_id, "mission_id"),
            "tasks": [item.model_dump(mode="json") for item in specs],
            "actor": self._required(actor, "actor"),
            "expected_mission_revision": expected_mission_revision,
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            mission = conn.execute(
                "SELECT status, revision, authority_ceiling FROM hive_missions "
                "WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()
            if not mission:
                raise HiveNotFoundError(f"Mission not found: {mission_id}")
            if idempotency_key:
                existing_event = conn.execute(
                    "SELECT request_sha256 FROM hive_events WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing_event:
                    if str(existing_event["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Delegation idempotency key was already used with "
                            "different content."
                        )
                    return self._snapshot(conn, mission_id)
            if str(mission["status"]) in TERMINAL_MISSION_STATUSES:
                raise HiveTransitionError(
                    "A terminal mission cannot accept delegated tasks."
                )
            if (
                expected_mission_revision is not None
                and int(mission["revision"]) != int(expected_mission_revision)
            ):
                raise HiveTransitionError(
                    "Stale mission revision: expected "
                    f"{expected_mission_revision}, current "
                    f"{int(mission['revision'])}."
                )

            lanes = {
                str(row["work_unit_id"]): row
                for row in conn.execute(
                    "SELECT * FROM hive_work_units WHERE mission_id = ?",
                    (mission_id,),
                ).fetchall()
            }
            opened = conn.execute(
                "SELECT payload_json FROM hive_events WHERE mission_id = ? "
                "AND event_type = 'MISSION_OPENED' "
                "ORDER BY created_at, event_id LIMIT 1",
                (mission_id,),
            ).fetchone()
            opened_payload = json.loads(str(opened["payload_json"])) if opened else {}
            project_contract = opened_payload.get("project_contract") or {}
            mission_sources = [
                str(item).strip()
                for item in project_contract.get("source_boundary") or []
                if str(item).strip()
            ]
            mission_source_keys = {item.casefold() for item in mission_sources}
            existing_rows = conn.execute(
                "SELECT * FROM hive_delegated_tasks WHERE mission_id = ?",
                (mission_id,),
            ).fetchall()
            existing_tasks = [self._task_from_row(row) for row in existing_rows]
            existing_ids = {task.task_id for task in existing_tasks}
            now = self._now_ms()
            normalized: list[HiveDelegatedTask] = []
            batch_ids: set[str] = set()
            mission_authority = str(mission["authority_ceiling"]).upper()
            for index, spec in enumerate(specs, start=1):
                lane_id = self._required(spec.parent_lane_id, "parent_lane_id")
                lane = lanes.get(lane_id)
                if lane is None:
                    raise HiveNotFoundError(f"Parent lane not found: {lane_id}")
                if str(lane["status"]) in TERMINAL_WORK_STATUSES:
                    raise HiveTransitionError(
                        f"Terminal lane {lane_id} cannot accept delegated tasks"
                    )
                task_id = str(
                    spec.task_id or f"{lane_id}-task-{len(existing_ids) + index:03d}"
                ).strip()
                if not task_id or task_id in existing_ids or task_id in batch_ids:
                    raise ValueError(f"Duplicate or empty task_id: {task_id!r}")
                if conn.execute(
                    "SELECT 1 FROM hive_delegated_tasks WHERE task_id = ?",
                    (task_id,),
                ).fetchone():
                    raise ValueError(f"Delegated task already exists: {task_id}")
                batch_ids.add(task_id)

                lane_authority = str(lane["authority_ceiling"]).upper()
                task_authority = (
                    lane_authority
                    if "authority_ceiling" not in spec.model_fields_set
                    else str(spec.authority_ceiling).strip().upper()
                )
                if task_authority not in AUTHORITY_RANK:
                    raise ValueError(
                        f"Unknown authority level for task {task_id}: {task_authority}"
                    )
                if (
                    AUTHORITY_RANK[task_authority] > AUTHORITY_RANK[lane_authority]
                    or AUTHORITY_RANK[lane_authority]
                    > AUTHORITY_RANK[mission_authority]
                ):
                    raise ValueError(
                        f"Task {task_id} authority must not exceed lane {lane_id} "
                        "or mission authority"
                    )

                sources = [
                    str(item).strip()
                    for item in (spec.source_boundary or mission_sources)
                    if str(item).strip()
                ]
                if not sources:
                    raise ValueError(f"Task {task_id} requires a source boundary")
                if mission_source_keys and not {
                    item.casefold() for item in sources
                }.issubset(mission_source_keys):
                    raise ValueError(
                        f"Task {task_id} source boundary exceeds the mission boundary"
                    )

                lane_domains = {
                    _normalize_domain(item)
                    for item in str(lane["writable_domain"]).split(";")
                    if item.strip()
                }
                task_domains = [
                    _normalize_domain(item)
                    for item in (spec.writable_domains or sorted(lane_domains))
                    if item.strip()
                ]
                if any(
                    not any(_domain_contains(parent, child) for parent in lane_domains)
                    for child in task_domains
                ):
                    raise ValueError(
                        f"Task {task_id} writable domain exceeds lane {lane_id}"
                    )
                normalized.append(
                    HiveDelegatedTask(
                        task_id=task_id,
                        mission_id=mission_id,
                        parent_lane_id=lane_id,
                        objective=self._required(spec.objective, "task objective"),
                        scope=self._required(spec.scope, "task scope"),
                        exclusions=list(spec.exclusions),
                        required_context=list(spec.required_context),
                        source_boundary=sources,
                        writable_domains=task_domains,
                        authority_ceiling=task_authority,
                        authority_level=authority_label(task_authority),
                        dependencies=list(spec.dependencies),
                        acceptance_criteria=list(spec.acceptance_criteria),
                        deliverables=list(spec.deliverables),
                        evidence_requirements=list(spec.evidence_requirements),
                        checkpoint=self._required(spec.checkpoint, "checkpoint"),
                        fan_in_owner=self._required(
                            spec.fan_in_owner, "fan_in_owner"
                        ),
                        closure_condition=self._required(
                            spec.closure_condition, "closure_condition"
                        ),
                        worker_binding=str(spec.worker_binding or "").strip(),
                        status=DelegatedTaskStatus.DELEGATED.value,
                        created_at=now,
                        updated_at=now,
                        revision=1,
                    )
                )

            all_tasks = existing_tasks + normalized
            for lane_id in sorted({task.parent_lane_id for task in normalized}):
                analyze_delegated_task_graph(
                    [task for task in all_tasks if task.parent_lane_id == lane_id],
                    parent_lane_id=lane_id,
                )

            for index, task in enumerate(normalized):
                conn.execute(
                    """
                    INSERT INTO hive_delegated_tasks(
                        task_id, mission_id, parent_lane_id, objective, scope,
                        exclusions_json, required_context_json, source_boundary_json,
                        writable_domains_json, authority_ceiling, dependencies_json,
                        acceptance_criteria_json, deliverables_json,
                        evidence_requirements_json, checkpoint, fan_in_owner,
                        closure_condition, worker_binding, status, evidence_json,
                        handoff_json, execution_lease_owner,
                        execution_lease_expires_at,
                        created_at, updated_at, revision
                    ) VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        '[]', '{}', '', 0, ?, ?, 1
                    )
                    """,
                    (
                        task.task_id,
                        mission_id,
                        task.parent_lane_id,
                        task.objective,
                        task.scope,
                        self._json(task.exclusions),
                        self._json(task.required_context),
                        self._json(task.source_boundary),
                        self._json(task.writable_domains),
                        task.authority_ceiling,
                        self._json(task.dependencies),
                        self._json(task.acceptance_criteria),
                        self._json(task.deliverables),
                        self._json(task.evidence_requirements),
                        task.checkpoint,
                        task.fan_in_owner,
                        task.closure_condition,
                        task.worker_binding,
                        task.status,
                        now,
                        now,
                    ),
                )
                self._insert_event(
                    conn,
                    mission_id=mission_id,
                    work_unit_id=task.parent_lane_id,
                    event_type="TASK_DELEGATED",
                    sender=request["actor"],
                    recipient=task.worker_binding or task.parent_lane_id,
                    payload={
                        "task_id": task.task_id,
                        "status": task.status,
                        "authority_level": task.authority_level,
                    },
                    idempotency_key=idempotency_key if index == 0 else None,
                    request_sha256=fingerprint,
                    created_at=now,
                )
            conn.execute(
                "UPDATE hive_missions SET updated_at = ?, revision = revision + 1 "
                "WHERE mission_id = ?",
                (now, mission_id),
            )
            self._record_action(
                conn,
                mission_id=mission_id,
                action_type="TASKS_DELEGATED",
                actor=request["actor"],
                payload=request,
                correlation_id=idempotency_key or "",
                created_at=now,
            )
            return self._snapshot(conn, mission_id)

    def transition_delegated_task(
        self,
        *,
        mission_id: str,
        task_id: str,
        status: str,
        actor: str,
        worker_binding: str = "",
        lease_seconds: int = 900,
        evidence: list[dict[str, Any]] | None = None,
        handoff_receipt: HiveHandoffReceipt | dict[str, Any] | None = None,
        expected_mission_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> HiveMissionSnapshot:
        target = self._required(status, "status").upper()
        receipt = (
            handoff_receipt
            if isinstance(handoff_receipt, HiveHandoffReceipt)
            else HiveHandoffReceipt.model_validate(handoff_receipt)
            if handoff_receipt is not None
            else None
        )
        request = {
            "mission_id": self._required(mission_id, "mission_id"),
            "task_id": self._required(task_id, "task_id"),
            "status": target,
            "actor": self._required(actor, "actor"),
            "worker_binding": str(worker_binding or "").strip(),
            "lease_seconds": int(lease_seconds),
            "evidence": evidence or [],
            "handoff_receipt": receipt.model_dump(mode="json") if receipt else None,
            "expected_mission_revision": expected_mission_revision,
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            mission = conn.execute(
                "SELECT status, revision FROM hive_missions WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()
            if not mission:
                raise HiveNotFoundError(f"Mission not found: {mission_id}")
            if idempotency_key:
                existing_event = conn.execute(
                    "SELECT request_sha256 FROM hive_events WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing_event:
                    if str(existing_event["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Task transition idempotency key was already used with "
                            "different content."
                        )
                    return self._snapshot(conn, mission_id)
            if str(mission["status"]) in TERMINAL_MISSION_STATUSES:
                raise HiveTransitionError(
                    "A terminal mission cannot transition delegated tasks."
                )
            if (
                expected_mission_revision is not None
                and int(mission["revision"]) != int(expected_mission_revision)
            ):
                raise HiveTransitionError(
                    "Stale mission revision: expected "
                    f"{expected_mission_revision}, current "
                    f"{int(mission['revision'])}."
                )
            row = conn.execute(
                "SELECT * FROM hive_delegated_tasks "
                "WHERE mission_id = ? AND task_id = ?",
                (mission_id, task_id),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Delegated task not found: {task_id}")
            task = self._task_from_row(row)
            if target not in TASK_TRANSITIONS.get(task.status, set()):
                raise HiveTransitionError(
                    f"Illegal delegated-task transition: {task.status} -> {target}"
                )

            if target in {
                DelegatedTaskStatus.ACCEPTED.value,
                DelegatedTaskStatus.ACTIVE.value,
            }:
                dependency_rows = conn.execute(
                    "SELECT task_id, status FROM hive_delegated_tasks "
                    "WHERE mission_id = ? AND parent_lane_id = ?",
                    (mission_id, task.parent_lane_id),
                ).fetchall()
                dependency_status = {
                    str(item["task_id"]): str(item["status"])
                    for item in dependency_rows
                }
                unmet = [
                    dependency
                    for dependency in task.dependencies
                    if dependency_status.get(dependency)
                    != DelegatedTaskStatus.COMPLETED.value
                ]
                if unmet:
                    raise HiveTransitionError(
                        f"Task {task_id} has incomplete dependencies: {unmet}"
                    )

            now = self._now_ms()
            owner = request["worker_binding"] or task.worker_binding or request["actor"]
            execution_lease_owner = ""
            execution_lease_expires_at = 0
            if target in {
                DelegatedTaskStatus.ACCEPTED.value,
                DelegatedTaskStatus.ACTIVE.value,
            }:
                seconds = int(request["lease_seconds"])
                if seconds <= 0 or seconds > MAX_TASK_LEASE_SECONDS:
                    raise ValueError(
                        f"lease_seconds must be between 1 and {MAX_TASK_LEASE_SECONDS}"
                    )
                if (
                    task.execution_lease_owner
                    and task.execution_lease_owner != owner
                    and task.execution_lease_expires_at > now
                ):
                    raise HiveTransitionError(
                        f"Task {task_id} execution lease is held by "
                        f"{task.execution_lease_owner}"
                    )
                execution_lease_owner = owner
                execution_lease_expires_at = now + seconds * 1000

            if target == DelegatedTaskStatus.ACTIVE.value:
                active_rows = conn.execute(
                    "SELECT * FROM hive_delegated_tasks WHERE mission_id = ? "
                    "AND parent_lane_id = ? AND task_id <> ? "
                    "AND status = ?",
                    (
                        mission_id,
                        task.parent_lane_id,
                        task_id,
                        DelegatedTaskStatus.ACTIVE.value,
                    ),
                ).fetchall()
                task_domains = {
                    _normalize_domain(item) for item in task.writable_domains
                }
                conflicting = [
                    other.task_id
                    for other in (self._task_from_row(item) for item in active_rows)
                    if task_domains
                    and _domain_sets_conflict(
                        task_domains,
                        {_normalize_domain(item) for item in other.writable_domains},
                    )
                ]
                if conflicting:
                    raise HiveTransitionError(
                        f"Task {task_id} has active writable-domain conflicts: "
                        f"{sorted(conflicting)}"
                    )

            existing_evidence = list(task.evidence)
            updated_evidence = existing_evidence + list(request["evidence"])
            if target == DelegatedTaskStatus.HANDOFF_READY.value:
                if receipt is None or not updated_evidence:
                    raise ValueError(
                        "HANDOFF_READY requires a typed handoff receipt and evidence"
                    )
            stored_receipt = receipt or task.handoff_receipt
            if target == DelegatedTaskStatus.COMPLETED.value and stored_receipt is None:
                raise HiveTransitionError(
                    "A delegated task cannot complete without a handoff receipt"
                )
            conn.execute(
                """
                UPDATE hive_delegated_tasks
                SET status = ?, worker_binding = ?, evidence_json = ?,
                    handoff_json = ?, execution_lease_owner = ?,
                    execution_lease_expires_at = ?,
                    updated_at = ?, revision = revision + 1
                WHERE mission_id = ? AND task_id = ?
                """,
                (
                    target,
                    owner if target in {
                        DelegatedTaskStatus.ACCEPTED.value,
                        DelegatedTaskStatus.ACTIVE.value,
                    } else task.worker_binding or owner,
                    self._json(updated_evidence),
                    self._json(
                        stored_receipt.model_dump(mode="json")
                        if stored_receipt is not None
                        else {}
                    ),
                    execution_lease_owner,
                    execution_lease_expires_at,
                    now,
                    mission_id,
                    task_id,
                ),
            )
            event_type = {
                DelegatedTaskStatus.ACCEPTED.value: "TASK_ACCEPTED",
                DelegatedTaskStatus.ACTIVE.value: "TASK_ACTIVE",
                DelegatedTaskStatus.BLOCKED.value: "TASK_BLOCKED",
                DelegatedTaskStatus.HANDOFF_READY.value: "HANDOFF_READY",
                DelegatedTaskStatus.COMPLETED.value: "HANDOFF_ACCEPTED",
                DelegatedTaskStatus.FAILED.value: "TASK_FAILED",
                DelegatedTaskStatus.CANCELLED.value: "TASK_CANCELLED",
            }[target]
            self._insert_event(
                conn,
                mission_id=mission_id,
                work_unit_id=task.parent_lane_id,
                event_type=event_type,
                sender=request["actor"],
                recipient=task.fan_in_owner,
                payload={
                    "task_id": task_id,
                    "status": target,
                    "worker_binding": owner,
                    "execution_lease_owner": execution_lease_owner,
                    "execution_lease_expires_at": execution_lease_expires_at,
                    "evidence": request["evidence"],
                    "handoff_receipt": request["handoff_receipt"],
                },
                idempotency_key=idempotency_key,
                request_sha256=fingerprint,
                created_at=now,
            )
            lane_statuses = [
                str(item["status"])
                for item in conn.execute(
                    "SELECT status FROM hive_delegated_tasks "
                    "WHERE mission_id = ? AND parent_lane_id = ?",
                    (mission_id, task.parent_lane_id),
                ).fetchall()
            ]
            if lane_statuses and all(
                item in TERMINAL_TASK_STATUSES for item in lane_statuses
            ):
                self._insert_event(
                    conn,
                    mission_id=mission_id,
                    work_unit_id=task.parent_lane_id,
                    event_type="LANE_FAN_IN_READY",
                    sender="hive-runtime",
                    recipient=task.fan_in_owner,
                    payload={"task_id": task_id, "task_count": len(lane_statuses)},
                    created_at=now,
                )
            conn.execute(
                "UPDATE hive_missions SET updated_at = ?, revision = revision + 1 "
                "WHERE mission_id = ?",
                (now, mission_id),
            )
            self._record_action(
                conn,
                mission_id=mission_id,
                action_type="TASK_TRANSITIONED",
                actor=request["actor"],
                payload=request,
                correlation_id=idempotency_key or task_id,
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
            conn.execute(
                """
                UPDATE hive_delegated_tasks
                SET status = ?, execution_lease_owner = '',
                    execution_lease_expires_at = 0,
                    updated_at = ?, revision = revision + 1
                WHERE mission_id = ? AND status NOT IN (?, ?, ?)
                """,
                (
                    DelegatedTaskStatus.CANCELLED.value,
                    now,
                    mission_id,
                    DelegatedTaskStatus.COMPLETED.value,
                    DelegatedTaskStatus.FAILED.value,
                    DelegatedTaskStatus.CANCELLED.value,
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
                    UPDATE hive_delegated_tasks
                    SET status = ?, execution_lease_owner = '',
                        execution_lease_expires_at = 0,
                        updated_at = ?, revision = revision + 1
                    WHERE mission_id = ? AND status NOT IN (?, ?, ?)
                    """,
                    (
                        DelegatedTaskStatus.CANCELLED.value,
                        now,
                        mission_id,
                        DelegatedTaskStatus.COMPLETED.value,
                        DelegatedTaskStatus.FAILED.value,
                        DelegatedTaskStatus.CANCELLED.value,
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
        delegated_task_count = int(
            conn.execute(
                "SELECT COUNT(*) FROM hive_delegated_tasks WHERE mission_id = ?",
                (mission_id,),
            ).fetchone()[0]
        )
        work_rows = conn.execute(
            "SELECT * FROM hive_work_units WHERE mission_id = ? "
            "ORDER BY created_at, work_unit_id",
            (mission_id,),
        ).fetchall()
        task_rows = conn.execute(
            "SELECT * FROM hive_delegated_tasks WHERE mission_id = ? "
            "ORDER BY created_at, task_id",
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
        mission_opened_row = conn.execute(
            "SELECT payload_json FROM hive_events WHERE mission_id = ? "
            "AND event_type = 'MISSION_OPENED' ORDER BY created_at, event_id LIMIT 1",
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
        delegated_tasks = [self._task_from_row(row) for row in task_rows]
        task_graph_receipts = [
            analyze_delegated_task_graph(
                [
                    task
                    for task in delegated_tasks
                    if task.parent_lane_id == lane_id
                ],
                parent_lane_id=lane_id,
            )
            for lane_id in sorted({task.parent_lane_id for task in delegated_tasks})
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
        opened_payload = (
            json.loads(str(mission_opened_row["payload_json"]))
            if mission_opened_row
            else {}
        )
        project_contract = (
            HiveProjectContract.model_validate(opened_payload["project_contract"])
            if opened_payload.get("project_contract")
            else None
        )
        graph_receipt = (
            HiveGraphReceipt.model_validate(opened_payload["graph_receipt"])
            if opened_payload.get("graph_receipt")
            else None
        )
        mission_keys = set(mission.keys())
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
            account_key=str(mission["account_key"] if "account_key" in mission_keys else ""),
            workspace_id=str(mission["workspace_id"] if "workspace_id" in mission_keys else ""),
            user_id=str(mission["user_id"] if "user_id" in mission_keys else ""),
            profile_name=str(mission["profile_name"] if "profile_name" in mission_keys else ""),
            created_at=int(mission["created_at"]),
            updated_at=int(mission["updated_at"]),
            revision=int(mission["revision"]),
            work_unit_count=work_count,
            event_count=event_count,
            action_count=action_count,
            delegated_task_count=delegated_task_count,
            work_units=work_units,
            delegated_tasks=delegated_tasks,
            task_graph_receipts=task_graph_receipts,
            events=events,
            actions=actions,
            decision=decision,
            project_contract=project_contract,
            graph_receipt=graph_receipt,
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
