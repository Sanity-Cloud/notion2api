"""Hierarchical multi-thread coordination for AIgentBee Hive missions.

One mission has one accountable leader conversation and one independent durable
conversation per worker lane.  The leader plans and coordinates; workers execute
concurrently and communicate through typed Hive events.  Conversation state,
MCP discovery, and tool receipts remain worker-scoped.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
from typing import Any, Iterable, Mapping, Sequence
import uuid

from pydantic import BaseModel, Field, model_validator

from app.account_scope import (
    account_key_matches,
    canonical_account_key,
    require_matching_account_key,
)
from app.hive_runtime import HiveMissionSnapshot, HiveRuntimeStore


class ThreadKind(str, Enum):
    LEADER = "leader"
    WORKER = "worker"
    REVIEWER = "reviewer"


class CrossThreadMessageKind(str, Enum):
    TASK_ASSIGNMENT = "TASK_ASSIGNMENT"
    SYNC_PULSE = "SYNC_PULSE"
    QUESTION = "QUESTION"
    ANSWER = "ANSWER"
    EVIDENCE = "EVIDENCE"
    RISK = "RISK"
    DISSENT = "DISSENT"
    DEPENDENCY_READY = "DEPENDENCY_READY"
    REPRIORITIZE = "REPRIORITIZE"
    PAUSE = "PAUSE"
    RESUME = "RESUME"
    COMPLETION = "COMPLETION"
    FAN_IN = "FAN_IN"


class MultithreadContractError(ValueError):
    """Raised when thread isolation or coordination invariants are violated."""


_FORBIDDEN_CREDENTIAL_KEYS = {
    "authorization",
    "cookie",
    "credentials",
    "password",
    "secret",
    "set-cookie",
    "token",
    "token_v2",
    "x-api-key",
    "x-csrf-token",
}


def _required(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise MultithreadContractError(f"{field_name} is required")
    return text


def _stable_slug(value: str, limit: int = 48) -> str:
    clean = "-".join(part for part in value.lower().replace("_", "-").split("-") if part)
    return (clean or "thread")[:limit]


def leader_conversation_id(mission_id: str) -> str:
    mission = _required(mission_id, "mission_id")
    digest = hashlib.sha256(mission.encode("utf-8")).hexdigest()[:12]
    return f"aigentbee:leader:{_stable_slug(mission)}:{digest}"


def worker_conversation_id(plan_id: str, worker_id: str) -> str:
    plan = _required(plan_id, "plan_id")
    worker = _required(worker_id, "worker_id")
    digest = hashlib.sha256(f"{plan}\0{worker}".encode("utf-8")).hexdigest()[:12]
    return f"aigentbee:worker:{_stable_slug(worker)}:{digest}"


class ThreadBinding(BaseModel):
    mission_id: str = Field(min_length=1)
    work_unit_id: str = ""
    worker_id: str = ""
    thread_kind: ThreadKind
    conversation_id: str = Field(min_length=1)
    leader_conversation_id: str = Field(min_length=1)
    profile_id: str = Field(min_length=1)
    notion_user_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    account_key: str = ""
    authority_ceiling: str = "A0"
    writable_domains: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_hierarchy(self) -> "ThreadBinding":
        expected_leader = leader_conversation_id(self.mission_id)
        if self.leader_conversation_id != expected_leader:
            raise MultithreadContractError("leader_conversation_id does not match mission")
        if self.thread_kind == ThreadKind.LEADER:
            if self.conversation_id != expected_leader:
                raise MultithreadContractError("leader must use the mission leader conversation")
            if self.work_unit_id or self.worker_id:
                raise MultithreadContractError("leader binding cannot impersonate a worker lane")
        else:
            if not self.work_unit_id or not self.worker_id:
                raise MultithreadContractError("worker bindings require work_unit_id and worker_id")
            if self.conversation_id == self.leader_conversation_id:
                raise MultithreadContractError("worker and leader conversations must be independent")
        resolved = canonical_account_key(self.workspace_id, self.notion_user_id)
        if self.account_key:
            require_matching_account_key(
                self.account_key,
                workspace_id=self.workspace_id,
                user_id=self.notion_user_id,
                profile_name=self.profile_id,
            )
        else:
            self.account_key = resolved
        return self

    @property
    def identity_tuple(self) -> tuple[str, ...]:
        return (
            self.mission_id,
            self.work_unit_id,
            self.worker_id,
            self.conversation_id,
            self.account_key,
            self.profile_id,
            self.notion_user_id,
            self.workspace_id,
        )


class MCPInvocationEnvelope(BaseModel):
    mission_id: str = Field(min_length=1)
    work_unit_id: str = Field(min_length=1)
    worker_id: str = Field(min_length=1)
    conversation_id: str = Field(min_length=1)
    notion_thread_id: str = Field(min_length=1)
    profile_id: str = Field(min_length=1)
    notion_user_id: str = Field(min_length=1)
    workspace_id: str = Field(min_length=1)
    account_key: str = ""
    mcp_server_id: str = Field(min_length=1)
    tool_name: str = Field(min_length=1)
    request_id: str = Field(min_length=1)
    receipt_id: str = Field(min_length=1)
    authority_ceiling: str = "A0"
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_no_credentials(self) -> "MCPInvocationEnvelope":
        _assert_no_credentials(self.payload)
        resolved = canonical_account_key(self.workspace_id, self.notion_user_id)
        if self.account_key:
            require_matching_account_key(
                self.account_key,
                workspace_id=self.workspace_id,
                user_id=self.notion_user_id,
                profile_name=self.profile_id,
            )
        else:
            self.account_key = resolved
        return self

    def validate_binding(self, binding: ThreadBinding) -> None:
        if binding.thread_kind == ThreadKind.LEADER:
            raise MultithreadContractError("leader binding cannot be used as a worker MCP identity")
        expected = (
            binding.mission_id,
            binding.work_unit_id,
            binding.worker_id,
            binding.conversation_id,
            binding.account_key,
            binding.profile_id,
            binding.notion_user_id,
            binding.workspace_id,
        )
        actual = (
            self.mission_id,
            self.work_unit_id,
            self.worker_id,
            self.conversation_id,
            self.account_key,
            self.profile_id,
            self.notion_user_id,
            self.workspace_id,
        )
        if actual != expected:
            raise MultithreadContractError("MCP invocation identity does not match worker binding")


class BeeNotionCallEnvelope(BaseModel):
    """Validated bee→bee / bee→lane notion2api chat call contract."""

    account_key: str = Field(min_length=1)
    conversation_id: str = Field(min_length=1)
    mission_id: str = Field(min_length=1)
    work_unit_id: str = Field(min_length=1)
    worker_id: str = ""
    notion_thread_id: str = ""
    idempotency_key: str = Field(min_length=1)
    profile_id: str = ""
    workspace_id: str = ""
    user_id: str = ""

    @model_validator(mode="after")
    def normalize_account(self) -> "BeeNotionCallEnvelope":
        workspace = str(self.workspace_id or "").strip()
        user = str(self.user_id or "").strip()
        if workspace and user:
            require_matching_account_key(
                self.account_key,
                workspace_id=workspace,
                user_id=user,
                profile_name=self.profile_id,
            )
        elif ":" not in self.account_key:
            raise MultithreadContractError(
                "account_key must be workspace_id:user_id for bee notion2api calls"
            )
        return self

    def validate_lane(
        self,
        binding: ThreadBinding,
        *,
        mission_conversation_ids: Iterable[str],
        bound_thread_id: str = "",
        lane_thread_ids: Mapping[str, str] | None = None,
    ) -> None:
        """Reject account/lane/thread borrowing across bees."""
        if self.mission_id != binding.mission_id:
            raise MultithreadContractError("bee call mission_id does not match lane binding")
        if self.conversation_id != binding.conversation_id:
            raise MultithreadContractError("bee call conversation_id does not match lane binding")
        if binding.thread_kind != ThreadKind.LEADER:
            if self.work_unit_id and self.work_unit_id != binding.work_unit_id:
                raise MultithreadContractError("bee call work_unit_id does not match lane binding")
            if self.worker_id and binding.worker_id and self.worker_id != binding.worker_id:
                raise MultithreadContractError("bee call worker_id does not match lane binding")
        if not account_key_matches(
            self.account_key,
            workspace_id=binding.workspace_id,
            user_id=binding.notion_user_id,
            profile_name=binding.profile_id,
        ):
            raise MultithreadContractError("bee call account_key does not match lane binding")
        allowed = {str(item).strip() for item in mission_conversation_ids if str(item).strip()}
        allowed.add(leader_conversation_id(self.mission_id))
        if self.conversation_id not in allowed:
            raise MultithreadContractError(
                "bee call conversation_id is not a declared mission lane conversation"
            )
        target_thread = str(self.notion_thread_id or "").strip()
        bound = str(bound_thread_id or "").strip()
        if target_thread and bound and target_thread != bound:
            raise MultithreadContractError(
                "bee call notion_thread_id does not match the conversation's bound Notion thread"
            )
        if target_thread and lane_thread_ids:
            owners = [
                lane_id
                for lane_id, thread_id in lane_thread_ids.items()
                if str(thread_id or "").strip() == target_thread
            ]
            if owners and binding.conversation_id not in owners and self.conversation_id not in owners:
                raise MultithreadContractError(
                    "bee call notion_thread_id is bound to a different mission lane"
                )


def _assert_no_credentials(value: Any, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key).strip().lower()
            if key_text in _FORBIDDEN_CREDENTIAL_KEYS or any(
                token in key_text for token in ("password", "secret", "token_v2")
            ):
                raise MultithreadContractError(f"credential-bearing key is forbidden at {path}.{key}")
            _assert_no_credentials(item, f"{path}.{key}")
    elif isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _assert_no_credentials(item, f"{path}[{index}]")


@dataclass(frozen=True)
class LaneDescriptor:
    work_unit_id: str
    worker_id: str
    writable_domains: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    priority: int = 100
    reviewer: bool = False


def plan_lane_dependencies(
    lane_ids: Sequence[str],
    *,
    reviewer_ids: Iterable[str] = (),
    dependency_count: int = 0,
    parallelizable_workstreams: int = 1,
) -> dict[str, list[str]]:
    """Create a concurrency-first dependency graph.

    The first ``parallelizable_workstreams`` non-review lanes are immediately ready.
    Additional dependency edges are distributed round-robin across prior lanes rather
    than forming one artificial chain. Reviewers wait for all non-review lanes.
    """
    ordered = [_required(item, "lane_id") for item in lane_ids]
    if len(ordered) != len(set(ordered)):
        raise MultithreadContractError("lane_ids must be unique")
    reviewers = {str(item).strip() for item in reviewer_ids if str(item).strip()}
    unknown_reviewers = reviewers.difference(ordered)
    if unknown_reviewers:
        raise MultithreadContractError(f"unknown reviewer lanes: {sorted(unknown_reviewers)}")
    workers = [item for item in ordered if item not in reviewers]
    width = max(1, min(int(parallelizable_workstreams or 1), max(1, len(workers))))
    remaining = max(0, int(dependency_count or 0))
    result = {item: [] for item in ordered}
    for index, lane_id in enumerate(workers):
        if index < width or remaining <= 0:
            continue
        predecessor = workers[index - width]
        result[lane_id] = [predecessor]
        remaining -= 1
    for reviewer_id in reviewers:
        result[reviewer_id] = list(workers)
    return result


def _domain_conflict(left: LaneDescriptor, right: LaneDescriptor) -> bool:
    left_domains = {item.strip().lower() for item in left.writable_domains if item.strip()}
    right_domains = {item.strip().lower() for item in right.writable_domains if item.strip()}
    return bool(left_domains.intersection(right_domains))


def select_concurrent_lanes(
    lanes: Sequence[LaneDescriptor],
    *,
    completed: Iterable[str] = (),
    active: Iterable[str] = (),
    max_parallel: int = 4,
) -> list[LaneDescriptor]:
    """Select the highest-priority dependency-ready lanes without write conflicts."""
    completed_ids = {str(item).strip() for item in completed if str(item).strip()}
    active_ids = {str(item).strip() for item in active if str(item).strip()}
    candidates = [
        lane
        for lane in lanes
        if lane.work_unit_id not in completed_ids
        and lane.work_unit_id not in active_ids
        and set(lane.dependencies).issubset(completed_ids)
    ]
    candidates.sort(key=lambda item: (item.reviewer, item.priority, item.work_unit_id))
    selected: list[LaneDescriptor] = []
    for lane in candidates:
        if len(selected) >= max(1, int(max_parallel or 1)):
            break
        if any(_domain_conflict(lane, other) for other in selected):
            continue
        selected.append(lane)
    return selected


class CrossThreadEnvelope(BaseModel):
    message_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    mission_id: str = Field(min_length=1)
    work_unit_id: str = ""
    message_kind: CrossThreadMessageKind
    sender_thread_id: str = Field(min_length=1)
    recipient_thread_id: str = Field(min_length=1)
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    context_version: int = 0
    payload: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_threads(self) -> "CrossThreadEnvelope":
        if self.sender_thread_id == self.recipient_thread_id:
            raise MultithreadContractError("cross-thread messages require distinct threads")
        _assert_no_credentials(self.payload)
        return self


class HiveMissionBus:
    """Typed cross-thread communication over the existing durable Hive event ledger."""

    def __init__(self, store: HiveRuntimeStore):
        self.store = store

    def publish(
        self,
        envelope: CrossThreadEnvelope,
        *,
        idempotency_key: str | None = None,
        work_unit_status: str | None = None,
        expected_mission_revision: int | None = None,
    ) -> HiveMissionSnapshot:
        payload = envelope.model_dump(mode="json")
        return self.store.append_event(
            mission_id=envelope.mission_id,
            event_type=envelope.message_kind.value,
            sender=envelope.sender_thread_id,
            recipient=envelope.recipient_thread_id,
            work_unit_id=envelope.work_unit_id,
            context_version=envelope.context_version,
            payload=payload,
            idempotency_key=idempotency_key or envelope.message_id,
            work_unit_status=work_unit_status,
            expected_mission_revision=expected_mission_revision,
        )

    def inbox(
        self,
        mission_id: str,
        thread_id: str,
        *,
        event_limit: int = 1000,
    ) -> list[CrossThreadEnvelope]:
        snapshot = self.store.get_mission(mission_id, event_limit=event_limit)
        accepted_recipients = {thread_id, "swarm"}
        messages: list[CrossThreadEnvelope] = []
        for event in snapshot.events:
            if event.recipient not in accepted_recipients:
                continue
            if event.event_type not in {item.value for item in CrossThreadMessageKind}:
                continue
            try:
                messages.append(CrossThreadEnvelope.model_validate(event.payload))
            except (ValueError, TypeError):
                continue
        return messages


def topology_receipt(
    *,
    mission_id: str,
    plan_id: str,
    workers: Sequence[tuple[str, str]],
    profile_id: str,
    notion_user_id: str,
    workspace_id: str,
    account_key: str = "",
) -> dict[str, Any]:
    """Return a secret-free leader/worker thread topology receipt."""
    leader_id = leader_conversation_id(mission_id)
    resolved_account = str(account_key or "").strip() or canonical_account_key(
        workspace_id, notion_user_id
    )
    bindings = [
        {
            "worker_id": worker_id,
            "work_unit_id": work_unit_id,
            "conversation_id": worker_conversation_id(plan_id, worker_id),
            "leader_conversation_id": leader_id,
            "account_key": resolved_account,
            "profile_id": profile_id,
            "notion_user_id": notion_user_id,
            "workspace_id": workspace_id,
        }
        for worker_id, work_unit_id in workers
    ]
    return {
        "mission_id": mission_id,
        "plan_id": plan_id,
        "account_key": resolved_account,
        "topology": "one_leader_many_independent_workers",
        "leader_conversation_id": leader_id,
        "worker_bindings": bindings,
        "communication_bus": "hive_events",
        "all_mcp_servers_discoverable_per_worker": True,
        "shared_pooled_tool_list": False,
        "credential_values_in_prompts_or_receipts": False,
    }


def snapshot_lane_descriptors(snapshot: HiveMissionSnapshot) -> list[LaneDescriptor]:
    return [
        LaneDescriptor(
            work_unit_id=unit.work_unit_id,
            worker_id=unit.role,
            writable_domains=tuple(
                item.strip() for item in unit.writable_domain.split(";") if item.strip()
            ),
            dependencies=tuple(unit.dependencies),
            reviewer="review" in unit.role.lower(),
        )
        for unit in snapshot.work_units
    ]
