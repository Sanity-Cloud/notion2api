"""Contract models for the SC-AMF governed memory adapter."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from hashlib import sha256
import json
from typing import Any, Iterable


UPSTREAM_REPOSITORY = "TencentCloud/TencentDB-Agent-Memory"
UPSTREAM_BRANCH = "feat/server_team"
UPSTREAM_COMMIT = "fe3230f176f1bf5832fee79d12494bbc2d19a8aa"
UPSTREAM_PYTHON_SDK_VERSION = "0.2.0"
CONTRACT_ID = "sanitycloud-memory-adapter/v0.2"


class AgentMemoryError(RuntimeError):
    """Fail-closed adapter error with a stable machine code."""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code = str(code or "AGENT_MEMORY_ERROR").upper()
        self.retryable = bool(retryable)


def _required(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise AgentMemoryError("SCOPE_DENIED", f"{field_name} is required")
    return text


@dataclass(frozen=True)
class IdentityEnvelope:
    workspace_ref: str
    memory_domain_id: str
    project_ref: str
    mission_id: str
    work_unit_id: str
    task_ref: str
    lane_id: str
    worker_ref: str
    principal_ref: str
    lease_ref: str
    context_version: str
    program_ref: str = ""

    def __post_init__(self) -> None:
        for name in (
            "workspace_ref",
            "memory_domain_id",
            "project_ref",
            "mission_id",
            "work_unit_id",
            "task_ref",
            "lane_id",
            "worker_ref",
            "principal_ref",
            "lease_ref",
            "context_version",
        ):
            object.__setattr__(self, name, _required(getattr(self, name), name))
        object.__setattr__(self, "program_ref", str(self.program_ref or "").strip())

    def scope_key(self) -> tuple[str, str, str, str, str]:
        return (
            self.workspace_ref,
            self.memory_domain_id,
            self.project_ref,
            self.principal_ref,
            self.lease_ref,
        )

    def receipt(self) -> dict[str, str]:
        return asdict(self)

    def assert_same_scope(self, other: "IdentityEnvelope") -> None:
        if self.scope_key() != other.scope_key():
            raise AgentMemoryError(
                "SCOPE_DENIED",
                "memory identity does not match the admitted workspace/domain/project/principal/lease scope",
            )


@dataclass(frozen=True)
class RetrievalBudget:
    max_assets: int = 12
    max_chars: int = 12_000
    timeout_seconds: float = 3.0
    max_graph_hops: int = 2

    def __post_init__(self) -> None:
        if self.max_assets < 1 or self.max_assets > 12:
            raise AgentMemoryError("BUDGET_DENIED", "max_assets must be between 1 and 12")
        if self.max_chars < 1 or self.max_chars > 12_000:
            raise AgentMemoryError("BUDGET_DENIED", "max_chars must be between 1 and 12000")
        if self.timeout_seconds <= 0 or self.timeout_seconds > 3.0:
            raise AgentMemoryError("BUDGET_DENIED", "timeout_seconds must be > 0 and <= 3")
        if self.max_graph_hops < 0 or self.max_graph_hops > 2:
            raise AgentMemoryError("BUDGET_DENIED", "max_graph_hops must be between 0 and 2")


@dataclass
class DerivedMemoryRecord:
    derived_memory_id: str
    asset_type: str
    layer: str
    identity: IdentityEnvelope
    payload: str
    payload_hash: str
    source_refs: list[str]
    source_hashes: list[str]
    state: str = "CANDIDATE"
    evidence_class: str = "DERIVED"
    sensitivity: str = "INTERNAL"
    confidence: float = 0.0
    upstream_asset_id: str = ""
    contradicts: list[str] = field(default_factory=list)
    supersedes: list[str] = field(default_factory=list)
    superseded_by: str = ""
    evidence_gaps: list[dict[str, Any]] = field(default_factory=list)
    dissent_refs: list[str] = field(default_factory=list)
    admission_receipt_ref: str = ""
    captured_at: str = ""

    def __post_init__(self) -> None:
        self.derived_memory_id = _required(self.derived_memory_id, "derived_memory_id")
        self.asset_type = _required(self.asset_type, "asset_type")
        self.layer = _required(self.layer, "layer")
        self.payload_hash = _required(self.payload_hash, "payload_hash")
        if self.state not in {
            "OBSERVED",
            "EXTRACTED",
            "CANDIDATE",
            "CORROBORATED",
            "RETRIEVAL_ELIGIBLE",
            "SUPERSEDED",
            "QUARANTINED",
            "RETIRED",
        }:
            raise AgentMemoryError("INVALID_STATE", f"unsupported derived state: {self.state}")
        if self.layer not in {"L0", "L1", "L2", "L3", "NON_LAYERED"}:
            raise AgentMemoryError("INVALID_LAYER", f"unsupported memory layer: {self.layer}")
        if not 0.0 <= float(self.confidence) <= 1.0:
            raise AgentMemoryError("INVALID_CONFIDENCE", "confidence must be between 0 and 1")

    def to_dict(self, *, include_payload: bool = True) -> dict[str, Any]:
        data = asdict(self)
        if not include_payload:
            data.pop("payload", None)
        return data


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def stable_hash(value: Any) -> str:
    return sha256(canonical_json(value).encode("utf-8")).hexdigest()


def payload_hash(text: str) -> str:
    return sha256(str(text).encode("utf-8")).hexdigest()


def normalize_strings(values: Iterable[Any]) -> list[str]:
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if text and text not in result:
            result.append(text)
    return result
