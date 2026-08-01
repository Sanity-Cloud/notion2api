"""Deterministic control-plane projection for the workspace organizational library.

The projection is deliberately separate from provider parsing. It organizes verified
Hive records without inventing missing governance facts or mutating Notion.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field

from app.governance import DEFAULT_AUTHORITY_PAGE_ID, DEFAULT_CONTRACT_VERSION


class WorkspaceLibraryRecord(BaseModel):
    record_id: str
    record_type: str
    parent_record_id: str = ""
    source_record_id: str = ""
    mission_id: str = ""
    work_unit_id: str = ""
    title: str
    status: str = ""
    purpose: str = ""
    scope: str = ""
    exclusions: list[str] = Field(default_factory=list)
    accountable_human: str = ""
    authority_basis: str = ""
    authority_owner: str = ""
    governance_contract: str = ""
    authority_receipt: dict[str, Any] = Field(default_factory=dict)
    authority_ceiling: str = ""
    source_boundary: list[str] = Field(default_factory=list)
    dependencies: list[str] = Field(default_factory=list)
    risks: list[dict[str, Any]] = Field(default_factory=list)
    acceptance_criteria: list[str] = Field(default_factory=list)
    decision_gates: list[str] = Field(default_factory=list)
    fan_in_owner: str = ""
    closure_condition: str = ""
    evidence: list[dict[str, Any]] = Field(default_factory=list)
    metadata: dict[str, Any] = Field(default_factory=dict)


class WorkspaceLibraryProjection(BaseModel):
    schema_version: int = 2
    root_record_id: str
    mission_id: str
    governance_contract: str = DEFAULT_CONTRACT_VERSION
    authority_page_id: str = DEFAULT_AUTHORITY_PAGE_ID
    authorization_basis: str = "governance_plan_inference"
    records: list[WorkspaceLibraryRecord] = Field(default_factory=list)
    evidence_gaps: list[str] = Field(default_factory=list)


def _as_dict(value: Any) -> dict[str, Any]:
    if hasattr(value, "model_dump"):
        dumped = value.model_dump(mode="json")
        return dumped if isinstance(dumped, dict) else {}
    return dict(value) if isinstance(value, dict) else {}


def _event_record_type(event_type: str) -> str:
    normalized = event_type.upper()
    if normalized.startswith("TASK_"):
        return "task"
    if normalized.startswith("RISK_"):
        return "risk"
    if normalized.startswith("OUTCOME_"):
        return "outcome"
    if "GATE" in normalized or "FANIN" in normalized:
        return "decision_gate"
    if "VALIDATION" in normalized or "RECEIPT" in normalized:
        return "receipt"
    return "event"


def build_workspace_library(
    snapshot: Any,
    *,
    governance_root_id: str = DEFAULT_AUTHORITY_PAGE_ID,
    accountable_human: str = "",
    authority_owner: str = "AIgentBee shared leader",
    governance_contract: str = DEFAULT_CONTRACT_VERSION,
    authorization_basis: str = "governance_plan_inference",
    authority_receipt: dict[str, Any] | None = None,
) -> WorkspaceLibraryProjection:
    """Project one Hive mission into a stable organizational-library hierarchy.

    Lineage: governance root -> mission/project -> work-unit/branch -> event records.
    Operational authority is projected from the canonical governance contract,
    adopted mission plan, authority ceiling, and decision receipts. The legacy
    accountable_human value is retained only as optional ultimate ownership metadata.
    Missing governance information is reported as an evidence gap and is never
    synthesized from titles, prompts, or model output.
    """
    data = _as_dict(snapshot)
    mission_id = str(data.get("mission_id") or "").strip()
    if not mission_id:
        raise ValueError("mission_id is required for workspace library projection")

    parent_context_id = str(data.get("parent_context_id") or governance_root_id).strip()
    records: list[WorkspaceLibraryRecord] = []
    gaps: list[str] = []
    authority_ceiling = str(data.get("authority_ceiling") or "").strip()
    receipt = dict(authority_receipt or data.get("authority_receipt") or {})
    receipt.setdefault("authorization_basis", authorization_basis)
    receipt.setdefault("governance_contract", governance_contract)
    receipt.setdefault("authority_page_id", governance_root_id)
    receipt.setdefault("authority_ceiling", authority_ceiling)
    receipt.setdefault("authority_owner", authority_owner)
    receipt.setdefault("per_action_human_approval_required", False)
    receipt.setdefault("ultimate_accountability_owner", accountable_human)

    mission_record_id = f"project:{mission_id}"
    records.append(
        WorkspaceLibraryRecord(
            record_id=mission_record_id,
            record_type="project",
            parent_record_id=parent_context_id,
            source_record_id=mission_id,
            mission_id=mission_id,
            title=str(data.get("title") or mission_id),
            status=str(data.get("status") or ""),
            purpose=str(data.get("objective") or ""),
            accountable_human=accountable_human,
            authority_basis=authorization_basis,
            authority_owner=authority_owner,
            governance_contract=governance_contract,
            authority_receipt=receipt,
            authority_ceiling=authority_ceiling,
            metadata={
                "ultimate_accountability_only": bool(accountable_human),
                "per_action_human_approval_required": False,
                "lifecycle_stage": str(data.get("lifecycle_stage") or ""),
                "revision": int(data.get("revision") or 0),
                "created_at": int(data.get("created_at") or 0),
                "updated_at": int(data.get("updated_at") or 0),
            },
        )
    )

    if not authority_ceiling:
        gaps.append(f"{mission_record_id}: authority_ceiling is not recorded")
    if not authority_owner:
        gaps.append(f"{mission_record_id}: authority_owner is not recorded")
    if not governance_contract:
        gaps.append(f"{mission_record_id}: governance_contract is not recorded")
    for field_name in (
        "exclusions",
        "source_boundary",
        "acceptance_criteria",
        "decision_gates",
        "fan_in_owner",
        "closure_condition",
    ):
        if not data.get(field_name):
            gaps.append(f"{mission_record_id}: {field_name} is not recorded")

    work_units = (
        data.get("work_units") if isinstance(data.get("work_units"), list) else []
    )
    known_work_units = {
        str(_as_dict(item).get("work_unit_id") or "").strip()
        for item in work_units
        if str(_as_dict(item).get("work_unit_id") or "").strip()
    }
    for raw in work_units:
        work = _as_dict(raw)
        work_unit_id = str(work.get("work_unit_id") or "").strip()
        if not work_unit_id:
            gaps.append(f"{mission_record_id}: work unit without work_unit_id")
            continue
        dependencies = [str(item) for item in (work.get("dependencies") or [])]
        unknown_dependencies = sorted(set(dependencies) - known_work_units)
        if unknown_dependencies:
            gaps.append(
                f"branch:{work_unit_id}: unknown dependencies {unknown_dependencies}"
            )
        if not str(work.get("writable_domain") or "").strip():
            gaps.append(f"branch:{work_unit_id}: scope/writable_domain is not recorded")
        records.append(
            WorkspaceLibraryRecord(
                record_id=f"branch:{work_unit_id}",
                record_type="branch",
                parent_record_id=mission_record_id,
                source_record_id=work_unit_id,
                mission_id=mission_id,
                work_unit_id=work_unit_id,
                title=str(work.get("title") or work_unit_id),
                status=str(work.get("status") or ""),
                purpose=str(work.get("role") or ""),
                scope=str(work.get("writable_domain") or ""),
                authority_basis=authorization_basis,
                authority_owner=authority_owner,
                governance_contract=governance_contract,
                authority_receipt={
                    **receipt,
                    "work_unit_id": work_unit_id,
                    "authority_ceiling": str(work.get("authority_ceiling") or ""),
                },
                authority_ceiling=str(work.get("authority_ceiling") or ""),
                dependencies=dependencies,
                metadata={
                    "conversation_id": str(work.get("conversation_id") or ""),
                    "revision": int(work.get("revision") or 0),
                    "created_at": int(work.get("created_at") or 0),
                    "updated_at": int(work.get("updated_at") or 0),
                },
            )
        )

    events = data.get("events") if isinstance(data.get("events"), list) else []
    for raw in events:
        event = _as_dict(raw)
        event_id = str(event.get("event_id") or "").strip()
        if not event_id:
            continue
        work_unit_id = str(event.get("work_unit_id") or "").strip()
        parent_id = f"branch:{work_unit_id}" if work_unit_id else mission_record_id
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        records.append(
            WorkspaceLibraryRecord(
                record_id=f"event:{event_id}",
                record_type=_event_record_type(str(event.get("event_type") or "EVENT")),
                parent_record_id=parent_id,
                source_record_id=event_id,
                mission_id=mission_id,
                work_unit_id=work_unit_id,
                title=str(event.get("event_type") or "EVENT"),
                status=str(payload.get("status") or ""),
                purpose=str(payload.get("summary") or payload.get("note") or ""),
                authority_basis=str(payload.get("authorization_basis") or authorization_basis),
                authority_owner=str(payload.get("authority_owner") or event.get("sender") or authority_owner),
                governance_contract=str(payload.get("governance_contract") or governance_contract),
                authority_receipt=(
                    payload.get("authority_receipt")
                    if isinstance(payload.get("authority_receipt"), dict)
                    else receipt
                ),
                evidence=(
                    payload.get("evidence")
                    if isinstance(payload.get("evidence"), list)
                    else []
                ),
                metadata={
                    "sender": str(event.get("sender") or ""),
                    "recipient": str(event.get("recipient") or ""),
                    "context_version": int(event.get("context_version") or 0),
                    "created_at": int(event.get("created_at") or 0),
                    "payload": payload,
                },
            )
        )

    decision = _as_dict(data.get("decision"))
    if decision:
        decision_id = str(decision.get("decision_id") or "latest").strip()
        records.append(
            WorkspaceLibraryRecord(
                record_id=f"decision:{decision_id}",
                record_type="decision",
                parent_record_id=mission_record_id,
                source_record_id=decision_id,
                mission_id=mission_id,
                title=str(decision.get("status") or "Decision"),
                status=str(decision.get("status") or ""),
                purpose=str(decision.get("summary") or ""),
                authority_basis=str(decision.get("authorization_basis") or authorization_basis),
                authority_owner=str(decision.get("authority_owner") or authority_owner),
                governance_contract=str(decision.get("governance_contract") or governance_contract),
                authority_receipt=(
                    decision.get("authority_receipt")
                    if isinstance(decision.get("authority_receipt"), dict)
                    else receipt
                ),
                evidence=(
                    decision.get("evidence")
                    if isinstance(decision.get("evidence"), list)
                    else []
                ),
                metadata={
                    "dissent": decision.get("dissent")
                    if isinstance(decision.get("dissent"), list)
                    else [],
                    "created_at": int(decision.get("created_at") or 0),
                },
            )
        )

    return WorkspaceLibraryProjection(
        root_record_id=parent_context_id,
        mission_id=mission_id,
        governance_contract=governance_contract,
        authority_page_id=governance_root_id,
        authorization_basis=authorization_basis,
        records=records,
        evidence_gaps=sorted(set(gaps)),
    )
