from app.governance import DEFAULT_AUTHORITY_PAGE_ID, DEFAULT_CONTRACT_VERSION
from app.workspace_library import build_workspace_library


def _snapshot() -> dict:
    return {
        "mission_id": "mission-agentic-authority",
        "title": "Agentic Authority Migration",
        "objective": "Project governed authority without a blocking human gate.",
        "status": "ACTIVE",
        "lifecycle_stage": "Adopt",
        "authority_ceiling": "A3",
        "exclusions": ["credential changes"],
        "source_boundary": ["canonical governance"],
        "acceptance_criteria": ["decision receipt recorded"],
        "decision_gates": ["validation"],
        "fan_in_owner": "AIgentBee shared leader",
        "closure_condition": "validated",
        "work_units": [],
        "events": [],
    }


def test_workspace_projection_uses_agentic_authority_without_human_gap() -> None:
    projection = build_workspace_library(_snapshot())
    project = projection.records[0]
    assert projection.schema_version == 2
    assert projection.root_record_id == DEFAULT_AUTHORITY_PAGE_ID
    assert projection.governance_contract == DEFAULT_CONTRACT_VERSION
    assert project.authority_basis == "governance_plan_inference"
    assert project.authority_owner == "AIgentBee shared leader"
    assert project.authority_receipt["per_action_human_approval_required"] is False
    assert not any("accountable_human" in gap for gap in projection.evidence_gaps)


def test_legacy_accountable_human_is_non_blocking_ultimate_ownership_metadata() -> None:
    projection = build_workspace_library(_snapshot(), accountable_human="Mathias")
    project = projection.records[0]
    assert project.accountable_human == "Mathias"
    assert project.metadata["ultimate_accountability_only"] is True
    assert project.metadata["per_action_human_approval_required"] is False
    assert project.authority_basis == "governance_plan_inference"
