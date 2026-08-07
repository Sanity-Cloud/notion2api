from app.workspace_library import build_workspace_library


def test_workspace_library_projects_lineage_without_inventing_governance_fields():
    projection = build_workspace_library(
        {
            "mission_id": "mission-1",
            "title": "Foundation Stabilization",
            "objective": "Repair parser and request handling.",
            "lifecycle_stage": "Validate",
            "status": "ACTIVE",
            "authority_ceiling": "A3",
            "parent_context_id": "SanityCloud/Engineering",
            "revision": 2,
            "work_units": [
                {
                    "work_unit_id": "parser",
                    "title": "Parser Reliability",
                    "role": "Parser branch",
                    "status": "ACTIVE",
                    "writable_domain": "app/stream_parser.py",
                    "dependencies": [],
                    "authority_ceiling": "A2",
                    "revision": 1,
                }
            ],
            "events": [
                {
                    "event_id": "event-1",
                    "work_unit_id": "parser",
                    "event_type": "RISK_RECORDED",
                    "sender": "reviewer",
                    "recipient": "parser",
                    "payload": {"summary": "False completion risk"},
                    "context_version": 1,
                    "created_at": 10,
                }
            ],
            "decision": {
                "decision_id": "decision-1",
                "status": "HOLD",
                "summary": "Await tests",
                "dissent": [],
                "evidence": [],
                "created_at": 11,
            },
        }
    )

    records = {record.record_id: record for record in projection.records}
    assert records["project:mission-1"].parent_record_id == "SanityCloud/Engineering"
    assert records["branch:parser"].parent_record_id == "project:mission-1"
    assert records["event:event-1"].parent_record_id == "branch:parser"
    assert records["event:event-1"].record_type == "risk"
    assert records["decision:decision-1"].parent_record_id == "project:mission-1"
    project = records["project:mission-1"]
    assert not any("accountable_human" in gap for gap in projection.evidence_gaps)
    assert project.accountable_human == ""
    assert project.authority_owner == "AIgentBee shared leader"
    assert project.authority_basis == "governance_plan_inference"
    assert project.authority_receipt["per_action_human_approval_required"] is False


def test_workspace_library_reports_unknown_branch_dependency():
    projection = build_workspace_library(
        {
            "mission_id": "mission-2",
            "title": "Test",
            "work_units": [
                {
                    "work_unit_id": "branch-a",
                    "title": "A",
                    "role": "worker",
                    "dependencies": ["missing-branch"],
                }
            ],
        },
        accountable_human="Mathias",
    )
    assert any("unknown dependencies" in gap for gap in projection.evidence_gaps)


def test_workspace_library_projects_governed_project_contract_and_graph_receipt():
    projection = build_workspace_library(
        {
            "mission_id": "mission-3",
            "title": "Cross-domain launch",
            "objective": "Deliver one governed SanityCloud initiative.",
            "status": "ACTIVE",
            "authority_ceiling": "A2",
            "parent_context_id": "sanitycloud-governance",
            "project_contract": {
                "project_kind": "hybrid",
                "scope": "Coding, business planning, and creative production.",
                "exclusions": ["Unapproved publication"],
                "accountable_human": "SanityCloud Founder",
                "source_boundary": ["Approved project sources"],
                "risks": [{"risk": "scope drift"}],
                "acceptance_criteria": ["Outcome passes independent review"],
                "decision_gates": ["Human publication approval"],
                "fan_in_owner": "AIgentBee leader",
                "closure_condition": "Decision and evidence receipts recorded",
            },
            "graph_receipt": {
                "validated": True,
                "dependency_waves": [["plan"], ["build"]],
            },
        }
    )

    project = projection.records[0]
    assert project.scope.startswith("Coding")
    assert project.accountable_human == "SanityCloud Founder"
    assert project.acceptance_criteria == ["Outcome passes independent review"]
    assert project.metadata["project_kind"] == "hybrid"
    assert project.metadata["graph_receipt"]["validated"] is True
    assert not projection.evidence_gaps


def test_workspace_library_projects_delegated_tasks_under_parent_lane():
    projection = build_workspace_library(
        {
            "mission_id": "mission-delegated",
            "title": "Delegated project",
            "status": "ACTIVE",
            "authority_ceiling": "A2",
            "work_units": [
                {
                    "work_unit_id": "build",
                    "title": "Build lane",
                    "role": "developer",
                    "status": "ACTIVE",
                    "writable_domain": "repo:project",
                    "authority_ceiling": "A2",
                }
            ],
            "delegated_tasks": [
                {
                    "task_id": "build-api",
                    "parent_lane_id": "build",
                    "objective": "Build the API",
                    "scope": "API implementation",
                    "exclusions": ["Deployment"],
                    "source_boundary": ["Approved repository"],
                    "writable_domains": ["repo:project/api"],
                    "authority_ceiling": "A2",
                    "dependencies": [],
                    "acceptance_criteria": ["Tests pass"],
                    "deliverables": ["API patch"],
                    "evidence_requirements": ["pytest receipt"],
                    "fan_in_owner": "lane-captain",
                    "closure_condition": "Handoff accepted",
                    "status": "ACTIVE",
                }
            ],
            "events": [
                {
                    "event_id": "task-event",
                    "work_unit_id": "build",
                    "event_type": "TASK_ACCEPTED",
                    "sender": "worker",
                    "payload": {"task_id": "build-api", "status": "ACCEPTED"},
                }
            ],
        }
    )

    records = {record.record_id: record for record in projection.records}
    task = records["task:build-api"]
    assert task.parent_record_id == "branch:build"
    assert task.metadata["authority_level"] == "Execute bounded work (A2)"
    assert records["event:task-event"].parent_record_id == "task:build-api"
