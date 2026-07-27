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
    assert any("accountable_human" in gap for gap in projection.evidence_gaps)
    assert records["project:mission-1"].accountable_human == ""


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
