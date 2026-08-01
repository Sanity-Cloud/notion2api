from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from fastapi.testclient import TestClient

from app import hive_overview


def _seed_db(path: Path) -> None:
    connection = sqlite3.connect(path)
    connection.executescript(
        """
        CREATE TABLE hive_missions (
            mission_id TEXT PRIMARY KEY, title TEXT, objective TEXT,
            lifecycle_stage TEXT, status TEXT, authority_ceiling TEXT,
            parent_context_id TEXT, cancellation_reason TEXT,
            idempotency_key TEXT, request_sha256 TEXT,
            created_at INTEGER, updated_at INTEGER, revision INTEGER
        );
        CREATE TABLE hive_work_units (
            work_unit_id TEXT PRIMARY KEY, mission_id TEXT, title TEXT,
            role TEXT, status TEXT, conversation_id TEXT,
            writable_domain TEXT, dependencies_json TEXT,
            authority_ceiling TEXT, created_at INTEGER,
            updated_at INTEGER, revision INTEGER
        );
        CREATE TABLE hive_events (
            event_id TEXT PRIMARY KEY, mission_id TEXT, work_unit_id TEXT,
            event_type TEXT, sender TEXT, recipient TEXT, payload_json TEXT,
            context_version INTEGER, idempotency_key TEXT,
            request_sha256 TEXT, created_at INTEGER
        );
        CREATE TABLE hive_decisions (
            decision_id TEXT PRIMARY KEY, mission_id TEXT, status TEXT,
            summary TEXT, dissent_json TEXT, evidence_json TEXT,
            idempotency_key TEXT, request_sha256 TEXT, created_at INTEGER
        );
        CREATE TABLE hive_actions (
            record_id TEXT PRIMARY KEY, mission_id TEXT, action_type TEXT,
            actor TEXT, correlation_id TEXT, payload_json TEXT,
            created_at INTEGER
        );
        """
    )
    connection.execute(
        "INSERT INTO hive_missions VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "mission-1", "Mission One", "Validate a read-only dashboard.",
            "Validate", "ACTIVE", "A3", "", "", "key", "hash",
            1000, 2000, 2,
        ),
    )
    connection.execute(
        "INSERT INTO hive_work_units VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            "lane-1", "mission-1", "Read lane", "Reviewer", "COMPLETED",
            "", "tests", json.dumps([]), "A2", 1000, 1500, 1,
        ),
    )
    connection.execute(
        "INSERT INTO hive_events VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (
            "event-1", "mission-1", "lane-1", "VALIDATED", "reviewer",
            "leader", json.dumps({"tests": 3}), 1, "event-key", "event-hash", 1800,
        ),
    )
    connection.execute(
        "INSERT INTO hive_decisions VALUES (?,?,?,?,?,?,?,?,?)",
        (
            "decision-1", "mission-1", "ACCEPTED", "Ready for pilot.",
            json.dumps([{"severity": "watch", "finding": "Read-only."}]),
            json.dumps([{"type": "tests", "passed": 3}]),
            "decision-key", "decision-hash", 1900,
        ),
    )
    connection.execute(
        "INSERT INTO hive_actions VALUES (?,?,?,?,?,?,?)",
        (
            "action-1", "mission-1", "EVENT_RECORDED", "reviewer", "corr",
            json.dumps({"event": "VALIDATED"}), 1800,
        ),
    )
    connection.commit()
    connection.close()


def test_health_and_mission_views_are_read_only(tmp_path: Path, monkeypatch) -> None:
    db_path = tmp_path / "hive.sqlite3"
    _seed_db(db_path)
    monkeypatch.setenv("NOTION2API_HIVE_RUNTIME_DB_PATH", str(db_path))
    with TestClient(hive_overview.app) as client:
        health = client.get("/health")
        assert health.status_code == 200
        assert health.json()["service"] == "hive-overview"
        assert health.json()["read_only"] is True
        assert "db_path" not in health.json()
        listing = client.get("/api/missions")
        assert listing.status_code == 200
        assert listing.json()["missions"][0]["mission_id"] == "mission-1"
        assert "db_path" not in listing.json()
        detail = client.get("/api/missions/mission-1")
        assert detail.status_code == 200
        body = detail.json()
        assert "db_path" not in body
        assert body["work_units"][0]["dependencies"] == []
        assert body["events"][0]["payload"] == {"tests": 3}
        assert body["decision"]["dissent"][0]["severity"] == "watch"


def test_dashboard_exposes_no_mutation_methods() -> None:
    forbidden = {"POST", "PUT", "PATCH", "DELETE"}
    exposed = {
        method
        for route in hive_overview.app.routes
        for method in (getattr(route, "methods", set()) or set())
    }
    assert exposed.isdisjoint(forbidden)


def test_health_is_degraded_when_database_is_missing(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.setenv(
        "NOTION2API_HIVE_RUNTIME_DB_PATH",
        str(tmp_path / "missing.sqlite3"),
    )
    with TestClient(hive_overview.app) as client:
        response = client.get("/health")
    assert response.status_code == 503
    assert response.json()["status"] == "degraded"
    assert response.json()["error"] == "Hive runtime database unavailable"
    assert str(tmp_path) not in response.text


def test_lifecycle_scripts_require_pwsh7_and_verify_process_identity() -> None:
    root = Path(__file__).resolve().parents[1]
    start = (root / "scripts" / "Start-HiveOverview.ps1").read_text(encoding="utf-8-sig")
    stop = (root / "scripts" / "Stop-HiveOverview.ps1").read_text(encoding="utf-8-sig")
    assert start.startswith("#Requires -Version 7.0")
    assert stop.startswith("#Requires -Version 7.0")
    assert "app\\.hive_overview" in start
    assert "app\\.hive_overview" in stop
    assert "NOTION2API_HIVE_RUNTIME_DB_PATH" in start
    assert "read_only = $true" in start
