from __future__ import annotations

import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse, JSONResponse

SERVICE_NAME = "hive-overview"
DEFAULT_DB_PATH = Path(
    os.environ.get(
        "NOTION2API_HIVE_RUNTIME_DB_PATH",
        r"X:\MCP\state\notion2api-hive\hive_runtime.sqlite3",
    )
).expanduser()
DASHBOARD_PATH = Path(__file__).resolve().parent / "static" / "hive-overview.html"
REQUIRED_TABLES = {
    "hive_missions",
    "hive_work_units",
    "hive_events",
    "hive_decisions",
    "hive_actions",
}

app = FastAPI(
    title="SanityCloud Hive Overview",
    version="1.0.0",
    description="Read-only dashboard over the durable AIgentBee Hive mission ledger.",
    redoc_url=None,
)


def runtime_db_path() -> Path:
    configured = os.environ.get("NOTION2API_HIVE_RUNTIME_DB_PATH", "").strip()
    return Path(configured).expanduser().resolve() if configured else DEFAULT_DB_PATH.resolve()


def _connect_read_only(path: Path | None = None) -> sqlite3.Connection:
    target = (path or runtime_db_path()).resolve()
    if not target.is_file():
        raise FileNotFoundError("Hive runtime database not found")
    connection = sqlite3.connect(
        f"file:{target.as_posix()}?mode=ro",
        uri=True,
        timeout=5,
        isolation_level=None,
    )
    connection.row_factory = sqlite3.Row
    connection.execute("PRAGMA query_only = ON")
    connection.execute("PRAGMA busy_timeout = 3000")
    present = {
        str(row[0])
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    }
    missing = REQUIRED_TABLES - present
    if missing:
        connection.close()
        raise RuntimeError(f"Hive runtime database is missing tables: {sorted(missing)}")
    return connection


def _load_json(value: Any, default: Any) -> Any:
    if value in (None, ""):
        return default
    try:
        return json.loads(str(value))
    except (TypeError, ValueError, json.JSONDecodeError):
        return default


def _row_dict(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return dict(row) if row is not None else None


def _mission_summaries(
    connection: sqlite3.Connection,
    *,
    status: str = "",
    limit: int = 100,
) -> list[dict[str, Any]]:
    where = "WHERE m.status = ?" if status else ""
    params: list[Any] = [status] if status else []
    params.append(limit)
    rows = connection.execute(
        f"""
        SELECT
            m.*,
            (SELECT COUNT(*) FROM hive_work_units w WHERE w.mission_id = m.mission_id)
                AS work_unit_count,
            (SELECT COUNT(*) FROM hive_events e WHERE e.mission_id = m.mission_id)
                AS event_count,
            (SELECT COUNT(*) FROM hive_actions a WHERE a.mission_id = m.mission_id)
                AS action_count,
            (SELECT d.status FROM hive_decisions d
                WHERE d.mission_id = m.mission_id
                ORDER BY d.created_at DESC, d.decision_id DESC LIMIT 1)
                AS decision_status,
            (SELECT d.summary FROM hive_decisions d
                WHERE d.mission_id = m.mission_id
                ORDER BY d.created_at DESC, d.decision_id DESC LIMIT 1)
                AS decision_summary
        FROM hive_missions m
        {where}
        ORDER BY m.updated_at DESC, m.mission_id ASC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


@app.get("/health")
def health() -> JSONResponse:
    path = runtime_db_path()
    try:
        with closing(_connect_read_only(path)) as connection:
            mission_count = int(
                connection.execute("SELECT COUNT(*) FROM hive_missions").fetchone()[0]
            )
        payload = {
            "status": "ok",
            "service": SERVICE_NAME,
            "read_only": True,
            "mission_count": mission_count,
        }
        return JSONResponse(payload)
    except (OSError, sqlite3.Error, RuntimeError):
        return JSONResponse(
            {
                "status": "degraded",
                "service": SERVICE_NAME,
                "read_only": True,
                "error": "Hive runtime database unavailable",
            },
            status_code=503,
        )


@app.get("/api/missions")
def list_missions(
    status: str = Query(default="", max_length=40),
    limit: int = Query(default=100, ge=1, le=200),
) -> dict[str, Any]:
    clean_status = status.strip().upper()
    try:
        with closing(_connect_read_only()) as connection:
            missions = _mission_summaries(
                connection,
                status=clean_status,
                limit=limit,
            )
    except (OSError, sqlite3.Error, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail="Hive runtime database unavailable") from exc
    return {
        "ok": True,
        "read_only": True,
        "count": len(missions),
        "missions": missions,
    }


@app.get("/api/missions/{mission_id}")
def get_mission(
    mission_id: str,
    event_limit: int = Query(default=100, ge=0, le=500),
    action_limit: int = Query(default=100, ge=0, le=500),
) -> dict[str, Any]:
    clean_id = mission_id.strip()
    if not clean_id:
        raise HTTPException(status_code=400, detail="mission_id is required")
    try:
        with closing(_connect_read_only()) as connection:
            mission = connection.execute(
                "SELECT * FROM hive_missions WHERE mission_id = ?",
                (clean_id,),
            ).fetchone()
            if mission is None:
                raise HTTPException(status_code=404, detail="Hive mission not found")
            work_units = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM hive_work_units WHERE mission_id = ? "
                    "ORDER BY created_at ASC, work_unit_id ASC",
                    (clean_id,),
                ).fetchall()
            ]
            for item in work_units:
                item["dependencies"] = _load_json(
                    item.pop("dependencies_json", "[]"), []
                )
            events = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM hive_events WHERE mission_id = ? "
                    "ORDER BY created_at DESC, event_id DESC LIMIT ?",
                    (clean_id, event_limit),
                ).fetchall()
            ]
            for item in events:
                item["payload"] = _load_json(item.pop("payload_json", "{}"), {})
                item.pop("request_sha256", None)
                item.pop("idempotency_key", None)
            actions = [
                dict(row)
                for row in connection.execute(
                    "SELECT * FROM hive_actions WHERE mission_id = ? "
                    "ORDER BY created_at DESC, record_id DESC LIMIT ?",
                    (clean_id, action_limit),
                ).fetchall()
            ]
            for item in actions:
                item["payload"] = _load_json(item.pop("payload_json", "{}"), {})
            decision_row = connection.execute(
                "SELECT * FROM hive_decisions WHERE mission_id = ? "
                "ORDER BY created_at DESC, decision_id DESC LIMIT 1",
                (clean_id,),
            ).fetchone()
            decision = _row_dict(decision_row)
            if decision is not None:
                decision["dissent"] = _load_json(
                    decision.pop("dissent_json", "[]"), []
                )
                decision["evidence"] = _load_json(
                    decision.pop("evidence_json", "[]"), []
                )
                decision.pop("request_sha256", None)
                decision.pop("idempotency_key", None)
    except HTTPException:
        raise
    except (OSError, sqlite3.Error, RuntimeError) as exc:
        raise HTTPException(status_code=503, detail="Hive runtime database unavailable") from exc
    return {
        "ok": True,
        "read_only": True,
        "mission": dict(mission),
        "work_units": work_units,
        "events": events,
        "actions": actions,
        "decision": decision,
    }


@app.get("/", response_class=HTMLResponse)
def dashboard() -> HTMLResponse:
    if not DASHBOARD_PATH.is_file():
        raise HTTPException(status_code=503, detail="Hive Overview dashboard asset is missing")
    return HTMLResponse(DASHBOARD_PATH.read_text(encoding="utf-8-sig"))
