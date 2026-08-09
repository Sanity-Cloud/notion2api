from __future__ import annotations

from pathlib import Path

from app.hive_multithread import leader_conversation_id
from app.mcp_server import _conversation_id_for_session


ROOT = Path(__file__).resolve().parents[1]


def test_leader_session_uses_declared_deterministic_leader_conversation(tmp_path: Path) -> None:
    mission_id = "mission-binding-test"
    expected = leader_conversation_id(mission_id)
    conversation_id, session_key, created = _conversation_id_for_session(
        "aigentbee-leader-mission-binding-test",
        conversation_id=expected,
        path=tmp_path / "mcp_sessions.json",
    )
    assert conversation_id == expected
    assert session_key == "aigentbee-leader-mission-binding-test"
    assert created is False


def test_send_leader_request_binds_session_to_mission_leader_conversation() -> None:
    source = (ROOT / "app" / "mcp_server.py").read_text(encoding="utf-8")
    assert "conversation_id=leader_conversation_id(mission_id)" in source
