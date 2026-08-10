from __future__ import annotations

import ast
import json
from pathlib import Path

import pytest

from app.har_contract_registry import (
    HarContractError,
    load_offline_har_bundle,
    schema_fingerprint,
)


FIXTURE = Path(__file__).parent / "fixtures" / "offline-har-contract-bundle.json"


def _bundle_payload() -> dict:
    return json.loads(FIXTURE.read_text(encoding="utf-8"))


def _write_bundle(tmp_path: Path, payload: dict) -> Path:
    path = tmp_path / "bundle.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path


def test_loads_sanitized_offline_bundle_with_stable_fingerprint():
    first = load_offline_har_bundle(FIXTURE)
    second = load_offline_har_bundle(FIXTURE)

    assert first.network_replay_allowed is False
    assert first.generated_from_sanitized_evidence is True
    assert len(first.endpoints) == 11
    assert len(first.fixtures) == 11
    assert first.fingerprint == second.fingerprint
    assert len(first.fingerprint) == 64


def test_et_client_remains_explicitly_excluded():
    bundle = load_offline_har_bundle(FIXTURE)

    endpoint = bundle.endpoint("/api/v3/etClient")

    assert endpoint.disposition == "excluded"
    assert endpoint.mutation_risk == "excluded-telemetry-client"


def test_schema_fingerprint_is_canonical_and_change_sensitive():
    left = {"b": [2, 1], "a": {"x": True}}
    reordered = {"a": {"x": True}, "b": [2, 1]}
    changed = {"a": {"x": False}, "b": [2, 1]}

    assert schema_fingerprint(left) == schema_fingerprint(reordered)
    assert schema_fingerprint(left) != schema_fingerprint(changed)


def test_rejects_bundle_that_permits_network_replay(tmp_path: Path):
    payload = _bundle_payload()
    payload["network_replay_allowed"] = True

    with pytest.raises(HarContractError, match="network_replay_allowed=false"):
        load_offline_har_bundle(_write_bundle(tmp_path, payload))


def test_rejects_fixture_that_permits_network_replay(tmp_path: Path):
    payload = _bundle_payload()
    payload["fixtures"][0]["network_replay_allowed"] = True

    with pytest.raises(HarContractError, match="every fixture"):
        load_offline_har_bundle(_write_bundle(tmp_path, payload))


def test_rejects_secret_bearing_headers(tmp_path: Path):
    payload = _bundle_payload()
    payload["fixtures"][0]["headers"] = {"Authorization": "Bearer secret"}

    with pytest.raises(HarContractError, match="secret-bearing key|headers must be empty"):
        load_offline_har_bundle(_write_bundle(tmp_path, payload))


def test_rejects_secret_bearing_values_anywhere(tmp_path: Path):
    payload = _bundle_payload()
    payload["fixtures"][0]["request_body"] = {"value": "Bearer captured-token"}

    with pytest.raises(HarContractError, match="secret-bearing value"):
        load_offline_har_bundle(_write_bundle(tmp_path, payload))


def test_rejects_duplicate_endpoint_path_and_method(tmp_path: Path):
    payload = _bundle_payload()
    payload["endpoints"].append(dict(payload["endpoints"][0]))

    with pytest.raises(HarContractError, match="duplicate endpoint"):
        load_offline_har_bundle(_write_bundle(tmp_path, payload))


def test_rejects_fixture_without_corresponding_contract(tmp_path: Path):
    payload = _bundle_payload()
    payload["fixtures"][0]["path"] = "/api/v3/not-observed"

    with pytest.raises(HarContractError, match="no endpoint contract"):
        load_offline_har_bundle(_write_bundle(tmp_path, payload))


def test_registry_module_has_no_network_client_imports():
    module_path = Path(__file__).parents[1] / "app" / "har_contract_registry.py"
    tree = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imported_roots.update(alias.name.split(".", 1)[0] for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imported_roots.add(node.module.split(".", 1)[0])

    assert imported_roots.isdisjoint(
        {"aiohttp", "httpx", "requests", "urllib", "urllib3", "websockets"}
    )
