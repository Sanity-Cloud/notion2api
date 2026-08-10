"""Offline-only contracts for sanitized HAR-derived Notion observations.

This module deliberately contains no HTTP client and cannot replay captured traffic.
It validates parser fixtures and produces deterministic fingerprints for review.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


class HarContractError(ValueError):
    """Raised when a sanitized HAR contract bundle violates the offline policy."""


_FORBIDDEN_KEY_FRAGMENTS = {
    "authorization",
    "cookie",
    "set-cookie",
    "token_v2",
    "x-api-key",
    "x-csrf-token",
    "x-notion-active-user-header",
    "x-notion-user-id",
}
_FORBIDDEN_VALUE_PREFIXES = ("bearer ", "token_v2=", "cookie:")
_ALLOWED_DISPOSITIONS = {"candidate", "excluded"}


def _canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def schema_fingerprint(value: Any) -> str:
    """Return a stable SHA-256 fingerprint for a JSON-compatible value."""
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _require_text(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise HarContractError(f"{field} is required")
    return text


def _walk_for_secrets(value: Any, *, path: str = "$") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            key_text = str(key).strip().lower()
            if any(fragment in key_text for fragment in _FORBIDDEN_KEY_FRAGMENTS):
                raise HarContractError(f"secret-bearing key is forbidden at {path}.{key}")
            _walk_for_secrets(item, path=f"{path}.{key}")
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for index, item in enumerate(value):
            _walk_for_secrets(item, path=f"{path}[{index}]")
        return
    if isinstance(value, str):
        lowered = value.strip().lower()
        if any(lowered.startswith(prefix) for prefix in _FORBIDDEN_VALUE_PREFIXES):
            raise HarContractError(f"secret-bearing value is forbidden at {path}")


@dataclass(frozen=True)
class EndpointContract:
    path: str
    disposition: str
    methods: tuple[str, ...]
    observation_count: int
    confidence: str
    mutation_risk: str
    volatility: str
    request_schema_fingerprints: tuple[str, ...]
    response_schema_fingerprints: tuple[str, ...]

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "EndpointContract":
        path = _require_text(raw.get("path"), "endpoint.path")
        if not path.startswith("/"):
            raise HarContractError(f"endpoint.path must be absolute: {path}")
        disposition = _require_text(raw.get("disposition"), "endpoint.disposition")
        if disposition not in _ALLOWED_DISPOSITIONS:
            raise HarContractError(f"unsupported endpoint disposition: {disposition}")
        methods = tuple(sorted({_require_text(item, "endpoint.method").upper() for item in raw.get("methods") or []}))
        if not methods:
            raise HarContractError(f"endpoint.methods is required for {path}")
        request_fingerprints = tuple(sorted(str(item) for item in raw.get("request_schema_fingerprints") or []))
        response_fingerprints = tuple(sorted(str(item) for item in raw.get("response_schema_fingerprints") or []))
        for fingerprint in (*request_fingerprints, *response_fingerprints):
            if len(fingerprint) != 64 or any(ch not in "0123456789abcdef" for ch in fingerprint.lower()):
                raise HarContractError(f"invalid schema fingerprint for {path}")
        return cls(
            path=path,
            disposition=disposition,
            methods=methods,
            observation_count=max(0, int(raw.get("observation_count") or 0)),
            confidence=str(raw.get("confidence") or "unknown").strip().lower(),
            mutation_risk=str(raw.get("mutation_risk") or "unknown").strip().lower(),
            volatility=str(raw.get("volatility") or "unknown").strip().lower(),
            request_schema_fingerprints=request_fingerprints,
            response_schema_fingerprints=response_fingerprints,
        )


@dataclass(frozen=True)
class ParserFixture:
    path: str
    method: str
    request_body: Any
    response_body: Any
    purpose: str

    @classmethod
    def from_mapping(cls, raw: Mapping[str, Any]) -> "ParserFixture":
        if raw.get("network_replay_allowed") is not False:
            raise HarContractError("every fixture must set network_replay_allowed=false")
        headers = raw.get("headers")
        if headers not in ({}, None):
            raise HarContractError("fixture headers must be empty")
        purpose = _require_text(raw.get("fixture_purpose"), "fixture.fixture_purpose")
        if purpose != "parser-contract-test-only":
            raise HarContractError(f"unsupported fixture purpose: {purpose}")
        _walk_for_secrets(raw)
        return cls(
            path=_require_text(raw.get("path"), "fixture.path"),
            method=_require_text(raw.get("method"), "fixture.method").upper(),
            request_body=raw.get("request_body"),
            response_body=raw.get("response_body"),
            purpose=purpose,
        )


@dataclass(frozen=True)
class OfflineHarBundle:
    source_sha256: str
    generated_from_sanitized_evidence: bool
    network_replay_allowed: bool
    endpoints: tuple[EndpointContract, ...]
    fixtures: tuple[ParserFixture, ...]

    @property
    def fingerprint(self) -> str:
        return schema_fingerprint(
            {
                "source_sha256": self.source_sha256,
                "endpoints": [endpoint.__dict__ for endpoint in self.endpoints],
                "fixtures": [fixture.__dict__ for fixture in self.fixtures],
            }
        )

    def endpoint(self, path: str) -> EndpointContract:
        for endpoint in self.endpoints:
            if endpoint.path == path:
                return endpoint
        raise KeyError(path)


def load_offline_har_bundle(path: str | Path) -> OfflineHarBundle:
    """Load and validate a sanitized, parser-only HAR contract bundle."""
    source = Path(path)
    try:
        raw = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HarContractError(f"unable to load HAR contract bundle: {exc}") from exc
    if not isinstance(raw, Mapping):
        raise HarContractError("HAR contract bundle must be a JSON object")
    if raw.get("generated_from_sanitized_evidence") is not True:
        raise HarContractError("bundle must declare generated_from_sanitized_evidence=true")
    if raw.get("network_replay_allowed") is not False:
        raise HarContractError("bundle must set network_replay_allowed=false")
    source_sha256 = _require_text(raw.get("source_sha256"), "source_sha256").lower()
    if len(source_sha256) != 64 or any(ch not in "0123456789abcdef" for ch in source_sha256):
        raise HarContractError("source_sha256 must be a lowercase SHA-256 digest")
    _walk_for_secrets(raw)
    endpoints = tuple(EndpointContract.from_mapping(item) for item in raw.get("endpoints") or [])
    fixtures = tuple(ParserFixture.from_mapping(item) for item in raw.get("fixtures") or [])
    if not endpoints or not fixtures:
        raise HarContractError("bundle requires endpoints and parser fixtures")
    endpoint_keys = [(endpoint.path, method) for endpoint in endpoints for method in endpoint.methods]
    if len(endpoint_keys) != len(set(endpoint_keys)):
        raise HarContractError("duplicate endpoint path/method contract")
    contract_keys = set(endpoint_keys)
    for fixture in fixtures:
        if (fixture.path, fixture.method) not in contract_keys:
            raise HarContractError(f"fixture has no endpoint contract: {fixture.method} {fixture.path}")
    return OfflineHarBundle(
        source_sha256=source_sha256,
        generated_from_sanitized_evidence=True,
        network_replay_allowed=False,
        endpoints=endpoints,
        fixtures=fixtures,
    )
