from __future__ import annotations

from types import SimpleNamespace

from starlette.requests import Request

from app.api.notion import (
    _effective_mutation_policy,
    _repo_ai_internal_plan_authorization,
)
from app.mutation_policy import MutationPolicy, require_mutation_capability


def _request(*, client_host: str = "127.0.0.1", marker: str = "1") -> Request:
    return Request(
        {
            "type": "http",
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": "/v1/notion/upload_file",
            "raw_path": b"/v1/notion/upload_file",
            "query_string": b"",
            "headers": [(b"x-repo-ai-internal", marker.encode("ascii"))],
            "client": (client_host, 50000),
            "server": ("127.0.0.1", 8120),
        }
    )


def _client(*, workspace: str = "sanity-management", key: str = "repoai:run:page.upload:abc"):
    return SimpleNamespace(workspace_key=workspace, request_idempotency_key=key)


def test_trusted_repoai_publication_derives_bounded_plan() -> None:
    request = _request()
    plan = _repo_ai_internal_plan_authorization(
        request,
        _client(),
        "page.upload",
        {"aligned": True},
    )

    assert plan["authorized"] is True
    assert plan["publication_authorized"] is True
    assert plan["authority_ceiling"] == "A3"
    assert plan["inferred_risk"] == "moderate"
    assert plan["plan_id"].startswith("repoai:")


def test_repoai_publication_requires_loopback_alignment_scope_and_idempotency() -> None:
    assert not _repo_ai_internal_plan_authorization(
        _request(client_host="192.0.2.10"), _client(), "page.upload", {"aligned": True}
    )
    assert not _repo_ai_internal_plan_authorization(
        _request(), _client(workspace="sanitycloud-hq"), "page.upload", {"aligned": True}
    )
    assert not _repo_ai_internal_plan_authorization(
        _request(), _client(key="other:run"), "page.upload", {"aligned": True}
    )
    assert not _repo_ai_internal_plan_authorization(
        _request(), _client(), "page.upload", {"aligned": False}
    )


def test_trusted_repoai_publication_gets_capability_without_global_enablement() -> None:
    request = _request()
    plan = _repo_ai_internal_plan_authorization(
        request,
        _client(),
        "page.upload",
        {"aligned": True},
    )
    baseline = MutationPolicy(
        enabled=False,
        publication_suppressed=True,
        capabilities=frozenset(),
    )

    effective, overridden = _effective_mutation_policy(
        request,
        baseline,
        "page.upload",
        plan,
    )
    receipt = require_mutation_capability(
        "page.upload",
        governance_aligned=True,
        plan_authorization=plan,
        policy=effective,
    )

    assert overridden is True
    assert effective.enabled is True
    assert effective.publication_suppressed is False
    assert effective.capabilities == frozenset({"page.upload"})
    assert baseline.enabled is False
    assert baseline.publication_suppressed is True
    assert baseline.capabilities == frozenset()
    assert receipt["publication_authorized"] is True


def test_non_repoai_request_does_not_override_policy() -> None:
    request = _request(marker="0")
    baseline = MutationPolicy(
        enabled=False,
        publication_suppressed=True,
        capabilities=frozenset(),
    )

    effective, overridden = _effective_mutation_policy(
        request,
        baseline,
        "page.upload",
        {},
    )

    assert overridden is False
    assert effective is baseline

def test_trusted_repoai_personalization_derives_nonpublication_plan() -> None:
    request = _request()
    client = _client(key="repoai:run:ai.personalization:abc")
    plan = _repo_ai_internal_plan_authorization(
        request,
        client,
        "ai.personalization",
        {"aligned": True},
    )

    assert plan["authorized"] is True
    assert plan["publication_authorized"] is False
    assert plan["action_id"] == "ai.personalization"


def test_trusted_repoai_personalization_does_not_unsuppress_publication() -> None:
    request = _request()
    plan = _repo_ai_internal_plan_authorization(
        request,
        _client(key="repoai:run:ai.personalization:abc"),
        "ai.personalization",
        {"aligned": True},
    )
    baseline = MutationPolicy(
        enabled=False,
        publication_suppressed=True,
        capabilities=frozenset(),
    )

    effective, overridden = _effective_mutation_policy(
        request,
        baseline,
        "ai.personalization",
        plan,
    )
    receipt = require_mutation_capability(
        "ai.personalization",
        governance_aligned=True,
        plan_authorization=plan,
        policy=effective,
    )

    assert overridden is False
    assert effective.enabled is True
    assert effective.publication_suppressed is True
    assert effective.capabilities == frozenset({"ai.personalization"})
    assert receipt["publication_authorized"] is False
