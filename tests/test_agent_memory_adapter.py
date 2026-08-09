from __future__ import annotations

import pytest

from app.agent_memory import AgentMemoryStore, IdentityEnvelope, RetrievalBudget, SanityCloudMemoryAdapter
from app.agent_memory.models import AgentMemoryError, UPSTREAM_COMMIT


def identity(**overrides):
    data = {
        "workspace_ref": "sanity-management",
        "memory_domain_id": "sc-amf-test",
        "project_ref": "SC-AMF-001",
        "mission_id": "SC-AMF-001-R1",
        "work_unit_id": "SC-AMF-001-R1-lane-04-sc-amf-integration",
        "task_ref": "adapter-test",
        "lane_id": "L2",
        "worker_ref": "sc-amf-integration",
        "principal_ref": "principal-a",
        "lease_ref": "lease-test-a",
        "context_version": "SC-AMF-001-v0.2",
    }
    data.update(overrides)
    return IdentityEnvelope(**data)


@pytest.fixture()
def adapter(tmp_path):
    def validate(ident, operation):
        return {
            "allowed": ident.lease_ref.startswith("lease-test-"),
            "operation": operation,
            "authority_ceiling": "A2",
        }

    return SanityCloudMemoryAdapter(
        AgentMemoryStore(str(tmp_path / "agent-memory.sqlite3")),
        lease_validator=validate,
    )


def write(adapter, ident, key, payload="alpha context", **kwargs):
    return adapter.write_candidate(
        identity=ident,
        request_id=f"request-{key}",
        idempotency_key=key,
        payload=payload,
        source_refs=["source-1"],
        source_hashes=["hash-1"],
        **kwargs,
    )


def test_health_pins_upstream_and_has_no_worker_credentials(adapter):
    result = adapter.health(identity())
    assert result["upstream"]["commit"] == UPSTREAM_COMMIT
    assert result["credential_handling"] is False
    assert "/v3/core/write" in " ".join(result["upstream"]["pilot_denied_routes"])


def test_identity_scope_fails_closed(adapter):
    owner = identity()
    created = write(adapter, owner, "scope-1")
    other = identity(principal_ref="principal-b", lease_ref="lease-test-b")
    with pytest.raises(AgentMemoryError) as exc:
        adapter.get(identity=other, derived_memory_id=created["derived_memory_id"])
    assert exc.value.code == "SCOPE_DENIED"


def test_candidate_is_not_retrieval_eligible_until_external_review(adapter):
    ident = identity()
    created = write(adapter, ident, "candidate-1")
    assert adapter.compile_context(identity=ident)["context"] == ""
    adapter.mark_retrieval_eligible_for_test(
        identity=ident,
        derived_memory_id=created["derived_memory_id"],
        reviewer_receipt="fan-in-1",
    )
    assert adapter.compile_context(identity=ident)["context"] == "alpha context"


def test_secret_payload_is_quarantined_without_raw_persistence(adapter):
    ident = identity()
    result = write(adapter, ident, "secret-1", payload="api_key=super-secret-value")
    assert result["status"] == "QUARANTINED"
    record = adapter.get(identity=ident, derived_memory_id=result["derived_memory_id"])
    assert "payload" not in record
    assert record["sensitivity"] == "SECRET_PROHIBITED"


def test_idempotent_write_returns_same_record(adapter):
    ident = identity()
    first = write(adapter, ident, "idem-1")
    second = write(adapter, ident, "idem-1")
    assert second == first
    with pytest.raises(AgentMemoryError) as exc:
        write(adapter, ident, "idem-1", payload="different")
    assert exc.value.code == "IDEMPOTENCY_CONFLICT"


def test_superseded_record_is_excluded_from_context(adapter):
    ident = identity()
    old = write(adapter, ident, "sup-old", payload="old value")
    new = write(adapter, ident, "sup-new", payload="new value")
    for item in (old, new):
        adapter.mark_retrieval_eligible_for_test(
            identity=ident,
            derived_memory_id=item["derived_memory_id"],
            reviewer_receipt="fan-in-2",
        )
    adapter.supersede(
        identity=ident,
        request_id="supersede-request",
        idempotency_key="supersede-key",
        old_id=old["derived_memory_id"],
        successor_id=new["derived_memory_id"],
        rationale="new source supersedes old source",
        reviewer_receipt="fan-in-3",
    )
    packet = adapter.compile_context(identity=ident)
    assert "old value" not in packet["context"]
    assert "new value" in packet["context"]


def test_retrieval_budget_is_enforced(adapter):
    ident = identity()
    ids = []
    for idx in range(3):
        created = write(adapter, ident, f"budget-{idx}", payload=f"item-{idx}")
        ids.append(created["derived_memory_id"])
        adapter.mark_retrieval_eligible_for_test(
            identity=ident,
            derived_memory_id=created["derived_memory_id"],
            reviewer_receipt=f"review-{idx}",
        )
    result = adapter.search(identity=ident, query="item", budget=RetrievalBudget(max_assets=2, max_chars=12000))
    assert len(result["items"]) == 2
    assert any(item["reason"] == "budget" for item in result["selection_manifest"]["excluded"])


def test_cancellation_stops_candidate_before_write(adapter):
    ident = identity()
    adapter.cancel(identity=ident, cancellation_ref="cancel-1", reason="test")
    result = write(adapter, ident, "cancel-write", cancellation_ref="cancel-1")
    assert result["status"] == "CANCELLED"
    assert adapter.audit_evidence(identity=ident)["records"] == 0


def test_dissent_and_evidence_gaps_survive_context_compile(adapter):
    ident = identity()
    created = write(
        adapter,
        ident,
        "dissent-1",
        evidence_gaps=[{"gap_id": "g1", "statement": "needs corroboration", "materiality": "HIGH"}],
        dissent_refs=["dissent-1"],
    )
    adapter.mark_retrieval_eligible_for_test(
        identity=ident,
        derived_memory_id=created["derived_memory_id"],
        reviewer_receipt="review-dissent",
    )
    packet = adapter.compile_context(identity=ident)
    assert packet["notices"]
    assert packet["canonical"] is False


def test_no_worker_callable_promote_surface(adapter):
    assert not hasattr(adapter, "promote")
    assert not hasattr(adapter, "approve_canonical")


def test_missing_authoritative_lease_validator_fails_closed(tmp_path):
    ungoverned = SanityCloudMemoryAdapter(AgentMemoryStore(str(tmp_path / "deny.sqlite3")))
    with pytest.raises(AgentMemoryError) as exc:
        ungoverned.health(identity())
    assert exc.value.code == "LEASE_UNVERIFIED"


def test_provenance_is_required_for_candidate_writes(adapter):
    with pytest.raises(AgentMemoryError) as exc:
        adapter.write_candidate(
            identity=identity(),
            request_id="missing-provenance",
            idempotency_key="missing-provenance",
            payload="claim",
        )
    assert exc.value.code == "PROVENANCE_REQUIRED"


def test_running_idempotency_receipt_blocks_semantic_replay(adapter):
    from app.agent_memory.models import CONTRACT_ID, stable_hash

    ident = identity()
    body = {
        "payload_hash": "placeholder",
        "asset_type": "chat_memory",
        "layer": "L1",
        "source_refs": ["source-1"],
        "source_hashes": ["hash-1"],
    }
    request_hash = stable_hash(
        {
            "identity": ident.receipt(),
            "operation": "memory.write_candidate",
            "payload": body,
            "contract_id": CONTRACT_ID,
        }
    )
    adapter.store.begin_operation(
        idempotency_key="inflight-1",
        request_id="inflight-1",
        operation="memory.write_candidate",
        request_hash=request_hash,
        identity=ident.receipt(),
    )
    with pytest.raises(AgentMemoryError) as exc:
        adapter._begin_mutation(
            identity=ident,
            request_id="inflight-1",
            idempotency_key="inflight-1",
            operation="memory.write_candidate",
            payload=body,
        )
    assert exc.value.code == "OUTCOME_UNKNOWN"


def test_outcome_unknown_requires_reconciliation_without_replay(adapter):
    from app.agent_memory.models import CONTRACT_ID, stable_hash

    ident = identity()
    payload = {"operation": "synthetic-provider-write"}
    request_hash = stable_hash(
        {
            "identity": ident.receipt(),
            "operation": "memory.synthetic_provider_write",
            "payload": payload,
            "contract_id": CONTRACT_ID,
        }
    )
    adapter.store.begin_operation(
        idempotency_key="unknown-1",
        request_id="unknown-1",
        operation="memory.synthetic_provider_write",
        request_hash=request_hash,
        identity=ident.receipt(),
    )
    unknown = adapter.mark_outcome_unknown(
        identity=ident,
        idempotency_key="unknown-1",
        reason="connection lost after provider acceptance",
        upstream_locator="synthetic-upstream-1",
    )
    assert unknown["semantic_replay_blocked"] is True
    reconciled = adapter.reconcile_outcome(
        identity=ident,
        idempotency_key="unknown-1",
        outcome="COMPLETED",
        evidence={"provider_receipt": "synthetic-upstream-1"},
    )
    assert reconciled["reconciled"] is True
    assert reconciled["semantic_replay_performed"] is False


def test_contradiction_is_bidirectional_preserved_and_excluded(adapter):
    ident = identity()
    first = write(adapter, ident, "conflict-a", payload="setting is enabled")
    second = write(adapter, ident, "conflict-b", payload="setting is disabled")
    for item in (first, second):
        adapter.mark_retrieval_eligible_for_test(
            identity=ident,
            derived_memory_id=item["derived_memory_id"],
            reviewer_receipt="review-before-conflict",
        )
    receipt = adapter.record_contradiction(
        identity=ident,
        first_id=first["derived_memory_id"],
        second_id=second["derived_memory_id"],
        evidence_gap={
            "gap_id": "conflict-gap",
            "statement": "sources disagree",
            "materiality": "HIGH",
        },
        dissent_ref="dissent-conflict",
    )
    assert receipt["winner_selected"] is False
    first_record = adapter.get(identity=ident, derived_memory_id=first["derived_memory_id"])
    second_record = adapter.get(identity=ident, derived_memory_id=second["derived_memory_id"])
    assert second["derived_memory_id"] in first_record["contradicts"]
    assert first["derived_memory_id"] in second_record["contradicts"]
    assert first_record["evidence_class"] == "CONFLICTED"
    assert adapter.compile_context(identity=ident)["context"] == ""
