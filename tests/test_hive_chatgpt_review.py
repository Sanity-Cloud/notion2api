"""No-network reviewer idempotency, source provenance and recovery tests."""
from __future__ import annotations

import hashlib
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.hive_chatgpt_review import (
    ReviewerCancelled, ReviewerOutcomeUnknown, reconcile_chatgpt_review,
    run_chatgpt_review,
)
from app.hive_dispatcher import HiveExecutionDispatcherStore

SOURCE = "Sanitized reviewer source excerpt under the exact Git commit."
DIGEST = hashlib.sha256(SOURCE.encode("utf-8")).hexdigest()
MANIFEST = {
    "repository": "Sanity-Cloud/notion2api",
    "pr_number": 11,
    "head_sha": "a" * 40,
    "base_sha": "b" * 40,
    "canonicalizer": "utf8-v1",
    "source_boundary": "Notion approved test evidence",
}
PAYLOAD = {"lane": "AG002", "evidence_sha256": DIGEST}


def _context(**updates):
    values = {
        "execution_id": "execution-101", "plan_id": "plan-101",
        "mission_id": "mission-101", "work_unit_id": "unit-101",
        "worker_id": "worker-101", "workspace_id": "workspace-101",
        "user_id": "user-101", "profile_name": "chatgpt-isolated-owner",
        "conversation_id": "hive-conversation-101",
        "hive_worker_lease_id": "lease-101",
        "dispatch_receipt_id": "dispatch-101",
        "source_boundary": "Notion approved test evidence",
        "authority_ceiling": "A1", "writable_domains": (),
    }
    values.update(updates)
    return SimpleNamespace(**values)


class _Transport:
    def __init__(self):
        self.sources = []
        self.reservations = []
        self.submissions = []
        self.readbacks = []
        self.lookups = []
        self.staged = {}
        self.status = "completed"
        self.readback_changes = {}
        self.source = SOURCE
        self.manifest = MANIFEST.copy()
        self.reservation_changes = {}
        self.fail_after_accept = False

    def resolve_evidence(self, *, evidence_sha256, context):
        self.sources.append((evidence_sha256, context))
        return {"source_text": self.source, "manifest": self.manifest.copy()}

    def reserve(self, *, idempotency_key, prompt_sha256,
                manifest_sha256, context):
        assert not hasattr(context, "secret_token")
        self.reservations.append((idempotency_key, context))
        record = {
            "idempotency_key": idempotency_key,
            "operation_id": "review-operation-101",
            "prompt_sha256": prompt_sha256,
            "manifest_sha256": manifest_sha256,
            "durable": True,
        }
        record.update(self.reservation_changes)
        self.staged[idempotency_key] = {
            **record, "id": "chatjob-accepted-101",
        }
        return record

    def submit(self, *, idempotency_key, operation_id, prompt, context):
        self.submissions.append((idempotency_key, operation_id, prompt, context))
        assert self.staged[idempotency_key]["operation_id"] == operation_id
        if self.fail_after_accept:
            raise ConnectionResetError("provider may have received the turn")
        return {
            "id": "chatjob-accepted-101",
            "operation_id": operation_id,
            "idempotency_key": idempotency_key,
        }

    def lookup_by_idempotency_key(self, *, idempotency_key, context):
        self.lookups.append((idempotency_key, context))
        return self.staged.get(idempotency_key, {})

    def retrieve(self, *, job_id, context):
        self.readbacks.append((job_id, context))
        last = next(iter(self.staged.values()))
        answer = "Independent reviewer found missing background-tab proof."
        record = {
            "id": job_id, "status": self.status, "provider": "chatgpt",
            "profile_name": context.profile_name,
            "operation_id": last["operation_id"],
            "idempotency_key": last["idempotency_key"],
            "source_manifest_sha256": last["manifest_sha256"],
            "prompt_sha256": last["prompt_sha256"],
            "binding": asdict(context),
            "verified_thread_id": "native-chatgpt-101",
            "review_text": answer,
            "response_sha256": hashlib.sha256(answer.encode()).hexdigest(),
        }
        record.update(self.readback_changes)
        return record


def _run(transport, *, context=None, payload=None, cancelled=None):
    return run_chatgpt_review(
        context=context or _context(),
        payload=PAYLOAD if payload is None else payload,
        transport=transport, cancelled=cancelled or (lambda: False),
    )


def _recover(transport, *, context=None, payload=None):
    return reconcile_chatgpt_review(
        context=context or _context(), payload=PAYLOAD if payload is None else payload,
        transport=transport,
    )


def test_compiled_adapter_stays_disabled_and_requires_approval(tmp_path):
    store = HiveExecutionDispatcherStore(tmp_path / "hive.sqlite3")
    adapter = store.list_adapters(
        adapter_id="builtin.aigentai_chatgpt_review.v1"
    ).adapters[0]
    assert adapter.status == "DISABLED"
    assert adapter.required_authority == "A1"
    assert adapter.requires_human_approval
    assert adapter.requires_independent_review
    assert adapter.writable_domains == []
    with pytest.raises(Exception, match="transport is not configured"):
        store._run_adapter(
            "nonexistent", "test", adapter.implementation_id,
            PAYLOAD, lambda: False,
        )


def test_one_submission_with_durable_reservation_and_bound_source():
    service = _Transport()
    original_context = _context(secret_token="must-not-reach-transport")
    result = _run(service, context=original_context)
    assert len(service.sources) == len(service.reservations) == 1
    assert len(service.submissions) == len(service.readbacks) == 1
    assert result["submission_attempts"] == 1
    assert result["native_conversation_id"] == "native-chatgpt-101"
    assert result["performed_external_effect"] is True
    assert result["provider"] == "chatgpt"
    assert "Sanitized reviewer source" in service.submissions[0][2]
    assert result["source_manifest"]["head_sha"] == "a" * 40
    assert result["source_manifest"]["base_sha"] == "b" * 40
    assert result["source_manifest"]["repository"] == "Sanity-Cloud/notion2api"
    assert result["adapter"] == "builtin.aigentai_chatgpt_review.v1"
    assert result["prompt_contract"] == "review-source-v2"
    assert result["provenance"]["workspace_id"] == "workspace-101"
    assert result["provenance"]["dispatch_receipt_id"] == "dispatch-101"
    assert not hasattr(service.submissions[0][3], "secret_token")
    other = _Transport()
    _run(other)
    assert service.submissions[0][0] == other.submissions[0][0]


@pytest.mark.parametrize("bad", [
    {}, {"lane": "AG002"}, {**PAYLOAD, "url": "https://evil.example"},
    {"lane": "AG002", "evidence_sha256": "A" * 64},
    {"lane": "FREEFORM", "evidence_sha256": DIGEST},
    {"lane": "AG002", "evidence_sha256": DIGEST[:-1]},
])
def test_invalid_payload_never_reserves_or_submits(bad):
    service = _Transport()
    with pytest.raises(ValueError):
        _run(service, payload=bad)
    assert not service.reservations
    assert not service.submissions


@pytest.mark.parametrize("updates", [
    {"authority_ceiling": "A0"}, {"writable_domains": ("production",)},
    {"profile_name": ""}, {"source_boundary": ""}, {"workspace_id": ""},
    {"plan_id": ""}, {"hive_worker_lease_id": ""},
])
def test_invalid_materialized_context_never_reserves(updates):
    service = _Transport()
    with pytest.raises(ValueError):
        _run(service, context=_context(**updates))
    assert not service.reservations
    assert not service.submissions


@pytest.mark.parametrize("defect", [
    "wrong_source", "wrong_repo", "wrong_head", "wrong_base", "wrong_boundary",
    "wrong_canonicalizer", "missing_manifest",
])
def test_evidence_manifest_mismatch_fails_before_provider_effect(defect):
    service = _Transport()
    if defect == "wrong_source":
        service.source += " modified"
    if defect == "wrong_repo":
        service.manifest["repository"] = "../../other-repo"
    if defect == "wrong_head":
        service.manifest["head_sha"] = "invalid"
    if defect == "wrong_base":
        service.manifest["base_sha"] = "invalid"
    if defect == "wrong_boundary":
        service.manifest["source_boundary"] = "another legal matter"
    if defect == "wrong_canonicalizer":
        service.manifest["canonicalizer"] = "unverified-v0"
    if defect == "missing_manifest":
        service.manifest.pop("head_sha")
    with pytest.raises(ValueError):
        _run(service)
    assert not service.reservations
    assert not service.submissions


def test_cancel_before_or_after_reservation_never_submits():
    service = _Transport()
    with pytest.raises(ReviewerCancelled):
        _run(service, cancelled=lambda: True)
    assert not service.reservations
    assert not service.submissions

    checks = iter((False, True))
    with pytest.raises(ReviewerCancelled):
        _run(service, cancelled=lambda: next(checks))
    assert len(service.reservations) == 1
    assert not service.submissions


@pytest.mark.parametrize("defect", [
    {"durable": False}, {"operation_id": ""},
    {"idempotency_key": "other"}, {"prompt_sha256": "0" * 64},
    {"manifest_sha256": "0" * 64},
])
def test_unverified_reservation_fails_without_submission(defect):
    service = _Transport()
    service.reservation_changes = defect
    with pytest.raises(ReviewerOutcomeUnknown, match="reservation_unverified"):
        _run(service)
    assert len(service.reservations) == 1
    assert not service.submissions


def test_post_accept_submit_exception_recovered_by_key_without_replay():
    service = _Transport()
    service.fail_after_accept = True
    with pytest.raises(ReviewerOutcomeUnknown, match="submission_uncertain") as exc:
        _run(service)
    assert exc.value.evidence["operation_id"] == "review-operation-101"
    assert len(service.submissions) == 1
    result = _recover(service)
    assert result["job_id"] == "chatjob-accepted-101"
    assert result["reconciled_without_replay"] is True
    assert result["submission_attempts"] == 0
    assert len(service.submissions) == 1
    assert len(service.lookups) == 1


@pytest.mark.parametrize("defect", [
    "missing_key", "wrong_job", "wrong_manifest", "lookup_error",
])
def test_reconciliation_never_resubmits_when_lookup_unproven(defect):
    service = _Transport()
    _run(service)
    original = service.lookup_by_idempotency_key

    def broken(*, idempotency_key, context):
        if defect == "lookup_error":
            raise TimeoutError("read-only lookup failed")
        record = dict(original(idempotency_key=idempotency_key, context=context))
        if defect == "missing_key":
            record["idempotency_key"] = "other"
        if defect == "wrong_job":
            record["id"] = ""
        if defect == "wrong_manifest":
            record["manifest_sha256"] = "0" * 64
        return record

    service.lookup_by_idempotency_key = broken
    with pytest.raises(ReviewerOutcomeUnknown):
        _recover(service)
    assert len(service.submissions) == 1


@pytest.mark.parametrize("field,value", [
    ("status", "queued"), ("provider", "copilot"),
    ("profile_name", "other-profile"), ("idempotency_key", "other"),
    ("source_manifest_sha256", "0" * 64), ("prompt_sha256", "0" * 64),
    ("verified_thread_id", ""), ("response_sha256", "0" * 64),
    ("operation_id", "another-operation"),
])
def test_unverified_readback_never_counts_as_success(field, value):
    service = _Transport()
    service.readback_changes[field] = value
    with pytest.raises(ReviewerOutcomeUnknown):
        _run(service)
    assert len(service.submissions) == 1


@pytest.mark.parametrize("name", [
    "user_id", "workspace_id", "conversation_id", "worker_id",
    "hive_worker_lease_id", "dispatch_receipt_id", "source_boundary",
])
def test_wrong_owner_affinity_refuses_readback(name):
    service = _Transport()
    def mismatched(*, job_id, context):
        original = _Transport.retrieve(service, job_id=job_id, context=context)
        original["binding"][name] = "different-owner-boundary"
        return original
    service.retrieve = mismatched
    with pytest.raises(ReviewerOutcomeUnknown, match="provider_or_owner_scope_mismatch"):
        _run(service)
    assert len(service.submissions) == 1


def test_unknown_source_or_transport_is_fail_closed():
    with pytest.raises(ValueError, match="transport is not configured"):
        _run(None)
    service = _Transport()
    service.reserve = Mock(side_effect=TimeoutError("durable reservation unavailable"))
    with pytest.raises(ReviewerOutcomeUnknown, match="reservation_unavailable"):
        _run(service)
    assert not service.submissions
