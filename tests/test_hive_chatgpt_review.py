"""No-network, no-credential admission tests for the disabled compiled adapter."""
from __future__ import annotations

import hashlib
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from app.hive_chatgpt_review import (
    ReviewerCancelled, ReviewerOutcomeUnknown, run_chatgpt_review,
)
from app.hive_dispatcher import HiveExecutionDispatcherStore


def _context(**updates):
    values = {
        "execution_id": "execution-101", "mission_id": "mission-101",
        "work_unit_id": "unit-101", "worker_id": "worker-101",
        "workspace_id": "workspace-101", "user_id": "user-101",
        "profile_name": "chatgpt-isolated-owner",
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
        self.submissions = []
        self.readbacks = []
        self.status = "completed"
        self.hash_mismatch = False

    def submit(self, *, idempotency_key, prompt, context):
        self.submissions.append((idempotency_key, prompt, context.profile_name))
        return {"id": "chatjob-accepted-101", "status": "queued"}

    def retrieve(self, *, job_id, context):
        self.readbacks.append((job_id, context.profile_name))
        answer = "Independent reviewer found missing background-tab proof."
        return {
            "id": job_id, "status": self.status, "provider": "chatgpt",
            "profile_name": context.profile_name,
            "verified_thread_id": "native-chatgpt-101", "review_text": answer,
            "response_sha256": (
                "0" * 64 if self.hash_mismatch
                else hashlib.sha256(answer.encode()).hexdigest()
            ),
        }


def _run(transport, *, context=None, payload=None, cancelled=None):
    return run_chatgpt_review(
        context=context or _context(),
        payload=(payload if payload is not None
                 else {"lane": "AG002", "evidence_sha256": "a" * 64}),
        transport=transport, cancelled=cancelled or (lambda: False),
    )


def test_registered_adapter_is_disabled_and_requires_independent_review(tmp_path):
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
            {"lane": "AG002", "evidence_sha256": "a" * 64},
            lambda: False,
        )


def test_exact_one_submission_verified_native_context_and_hash():
    service = _Transport()
    result = _run(service)
    assert len(service.submissions) == len(service.readbacks) == 1
    assert result["submission_attempts"] == 1
    assert result["native_conversation_id"] == "native-chatgpt-101"
    assert result["performed_external_effect"] is True
    assert result["provider"] == "chatgpt"
    assert "Do not invoke tools" in service.submissions[0][1]
    assert service.submissions[0][2] == "chatgpt-isolated-owner"
    other = _Transport()
    _run(other)
    assert service.submissions[0][0] == other.submissions[0][0]


@pytest.mark.parametrize("bad", [
    {}, {"lane": "AG002"}, {"lane": "AG002", "evidence_sha256": "a" * 64,
                            "url": "https://evil.example"},
    {"lane": "AG002", "evidence_sha256": "A" * 64},
    {"lane": "FREEFORM", "evidence_sha256": "a" * 64},
    {"lane": "AG002", "evidence_sha256": "a" * 63},
])
def test_bad_payload_never_submits(bad):
    service = _Transport()
    with pytest.raises(ValueError):
        _run(service, payload=bad)
    assert service.submissions == []


@pytest.mark.parametrize("bad_context", [
    {"authority_ceiling": "A0"}, {"writable_domains": ("production",)},
    {"profile_name": ""}, {"source_boundary": ""}, {"workspace_id": ""},
])
def test_unbound_or_writable_lease_never_submits(bad_context):
    service = _Transport()
    with pytest.raises(ValueError):
        _run(service, context=_context(**bad_context))
    assert not service.submissions


def test_cancel_before_submit_has_no_provider_side_effect():
    service = _Transport()
    with pytest.raises(ReviewerCancelled):
        _run(service, cancelled=lambda: True)
    assert service.submissions == []


def test_uncertain_submit_is_not_retried():
    service = _Transport()
    service.submit = Mock(side_effect=TimeoutError("provider may have accepted"))
    with pytest.raises(ReviewerOutcomeUnknown, match="submission_uncertain"):
        _run(service)
    assert service.submit.call_count == 1


@pytest.mark.parametrize("defect", ["pending", "wrong_provider", "wrong_profile",
                                      "wrong_hash", "missing_native", "read_error"])
def test_uncertain_readback_is_not_resent(defect):
    service = _Transport()
    original = service.retrieve

    def broken(*, job_id, context):
        if defect == "read_error":
            raise TimeoutError("post-submit readback uncertain")
        value = original(job_id=job_id, context=context)
        if defect == "pending":
            value["status"] = "queued"
        if defect == "wrong_provider":
            value["provider"] = "copilot"
        if defect == "wrong_profile":
            value["profile_name"] = "other-profile"
        if defect == "wrong_hash":
            value["response_sha256"] = "0" * 64
        if defect == "missing_native":
            value["verified_thread_id"] = ""
        return value

    service.retrieve = broken
    with pytest.raises(ReviewerOutcomeUnknown) as error:
        _run(service)
    assert error.value.evidence["job_id"] == "chatjob-accepted-101"
    assert len(service.submissions) == 1
