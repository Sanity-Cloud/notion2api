"""Compiled, fail-closed AIgentBee ChatGPT reviewer contract.

No network client or credentials are shipped here. The trusted owner runtime must
supply a separately qualified transport; registry seeding leaves this disabled.
"""
from __future__ import annotations

import hashlib
import re
from typing import Any, Callable, Protocol

PROMPTS = {
    "AG002": (
        "Read-only review: identify fail-closed CDP background-tab affinity and "
        "native user/assistant turn evidence. Do not send provider prompts."
    ),
    "AG003": (
        "Read-only review: Arena Direct/Side-by-Side native turn provenance and "
        "uncertain-send rollback tests. Do not enable the HTTP 423 write routes."
    ),
    "AG005": (
        "Read-only security review: owner OAuth, Cloudflare Access JWT, and MCP "
        "initialize/tools/list evidence. Do not request or alter credentials."
    ),
    "V5": (
        "Read-only release review: v4-v5 SQLite WAL writer exclusion, unknown "
        "outcomes, backup, live migration and rollback acceptance. No writes."
    ),
}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_NATIVE = re.compile(r"[a-zA-Z0-9_-]{6,256}\Z")


class ReviewerCancelled(RuntimeError):
    """Cancellation happened before a provider submission."""


class ReviewerOutcomeUnknown(RuntimeError):
    """A provider turn may exist and must only be reconciled, never resent."""

    def __init__(self, stage: str, *, job_id: str = "") -> None:
        super().__init__(f"ChatGPT reviewer outcome unknown: {stage}")
        self.stage = stage
        self.evidence = {"job_id": job_id} if job_id else {}


class ReviewerTransport(Protocol):
    """Trusted, owner-bound transport; never supplied through dispatch payload."""

    def submit(self, *, idempotency_key: str, prompt: str, context: Any) -> dict[str, Any]:
        ...

    def retrieve(self, *, job_id: str, context: Any) -> dict[str, Any]:
        ...


def _validated_payload(payload: dict[str, Any]) -> tuple[str, str]:
    if type(payload) is not dict or set(payload) != {"lane", "evidence_sha256"}:
        raise ValueError("Only lane and evidence_sha256 are accepted.")
    lane = payload["lane"]
    digest = payload["evidence_sha256"]
    if not isinstance(lane, str) or lane not in PROMPTS:
        raise ValueError("Unknown compiled review lane.")
    if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
        raise ValueError("Evidence SHA-256 must be lowercase hexadecimal.")
    return lane, digest


def _validated_context(context: Any) -> None:
    for name in (
        "execution_id", "mission_id", "work_unit_id", "worker_id",
        "workspace_id", "user_id", "profile_name", "conversation_id",
        "hive_worker_lease_id", "dispatch_receipt_id", "source_boundary",
    ):
        if not isinstance(getattr(context, name, None), str) or not getattr(context, name):
            raise ValueError(f"Missing materialized reviewer context: {name}.")
    if getattr(context, "authority_ceiling", "") not in ("A1", "A2", "A3", "A4"):
        raise ValueError("Review worker requires appointed A1 or higher authority.")
    if tuple(getattr(context, "writable_domains", ())) != ():
        raise ValueError("ChatGPT reviewer cannot receive writable domains.")


def run_chatgpt_review(
    *, context: Any, payload: dict[str, Any],
    transport: ReviewerTransport | None, cancelled: Callable[[], bool],
) -> dict[str, Any]:
    """Submit once, retrieve once; never retry uncertain external effects."""
    lane, digest = _validated_payload(payload)
    _validated_context(context)
    if transport is None:
        raise ValueError("Qualified owner-bound ChatGPT transport is not configured.")
    if cancelled():
        raise ReviewerCancelled("Cancelled before any ChatGPT submission.")
    fingerprint = "|".join(
        (context.execution_id, context.mission_id, context.work_unit_id,
         context.worker_id, context.profile_name, lane, digest)
    )
    key = "aigentbee-review-" + hashlib.sha256(fingerprint.encode()).hexdigest()
    prompt = (
        "AiGentAI isolated ChatGPT review. Operate only as a read-only advisor. "
        "Do not invoke tools, modify accounts, or replay unknown provider sends. "
        + PROMPTS[lane] + " Evidence digest: " + digest
    )
    try:
        queued = transport.submit(
            idempotency_key=key, prompt=prompt, context=context
        )
    except Exception as exc:
        raise ReviewerOutcomeUnknown("submission_uncertain") from exc
    if not isinstance(queued, dict) or not isinstance(queued.get("id"), str):
        raise ReviewerOutcomeUnknown("missing_durable_job_id")
    job = queued["id"]
    if not _NATIVE.fullmatch(job):
        raise ReviewerOutcomeUnknown("invalid_durable_job_id")
    if cancelled():
        raise ReviewerOutcomeUnknown("cancelled_after_submission", job_id=job)
    try:
        readback = transport.retrieve(job_id=job, context=context)
    except Exception as exc:
        raise ReviewerOutcomeUnknown("retrieval_uncertain", job_id=job) from exc
    if not isinstance(readback, dict) or readback.get("status") != "completed":
        raise ReviewerOutcomeUnknown("pending_or_failed_readback", job_id=job)
    if (readback.get("id") != job
            or readback.get("provider") != "chatgpt"
            or readback.get("profile_name") != context.profile_name):
        raise ReviewerOutcomeUnknown("provider_scope_mismatch", job_id=job)
    native = readback.get("verified_thread_id")
    answer = readback.get("review_text")
    if (not isinstance(native, str) or not _NATIVE.fullmatch(native)
            or not isinstance(answer, str) or not 1 <= len(answer) <= 3500):
        raise ReviewerOutcomeUnknown("native_turn_unverified", job_id=job)
    sha = hashlib.sha256(answer.encode("utf-8")).hexdigest()
    if readback.get("response_sha256") != sha:
        raise ReviewerOutcomeUnknown("response_hash_mismatch", job_id=job)
    return {
        "lane": lane, "job_id": job, "native_conversation_id": native,
        "review_text": answer, "response_sha256": sha,
        "evidence_sha256": digest, "performed_external_effect": True,
        "submission_attempts": 1, "provider": "chatgpt",
    }
