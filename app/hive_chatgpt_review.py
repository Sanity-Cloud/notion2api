"""Disabled AIgentBee ChatGPT reviewer contract, with read-only reconciliation.

No network client, credential, profile selector or provider send implementation lives
here. A separately authorized, qualified owner runtime supplies the transport.
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import asdict, dataclass
from typing import Any, Callable, Protocol

PROMPTS = {
    "AG002": (
        "Review Copilot CDP hidden-tab affinity and native individual-turn evidence. "
        "Do not submit additional provider prompts."
    ),
    "AG003": (
        "Review Arena Direct/Side-by-Side turn provenance and unknown-send recovery. "
        "Never lift the Arena HTTP write gate."
    ),
    "AG005": (
        "Review owner OAuth, Cloudflare Access and authenticated MCP tool discovery. "
        "Never request secrets or circumvent authorization."
    ),
    "V5": (
        "Review exclusive v4-v5 SQLite WAL migration, recovery and rollback. "
        "Never modify production owner journals."
    ),
}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_NATIVE = re.compile(r"[A-Za-z0-9_-]{6,256}\Z")
_REPO = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+\Z")
_SHA40 = re.compile(r"[0-9a-f]{40}\Z")
_SCOPE_FIELDS = (
    "execution_id", "plan_id", "mission_id", "work_unit_id", "worker_id",
    "workspace_id", "user_id", "profile_name", "conversation_id",
    "hive_worker_lease_id", "dispatch_receipt_id", "source_boundary",
    "authority_ceiling",
)
_ADAPTER_VERSION = "builtin.aigentai_chatgpt_review.v1"
_CONTRACT_VERSION = "review-source-v2"


@dataclass(frozen=True, slots=True)
class ReviewerScope:
    """Only the permitted immutable authorization fields reach the transport."""

    execution_id: str
    plan_id: str
    mission_id: str
    work_unit_id: str
    worker_id: str
    workspace_id: str
    user_id: str
    profile_name: str
    conversation_id: str
    hive_worker_lease_id: str
    dispatch_receipt_id: str
    source_boundary: str
    authority_ceiling: str
    writable_domains: tuple[str, ...] = ()


class ReviewerCancelled(RuntimeError):
    """Cancellation before any provider effect."""


class ReviewerOutcomeUnknown(RuntimeError):
    """Never replay: reconcile by the original stable key."""

    def __init__(self, stage: str, **evidence: str) -> None:
        super().__init__(f"ChatGPT reviewer outcome unknown: {stage}")
        self.stage = stage
        self.evidence = {
            name: value for name, value in evidence.items()
            if isinstance(value, str) and value and len(value) <= 256
        }


class ReviewerTransport(Protocol):
    """Injected trusted transport must enforce durable idempotency and owner scope.

    reserve MUST durably commit the operation key BEFORE any provider submission.
    lookup_by_idempotency_key MUST be read-only and MUST NOT submit. This is a
    required contract to qualify, not proof any concrete transport satisfies it.
    """

    def resolve_evidence(
        self, *, evidence_sha256: str, context: ReviewerScope,
    ) -> dict[str, Any]:
        ...

    def reserve(
        self, *, idempotency_key: str, prompt_sha256: str,
        manifest_sha256: str, context: ReviewerScope,
    ) -> dict[str, Any]:
        ...

    def submit(
        self, *, idempotency_key: str, operation_id: str,
        prompt: str, context: ReviewerScope,
    ) -> dict[str, Any]:
        ...

    def lookup_by_idempotency_key(
        self, *, idempotency_key: str, context: ReviewerScope,
    ) -> dict[str, Any]:
        ...

    def retrieve(
        self, *, job_id: str, context: ReviewerScope,
    ) -> dict[str, Any]:
        ...


def _scope(context: Any) -> ReviewerScope:
    values = {}
    for name in _SCOPE_FIELDS:
        value = getattr(context, name, None)
        max_length = 256 if name == "source_boundary" else 128
        if not isinstance(value, str) or not value.strip() or len(value) > max_length:
            raise ValueError(f"Invalid materialized reviewer context: {name}.")
        values[name] = value
    if values["authority_ceiling"] not in {"A1", "A2", "A3", "A4"}:
        raise ValueError("Reviewer requires an appointed A1 or higher authority.")
    if tuple(getattr(context, "writable_domains", ()) or ()) != ():
        raise ValueError("ChatGPT reviewer cannot receive writable domains.")
    return ReviewerScope(**values)


def _payload(payload: dict[str, Any]) -> tuple[str, str]:
    if type(payload) is not dict or set(payload) != {"lane", "evidence_sha256"}:
        raise ValueError("Only compiled lane and evidence_sha256 are accepted.")
    lane, digest = payload["lane"], payload["evidence_sha256"]
    if not isinstance(lane, str) or lane not in PROMPTS:
        raise ValueError("Unknown compiled reviewer lane.")
    if not isinstance(digest, str) or not _SHA256.fullmatch(digest):
        raise ValueError("Evidence digest must be lower-case SHA-256.")
    return lane, digest


def _source(
    transport: ReviewerTransport, digest: str, scope: ReviewerScope,
) -> tuple[str, dict[str, Any], str]:
    bundle = transport.resolve_evidence(evidence_sha256=digest, context=scope)
    if not isinstance(bundle, dict):
        raise ValueError("Trusted source bundle unavailable.")
    source = bundle.get("source_text")
    manifest = bundle.get("manifest")
    if (not isinstance(source, str) or not 1 <= len(source) <= 6000
            or hashlib.sha256(source.encode("utf-8")).hexdigest() != digest):
        raise ValueError("Trusted source bytes/digest mismatch.")
    required = {
        "repository", "pr_number", "head_sha", "base_sha",
        "canonicalizer", "source_boundary",
    }
    if not isinstance(manifest, dict) or set(manifest) != required:
        raise ValueError("Trusted evidence manifest incomplete.")
    if (not isinstance(manifest["repository"], str)
            or len(manifest["repository"]) > 128
            or not _REPO.fullmatch(manifest["repository"])
            or type(manifest["pr_number"]) is not int
            or manifest["pr_number"] < 1
            or not isinstance(manifest["head_sha"], str)
            or not _SHA40.fullmatch(manifest["head_sha"])
            or not isinstance(manifest["base_sha"], str)
            or not _SHA40.fullmatch(manifest["base_sha"])
            or manifest["canonicalizer"] != "utf8-v1"
            or manifest["source_boundary"] != scope.source_boundary):
        raise ValueError("Untrusted or mismatched review manifest.")
    hashed = hashlib.sha256(
        json.dumps(
            {**manifest, "evidence_sha256": digest},
            sort_keys=True, separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return source, manifest, hashed


def _prepare(
    transport: ReviewerTransport, context: Any, payload: dict[str, Any],
) -> tuple[ReviewerScope, dict[str, Any], str, str, str, str]:
    lane, digest = _payload(payload)
    scope = _scope(context)
    source, manifest, manifest_sha = _source(transport, digest, scope)
    fingerprint = json.dumps(
        {"scope": asdict(scope), "lane": lane, "digest": digest,
         "manifest_sha256": manifest_sha, "contract": _CONTRACT_VERSION},
        sort_keys=True, separators=(",", ":"),
    )
    key = "aigentbee-review-" + hashlib.sha256(fingerprint.encode()).hexdigest()
    prompt = (
        "Read-only isolated ChatGPT review. No tools, account changes or retries. "
        + PROMPTS[lane] + " Review this exact trusted source. Repository: "
        + manifest["repository"] + ", PR #" + str(manifest["pr_number"])
        + ", base " + manifest["base_sha"] + ", head " + manifest["head_sha"]
        + ". Source: " + source
    )
    return scope, manifest, manifest_sha, key, prompt, digest


def _verified_result(
    transport: ReviewerTransport, scope: ReviewerScope,
    manifest: dict[str, Any], manifest_sha: str, key: str, prompt: str,
    digest: str, job: str, operation_id: str, lane: str,
) -> dict[str, Any]:
    if not isinstance(job, str) or not _NATIVE.fullmatch(job):
        raise ReviewerOutcomeUnknown(
            "missing_durable_job_id", idempotency_key=key,
            operation_id=operation_id,
        )
    try:
        readback = transport.retrieve(job_id=job, context=scope)
    except Exception as exc:
        raise ReviewerOutcomeUnknown(
            "retrieval_uncertain", job_id=job, idempotency_key=key,
            operation_id=operation_id,
        ) from exc
    if not isinstance(readback, dict) or readback.get("status") != "completed":
        raise ReviewerOutcomeUnknown(
            "pending_or_failed_readback", job_id=job, idempotency_key=key,
            operation_id=operation_id,
        )
    prompt_sha = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    expected = {
        "id": job, "operation_id": operation_id, "idempotency_key": key,
        "provider": "chatgpt", "profile_name": scope.profile_name,
        "source_manifest_sha256": manifest_sha, "prompt_sha256": prompt_sha,
        "binding": asdict(scope),
    }
    if any(readback.get(name) != value for name, value in expected.items()):
        raise ReviewerOutcomeUnknown(
            "provider_or_owner_scope_mismatch", job_id=job,
            idempotency_key=key, operation_id=operation_id,
        )
    native = readback.get("verified_thread_id")
    answer = readback.get("review_text")
    if (not isinstance(native, str) or not _NATIVE.fullmatch(native)
            or not isinstance(answer, str) or not 1 <= len(answer) <= 3500):
        raise ReviewerOutcomeUnknown(
            "native_turn_unverified", job_id=job, idempotency_key=key,
            operation_id=operation_id,
        )
    response_sha = hashlib.sha256(answer.encode("utf-8")).hexdigest()
    if readback.get("response_sha256") != response_sha:
        raise ReviewerOutcomeUnknown(
            "response_hash_mismatch", job_id=job, idempotency_key=key,
            operation_id=operation_id,
        )
    return {
        "lane": lane, "job_id": job, "operation_id": operation_id,
        "idempotency_key": key, "provider": "chatgpt",
        "native_conversation_id": native, "review_text": answer,
        "response_sha256": response_sha, "evidence_sha256": digest,
        "source_manifest_sha256": manifest_sha, "source_manifest": manifest,
        "prompt_sha256": prompt_sha, "adapter": _ADAPTER_VERSION,
        "prompt_contract": _CONTRACT_VERSION, "provenance": asdict(scope),
        "performed_external_effect": True, "submission_attempts": 1,
        "reconciled_without_replay": False,
    }


def run_chatgpt_review(
    *, context: Any, payload: dict[str, Any],
    transport: ReviewerTransport | None, cancelled: Callable[[], bool],
) -> dict[str, Any]:
    """Reserve durably, submit once, verify scoped native response."""
    if transport is None:
        raise ValueError("Qualified owner-bound ChatGPT transport is not configured.")
    scope, manifest, manifest_sha, key, prompt, digest = _prepare(
        transport, context, payload
    )
    if cancelled():
        raise ReviewerCancelled("Cancelled before any ChatGPT submission.")
    prompt_sha = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    try:
        receipt = transport.reserve(
            idempotency_key=key, prompt_sha256=prompt_sha,
            manifest_sha256=manifest_sha, context=scope,
        )
    except Exception as exc:
        raise ReviewerOutcomeUnknown(
            "durable_reservation_unavailable", idempotency_key=key,
        ) from exc
    if (not isinstance(receipt, dict)
            or receipt.get("idempotency_key") != key
            or receipt.get("prompt_sha256") != prompt_sha
            or receipt.get("manifest_sha256") != manifest_sha
            or receipt.get("durable") is not True
            or not isinstance(receipt.get("operation_id"), str)
            or not _NATIVE.fullmatch(receipt["operation_id"])):
        raise ReviewerOutcomeUnknown(
            "durable_reservation_unverified", idempotency_key=key,
        )
    operation_id = receipt["operation_id"]
    if cancelled():
        raise ReviewerCancelled("Cancelled after durable reservation, before send.")
    try:
        queued = transport.submit(
            idempotency_key=key, operation_id=operation_id,
            prompt=prompt, context=scope,
        )
    except Exception as exc:
        raise ReviewerOutcomeUnknown(
            "submission_uncertain", idempotency_key=key,
            operation_id=operation_id,
        ) from exc
    job = queued.get("id") if isinstance(queued, dict) else None
    if (not isinstance(queued, dict)
            or queued.get("operation_id") != operation_id
            or queued.get("idempotency_key") != key):
        raise ReviewerOutcomeUnknown(
            "submission_receipt_mismatch", idempotency_key=key,
            operation_id=operation_id,
        )
    if cancelled():
        raise ReviewerOutcomeUnknown(
            "cancelled_after_submission", idempotency_key=key,
            operation_id=operation_id, job_id=job if isinstance(job, str) else "",
        )
    return _verified_result(
        transport, scope, manifest, manifest_sha, key, prompt,
        digest, job, operation_id, payload["lane"],
    )


def reconcile_chatgpt_review(
    *, context: Any, payload: dict[str, Any], transport: ReviewerTransport | None,
) -> dict[str, Any]:
    """Read-only discovery of the original operation, never submit or reserve."""
    if transport is None:
        raise ValueError("Qualified owner-bound ChatGPT transport is not configured.")
    scope, manifest, manifest_sha, key, prompt, digest = _prepare(
        transport, context, payload
    )
    try:
        located = transport.lookup_by_idempotency_key(
            idempotency_key=key, context=scope
        )
    except Exception as exc:
        raise ReviewerOutcomeUnknown(
            "lookup_uncertain", idempotency_key=key,
        ) from exc
    prompt_sha = hashlib.sha256(prompt.encode("utf-8")).hexdigest()
    if (not isinstance(located, dict)
            or located.get("idempotency_key") != key
            or located.get("prompt_sha256") != prompt_sha
            or located.get("manifest_sha256") != manifest_sha
            or not isinstance(located.get("operation_id"), str)
            or not _NATIVE.fullmatch(located["operation_id"])):
        raise ReviewerOutcomeUnknown(
            "lookup_provenance_unverified", idempotency_key=key,
        )
    result = _verified_result(
        transport, scope, manifest, manifest_sha, key, prompt,
        digest, located.get("id"), located["operation_id"], payload["lane"],
    )
    return {**result, "reconciled_without_replay": True, "submission_attempts": 0}
