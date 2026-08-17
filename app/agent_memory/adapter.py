"""Contract-first SC-AMF adapter with fail-closed local governance controls."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
import re
import time
import uuid
from typing import Any, Callable, Mapping

from app.diagnostics import emit_diagnostic_event

from .models import (
    AgentMemoryError,
    CONTRACT_ID,
    DerivedMemoryRecord,
    IdentityEnvelope,
    RetrievalBudget,
    normalize_strings,
    payload_hash,
    stable_hash,
)
from .store import AgentMemoryStore
from .upstream import UPSTREAM_CONTRACT, UpstreamMemoryClient


_SECRET_RE = re.compile(
    r"(?i)(?:api[_-]?key|authorization|bearer|refresh[_-]?token|private[_-]?key|sapisd|sid|cookie)\s*[:=]\s*\S+"
)
_NORMAL_RETRIEVAL_STATES = {"CORROBORATED", "RETRIEVAL_ELIGIBLE"}
LeaseValidator = Callable[[IdentityEnvelope, str], Mapping[str, Any] | bool]


class SanityCloudMemoryAdapter:
    """Governed memory plane.

    The adapter stores only derived/candidate state locally for the R1 pilot.
    A preconfigured upstream client may be injected for bounded read-only calls;
    worker requests never carry reusable upstream credentials.
    """

    def __init__(
        self,
        store: AgentMemoryStore,
        *,
        upstream: UpstreamMemoryClient | None = None,
        lease_validator: LeaseValidator | None = None,
    ) -> None:
        self.store = store
        self.upstream = upstream
        self.lease_validator = lease_validator

    def _diagnostic(self, code: str, message: str, identity: IdentityEnvelope, **details: Any) -> None:
        emit_diagnostic_event(
            code=code,
            message=message,
            operation="agent_memory",
            category="agent_memory",
            severity="warning" if code not in {"SECRET_PROHIBITED", "SCOPE_DENIED"} else "error",
            kind="governance",
            retryable=code in {"UPSTREAM_UNAVAILABLE", "OUTCOME_UNKNOWN"},
            project_id=identity.project_ref,
            lane_id=identity.lane_id,
            parent_record_id=identity.mission_id,
            details=details,
        )

    @staticmethod
    def _now() -> str:
        return datetime.now(timezone.utc).isoformat()

    def _validate_request_identity(
        self,
        identity: IdentityEnvelope,
        operation: str,
    ) -> dict[str, Any]:
        if not identity.lease_ref:
            raise AgentMemoryError("SCOPE_DENIED", "lease_ref is required")
        if self.lease_validator is None:
            raise AgentMemoryError(
                "LEASE_UNVERIFIED",
                "no authoritative Session Broker/Hive lease validator is configured",
            )
        try:
            decision = self.lease_validator(identity, operation)
        except AgentMemoryError:
            raise
        except Exception as exc:
            self._diagnostic(
                "LEASE_VALIDATION_FAILED",
                "lease validation failed",
                identity,
                operation_name=operation,
                error_type=type(exc).__name__,
            )
            raise AgentMemoryError("LEASE_UNVERIFIED", "lease validation failed") from exc
        if decision is False or decision is None:
            raise AgentMemoryError("SCOPE_DENIED", "lease validator denied the operation")
        if decision is True:
            return {"allowed": True}
        normalized = dict(decision)
        if not normalized.get("allowed", False):
            raise AgentMemoryError("SCOPE_DENIED", "lease validator denied the operation")
        return normalized

    def _begin_mutation(
        self,
        *,
        identity: IdentityEnvelope,
        request_id: str,
        idempotency_key: str,
        operation: str,
        payload: dict[str, Any],
    ) -> tuple[dict[str, Any], str]:
        lease_receipt = self._validate_request_identity(identity, operation)
        request_hash = stable_hash(
            {
                "identity": identity.receipt(),
                "operation": operation,
                "payload": payload,
                "contract_id": CONTRACT_ID,
            }
        )
        receipt = self.store.begin_operation(
            idempotency_key=idempotency_key,
            request_id=request_id,
            operation=operation,
            request_hash=request_hash,
            identity=identity.receipt(),
        )
        receipt["lease_validation"] = lease_receipt
        if not receipt.get("_new", False) and receipt.get("outcome") in {
            "RUNNING",
            "OUTCOME_UNKNOWN",
        }:
            raise AgentMemoryError(
                "OUTCOME_UNKNOWN",
                "an identical operation is already running or has an unreconciled outcome; semantic replay is blocked",
                retryable=False,
            )
        return receipt, request_hash

    def health(self, identity: IdentityEnvelope) -> dict[str, Any]:
        lease_receipt = self._validate_request_identity(identity, "memory.health")
        return {
            "ok": True,
            "contract_id": CONTRACT_ID,
            "derived_plane_only": True,
            "credential_handling": False,
            "upstream_read_client_configured": self.upstream is not None,
            "upstream": UPSTREAM_CONTRACT,
            "identity": identity.receipt(),
            "lease_validation": lease_receipt,
        }

    def write_candidate(
        self,
        *,
        identity: IdentityEnvelope,
        request_id: str,
        idempotency_key: str,
        payload: str,
        asset_type: str = "chat_memory",
        layer: str = "L1",
        source_refs: list[str] | None = None,
        source_hashes: list[str] | None = None,
        evidence_gaps: list[dict[str, Any]] | None = None,
        dissent_refs: list[str] | None = None,
        cancellation_ref: str = "",
    ) -> dict[str, Any]:
        body = {
            "payload_hash": payload_hash(payload),
            "asset_type": asset_type,
            "layer": layer,
            "source_refs": normalize_strings(source_refs or []),
            "source_hashes": normalize_strings(source_hashes or []),
        }
        if not body["source_refs"] or not body["source_hashes"]:
            raise AgentMemoryError(
                "PROVENANCE_REQUIRED",
                "candidate writes require non-empty source_refs and source_hashes",
            )
        prior, _ = self._begin_mutation(
            identity=identity,
            request_id=request_id,
            idempotency_key=idempotency_key,
            operation="memory.write_candidate",
            payload=body,
        )
        if not prior.get("_new", False):
            existing = self.store.operation_result(idempotency_key)
            return dict(existing["result"] if existing else {})
        if cancellation_ref and self.store.is_cancelled(cancellation_ref):
            result = {"status": "CANCELLED", "request_id": request_id}
            self.store.complete_operation(idempotency_key=idempotency_key, outcome="CANCELLED", result=result)
            return result

        if _SECRET_RE.search(str(payload)):
            record = DerivedMemoryRecord(
                derived_memory_id=str(uuid.uuid4()),
                asset_type=asset_type,
                layer=layer,
                identity=identity,
                payload="[REDACTED:SECRET_PROHIBITED]",
                payload_hash=body["payload_hash"],
                source_refs=body["source_refs"],
                source_hashes=body["source_hashes"],
                state="QUARANTINED",
                sensitivity="SECRET_PROHIBITED",
                evidence_class="UNVERIFIED",
                evidence_gaps=list(evidence_gaps or []),
                dissent_refs=normalize_strings(dissent_refs or []),
                admission_receipt_ref=request_id,
                captured_at=self._now(),
            )
            self.store.put_record(record)
            result = {
                "status": "QUARANTINED",
                "code": "SECRET_PROHIBITED",
                "derived_memory_id": record.derived_memory_id,
                "payload_hash": record.payload_hash,
                "payload_persisted": False,
            }
            self.store.complete_operation(idempotency_key=idempotency_key, outcome="QUARANTINED", result=result)
            self._diagnostic("SECRET_PROHIBITED", "candidate payload quarantined", identity, request_id=request_id)
            return result


        record = DerivedMemoryRecord(
            derived_memory_id=str(uuid.uuid4()),
            asset_type=asset_type,
            layer=layer,
            identity=identity,
            payload=str(payload),
            payload_hash=body["payload_hash"],
            source_refs=body["source_refs"],
            source_hashes=body["source_hashes"],
            state="CANDIDATE",
            evidence_class="DERIVED",
            evidence_gaps=list(evidence_gaps or []),
            dissent_refs=normalize_strings(dissent_refs or []),
            admission_receipt_ref=request_id,
            captured_at=self._now(),
        )
        self.store.put_record(record)
        result = {
            "status": "CANDIDATE",
            "derived_memory_id": record.derived_memory_id,
            "payload_hash": record.payload_hash,
            "canonical_adoption": False,
        }
        self.store.complete_operation(idempotency_key=idempotency_key, outcome="COMPLETED", result=result)
        return result

    def get(self, *, identity: IdentityEnvelope, derived_memory_id: str) -> dict[str, Any]:
        self._validate_request_identity(identity, "memory.get")
        record = self.store.get_record(derived_memory_id)
        if record is None:
            raise AgentMemoryError("NOT_FOUND", "derived memory record not found")
        identity.assert_same_scope(record.identity)
        return record.to_dict(include_payload=record.state != "QUARANTINED")

    def search(
        self,
        *,
        identity: IdentityEnvelope,
        query: str,
        budget: RetrievalBudget | None = None,
    ) -> dict[str, Any]:
        self._validate_request_identity(identity, "memory.search")
        budget = budget or RetrievalBudget()
        started = time.monotonic()
        needle = str(query or "").casefold()
        candidates = self.store.list_scope(identity)
        included: list[dict[str, Any]] = []
        excluded: list[dict[str, str]] = []
        chars = 0
        for record in candidates:
            if time.monotonic() - started > budget.timeout_seconds:
                raise AgentMemoryError("BUDGET_EXCEEDED", "memory search exceeded timeout budget")
            if record.state not in _NORMAL_RETRIEVAL_STATES:
                excluded.append({"id": record.derived_memory_id, "reason": f"state:{record.state}"})
                continue
            if record.evidence_class == "CONFLICTED":
                excluded.append({"id": record.derived_memory_id, "reason": "evidence_class:CONFLICTED"})
                continue
            if needle and needle not in record.payload.casefold():
                continue
            next_chars = chars + len(record.payload)
            if len(included) >= budget.max_assets or next_chars > budget.max_chars:
                excluded.append({"id": record.derived_memory_id, "reason": "budget"})
                continue
            chars = next_chars
            included.append(record.to_dict(include_payload=True))
        return {
            "items": included,
            "selection_manifest": {
                "included_ids": [item["derived_memory_id"] for item in included],
                "excluded": excluded,
                "chars": chars,
                "budget": {
                    "max_assets": budget.max_assets,
                    "max_chars": budget.max_chars,
                    "timeout_seconds": budget.timeout_seconds,
                    "max_graph_hops": budget.max_graph_hops,
                },
            },
        }

    def compile_context(
        self,
        *,
        identity: IdentityEnvelope,
        query: str = "",
        budget: RetrievalBudget | None = None,
    ) -> dict[str, Any]:
        result = self.search(identity=identity, query=query, budget=budget)
        context = "\n\n".join(item["payload"] for item in result["items"])
        notices = []
        for item in result["items"]:
            if item.get("evidence_gaps"):
                notices.append({"id": item["derived_memory_id"], "evidence_gaps": item["evidence_gaps"]})
            if item.get("dissent_refs"):
                notices.append({"id": item["derived_memory_id"], "dissent_refs": item["dissent_refs"]})
        return {
            "context": context,
            "selection_manifest": result["selection_manifest"],
            "notices": notices,
            "canonical": False,
        }

    def mark_retrieval_eligible_for_test(
        self,
        *,
        identity: IdentityEnvelope,
        derived_memory_id: str,
        reviewer_receipt: str,
    ) -> dict[str, Any]:
        """Test-only gate helper; deliberately not exposed as worker MCP/API surface."""
        self._validate_request_identity(identity, "memory.test_mark_retrieval_eligible")
        if not reviewer_receipt:
            raise AgentMemoryError("REVIEW_REQUIRED", "reviewer/fan-in receipt is required")
        record = self.store.get_record(derived_memory_id)
        if record is None:
            raise AgentMemoryError("NOT_FOUND", "derived memory record not found")
        identity.assert_same_scope(record.identity)
        if record.state in {"QUARANTINED", "SUPERSEDED", "RETIRED"}:
            raise AgentMemoryError("INVALID_STATE", f"cannot admit state {record.state}")
        updated = replace(record, state="RETRIEVAL_ELIGIBLE")
        self.store.update_record(updated)
        return {"derived_memory_id": derived_memory_id, "state": updated.state, "reviewer_receipt": reviewer_receipt}

    def supersede(
        self,
        *,
        identity: IdentityEnvelope,
        request_id: str,
        idempotency_key: str,
        old_id: str,
        successor_id: str,
        rationale: str,
        reviewer_receipt: str,
    ) -> dict[str, Any]:
        if not rationale or not reviewer_receipt:
            raise AgentMemoryError("REVIEW_REQUIRED", "supersession requires rationale and reviewer receipt")
        body = {
            "old_id": old_id,
            "successor_id": successor_id,
            "rationale": rationale,
            "reviewer_receipt": reviewer_receipt,
        }
        prior, _ = self._begin_mutation(
            identity=identity,
            request_id=request_id,
            idempotency_key=idempotency_key,
            operation="memory.supersede",
            payload=body,
        )
        if not prior.get("_new", False):
            existing = self.store.operation_result(idempotency_key)
            return dict(existing["result"] if existing else {})
        old = self.store.get_record(old_id)
        successor = self.store.get_record(successor_id)
        if old is None or successor is None:
            raise AgentMemoryError("NOT_FOUND", "supersession record not found")
        identity.assert_same_scope(old.identity)
        identity.assert_same_scope(successor.identity)
        old = replace(old, state="SUPERSEDED", superseded_by=successor_id)
        successor = replace(successor, supersedes=normalize_strings([*successor.supersedes, old_id]))
        self.store.update_record(old)
        self.store.update_record(successor)
        result = {"status": "COMPLETED", "old_id": old_id, "successor_id": successor_id}
        self.store.complete_operation(idempotency_key=idempotency_key, outcome="COMPLETED", result=result)
        return result

    def record_contradiction(
        self,
        *,
        identity: IdentityEnvelope,
        first_id: str,
        second_id: str,
        evidence_gap: dict[str, Any],
        dissent_ref: str = "",
    ) -> dict[str, Any]:
        """Preserve opposing records without selecting a winner."""
        self._validate_request_identity(identity, "memory.record_contradiction")
        first = self.store.get_record(first_id)
        second = self.store.get_record(second_id)
        if first is None or second is None:
            raise AgentMemoryError("NOT_FOUND", "contradiction record not found")
        identity.assert_same_scope(first.identity)
        identity.assert_same_scope(second.identity)
        gap = dict(evidence_gap or {})
        if not gap.get("statement"):
            raise AgentMemoryError("EVIDENCE_GAP_REQUIRED", "contradiction requires an evidence-gap statement")
        first = replace(
            first,
            evidence_class="CONFLICTED",
            contradicts=normalize_strings([*first.contradicts, second_id]),
            evidence_gaps=[*first.evidence_gaps, gap],
            dissent_refs=normalize_strings([*first.dissent_refs, dissent_ref]),
        )
        second = replace(
            second,
            evidence_class="CONFLICTED",
            contradicts=normalize_strings([*second.contradicts, first_id]),
            evidence_gaps=[*second.evidence_gaps, gap],
            dissent_refs=normalize_strings([*second.dissent_refs, dissent_ref]),
        )
        self.store.update_record(first)
        self.store.update_record(second)
        return {
            "status": "CONFLICTED",
            "record_ids": [first_id, second_id],
            "winner_selected": False,
        }

    def audit_evidence(self, *, identity: IdentityEnvelope) -> dict[str, Any]:
        self._validate_request_identity(identity, "memory.audit_evidence")
        records = self.store.list_scope(identity)
        return {
            "records": len(records),
            "gaps": [
                {"id": record.derived_memory_id, "gaps": record.evidence_gaps}
                for record in records
                if record.evidence_gaps
            ],
            "dissent": [
                {"id": record.derived_memory_id, "dissent_refs": record.dissent_refs}
                for record in records
                if record.dissent_refs
            ],
            "quarantined_ids": [r.derived_memory_id for r in records if r.state == "QUARANTINED"],
            "superseded_ids": [r.derived_memory_id for r in records if r.state == "SUPERSEDED"],
            "canonical": False,
        }

    def cancel(
        self,
        *,
        identity: IdentityEnvelope,
        cancellation_ref: str,
        reason: str = "cancelled",
    ) -> dict[str, Any]:
        self._validate_request_identity(identity, "memory.cancel")
        if not cancellation_ref:
            raise AgentMemoryError("INVALID_CANCEL", "cancellation_ref is required")
        self.store.cancel(cancellation_ref, reason)
        return {"status": "CANCELLED", "cancellation_ref": cancellation_ref}

    def mark_outcome_unknown(
        self,
        *,
        identity: IdentityEnvelope,
        idempotency_key: str,
        reason: str,
        upstream_locator: str = "",
    ) -> dict[str, Any]:
        """Freeze a possibly accepted operation until an authorized reconciler resolves it."""
        self._validate_request_identity(identity, "memory.reconcile")
        receipt = self.store.operation_result(idempotency_key)
        if receipt is None:
            raise AgentMemoryError("NOT_FOUND", "operation receipt not found")
        if receipt.get("identity") != identity.receipt():
            raise AgentMemoryError("SCOPE_DENIED", "operation receipt identity does not match reconciler scope")
        result = {
            "status": "OUTCOME_UNKNOWN",
            "reason": str(reason or "provider outcome could not be established"),
            "semantic_replay_blocked": True,
        }
        self.store.complete_operation(
            idempotency_key=idempotency_key,
            outcome="OUTCOME_UNKNOWN",
            result=result,
            upstream_locator=upstream_locator,
        )
        self._diagnostic(
            "OUTCOME_UNKNOWN",
            "operation outcome requires reconciliation",
            identity,
            idempotency_key_hash=stable_hash(idempotency_key),
            upstream_locator=upstream_locator,
        )
        return result

    def reconcile_outcome(
        self,
        *,
        identity: IdentityEnvelope,
        idempotency_key: str,
        outcome: str,
        evidence: dict[str, Any],
    ) -> dict[str, Any]:
        """Resolve OUTCOME_UNKNOWN without replaying the semantic operation."""
        self._validate_request_identity(identity, "memory.reconcile")
        receipt = self.store.operation_result(idempotency_key)
        if receipt is None:
            raise AgentMemoryError("NOT_FOUND", "operation receipt not found")
        if receipt.get("identity") != identity.receipt():
            raise AgentMemoryError("SCOPE_DENIED", "operation receipt identity does not match reconciler scope")
        if receipt.get("outcome") != "OUTCOME_UNKNOWN":
            raise AgentMemoryError("INVALID_STATE", "only OUTCOME_UNKNOWN receipts can be reconciled")
        normalized = str(outcome or "").upper()
        if normalized not in {"COMPLETED", "FAILED", "CANCELLED", "QUARANTINED"}:
            raise AgentMemoryError("INVALID_STATE", "unsupported reconciliation outcome")
        result = {
            "status": normalized,
            "reconciled": True,
            "semantic_replay_performed": False,
            "evidence": dict(evidence or {}),
        }
        self.store.complete_operation(idempotency_key=idempotency_key, outcome=normalized, result=result)
        return result

    def query_upstream_atomic(
        self,
        *,
        identity: IdentityEnvelope,
        query: str,
        limit: int = 5,
    ) -> dict[str, Any]:
        self._validate_request_identity(identity, "memory.query_upstream_atomic")
        if self.upstream is None:
            raise AgentMemoryError("UPSTREAM_UNAVAILABLE", "no governed upstream client is configured", retryable=True)
        if limit < 1 or limit > 12:
            raise AgentMemoryError("BUDGET_DENIED", "upstream limit must be between 1 and 12")
        try:
            return self.upstream.search_atomic(query, limit=limit)
        except Exception as exc:
            self._diagnostic("UPSTREAM_UNAVAILABLE", "upstream atomic search failed", identity, error_type=type(exc).__name__)
            raise AgentMemoryError("UPSTREAM_UNAVAILABLE", "upstream atomic search failed", retryable=True) from exc

    def query_wiki(self, *, identity: IdentityEnvelope, path: str) -> dict[str, Any]:
        self._validate_request_identity(identity, "memory.query_wiki")
        if self.upstream is None:
            raise AgentMemoryError("UPSTREAM_UNAVAILABLE", "no governed upstream client is configured", retryable=True)
        return self.upstream.read_scenario(path)

    def query_core_reference(self, *, identity: IdentityEnvelope) -> dict[str, Any]:
        """Read L3 as non-authoritative reference; cross-principal sharing remains denied by scope."""
        self._validate_request_identity(identity, "memory.query_core_reference")
        if self.upstream is None:
            raise AgentMemoryError("UPSTREAM_UNAVAILABLE", "no governed upstream client is configured", retryable=True)
        result = dict(self.upstream.read_core())
        result["canonical"] = False
        result["cross_principal_allowed"] = False
        return result
