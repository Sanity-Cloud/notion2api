from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import stat
import threading
import time
import uuid
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Iterator

from pydantic import BaseModel, Field

from app.hive_runtime import (
    HiveIdempotencyConflict,
    HiveNotFoundError,
    HiveTransitionError,
    default_hive_runtime_db_path,
)
from app.hive_workforce import (
    AUTHORITY_RANK,
    WorkerClass,
    WorkerStage,
    get_hive_workforce_store,
)

EXTERNAL_IMPLEMENTATION_ID = "builtin.sandbox_artifact.v1"
ALLOWED_EFFECT_EXTENSIONS = {".json", ".md", ".txt"}
MAX_COMPILED_EFFECT_BYTES = 65536


class CertificationStatus(str, Enum):
    CERTIFIED = "CERTIFIED"
    SUSPENDED = "SUSPENDED"
    REVOKED = "REVOKED"


class EffectStatus(str, Enum):
    PLANNED = "PLANNED"
    APPLIED = "APPLIED"
    ROLLED_BACK = "ROLLED_BACK"
    COMPENSATION_FAILED = "COMPENSATION_FAILED"
    TAMPERED = "TAMPERED"


class ExternalAdapterCertification(BaseModel):
    certification_id: str
    adapter_id: str
    implementation_id: str
    status: str
    sandbox_name: str
    sandbox_root: str
    allowed_extensions: list[str] = Field(default_factory=list)
    max_effect_bytes: int
    threat_model: dict[str, Any] = Field(default_factory=dict)
    credential_boundary: str
    rollback_contract: dict[str, Any] = Field(default_factory=dict)
    reviewer_worker_id: str
    certified_by: str
    contract_sha256: str
    created_at: int
    updated_at: int
    revision: int


class ExternalEffectReceipt(BaseModel):
    effect_id: str
    certification_id: str
    execution_id: str
    operation: str
    dry_run: bool
    relative_name: str
    target_path: str
    status: str
    before_exists: bool
    before_sha256: str
    after_exists: bool
    after_sha256: str
    request_sha256: str
    rollback_token: str = ""
    rollback_reason: str = ""
    actor: str
    created_at: int
    updated_at: int
    revision: int


class ExternalEffectSnapshot(BaseModel):
    ok: bool = True
    found: bool = True
    db_path: str = ""
    effect_root: str = ""
    error: str = ""
    count: int = 0
    certifications: list[ExternalAdapterCertification] = Field(default_factory=list)
    effects: list[ExternalEffectReceipt] = Field(default_factory=list)


def default_external_effect_root(db_path: str | Path | None = None) -> Path:
    configured = str(os.getenv("SANITYCLOUD_EXTERNAL_EFFECT_ROOT") or "").strip()
    if configured:
        return Path(configured).expanduser().resolve()
    database = Path(db_path or default_hive_runtime_db_path()).expanduser().resolve()
    return (database.parent / "external-effects").resolve()


class ExternalEffectCertificationStore:
    """Durable certification and reversible sandbox-effect ledger."""

    def __init__(self, path: str | Path, effect_root: str | Path | None = None):
        self.path = Path(path).expanduser().resolve()
        self.effect_root = (
            Path(effect_root or default_external_effect_root(self.path))
            .expanduser()
            .resolve()
        )
        self._schema_lock = threading.RLock()
        self.workforce = get_hive_workforce_store(self.path)
        self.effect_root.mkdir(parents=True, exist_ok=True)
        if self._is_reparse(self.effect_root):
            raise HiveTransitionError(
                "External-effect root cannot be a symlink or reparse point."
            )
        self._ensure_schema()

    @staticmethod
    def _now_ms() -> int:
        return int(time.time() * 1000)

    @staticmethod
    def _json(value: Any) -> str:
        return json.dumps(
            value, ensure_ascii=False, separators=(",", ":"), sort_keys=True
        )

    @classmethod
    def _fingerprint(cls, value: Any) -> str:
        return hashlib.sha256(cls._json(value).encode("utf-8")).hexdigest()

    @staticmethod
    def _sha256(data: bytes) -> str:
        return hashlib.sha256(data).hexdigest()

    @staticmethod
    def _required(value: str, field_name: str) -> str:
        clean = str(value or "").strip()
        if not clean:
            raise ValueError(f"{field_name} is required")
        return clean

    @staticmethod
    def _is_reparse(path: Path) -> bool:
        if path.is_symlink():
            return True
        try:
            attrs = getattr(path.stat(), "st_file_attributes", 0)
        except FileNotFoundError:
            return False
        return bool(attrs & getattr(stat, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400))

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.path), timeout=10, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout = 5000")
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    @contextmanager
    def _write(self) -> Iterator[sqlite3.Connection]:
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.commit()
        except Exception:
            if conn.in_transaction:
                conn.rollback()
            raise
        finally:
            conn.close()

    def _ensure_schema(self) -> None:
        with self._schema_lock:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with self._connect() as conn:
                conn.executescript(
                    """
                    CREATE TABLE IF NOT EXISTS hive_external_adapter_certifications (
                        certification_id TEXT PRIMARY KEY,
                        adapter_id TEXT NOT NULL,
                        implementation_id TEXT NOT NULL,
                        status TEXT NOT NULL,
                        sandbox_name TEXT NOT NULL,
                        sandbox_root TEXT NOT NULL,
                        allowed_extensions_json TEXT NOT NULL DEFAULT '[]',
                        max_effect_bytes INTEGER NOT NULL,
                        threat_model_json TEXT NOT NULL DEFAULT '{}',
                        credential_boundary TEXT NOT NULL,
                        rollback_contract_json TEXT NOT NULL DEFAULT '{}',
                        reviewer_worker_id TEXT NOT NULL,
                        certified_by TEXT NOT NULL,
                        contract_sha256 TEXT NOT NULL,
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_external_certification_events (
                        event_id TEXT PRIMARY KEY,
                        certification_id TEXT NOT NULL REFERENCES
                            hive_external_adapter_certifications(certification_id)
                            ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_external_effect_receipts (
                        effect_id TEXT PRIMARY KEY,
                        certification_id TEXT NOT NULL REFERENCES
                            hive_external_adapter_certifications(certification_id),
                        execution_id TEXT NOT NULL DEFAULT '',
                        operation TEXT NOT NULL,
                        dry_run INTEGER NOT NULL DEFAULT 0,
                        relative_name TEXT NOT NULL,
                        target_path TEXT NOT NULL,
                        status TEXT NOT NULL,
                        before_exists INTEGER NOT NULL DEFAULT 0,
                        before_sha256 TEXT NOT NULL DEFAULT '',
                        before_content_b64 TEXT NOT NULL DEFAULT '',
                        after_exists INTEGER NOT NULL DEFAULT 0,
                        after_sha256 TEXT NOT NULL DEFAULT '',
                        request_sha256 TEXT NOT NULL,
                        rollback_token_sha256 TEXT NOT NULL DEFAULT '',
                        rollback_reason TEXT NOT NULL DEFAULT '',
                        actor TEXT NOT NULL,
                        idempotency_key TEXT UNIQUE,
                        created_at INTEGER NOT NULL,
                        updated_at INTEGER NOT NULL,
                        revision INTEGER NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS hive_external_effect_events (
                        event_id TEXT PRIMARY KEY,
                        effect_id TEXT NOT NULL REFERENCES hive_external_effect_receipts(effect_id)
                            ON DELETE CASCADE,
                        event_type TEXT NOT NULL,
                        actor TEXT NOT NULL,
                        payload_json TEXT NOT NULL DEFAULT '{}',
                        idempotency_key TEXT UNIQUE,
                        request_sha256 TEXT NOT NULL,
                        created_at INTEGER NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_external_cert_status_updated
                        ON hive_external_adapter_certifications(status, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_external_effect_execution
                        ON hive_external_effect_receipts(execution_id, updated_at DESC);
                    CREATE INDEX IF NOT EXISTS idx_external_effect_status_updated
                        ON hive_external_effect_receipts(status, updated_at DESC);
                    """
                )

    def _reviewer(self, worker_id: str):
        key = self._required(worker_id, "reviewer_worker_id")
        workers = self.workforce.list_workers(
            stage=WorkerStage.APPOINTED.value, limit=1000
        ).workers
        reviewer = next((item for item in workers if item.worker_id == key), None)
        if not reviewer:
            raise HiveTransitionError(
                "Certification reviewer must be an appointed worker."
            )
        if reviewer.worker_class != WorkerClass.GOVERNANCE_REVIEWER.value:
            raise HiveTransitionError(
                "Certification reviewer must be a GOVERNANCE_REVIEWER."
            )
        if AUTHORITY_RANK.get(reviewer.authority_ceiling, 0) < AUTHORITY_RANK["A2"]:
            raise HiveTransitionError(
                "Certification reviewer requires authority A2 or higher."
            )
        return reviewer

    @staticmethod
    def _sandbox_name(value: str) -> str:
        clean = str(value or "").strip()
        if not clean or len(clean) > 80:
            raise HiveTransitionError("sandbox_name must contain 1 to 80 characters.")
        allowed = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_"
        if clean in {".", ".."} or any(ch not in allowed for ch in clean):
            raise HiveTransitionError(
                "sandbox_name may contain only letters, numbers, hyphens, and underscores."
            )
        return clean

    @staticmethod
    def _relative_name(value: str, extensions: set[str]) -> str:
        clean = str(value or "").strip()
        if not clean or len(clean) > 120:
            raise HiveTransitionError("relative_name must contain 1 to 120 characters.")
        if clean != Path(clean).name or "/" in clean or "\\" in clean or ":" in clean:
            raise HiveTransitionError(
                "External effects are limited to one sandbox filename."
            )
        if clean in {".", ".."} or clean.startswith("."):
            raise HiveTransitionError(
                "Hidden, dot, and traversal filenames are prohibited."
            )
        suffix = Path(clean).suffix.lower()
        if suffix not in extensions:
            raise HiveTransitionError(
                f"File extension is not certified: {suffix or '<none>'}"
            )
        return clean

    def _root(self, sandbox_name: str) -> Path:
        root = (self.effect_root / sandbox_name).resolve()
        if os.path.commonpath([str(self.effect_root), str(root)]) != str(
            self.effect_root
        ):
            raise HiveTransitionError(
                "Sandbox root escapes the configured external-effect root."
            )
        root.mkdir(parents=True, exist_ok=True)
        if self._is_reparse(root):
            raise HiveTransitionError(
                "Certified sandbox cannot be a symlink or reparse point."
            )
        return root

    @staticmethod
    def _cert(row: sqlite3.Row) -> ExternalAdapterCertification:
        return ExternalAdapterCertification(
            certification_id=str(row["certification_id"]),
            adapter_id=str(row["adapter_id"]),
            implementation_id=str(row["implementation_id"]),
            status=str(row["status"]),
            sandbox_name=str(row["sandbox_name"]),
            sandbox_root=str(row["sandbox_root"]),
            allowed_extensions=json.loads(str(row["allowed_extensions_json"])),
            max_effect_bytes=int(row["max_effect_bytes"]),
            threat_model=json.loads(str(row["threat_model_json"])),
            credential_boundary=str(row["credential_boundary"]),
            rollback_contract=json.loads(str(row["rollback_contract_json"])),
            reviewer_worker_id=str(row["reviewer_worker_id"]),
            certified_by=str(row["certified_by"]),
            contract_sha256=str(row["contract_sha256"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    @staticmethod
    def _effect(row: sqlite3.Row, token: str = "") -> ExternalEffectReceipt:
        return ExternalEffectReceipt(
            effect_id=str(row["effect_id"]),
            certification_id=str(row["certification_id"]),
            execution_id=str(row["execution_id"]),
            operation=str(row["operation"]),
            dry_run=bool(row["dry_run"]),
            relative_name=str(row["relative_name"]),
            target_path=str(row["target_path"]),
            status=str(row["status"]),
            before_exists=bool(row["before_exists"]),
            before_sha256=str(row["before_sha256"]),
            after_exists=bool(row["after_exists"]),
            after_sha256=str(row["after_sha256"]),
            request_sha256=str(row["request_sha256"]),
            rollback_token=token,
            rollback_reason=str(row["rollback_reason"]),
            actor=str(row["actor"]),
            created_at=int(row["created_at"]),
            updated_at=int(row["updated_at"]),
            revision=int(row["revision"]),
        )

    @staticmethod
    def _contract_payload(
        certification: ExternalAdapterCertification | dict[str, Any],
    ) -> dict[str, Any]:
        if isinstance(certification, dict):
            return {
                "adapter_id": certification["adapter_id"],
                "implementation_id": certification["implementation_id"],
                "sandbox_name": certification["sandbox_name"],
                "sandbox_root": certification["sandbox_root"],
                "allowed_extensions": certification["allowed_extensions"],
                "max_effect_bytes": certification["max_effect_bytes"],
                "threat_model": certification["threat_model"],
                "credential_boundary": certification["credential_boundary"],
                "rollback_contract": certification["rollback_contract"],
                "reviewer_worker_id": certification["reviewer_worker_id"],
            }
        return {
            "adapter_id": certification.adapter_id,
            "implementation_id": certification.implementation_id,
            "sandbox_name": certification.sandbox_name,
            "sandbox_root": certification.sandbox_root,
            "allowed_extensions": certification.allowed_extensions,
            "max_effect_bytes": certification.max_effect_bytes,
            "threat_model": certification.threat_model,
            "credential_boundary": certification.credential_boundary,
            "rollback_contract": certification.rollback_contract,
            "reviewer_worker_id": certification.reviewer_worker_id,
        }

    def _verify_contract(self, certification: ExternalAdapterCertification) -> None:
        if (
            self._fingerprint(self._contract_payload(certification))
            != certification.contract_sha256
        ):
            raise HiveTransitionError(
                "External-effect certification contract failed tamper verification."
            )

    def certify_adapter(
        self,
        *,
        adapter_id: str,
        implementation_id: str,
        sandbox_name: str,
        allowed_extensions: list[str] | None,
        max_effect_bytes: int,
        threat_model: dict[str, Any],
        credential_boundary: str,
        rollback_contract: dict[str, Any],
        reviewer_worker_id: str,
        actor: str,
        human_approval: bool = False,
        certification_id: str | None = None,
        idempotency_key: str | None = None,
    ) -> ExternalEffectSnapshot:
        if not human_approval:
            raise HiveTransitionError(
                "Human approval is required to certify an external-effect adapter."
            )
        adapter_key = self._required(adapter_id, "adapter_id")
        implementation_key = self._required(implementation_id, "implementation_id")
        if (
            adapter_key != EXTERNAL_IMPLEMENTATION_ID
            or implementation_key != EXTERNAL_IMPLEMENTATION_ID
        ):
            raise HiveTransitionError(
                "Only the compiled sandbox artifact adapter can be certified in Phase 4."
            )
        reviewer = self._reviewer(reviewer_worker_id)
        actor_key = self._required(actor, "actor")
        if actor_key == reviewer.worker_id:
            raise HiveTransitionError(
                "Certification actor and independent reviewer must be distinct."
            )
        sandbox_key = self._sandbox_name(sandbox_name)
        root = self._root(sandbox_key)
        extensions = sorted(
            {
                str(item).strip().lower()
                for item in (allowed_extensions or [])
                if str(item).strip()
            }
        ) or [".json"]
        if any(item not in ALLOWED_EFFECT_EXTENSIONS for item in extensions):
            raise HiveTransitionError(
                "Certified extensions must be a subset of .json, .md, and .txt."
            )
        limit = int(max_effect_bytes or 0)
        if limit < 1 or limit > MAX_COMPILED_EFFECT_BYTES:
            raise HiveTransitionError(
                f"max_effect_bytes must be between 1 and {MAX_COMPILED_EFFECT_BYTES}."
            )
        required_threat = {
            "attack_surface",
            "abuse_cases",
            "mitigations",
            "residual_risk",
        }
        if not isinstance(threat_model, dict) or not required_threat.issubset(
            threat_model
        ):
            raise HiveTransitionError(
                "Threat model must define attack_surface, abuse_cases, mitigations, and residual_risk."
            )
        if str(credential_boundary or "").strip().lower() != "none":
            raise HiveTransitionError(
                "The Phase 4 sandbox adapter cannot receive or use credentials."
            )
        if (
            not isinstance(rollback_contract, dict)
            or rollback_contract.get("strategy") != "preimage_restore"
        ):
            raise HiveTransitionError("Rollback contract must use preimage_restore.")
        if int(rollback_contract.get("retention_seconds") or 0) < 60:
            raise HiveTransitionError("Rollback retention_seconds must be at least 60.")
        stable = hashlib.sha256(
            (idempotency_key or str(uuid.uuid4())).encode("utf-8")
        ).hexdigest()[:20]
        cert_key = self._required(
            certification_id or f"certification-{stable}", "certification_id"
        )
        contract = {
            "adapter_id": adapter_key,
            "implementation_id": implementation_key,
            "sandbox_name": sandbox_key,
            "sandbox_root": str(root),
            "allowed_extensions": extensions,
            "max_effect_bytes": limit,
            "threat_model": threat_model,
            "credential_boundary": "none",
            "rollback_contract": rollback_contract,
            "reviewer_worker_id": reviewer.worker_id,
        }
        request = {
            **contract,
            "certification_id": cert_key,
            "actor": actor_key,
            "human_approval": True,
        }
        fingerprint = self._fingerprint(request)
        contract_sha = self._fingerprint(contract)
        now = self._now_ms()
        with self._write() as conn:
            if idempotency_key:
                existing = conn.execute(
                    "SELECT certification_id, request_sha256 FROM hive_external_adapter_certifications WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if existing:
                    if str(existing["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Certification idempotency key was reused with different content."
                        )
                    return self.get_certifications(
                        certification_id=str(existing["certification_id"])
                    )
            if conn.execute(
                "SELECT 1 FROM hive_external_adapter_certifications WHERE certification_id = ?",
                (cert_key,),
            ).fetchone():
                raise HiveTransitionError(f"Certification already exists: {cert_key}")
            conn.execute(
                """
                INSERT INTO hive_external_adapter_certifications(
                    certification_id, adapter_id, implementation_id, status,
                    sandbox_name, sandbox_root, allowed_extensions_json,
                    max_effect_bytes, threat_model_json, credential_boundary,
                    rollback_contract_json, reviewer_worker_id, certified_by,
                    contract_sha256, idempotency_key, request_sha256,
                    created_at, updated_at, revision
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'none', ?, ?, ?, ?, ?, ?, ?, ?, 1)
                """,
                (
                    cert_key,
                    adapter_key,
                    implementation_key,
                    CertificationStatus.CERTIFIED.value,
                    sandbox_key,
                    str(root),
                    self._json(extensions),
                    limit,
                    self._json(threat_model),
                    self._json(rollback_contract),
                    reviewer.worker_id,
                    actor_key,
                    contract_sha,
                    idempotency_key,
                    fingerprint,
                    now,
                    now,
                ),
            )
            conn.execute(
                """
                INSERT INTO hive_external_certification_events(
                    event_id, certification_id, event_type, actor, payload_json,
                    idempotency_key, request_sha256, created_at
                ) VALUES (?, ?, 'CERTIFIED', ?, ?, ?, ?, ?)
                """,
                (
                    str(uuid.uuid4()),
                    cert_key,
                    actor_key,
                    self._json(
                        {
                            "reviewer_worker_id": reviewer.worker_id,
                            "contract_sha256": contract_sha,
                        }
                    ),
                    f"{idempotency_key}:event" if idempotency_key else None,
                    fingerprint,
                    now,
                ),
            )
        return self.get_certifications(certification_id=cert_key)

    def get_certifications(
        self,
        *,
        certification_id: str = "",
        status: str = "",
        limit: int = 100,
    ) -> ExternalEffectSnapshot:
        clauses: list[str] = []
        params: list[Any] = []
        if certification_id:
            clauses.append("certification_id = ?")
            params.append(str(certification_id).strip())
        if status:
            clauses.append("status = ?")
            params.append(CertificationStatus(str(status).strip().upper()).value)
        query = "SELECT * FROM hive_external_adapter_certifications"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(max(1, min(int(limit), 1000)))
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        records = [self._cert(row) for row in rows]
        return ExternalEffectSnapshot(
            db_path=str(self.path),
            effect_root=str(self.effect_root),
            found=bool(records),
            count=len(records),
            certifications=records,
        )

    def transition_certification(
        self,
        *,
        certification_id: str,
        target_status: str,
        actor: str,
        reason: str,
        human_approval: bool = False,
        expected_revision: int | None = None,
        idempotency_key: str | None = None,
    ) -> ExternalEffectSnapshot:
        if not human_approval:
            raise HiveTransitionError(
                "Human approval is required to change certification status."
            )
        target = CertificationStatus(str(target_status).strip().upper()).value
        cert_key = self._required(certification_id, "certification_id")
        request = {
            "certification_id": cert_key,
            "target_status": target,
            "actor": self._required(actor, "actor"),
            "reason": self._required(reason, "reason"),
            "human_approval": True,
        }
        fingerprint = self._fingerprint(request)
        with self._write() as conn:
            if idempotency_key:
                event = conn.execute(
                    "SELECT certification_id, request_sha256 FROM hive_external_certification_events WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
                if event:
                    if str(event["request_sha256"]) != fingerprint:
                        raise HiveIdempotencyConflict(
                            "Certification transition key was reused with different content."
                        )
                    return self.get_certifications(
                        certification_id=str(event["certification_id"])
                    )
            row = conn.execute(
                "SELECT * FROM hive_external_adapter_certifications WHERE certification_id = ?",
                (cert_key,),
            ).fetchone()
            if not row:
                raise HiveNotFoundError(f"Certification not found: {cert_key}")
            current = self._cert(row)
            if expected_revision is not None and current.revision != int(
                expected_revision
            ):
                raise HiveTransitionError(
                    f"Certification revision mismatch: expected {expected_revision}, current {current.revision}."
                )
            if (
                current.status == CertificationStatus.REVOKED.value
                and target != current.status
            ):
                raise HiveTransitionError("A revoked certification is terminal.")
            now = self._now_ms()
            conn.execute(
                "UPDATE hive_external_adapter_certifications SET status = ?, updated_at = ?, revision = revision + 1 WHERE certification_id = ?",
                (target, now, cert_key),
            )
            conn.execute(
                "INSERT INTO hive_external_certification_events(event_id, certification_id, event_type, actor, payload_json, idempotency_key, request_sha256, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    cert_key,
                    f"CERTIFICATION_{target}",
                    request["actor"],
                    self._json({"reason": request["reason"]}),
                    idempotency_key,
                    fingerprint,
                    now,
                ),
            )
        return self.get_certifications(certification_id=cert_key)

    def _active_certification(
        self, certification_id: str
    ) -> ExternalAdapterCertification:
        snapshot = self.get_certifications(certification_id=certification_id)
        if not snapshot.certifications:
            raise HiveNotFoundError(f"Certification not found: {certification_id}")
        certification = snapshot.certifications[0]
        self._verify_contract(certification)
        if certification.status != CertificationStatus.CERTIFIED.value:
            raise HiveTransitionError(
                f"Certification is not active: {certification.status}"
            )
        if certification.implementation_id != EXTERNAL_IMPLEMENTATION_ID:
            raise HiveTransitionError(
                "Certification implementation does not match the compiled adapter."
            )
        root = Path(certification.sandbox_root).resolve()
        expected = (self.effect_root / certification.sandbox_name).resolve()
        if root != expected or os.path.commonpath(
            [str(self.effect_root), str(root)]
        ) != str(self.effect_root):
            raise HiveTransitionError(
                "Certification sandbox root failed boundary verification."
            )
        if self._is_reparse(root):
            raise HiveTransitionError(
                "Certification sandbox became a symlink or reparse point."
            )
        return certification

    def get_effects(
        self,
        *,
        effect_id: str = "",
        execution_id: str = "",
        status: str = "",
        limit: int = 100,
    ) -> ExternalEffectSnapshot:
        clauses: list[str] = []
        params: list[Any] = []
        if effect_id:
            clauses.append("effect_id = ?")
            params.append(str(effect_id).strip())
        if execution_id:
            clauses.append("execution_id = ?")
            params.append(str(execution_id).strip())
        if status:
            clauses.append("status = ?")
            params.append(EffectStatus(str(status).strip().upper()).value)
        query = "SELECT * FROM hive_external_effect_receipts"
        if clauses:
            query += " WHERE " + " AND ".join(clauses)
        query += " ORDER BY updated_at DESC LIMIT ?"
        params.append(max(1, min(int(limit), 1000)))
        with self._connect() as conn:
            rows = conn.execute(query, tuple(params)).fetchall()
        records = [self._effect(row) for row in rows]
        return ExternalEffectSnapshot(
            db_path=str(self.path),
            effect_root=str(self.effect_root),
            found=bool(records),
            count=len(records),
            effects=records,
        )

    def execute_sandbox_artifact(
        self,
        *,
        execution_id: str,
        payload: dict[str, Any],
        actor: str,
        cancelled: Callable[[], bool],
    ) -> dict[str, Any]:
        allowed_keys = {
            "certification_id",
            "operation",
            "relative_name",
            "content",
            "dry_run",
            "dry_run_effect_id",
            "expected_preimage_sha256",
        }
        extra = sorted(set(payload) - allowed_keys)
        if extra:
            raise HiveTransitionError(
                f"Unsupported sandbox artifact payload keys: {', '.join(extra)}"
            )
        cert_id = self._required(
            str(payload.get("certification_id") or ""), "certification_id"
        )
        certification = self._active_certification(cert_id)
        operation = str(payload.get("operation") or "").strip().lower()
        if operation not in {"write", "delete"}:
            raise HiveTransitionError(
                "Sandbox artifact operation must be write or delete."
            )
        relative_name = self._relative_name(
            str(payload.get("relative_name") or ""),
            set(certification.allowed_extensions),
        )
        root = Path(certification.sandbox_root).resolve()
        target = (root / relative_name).resolve()
        if target.parent != root:
            raise HiveTransitionError("Sandbox target escaped the certified root.")
        if target.exists() and self._is_reparse(target):
            raise HiveTransitionError(
                "Sandbox target cannot be a symlink or reparse point."
            )
        before_exists = target.exists()
        before_data = target.read_bytes() if before_exists else b""
        if len(before_data) > certification.max_effect_bytes:
            raise HiveTransitionError(
                "Existing artifact exceeds the certified rollback boundary."
            )
        before_sha = self._sha256(before_data) if before_exists else ""
        expected_preimage = (
            str(payload.get("expected_preimage_sha256") or "").strip().lower()
        )
        if expected_preimage and expected_preimage != before_sha:
            raise HiveTransitionError(
                "Current preimage does not match expected_preimage_sha256."
            )
        if operation == "write":
            content = payload.get("content")
            if not isinstance(content, str):
                raise HiveTransitionError(
                    "Sandbox write content must be a UTF-8 string."
                )
            after_data = content.encode("utf-8")
            if len(after_data) > certification.max_effect_bytes:
                raise HiveTransitionError(
                    "Sandbox write exceeds the certified effect boundary."
                )
            after_exists = True
            after_sha = self._sha256(after_data)
        else:
            if payload.get("content") not in {None, ""}:
                raise HiveTransitionError("Sandbox delete does not accept content.")
            after_data = b""
            after_exists = False
            after_sha = ""
        request_contract = {
            "certification_id": certification.certification_id,
            "contract_sha256": certification.contract_sha256,
            "operation": operation,
            "relative_name": relative_name,
            "before_sha256": before_sha,
            "after_sha256": after_sha,
        }
        request_sha = self._fingerprint(request_contract)
        if cancelled():
            raise HiveTransitionError(
                "Execution cancellation was requested before the external effect."
            )
        dry_run = bool(payload.get("dry_run"))
        now = self._now_ms()
        if dry_run:
            effect_id = f"effect-plan-{uuid.uuid4()}"
            with self._write() as conn:
                conn.execute(
                    """
                    INSERT INTO hive_external_effect_receipts(
                        effect_id, certification_id, execution_id, operation,
                        dry_run, relative_name, target_path, status,
                        before_exists, before_sha256, before_content_b64,
                        after_exists, after_sha256, request_sha256,
                        rollback_token_sha256, rollback_reason, actor,
                        idempotency_key, created_at, updated_at, revision
                    ) VALUES (?, ?, ?, ?, 1, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', '', ?, ?, ?, ?, 1)
                    """,
                    (
                        effect_id,
                        certification.certification_id,
                        execution_id,
                        operation,
                        relative_name,
                        str(target),
                        EffectStatus.PLANNED.value,
                        int(before_exists),
                        before_sha,
                        base64.b64encode(before_data).decode("ascii"),
                        int(after_exists),
                        after_sha,
                        request_sha,
                        actor,
                        f"dry-run:{execution_id}",
                        now,
                        now,
                    ),
                )
                conn.execute(
                    "INSERT INTO hive_external_effect_events(event_id, effect_id, event_type, actor, payload_json, idempotency_key, request_sha256, created_at) VALUES (?, ?, 'EFFECT_PLANNED', ?, ?, ?, ?, ?)",
                    (
                        str(uuid.uuid4()),
                        effect_id,
                        actor,
                        self._json(request_contract),
                        f"dry-run:{execution_id}:event",
                        request_sha,
                        now,
                    ),
                )
            return {
                "adapter": EXTERNAL_IMPLEMENTATION_ID,
                "effect_id": effect_id,
                "dry_run": True,
                "operation": operation,
                "relative_name": relative_name,
                "before_sha256": before_sha,
                "after_sha256": after_sha,
                "certification_id": certification.certification_id,
                "contract_sha256": certification.contract_sha256,
                "performed_external_effect": False,
            }

        dry_run_effect_id = self._required(
            str(payload.get("dry_run_effect_id") or ""),
            "dry_run_effect_id",
        )
        with self._connect() as conn:
            plan_row = conn.execute(
                "SELECT * FROM hive_external_effect_receipts WHERE effect_id = ?",
                (dry_run_effect_id,),
            ).fetchone()
        if not plan_row:
            raise HiveNotFoundError(
                f"Dry-run effect receipt not found: {dry_run_effect_id}"
            )
        plan = self._effect(plan_row)
        if not plan.dry_run or plan.status != EffectStatus.PLANNED.value:
            raise HiveTransitionError(
                "Referenced effect receipt is not an active dry-run plan."
            )
        if (
            plan.certification_id != certification.certification_id
            or plan.request_sha256 != request_sha
        ):
            raise HiveTransitionError(
                "Live effect does not match the certified dry-run contract."
            )
        if cancelled():
            raise HiveTransitionError(
                "Execution cancellation was requested before the external effect."
            )
        effect_id = f"effect-{uuid.uuid4()}"
        rollback_token = uuid.uuid4().hex + uuid.uuid4().hex
        rollback_hash = self._sha256(rollback_token.encode("utf-8"))
        temp = root / f".phase4-{uuid.uuid4().hex}.tmp"
        try:
            if operation == "write":
                with temp.open("xb") as handle:
                    handle.write(after_data)
                    handle.flush()
                    os.fsync(handle.fileno())
                if cancelled():
                    raise HiveTransitionError(
                        "Execution cancellation was requested before commit."
                    )
                os.replace(temp, target)
            elif target.exists():
                target.unlink()
            actual_exists = target.exists()
            actual_data = target.read_bytes() if actual_exists else b""
            actual_sha = self._sha256(actual_data) if actual_exists else ""
            if actual_exists != after_exists or actual_sha != after_sha:
                raise HiveTransitionError("Post-effect verification failed.")
        except Exception:
            temp.unlink(missing_ok=True)
            if before_exists:
                target.write_bytes(before_data)
            else:
                target.unlink(missing_ok=True)
            raise
        now = self._now_ms()
        with self._write() as conn:
            conn.execute(
                """
                INSERT INTO hive_external_effect_receipts(
                    effect_id, certification_id, execution_id, operation,
                    dry_run, relative_name, target_path, status,
                    before_exists, before_sha256, before_content_b64,
                    after_exists, after_sha256, request_sha256,
                    rollback_token_sha256, rollback_reason, actor,
                    idempotency_key, created_at, updated_at, revision
                ) VALUES (?, ?, ?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', ?, ?, ?, ?, 1)
                """,
                (
                    effect_id,
                    certification.certification_id,
                    execution_id,
                    operation,
                    relative_name,
                    str(target),
                    EffectStatus.APPLIED.value,
                    int(before_exists),
                    before_sha,
                    base64.b64encode(before_data).decode("ascii"),
                    int(after_exists),
                    after_sha,
                    request_sha,
                    rollback_hash,
                    actor,
                    f"apply:{execution_id}",
                    now,
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO hive_external_effect_events(event_id, effect_id, event_type, actor, payload_json, idempotency_key, request_sha256, created_at) VALUES (?, ?, 'EFFECT_APPLIED', ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    effect_id,
                    actor,
                    self._json(
                        {"dry_run_effect_id": dry_run_effect_id, **request_contract}
                    ),
                    f"apply:{execution_id}:event",
                    request_sha,
                    now,
                ),
            )
        return {
            "adapter": EXTERNAL_IMPLEMENTATION_ID,
            "effect_id": effect_id,
            "dry_run": False,
            "operation": operation,
            "relative_name": relative_name,
            "before_sha256": before_sha,
            "after_sha256": after_sha,
            "rollback_token": rollback_token,
            "certification_id": certification.certification_id,
            "contract_sha256": certification.contract_sha256,
            "performed_external_effect": True,
        }

    def rollback_effect(
        self,
        *,
        effect_id: str,
        rollback_token: str,
        reviewer_worker_id: str,
        actor: str,
        reason: str,
        human_approval: bool = False,
        idempotency_key: str | None = None,
    ) -> ExternalEffectSnapshot:
        if not human_approval:
            raise HiveTransitionError(
                "Human approval is required to roll back an external effect."
            )
        reviewer = self._reviewer(reviewer_worker_id)
        actor_key = self._required(actor, "actor")
        if reviewer.worker_id == actor_key:
            raise HiveTransitionError(
                "Rollback actor and independent reviewer must be distinct."
            )
        effect_key = self._required(effect_id, "effect_id")
        request = {
            "effect_id": effect_key,
            "reviewer_worker_id": reviewer.worker_id,
            "actor": actor_key,
            "reason": self._required(reason, "reason"),
            "human_approval": True,
        }
        fingerprint = self._fingerprint(request)
        if idempotency_key:
            with self._connect() as conn:
                existing = conn.execute(
                    "SELECT effect_id, request_sha256 FROM hive_external_effect_events WHERE idempotency_key = ?",
                    (idempotency_key,),
                ).fetchone()
            if existing:
                if str(existing["request_sha256"]) != fingerprint:
                    raise HiveIdempotencyConflict(
                        "Rollback idempotency key was reused with different content."
                    )
                return self.get_effects(effect_id=str(existing["effect_id"]))
        with self._connect() as conn:
            row = conn.execute(
                "SELECT * FROM hive_external_effect_receipts WHERE effect_id = ?",
                (effect_key,),
            ).fetchone()
        if not row:
            raise HiveNotFoundError(f"External effect not found: {effect_key}")
        effect = self._effect(row)
        if effect.status == EffectStatus.ROLLED_BACK.value:
            return self.get_effects(effect_id=effect_key)
        if effect.status != EffectStatus.APPLIED.value or effect.dry_run:
            raise HiveTransitionError(
                f"External effect cannot roll back from status {effect.status}."
            )
        if self._sha256(str(rollback_token or "").encode("utf-8")) != str(
            row["rollback_token_sha256"]
        ):
            raise HiveTransitionError("Rollback token is invalid.")
        certification = self._active_certification(effect.certification_id)
        target = Path(effect.target_path).resolve()
        if target.parent != Path(certification.sandbox_root).resolve():
            raise HiveTransitionError(
                "Rollback target failed certified sandbox verification."
            )
        current_exists = target.exists()
        current_data = target.read_bytes() if current_exists else b""
        current_sha = self._sha256(current_data) if current_exists else ""
        now = self._now_ms()
        if current_exists != effect.after_exists or current_sha != effect.after_sha256:
            with self._write() as conn:
                conn.execute(
                    "UPDATE hive_external_effect_receipts SET status = ?, rollback_reason = ?, updated_at = ?, revision = revision + 1 WHERE effect_id = ?",
                    (EffectStatus.TAMPERED.value, request["reason"], now, effect_key),
                )
                conn.execute(
                    "INSERT INTO hive_external_effect_events(event_id, effect_id, event_type, actor, payload_json, idempotency_key, request_sha256, created_at) VALUES (?, ?, 'ROLLBACK_TAMPER_DETECTED', ?, ?, ?, ?, ?)",
                    (
                        str(uuid.uuid4()),
                        effect_key,
                        actor_key,
                        self._json(
                            {
                                "current_sha256": current_sha,
                                "expected_sha256": effect.after_sha256,
                            }
                        ),
                        idempotency_key,
                        fingerprint,
                        now,
                    ),
                )
            raise HiveTransitionError(
                "Rollback refused because the target changed after the certified effect."
            )
        before_data = base64.b64decode(str(row["before_content_b64"]) or "")
        try:
            if effect.before_exists:
                target.write_bytes(before_data)
            else:
                target.unlink(missing_ok=True)
            restored_exists = target.exists()
            restored_data = target.read_bytes() if restored_exists else b""
            restored_sha = self._sha256(restored_data) if restored_exists else ""
            if (
                restored_exists != effect.before_exists
                or restored_sha != effect.before_sha256
            ):
                raise HiveTransitionError("Rollback postcondition verification failed.")
        except Exception as exc:
            with self._write() as conn:
                conn.execute(
                    "UPDATE hive_external_effect_receipts SET status = ?, rollback_reason = ?, updated_at = ?, revision = revision + 1 WHERE effect_id = ?",
                    (EffectStatus.COMPENSATION_FAILED.value, str(exc), now, effect_key),
                )
                conn.execute(
                    "INSERT INTO hive_external_effect_events(event_id, effect_id, event_type, actor, payload_json, idempotency_key, request_sha256, created_at) VALUES (?, ?, 'ROLLBACK_COMPENSATION_FAILED', ?, ?, ?, ?, ?)",
                    (
                        str(uuid.uuid4()),
                        effect_key,
                        actor_key,
                        self._json({"error": str(exc)}),
                        idempotency_key,
                        fingerprint,
                        now,
                    ),
                )
            raise
        with self._write() as conn:
            conn.execute(
                "UPDATE hive_external_effect_receipts SET status = ?, rollback_reason = ?, updated_at = ?, revision = revision + 1 WHERE effect_id = ?",
                (EffectStatus.ROLLED_BACK.value, request["reason"], now, effect_key),
            )
            conn.execute(
                "INSERT INTO hive_external_effect_events(event_id, effect_id, event_type, actor, payload_json, idempotency_key, request_sha256, created_at) VALUES (?, ?, 'EFFECT_ROLLED_BACK', ?, ?, ?, ?, ?)",
                (
                    str(uuid.uuid4()),
                    effect_key,
                    actor_key,
                    self._json(
                        {
                            "reviewer_worker_id": reviewer.worker_id,
                            "reason": request["reason"],
                        }
                    ),
                    idempotency_key,
                    fingerprint,
                    now,
                ),
            )
        return self.get_effects(effect_id=effect_key)


_EXTERNAL_EFFECT_LOCK = threading.RLock()
_EXTERNAL_EFFECT_CACHE: dict[tuple[str, str], ExternalEffectCertificationStore] = {}


def get_hive_external_effect_store(
    path: str | Path | None = None,
    effect_root: str | Path | None = None,
) -> ExternalEffectCertificationStore:
    resolved = Path(path or default_hive_runtime_db_path()).expanduser().resolve()
    root = (
        Path(effect_root or default_external_effect_root(resolved))
        .expanduser()
        .resolve()
    )
    key = (str(resolved), str(root))
    with _EXTERNAL_EFFECT_LOCK:
        store = _EXTERNAL_EFFECT_CACHE.get(key)
        if store is None:
            store = ExternalEffectCertificationStore(resolved, root)
            _EXTERNAL_EFFECT_CACHE[key] = store
        return store
