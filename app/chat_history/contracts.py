"""Versioned contracts for Notion transcript normalization and storage."""

from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path


PARSER_CONTRACT_VERSION = "notion-thread-message-v1"
PROJECTION_VERSION = 1
HISTORY_SCHEMA_VERSION = 2
RUNTIME_HISTORY_CONTRACT_VERSION = 1

# The server archive is populated by explicit synchronization/reconciliation.
# Change this only when an automatic lossless server-record ingestion loop exists.
LIVE_SERVER_ARCHIVE_MODE = "reconciliation_only"


def _opaque_store_id(kind: str, path: str | os.PathLike[str]) -> str:
    """Fingerprint a storage binding without exposing its filesystem path."""
    resolved = str(Path(path).expanduser().resolve())
    payload = f"notion2api:{kind}:{resolved}".encode("utf-8", errors="replace")
    return hashlib.sha256(payload).hexdigest()[:24]


def _deployed_commit(value: str | None) -> str:
    candidate = str(value or "").strip()
    return candidate.lower() if re.fullmatch(r"[0-9a-fA-F]{7,64}", candidate) else "unverified"


def runtime_history_contract(
    *,
    conversation_store_path: str | os.PathLike[str],
    history_store_root: str | os.PathLike[str],
    history_schema_hash: str,
    build_commit: str | None = None,
) -> dict[str, str | int]:
    """Return non-secret history/runtime fingerprints suitable for health output.

    The build identifier must be injected by the deployment/startup receipt.  A
    missing value is reported as unverified rather than inferred from a possibly
    dirty checkout.
    """
    deployed_commit = _deployed_commit(
        build_commit
        or os.getenv("NOTION2API_BUILD_COMMIT")
        or "unverified"
    )
    return {
        "runtime_contract_version": RUNTIME_HISTORY_CONTRACT_VERSION,
        "build_commit": deployed_commit,
        "history_schema_version": HISTORY_SCHEMA_VERSION,
        "history_schema_hash": history_schema_hash,
        "history_schema_hash_scope": "declared_contract",
        "parser_contract_version": PARSER_CONTRACT_VERSION,
        "projection_version": PROJECTION_VERSION,
        "conversation_store_id": _opaque_store_id(
            "conversation-store", conversation_store_path
        ),
        "history_store_id": _opaque_store_id("history-store", history_store_root),
        "live_server_archive_mode": LIVE_SERVER_ARCHIVE_MODE,
    }
