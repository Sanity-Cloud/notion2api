"""Workspace model discovery from user-verified UI evidence, not Notion's picker API.

This is an evidence-backed *display* catalog. It does not grant routing authority.
Historical codenames are retained as candidates until separately verified.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import date, datetime, timezone
from typing import Any, Iterable

from app.model_catalog import CatalogEnvelope

OBSERVED_ON = date(2026, 11, 22)
WORKSPACE_KEY = "sanity-monkey"
EVIDENCE = "user_confirmed_current_workspace_picker_screenshots"
VISIBLE_RESTRICTIONS = frozenset({"GPT-6 Astra"})
SELECTED_IN_PICKER = frozenset({"Grok 4.3"})

# Names transcribed from six user-provided screenshots. Not an availability claim.
OBSERVED_LABELS = (
    "DeepSeek V4 Flash",
    "DeepSeek V4 Pro",
    "DeepSeek V4.1 Flash",
    "Gemini 3.1 Pro",
    "Gemini 3.5 Flash",
    "Gemini 3.6 Flash",
    "Gemini 3.7 Flash",
    "GLM 5.2",
    "GLM 5.3",
    "GLM 5.3 Flash",
    "GPT-5.2",
    "GPT-5.4",
    "GPT-5.5",
    "GPT-5.6 Luna",
    "GPT-5.6 Sol",
    "GPT-5.6 Terra",
    "GPT-6 Astra",
    "GPT-6 Luna",
    "GPT-6.1 Sol",
    "GPT-6.1 Terra",
    "GPT-6.2 Sol",
    "Grok 4.3",
    "Grok 4.5",
    "Grok 4.6",
    "Grok 4.7",
    "Grok Build 0.1",
    "Haiku 5.5",
    "Kimi K3",
    "Opus 4.7",
    "Opus 4.8",
    "Opus 5",
    "Opus 5.5",
    "Sonnet 4.6",
    "Sonnet 5",
    "Sonnet 5.5",
)


def _normalize_label(value: object) -> str:
    return " ".join(str(value or "").split()).casefold()


def _unknown_id(label: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", label.lower()).strip("-")
    return f"unverified:{slug}"


def curated_ui_envelope(
    historical_records: Iterable[dict[str, Any]],
    *,
    today: date | None = None,
) -> CatalogEnvelope:
    """List current UI labels without trusting historical routing/entitlements."""
    records = {_normalize_label(item.get("display_name")): item
               for item in historical_records if isinstance(item, dict)}
    models: list[dict[str, Any]] = []
    used_ids: set[str] = set()
    for label in OBSERVED_LABELS:
        historical = records.get(_normalize_label(label), {})
        previous_id = str(historical.get("canonical_id") or "").strip()
        # A historical route ID is evidence, not a currently authorized model ID.
        # Expose only a deliberately non-routable identifier until verified.
        model_id = _unknown_id(label)
        if model_id in used_ids:
            raise ValueError(f"Duplicate catalog candidate ID: {model_id}")
        used_ids.add(model_id)
        explicitly_restricted = label in VISIBLE_RESTRICTIONS
        models.append({
            # Only the visible label and its explicit restriction are current
            # evidence. Historical catalog fields must not be laundered as UI.
            "canonical_id": model_id,
            "display_name": label,
            "public_name": label,
            "model_family": historical.get("model_family") or (
                "openai" if label.startswith("GPT-") else "unknown"
            ),
            "model_provider": historical.get("model_family") or "unknown",
            "display_group": "",
            "model_card_attributes": None,
            "supported_reasoning_efforts": [],
            "default_reasoning_effort": "",
            "is_disabled": explicitly_restricted,
            "disabled_reason": (
                "workspace_restricted_visible" if explicitly_restricted else ""
            ),
            "permission_status": (
                "restricted" if explicitly_restricted else "unverified"
            ),
            "historical_disabled": bool(historical.get("is_disabled")),
            "previously_observed_route_id": previous_id,
            "route_verified": False,
            "selectable": False,
            "selected_in_picker": label in SELECTED_IN_PICKER,
            "metadata_source": "user_confirmed_ui_label",
            "route_candidate_source": (
                "historical_static_mapping" if previous_id else "unverified"
            ),
            "routes": {},
        })
    snapshot = {
        "schema_version": 1,
        "workspace_key": WORKSPACE_KEY,
        "catalog_evidence": EVIDENCE,
        "catalog_observed_on": OBSERVED_ON.isoformat(),
        "model_selection_restricted": None,
        "catalog_routes_verified": False,
        "models": models,
    }
    canonical = json.dumps(snapshot, sort_keys=True, separators=(",", ":"))
    snapshot["snapshot_sha256"] = hashlib.sha256(canonical.encode()).hexdigest()
    observed = datetime.combine(OBSERVED_ON, datetime.min.time(), tzinfo=timezone.utc)
    observation_age_days = (today or datetime.now(timezone.utc).date()) - OBSERVED_ON
    age_seconds = max(0.0, observation_age_days.days * 86400.0)
    return CatalogEnvelope(
        snapshot=snapshot, source="curated_ui", fetched_at=observed.timestamp(),
        expires_at=(observed.timestamp() + 86400.0), age_seconds=age_seconds,
        stale=observation_age_days.days > 0,
        stale_policy_exceeded=observation_age_days.days > 7,
    )
