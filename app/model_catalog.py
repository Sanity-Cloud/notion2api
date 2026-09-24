from __future__ import annotations

import hashlib
import json
import os
import threading
import time
import uuid
from dataclasses import dataclass
from typing import Any, Callable, Iterable

from app.model_restriction_cache import ModelRestrictionCache

CATALOG_SCHEMA_VERSION = 1
CATALOG_CACHE_PREFIX = "model-catalog:v1"
DEFAULT_CATALOG_TTL_SECONDS = 300.0
DEFAULT_CATALOG_MAX_STALE_SECONDS = 86_400.0
DEFAULT_REFRESH_WAIT_SECONDS = 5.0


class ModelCatalogError(RuntimeError):
    """Base error for authoritative model catalog operations."""


class ModelCatalogUnavailable(ModelCatalogError):
    """Raised when neither live nor policy-valid last-known-good catalog exists."""


class ModelCatalogValidationError(ModelCatalogError):
    """Raised when an upstream picker response is structurally invalid."""


class ModelSelectionError(ModelCatalogError):
    def __init__(self, message: str, *, code: str, param: str = "model") -> None:
        super().__init__(message)
        self.code = code
        self.param = param


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _clean_efforts(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    efforts: list[str] = []
    for item in value:
        effort = _clean_text(item)
        if effort and effort not in efforts:
            efforts.append(effort)
    return efforts


def _route_record(value: Any) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {"final_model_name": "", "beta": False, "supported": False}
    route = _clean_text(value.get("finalModelName"))
    return {
        "final_model_name": route,
        "beta": bool(value.get("beta", False)),
        "supported": bool(route),
    }


def _normalized_model_record(item: dict[str, Any]) -> dict[str, Any]:
    canonical_id = _clean_text(item.get("model"))
    if not canonical_id:
        raise ModelCatalogValidationError("Picker model entry is missing 'model'.")

    configuration = item.get("modelConfiguration")
    if configuration is None:
        configuration = {}
    if not isinstance(configuration, dict):
        raise ModelCatalogValidationError(
            f"Picker model '{canonical_id}' has invalid modelConfiguration."
        )
    efforts = _clean_efforts(configuration.get("supportedReasoningEfforts"))
    default_effort = _clean_text(configuration.get("defaultReasoningEffort"))
    if default_effort and default_effort not in efforts:
        raise ModelCatalogValidationError(
            f"Picker model '{canonical_id}' default effort is not supported."
        )

    ratings = item.get("modelCardAttributes")
    if not isinstance(ratings, dict):
        ratings = {}
    normalized_ratings: dict[str, int] = {}
    for key in ("speed", "intelligence", "cost"):
        value = ratings.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            normalized_ratings[key] = value

    disabled_reason = _clean_text(item.get("disabledReason"))
    restricted_codename = _clean_text(item.get("restrictedAccessModelCodename"))
    return {
        "canonical_id": canonical_id,
        "display_name": _clean_text(item.get("modelMessage")) or canonical_id,
        "model_family": _clean_text(item.get("modelFamily")) or "unknown",
        "model_provider": _clean_text(item.get("modelProvider")) or "unknown",
        "display_group": _clean_text(item.get("displayGroup")),
        "supported_reasoning_efforts": efforts,
        "default_reasoning_effort": default_effort,
        "restricted_access_model_codename": restricted_codename,
        "is_disabled": bool(item.get("isDisabled", False)) or bool(disabled_reason),
        "disabled_reason": disabled_reason,
        "is_disabled_only_by_disaster_recovery": bool(
            item.get("isDisabledOnlyByDisasterRecovery", False)
        ),
        "is_approaching_rate_limit": bool(item.get("isApproachingRateLimit", False)),
        "model_card_attributes": normalized_ratings or None,
        "routes": {
            "workflow": _route_record(item.get("workflow")),
            "custom_agent": _route_record(item.get("customAgent")),
            "agent_service": _route_record(item.get("agentService")),
        },
    }


def parse_picker_catalog(payload: dict[str, Any]) -> dict[str, Any]:
    """Validate and normalize one complete getAvailableModels response."""
    if not isinstance(payload, dict):
        raise ModelCatalogValidationError("Picker response must be an object.")
    raw_models = payload.get("models")
    if not isinstance(raw_models, list):
        raise ModelCatalogValidationError("Picker response 'models' must be a list.")
    if not raw_models:
        restriction = (
            " Model selection is restricted for this workspace."
            if bool(payload.get("modelSelectionRestricted", False))
            else ""
        )
        raise ModelCatalogValidationError(
            f"Picker response contains no usable models.{restriction}"
        )

    models: list[dict[str, Any]] = []
    rejected_models: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(raw_models):
        if not isinstance(raw, dict):
            rejected_models.append(
                {
                    "index": index,
                    "canonical_id": "",
                    "error": "Picker model entry must be an object.",
                }
            )
            continue
        try:
            model = _normalized_model_record(raw)
        except ModelCatalogValidationError as exc:
            rejected_models.append(
                {
                    "index": index,
                    "canonical_id": _clean_text(raw.get("model")),
                    "error": str(exc),
                }
            )
            continue
        canonical_id = model["canonical_id"]
        if canonical_id in seen:
            rejected_models.append(
                {
                    "index": index,
                    "canonical_id": canonical_id,
                    "error": f"Duplicate picker model '{canonical_id}'.",
                }
            )
            continue
        seen.add(canonical_id)
        models.append(model)

    if not models:
        raise ModelCatalogValidationError(
            "Picker response contained models, but none were individually valid."
        )

    restricted: dict[str, dict[str, str]] = {}
    restricted_entries = payload.get("restrictedAccessModelsInPickerConfig")
    if restricted_entries is None:
        restricted_entries = []
    if not isinstance(restricted_entries, list):
        raise ModelCatalogValidationError(
            "restrictedAccessModelsInPickerConfig must be a list."
        )
    for raw in restricted_entries:
        if not isinstance(raw, dict):
            continue
        codename = _clean_text(raw.get("codename"))
        if not codename:
            continue
        restricted[codename] = {
            "disabled_reason": _clean_text(raw.get("disabledReason"))
            or "restricted_access",
            "display_name": _clean_text(raw.get("modelMessage")),
            "model_family": _clean_text(raw.get("modelFamily")),
        }

    for model in models:
        restriction_key = (
            model.get("restricted_access_model_codename")
            or model["canonical_id"]
        )
        restriction = restricted.get(str(restriction_key))
        if restriction:
            model["is_disabled"] = True
            model["disabled_reason"] = restriction["disabled_reason"]

    normalized = {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "restricted_geo_policy_applied": bool(
            payload.get("restrictedGeoPolicyApplied", False)
        ),
        "model_selection_restricted": bool(
            payload.get("modelSelectionRestricted", False)
        ),
        "upstream_model_count": len(raw_models),
        "rejected_model_count": len(rejected_models),
        "rejected_models": rejected_models,
        "models": models,
        "restricted_models": sorted(restricted),
    }
    serialized = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    normalized["snapshot_sha256"] = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return normalized


def static_catalog_snapshot(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Build a transparent, non-authoritative fallback for observability only."""
    models = [dict(record) for record in records]
    normalized = {
        "schema_version": CATALOG_SCHEMA_VERSION,
        "restricted_geo_policy_applied": False,
        "models": models,
        "restricted_models": sorted(
            model["canonical_id"] for model in models if model.get("is_disabled")
        ),
    }
    serialized = json.dumps(normalized, sort_keys=True, separators=(",", ":"), default=str)
    normalized["snapshot_sha256"] = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
    return normalized


@dataclass(frozen=True)
class CatalogEnvelope:
    snapshot: dict[str, Any]
    source: str
    fetched_at: float
    expires_at: float
    age_seconds: float
    stale: bool
    upstream_error: str = ""
    stale_policy_exceeded: bool = False

    def receipt(self) -> dict[str, Any]:
        fallback = self.source in {"last_known_good", "static_fallback"}
        return {
            "catalog_source": self.source,
            "catalog_snapshot_sha256": _clean_text(
                self.snapshot.get("snapshot_sha256")
            ),
            "catalog_fetched_at": self.fetched_at,
            "catalog_expires_at": self.expires_at,
            "catalog_age_seconds": round(max(0.0, self.age_seconds), 3),
            "catalog_stale": self.stale,
            "catalog_stale_policy_exceeded": self.stale_policy_exceeded,
            "catalog_fallback": fallback,
            "catalog_fallback_reason": self.upstream_error if fallback else "",
            "catalog_upstream_error": self.upstream_error,
            "catalog_model_count": len(self.snapshot.get("models") or []),
            "catalog_rejected_model_count": int(
                self.snapshot.get("rejected_model_count") or 0
            ),
            "catalog_rejected_models": list(
                self.snapshot.get("rejected_models") or []
            ),
            "catalog_model_selection_restricted": bool(
                self.snapshot.get("model_selection_restricted", False)
            ),
        }


class ModelCatalogService:
    """Workspace-global authoritative picker cache with last-known-good fallback."""

    def __init__(
        self,
        cache: ModelRestrictionCache | None = None,
        *,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.cache = cache or ModelRestrictionCache()
        self.clock = clock
        self.owner_id = f"{os.getpid()}:{uuid.uuid4().hex}"
        self._locks: dict[str, threading.Lock] = {}
        self._locks_guard = threading.Lock()

    @staticmethod
    def cache_key(space_id: str) -> str:
        clean_space = _clean_text(space_id)
        if not clean_space:
            raise ModelCatalogUnavailable("Notion workspace id is unavailable.")
        return f"{CATALOG_CACHE_PREFIX}:{clean_space}"

    def _lock_for(self, cache_key: str) -> threading.Lock:
        with self._locks_guard:
            return self._locks.setdefault(cache_key, threading.Lock())

    @staticmethod
    def _ttl_seconds() -> float:
        return max(
            1.0,
            float(
                os.getenv(
                    "NOTION_MODEL_CATALOG_CACHE_TTL_SECONDS",
                    str(DEFAULT_CATALOG_TTL_SECONDS),
                )
            ),
        )

    @staticmethod
    def _max_stale_seconds() -> float:
        return max(
            0.0,
            float(
                os.getenv(
                    "NOTION_MODEL_CATALOG_MAX_STALE_SECONDS",
                    str(DEFAULT_CATALOG_MAX_STALE_SECONDS),
                )
            ),
        )

    @staticmethod
    def _refresh_wait_seconds() -> float:
        return max(
            0.0,
            float(
                os.getenv(
                    "NOTION_MODEL_CATALOG_REFRESH_WAIT_SECONDS",
                    str(DEFAULT_REFRESH_WAIT_SECONDS),
                )
            ),
        )

    def _from_cache(
        self,
        cached: dict[str, Any],
        *,
        source: str,
        upstream_error: str = "",
        stale_policy_exceeded: bool = False,
    ) -> CatalogEnvelope:
        now = self.clock()
        fetched_at = float(cached.get("fetched_at") or 0.0)
        expires_at = float(cached.get("expires_at") or 0.0)
        payload = cached.get("payload")
        if not isinstance(payload, dict) or not isinstance(payload.get("models"), list):
            raise ModelCatalogUnavailable("Cached model catalog is malformed.")
        return CatalogEnvelope(
            snapshot=payload,
            source=source,
            fetched_at=fetched_at,
            expires_at=expires_at,
            age_seconds=max(0.0, now - fetched_at),
            stale=expires_at <= now,
            upstream_error=upstream_error,
            stale_policy_exceeded=stale_policy_exceeded,
        )

    def get(
        self,
        client: Any,
        *,
        allow_static_fallback: bool = False,
        static_records: Iterable[dict[str, Any]] = (),
    ) -> CatalogEnvelope:
        cache_key = self.cache_key(getattr(client, "space_id", ""))
        fresh = self.cache.get(cache_key)
        if fresh is not None:
            return self._from_cache(fresh, source="authoritative_cache")

        with self._lock_for(cache_key):
            fresh = self.cache.get(cache_key)
            if fresh is not None:
                return self._from_cache(fresh, source="authoritative_cache")

            lease = self.cache.claim_refresh(cache_key, owner_id=self.owner_id)
            if not lease:
                deadline = self.clock() + self._refresh_wait_seconds()
                while self.clock() < deadline:
                    time.sleep(0.05)
                    fresh = self.cache.get(cache_key)
                    if fresh is not None:
                        return self._from_cache(fresh, source="authoritative_cache")
                return self._fallback(
                    cache_key,
                    "catalog_refresh_in_progress",
                    allow_static_fallback=allow_static_fallback,
                    static_records=static_records,
                )

            try:
                raw = client.get_ai_model_picker_config()
                normalized = parse_picker_catalog(raw)
                stored = self.cache.store(
                    cache_key,
                    normalized,
                    lease_token=lease,
                    ttl_seconds=self._ttl_seconds(),
                )
                if not stored:
                    raise ModelCatalogUnavailable(
                        "Model catalog refresh lease expired before publication."
                    )
                fresh = self.cache.get(cache_key)
                if fresh is None:
                    raise ModelCatalogUnavailable(
                        "Model catalog was not readable after publication."
                    )
                return self._from_cache(fresh, source="authoritative_live")
            except Exception as exc:
                self.cache.release_refresh(cache_key, lease_token=lease)
                return self._fallback(
                    cache_key,
                    f"{type(exc).__name__}: {exc}",
                    allow_static_fallback=allow_static_fallback,
                    static_records=static_records,
                )

    def _fallback(
        self,
        cache_key: str,
        upstream_error: str,
        *,
        allow_static_fallback: bool,
        static_records: Iterable[dict[str, Any]],
    ) -> CatalogEnvelope:
        stale = self.cache.get(cache_key, allow_stale=True)
        if stale is not None:
            envelope = self._from_cache(
                stale,
                source="last_known_good",
                upstream_error=upstream_error,
            )
            stale_policy_exceeded = (
                envelope.age_seconds > self._max_stale_seconds()
            )
            return self._from_cache(
                stale,
                source="last_known_good",
                upstream_error=upstream_error,
                stale_policy_exceeded=stale_policy_exceeded,
            )
        if allow_static_fallback:
            return CatalogEnvelope(
                snapshot=static_catalog_snapshot(static_records),
                source="static_fallback",
                fetched_at=0.0,
                expires_at=0.0,
                age_seconds=0.0,
                stale=True,
                upstream_error=upstream_error,
            )
        raise ModelCatalogUnavailable(
            "Authoritative model catalog is unavailable and no valid last-known-good snapshot exists."
        )


def model_by_id(snapshot: dict[str, Any], canonical_id: str) -> dict[str, Any] | None:
    requested = _clean_text(canonical_id)
    for model in snapshot.get("models", []):
        if isinstance(model, dict) and _clean_text(model.get("canonical_id")) == requested:
            return model
    return None


def resolve_reasoning_effort(
    model: dict[str, Any], requested_effort: str | None
) -> dict[str, Any]:
    supported = list(model.get("supported_reasoning_efforts") or [])
    default = _clean_text(model.get("default_reasoning_effort"))
    requested = None if requested_effort is None else str(requested_effort)
    if requested is not None:
        if not requested or requested not in supported:
            raise ModelSelectionError(
                f"Reasoning effort '{requested}' is unsupported for model "
                f"'{model.get('canonical_id')}'. Supported values: "
                f"{', '.join(supported) if supported else 'none'}.",
                code="reasoning_effort_not_supported",
                param="reasoning_effort",
            )
        return {
            "requested_reasoning_effort": requested,
            "resolved_reasoning_effort": requested,
            "reasoning_effort_source": "explicit",
            "supported_reasoning_efforts": supported,
            "default_reasoning_effort": default,
        }
    return {
        "requested_reasoning_effort": None,
        "resolved_reasoning_effort": default or None,
        "reasoning_effort_source": "catalog_default" if default else "not_supported",
        "supported_reasoning_efforts": supported,
        "default_reasoning_effort": default,
    }