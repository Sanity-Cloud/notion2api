from __future__ import annotations

import hashlib
import json
import math
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Callable, Iterator
from urllib.parse import urlparse

from app.notion_admission_store import SharedAdmissionStore
from app.notion_request_telemetry import (
    NotionRequestTelemetryStore,
    UsageQuotaExceededError,
)


class AdmissionError(RuntimeError):
    """Base error raised before an upstream Notion request is admitted."""


class DuplicateAdmissionError(AdmissionError):
    """Raised when an idempotency key is already active or recently completed."""


class AdmissionTimeoutError(AdmissionError):
    """Raised when an operation cannot enter the keyed queue before its deadline."""


_REQUEST_TELEMETRY = NotionRequestTelemetryStore()
_WEIGHT_SCALE = 1000


def _normalized_weight(value: Any, *, capacity: float | None = None) -> float:
    if isinstance(value, bool):
        raise AdmissionError("Admission weight must be a finite positive number")
    try:
        numeric = float(value)
    except (TypeError, ValueError) as exc:
        raise AdmissionError("Admission weight must be a finite positive number") from exc
    if not math.isfinite(numeric) or numeric <= 0:
        raise AdmissionError("Admission weight must be a finite positive number")
    units = int(round(numeric * _WEIGHT_SCALE))
    if units <= 0:
        raise AdmissionError("Admission weight is below the supported precision")
    normalized = units / _WEIGHT_SCALE
    if capacity is not None and normalized > capacity:
        raise AdmissionError(
            f"Admission weight {normalized:.3f} exceeds account capacity {capacity:.3f}"
        )
    return normalized


def _bounded_identifier(value: Any, *, maximum: int = 160) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    safe = all(character.isalnum() or character in "._:-/" for character in text)
    if safe and len(text) <= maximum:
        return text
    digest = hashlib.sha256(text.encode("utf-8", errors="replace")).hexdigest()
    return f"sha256:{digest}"


def _bounded_error_class(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if len(text) <= 96 and all(
        character.isalnum() or character in "._:-" for character in text
    ):
        return text
    return "other"


def _env_float(name: str, default: float, *, minimum: float = 0.0) -> float:
    try:
        return max(minimum, float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _env_int(name: str, default: int, *, minimum: int = 0) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _normalized_id(value: Any) -> str:
    return str(value or "").strip().replace("-", "").casefold()


def _stable_payload_fingerprint(payload: Any) -> str:
    try:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        encoded = repr(payload)
    return hashlib.sha256(encoded.encode("utf-8", errors="replace")).hexdigest()[:20]


def _extract_thread_id(payload: Any, fallback: str = "") -> str:
    if isinstance(payload, dict):
        for key in (
            "threadId",
            "thread_id",
            "conversationId",
            "conversation_id",
            "chatId",
            "chat_id",
        ):
            value = str(payload.get(key) or "").strip()
            if value:
                return value
        transcript = payload.get("transcript")
        if isinstance(transcript, list):
            for block in transcript:
                if not isinstance(block, dict):
                    continue
                value = block.get("value")
                if isinstance(value, dict):
                    for key in ("threadId", "thread_id"):
                        candidate = str(value.get(key) or "").strip()
                        if candidate:
                            return candidate
    return str(fallback or "").strip()


def _retry_after_seconds(response: Any) -> float | None:
    headers = getattr(response, "headers", None)
    if not headers:
        return None
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None
    try:
        return max(0.0, float(raw))
    except (TypeError, ValueError):
        return None


def _safe_json_size_bytes(payload: Any) -> int:
    if payload is None:
        return 0
    try:
        encoded = json.dumps(
            payload,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8", errors="replace")
    except Exception:
        encoded = repr(payload).encode("utf-8", errors="replace")
    return len(encoded)


def _estimated_tokens(byte_count: int) -> int:
    try:
        bytes_per_token = max(
            1.0,
            float(os.getenv("NOTION_TOKEN_ESTIMATE_BYTES_PER_TOKEN", "4")),
        )
    except (TypeError, ValueError):
        bytes_per_token = 4.0
    if byte_count <= 0:
        return 0
    return max(1, int((byte_count + bytes_per_token - 1) // bytes_per_token))


def _extract_model_id(payload: Any, owner: Any) -> str:
    explicit = str(getattr(owner, "request_model_id", "") or "").strip()
    if explicit:
        return explicit
    if not isinstance(payload, dict):
        return ""
    direct = str(payload.get("model") or payload.get("modelId") or "").strip()
    if direct:
        return direct
    transcript = payload.get("transcript")
    if isinstance(transcript, list):
        for block in transcript:
            if not isinstance(block, dict) or block.get("type") != "config":
                continue
            value = block.get("value")
            if isinstance(value, dict):
                model_id = str(value.get("model") or "").strip()
                if model_id:
                    return model_id
    return ""


def _request_lineage(payload: Any, owner: Any) -> tuple[str, str]:
    trace_id = str(getattr(owner, "request_trace_id", "") or "").strip()
    context_id = str(getattr(owner, "request_context_id", "") or "").strip()
    if isinstance(payload, dict):
        trace_id = trace_id or str(
            payload.get("traceId")
            or payload.get("requestId")
            or payload.get("request_id")
            or ""
        ).strip()
        context_id = context_id or str(
            payload.get("conversationId")
            or payload.get("conversation_id")
            or payload.get("threadId")
            or payload.get("thread_id")
            or ""
        ).strip()
    return trace_id, context_id


def _workload_profile(
    operation: str,
    *,
    estimated_input_tokens: int,
    capacity: float,
) -> tuple[str, float]:
    normalized = str(operation or "").casefold()
    if "getavailablemodels" in normalized:
        return (
            "metadata",
            min(
                capacity,
                _env_float("NOTION_ADMISSION_METADATA_WEIGHT", 0.25, minimum=0.001),
            ),
        )
    if "runinferencetranscript" in normalized:
        base = _env_float("NOTION_ADMISSION_INFERENCE_BASE_WEIGHT", 1.0, minimum=0.001)
        quantum = _env_float(
            "NOTION_ADMISSION_INFERENCE_TOKEN_QUANTUM",
            8000.0,
            minimum=1.0,
        )
        extra = max(0.0, float(estimated_input_tokens) / quantum)
        return "inference", min(capacity, max(0.001, base + extra))
    if any(
        marker in normalized
        for marker in (
            "getrecordvalues",
            "gettasks",
            "getspaces",
            "loadpagechunk",
            "search",
        )
    ):
        return (
            "read",
            min(
                capacity,
                _env_float("NOTION_ADMISSION_READ_WEIGHT", 0.5, minimum=0.001),
            ),
        )
    return (
        "mutation",
        min(
            capacity,
            _env_float("NOTION_ADMISSION_DEFAULT_WEIGHT", 1.0, minimum=0.001),
        ),
    )


def _response_size_bytes(response: Any) -> int:
    headers = getattr(response, "headers", None) or {}
    raw_length = headers.get("Content-Length") or headers.get("content-length")
    try:
        if raw_length is not None:
            return max(0, int(raw_length))
    except (TypeError, ValueError):
        pass
    content = getattr(response, "content", None)
    if isinstance(content, bytes):
        return len(content)
    if isinstance(content, str):
        return len(content.encode("utf-8", errors="replace"))
    return 0


def _actual_usage(response: Any) -> tuple[int | None, int | None, int | None]:
    usage = getattr(response, "usage", None)
    headers = getattr(response, "headers", None) or {}
    if not isinstance(usage, dict):
        usage = {}
        content_type = str(
            headers.get("Content-Type") or headers.get("content-type") or ""
        ).casefold()
        response_json = getattr(response, "json", None)
        if "json" in content_type and callable(response_json):
            try:
                body = response_json()
                if isinstance(body, dict) and isinstance(body.get("usage"), dict):
                    usage = body["usage"]
            except Exception:
                usage = {}

    def value(*keys: str) -> int | None:
        for key in keys:
            candidate = usage.get(key)
            if candidate is None:
                candidate = headers.get(key) or headers.get(key.lower())
            try:
                if candidate is not None:
                    return max(0, int(candidate))
            except (TypeError, ValueError):
                continue
        return None

    input_tokens = value("input_tokens", "prompt_tokens", "X-Usage-Input-Tokens")
    output_tokens = value(
        "output_tokens", "completion_tokens", "X-Usage-Output-Tokens"
    )
    total_tokens = value("total_tokens", "X-Usage-Total-Tokens")
    if total_tokens is None and (input_tokens is not None or output_tokens is not None):
        total_tokens = int(input_tokens or 0) + int(output_tokens or 0)
    return input_tokens, output_tokens, total_tokens


@dataclass
class AdmissionReceipt:
    disposition: str
    account_key: str
    thread_key: str
    idempotency_key: str
    queue_depth: int
    account_queue_depth: int
    waited_seconds: float
    throttled_seconds: float
    admitted_at: float
    operation: str
    retry_count: int = 0
    retry_after_seconds: float = 0.0
    attempt_id: str = ""
    workload_class: str = "legacy"
    admission_weight: float = 1.0
    trace_id: str = ""
    request_context_id: str = ""
    model_id: str = ""
    request_bytes: int = 0
    estimated_input_tokens: int = 0
    completion: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "disposition": self.disposition,
            "account_key": self.account_key,
            "thread_key": self.thread_key,
            "idempotency_key": self.idempotency_key,
            "queue_depth": self.queue_depth,
            "account_queue_depth": self.account_queue_depth,
            "waited_seconds": round(self.waited_seconds, 3),
            "throttled_seconds": round(self.throttled_seconds, 3),
            "admitted_at": self.admitted_at,
            "operation": self.operation,
            "retry_count": self.retry_count,
            "retry_after_seconds": round(self.retry_after_seconds, 3),
            "attempt_id": self.attempt_id,
            "workload_class": self.workload_class,
            "admission_weight": round(self.admission_weight, 3),
            "trace_id": self.trace_id,
            "request_context_id": self.request_context_id,
            "model_id": self.model_id,
            "request_bytes": self.request_bytes,
            "estimated_input_tokens": self.estimated_input_tokens,
            "completion": dict(self.completion),
        }


class _TokenBucket:
    def __init__(self, capacity: float, refill_per_second: float, now: float) -> None:
        self.capacity_units = max(1, int(round(capacity * _WEIGHT_SCALE)))
        self.refill_units_per_second = max(
            1, int(round(refill_per_second * _WEIGHT_SCALE))
        )
        self.token_units = self.capacity_units
        self.updated_at = now

    @property
    def capacity(self) -> float:
        return self.capacity_units / _WEIGHT_SCALE

    @property
    def refill_per_second(self) -> float:
        return self.refill_units_per_second / _WEIGHT_SCALE

    @property
    def tokens(self) -> float:
        return self.token_units / _WEIGHT_SCALE

    def _refill(self, now: float) -> None:
        elapsed = max(0.0, now - self.updated_at)
        added_units = int(elapsed * self.refill_units_per_second)
        if added_units > 0:
            self.token_units = min(
                self.capacity_units,
                self.token_units + added_units,
            )
            if self.token_units >= self.capacity_units:
                self.updated_at = now
            else:
                self.updated_at += added_units / self.refill_units_per_second

    def delay_for_token(self, now: float, amount: float = 1.0) -> float:
        amount = _normalized_weight(amount, capacity=self.capacity)
        amount_units = int(round(amount * _WEIGHT_SCALE))
        self._refill(now)
        if self.token_units >= amount_units:
            return 0.0
        return (
            amount_units - self.token_units
        ) / self.refill_units_per_second

    def consume(self, now: float, amount: float = 1.0) -> None:
        amount = _normalized_weight(amount, capacity=self.capacity)
        amount_units = int(round(amount * _WEIGHT_SCALE))
        self._refill(now)
        self.token_units = max(0, self.token_units - amount_units)


class AdmissionPermit:
    def __init__(
        self,
        controller: "NotionAdmissionController",
        *,
        account_key: str,
        thread_key: str,
        idempotency_key: str,
        receipt: AdmissionReceipt,
        shared_lease_id: str = "",
    ) -> None:
        self._controller = controller
        self.account_key = account_key
        self.thread_key = thread_key
        self.idempotency_key = idempotency_key
        self.receipt = receipt
        self.shared_lease_id = str(shared_lease_id or "")
        self._released = False
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None
        if self.shared_lease_id:
            self._heartbeat_thread = threading.Thread(
                target=self._controller._heartbeat_loop,
                args=(self.shared_lease_id, self._heartbeat_stop),
                name=f"notion-admission-heartbeat-{self.shared_lease_id[:8]}",
                daemon=True,
            )
            self._heartbeat_thread.start()

    def release(
        self,
        *,
        success: bool = True,
        completion: dict[str, Any] | None = None,
    ) -> None:
        if self._released:
            return
        self._released = True
        self._heartbeat_stop.set()
        self._controller.release(self, success=success, completion=completion)

    def __enter__(self) -> "AdmissionPermit":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        self.release(success=exc is None)


class NotionAdmissionController:
    """Process-wide admission controller shared by every fresh Notion client."""

    def __init__(
        self,
        *,
        clock: Callable[[], float] = time.monotonic,
        sleeper: Callable[[float], None] = time.sleep,
        random_source: Callable[[], float] = random.random,
        shared_store: SharedAdmissionStore | bool | None = None,
    ) -> None:
        self._clock = clock
        self._sleep = sleeper
        self._random = random_source
        self._shared_store = (
            None
            if shared_store is False
            else shared_store
            if isinstance(shared_store, SharedAdmissionStore)
            else SharedAdmissionStore()
        )
        self._owner_id = f"{os.getpid()}:{uuid.uuid4().hex}"
        self._condition = threading.Condition(threading.RLock())
        self._thread_queues: dict[str, deque[str]] = defaultdict(deque)
        self._account_queues: dict[str, deque[str]] = defaultdict(deque)
        self._thread_active: set[str] = set()
        self._account_inflight: dict[str, int] = defaultdict(int)
        self._active_idempotency: set[str] = set()
        self._recent_idempotency: dict[str, float] = {}
        self._buckets: dict[str, _TokenBucket] = {}
        self._last_receipts: deque[dict[str, Any]] = deque(maxlen=200)
        self._counters: dict[str, int] = defaultdict(int)

    @property
    def capacity(self) -> float:
        return _env_float("NOTION_ADMISSION_ACCOUNT_CAPACITY", 2.0, minimum=1.0)

    @property
    def refill_per_second(self) -> float:
        return _env_float("NOTION_ADMISSION_ACCOUNT_REFILL_PER_SECOND", 0.5, minimum=0.001)

    @property
    def max_account_inflight(self) -> int:
        # Default raised for multi-bee fleets per account; thread_key still
        # enforces one in-flight Notion operation per conversation/thread.
        return _env_int("NOTION_ADMISSION_ACCOUNT_MAX_INFLIGHT", 4, minimum=1)

    @property
    def queue_timeout(self) -> float:
        return _env_float("NOTION_ADMISSION_QUEUE_TIMEOUT_SECONDS", 180.0, minimum=0.1)

    @property
    def idempotency_ttl(self) -> float:
        return _env_float("NOTION_ADMISSION_IDEMPOTENCY_TTL_SECONDS", 30.0, minimum=0.0)

    @property
    def lease_seconds(self) -> float:
        return _env_float("NOTION_ADMISSION_LEASE_SECONDS", 180.0, minimum=5.0)

    @property
    def heartbeat_seconds(self) -> float:
        configured = _env_float(
            "NOTION_ADMISSION_HEARTBEAT_SECONDS",
            min(30.0, self.lease_seconds / 3.0),
            minimum=1.0,
        )
        return min(configured, max(1.0, self.lease_seconds / 2.0))

    def _heartbeat_loop(self, lease_id: str, stop_event: threading.Event) -> None:
        store = self._shared_store
        if store is None:
            return
        while not stop_event.wait(self.heartbeat_seconds):
            try:
                if not store.heartbeat(lease_id, lease_seconds=self.lease_seconds):
                    return
            except Exception:
                self._counters["heartbeat_failures"] += 1

    def _acquire_shared(
        self,
        *,
        account_key: str,
        thread_key: str,
        idempotency_key: str,
        operation: str,
        started: float,
        deadline: float,
        admission_weight: float,
    ) -> tuple[str, int, int, float]:
        store = self._shared_store
        if store is None:
            return "", 0, 0, 0.0
        ticket_id = store.register_waiter(
            account_key=account_key,
            thread_key=thread_key,
            idempotency_key=idempotency_key,
            owner_id=self._owner_id,
            operation=operation,
            timeout_seconds=max(0.1, deadline - self._clock()),
        )
        max_account_depth = 0
        max_thread_depth = 0
        throttled_seconds = 0.0
        try:
            while True:
                now = self._clock()
                if now >= deadline:
                    store.cancel_waiter(ticket_id, reason="timeout")
                    self._counters["timeouts"] += 1
                    raise AdmissionTimeoutError(
                        f"Timed out waiting for shared Notion admission after {now - started:.3f}s"
                    )
                result = store.try_acquire(
                    ticket_id=ticket_id,
                    capacity=self.capacity,
                    refill_per_second=self.refill_per_second,
                    max_account_inflight=self.max_account_inflight,
                    lease_seconds=self.lease_seconds,
                    waited_seconds=max(0.0, now - started),
                    admission_weight=admission_weight,
                )
                max_account_depth = max(
                    max_account_depth, result.account_queue_depth
                )
                max_thread_depth = max(max_thread_depth, result.thread_queue_depth)
                if result.status == "acquired":
                    return (
                        result.lease_id,
                        max_account_depth,
                        max_thread_depth,
                        throttled_seconds,
                    )
                if result.status == "duplicate":
                    self._counters["duplicates_rejected"] += 1
                    raise DuplicateAdmissionError(
                        f"Duplicate Notion operation rejected: {idempotency_key}"
                    )
                if result.status == "missing":
                    raise AdmissionError("Shared Notion admission waiter disappeared")
                if result.status == "invalid":
                    raise AdmissionError(
                        f"Invalid Notion admission request: {result.reason or 'unknown'}"
                    )
                delay = min(
                    max(0.01, float(result.retry_after_seconds or 0.05)),
                    max(0.01, deadline - now),
                )
                if result.status == "throttled":
                    self._counters["throttled"] += 1
                    self._counters["throttle_wait_events"] += 1
                    throttled_seconds += delay
                else:
                    self._counters["queue_wait_events"] += 1
                self._sleep(delay)
        except Exception:
            try:
                store.cancel_waiter(ticket_id, reason="error")
            except Exception:
                pass
            raise

    def _prune_recent_unlocked(self, now: float) -> None:
        expired = [key for key, deadline in self._recent_idempotency.items() if deadline <= now]
        for key in expired:
            self._recent_idempotency.pop(key, None)

    def acquire(
        self,
        *,
        workspace_id: str,
        user_id: str,
        thread_id: str = "",
        idempotency_key: str = "",
        operation: str = "notion_request",
        timeout_seconds: float | None = None,
        attempt_id: str = "",
        workload_class: str = "legacy",
        admission_weight: float = 1.0,
        trace_id: str = "",
        request_context_id: str = "",
        model_id: str = "",
        request_bytes: int = 0,
        estimated_input_tokens: int = 0,
    ) -> AdmissionPermit:
        account_key = f"{_normalized_id(workspace_id)}:{_normalized_id(user_id)}"
        if account_key == ":":
            raise AdmissionError("workspace_id and user_id are required for admission")
        normalized_thread = _normalized_id(thread_id)
        thread_key = f"{account_key}:{normalized_thread}" if normalized_thread else ""
        normalized_idempotency = str(idempotency_key or "").strip()
        admission_weight = _normalized_weight(
            admission_weight,
            capacity=self.capacity,
        )
        attempt_id = str(attempt_id or uuid.uuid4().hex)
        ticket = f"{threading.get_ident()}:{time.time_ns()}"
        self._counters["queue_entries"] += 1
        started = self._clock()
        deadline = started + (self.queue_timeout if timeout_seconds is None else timeout_seconds)
        shared_lease_id = ""
        shared_account_depth = 0
        shared_thread_depth = 0
        throttled_seconds = 0.0
        if self._shared_store is not None:
            (
                shared_lease_id,
                shared_account_depth,
                shared_thread_depth,
                throttled_seconds,
            ) = self._acquire_shared(
                account_key=account_key,
                thread_key=thread_key,
                idempotency_key=normalized_idempotency,
                operation=operation,
                started=started,
                deadline=deadline,
                admission_weight=admission_weight,
            )

        with self._condition:
            now = self._clock()
            self._prune_recent_unlocked(now)
            if normalized_idempotency and (
                normalized_idempotency in self._active_idempotency
                or normalized_idempotency in self._recent_idempotency
            ):
                self._counters["duplicates_rejected"] += 1
                raise DuplicateAdmissionError(
                    f"Duplicate Notion operation rejected: {normalized_idempotency}"
                )
            if normalized_idempotency:
                self._active_idempotency.add(normalized_idempotency)
            self._account_queues[account_key].append(ticket)
            if thread_key:
                self._thread_queues[thread_key].append(ticket)
            account_queue_depth = (
                len(self._account_queues[account_key])
                - 1
                + self._account_inflight[account_key]
            )
            thread_queue_depth = (
                len(self._thread_queues[thread_key])
                - 1
                + (1 if thread_key in self._thread_active else 0)
                if thread_key
                else 0
            )
            account_queue_depth = max(account_queue_depth, shared_account_depth)
            thread_queue_depth = max(thread_queue_depth, shared_thread_depth)
            if account_queue_depth or thread_queue_depth:
                self._counters["queued_unique_jobs"] += 1
                # Backward-compatible alias: queued now means unique queued jobs.
                self._counters["queued"] += 1

            try:
                while True:
                    now = self._clock()
                    account_head = self._account_queues[account_key][0] == ticket
                    thread_head = (
                        not thread_key or self._thread_queues[thread_key][0] == ticket
                    )
                    account_available = (
                        self._account_inflight[account_key] < self.max_account_inflight
                    )
                    thread_available = not thread_key or thread_key not in self._thread_active
                    bucket = self._buckets.get(account_key)
                    token_delay = 0.0
                    if self._shared_store is None:
                        if bucket is None:
                            bucket = _TokenBucket(self.capacity, self.refill_per_second, now)
                            self._buckets[account_key] = bucket
                        token_delay = bucket.delay_for_token(now, admission_weight)

                    if account_head and thread_head and account_available and thread_available:
                        if token_delay > 0:
                            if now >= deadline:
                                self._counters["timeouts"] += 1
                                raise AdmissionTimeoutError(
                                    f"Timed out waiting for Notion admission after {now - started:.3f}s"
                                )
                            self._counters["throttled"] += 1
                            self._counters["throttle_wait_events"] += 1
                            sleep_for = min(token_delay, max(0.01, deadline - now))
                            throttled_seconds += sleep_for
                            self._condition.wait(timeout=sleep_for)
                            continue
                        waited = max(0.0, self._clock() - started)
                        disposition = (
                            "throttled"
                            if throttled_seconds > 0
                            else "queued"
                            if account_queue_depth or thread_queue_depth
                            else "admitted"
                        )
                        receipt = AdmissionReceipt(
                            disposition=disposition,
                            account_key=account_key,
                            thread_key=thread_key,
                            idempotency_key=normalized_idempotency,
                            queue_depth=thread_queue_depth,
                            account_queue_depth=account_queue_depth,
                            waited_seconds=waited,
                            throttled_seconds=throttled_seconds,
                            admitted_at=time.time(),
                            operation=operation,
                            attempt_id=attempt_id,
                            workload_class=str(workload_class or "legacy"),
                            admission_weight=admission_weight,
                            trace_id=str(trace_id or ""),
                            request_context_id=str(request_context_id or ""),
                            model_id=str(model_id or ""),
                            request_bytes=max(0, int(request_bytes or 0)),
                            estimated_input_tokens=max(
                                0, int(estimated_input_tokens or 0)
                            ),
                        )
                        try:
                            _REQUEST_TELEMETRY.start(receipt.as_dict())
                        except UsageQuotaExceededError:
                            self._counters["quota_rejected"] += 1
                            raise
                        except Exception:
                            self._counters["telemetry_start_failures"] += 1
                        if bucket is not None:
                            bucket.consume(now, admission_weight)
                        self._account_queues[account_key].popleft()
                        if not self._account_queues[account_key]:
                            self._account_queues.pop(account_key, None)
                        if thread_key:
                            self._thread_queues[thread_key].popleft()
                            if not self._thread_queues[thread_key]:
                                self._thread_queues.pop(thread_key, None)
                            self._thread_active.add(thread_key)
                        self._account_inflight[account_key] += 1
                        self._counters["admitted"] += 1
                        self._last_receipts.append(receipt.as_dict())
                        return AdmissionPermit(
                            self,
                            account_key=account_key,
                            thread_key=thread_key,
                            idempotency_key=normalized_idempotency,
                            receipt=receipt,
                            shared_lease_id=shared_lease_id,
                        )
                    if now >= deadline:
                        self._counters["timeouts"] += 1
                        raise AdmissionTimeoutError(
                            f"Timed out waiting for Notion admission after {now - started:.3f}s"
                        )
                    remaining = max(0.01, deadline - now)
                    self._counters["queue_wait_events"] += 1
                    self._condition.wait(timeout=min(0.25, remaining))
            except Exception:
                account_queue = self._account_queues.get(account_key)
                if account_queue and ticket in account_queue:
                    account_queue.remove(ticket)
                    if not account_queue:
                        self._account_queues.pop(account_key, None)
                if thread_key:
                    thread_queue = self._thread_queues.get(thread_key)
                    if thread_queue and ticket in thread_queue:
                        thread_queue.remove(ticket)
                        if not thread_queue:
                            self._thread_queues.pop(thread_key, None)
                if normalized_idempotency:
                    self._active_idempotency.discard(normalized_idempotency)
                if shared_lease_id and self._shared_store is not None:
                    try:
                        self._shared_store.release(
                            shared_lease_id,
                            success=False,
                            idempotency_ttl=0.0,
                            waited_seconds=max(0.0, self._clock() - started),
                        )
                    except Exception:
                        pass
                self._condition.notify_all()
                raise

    def release(
        self,
        permit: AdmissionPermit,
        *,
        success: bool,
        completion: dict[str, Any] | None = None,
    ) -> None:
        completion = dict(completion or {})
        completion["error_class"] = _bounded_error_class(
            completion.get("error_class")
        )
        completion.setdefault("retry_count", permit.receipt.retry_count)
        completion.setdefault(
            "retry_after_seconds", permit.receipt.retry_after_seconds
        )
        try:
            permit.receipt.completion = _REQUEST_TELEMETRY.finish(
                permit.receipt.attempt_id,
                success=success,
                status_code=completion.get("status_code"),
                response_bytes=max(0, int(completion.get("response_bytes") or 0)),
                estimated_output_tokens=max(
                    0, int(completion.get("estimated_output_tokens") or 0)
                ),
                actual_input_tokens=completion.get("actual_input_tokens"),
                actual_output_tokens=completion.get("actual_output_tokens"),
                actual_total_tokens=completion.get("actual_total_tokens"),
                retry_count=max(0, int(completion.get("retry_count") or 0)),
                retry_after_seconds=max(
                    0.0, float(completion.get("retry_after_seconds") or 0.0)
                ),
                error_class=_bounded_error_class(completion.get("error_class")),
            )
        except Exception:
            self._counters["telemetry_finish_failures"] += 1
            permit.receipt.completion = {
                "outcome": "succeeded" if success else "failed",
                **completion,
            }
        if permit.shared_lease_id and self._shared_store is not None:
            try:
                self._shared_store.release(
                    permit.shared_lease_id,
                    success=success,
                    idempotency_ttl=self.idempotency_ttl,
                    waited_seconds=permit.receipt.waited_seconds,
                )
            except Exception:
                self._counters["shared_release_failures"] += 1
        with self._condition:
            self._account_inflight[permit.account_key] = max(
                0, self._account_inflight[permit.account_key] - 1
            )
            if self._account_inflight[permit.account_key] == 0:
                self._account_inflight.pop(permit.account_key, None)
            if permit.thread_key:
                self._thread_active.discard(permit.thread_key)
            if permit.idempotency_key:
                self._active_idempotency.discard(permit.idempotency_key)
                if success and self.idempotency_ttl > 0:
                    self._recent_idempotency[permit.idempotency_key] = (
                        self._clock() + self.idempotency_ttl
                    )
            self._counters["completed" if success else "failed"] += 1
            self._last_receipts.append(permit.receipt.as_dict())
            self._condition.notify_all()

    def note_retry(
        self,
        permit: AdmissionPermit,
        *,
        retry_after_seconds: float,
    ) -> None:
        receipt = permit.receipt
        with self._condition:
            receipt.retry_count += 1
            receipt.retry_after_seconds += max(0.0, retry_after_seconds)
            receipt.disposition = "retry_after"
            self._counters["retry_after"] += 1
            self._last_receipts.append(receipt.as_dict())
        try:
            _REQUEST_TELEMETRY.note_retry(
                receipt.attempt_id,
                retry_count=receipt.retry_count,
                retry_after_seconds=receipt.retry_after_seconds,
            )
        except Exception:
            self._counters["telemetry_retry_failures"] += 1
        if permit.shared_lease_id and self._shared_store is not None:
            try:
                self._shared_store.note_retry(
                    permit.shared_lease_id,
                    retry_after_seconds=retry_after_seconds,
                    retry_count=receipt.retry_count,
                )
            except Exception:
                self._counters["shared_retry_receipt_failures"] += 1

    def snapshot(self) -> dict[str, Any]:
        with self._condition:
            now = self._clock()
            self._prune_recent_unlocked(now)
            counters = dict(self._counters)
            for key in (
                "queue_entries",
                "queued_unique_jobs",
                "queue_wait_events",
                "throttle_wait_events",
                "admitted",
                "completed",
                "failed",
                "timeouts",
            ):
                counters.setdefault(key, 0)
            counters.setdefault("queued", counters["queued_unique_jobs"])
            local = {
                "enabled": True,
                "account_capacity": self.capacity,
                "account_refill_per_second": self.refill_per_second,
                "account_max_inflight": self.max_account_inflight,
                "active_accounts": dict(self._account_inflight),
                "active_threads": len(self._thread_active),
                "account_queue_depth": sum(len(queue) for queue in self._account_queues.values()),
                "thread_queue_depth": sum(len(queue) for queue in self._thread_queues.values()),
                "active_idempotency_keys": len(self._active_idempotency),
                "recent_idempotency_keys": len(self._recent_idempotency),
                "metric_schema_version": 2,
                "counter_semantics": {
                    "queue_entries": "admission requests entering the controller",
                    "queued_unique_jobs": "distinct requests that observed queue depth",
                    "queued": "backward-compatible alias of queued_unique_jobs",
                    "queue_wait_events": "queue/recheck observations while waiting",
                    "throttle_wait_events": "token-bucket or provider throttle waits",
                    "admitted": "requests granted an admission permit",
                    "completed": "admitted requests released successfully",
                    "failed": "admitted requests released unsuccessfully",
                    "timeouts": "requests that exceeded admission timeout",
                },
                "counters": counters,
                "recent_receipts": list(self._last_receipts)[-20:],
            }
        try:
            local["request_telemetry"] = _REQUEST_TELEMETRY.snapshot()
        except Exception as exc:
            local["request_telemetry"] = {
                "healthy": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        if self._shared_store is None:
            local["cross_process"] = {"enabled": False}
            return local
        try:
            local["cross_process"] = self._shared_store.snapshot()
        except Exception as exc:
            local["cross_process"] = {
                "enabled": True,
                "healthy": False,
                "error": f"{type(exc).__name__}: {exc}",
            }
        return local


class AdmittedResponse:
    """Response proxy that retains its admission permit through stream consumption."""

    def __init__(
        self,
        response: Any,
        permit: AdmissionPermit,
        owner: Any,
    ) -> None:
        self._response = response
        self._permit = permit
        self._owner = owner
        self._released = False
        self._response_bytes = 0

    def _count_chunk(self, chunk: Any, *, line: bool = False) -> None:
        if isinstance(chunk, bytes):
            size = len(chunk)
        elif isinstance(chunk, str):
            size = len(chunk.encode("utf-8", errors="replace"))
        else:
            size = 0
        self._response_bytes += size + (1 if line and size else 0)

    def _release(self, *, success: bool | None = None) -> None:
        if self._released:
            return
        status_code = int(getattr(self._response, "status_code", 0) or 0)
        if success is None:
            success = 200 <= status_code < 400
        actual_input, actual_output, actual_total = _actual_usage(self._response)
        completion = {
            "status_code": status_code or None,
            "response_bytes": self._response_bytes,
            "estimated_output_tokens": _estimated_tokens(self._response_bytes),
            "actual_input_tokens": actual_input,
            "actual_output_tokens": actual_output,
            "actual_total_tokens": actual_total,
            "error_class": "" if success else f"http_{status_code or 'unknown'}",
        }
        self._released = True
        self._permit.release(success=bool(success), completion=completion)
        setattr(self._owner, "last_admission_receipt", self._permit.receipt.as_dict())
        setattr(
            self._owner,
            "last_request_telemetry",
            dict(self._permit.receipt.completion),
        )

    def iter_lines(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        completed = False
        try:
            for chunk in self._response.iter_lines(*args, **kwargs):
                self._count_chunk(chunk, line=True)
                yield chunk
            completed = True
        finally:
            self._release(success=completed)

    def iter_content(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        completed = False
        try:
            for chunk in self._response.iter_content(*args, **kwargs):
                self._count_chunk(chunk)
                yield chunk
            completed = True
        finally:
            self._release(success=completed)

    @property
    def text(self) -> Any:
        value = self._response.text
        if int(getattr(self._response, "status_code", 0) or 0) >= 400:
            self._release(success=False)
        return value

    @property
    def content(self) -> Any:
        value = self._response.content
        if int(getattr(self._response, "status_code", 0) or 0) >= 400:
            self._release(success=False)
        return value

    def close(self) -> None:
        try:
            self._response.close()
        finally:
            self._release(success=False)

    def __enter__(self) -> "AdmittedResponse":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            self._response.close()
        finally:
            self._release(success=False)

    def __del__(self) -> None:
        try:
            self._release(success=False)
        except Exception:
            pass

    def __getattr__(self, name: str) -> Any:
        return getattr(self._response, name)


class AdmittedSession:
    """HTTP session proxy applying shared admission and rate-limit retry handling."""

    def __init__(
        self,
        session: Any,
        owner: Any,
        controller: NotionAdmissionController,
    ) -> None:
        object.__setattr__(self, "_session", session)
        object.__setattr__(self, "_owner", owner)
        object.__setattr__(self, "_controller", controller)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._session, name)

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith("_"):
            object.__setattr__(self, name, value)
        else:
            setattr(self._session, name, value)

    def _operation_name(self, method: str, url: str) -> str:
        path = urlparse(str(url or "")).path.rstrip("/")
        return f"{method.upper()} {path or '/'}"

    def _idempotency_key(self, method: str, url: str, payload: Any) -> str:
        path = urlparse(str(url or "")).path.rstrip("/").casefold()
        read_like_endpoints = (
            "/gettasks",
            "/getavailablemodels",
            "/getrecordvalues",
            "/getdownloadfileurl",
            "/getspaces",
            "/loadpagechunk",
            "/loadcachedpagechunk",
            "/search",
        )
        if method.upper() == "GET" or path.endswith(read_like_endpoints):
            return ""

        explicit = str(
            getattr(self._owner, "request_idempotency_key", "") or ""
        ).strip()
        if not explicit and isinstance(payload, dict):
            for key in ("idempotencyKey", "idempotency_key", "requestFingerprint"):
                value = str(payload.get(key) or "").strip()
                if value:
                    explicit = value
                    break
        if not explicit:
            return ""

        payload_scope = payload
        if isinstance(payload, dict):
            payload_scope = {
                key: value
                for key, value in payload.items()
                if key not in {"traceId", "requestId", "request_id"}
            }
        operation_scope = f"{method.upper()}:{path}:{_stable_payload_fingerprint(payload_scope)}"
        return f"{explicit}:{operation_scope}"

    def request(self, method: str, url: str, **kwargs: Any) -> Any:
        payload = kwargs.get("json")
        thread_id = _extract_thread_id(
            payload,
            fallback=str(getattr(self._owner, "current_thread_id", "") or ""),
        )
        operation = self._operation_name(method, url)
        idempotency_key = self._idempotency_key(method, url, payload)
        request_bytes = _safe_json_size_bytes(payload)
        estimated_input_tokens = _estimated_tokens(request_bytes)
        workload_class, admission_weight = _workload_profile(
            operation,
            estimated_input_tokens=estimated_input_tokens,
            capacity=self._controller.capacity,
        )
        trace_id, request_context_id = _request_lineage(payload, self._owner)
        trace_id = _bounded_identifier(trace_id)
        request_context_id = _bounded_identifier(request_context_id)
        model_id = _bounded_identifier(_extract_model_id(payload, self._owner))
        permit = self._controller.acquire(
            workspace_id=str(getattr(self._owner, "space_id", "") or ""),
            user_id=str(getattr(self._owner, "user_id", "") or ""),
            thread_id=thread_id,
            idempotency_key=idempotency_key,
            operation=operation,
            attempt_id=uuid.uuid4().hex,
            workload_class=workload_class,
            admission_weight=admission_weight,
            trace_id=trace_id,
            request_context_id=request_context_id,
            model_id=model_id,
            request_bytes=request_bytes,
            estimated_input_tokens=estimated_input_tokens,
        )
        setattr(self._owner, "last_admission_receipt", permit.receipt.as_dict())
        stream = bool(kwargs.get("stream"))
        max_retries = _env_int("NOTION_ADMISSION_MAX_429_RETRIES", 2, minimum=0)
        base_backoff = _env_float("NOTION_ADMISSION_BACKOFF_BASE_SECONDS", 2.0, minimum=0.0)
        jitter_ratio = _env_float("NOTION_ADMISSION_BACKOFF_JITTER_RATIO", 0.25, minimum=0.0)

        try:
            for attempt in range(max_retries + 1):
                raw_method = getattr(self._session, method.lower(), None)
                response = (
                    raw_method(url, **kwargs)
                    if callable(raw_method)
                    else self._session.request(method, url, **kwargs)
                )
                try:
                    status_code = int(getattr(response, "status_code", 0) or 0)
                except (TypeError, ValueError):
                    status_code = 0
                if status_code != 429 or attempt >= max_retries:
                    if stream:
                        return AdmittedResponse(response, permit, self._owner)
                    success = status_code < 500 and status_code != 429
                    response_bytes = _response_size_bytes(response)
                    actual_input, actual_output, actual_total = _actual_usage(response)
                    permit.release(
                        success=success,
                        completion={
                            "status_code": status_code or None,
                            "response_bytes": response_bytes,
                            "estimated_output_tokens": _estimated_tokens(response_bytes),
                            "actual_input_tokens": actual_input,
                            "actual_output_tokens": actual_output,
                            "actual_total_tokens": actual_total,
                            "error_class": ""
                            if success
                            else f"http_{status_code or 'unknown'}",
                        },
                    )
                    setattr(
                        self._owner,
                        "last_admission_receipt",
                        permit.receipt.as_dict(),
                    )
                    setattr(
                        self._owner,
                        "last_request_telemetry",
                        dict(permit.receipt.completion),
                    )
                    return response

                retry_after = _retry_after_seconds(response)
                exponential = base_backoff * (2**attempt)
                delay = retry_after if retry_after is not None else exponential
                delay += delay * jitter_ratio * self._controller._random()
                try:
                    response.close()
                except Exception:
                    pass
                self._controller.note_retry(
                    permit,
                    retry_after_seconds=delay,
                )
                setattr(self._owner, "last_admission_receipt", permit.receipt.as_dict())
                self._controller._sleep(delay)
        except Exception as exc:
            permit.release(
                success=False,
                completion={"error_class": type(exc).__name__},
            )
            setattr(self._owner, "last_admission_receipt", permit.receipt.as_dict())
            setattr(
                self._owner,
                "last_request_telemetry",
                dict(permit.receipt.completion),
            )
            raise

    def get(self, url: str, **kwargs: Any) -> Any:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> Any:
        return self.request("POST", url, **kwargs)

    def put(self, url: str, **kwargs: Any) -> Any:
        return self.request("PUT", url, **kwargs)

    def patch(self, url: str, **kwargs: Any) -> Any:
        return self.request("PATCH", url, **kwargs)

    def delete(self, url: str, **kwargs: Any) -> Any:
        return self.request("DELETE", url, **kwargs)


_GLOBAL_CONTROLLER = NotionAdmissionController()


def get_notion_admission_controller() -> NotionAdmissionController:
    return _GLOBAL_CONTROLLER


def get_notion_usage_store() -> NotionRequestTelemetryStore:
    """Return the canonical durable usage/quota store used by admission."""
    return _REQUEST_TELEMETRY


def admitted_session(session: Any, owner: Any) -> AdmittedSession:
    return AdmittedSession(session, owner, _GLOBAL_CONTROLLER)


def derive_operation_idempotency_key(
    *,
    caller_key: str,
    operation: str,
    payload: Any,
) -> str:
    caller = str(caller_key or "").strip()
    if not caller:
        return ""
    return f"{caller}:{operation}:{_stable_payload_fingerprint(payload)}"
