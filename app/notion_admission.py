from __future__ import annotations

import hashlib
import json
import os
import random
import threading
import time
import uuid
from collections import defaultdict, deque
from dataclasses import dataclass
from typing import Any, Callable, Iterator
from urllib.parse import urlparse

from app.notion_admission_store import SharedAdmissionStore


class AdmissionError(RuntimeError):
    """Base error raised before an upstream Notion request is admitted."""


class DuplicateAdmissionError(AdmissionError):
    """Raised when an idempotency key is already active or recently completed."""


class AdmissionTimeoutError(AdmissionError):
    """Raised when an operation cannot enter the keyed queue before its deadline."""


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
        }


class _TokenBucket:
    def __init__(self, capacity: float, refill_per_second: float, now: float) -> None:
        self.capacity = max(1.0, capacity)
        self.refill_per_second = max(0.001, refill_per_second)
        self.tokens = self.capacity
        self.updated_at = now

    def delay_for_token(self, now: float) -> float:
        elapsed = max(0.0, now - self.updated_at)
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_per_second)
        self.updated_at = now
        if self.tokens >= 1.0:
            return 0.0
        return (1.0 - self.tokens) / self.refill_per_second

    def consume(self, now: float) -> None:
        self.delay_for_token(now)
        self.tokens = max(0.0, self.tokens - 1.0)


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

    def release(self, *, success: bool = True) -> None:
        if self._released:
            return
        self._released = True
        self._heartbeat_stop.set()
        self._controller.release(self, success=success)

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
        return _env_int("NOTION_ADMISSION_ACCOUNT_MAX_INFLIGHT", 1, minimum=1)

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
    ) -> AdmissionPermit:
        account_key = f"{_normalized_id(workspace_id)}:{_normalized_id(user_id)}"
        if account_key == ":":
            raise AdmissionError("workspace_id and user_id are required for admission")
        normalized_thread = _normalized_id(thread_id)
        thread_key = f"{account_key}:{normalized_thread}" if normalized_thread else ""
        normalized_idempotency = str(idempotency_key or "").strip()
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
                    if now >= deadline:
                        self._counters["timeouts"] += 1
                        raise AdmissionTimeoutError(
                            f"Timed out waiting for Notion admission after {now - started:.3f}s"
                        )
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
                        token_delay = bucket.delay_for_token(now)

                    if account_head and thread_head and account_available and thread_available:
                        if token_delay > 0:
                            self._counters["throttled"] += 1
                            self._counters["throttle_wait_events"] += 1
                            sleep_for = min(token_delay, max(0.01, deadline - now))
                            throttled_seconds += sleep_for
                            self._condition.wait(timeout=sleep_for)
                            continue
                        if bucket is not None:
                            bucket.consume(now)
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
                        )
                        self._last_receipts.append(receipt.as_dict())
                        return AdmissionPermit(
                            self,
                            account_key=account_key,
                            thread_key=thread_key,
                            idempotency_key=normalized_idempotency,
                            receipt=receipt,
                            shared_lease_id=shared_lease_id,
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

    def release(self, permit: AdmissionPermit, *, success: bool) -> None:
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

    def __init__(self, response: Any, permit: AdmissionPermit) -> None:
        self._response = response
        self._permit = permit
        self._released = False

    def _release(self, *, success: bool | None = None) -> None:
        if self._released:
            return
        if success is None:
            status_code = int(getattr(self._response, "status_code", 0) or 0)
            success = 200 <= status_code < 400
        self._released = True
        self._permit.release(success=bool(success))

    def iter_lines(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        try:
            yield from self._response.iter_lines(*args, **kwargs)
            self._release(success=True)
        except Exception:
            self._release(success=False)
            raise

    def iter_content(self, *args: Any, **kwargs: Any) -> Iterator[Any]:
        try:
            yield from self._response.iter_content(*args, **kwargs)
            self._release(success=True)
        except Exception:
            self._release(success=False)
            raise

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
            self._release(success=None)

    def __enter__(self) -> "AdmittedResponse":
        return self

    def __exit__(self, exc_type: Any, exc: Any, traceback: Any) -> None:
        try:
            self._response.close()
        finally:
            self._release(success=False if exc is not None else None)

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
        permit = self._controller.acquire(
            workspace_id=str(getattr(self._owner, "space_id", "") or ""),
            user_id=str(getattr(self._owner, "user_id", "") or ""),
            thread_id=thread_id,
            idempotency_key=idempotency_key,
            operation=operation,
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
                        return AdmittedResponse(response, permit)
                    permit.release(success=status_code < 500 and status_code != 429)
                    setattr(self._owner, "last_admission_receipt", permit.receipt.as_dict())
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
        except Exception:
            permit.release(success=False)
            setattr(self._owner, "last_admission_receipt", permit.receipt.as_dict())
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
