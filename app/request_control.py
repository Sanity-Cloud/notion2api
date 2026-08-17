from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from collections import deque
from dataclasses import dataclass
from functools import wraps
from typing import Any, Awaitable, Callable

from fastapi import HTTPException, Request
from fastapi.responses import JSONResponse, StreamingResponse

from app.logger import logger


def _env_int(name: str, default: int, minimum: int, maximum: int) -> int:
    raw = os.getenv(name)
    try:
        value = int(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


def _env_float(name: str, default: float, minimum: float, maximum: float) -> float:
    raw = os.getenv(name)
    try:
        value = float(raw) if raw is not None else default
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


class AdmissionRejected(RuntimeError):
    def __init__(self, code: str, message: str, *, status_code: int, retry_after: int = 1):
        super().__init__(message)
        self.code = code
        self.status_code = status_code
        self.retry_after = retry_after


@dataclass
class RequestLease:
    controller: "RequestController"
    fingerprint: str
    conversation_key: str
    key_lock: asyncio.Lock
    released: bool = False

    async def release(self) -> None:
        if not self.released:
            self.released = True
            await self.controller._release(self)


class RequestController:
    """Bounded admission control for expensive Notion inference requests."""

    def __init__(
        self,
        *,
        max_concurrency: int = 3,
        queue_timeout_seconds: float = 30.0,
        failure_threshold: int = 4,
        failure_window_seconds: float = 60.0,
        recovery_seconds: float = 60.0,
    ) -> None:
        self.max_concurrency = max(1, max_concurrency)
        self.queue_timeout_seconds = max(0.01, queue_timeout_seconds)
        self.failure_threshold = max(1, failure_threshold)
        self.failure_window_seconds = max(0.01, failure_window_seconds)
        self.recovery_seconds = max(0.01, recovery_seconds)
        self._semaphore = asyncio.Semaphore(self.max_concurrency)
        self._state_lock = asyncio.Lock()
        self._conversation_locks: dict[str, asyncio.Lock] = {}
        self._conversation_refs: dict[str, int] = {}
        self._active_fingerprints: set[str] = set()
        self._failures: deque[float] = deque()
        self._open_until = 0.0
        self.active = 0
        self.admitted_total = 0
        self.rejected_total = 0
        self.duplicate_rejected_total = 0
        self.queue_timeout_total = 0
        self.circuit_rejected_total = 0

    @classmethod
    def from_env(cls) -> "RequestController":
        return cls(
            max_concurrency=_env_int("NOTION_MAX_CONCURRENT_INFERENCE", 3, 1, 32),
            queue_timeout_seconds=_env_float("NOTION_INFERENCE_QUEUE_TIMEOUT_SECONDS", 30.0, 0.1, 300.0),
            failure_threshold=_env_int("NOTION_CIRCUIT_FAILURE_THRESHOLD", 4, 1, 50),
            failure_window_seconds=_env_float("NOTION_CIRCUIT_FAILURE_WINDOW_SECONDS", 60.0, 1.0, 900.0),
            recovery_seconds=_env_float("NOTION_CIRCUIT_RECOVERY_SECONDS", 60.0, 1.0, 900.0),
        )

    async def acquire(self, conversation_key: str, fingerprint: str) -> RequestLease:
        now = time.monotonic()
        async with self._state_lock:
            self._prune_failures(now)
            if now < self._open_until:
                self.rejected_total += 1
                self.circuit_rejected_total += 1
                raise AdmissionRejected(
                    "UPSTREAM_CIRCUIT_OPEN",
                    "Notion inference is temporarily paused after repeated upstream failures.",
                    status_code=503,
                    retry_after=max(1, int(self._open_until - now)),
                )
            if fingerprint in self._active_fingerprints:
                self.rejected_total += 1
                self.duplicate_rejected_total += 1
                raise AdmissionRejected(
                    "DUPLICATE_IN_FLIGHT",
                    "An identical inference request is already running.",
                    status_code=409,
                )
            self._active_fingerprints.add(fingerprint)
            key_lock = self._conversation_locks.setdefault(conversation_key, asyncio.Lock())
            self._conversation_refs[conversation_key] = self._conversation_refs.get(conversation_key, 0) + 1

        key_acquired = False
        global_acquired = False
        try:
            await asyncio.wait_for(key_lock.acquire(), timeout=self.queue_timeout_seconds)
            key_acquired = True
            await asyncio.wait_for(self._semaphore.acquire(), timeout=self.queue_timeout_seconds)
            global_acquired = True
        except (TimeoutError, asyncio.TimeoutError) as exc:
            if global_acquired:
                self._semaphore.release()
            if key_acquired and key_lock.locked():
                key_lock.release()
            async with self._state_lock:
                self._active_fingerprints.discard(fingerprint)
                self._drop_conversation_ref(conversation_key, key_lock)
                self.rejected_total += 1
                self.queue_timeout_total += 1
            raise AdmissionRejected(
                "INFERENCE_QUEUE_TIMEOUT",
                "Inference capacity is full; retry after active work completes.",
                status_code=429,
            ) from exc
        except BaseException:
            if global_acquired:
                self._semaphore.release()
            if key_acquired and key_lock.locked():
                key_lock.release()
            async with self._state_lock:
                self._active_fingerprints.discard(fingerprint)
                self._drop_conversation_ref(conversation_key, key_lock)
            raise

        async with self._state_lock:
            self.active += 1
            self.admitted_total += 1
        return RequestLease(self, fingerprint, conversation_key, key_lock)

    async def _release(self, lease: RequestLease) -> None:
        self._semaphore.release()
        if lease.key_lock.locked():
            lease.key_lock.release()
        async with self._state_lock:
            self.active = max(0, self.active - 1)
            self._active_fingerprints.discard(lease.fingerprint)
            self._drop_conversation_ref(lease.conversation_key, lease.key_lock)

    def _drop_conversation_ref(self, conversation_key: str, key_lock: asyncio.Lock) -> None:
        remaining = self._conversation_refs.get(conversation_key, 1) - 1
        if remaining <= 0:
            self._conversation_refs.pop(conversation_key, None)
            if not key_lock.locked():
                self._conversation_locks.pop(conversation_key, None)
        else:
            self._conversation_refs[conversation_key] = remaining

    async def record_failure(self) -> None:
        now = time.monotonic()
        async with self._state_lock:
            self._failures.append(now)
            self._prune_failures(now)
            if len(self._failures) >= self.failure_threshold:
                self._open_until = max(self._open_until, now + self.recovery_seconds)

    async def record_success(self) -> None:
        now = time.monotonic()
        async with self._state_lock:
            self._prune_failures(now)

    def _prune_failures(self, now: float) -> None:
        cutoff = now - self.failure_window_seconds
        while self._failures and self._failures[0] < cutoff:
            self._failures.popleft()
        if self._open_until and now >= self._open_until:
            self._open_until = 0.0
            self._failures.clear()

    async def snapshot(self) -> dict[str, Any]:
        now = time.monotonic()
        async with self._state_lock:
            self._prune_failures(now)
            return {
                "max_concurrency": self.max_concurrency,
                "active": self.active,
                "queued_conversations": sum(1 for lock in self._conversation_locks.values() if lock.locked()),
                "active_fingerprints": len(self._active_fingerprints),
                "admitted_total": self.admitted_total,
                "rejected_total": self.rejected_total,
                "duplicate_rejected_total": self.duplicate_rejected_total,
                "queue_timeout_total": self.queue_timeout_total,
                "circuit_rejected_total": self.circuit_rejected_total,
                "recent_failures": len(self._failures),
                "circuit_open": now < self._open_until,
                "circuit_retry_after_seconds": max(0, int(self._open_until - now)),
            }


def _hash_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def request_fingerprint(req_body: Any) -> str:
    if hasattr(req_body, "model_dump"):
        payload = req_body.model_dump(mode="json")
    elif hasattr(req_body, "dict"):
        payload = req_body.dict()
    else:
        payload = req_body
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def scoped_fingerprint(conversation_key: str, fingerprint: str) -> str:
    return _hash_text(f"{conversation_key}|{fingerprint}")


def safe_scope_label(conversation_key: str) -> str:
    return _hash_text(conversation_key)[:16]


def conversation_key(request: Request, req_body: Any, fingerprint: str) -> str:
    for value in (
        getattr(req_body, "conversation_id", None),
        getattr(req_body, "session_name", None),
        request.headers.get("x-session-name"),
        request.headers.get("x-conversation-id"),
    ):
        if str(value or "").strip():
            return str(value).strip()
    metadata = getattr(req_body, "metadata", None)
    if isinstance(metadata, dict):
        for key in ("session_name", "conversation_id", "caller_id"):
            if str(metadata.get(key) or "").strip():
                return str(metadata[key]).strip()
    return f"anonymous:{fingerprint}"


def _rejection_response(exc: AdmissionRejected) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        headers={"Retry-After": str(exc.retry_after)},
        content={
            "error": {
                "message": str(exc),
                "type": "request_control_error",
                "code": exc.code,
            }
        },
    )


def controlled_chat_request(func: Callable[..., Awaitable[Any]]) -> Callable[..., Awaitable[Any]]:
    @wraps(func)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        request = kwargs.get("request")
        req_body = kwargs.get("req_body")
        if request is None:
            request = next(
                (
                    arg
                    for arg in args
                    if isinstance(arg, Request)
                    or (hasattr(arg, "app") and hasattr(arg, "headers"))
                ),
                None,
            )
        if req_body is None and len(args) > 1:
            req_body = args[1]
        app = getattr(request, "app", None) if request else None
        controller = getattr(getattr(app, "state", None), "request_control", None)
        if controller is None or req_body is None:
            return await func(*args, **kwargs)

        base_fingerprint = request_fingerprint(req_body)
        key = conversation_key(request, req_body, base_fingerprint)
        fingerprint = scoped_fingerprint(key, base_fingerprint)
        try:
            lease = await controller.acquire(key, fingerprint)
        except AdmissionRejected as exc:
            logger.warning(
                "Inference request rejected by admission control",
                extra={"request_info": {"event": "inference_admission_rejected", "code": exc.code, "conversation_scope": safe_scope_label(key)}},
            )
            return _rejection_response(exc)

        try:
            result = await func(*args, **kwargs)
        except asyncio.CancelledError:
            await lease.release()
            raise
        except HTTPException as exc:
            if exc.status_code == 429 or exc.status_code >= 500:
                await controller.record_failure()
            await lease.release()
            raise
        except BaseException:
            await controller.record_failure()
            await lease.release()
            raise

        if isinstance(result, StreamingResponse):
            original_iterator = result.body_iterator

            async def controlled_iterator():
                failed = False
                try:
                    async for chunk in original_iterator:
                        yield chunk
                except asyncio.CancelledError:
                    failed = True
                    raise
                except BaseException:
                    failed = True
                    await controller.record_failure()
                    raise
                finally:
                    if not failed:
                        await controller.record_success()
                    await lease.release()

            result.body_iterator = controlled_iterator()
            result.headers["X-Request-Fingerprint"] = fingerprint[:16]
            return result

        status_code = int(getattr(result, "status_code", 200) or 200)
        if status_code == 429 or status_code >= 500:
            await controller.record_failure()
        else:
            await controller.record_success()
        await lease.release()
        if hasattr(result, "headers"):
            result.headers["X-Request-Fingerprint"] = fingerprint[:16]
        return result

    return wrapper
