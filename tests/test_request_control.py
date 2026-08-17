import asyncio
from types import SimpleNamespace

from fastapi import HTTPException

import pytest

from app.request_control import (
    AdmissionRejected,
    RequestController,
    controlled_chat_request,
    request_fingerprint,
    safe_scope_label,
    scoped_fingerprint,
)


@pytest.mark.asyncio
async def test_duplicate_in_flight_is_rejected():
    controller = RequestController(max_concurrency=2, queue_timeout_seconds=0.05)
    first = await controller.acquire("session-a", "same")
    with pytest.raises(AdmissionRejected) as caught:
        await controller.acquire("session-a", "same")
    assert caught.value.code == "DUPLICATE_IN_FLIGHT"
    await first.release()


@pytest.mark.asyncio
async def test_global_capacity_times_out():
    controller = RequestController(max_concurrency=1, queue_timeout_seconds=0.02)
    first = await controller.acquire("session-a", "one")
    with pytest.raises(AdmissionRejected) as caught:
        await controller.acquire("session-b", "two")
    assert caught.value.code == "INFERENCE_QUEUE_TIMEOUT"
    await first.release()


@pytest.mark.asyncio
async def test_conversation_requests_are_serialized():
    controller = RequestController(max_concurrency=2, queue_timeout_seconds=0.02)
    first = await controller.acquire("session-a", "one")
    with pytest.raises(AdmissionRejected) as caught:
        await controller.acquire("session-a", "two")
    assert caught.value.code == "INFERENCE_QUEUE_TIMEOUT"
    await first.release()


@pytest.mark.asyncio
async def test_circuit_breaker_opens_and_recovers():
    controller = RequestController(
        max_concurrency=1,
        failure_threshold=2,
        failure_window_seconds=60,
        recovery_seconds=0.02,
    )
    await controller.record_failure()
    await controller.record_failure()
    with pytest.raises(AdmissionRejected) as caught:
        await controller.acquire("session-a", "one")
    assert caught.value.code == "UPSTREAM_CIRCUIT_OPEN"
    await asyncio.sleep(0.03)
    lease = await controller.acquire("session-a", "one")
    await lease.release()


def test_fingerprint_is_stable_and_non_plaintext():
    body = SimpleNamespace(dict=lambda: {"model": "terra", "messages": [{"content": "secret"}]})
    first = request_fingerprint(body)
    second = request_fingerprint(body)
    assert first == second
    assert len(first) == 64
    assert "secret" not in first


@pytest.mark.asyncio
async def test_snapshot_reports_capacity_and_rejections():
    controller = RequestController(max_concurrency=1, queue_timeout_seconds=0.01)
    lease = await controller.acquire("session-a", "one")
    with pytest.raises(AdmissionRejected):
        await controller.acquire("session-a", "one")
    snapshot = await controller.snapshot()
    assert snapshot["active"] == 1
    assert snapshot["max_concurrency"] == 1
    assert snapshot["duplicate_rejected_total"] == 1
    await lease.release()


@pytest.mark.asyncio
async def test_client_http_exception_does_not_trip_circuit():
    controller = RequestController(max_concurrency=1, failure_threshold=1)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(request_control=controller)), headers={})
    body = SimpleNamespace(model_dump=lambda mode="json": {"model": "terra", "messages": []})

    @controlled_chat_request
    async def handler(request, req_body):
        raise HTTPException(status_code=400, detail="invalid")

    with pytest.raises(HTTPException):
        await handler(request, body)
    snapshot = await controller.snapshot()
    assert snapshot["active"] == 0
    assert snapshot["recent_failures"] == 0
    assert not snapshot["circuit_open"]


@pytest.mark.asyncio
async def test_server_http_exception_counts_as_failure():
    controller = RequestController(max_concurrency=1, failure_threshold=1)
    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(request_control=controller)), headers={})
    body = SimpleNamespace(model_dump=lambda mode="json": {"model": "terra", "messages": []})

    @controlled_chat_request
    async def handler(request, req_body):
        raise HTTPException(status_code=503, detail="upstream unavailable")

    with pytest.raises(HTTPException):
        await handler(request, body)
    snapshot = await controller.snapshot()
    assert snapshot["active"] == 0
    assert snapshot["recent_failures"] == 1
    assert snapshot["circuit_open"]


def test_fingerprint_is_scoped_without_exposing_scope():
    base = "a" * 64
    first = scoped_fingerprint("session-a", base)
    second = scoped_fingerprint("session-b", base)
    assert first != second
    assert "session-a" not in first
    assert safe_scope_label("case-sensitive-session") != "case-sensitive-session"
