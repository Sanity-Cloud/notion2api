from __future__ import annotations

import asyncio
import threading
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app import notion_admission
from app.notion_admission import NotionAdmissionController
from app.notion_client import NotionOpusAPI
from app.notion_request_telemetry import (
    NotionRequestTelemetryStore,
    UsageQuotaExceededError,
)
from app.notion_usage import normalize_notion_ai_allowance, normalize_notion_ai_usage
from app.server import app, usage_quota_exceeded_handler


def _receipt(
    attempt_id: str,
    *,
    account_key: str = "workspace:user",
    workload_class: str = "interactive",
    request_bytes: int = 10,
    estimated_input_tokens: int = 5,
) -> dict[str, object]:
    return {
        "attempt_id": attempt_id,
        "account_key": account_key,
        "workload_class": workload_class,
        "operation": "test",
        "request_bytes": request_bytes,
        "estimated_input_tokens": estimated_input_tokens,
    }


def test_usage_summary_keeps_actual_and_estimated_tokens_separate(tmp_path) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    store.start(_receipt("success", request_bytes=12, estimated_input_tokens=7))
    store.finish(
        "success",
        success=True,
        response_bytes=20,
        estimated_output_tokens=3,
        actual_input_tokens=6,
        actual_output_tokens=2,
        actual_total_tokens=8,
        retry_count=1,
    )
    store.start(_receipt("failure", request_bytes=4, estimated_input_tokens=2))
    store.finish(
        "failure",
        success=False,
        response_bytes=5,
        estimated_output_tokens=1,
        error_class="upstream_timeout",
    )

    summary = store.usage_summary(window_seconds=3600)

    assert summary["request_count"] == 2
    assert summary["succeeded"] == 1
    assert summary["failed"] == 1
    assert summary["request_bytes"] == 16
    assert summary["response_bytes"] == 25
    assert summary["estimated_input_tokens"] == 9
    assert summary["estimated_output_tokens"] == 4
    assert summary["actual_total_tokens"] == 8
    assert summary["actual_token_attempts"] == 1
    assert summary["retry_count"] == 1
    assert summary["token_accounting"] == "actual_and_estimated_reported_separately"


@pytest.mark.parametrize(
    "policy",
    [
        {"scope": "unknown", "max_requests": 1},
        {"scope": "account", "max_requests": 1},
        {"scope": "workload", "max_requests": 1},
        {"scope": "global"},
        {"scope": "global", "window_seconds": 59, "max_requests": 1},
        {"scope": "global", "max_tokens": -1},
    ],
)
def test_quota_policy_validation_rejects_invalid_contracts(tmp_path, policy) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    with pytest.raises(ValueError):
        store.upsert_quota("invalid", **policy)


def test_quota_revisions_are_durable_and_public_views_hide_account_key(
    tmp_path,
) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    first = store.upsert_quota(
        "account-hourly",
        scope="account",
        account_key="workspace:secret-user",
        max_requests=4,
    )
    second = store.upsert_quota(
        "account-hourly",
        scope="account",
        account_key="workspace:secret-user",
        max_requests=4,
        enabled=False,
    )

    assert first["revision"] == 1
    assert second["revision"] == 2
    assert second["enabled"] is False
    assert second["account_id"]
    assert "account_key" not in second
    assert store.list_quotas(include_disabled=False) == []
    with store._connect() as conn:
        events = conn.execute(
            "SELECT action, policy_json FROM notion_usage_quota_events ORDER BY revision"
        ).fetchall()
    assert [row["action"] for row in events] == ["enabled", "disabled"]
    assert all("secret-user" not in row["policy_json"] for row in events)


def test_allowance_observation_matches_screenshot_semantics_without_account_leak(
    tmp_path,
) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    observation = store.record_allowance_observation(
        account_key="workspace:secret-user",
        rolling_used_percent=1,
        rolling_resets_at=2_000_000_000,
        monthly_used_percent=24,
        monthly_resets_at=2_000_100_000,
        observed_at=1_999_900_000,
        source="notion_settings_ui",
    )

    assert observation["rolling"]["window_seconds"] == 21600
    assert observation["rolling"]["used_percent"] == 1
    assert observation["monthly"]["used_percent"] == 24
    assert observation["excluded_products"] == ["custom_agents", "workers"]
    assert observation["authoritative_for_enforcement"] is False
    assert "account_key" not in observation
    assert (
        store.latest_allowance_observation(account_key="workspace:secret-user")
        == observation
    )


def test_allowance_observation_rejects_invalid_percentage(tmp_path) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    with pytest.raises(ValueError):
        store.record_allowance_observation(
            account_key="workspace:user",
            rolling_used_percent=101,
            monthly_used_percent=24,
        )


def test_chat_usage_analysis_correlates_tokens_to_allowance_intervals(tmp_path) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    for attempt_id, created_at, input_tokens, output_tokens, actual_total in (
        ("first", 1_000, 5, 2, 6),
        ("second", 1_100, 4, 3, None),
    ):
        receipt = _receipt(
            attempt_id,
            account_key="workspace:user",
            estimated_input_tokens=input_tokens,
        )
        receipt["operation"] = "POST /api/v3/runInferenceTranscript"
        store.start(receipt)
        store.finish(
            attempt_id,
            success=True,
            estimated_output_tokens=output_tokens,
            actual_total_tokens=actual_total,
        )
        with store._connect() as conn:
            conn.execute(
                "UPDATE notion_request_attempts SET created_at = ? WHERE attempt_id = ?",
                (created_at, attempt_id),
            )
    first = store.record_allowance_observation(
        account_key="workspace:user",
        rolling_used_percent=10,
        monthly_used_percent=20,
        observed_at=1_050,
    )
    second = store.record_allowance_observation(
        account_key="workspace:user",
        rolling_used_percent=13,
        monthly_used_percent=21,
        observed_at=1_200,
    )

    analysis = store.chat_usage_analysis(
        account_key="workspace:user", start_at=900, end_at=1_300
    )

    assert analysis["chat_operation"] == "POST /api/v3/runInferenceTranscript"
    assert analysis["chat_usage"]["request_count"] == 2
    assert analysis["chat_usage"]["tracked_tokens"] == 13
    assert analysis["percentage_attribution"] == (
        "not_available_from_provider; observational_only"
    )
    assert len(analysis["allowance_correlations"]) == 2
    assert analysis["allowance_correlations"][0]["allowance"]["observation_id"] == first[
        "observation_id"
    ]
    correlated = analysis["allowance_correlations"][1]
    assert correlated["allowance"]["observation_id"] == second["observation_id"]
    assert correlated["chat_usage"]["request_count"] == 1
    assert correlated["chat_usage"]["tracked_tokens"] == 7
    assert correlated["rolling_used_percent_delta"] == 3
    assert correlated["monthly_used_percent_delta"] == 1
    assert correlated["correlation"] == "observational_not_causal"


def test_request_quota_is_enforced_before_second_attempt_is_recorded(tmp_path) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    store.upsert_quota("one-request", scope="global", max_requests=1)
    store.start(_receipt("first"))
    store.finish("first", success=True)

    with pytest.raises(UsageQuotaExceededError) as raised:
        store.start(_receipt("second"))

    assert raised.value.quota_id == "one-request"
    assert raised.value.dimension == "requests"
    assert store.usage_summary()["request_count"] == 1


def test_duplicate_attempt_start_is_idempotent_at_quota_boundary(tmp_path) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    store.upsert_quota("one-request", scope="global", max_requests=1)

    store.start(_receipt("same-attempt"))
    store.start(_receipt("same-attempt"))

    assert store.usage_summary()["request_count"] == 1


def test_account_workload_quota_does_not_reject_unrelated_scope(tmp_path) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    store.upsert_quota(
        "scoped",
        scope="account_workload",
        account_key="workspace:limited",
        workload_class="batch",
        max_requests=0,
    )

    store.start(
        _receipt(
            "unrelated-account",
            account_key="workspace:other",
            workload_class="batch",
        )
    )
    store.start(
        _receipt(
            "unrelated-workload",
            account_key="workspace:limited",
            workload_class="interactive",
        )
    )
    with pytest.raises(UsageQuotaExceededError):
        store.start(
            _receipt(
                "matching",
                account_key="workspace:limited",
                workload_class="batch",
            )
        )

    assert store.usage_summary()["request_count"] == 2


def test_actual_tokens_replace_estimate_for_later_quota_decisions(tmp_path) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    store.upsert_quota("ten-tokens", scope="global", max_tokens=10)
    store.start(_receipt("first", estimated_input_tokens=5))
    store.finish("first", success=True, actual_total_tokens=9)

    with pytest.raises(UsageQuotaExceededError) as raised:
        store.start(_receipt("second", estimated_input_tokens=2))

    assert raised.value.dimension == "tokens"
    status = store.quota_status(
        account_key="workspace:user",
        workload_class="interactive",
        projected_tokens=2,
    )[0]
    assert status["usage"]["tokens"] == 9
    assert status["projected_usage"]["tokens"] == 11


def test_atomic_quota_check_allows_only_one_concurrent_request(tmp_path) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    store.upsert_quota("one-request", scope="global", max_requests=1)
    barrier = threading.Barrier(2)
    accepted: list[str] = []
    rejected: list[str] = []

    def contender(attempt_id: str) -> None:
        barrier.wait()
        try:
            store.start(_receipt(attempt_id))
            accepted.append(attempt_id)
        except UsageQuotaExceededError:
            rejected.append(attempt_id)

    workers = [threading.Thread(target=contender, args=(name,)) for name in ("a", "b")]
    for worker in workers:
        worker.start()
    for worker in workers:
        worker.join(timeout=2)

    assert len(accepted) == 1
    assert len(rejected) == 1
    assert store.usage_summary()["request_count"] == 1


def test_admission_quota_rejection_does_not_leak_local_capacity(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store = NotionRequestTelemetryStore(tmp_path / "usage.sqlite3")
    store.upsert_quota("blocked", scope="global", max_requests=0)
    monkeypatch.setattr(notion_admission, "_REQUEST_TELEMETRY", store)
    controller = NotionAdmissionController(shared_store=False)

    with pytest.raises(UsageQuotaExceededError):
        controller.acquire(
            workspace_id="workspace",
            user_id="user",
            thread_id="thread",
            attempt_id="blocked-attempt",
        )

    snapshot = controller.snapshot()
    assert sum(snapshot["active_accounts"].values()) == 0
    assert snapshot["active_threads"] == 0
    assert snapshot["account_queue_depth"] == 0
    assert snapshot["thread_queue_depth"] == 0
    assert snapshot["counters"]["quota_rejected"] == 1
    assert store.usage_summary()["request_count"] == 0


def test_usage_management_api_creates_lists_and_reports_quotas() -> None:
    with TestClient(app) as client:
        created = client.put(
            "/v1/usage/quotas/api-hourly",
            json={"scope": "global", "window_seconds": 3600, "max_requests": 2},
        )
        assert created.status_code == 200
        assert created.json()["quota"]["revision"] == 1
        assert created.json()["billing_grade"] is False

        listed = client.get("/v1/usage/quotas")
        assert listed.status_code == 200
        assert listed.json()["count"] == 1

        status = client.get(
            "/v1/usage/quotas/status",
            params={"projected_requests": 1},
        )
        assert status.status_code == 200
        assert status.json()["quotas"][0]["projected_usage"]["requests"] == 1

        invalid = client.put(
            "/v1/usage/quotas/api-hourly",
            json={"scope": "global", "max_requests": 2, "unexpected": True},
        )
        assert invalid.status_code == 400


def test_provider_usage_endpoint_returns_only_normalized_credit_contract() -> None:
    provider_payload = {
        "usage": {
            "currentServicePeriod": {"spaceUsage": 2, "userUsage": 1},
            "lifetime": {
                "spaceUsage": 20,
                "userUsage": 10,
                "userPromotionalUsage": 0,
            },
            "totalCreditBalance": 30,
            "creditsInOverage": 0,
        },
        "limits": {
            "purchased": {
                "totalLimit": 30,
                "perSource": {"monthlyAllocated": 30},
            },
            "free": {"spaceLimit": 20, "userLimit": 10},
        },
        "dependencies": [{"key": "sensitive-provider-identifier"}],
    }
    normalized = normalize_notion_ai_usage(provider_payload)
    assert normalized["credit_based_products"]["current_service_period"] == {
        "spaceUsage": 2,
        "userUsage": 1,
    }
    assert normalized["plan_ai_allowance"]["retrieval_status"] == (
        "not_present_in_har_response_contract"
    )
    assert "sensitive-provider-identifier" not in str(normalized)

    fake_client = SimpleNamespace(get_ai_usage_eligibility=lambda: provider_payload)
    fake_pool = SimpleNamespace(get_client_for_selector=lambda selector: fake_client)
    with TestClient(app) as client:
        app.state.account_pool = fake_pool
        response = client.get(
            "/v1/usage/provider", params={"profile_name": "account-one"}
        )
    assert response.status_code == 200
    assert response.json()["provider_usage"] == normalized


def test_notion_usage_retrieval_uses_har_endpoint_and_space_binding() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {"usage": {}, "limits": {}}

    class Scraper:
        def post(self, url: str, **kwargs):
            calls.append((url, kwargs))
            return Response()

    notion_client = NotionOpusAPI.__new__(NotionOpusAPI)
    notion_client.space_id = "synthetic-space"
    notion_client._scraper = Scraper()
    notion_client._build_chat_history_headers = lambda: {
        "x-notion-space-id": "synthetic-space"
    }

    assert notion_client.get_ai_usage_eligibility() == {
        "usage": {},
        "limits": {},
    }
    assert calls == [
        (
            "https://app.notion.com/api/v3/getAIUsageEligibilityV2",
            {
                "headers": {"x-notion-space-id": "synthetic-space"},
                "json": {"spaceId": "synthetic-space"},
                "timeout": 30,
            },
        )
    ]


def test_notion_allowance_retrieval_uses_credit_rate_limit_status_endpoint() -> None:
    calls: list[tuple[str, dict[str, object]]] = []

    class Response:
        def raise_for_status(self) -> None:
            return None

        def json(self) -> dict[str, object]:
            return {
                "status": "within_limit",
                "window": {"window": "6h", "used": 2.44, "limit": 100},
                "billingPeriodWindow": {
                    "cadence": "billing_period",
                    "used": 28.59,
                    "limit": 100,
                    "periodEndMs": 2_000_000_000_000,
                },
            }

    class Scraper:
        def post(self, url: str, **kwargs):
            calls.append((url, kwargs))
            return Response()

    notion_client = NotionOpusAPI.__new__(NotionOpusAPI)
    notion_client.space_id = "synthetic-space"
    notion_client._scraper = Scraper()
    notion_client._build_chat_history_headers = lambda: {
        "x-notion-space-id": "synthetic-space"
    }

    payload = notion_client.get_ai_allowance_status()
    assert normalize_notion_ai_allowance(payload) == {
        "rolling_used_percent": 2.44,
        "monthly_used_percent": 28.59,
        "monthly_resets_at": 2_000_000_000.0,
    }
    assert calls == [
        (
            "https://app.notion.com/api/v3/getCreditRateLimitStatus",
            {
                "headers": {"x-notion-space-id": "synthetic-space"},
                "json": {"spaceId": "synthetic-space"},
                "timeout": 30,
            },
        )
    ]


def test_refresh_allowance_records_provider_percentages_for_profile(monkeypatch, tmp_path) -> None:
    provider_payload = {
        "status": "within_limit",
        "window": {"window": "6h", "used": 12, "limit": 100},
        "billingPeriodWindow": {
            "cadence": "billing_period",
            "used": 34.5,
            "limit": 100,
            "periodEndMs": 2_000_100_000_000,
        },
    }
    fake_client = SimpleNamespace(
        space_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
        user_id="11111111-2222-3333-4444-555555555555",
        get_ai_allowance_status=lambda: provider_payload,
    )
    fake_pool = SimpleNamespace(get_client_for_selector=lambda selector: fake_client)
    store = NotionRequestTelemetryStore(tmp_path / "allowance.sqlite3")
    monkeypatch.setattr(notion_admission, "_REQUEST_TELEMETRY", store)

    with TestClient(app) as client:
        app.state.account_pool = fake_pool
        response = client.post(
            "/v1/usage/allowance/refresh",
            params={"profile_name": "account-one"},
        )

    assert response.status_code == 200
    allowance = response.json()["allowance"]
    assert allowance["rolling"]["used_percent"] == 12
    assert allowance["monthly"]["used_percent"] == 34.5
    assert allowance["monthly"]["resets_at"] == 2_000_100_000.0
    assert allowance["source"] == "notion_settings_api"
    assert "account_key" not in allowance


def test_quota_exception_handler_returns_retry_contract() -> None:
    error = UsageQuotaExceededError(
        {
            "quota_id": "hourly",
            "exceeded_dimension": "tokens",
            "retry_after_seconds": 2.1,
        }
    )

    response = asyncio.run(usage_quota_exceeded_handler(None, error))

    assert response.status_code == 429
    assert response.headers["retry-after"] == "3"
    assert b'"code":"usage_quota_exceeded"' in response.body
