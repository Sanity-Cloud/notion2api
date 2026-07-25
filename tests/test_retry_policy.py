from app.retry_policy import should_retry_upstream, upstream_max_attempts


def test_attempt_budget_defaults_to_two(monkeypatch):
    monkeypatch.delenv("NOTION_UPSTREAM_MAX_ATTEMPTS", raising=False)
    assert upstream_max_attempts(1) == 2
    assert upstream_max_attempts(4) == 2


def test_attempt_budget_is_bounded(monkeypatch):
    monkeypatch.setenv("NOTION_UPSTREAM_MAX_ATTEMPTS", "99")
    assert upstream_max_attempts(20) == 5
    monkeypatch.setenv("NOTION_UPSTREAM_MAX_ATTEMPTS", "0")
    assert upstream_max_attempts(2) == 1


def test_non_retriable_failure_never_retries():
    assert not should_retry_upstream(retriable=False, attempt=1, max_attempts=5)


def test_retriable_failure_stops_at_budget():
    assert should_retry_upstream(retriable=True, attempt=1, max_attempts=2)
    assert not should_retry_upstream(retriable=True, attempt=2, max_attempts=2)
