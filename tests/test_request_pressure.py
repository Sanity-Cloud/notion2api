import json

from app.request_pressure import analyze_lines


def test_counts_real_request_lines_and_peak_minute():
    lines = [
        json.dumps({"timestamp": "2026-07-25T14:30:11", "message": "Request processed", "method": "POST", "path": "/v1/chat/completions", "status_code": 200}),
        json.dumps({"timestamp": "2026-07-25T14:30:13", "message": "Request processed", "method": "POST", "path": "/v1/chat/completions", "status_code": 429}),
        json.dumps({"timestamp": "2026-07-25T14:31:00", "message": "Request processed", "method": "GET", "path": "/health", "status_code": 200}),
    ]
    result = analyze_lines(lines)
    assert result["total_inference_requests"] == 2
    assert result["peak_requests_per_minute"] == 2
    assert result["status_counts"] == {"200": 1, "429": 1}


def test_flags_non_retriable_failure_after_prior_attempt():
    lines = [json.dumps({"event": "standard_notion_upstream_failed", "attempt": 2, "retriable": False})]
    result = analyze_lines(lines)
    assert result["non_retriable_failures_after_attempt_one"] == 1
    assert result["retry_attempt_counts"] == {"2": 1}


def test_parses_uvicorn_request_line():
    line = '2026-07-25T14:24:44 INFO 127.0.0.1 - "POST /v1/chat/completions HTTP/1.1" 200'
    result = analyze_lines([line])
    assert result["total_inference_requests"] == 1
    assert result["status_counts"] == {"200": 1}
