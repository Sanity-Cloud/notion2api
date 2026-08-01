from __future__ import annotations

import ipaddress

from fastapi import Request

REPO_AI_CALLER_HEADER = "x-repo-ai-internal"
REPO_AI_CALLER_VALUE = "1"


def _is_loopback_host(value: str) -> bool:
    host = str(value or "").strip().lower().strip("[]")
    if host == "localhost":
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def is_repo_ai_internal_request(request: Request) -> bool:
    """Recognize only explicit loopback RepoAI calls and fail closed on test stubs."""
    client = getattr(request, "client", None)
    if isinstance(client, tuple):
        client_host = str(client[0] or "") if client else ""
    else:
        client_host = str(getattr(client, "host", "") or "")

    url = getattr(request, "url", None)
    request_host = str(getattr(url, "hostname", "") or "")
    headers = getattr(request, "headers", None)
    marker = ""
    if headers is not None and hasattr(headers, "get"):
        marker = str(headers.get(REPO_AI_CALLER_HEADER, "") or "")

    return (
        marker == REPO_AI_CALLER_VALUE
        and _is_loopback_host(client_host)
        and _is_loopback_host(request_host)
    )
