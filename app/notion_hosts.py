"""Safe Notion web and app host resolution.

Defaults stay ``https://www.notion.so`` and ``https://app.notion.com``.
``NOTION_URL`` and ``NOTION_DOMAIN`` override those hosts only when the value
is an HTTPS Notion host with no credentials, port, query, or path.
"""

from __future__ import annotations

import os
from urllib.parse import urlparse

DEFAULT_NOTION_WEB_ORIGIN = "https://www.notion.so"
DEFAULT_NOTION_APP_ORIGIN = "https://app.notion.com"


def _allowed_notion_host(host: str) -> bool:
    normalized = host.lower().rstrip(".")
    if normalized in {"www.notion.so", "notion.so", "app.notion.com", "notion.com"}:
        return True
    return normalized.endswith(".notion.so") or normalized.endswith(".notion.com")


def resolve_notion_origin(raw: str | None, *, default: str) -> str:
    """Return a safe HTTPS origin, or the default when the override is unsafe."""

    candidate = str(raw or "").strip()
    if not candidate:
        return default
    if "://" not in candidate:
        candidate = f"https://{candidate}"
    try:
        parsed = urlparse(candidate)
        host = str(parsed.hostname or "").lower().rstrip(".")
        port = parsed.port
        unsafe = (
            parsed.scheme != "https"
            or parsed.username
            or parsed.password
            or port not in {None, 443}
            or parsed.query
            or parsed.fragment
            or parsed.path not in {"", "/"}
            or not host
            or not _allowed_notion_host(host)
        )
    except ValueError:
        return default
    if unsafe:
        return default
    return f"https://{host}"


def notion_web_origin() -> str:
    """Public Notion web origin. ``NOTION_URL`` overrides it only when safe."""

    return resolve_notion_origin(
        os.getenv("NOTION_URL"),
        default=DEFAULT_NOTION_WEB_ORIGIN,
    )


def notion_app_origin() -> str:
    """Notion app API origin. ``NOTION_DOMAIN`` overrides it only when safe."""

    return resolve_notion_origin(
        os.getenv("NOTION_DOMAIN"),
        default=DEFAULT_NOTION_APP_ORIGIN,
    )


def notion_cookie_domain() -> str:
    """Cookie domain follows the resolved web host, defaulting to www.notion.so."""

    return str(urlparse(notion_web_origin()).hostname or "www.notion.so")


def notion_web_api(endpoint_name: str) -> str:
    """Build a www.notion.so-style ``/api/v3`` URL."""

    return f"{notion_web_origin()}/api/v3/{endpoint_name.lstrip('/')}"


def notion_app_api(endpoint_name: str) -> str:
    """Build an app.notion.com-style ``/api/v3`` URL."""

    return f"{notion_app_origin()}/api/v3/{endpoint_name.lstrip('/')}"


def notion_web_api_base() -> str:
    """Return the web ``/api/v3`` prefix."""

    return f"{notion_web_origin()}/api/v3"


def sec_fetch_site(page_origin: str, endpoint: str) -> str:
    """Match browser ``sec-fetch-site`` for a page origin and request URL."""

    page_host = str(urlparse(page_origin).hostname or "").lower().rstrip(".")
    target_host = str(urlparse(endpoint).hostname or "").lower().rstrip(".")
    if not page_host or not target_host or page_host == target_host:
        return "same-origin"

    def site(host: str) -> str:
        parts = host.split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else host

    if site(page_host) == site(target_host):
        return "same-site"
    return "cross-site"


def referer_path_for_endpoint(endpoint: str) -> str:
    """Use the AI history page for transcript reads and chat for inference."""

    path = urlparse(endpoint).path.rstrip("/")
    history_suffixes = (
        "getInferenceTranscriptsForUser",
        "getInferenceTranscriptsUnreadCount",
        "markInferenceTranscriptSeen",
        "syncRecordValuesSpaceInitial",
    )
    if any(path.endswith(suffix) for suffix in history_suffixes):
        return "/ai"
    return "/chat"
