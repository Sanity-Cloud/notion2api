"""Shared browser request headers and safe Notion endpoint configuration."""

from __future__ import annotations

import os
from urllib.parse import urlsplit

BROWSER_USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/152.0.0.0 Safari/537.36"
)
BROWSER_CLIENT_HINT = '"Chromium";v="152", "Not?A_Brand";v="24", "Google Chrome";v="152"'
OFFICIAL_NOTION_HOSTS = frozenset({"www.notion.so", "app.notion.com"})


def configured_notion_origin(env_name: str, default: str) -> str:
    """Accept HTTPS origins only; require explicit opt-in for non-Notion hosts."""
    value = (os.getenv(env_name) or default).strip().rstrip("/")
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{env_name} must be an HTTPS origin without path or credentials")
    allow_custom = (os.getenv("NOTION_ALLOW_CUSTOM_HOSTS") or "").lower() in {
        "1", "true", "yes", "on",
    }
    if parsed.hostname not in OFFICIAL_NOTION_HOSTS and not allow_custom:
        raise ValueError(
            f"{env_name} requires NOTION_ALLOW_CUSTOM_HOSTS=true for a non-Notion host"
        )
    return value


def browser_request_headers(
    origin: str, *, accept: str, referer_path: str = "/chat",
) -> dict[str, str]:
    """Construct browser-looking HTTP metadata for a same-origin request."""
    return {
        "Accept": accept,
        "User-Agent": BROWSER_USER_AGENT,
        "origin": origin,
        "referer": f"{origin}{referer_path}",
        "sec-ch-ua": BROWSER_CLIENT_HINT,
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"Linux"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-origin",
    }
