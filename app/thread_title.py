from __future__ import annotations

import re
from typing import Any, Mapping


MAX_THREAD_TITLE_LENGTH = 120


def normalize_thread_title(value: Any) -> str | None:
    """Return a compact, bounded thread title suitable for local and Notion storage."""
    clean = " ".join(str(value or "").split()).strip()
    clean = re.sub(
        r"^(?:chat\s+thread\s+title|repoai\s+thread\s+title)\s*:\s*",
        "",
        clean,
        flags=re.IGNORECASE,
    ).strip()
    if not clean:
        return None
    return clean[:MAX_THREAD_TITLE_LENGTH].rstrip()


def resolve_requested_thread_title(
    *,
    chat_title: Any = None,
    title: Any = None,
    session_name: Any = None,
    metadata: Mapping[str, Any] | None = None,
) -> str | None:
    """Resolve caller-provided title aliases in explicit-to-compatibility order."""
    metadata = metadata if isinstance(metadata, Mapping) else {}
    candidates = (
        chat_title,
        title,
        session_name,
        metadata.get("repo_ai_thread_title"),
        metadata.get("chat_title"),
        metadata.get("title"),
        metadata.get("session_name"),
    )
    for candidate in candidates:
        normalized = normalize_thread_title(candidate)
        if normalized:
            return normalized
    return None
