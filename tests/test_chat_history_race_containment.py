from __future__ import annotations

import asyncio
import threading
from pathlib import Path

import pytest

from app.api.chat_history import OperationInProgress, _run_singleflight


def test_singleflight_rejects_duplicate_and_releases_key() -> None:
    started = threading.Event()
    release = threading.Event()

    def blocking_operation() -> str:
        started.set()
        assert release.wait(timeout=2)
        return "completed"

    async def scenario() -> None:
        first = asyncio.create_task(_run_singleflight("same-key", blocking_operation))
        assert await asyncio.to_thread(started.wait, 1)

        with pytest.raises(OperationInProgress):
            await _run_singleflight("same-key", lambda: "duplicate")

        # The blocking RPC runs off the API event loop.
        await asyncio.sleep(0)
        release.set()
        assert await first == "completed"
        assert await _run_singleflight("same-key", lambda: "reusable") == "reusable"

    asyncio.run(scenario())


def test_chat_history_browser_sync_is_opt_in_and_window_idempotent() -> None:
    script = (
        Path(__file__).resolve().parents[1]
        / "frontend"
        / "js"
        / "chat-history-import.js"
    ).read_text(encoding="utf-8")

    assert "enabled: false" in script
    assert "window.__notionChatHistoryImportInitialized" in script
    assert "setInterval(" not in script
