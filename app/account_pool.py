from __future__ import annotations

import threading
import time
from typing import Dict, List

from app.logger import logger
from app.notion_client import NotionOpusAPI


class AccountPool:
    """Round-robin Notion account pool with request-isolated clients.

    Cooldown and selection state are account-scoped, but each admitted request
    receives a new ``NotionOpusAPI`` instance. This prevents independent chats
    from sharing mutable thread IDs, upload state, or a requests session.
    """

    def __init__(self, accounts: List[dict]):
        if not accounts:
            raise ValueError("At least one Notion account is required")

        self.account_configs = [dict(account) for account in accounts]
        # Template clients remain available for read-only account/model metadata.
        self.clients = [self._new_client(index) for index in range(len(accounts))]
        self.cooldown_until = [0.0 for _ in self.account_configs]
        self._current_index = 0
        self._lock = threading.Lock()

    def _new_client(self, account_index: int) -> NotionOpusAPI:
        client = NotionOpusAPI(dict(self.account_configs[account_index]))
        setattr(client, "_account_pool_index", account_index)
        return client

    def get_client(self, wait_if_cooling: bool = True) -> NotionOpusAPI:
        """Return a fresh client for the next available account."""
        now = time.time()
        with self._lock:
            start_index = self._current_index

            while True:
                idx = self._current_index
                if self.cooldown_until[idx] <= now:
                    self._current_index = (self._current_index + 1) % len(
                        self.account_configs
                    )
                    return self._new_client(idx)

                self._current_index = (self._current_index + 1) % len(
                    self.account_configs
                )
                if self._current_index == start_index:
                    next_available = min(self.cooldown_until)
                    wait_seconds = max(0.5, next_available - now)
                    if wait_if_cooling and wait_seconds <= 15:
                        logger.info(
                            f"All accounts cooling, waiting {wait_seconds:.1f}s",
                            extra={
                                "request_info": {
                                    "event": "account_pool_wait_cooling",
                                    "wait_seconds": round(wait_seconds, 1),
                                }
                            },
                        )
                        self._lock.release()
                        try:
                            time.sleep(wait_seconds)
                        finally:
                            self._lock.acquire()
                        now = time.time()
                        continue
                    raise RuntimeError(
                        f"All Notion accounts are cooling for about "
                        f"{max(1, int(wait_seconds))} seconds"
                    )

    def get_status_summary(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            active = sum(1 for timestamp in self.cooldown_until if timestamp <= now)
            cooling = len(self.cooldown_until) - active
            return {
                "total": len(self.account_configs),
                "active": active,
                "cooling": cooling,
            }

    def mark_failed(self, client: NotionOpusAPI, cooldown_seconds: int = 3) -> None:
        """Apply cooldown to the account that created a request client."""
        with self._lock:
            raw_index = getattr(client, "_account_pool_index", None)
            idx = raw_index if isinstance(raw_index, int) else None
            if idx is None or idx < 0 or idx >= len(self.account_configs):
                # Compatibility fallback for template clients and older callers.
                idx = next(
                    (
                        index
                        for index, template in enumerate(self.clients)
                        if template.account_key == client.account_key
                        and template.space_id == client.space_id
                    ),
                    None,
                )
            if idx is None:
                logger.warning(
                    "Attempted to mark unknown account as failed",
                    extra={"request_info": {"event": "account_failed_unknown"}},
                )
                return

            self.cooldown_until[idx] = time.time() + cooldown_seconds
            logger.warning(
                "Account marked as failed",
                extra={
                    "request_info": {
                        "event": "account_failed",
                        "account": client.account_key,
                        "space_id": client.space_id,
                        "cooldown_seconds": cooldown_seconds,
                    }
                },
            )
