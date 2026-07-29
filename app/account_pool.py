from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

from app.governance import governance_receipt_from_client
from app.logger import logger
from app.notion_client import NotionOpusAPI


class AccountPool:
    """Notion account pool with explicit auto and pinned selection modes.

    Each admitted request receives a fresh ``NotionOpusAPI`` instance. Switching
    therefore affects new requests only; in-flight work retains the account it
    started with. Pinned mode never silently changes account identity.
    """

    def __init__(self, accounts: List[dict]):
        if not accounts:
            raise ValueError("At least one Notion account is required")

        self.account_configs = [dict(account) for account in accounts]
        self.clients = [self._new_client(index) for index in range(len(accounts))]
        self.cooldown_until = [0.0 for _ in self.account_configs]
        self._current_index = 0
        self._selection_mode = "auto"
        self._selected_index = 0
        self._previous_selection = ("auto", 0)
        self._lock = threading.Lock()
        raw_state_path = os.getenv("NOTION_ACCOUNT_SELECTION_STATE", "").strip()
        self._selection_state_path = Path(raw_state_path) if raw_state_path else None
        self._restore_selection_state()

    def _new_client(self, account_index: int) -> NotionOpusAPI:
        client = NotionOpusAPI(dict(self.account_configs[account_index]))
        setattr(client, "_account_pool_index", account_index)
        return client

    def _profile_name(self, index: int) -> str:
        configured = str(self.account_configs[index].get("profile_name") or "").strip()
        return configured or f"account-{index + 1}"

    def _restore_selection_state(self) -> None:
        path = self._selection_state_path
        if path is None or not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return
            mode = str(payload.get("mode") or "auto").strip().lower()
            if mode == "pinned":
                selector = str(payload.get("profile_name") or "").strip()
                self._selected_index = self._resolve_account_index_unlocked(selector)
                self._selection_mode = "pinned"
            else:
                self._selection_mode = "auto"
            self._previous_selection = (self._selection_mode, self._selected_index)
        except Exception as exc:
            logger.warning(
                "Account selection state could not be restored; using automatic mode",
                extra={
                    "request_info": {
                        "event": "account_selection_restore_failed",
                        "path": str(path),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                },
            )
            self._selection_mode = "auto"
            self._selected_index = 0
            self._previous_selection = ("auto", 0)

    def _persist_selection_state_unlocked(self) -> None:
        path = self._selection_state_path
        if path is None:
            return
        selected = self._selection_descriptor_unlocked(
            self._selection_mode, self._selected_index
        )
        payload = {
            "version": 1,
            "mode": selected["mode"],
            "profile_name": selected["profile_name"],
            "updated_at": int(time.time()),
        }
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(path.suffix + ".tmp")
            temporary.write_text(
                json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            temporary.replace(path)
        except Exception as exc:
            logger.warning(
                "Account selection state could not be persisted",
                extra={
                    "request_info": {
                        "event": "account_selection_persist_failed",
                        "path": str(path),
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                },
            )

    def _selection_descriptor_unlocked(self, mode: str, index: int) -> dict[str, Any]:
        if mode == "auto":
            return {"mode": "auto", "account_number": None, "profile_name": None}
        return {
            "mode": "pinned",
            "account_number": index + 1,
            "profile_name": self._profile_name(index),
        }

    def _resolve_account_index_unlocked(self, selector: str) -> int:
        normalized = str(selector or "").strip().casefold()
        if not normalized:
            raise ValueError("An account selector is required for pinned mode")

        matches: list[int] = []
        for index, account in enumerate(self.account_configs):
            values = {
                self._profile_name(index),
                str(account.get("user_email") or "").strip(),
                str(account.get("user_id") or "").strip(),
                str(account.get("user_name") or "").strip(),
            }
            if normalized in {value.casefold() for value in values if value}:
                matches.append(index)

        if not matches and normalized.isdigit():
            account_number = int(normalized)
            if 1 <= account_number <= len(self.account_configs):
                matches.append(account_number - 1)

        if not matches:
            raise ValueError(
                f"No configured Notion account matches selector: {selector}"
            )
        if len(matches) > 1:
            raise ValueError(f"Account selector is ambiguous: {selector}")
        return matches[0]

    def get_client_for_selector(self, selector: str) -> NotionOpusAPI:
        with self._lock:
            return self._new_client(self._resolve_account_index_unlocked(selector))

    def get_metadata_client(self) -> NotionOpusAPI:
        """Return a fresh client representing the current selection without rotating."""
        with self._lock:
            index = (
                self._selected_index
                if self._selection_mode == "pinned"
                else self._current_index
            )
            return self._new_client(index)

    def get_client(self, wait_if_cooling: bool = True) -> NotionOpusAPI:
        """Return a fresh client using the configured selection mode."""
        while True:
            now = time.time()
            wait_seconds: float | None = None
            with self._lock:
                if self._selection_mode == "pinned":
                    index = self._selected_index
                    if self.cooldown_until[index] <= now:
                        return self._new_client(index)
                    wait_seconds = max(0.5, self.cooldown_until[index] - now)
                else:
                    start_index = self._current_index
                    while True:
                        index = self._current_index
                        self._current_index = (self._current_index + 1) % len(
                            self.account_configs
                        )
                        if self.cooldown_until[index] <= now:
                            return self._new_client(index)
                        if self._current_index == start_index:
                            wait_seconds = max(0.5, min(self.cooldown_until) - now)
                            break

            if wait_seconds is None:
                continue
            if not wait_if_cooling or wait_seconds > 15:
                mode_label = (
                    "Pinned account"
                    if self._selection_mode == "pinned"
                    else "All Notion accounts"
                )
                raise RuntimeError(
                    f"{mode_label} cooling for about {max(1, int(wait_seconds))} seconds"
                )
            logger.info(
                "Account pool waiting for cooldown",
                extra={
                    "request_info": {
                        "event": "account_pool_wait_cooling",
                        "selection_mode": self._selection_mode,
                        "wait_seconds": round(wait_seconds, 1),
                    }
                },
            )
            time.sleep(wait_seconds)

    def _account_summary_unlocked(self, index: int, now: float) -> dict[str, Any]:
        account = self.account_configs[index]
        cooldown_remaining = max(0.0, self.cooldown_until[index] - now)
        return {
            "account_number": index + 1,
            "profile_name": self._profile_name(index),
            "user_name": str(account.get("user_name") or "").strip(),
            "user_email": str(account.get("user_email") or "").strip(),
            "space_id": str(account.get("space_id") or "").strip(),
            "user_id": str(account.get("user_id") or "").strip(),
            "selected": self._selection_mode == "pinned"
            and index == self._selected_index,
            "next_in_rotation": self._selection_mode == "auto"
            and index == self._current_index,
            "available": cooldown_remaining <= 0,
            "cooldown_remaining_seconds": round(cooldown_remaining, 3),
            "governance_aligned": bool(
                governance_receipt_from_client(self.clients[index]).get("aligned")
            ),
        }

    def get_selection_summary(self) -> dict[str, Any]:
        now = time.time()
        with self._lock:
            previous_mode, previous_index = self._previous_selection
            selected = self._selection_descriptor_unlocked(
                self._selection_mode, self._selected_index
            )
            return {
                **selected,
                "selected_account_number": selected["account_number"],
                "selected_profile_name": selected["profile_name"],
                "next_account_number": self._current_index + 1
                if self._selection_mode == "auto"
                else None,
                "previous_selection": self._selection_descriptor_unlocked(
                    previous_mode, previous_index
                ),
                "effective_for_new_requests": True,
                "persistence_enabled": self._selection_state_path is not None,
                "accounts": [
                    self._account_summary_unlocked(index, now)
                    for index in range(len(self.account_configs))
                ],
            }

    def switch_account(
        self, *, mode: str, selector: str | None = None
    ) -> dict[str, Any]:
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {"auto", "pinned"}:
            raise ValueError("Account selection mode must be 'auto' or 'pinned'")

        with self._lock:
            previous = (self._selection_mode, self._selected_index)
            if normalized_mode == "auto":
                self._previous_selection = previous
                self._selection_mode = "auto"
            else:
                selected_index = self._resolve_account_index_unlocked(selector or "")
                if self.cooldown_until[selected_index] > time.time():
                    remaining = self.cooldown_until[selected_index] - time.time()
                    raise RuntimeError(
                        f"Selected Notion account is cooling for about {max(1, int(remaining))} seconds"
                    )
                self._previous_selection = previous
                self._selection_mode = "pinned"
                self._selected_index = selected_index
            self._persist_selection_state_unlocked()

        return self.get_selection_summary()

    def rollback_account_switch(self) -> dict[str, Any]:
        with self._lock:
            current = (self._selection_mode, self._selected_index)
            previous_mode, previous_index = self._previous_selection
            self._selection_mode = previous_mode
            self._selected_index = previous_index
            self._previous_selection = current
            self._persist_selection_state_unlocked()
        return self.get_selection_summary()

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

    def get_governance_summary(self) -> dict:
        receipts = [governance_receipt_from_client(client) for client in self.clients]
        canonical = dict(receipts[0]) if receipts else {}
        canonical["aligned"] = bool(receipts) and all(
            receipt == receipts[0] and bool(receipt.get("aligned"))
            for receipt in receipts
        )
        canonical["account_count"] = len(receipts)
        return canonical

    def mark_failed(self, client: NotionOpusAPI, cooldown_seconds: int = 3) -> None:
        """Apply cooldown to the account that created a request client."""
        with self._lock:
            raw_index = getattr(client, "_account_pool_index", None)
            index = raw_index if isinstance(raw_index, int) else None
            if index is None or index < 0 or index >= len(self.account_configs):
                index = next(
                    (
                        candidate
                        for candidate, template in enumerate(self.clients)
                        if template.account_key == client.account_key
                        and template.space_id == client.space_id
                    ),
                    None,
                )
            if index is None:
                logger.warning(
                    "Attempted to mark unknown account as failed",
                    extra={"request_info": {"event": "account_failed_unknown"}},
                )
                return

            self.cooldown_until[index] = time.time() + cooldown_seconds
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
