from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path
from typing import Any, Dict, List

from app.diagnostics import emit_diagnostic_event
from app.governance import governance_receipt_from_client
from app.logger import logger
from app.notion_client import NotionOpusAPI
from app.workspace_routing import resolve_workspace_definition, workspace_descriptors


class AccountPool:
    """Workspace-pinned Notion account pool with per-workspace account rotation.

    Workspace selection is explicit and never rotates automatically. New chats and
    requests receive an account from the selected workspace; persistent chats must
    later reacquire their exact ``workspace_id + user_id`` binding.
    """

    def __init__(self, accounts: List[dict]):
        if not accounts:
            raise ValueError("At least one Notion account is required")

        self.account_configs = [dict(account) for account in accounts]
        self.clients = [self._new_client(index) for index in range(len(accounts))]
        self.cooldown_until = [0.0 for _ in self.account_configs]
        configured_keys = list(
            dict.fromkeys(self._workspace_key_for_index(index) for index in range(len(accounts)))
        )
        requested_workspace = os.getenv("SANITYCLOUD_DEFAULT_WORKSPACE", configured_keys[0])
        try:
            resolved_workspace = resolve_workspace_definition(requested_workspace).key
        except ValueError:
            resolved_workspace = str(requested_workspace or "").strip().casefold()
        self._workspace_key = (
            resolved_workspace if resolved_workspace in configured_keys else configured_keys[0]
        )
        self._workspace_cursors = {
            key: self._workspace_indices_for_key(key)[0] for key in configured_keys
        }
        self._current_index = self._workspace_cursors[self._workspace_key]
        self._selection_mode = "auto"
        self._selected_index = self._current_index
        self._previous_selection = (
            self._workspace_key,
            "auto",
            self._current_index,
        )
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

    def _workspace_key_for_index(self, index: int) -> str:
        account = self.account_configs[index]
        configured = str(account.get("workspace_key") or "").strip().casefold()
        if configured:
            return configured
        workspace_id = str(account.get("space_id") or "").strip()
        try:
            return resolve_workspace_definition(workspace_id).key
        except ValueError:
            normalized = workspace_id.replace("-", "").casefold()
            return f"legacy:{normalized or 'default'}"

    def _workspace_indices_for_key(self, workspace_key: str) -> list[int]:
        normalized = str(workspace_key or "").strip().casefold()
        indices = [
            index
            for index in range(len(self.account_configs))
            if self._workspace_key_for_index(index) == normalized
        ]
        if not indices:
            raise ValueError(f"No configured accounts for workspace: {workspace_key}")
        return indices

    def _active_workspace_indices_unlocked(self) -> list[int]:
        return self._workspace_indices_for_key(self._workspace_key)

    def _resolve_workspace_key_unlocked(self, selector: str) -> str:
        normalized = str(selector or "").strip().casefold()
        configured = {
            self._workspace_key_for_index(index)
            for index in range(len(self.account_configs))
        }
        if normalized in configured:
            return normalized
        resolved = resolve_workspace_definition(selector).key
        self._workspace_indices_for_key(resolved)
        return resolved

    def _set_workspace_unlocked(self, workspace_key: str) -> None:
        indices = self._workspace_indices_for_key(workspace_key)
        self._workspace_key = workspace_key
        current = self._workspace_cursors.get(workspace_key, indices[0])
        self._current_index = current if current in indices else indices[0]
        self._workspace_cursors[workspace_key] = self._current_index
        self._selection_mode = "auto"
        self._selected_index = self._current_index

    def _restore_selection_state(self) -> None:
        path = self._selection_state_path
        if path is None or not path.exists():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if not isinstance(payload, dict):
                return
            workspace_selector = str(
                payload.get("workspace_key")
                or payload.get("workspace_id")
                or self._workspace_key
            ).strip()
            workspace_key = self._resolve_workspace_key_unlocked(workspace_selector)
            self._set_workspace_unlocked(workspace_key)
            mode = str(payload.get("mode") or "auto").strip().lower()
            if mode == "pinned":
                selector = str(payload.get("profile_name") or "").strip()
                self._selected_index = self._resolve_account_index_unlocked(selector)
                self._selection_mode = "pinned"
            self._previous_selection = (
                self._workspace_key,
                self._selection_mode,
                self._selected_index,
            )
        except Exception as exc:
            emit_diagnostic_event(
                code="ACCOUNT_SELECTION_RESTORE_FAILED",
                message="Notion2API could not restore persisted account-selection state and fell back to automatic mode.",
                operation="restore_account_selection_state",
                category="state_persistence",
                severity="warning",
                kind="state_restore_failure",
                retryable=False,
                details={
                    "exception_type": type(exc).__name__,
                    "fallback_mode": "auto",
                },
            )
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
            indices = self._active_workspace_indices_unlocked()
            self._selection_mode = "auto"
            self._current_index = indices[0]
            self._selected_index = self._current_index
            self._previous_selection = (
                self._workspace_key,
                "auto",
                self._current_index,
            )

    def _persist_selection_state_unlocked(self) -> None:
        path = self._selection_state_path
        if path is None:
            return
        selected = self._selection_descriptor_unlocked(
            self._selection_mode, self._selected_index
        )
        workspace = self._workspace_descriptor_unlocked()
        payload = {
            "version": 2,
            "workspace_key": workspace["workspace_key"],
            "workspace_id": workspace["workspace_id"],
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
            emit_diagnostic_event(
                code="ACCOUNT_SELECTION_PERSIST_FAILED",
                message="Notion2API could not persist account-selection state.",
                operation="persist_account_selection_state",
                category="state_persistence",
                severity="warning",
                kind="persistence_failure",
                retryable=True,
                details={"exception_type": type(exc).__name__},
            )
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

    def _workspace_descriptor_unlocked(self) -> dict[str, Any]:
        index = self._active_workspace_indices_unlocked()[0]
        account = self.account_configs[index]
        return {
            "workspace_key": self._workspace_key,
            "workspace_name": str(account.get("workspace_name") or "").strip(),
            "workspace_id": str(account.get("space_id") or "").strip(),
            "teamspace_name": str(account.get("teamspace_name") or "").strip(),
            "teamspace_id": str(account.get("governance_teamspace_id") or "").strip(),
        }

    def _workspace_summaries_unlocked(self) -> list[dict[str, Any]]:
        available = {
            self._workspace_key_for_index(index)
            for index in range(len(self.account_configs))
        }
        return [
            {
                **descriptor,
                "selected": descriptor["workspace_key"] == self._workspace_key,
                "account_count": len(
                    self._workspace_indices_for_key(descriptor["workspace_key"])
                ),
            }
            for descriptor in workspace_descriptors()
            if descriptor["workspace_key"] in available
        ]

    def _selection_descriptor_unlocked(self, mode: str, index: int) -> dict[str, Any]:
        workspace_key = self._workspace_key_for_index(index)
        if mode == "auto":
            return {
                "mode": "auto",
                "account_number": None,
                "profile_name": None,
                "workspace_key": workspace_key,
            }
        workspace_indices = self._workspace_indices_for_key(workspace_key)
        return {
            "mode": "pinned",
            "account_number": workspace_indices.index(index) + 1,
            "profile_name": self._profile_name(index),
            "workspace_key": workspace_key,
        }

    def _resolve_account_index_in_workspace_unlocked(
        self, selector: str, workspace_key: str
    ) -> int:
        normalized = str(selector or "").strip().casefold()
        if not normalized:
            raise ValueError("An account selector is required for pinned mode")

        workspace_indices = self._workspace_indices_for_key(workspace_key)
        matches: list[int] = []
        for index in workspace_indices:
            account = self.account_configs[index]
            values = {
                self._profile_name(index),
                str(account.get("base_profile_name") or "").strip(),
                str(account.get("routing_profile_name") or "").strip(),
                str(account.get("user_email") or "").strip(),
                str(account.get("user_id") or "").strip(),
                str(account.get("user_name") or "").strip(),
            }
            if normalized in {value.casefold() for value in values if value}:
                matches.append(index)

        if not matches and normalized.isdigit():
            account_number = int(normalized)
            if 1 <= account_number <= len(workspace_indices):
                matches.append(workspace_indices[account_number - 1])

        if not matches:
            raise ValueError(
                f"No configured Notion account in workspace {workspace_key!r} "
                f"matches selector: {selector}"
            )
        if len(matches) > 1:
            raise ValueError(f"Account selector is ambiguous: {selector}")
        return matches[0]

    def _resolve_account_index_unlocked(self, selector: str) -> int:
        return self._resolve_account_index_in_workspace_unlocked(
            selector, self._workspace_key
        )

    def get_client_for_selector(self, selector: str) -> NotionOpusAPI:
        with self._lock:
            return self._new_client(self._resolve_account_index_unlocked(selector))

    def get_client_for_workspace_account(
        self, workspace_selector: str, account_selector: str
    ) -> NotionOpusAPI:
        """Return one explicitly selected account without mutating pool cursors."""
        with self._lock:
            workspace_key = (
                self._resolve_workspace_key_unlocked(workspace_selector)
                if str(workspace_selector or "").strip()
                else self._workspace_key
            )
            index = self._resolve_account_index_in_workspace_unlocked(
                account_selector, workspace_key
            )
            return self._new_client(index)

    def get_client_for_binding(self, *, workspace_id: str, user_id: str) -> NotionOpusAPI:
        """Return the exact client bound to a persistent chat identity."""
        normalized_workspace = str(workspace_id or "").replace("-", "").casefold()
        normalized_user = str(user_id or "").replace("-", "").casefold()
        if not normalized_workspace or not normalized_user:
            raise ValueError("workspace_id and user_id are required for chat binding")
        with self._lock:
            matches = [
                index
                for index, account in enumerate(self.account_configs)
                if str(account.get("space_id") or "").replace("-", "").casefold()
                == normalized_workspace
                and str(account.get("user_id") or "").replace("-", "").casefold()
                == normalized_user
            ]
            if len(matches) != 1:
                raise ValueError(
                    "Persistent chat binding does not resolve to exactly one configured account"
                )
            return self._new_client(matches[0])

    def get_metadata_client(self) -> NotionOpusAPI:
        """Return a fresh client representing the current selection without rotating."""
        with self._lock:
            index = (
                self._selected_index
                if self._selection_mode == "pinned"
                else self._current_index
            )
            return self._new_client(index)

    def get_metadata_client_for_workspace(self, selector: str) -> NotionOpusAPI:
        """Inspect one workspace without mutating the interactive selection."""
        with self._lock:
            workspace_key = self._resolve_workspace_key_unlocked(selector)
            indices = self._workspace_indices_for_key(workspace_key)
            index = self._workspace_cursors.get(workspace_key, indices[0])
            if index not in indices:
                index = indices[0]
            return self._new_client(index)

    def get_client_for_workspace(
        self, selector: str, wait_if_cooling: bool = True
    ) -> NotionOpusAPI:
        """Rotate accounts inside one request-scoped workspace without selecting it globally."""
        with self._lock:
            workspace_key = self._resolve_workspace_key_unlocked(selector)

        while True:
            now = time.time()
            wait_seconds: float | None = None
            with self._lock:
                workspace_indices = self._workspace_indices_for_key(workspace_key)
                cursor = self._workspace_cursors.get(workspace_key, workspace_indices[0])
                if cursor not in workspace_indices:
                    cursor = workspace_indices[0]
                start_position = workspace_indices.index(cursor)
                for offset in range(len(workspace_indices)):
                    position = (start_position + offset) % len(workspace_indices)
                    index = workspace_indices[position]
                    next_index = workspace_indices[
                        (position + 1) % len(workspace_indices)
                    ]
                    self._workspace_cursors[workspace_key] = next_index
                    if workspace_key == self._workspace_key and self._selection_mode == "auto":
                        self._current_index = next_index
                    if self.cooldown_until[index] <= now:
                        return self._new_client(index)
                wait_seconds = max(
                    0.5,
                    min(self.cooldown_until[index] for index in workspace_indices) - now,
                )

            if not wait_if_cooling or wait_seconds > 15:
                raise RuntimeError(
                    f"All accounts in workspace {workspace_key} cooling for about "
                    f"{max(1, int(wait_seconds))} seconds"
                )
            logger.info(
                "Request-scoped workspace pool waiting for cooldown",
                extra={
                    "request_info": {
                        "event": "workspace_account_pool_wait_cooling",
                        "workspace_key": workspace_key,
                        "wait_seconds": round(wait_seconds, 1),
                    }
                },
            )
            time.sleep(wait_seconds)

    def get_client(self, wait_if_cooling: bool = True) -> NotionOpusAPI:
        """Return a fresh client from the explicitly selected workspace."""
        while True:
            now = time.time()
            wait_seconds: float | None = None
            with self._lock:
                workspace_indices = self._active_workspace_indices_unlocked()
                if self._selection_mode == "pinned":
                    index = self._selected_index
                    if index not in workspace_indices:
                        raise RuntimeError("Pinned account is outside the selected workspace")
                    if self.cooldown_until[index] <= now:
                        return self._new_client(index)
                    wait_seconds = max(0.5, self.cooldown_until[index] - now)
                else:
                    cursor = self._workspace_cursors.get(
                        self._workspace_key, workspace_indices[0]
                    )
                    if cursor not in workspace_indices:
                        cursor = workspace_indices[0]
                    start_position = workspace_indices.index(cursor)
                    for offset in range(len(workspace_indices)):
                        position = (start_position + offset) % len(workspace_indices)
                        index = workspace_indices[position]
                        next_index = workspace_indices[
                            (position + 1) % len(workspace_indices)
                        ]
                        self._current_index = next_index
                        self._workspace_cursors[self._workspace_key] = next_index
                        if self.cooldown_until[index] <= now:
                            return self._new_client(index)
                    wait_seconds = max(
                        0.5,
                        min(self.cooldown_until[index] for index in workspace_indices)
                        - now,
                    )

            if wait_seconds is None:
                continue
            if not wait_if_cooling or wait_seconds > 15:
                mode_label = (
                    "Pinned account"
                    if self._selection_mode == "pinned"
                    else f"All accounts in workspace {self._workspace_key}"
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
                        "workspace_key": self._workspace_key,
                        "wait_seconds": round(wait_seconds, 1),
                    }
                },
            )
            time.sleep(wait_seconds)

    def _account_summary_unlocked(self, index: int, now: float) -> dict[str, Any]:
        account = self.account_configs[index]
        workspace_key = self._workspace_key_for_index(index)
        workspace_indices = self._workspace_indices_for_key(workspace_key)
        cooldown_remaining = max(0.0, self.cooldown_until[index] - now)
        return {
            "account_number": workspace_indices.index(index) + 1,
            "profile_name": self._profile_name(index),
            "base_profile_name": str(account.get("base_profile_name") or "").strip(),
            "workspace_key": workspace_key,
            "workspace_name": str(account.get("workspace_name") or "").strip(),
            "teamspace_name": str(account.get("teamspace_name") or "").strip(),
            "user_name": str(account.get("user_name") or "").strip(),
            "user_email": str(account.get("user_email") or "").strip(),
            "space_id": str(account.get("space_id") or "").strip(),
            "user_id": str(account.get("user_id") or "").strip(),
            "selected": self._selection_mode == "pinned"
            and index == self._selected_index,
            "next_in_rotation": self._selection_mode == "auto"
            and workspace_key == self._workspace_key
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
            previous_workspace, previous_mode, previous_index = self._previous_selection
            selected = self._selection_descriptor_unlocked(
                self._selection_mode, self._selected_index
            )
            workspace = self._workspace_descriptor_unlocked()
            active_indices = self._active_workspace_indices_unlocked()
            previous_descriptor = self._selection_descriptor_unlocked(
                previous_mode, previous_index
            )
            previous_descriptor["workspace_key"] = previous_workspace
            return {
                **selected,
                **workspace,
                "workspace_mode": "pinned",
                "selected_account_number": selected["account_number"],
                "selected_profile_name": selected["profile_name"],
                "next_account_number": (
                    active_indices.index(self._current_index) + 1
                    if self._selection_mode == "auto"
                    else None
                ),
                "previous_selection": previous_descriptor,
                "effective_for_new_requests": True,
                "persistence_enabled": self._selection_state_path is not None,
                "workspaces": self._workspace_summaries_unlocked(),
                "accounts": [
                    self._account_summary_unlocked(index, now)
                    for index in active_indices
                ],
            }

    def switch_workspace(self, selector: str) -> dict[str, Any]:
        """Pin new requests to one workspace and restore account auto-rotation there."""
        with self._lock:
            previous = (
                self._workspace_key,
                self._selection_mode,
                self._selected_index,
            )
            workspace_key = self._resolve_workspace_key_unlocked(selector)
            self._previous_selection = previous
            self._set_workspace_unlocked(workspace_key)
            self._persist_selection_state_unlocked()
        return self.get_selection_summary()

    def switch_account(
        self, *, mode: str, selector: str | None = None
    ) -> dict[str, Any]:
        normalized_mode = str(mode or "").strip().lower()
        if normalized_mode not in {"auto", "pinned"}:
            raise ValueError("Account selection mode must be 'auto' or 'pinned'")

        with self._lock:
            previous = (
                self._workspace_key,
                self._selection_mode,
                self._selected_index,
            )
            if normalized_mode == "auto":
                self._previous_selection = previous
                self._selection_mode = "auto"
                self._selected_index = self._current_index
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
            current = (
                self._workspace_key,
                self._selection_mode,
                self._selected_index,
            )
            previous_workspace, previous_mode, previous_index = self._previous_selection
            self._set_workspace_unlocked(previous_workspace)
            self._selection_mode = previous_mode
            self._selected_index = previous_index
            self._previous_selection = current
            self._persist_selection_state_unlocked()
        return self.get_selection_summary()

    def get_workspace_account_count(self, selector: str = "") -> int:
        with self._lock:
            workspace_key = (
                self._resolve_workspace_key_unlocked(selector)
                if str(selector or "").strip()
                else self._workspace_key
            )
            return len(self._workspace_indices_for_key(workspace_key))

    def get_status_summary(self) -> Dict[str, int]:
        now = time.time()
        with self._lock:
            indices = self._active_workspace_indices_unlocked()
            active = sum(1 for index in indices if self.cooldown_until[index] <= now)
            cooling = len(indices) - active
            return {
                "total": len(indices),
                "active": active,
                "cooling": cooling,
            }

    def get_governance_summary(self) -> dict:
        with self._lock:
            indices = self._active_workspace_indices_unlocked()
            receipts = [
                governance_receipt_from_client(self.clients[index]) for index in indices
            ]
            canonical = dict(receipts[0]) if receipts else {}
            canonical["aligned"] = bool(receipts) and all(
                receipt == receipts[0] and bool(receipt.get("aligned"))
                for receipt in receipts
            )
            canonical["account_count"] = len(receipts)
            canonical.update(self._workspace_descriptor_unlocked())
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
