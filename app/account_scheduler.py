"""Adaptive selection among capacity-role accounts within one workspace."""

from __future__ import annotations

import hashlib
import os
import random
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from app.account_capacity import (
    CapacityRole,
    canary_fraction,
    is_development_workload,
    production_auto_route_roles,
)
from app.account_health import AccountHealthSignal, score_account_health


@dataclass(frozen=True)
class RoutingDecision:
    selected_index: int
    reason: str
    candidates: list[dict[str, Any]]
    workload_class: str = "production"
    canary_included: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "selected_index": self.selected_index,
            "reason": self.reason,
            "candidates": list(self.candidates),
            "workload_class": self.workload_class,
            "canary_included": self.canary_included,
        }


def _stable_unit_interval(seed: str) -> float:
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return int(digest[:8], 16) / 0xFFFFFFFF


def _candidate_payload(
    index: int,
    role: CapacityRole,
    health: AccountHealthSignal,
) -> dict[str, Any]:
    return {
        "index": index,
        "capacity_role": role.value,
        "account_alias": health.account_alias or role.value.capitalize(),
        "health_score": health.health_score,
        "health_reason": health.health_reason,
        "available": health.available,
        "inflight": health.inflight,
        "cooldown_remaining_seconds": health.cooldown_remaining_seconds,
        "retry_after_seconds": health.retry_after_seconds,
    }


def select_account_index(
    *,
    workspace_indices: Sequence[int],
    roles_by_index: Mapping[int, CapacityRole],
    health_by_index: Mapping[int, AccountHealthSignal],
    workload: Mapping[str, Any] | None = None,
    sticky_index: int | None = None,
    explicit_index: int | None = None,
    fairness_cursor: int | None = None,
    rng: random.Random | None = None,
    route_seed: str = "",
) -> RoutingDecision:
    """Choose an account for new work using health, role policy, and optional stickiness."""

    workload = dict(workload or {})
    workload_class = str(workload.get("workload_class") or "production")
    development = is_development_workload(workload)

    if sticky_index is not None and sticky_index in workspace_indices:
        health = health_by_index.get(sticky_index)
        role = roles_by_index.get(sticky_index, CapacityRole.ALPHA)
        return RoutingDecision(
            selected_index=sticky_index,
            reason="sticky_thread_binding",
            candidates=[
                _candidate_payload(
                    sticky_index,
                    role,
                    health
                    or score_account_health(
                        account_key=str(sticky_index),
                        capacity_role=role.value,
                    ),
                )
            ],
            workload_class=workload_class,
        )

    if explicit_index is not None:
        if explicit_index not in workspace_indices:
            raise ValueError("Explicit account is outside the selected workspace")
        health = health_by_index.get(explicit_index)
        role = roles_by_index.get(explicit_index, CapacityRole.ALPHA)
        return RoutingDecision(
            selected_index=explicit_index,
            reason="explicit_account_selection",
            candidates=[
                _candidate_payload(
                    explicit_index,
                    role,
                    health
                    or score_account_health(
                        account_key=str(explicit_index),
                        capacity_role=role.value,
                    ),
                )
            ],
            workload_class=workload_class,
        )

    production_roles = production_auto_route_roles()
    eligible: list[tuple[int, CapacityRole, AccountHealthSignal]] = []
    canary_candidates: list[tuple[int, CapacityRole, AccountHealthSignal]] = []
    all_payloads: list[dict[str, Any]] = []

    for index in workspace_indices:
        role = roles_by_index.get(index, CapacityRole.ALPHA)
        health = health_by_index.get(index) or score_account_health(
            account_key=str(index),
            capacity_role=role.value,
        )
        payload = _candidate_payload(index, role, health)
        all_payloads.append(payload)
        if not health.available:
            continue
        if role == CapacityRole.DEV:
            if development:
                eligible.append((index, role, health))
            continue
        if role == CapacityRole.CANARY:
            canary_candidates.append((index, role, health))
            continue
        if role in production_roles:
            eligible.append((index, role, health))

    canary_included = False
    fraction = canary_fraction()
    if canary_candidates and fraction > 0:
        seed = route_seed or str(workload.get("route_seed") or "")
        draw = (
            _stable_unit_interval(seed)
            if seed
            else (rng or random.Random()).random()
        )
        if draw < fraction:
            eligible.extend(canary_candidates)
            canary_included = True

    if not eligible and development and canary_candidates:
        # Development workloads may use canary when no Dev account is available.
        eligible.extend(canary_candidates)
        canary_included = True

    if not eligible:
        # Fail soft to the least-unhealthy production peer rather than invent capacity.
        fallback = [
            (index, roles_by_index.get(index, CapacityRole.ALPHA), health_by_index[index])
            for index in workspace_indices
            if index in health_by_index
            and roles_by_index.get(index) in production_roles
        ]
        if not fallback:
            fallback = [
                (
                    index,
                    roles_by_index.get(index, CapacityRole.ALPHA),
                    health_by_index.get(index)
                    or score_account_health(
                        account_key=str(index),
                        capacity_role=roles_by_index.get(index, CapacityRole.ALPHA).value,
                    ),
                )
                for index in workspace_indices
            ]
        fallback.sort(key=lambda item: (-item[2].health_score, item[0]))
        chosen = fallback[0]
        return RoutingDecision(
            selected_index=chosen[0],
            reason="fallback_least_unhealthy_production",
            candidates=all_payloads,
            workload_class=workload_class,
            canary_included=False,
        )

    eligible.sort(key=lambda item: (-item[2].health_score, item[2].inflight, item[0]))
    best_score = eligible[0][2].health_score
    top = [item for item in eligible if item[2].health_score >= best_score - 0.05]
    if len(top) == 1:
        chosen = top[0]
    else:
        # Fairness among equal-health peers: advance from the workspace cursor.
        ordered_indices = [item[0] for item in top]
        start = 0
        if fairness_cursor in ordered_indices:
            start = ordered_indices.index(fairness_cursor)
        elif fairness_cursor in workspace_indices:
            # Prefer the first peer at/after the cursor in workspace order.
            workspace_order = [idx for idx in workspace_indices if idx in ordered_indices]
            for offset, idx in enumerate(workspace_order):
                position = workspace_indices.index(idx)
                cursor_position = workspace_indices.index(fairness_cursor)
                if position >= cursor_position:
                    start = ordered_indices.index(idx)
                    break
            else:
                start = 0
        chosen_index = ordered_indices[start % len(ordered_indices)]
        chosen = next(item for item in top if item[0] == chosen_index)

    reason = "health_aware_selection"
    if canary_included and chosen[1] == CapacityRole.CANARY:
        reason = "canary_fraction_selection"
    elif chosen[1] == CapacityRole.DEV:
        reason = "development_workload_selection"

    return RoutingDecision(
        selected_index=chosen[0],
        reason=reason,
        candidates=all_payloads,
        workload_class=workload_class,
        canary_included=canary_included,
    )


def selection_mode_label() -> str:
    return os.getenv("NOTION_ACCOUNT_SELECTION_POLICY", "health_aware").strip() or "health_aware"
