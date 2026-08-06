from __future__ import annotations

import argparse
import json
import os
import signal
import threading
import time
from pathlib import Path
from typing import Any, Callable

from app.hive_materialization import get_hive_materialization_store
from app.hive_runtime import default_hive_runtime_db_path
from app.hive_workforce_control_plane import HiveWorkforceControlPlaneStore


class HiveWorkforceGovernor:
    """Independent workforce-governance loop; the portal is never the scheduler."""

    def __init__(
        self,
        *,
        db_path: str | Path | None = None,
        actor: str = "aigentbee-workforce-governor",
        governance_authorization: dict[str, Any] | None = None,
    ) -> None:
        self.db_path = Path(db_path or default_hive_runtime_db_path()).expanduser().resolve()
        self.actor = str(actor or "aigentbee-workforce-governor").strip()
        self.materialization = get_hive_materialization_store(self.db_path)
        self.control_plane: HiveWorkforceControlPlaneStore = (
            self.materialization.control_plane
        )
        self.governance_authorization = dict(governance_authorization or {})

    @staticmethod
    def _run_lane(
        result: dict[str, Any],
        lane_name: str,
        operation: Callable[[], Any],
    ) -> Any | None:
        try:
            value = operation()
            result[lane_name] = (
                value.model_dump(mode="json")
                if hasattr(value, "model_dump")
                else value
            )
            result["lane_statuses"][lane_name] = "COMPLETED"
            return value
        except Exception as exc:
            result[lane_name] = {"ok": False, "error": str(exc)}
            result["lane_statuses"][lane_name] = "FAILED"
            result["errors"].append({"lane": lane_name, "error": str(exc)})
            return None

    def run_once(self) -> dict[str, Any]:
        started_at = int(time.time() * 1000)
        policy = self.control_plane.get_policy()
        result: dict[str, Any] = {
            "started_at": started_at,
            "policy_revision": policy.revision,
            "lease_reconciliation": {},
            "recruitment": {},
            "workforce_audit": {},
            "lane_statuses": {},
            "errors": [],
            "status": "COMPLETED",
            "error": "",
        }

        lease_result = self._run_lane(
            result,
            "lease_reconciliation",
            lambda: self.materialization.reconcile_stale_leases(
                actor=self.actor,
                dry_run=False,
                heartbeat_stale_after_seconds=policy.stale_heartbeat_seconds,
                no_heartbeat_grace_seconds=policy.no_heartbeat_grace_seconds,
                revoke=False,
            ),
        )
        self._run_lane(
            result,
            "recruitment",
            lambda: self.control_plane.process_recruitment_queue(
                actor=self.actor,
                governance_authorization=self.governance_authorization,
                limit=25,
            ),
        )
        audit_result = self._run_lane(
            result,
            "workforce_audit",
            lambda: self.materialization.audit_workforce(
                actor=self.actor,
                dry_run=not policy.auto_offboard_enabled,
                stale_after_days=30,
                include_protected=False,
                governance_authorization=(
                    self.governance_authorization
                    if policy.auto_offboard_enabled
                    else None
                ),
            ),
        )

        failed_lanes = sum(
            status == "FAILED" for status in result["lane_statuses"].values()
        )
        if failed_lanes == len(result["lane_statuses"]):
            result["status"] = "FAILED"
        elif failed_lanes:
            result["status"] = "PARTIAL"
        result["error"] = "; ".join(
            f"{item['lane']}: {item['error']}" for item in result["errors"]
        )

        run_id = self.control_plane.record_governor_run(
            started_at=started_at,
            status=result["status"],
            lease_changes=(lease_result.changed_count if lease_result else 0),
            audit_findings=(audit_result.finding_count if audit_result else 0),
            audit_actions=(audit_result.action_count if audit_result else 0),
            error=result["error"],
        )
        result["run_id"] = run_id
        result["finished_at"] = int(time.time() * 1000)
        return result

    def serve(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            policy = self.control_plane.get_policy()
            self.run_once()
            interval = max(60, int(policy.lease_reconcile_interval_seconds))
            stop_event.wait(interval)


def _load_authorization() -> dict[str, Any]:
    path = str(os.getenv("HIVE_WORKFORCE_GOVERNANCE_AUTH_FILE", "")).strip()
    if not path:
        return {}
    source = Path(path).expanduser().resolve()
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Governance authorization file must contain a JSON object.")
    return payload


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the independent AIgentBee Hive workforce governor."
    )
    parser.add_argument("--db", default=str(default_hive_runtime_db_path()))
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--actor", default="aigentbee-workforce-governor")
    args = parser.parse_args()

    governor = HiveWorkforceGovernor(
        db_path=args.db,
        actor=args.actor,
        governance_authorization=_load_authorization(),
    )
    if args.once:
        print(json.dumps(governor.run_once(), indent=2, sort_keys=True))
        return 0

    stop_event = threading.Event()

    def _stop(_signum: int, _frame: Any) -> None:
        stop_event.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)
    governor.serve(stop_event)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
