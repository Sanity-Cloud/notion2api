"""Pinned TencentDB Agent Memory upstream contract observations.

No credential is accepted from a worker-facing request.  A trusted runtime may
inject a preconfigured upstream client after Session Broker capability checks.
"""

from __future__ import annotations

from typing import Any, Protocol

from .models import UPSTREAM_BRANCH, UPSTREAM_COMMIT, UPSTREAM_PYTHON_SDK_VERSION


class UpstreamMemoryClient(Protocol):
    def search_atomic(self, query: str, *, limit: int | None = None) -> dict[str, Any]: ...

    def read_scenario(self, path: str) -> dict[str, Any]: ...

    def read_core(self) -> dict[str, Any]: ...


UPSTREAM_CONTRACT = {
    "repository": "TencentCloud/TencentDB-Agent-Memory",
    "branch": UPSTREAM_BRANCH,
    "commit": UPSTREAM_COMMIT,
    "python_sdk_version": UPSTREAM_PYTHON_SDK_VERSION,
    "observed_v3_isolation": {
        "required": ["team_id", "agent_id", "user_id"],
        "conversation_write_requires": ["session_id"],
        "optional": ["task_id"],
    },
    "observed_read_routes": [
        "POST /v3/conversation/query",
        "POST /v3/conversation/search",
        "POST /v3/atomic/query",
        "POST /v3/atomic/search",
        "POST /v3/scenario/ls",
        "POST /v3/scenario/read",
        "POST /v3/core/read",
    ],
    "pilot_denied_routes": [
        "POST /v3/conversation/delete",
        "POST /v3/atomic/delete",
        "POST /v3/scenario/rm",
        "POST /v3/core/write",
        "POST /v3/knowledge/delete",
    ],
}
