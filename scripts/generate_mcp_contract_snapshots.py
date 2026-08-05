from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Iterator

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

os.environ.setdefault(
    "NOTION_ACCOUNTS",
    json.dumps(
        [
            {
                "profile_name": "contract-snapshot",
                "token_v2": "test-token",
                "space_id": "test-space",
                "user_id": "test-user",
                "space_view_id": "test-view",
                "user_name": "Contract Snapshot",
                "user_email": "snapshot@example.invalid",
            }
        ]
    ),
)

from app.mcp_server import create_server  # noqa: E402

SCHEMA_VERSION = 1
PROFILE_ENV_KEYS = (
    "MCP_SERVER_NAME",
    "MCP_TOOL_PREFIX",
    "SANITYCLOUD_TOOL_NAMESPACE",
    "SANITYCLOUD_INVOCATION_ALIAS",
)
PROFILE_CONFIGS: dict[str, dict[str, str]] = {
    "notion2api": {
        "MCP_SERVER_NAME": "notion2api",
        "MCP_TOOL_PREFIX": "",
        "SANITYCLOUD_TOOL_NAMESPACE": "",
        "SANITYCLOUD_INVOCATION_ALIAS": "",
    },
    "aigentbee": {
        "MCP_SERVER_NAME": "AIgentBee",
        "MCP_TOOL_PREFIX": "aigentbee",
        "SANITYCLOUD_TOOL_NAMESPACE": "A!",
        "SANITYCLOUD_INVOCATION_ALIAS": "A!B",
    },
}


@contextmanager
def profile_environment(profile: str) -> Iterator[None]:
    config = PROFILE_CONFIGS[profile]
    previous = {key: os.environ.get(key) for key in PROFILE_ENV_KEYS}
    try:
        for key, value in config.items():
            os.environ[key] = value
        yield
    finally:
        for key, value in previous.items():
            if value is None:
                os.environ.pop(key, None)
            else:
                os.environ[key] = value


def _canonical_tool(tool: Any) -> dict[str, Any]:
    return tool.model_dump(mode="json", exclude_none=True)


def build_profile_contract(profile: str) -> dict[str, Any]:
    if profile not in PROFILE_CONFIGS:
        raise ValueError(f"Unknown MCP profile: {profile}")
    with profile_environment(profile):
        server = create_server(
            base_url="http://127.0.0.1:8120",
            api_key="contract-test-key",
            timeout=1,
            host="127.0.0.1",
            port=8130,
            mcp_path="/mcp",
        )
        tools = sorted(asyncio.run(server.list_tools()), key=lambda tool: tool.name)
        return {
            "schema_version": SCHEMA_VERSION,
            "profile": profile,
            "server_name": server.name,
            "instructions": server.instructions,
            "tool_count": len(tools),
            "tools": [_canonical_tool(tool) for tool in tools],
        }


def write_snapshots(output_dir: Path) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for profile in PROFILE_CONFIGS:
        destination = output_dir / f"{profile}.json"
        destination.write_text(
            json.dumps(build_profile_contract(profile), indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        written.append(destination)
    return written


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate deterministic client-visible MCP profile contract snapshots."
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=ROOT / "contracts" / "mcp",
    )
    args = parser.parse_args()
    for path in write_snapshots(args.output_dir.resolve()):
        print(path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
