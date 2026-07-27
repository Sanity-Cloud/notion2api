from __future__ import annotations

# ruff: noqa: E402

import asyncio
import json
import os
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

REPO_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault(
    "NOTION_ACCOUNTS",
    '[{"token_v2":"fixture","space_id":"fixture","user_id":"fixture"}]',
)

import httpx
import uvicorn
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from app.api import chat
from app.mcp_server import Notion2APIClient

ROOT = REPO_ROOT
ARTIFACT_DIR = ROOT / "artifacts" / "hive-stream-round2-fixture-validation-20260727" / "smoke"


@dataclass
class SmokeSource(Iterator[str]):
    chunks: list[str]
    terminal_error: BaseException | None = None
    close_error: bool = False
    index: int = 0
    close_calls: int = 0

    def __iter__(self) -> "SmokeSource":
        return self

    def __next__(self) -> str:
        if self.index < len(self.chunks):
            value = self.chunks[self.index]
            self.index += 1
            return value
        if self.terminal_error is not None:
            error = self.terminal_error
            self.terminal_error = None
            raise error
        raise StopIteration

    def close(self) -> None:
        self.close_calls += 1
        if self.close_error:
            raise RuntimeError("smoke close failure")


def _clean_chunks(text: str) -> list[str]:
    return [
        chat._build_stream_chunk("smoke-id", "smoke-model", role="assistant"),
        chat._build_stream_chunk("smoke-id", "smoke-model", content=text),
        chat._build_stream_chunk("smoke-id", "smoke-model", finish_reason="stop"),
        "data: [DONE]\n\n",
    ]


def create_app(source_registry: dict[str, SmokeSource]) -> FastAPI:
    app = FastAPI()

    @app.get("/health")
    async def health() -> dict[str, bool]:
        return {"ok": True}

    @app.post("/v1/chat/completions")
    async def completions(request: Request) -> StreamingResponse:
        payload = await request.json()
        case = str(payload.get("model") or "smoke-clean")
        if case == "smoke-clean":
            source = SmokeSource(_clean_chunks("smoke-ok"))
        elif case == "smoke-cleanup":
            source = SmokeSource(_clean_chunks("smoke-secret"), close_error=True)
        elif case == "smoke-interrupted":
            source = SmokeSource(
                [chat._build_stream_chunk("smoke-id", "smoke-model", content="partial-secret")],
                terminal_error=RuntimeError("smoke interruption"),
            )
        else:
            source = SmokeSource([])
        source_registry[case] = source
        stream = chat._guard_stream_until_integrity(
            source,
            response_id="smoke-id",
            model="smoke-model",
        )
        return StreamingResponse(
            stream,
            media_type="text/event-stream",
            headers={
                "X-Conversation-Id": "smoke-conversation",
                "X-Notion-Thread-Id": "smoke-thread",
            },
        )

    return app


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def wait_for_health(base_url: str) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        try:
            response = httpx.get(f"{base_url}/health", timeout=0.5)
            if response.status_code == 200:
                return
        except httpx.HTTPError:
            pass
        time.sleep(0.05)
    raise RuntimeError("isolated smoke server did not become healthy")


async def run_mcp_checks(base_url: str) -> dict[str, object]:
    client = Notion2APIClient(base_url, api_key="fixture", timeout=10)
    results: dict[str, object] = {}
    for case in ("smoke-clean", "smoke-cleanup", "smoke-interrupted"):
        progress: list[tuple[str, str, int, bool]] = []
        result = await client.post_chat_stream(
            "/v1/chat/completions",
            {"model": case, "messages": [{"role": "user", "content": "smoke"}]},
            lambda reasoning, content, count, final: progress.append(
                (reasoning, content, count, final)
            ),
        )
        results[case] = {"result": result, "progress": progress}

    clean = results["smoke-clean"]["result"]
    assert clean["ok"] is True
    assert clean["choices"][0]["message"]["content"] == "smoke-ok"
    assert clean["choices"][0]["finish_reason"] == "stop"
    assert clean["terminal_state"]["done_received"] is True
    assert len([row for row in results["smoke-clean"]["progress"] if row[-1] is True]) == 1

    cleanup = results["smoke-cleanup"]["result"]
    assert cleanup["ok"] is False
    assert cleanup["error"]["code"] == "ERR_STREAM_SOURCE_CLEANUP"
    assert cleanup["choices"] == []
    assert cleanup["partial_content"]["char_count"] == 0
    assert not [row for row in results["smoke-cleanup"]["progress"] if row[-1] is True]

    interrupted = results["smoke-interrupted"]["result"]
    assert interrupted["ok"] is False
    assert interrupted["error"]["code"] == "ERR_STREAM_INTERRUPTED"
    assert interrupted["choices"] == []
    assert interrupted["partial_content"]["char_count"] == 0
    return results


def main() -> int:
    registry: dict[str, SmokeSource] = {}
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    config = uvicorn.Config(
        create_app(registry),
        host="127.0.0.1",
        port=port,
        log_level="error",
        access_log=False,
    )
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    try:
        wait_for_health(base_url)
        raw: dict[str, str] = {}
        with httpx.Client(timeout=10) as client:
            for case in ("smoke-clean", "smoke-cleanup", "smoke-interrupted"):
                response = client.post(
                    f"{base_url}/v1/chat/completions",
                    json={"model": case, "messages": []},
                    headers={"Accept": "text/event-stream"},
                )
                response.raise_for_status()
                raw[case] = response.text

        assert "smoke-ok" in raw["smoke-clean"]
        assert "data: [DONE]" in raw["smoke-clean"]
        assert "smoke-secret" not in raw["smoke-cleanup"]
        assert "ERR_STREAM_SOURCE_CLEANUP" in raw["smoke-cleanup"]
        assert '"finish_reason": "error"' in raw["smoke-cleanup"]
        assert "data: [DONE]" not in raw["smoke-cleanup"]
        assert "partial-secret" not in raw["smoke-interrupted"]
        assert "ERR_STREAM_INTERRUPTED" in raw["smoke-interrupted"]
        assert "data: [DONE]" not in raw["smoke-interrupted"]

        mcp_results = asyncio.run(run_mcp_checks(base_url))
        node = subprocess.run(
            [
                "node",
                str(ROOT / "tests/stream_round2/isolated_browser_http_smoke.test.js"),
            ],
            cwd=ROOT,
            env={**os.environ, "SMOKE_BASE_URL": base_url},
            capture_output=True,
            text=True,
            check=False,
        )
        assert node.returncode == 0, node.stderr or node.stdout

        for case, source in registry.items():
            assert source.close_calls == 1, (case, source.close_calls)

        ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
        receipt = {
            "schema_version": "round2-isolated-http-smoke/1",
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "base_url": "http://127.0.0.1:<ephemeral>",
            "network_scope": "loopback-only",
            "existing_services_touched": False,
            "provider_calls": False,
            "cases": {
                "clean": {
                    "raw_done": True,
                    "mcp_ok": True,
                    "browser_parser_ok": True,
                },
                "cleanup_failure": {
                    "raw_done": False,
                    "raw_content_leaked": False,
                    "code": mcp_results["smoke-cleanup"]["result"]["error"]["code"],
                    "mcp_ok": False,
                    "browser_parser_failed_closed": True,
                },
                "interrupted": {
                    "raw_done": False,
                    "raw_content_leaked": False,
                    "code": mcp_results["smoke-interrupted"]["result"]["error"]["code"],
                    "mcp_ok": False,
                    "browser_parser_failed_closed": True,
                },
            },
            "close_counts": {case: source.close_calls for case, source in sorted(registry.items())},
            "node_stdout": node.stdout.strip(),
            "gate": "PASS",
            "deploy_authorized": False,
        }
        (ARTIFACT_DIR / "receipt.json").write_text(
            json.dumps(receipt, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        (ARTIFACT_DIR / "analysis.md").write_text(
            "# Round 2 isolated HTTP smoke\n\n"
            "Loopback-only uvicorn exercised the actual stream guard, HTTP SSE transport, MCP consumer, and modular browser parser. "
            "Clean output completed with exactly one finish and `[DONE]`. Cleanup failure and interruption emitted structured errors, "
            "withheld partial content, omitted `[DONE]`, and were rejected by both consumers. No existing service, provider, shared checkout, merge, or deployment was touched.\n",
            encoding="utf-8",
            newline="\n",
        )
        print(json.dumps(receipt, sort_keys=True))
        return 0
    finally:
        server.should_exit = True
        thread.join(timeout=10)
        if thread.is_alive():
            raise RuntimeError("isolated smoke server did not stop")


if __name__ == "__main__":
    raise SystemExit(main())
