from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def _check_js(source: str, tmp_path: Path, name: str) -> None:
    path = tmp_path / name
    path.write_text(source, encoding="utf-8", newline="\n")
    completed = subprocess.run(
        ["node", "--check", str(path)],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr


def test_modular_and_inline_consumers_share_fail_closed_contract(tmp_path: Path) -> None:
    modular = (ROOT / "frontend/js/chat/streaming.js").read_text(encoding="utf-8")
    index = (ROOT / "frontend/index.html").read_text(encoding="utf-8")
    embed = (ROOT / "frontend/embed.html").read_text(encoding="utf-8")
    app = (ROOT / "frontend/js/core/app.js").read_text(encoding="utf-8")

    for source in (modular, index, embed):
        for marker in (
            "stream_error",
            "content_filter",
            "function_call",
            "finishCount",
            "[DONE]",
            "incomplete_terminal_state",
        ):
            assert marker in source

    assert len(re.findall(r"^\s*createTerminalState\(\)\s*\{", modular, re.MULTILINE)) == 1
    assert len(re.findall(r"^\s*createTerminalState\(\)\s*\{", index, re.MULTILINE)) == 1
    assert "while (!terminalState.done && !terminalState.failure)" in modular
    assert "while(!terminalState.done&&!terminalState.failure)" in index
    assert "while(!terminal.done&&!terminal.failure)" in embed
    assert "updateAIMessage(aiWrapper, '', false)" in modular
    assert "updateAIMessage(aiWrapper,'',false)" in index
    assert "status.textContent=error.name==='AbortError'?'Cancelled':'Failed'" in embed

    await_index = app.index("await window.NotionAI.Chat.Streaming.streamResponse")
    history_index = app.index("role: 'assistant'")
    assert await_index < history_index
    assert "if (err.name !== 'AbortError')" in app

    index_script = re.search(
        r"<script>\s*/\* ===== STREAMING ===== \*/(.*?)</script>",
        index,
        re.DOTALL,
    )
    assert index_script
    _check_js(index_script.group(1), tmp_path, "index-streaming.js")

    embed_scripts = re.findall(r"<script>(.*?)</script>", embed, re.DOTALL)
    assert embed_scripts
    _check_js(embed_scripts[-1], tmp_path, "embed.js")
