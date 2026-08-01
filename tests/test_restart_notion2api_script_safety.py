from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "restart_notion2api_backend_and_mcp.ps1"


def test_restart_script_requires_pwsh7_and_avoids_unqualified_tree_kill() -> None:
    text = SCRIPT.read_text(encoding="utf-8-sig")
    assert text.startswith("#Requires -Version 7.0")
    assert "taskkill" not in text.lower()
    assert "Stop-VerifiedListener" in text
    assert "Test-ExpectedBackendProcess" in text
    assert "Test-ExpectedMcpProcess" in text


def test_restart_script_verifies_turnover_and_writes_receipt() -> None:
    text = SCRIPT.read_text(encoding="utf-8-sig")
    assert "backend PID did not turn over" in text
    assert "MCP PID did not turn over" in text
    assert "restart-receipt.json" in text
    assert "Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM" in text


def test_restart_script_uses_discrete_process_arguments() -> None:
    text = SCRIPT.read_text(encoding="utf-8-sig")
    assert "ArgumentList = @(" in text
    assert "Start-Process @backendParameters" in text
    assert "Get-Command 'pwsh.exe'" in text
    assert "-Notion2ApiRoot $RepoRoot" in text
