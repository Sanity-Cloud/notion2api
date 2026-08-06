#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$StateRoot = 'X:\MCP\state\notion2api-hive',
    [switch]$NoPause
)

$ErrorActionPreference = 'Stop'

try {
    $receiptPath = Join-Path $StateRoot 'hive-workforce-governor-runtime.json'
    if (-not (Test-Path -LiteralPath $receiptPath -PathType Leaf)) {
        Write-Output 'service=hive-workforce-governor status=not-running'
        return
    }
    $receipt = Get-Content -LiteralPath $receiptPath -Raw | ConvertFrom-Json
    $process = Get-Process -Id ([int]$receipt.pid) -ErrorAction SilentlyContinue
    if ($process) {
        Stop-Process -Id $process.Id -Force
        $process.WaitForExit(5000)
    }
    Remove-Item -LiteralPath $receiptPath -Force -ErrorAction SilentlyContinue
    Write-Output "service=hive-workforce-governor pid=$($receipt.pid) status=stopped"
} finally {
    if (-not $NoPause) { Read-Host 'Press Enter to close' | Out-Null }
}
