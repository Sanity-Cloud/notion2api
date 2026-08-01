#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$RepoRoot = 'X:\Code\notion2api',
    [string]$DbPath = 'X:\MCP\state\notion2api-hive\hive_runtime.sqlite3',
    [string]$StateRoot = 'X:\MCP\state\notion2api-hive',
    [string]$LogRoot = 'X:\MCP\logs\notion2api-hive',
    [string]$PythonPath = 'C:\Program Files\Python310\python.exe',
    [string]$HostAddress = '127.0.0.1',
    [int]$Port = 8162,
    [switch]$Force,
    [switch]$NoPause
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-HiveOverviewListener {
    Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
        Select-Object -First 1
}

try {
    $entry = Join-Path $RepoRoot 'app\hive_overview.py'
    if (-not (Test-Path -LiteralPath $entry)) {
        throw "Hive Overview entrypoint not found: $entry"
    }
    if (-not (Test-Path -LiteralPath $DbPath)) {
        throw "Hive runtime database not found: $DbPath"
    }
    if (-not (Test-Path -LiteralPath $PythonPath)) {
        throw "Python executable not found: $PythonPath"
    }

    $listener = Get-HiveOverviewListener
    if ($listener) {
        $pidValue = [int]$listener.OwningProcess
        try {
            $health = Invoke-RestMethod -Uri "http://$HostAddress`:$Port/health" -TimeoutSec 3
        } catch {
            $health = $null
        }
        if ($health -and $health.service -eq 'hive-overview') {
            Write-Output "service=hive-overview already_running pid=$pidValue port=$Port"
            return
        }
        $process = Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue"
        $commandLine = [string]$process.CommandLine
        if (-not $Force) {
            throw "Port $Port is already owned by PID $pidValue. Use -Force only for a verified Hive Overview process."
        }
        if ($commandLine -notmatch '(?i)app\.hive_overview') {
            throw "Refusing to stop PID $pidValue because it is not a verified Hive Overview process."
        }
        Stop-Process -Id $pidValue -Force -ErrorAction Stop
        Start-Sleep -Milliseconds 600
    }

    New-Item -ItemType Directory -Force -Path $StateRoot, $LogRoot | Out-Null
    $stdout = Join-Path $LogRoot 'hive-overview-8162.out.log'
    $stderr = Join-Path $LogRoot 'hive-overview-8162.err.log'
    $env:NOTION2API_HIVE_RUNTIME_DB_PATH = $DbPath
    $startParameters = @{
        FilePath = $PythonPath
        WorkingDirectory = $RepoRoot
        ArgumentList = @(
            '-m', 'uvicorn', 'app.hive_overview:app',
            '--host', $HostAddress,
            '--port', [string]$Port,
            '--no-access-log'
        )
        RedirectStandardOutput = $stdout
        RedirectStandardError = $stderr
        WindowStyle = 'Hidden'
        PassThru = $true
    }
    $process = Start-Process @startParameters

    $ready = $false
    $health = $null
    foreach ($attempt in 1..60) {
        Start-Sleep -Milliseconds 300
        if ($process.HasExited) { break }
        try {
            $health = Invoke-RestMethod -Uri "http://$HostAddress`:$Port/health" -TimeoutSec 2
            if ($health.service -eq 'hive-overview' -and $health.status -eq 'ok') {
                $ready = $true
                break
            }
        } catch {}
    }
    if (-not $ready) {
        Stop-Process -Id $process.Id -Force -ErrorAction SilentlyContinue
        throw "Hive Overview failed health verification. See $stderr"
    }

    $receipt = [ordered]@{
        service = 'hive-overview'
        started_at = (Get-Date).ToString('o')
        repo = $RepoRoot
        commit = (git -C $RepoRoot rev-parse HEAD).Trim()
        db_path = $DbPath
        pid = $process.Id
        host = $HostAddress
        port = $Port
        read_only = $true
        mission_count = $health.mission_count
    }
    $receiptPath = Join-Path $StateRoot 'hive-overview-runtime.json'
    $receipt | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
    Write-Output "service=hive-overview pid=$($process.Id) port=$Port health=ok read_only=true"
} finally {
    if (-not $NoPause) { Read-Host 'Press Enter to close' | Out-Null }
}

