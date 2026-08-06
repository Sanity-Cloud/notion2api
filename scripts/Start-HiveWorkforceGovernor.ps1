#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$RepoRoot = 'X:\Code\notion2api',
    [string]$DbPath = 'X:\MCP\state\notion2api-hive\hive_runtime.sqlite3',
    [string]$Python = 'X:\python313\python.exe',
    [string]$StateRoot = 'X:\MCP\state\notion2api-hive',
    [string]$LogRoot = 'X:\MCP\logs\notion2api-hive',
    [string]$GovernanceAuthorizationFile = '',
    [switch]$NoPause
)

$ErrorActionPreference = 'Stop'

try {
    if (-not (Test-Path -LiteralPath $RepoRoot -PathType Container)) {
        throw "Repository root not found: $RepoRoot"
    }
    if (-not (Test-Path -LiteralPath $Python -PathType Leaf)) {
        $repoPython = Join-Path $RepoRoot '.venv\Scripts\python.exe'
        if (Test-Path -LiteralPath $repoPython -PathType Leaf) {
            $Python = $repoPython
        } else {
            throw "Python executable not found: $Python"
        }
    }

    New-Item -ItemType Directory -Path $StateRoot -Force | Out-Null
    New-Item -ItemType Directory -Path $LogRoot -Force | Out-Null
    $receiptPath = Join-Path $StateRoot 'hive-workforce-governor-runtime.json'
    if (Test-Path -LiteralPath $receiptPath) {
        $prior = Get-Content -LiteralPath $receiptPath -Raw | ConvertFrom-Json
        $priorProcess = Get-Process -Id ([int]$prior.pid) -ErrorAction SilentlyContinue
        if ($priorProcess) {
            Write-Output "service=hive-workforce-governor pid=$($prior.pid) status=already-running"
            return
        }
    }

    $stdout = Join-Path $LogRoot 'hive-workforce-governor.out.log'
    $stderr = Join-Path $LogRoot 'hive-workforce-governor.err.log'
    $arguments = @('-m', 'app.hive_workforce_governor', '--db', $DbPath)
    $previousAuthFile = $env:HIVE_WORKFORCE_GOVERNANCE_AUTH_FILE
    if ($GovernanceAuthorizationFile) {
        if (-not (Test-Path -LiteralPath $GovernanceAuthorizationFile -PathType Leaf)) {
            throw "Governance authorization file not found: $GovernanceAuthorizationFile"
        }
        $env:HIVE_WORKFORCE_GOVERNANCE_AUTH_FILE = $GovernanceAuthorizationFile
    }
    try {
        $process = Start-Process -FilePath $Python `
            -ArgumentList $arguments `
            -WorkingDirectory $RepoRoot `
            -RedirectStandardOutput $stdout `
            -RedirectStandardError $stderr `
            -WindowStyle Hidden `
            -PassThru
    } finally {
        $env:HIVE_WORKFORCE_GOVERNANCE_AUTH_FILE = $previousAuthFile
    }

    Start-Sleep -Seconds 1
    if ($process.HasExited) {
        throw "Hive workforce governor exited during startup. See $stderr"
    }

    $receipt = [ordered]@{
        service = 'hive-workforce-governor'
        started_at = (Get-Date).ToString('o')
        repo = $RepoRoot
        commit = (git -C $RepoRoot rev-parse HEAD).Trim()
        db_path = $DbPath
        pid = $process.Id
        stdout = $stdout
        stderr = $stderr
        governance_authorization_file_configured = [bool]$GovernanceAuthorizationFile
        portal_dependent = $false
    }
    $receipt | ConvertTo-Json -Depth 5 | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
    Write-Output "service=hive-workforce-governor pid=$($process.Id) status=running portal_dependent=false"
} finally {
    if (-not $NoPause) { Read-Host 'Press Enter to close' | Out-Null }
}
