#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$RepoRoot = 'X:\Code\notion2api',
    [string]$BackendHost = '127.0.0.1',
    [int]$BackendPort = 8120,
    [string]$McpHost = '0.0.0.0',
    [int]$McpPort = 8130,
    [string]$McpPath = '/mcp',
    [string]$StateRoot = 'X:\MCP\state\notion2api-primary',
    [string]$LogRoot = 'X:\MCP\logs',
    [string]$McpStartScript = 'X:\Code\sanity-cloud-ai-portal\scripts\start-notion2api-mcp.ps1',
    [switch]$NoPause
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

function Get-PortListener {
    param([int]$Port)
    return Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
        Select-Object -First 1
}

function Get-ProcessCommandLine {
    param([int]$ProcessId)
    $process = Get-CimInstance Win32_Process -Filter "ProcessId=$ProcessId" -ErrorAction Stop
    return [string]$process.CommandLine
}

function Test-ExpectedBackendProcess {
    param([int]$ProcessId, [int]$Port)
    $commandLine = Get-ProcessCommandLine -ProcessId $ProcessId
    if ([string]::IsNullOrWhiteSpace($commandLine)) {
        # Some hosts redact Win32 CommandLine; fall back to health shape.
        try {
            $health = Invoke-RestMethod -Uri "http://127.0.0.1:$Port/health" -TimeoutSec 2
            return ($health.status -eq 'ok' -or $health.ok)
        } catch {
            return $false
        }
    }
    return (
        $commandLine -match '(?i)(^|\s)-m\s+uvicorn(\.main:main)?(\s|$)' -and
        $commandLine -match '(?i)(^|\s)app\.server:app(\s|$)' -and
        $commandLine -match "(?i)(^|\s)--port\s+$Port(\s|$)"
    )
}

function Test-ExpectedMcpProcess {
    param([int]$ProcessId, [int]$Port)
    $commandLine = Get-ProcessCommandLine -ProcessId $ProcessId
    if ([string]::IsNullOrWhiteSpace($commandLine)) {
        $process = Get-Process -Id $ProcessId -ErrorAction SilentlyContinue
        return ($null -ne $process -and $process.ProcessName -match '(?i)python')
    }
    return (
        $commandLine -match '(?i)(^|\s)-m\s+app\.mcp_server(\s|$)' -and
        $commandLine -match "(?i)(^|\s)--port\s+$Port(\s|$)"
    )
}

function Stop-VerifiedListener {
    param(
        [int]$Port,
        [ValidateSet('backend', 'mcp')]
        [string]$Role
    )

    $listener = Get-PortListener -Port $Port
    if (-not $listener) {
        return $null
    }

    $processId = [int]$listener.OwningProcess
    $verified = if ($Role -eq 'backend') {
        Test-ExpectedBackendProcess -ProcessId $processId -Port $Port
    }
    else {
        Test-ExpectedMcpProcess -ProcessId $processId -Port $Port
    }
    if (-not $verified) {
        $commandLine = Get-ProcessCommandLine -ProcessId $processId
        throw "Refusing to stop PID $processId on port $Port because it is not the expected Notion2API $Role process. CommandLine=$commandLine"
    }

    Stop-Process -Id $processId -Force -ErrorAction Stop
    foreach ($attempt in 1..40) {
        Start-Sleep -Milliseconds 250
        if (-not (Get-Process -Id $processId -ErrorAction SilentlyContinue) -and -not (Get-PortListener -Port $Port)) {
            return $processId
        }
    }
    throw "Notion2API $Role PID $processId did not terminate cleanly or port $Port remained occupied."
}

function Wait-HttpHealthy {
    param([string]$Url, [int]$Attempts = 60)
    foreach ($attempt in 1..$Attempts) {
        Start-Sleep -Milliseconds 300
        try {
            $response = Invoke-WebRequest -Uri $Url -UseBasicParsing -TimeoutSec 2
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                return $true
            }
        }
        catch {}
    }
    return $false
}

function Resolve-BackendPython {
    param([string]$Root)
    $candidates = @(
        (Join-Path $Root '.venv\Scripts\python.exe'),
        'C:\Program Files\Python310\python.exe'
    )
    foreach ($candidate in $candidates) {
        if (Test-Path -LiteralPath $candidate) {
            return $candidate
        }
    }
    throw 'No validated Notion2API backend Python was found.'
}

try {
    if (-not (Test-Path -LiteralPath $RepoRoot)) {
        throw "Notion2API repository not found: $RepoRoot"
    }
    if (-not (Test-Path -LiteralPath $McpStartScript)) {
        throw "Notion2API MCP start script not found: $McpStartScript"
    }

    New-Item -ItemType Directory -Force -Path $LogRoot, $StateRoot | Out-Null
    $before = [ordered]@{
        backend_pid = $null
        mcp_pid = $null
    }
    $backendListener = Get-PortListener -Port $BackendPort
    if ($backendListener) { $before.backend_pid = [int]$backendListener.OwningProcess }
    $mcpListener = Get-PortListener -Port $McpPort
    if ($mcpListener) { $before.mcp_pid = [int]$mcpListener.OwningProcess }

    $stoppedMcpPid = Stop-VerifiedListener -Port $McpPort -Role mcp
    $stoppedBackendPid = Stop-VerifiedListener -Port $BackendPort -Role backend

    $sharedAdmissionState = 'X:\MCP\state\notion2api-shared'
    New-Item -ItemType Directory -Force -Path $sharedAdmissionState, $StateRoot | Out-Null
    $env:NOTION_ACCOUNT_SELECTION_STATE = Join-Path $StateRoot 'account-selection.json'
    $env:NOTION_ADMISSION_DB_PATH = Join-Path $sharedAdmissionState 'notion-admission.sqlite3'
    # Align with multi-account hive fleets; one-inflight-per-thread remains in code.
    $env:NOTION_ADMISSION_ACCOUNT_MAX_INFLIGHT = '4'
    $env:NOTION_ADMISSION_ACCOUNT_CAPACITY = '4'
    $env:NOTION_ADMISSION_ACCOUNT_REFILL_PER_SECOND = '1.0'
    $env:CHAT_HISTORY_DB_DIR = Join-Path $StateRoot 'chat_history'
    if (-not $env:CHAT_HISTORY_DB_PATH) {
        $env:CHAT_HISTORY_DB_PATH = Join-Path $StateRoot 'chat_history.db'
    }

    $python = Resolve-BackendPython -Root $RepoRoot
    $backendOutLog = Join-Path $LogRoot "notion2api-backend-$BackendPort.out.log"
    $backendErrLog = Join-Path $LogRoot "notion2api-backend-$BackendPort.err.log"
    $backendParameters = @{
        FilePath = $python
        WorkingDirectory = $RepoRoot
        ArgumentList = @(
            '-m', 'uvicorn', 'app.server:app',
            '--host', $BackendHost,
            '--port', [string]$BackendPort
        )
        RedirectStandardOutput = $backendOutLog
        RedirectStandardError = $backendErrLog
        WindowStyle = 'Hidden'
        PassThru = $true
    }
    $backendProcess = Start-Process @backendParameters
    $backendHealth = "http://$BackendHost`:$BackendPort/health"
    if (-not (Wait-HttpHealthy -Url $backendHealth)) {
        Stop-Process -Id $backendProcess.Id -Force -ErrorAction SilentlyContinue
        throw "Notion2API backend did not become healthy at $backendHealth. Check $backendErrLog"
    }
    $backendAfter = Get-PortListener -Port $BackendPort
    if (-not $backendAfter) {
        throw "Notion2API backend health responded but no listener owns port $BackendPort."
    }
    $backendPid = [int]$backendAfter.OwningProcess
    if (-not (Test-ExpectedBackendProcess -ProcessId $backendPid -Port $BackendPort)) {
        throw "The new listener on port $BackendPort is not the expected Notion2API backend process."
    }
    if ($before.backend_pid -and $backendPid -eq $before.backend_pid) {
        throw "Notion2API backend PID did not turn over: $backendPid"
    }

    $pwsh = (Get-Command 'pwsh.exe' -ErrorAction Stop).Source
    & $pwsh -NoProfile -ExecutionPolicy Bypass -File $McpStartScript `
        -BaseUrl "http://$BackendHost`:$BackendPort" `
        -HostName $McpHost `
        -Port $McpPort `
        -McpPath $McpPath `
        -Notion2ApiRoot $RepoRoot `
        -NoPause
    if ($LASTEXITCODE -ne 0) {
        throw "Notion2API MCP restart failed with exit code $LASTEXITCODE."
    }

    $mcpAfter = $null
    foreach ($attempt in 1..60) {
        Start-Sleep -Milliseconds 300
        $mcpAfter = Get-PortListener -Port $McpPort
        if ($mcpAfter) { break }
    }
    if (-not $mcpAfter) {
        throw "Notion2API MCP did not open port $McpPort."
    }
    $mcpPid = [int]$mcpAfter.OwningProcess
    if (-not (Test-ExpectedMcpProcess -ProcessId $mcpPid -Port $McpPort)) {
        throw "The new listener on port $McpPort is not the expected Notion2API MCP process."
    }
    if ($before.mcp_pid -and $mcpPid -eq $before.mcp_pid) {
        throw "Notion2API MCP PID did not turn over: $mcpPid"
    }

    $receipt = [ordered]@{
        service = 'notion2api-primary'
        restarted_at = (Get-Date).ToString('o')
        repo = $RepoRoot
        commit = (git -C $RepoRoot rev-parse HEAD).Trim()
        backend = [ordered]@{
            host = $BackendHost
            port = $BackendPort
            previous_pid = $before.backend_pid
            stopped_pid = $stoppedBackendPid
            pid = $backendPid
            health = $backendHealth
        }
        mcp = [ordered]@{
            host = $McpHost
            port = $McpPort
            path = $McpPath
            previous_pid = $before.mcp_pid
            stopped_pid = $stoppedMcpPid
            pid = $mcpPid
        }
        powershell = $PSVersionTable.PSVersion.ToString()
    }
    $receiptPath = Join-Path $StateRoot 'restart-receipt.json'
    $receipt | ConvertTo-Json -Depth 8 | Set-Content -LiteralPath $receiptPath -Encoding utf8NoBOM
    $receipt | ConvertTo-Json -Depth 8
}
finally {
    if (-not $NoPause) {
        Read-Host 'Press Enter to close' | Out-Null
    }
}
