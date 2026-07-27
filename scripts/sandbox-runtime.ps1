[CmdletBinding()]
param(
    [ValidateSet("Initialize", "Install", "Start", "Stop", "Status", "Test", "Reset")]
    [string]$Action = "Status",
    [string]$RepositoryRoot = "",
    [string]$SourceAccountsFile = "X:\Code\notion2api\accounts.json",
    [string]$SandboxRoot = (Join-Path $env:LOCALAPPDATA "SanityCloud\notion2api-sandbox"),
    [int]$Port = 18120,
    [switch]$RemoveSecrets
)

$ErrorActionPreference = "Stop"
if (-not $RepositoryRoot) { $RepositoryRoot = Split-Path -Parent $PSScriptRoot }
$SecretsDir = Join-Path $SandboxRoot "secrets"
$AccountsFile = Join-Path $SecretsDir "accounts.json"
$ApiKeyFile = Join-Path $SecretsDir "api-key.txt"
$DataDir = Join-Path $SandboxRoot "data"
$LogsDir = Join-Path $SandboxRoot "logs"
$VenvDir = Join-Path $SandboxRoot "venv"
$Python = Join-Path $VenvDir "Scripts\python.exe"
$PidFile = Join-Path $SandboxRoot "sandbox.pid"
$StdoutLog = Join-Path $LogsDir "server.out.log"
$StderrLog = Join-Path $LogsDir "server.err.log"
$MetadataFile = Join-Path $SandboxRoot "sandbox.json"

function Set-PrivateAcl([string]$Path) {
    $identity = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
    & icacls.exe $Path /inheritance:r /grant:r "${identity}:(F)" "SYSTEM:(F)" | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "Failed to secure $Path" }
}

function Initialize-Sandbox {
    if (-not (Test-Path $SourceAccountsFile)) {
        throw "Credential source not found: $SourceAccountsFile"
    }
    $accounts = Get-Content $SourceAccountsFile -Raw | ConvertFrom-Json
    if (-not $accounts -or -not $accounts[0].token_v2 -or -not $accounts[0].space_id -or -not $accounts[0].user_id) {
        throw "Credential source is missing token_v2, space_id, or user_id."
    }
    New-Item -ItemType Directory -Force $SecretsDir, $DataDir, $LogsDir | Out-Null
    Copy-Item $SourceAccountsFile $AccountsFile -Force
    if (-not (Test-Path $ApiKeyFile)) {
        $bytes = New-Object byte[] 32
        $rng = [System.Security.Cryptography.RandomNumberGenerator]::Create()
        try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
        ([Convert]::ToBase64String($bytes)).Trim() | Set-Content $ApiKeyFile -Encoding ASCII -NoNewline
    }
    Set-PrivateAcl $SecretsDir
    Set-PrivateAcl $AccountsFile
    Set-PrivateAcl $ApiKeyFile
    @{
        repository_root = $RepositoryRoot
        port = $Port
        accounts_sha256 = (Get-FileHash $AccountsFile -Algorithm SHA256).Hash
        api_key_sha256 = (Get-FileHash $ApiKeyFile -Algorithm SHA256).Hash
        initialized_at = (Get-Date).ToString("o")
        remote_access_requires_explicit_opt_in = $true
    } | ConvertTo-Json | Set-Content $MetadataFile -Encoding UTF8
    Write-Host "Sandbox secrets initialized at $SecretsDir (values not displayed)."
}

function Install-Sandbox {
    if (-not (Test-Path $AccountsFile)) { Initialize-Sandbox }
    if (-not (Test-Path $Python)) {
        python -m venv $VenvDir
    }
    & $Python -m pip install --disable-pip-version-check -r (Join-Path $RepositoryRoot "requirements.txt")
    if ($LASTEXITCODE -ne 0) { throw "Sandbox dependency installation failed." }
}

function Get-SandboxProcess {
    if (-not (Test-Path $PidFile)) { return $null }
    $savedPid = [int](Get-Content $PidFile -Raw).Trim()
    $process = Get-Process -Id $savedPid -ErrorAction SilentlyContinue
    if (-not $process) { Remove-Item $PidFile -Force -ErrorAction SilentlyContinue }
    return $process
}

function Start-Sandbox {
    if (Get-SandboxProcess) { Write-Host "Sandbox is already running."; return }
    if (-not (Test-Path $Python)) { Install-Sandbox }
    New-Item -ItemType Directory -Force $DataDir, $LogsDir | Out-Null

    $saved = @{}
    foreach ($name in @("NOTION_ACCOUNTS_FILE", "NOTION_SANDBOX_REQUIRE_EXPLICIT_REMOTE", "API_KEY", "DB_PATH", "HOST", "PORT")) {
        $saved[$name] = [Environment]::GetEnvironmentVariable($name, "Process")
    }
    try {
        $env:NOTION_ACCOUNTS_FILE = $AccountsFile
        $env:NOTION_SANDBOX_REQUIRE_EXPLICIT_REMOTE = "true"
        $env:API_KEY = (Get-Content $ApiKeyFile -Raw).Trim()
        $env:DB_PATH = (Join-Path $DataDir "conversations.db")
        $env:HOST = "127.0.0.1"
        $env:PORT = "$Port"
        $process = Start-Process -FilePath $Python `
            -ArgumentList @("-m", "uvicorn", "app.server:app", "--host", "127.0.0.1", "--port", "$Port") `
            -WorkingDirectory $RepositoryRoot `
            -RedirectStandardOutput $StdoutLog `
            -RedirectStandardError $StderrLog `
            -WindowStyle Hidden `
            -PassThru
        Set-Content $PidFile $process.Id -Encoding ASCII
    } finally {
        foreach ($name in $saved.Keys) {
            [Environment]::SetEnvironmentVariable($name, $saved[$name], "Process")
        }
    }
    Start-Sleep -Seconds 2
    if (-not (Get-SandboxProcess)) { throw "Sandbox exited during startup. Check $StderrLog" }
    Write-Host "Sandbox started: http://127.0.0.1:$Port (PID $($process.Id))"
}

function Stop-Sandbox {
    $process = Get-SandboxProcess
    if (-not $process) { Write-Host "Sandbox is not running."; return }
    Stop-Process -Id $process.Id -Force
    Remove-Item $PidFile -Force -ErrorAction SilentlyContinue
    Write-Host "Sandbox stopped."
}

function Test-Sandbox {
    $health = Invoke-RestMethod "http://127.0.0.1:$Port/health" -TimeoutSec 10
    $body = @{model="terra"; messages=@(@{role="user"; content="Reply with OK."}); stream=$false} | ConvertTo-Json -Depth 6
    $headers = @{ Authorization = "Bearer $((Get-Content $ApiKeyFile -Raw).Trim())" }
    $probe = Invoke-RestMethod "http://127.0.0.1:$Port/v1/chat/completions" -Method Post -Headers $headers -ContentType "application/json" -Body $body -TimeoutSec 10
    if ($probe.choices[0].message.content -ne "OK") { throw "Local probe failed." }
    Write-Host "Health and non-remote probe passed."
}

switch ($Action) {
    "Initialize" { Initialize-Sandbox }
    "Install" { Install-Sandbox }
    "Start" { Start-Sandbox }
    "Stop" { Stop-Sandbox }
    "Status" {
        $process = Get-SandboxProcess
        [pscustomobject]@{
            Running = [bool]$process
            PID = if ($process) { $process.Id } else { $null }
            URL = "http://127.0.0.1:$Port"
            SecretFileExists = Test-Path $AccountsFile
            SecretSHA256 = if (Test-Path $AccountsFile) { (Get-FileHash $AccountsFile -Algorithm SHA256).Hash } else { $null }
            ApiKeyFileExists = Test-Path $ApiKeyFile
            ApiKeySHA256 = if (Test-Path $ApiKeyFile) { (Get-FileHash $ApiKeyFile -Algorithm SHA256).Hash } else { $null }
            RemoteAccessRequiresExplicitOptIn = $true
        } | Format-List
    }
    "Test" { Test-Sandbox }
    "Reset" {
        Stop-Sandbox
        Remove-Item $DataDir, $LogsDir, $VenvDir, $MetadataFile -Recurse -Force -ErrorAction SilentlyContinue
        if ($RemoveSecrets) { Remove-Item $SecretsDir -Recurse -Force -ErrorAction SilentlyContinue }
        Write-Host "Sandbox runtime reset. Secrets removed: $([bool]$RemoveSecrets)"
    }
}

