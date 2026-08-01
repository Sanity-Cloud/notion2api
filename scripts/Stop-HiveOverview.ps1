#Requires -Version 7.0
[CmdletBinding()]
param(
    [string]$HostAddress = '127.0.0.1',
    [int]$Port = 8162,
    [switch]$NoPause
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

try {
    $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if (-not $listener) {
        Write-Output "service=hive-overview already_stopped port=$Port"
        return
    }
    $pidValue = [int]$listener.OwningProcess
    $process = Get-CimInstance Win32_Process -Filter "ProcessId=$pidValue"
    $commandLine = [string]$process.CommandLine
    if ($commandLine -notmatch '(?i)app\.hive_overview') {
        throw "Refusing to stop PID $pidValue because port $Port is not owned by a verified Hive Overview process."
    }
    try {
        $health = Invoke-RestMethod -Uri "http://$HostAddress`:$Port/health" -TimeoutSec 3
    } catch {
        $health = $null
    }
    if ($health -and $health.service -ne 'hive-overview') {
        throw "Refusing to stop PID $pidValue because the health identity is not Hive Overview."
    }
    Stop-Process -Id $pidValue -Force -ErrorAction Stop
    Write-Output "service=hive-overview stopped pid=$pidValue port=$Port"
} finally {
    if (-not $NoPause) { Read-Host 'Press Enter to close' | Out-Null }
}
