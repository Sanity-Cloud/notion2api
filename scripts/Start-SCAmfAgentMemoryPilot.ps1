[CmdletBinding()]
param(
    [switch]$Recreate
)

$ErrorActionPreference = 'Stop'

# SC-AMF R1 local smoke topology.  This script intentionally has no parameter
# for reusable LLM/provider credentials.  It proves container/topology health
# only; authenticated/live memory operations remain a Session Broker concern.
$CoreImage = 'agentmemory/memory-core@sha256:f9b286246d0e5020a7f0cb011b7074703d10b76b424a834a117482392f7bd424'
$HubImage = 'agentmemory/memory-hub@sha256:99c234f606be6e0496e78cddf220a9ebf12248863276991f2132d2a1b7d9a95f'
$Network = 'sanitycloud-sc-amf-r1'
$CoreVolume = 'sanitycloud-sc-amf-r1-core-data'
$HubVolume = 'sanitycloud-sc-amf-r1-hub-data'
$CoreContainer = 'sanitycloud-sc-amf-r1-memory-core'
$HubContainer = 'sanitycloud-sc-amf-r1-memory-hub'

function Set-StandardWindowsEnvironment {
    if (-not $env:ProgramData) { $env:ProgramData = 'C:\ProgramData' }
    if (-not $env:USERPROFILE) { $env:USERPROFILE = 'C:\Users\harmo' }
    if (-not $env:APPDATA) { $env:APPDATA = Join-Path $env:USERPROFILE 'AppData\Roaming' }
    if (-not $env:LOCALAPPDATA) { $env:LOCALAPPDATA = Join-Path $env:USERPROFILE 'AppData\Local' }
    if (-not $env:HOMEDRIVE) { $env:HOMEDRIVE = 'C:' }
    if (-not $env:HOMEPATH) { $env:HOMEPATH = '\Users\harmo' }
}

function Test-DockerEngine {
    try {
        $version = (& docker version --format '{{.Server.Version}}' 2>$null).Trim()
        return [bool]$version
    } catch {
        return $false
    }
}

function Start-DockerDesktopIfNeeded {
    if (Test-DockerEngine) { return }
    Set-StandardWindowsEnvironment
    $desktop = 'C:\Program Files\Docker\Docker\Docker Desktop.exe'
    if (-not (Test-Path -LiteralPath $desktop)) {
        throw 'Docker Engine is unavailable and Docker Desktop was not found.'
    }
    Start-Process -FilePath $desktop | Out-Null
    $deadline = (Get-Date).AddSeconds(90)
    do {
        Start-Sleep -Seconds 3
        if (Test-DockerEngine) { return }
    } while ((Get-Date) -lt $deadline)
    throw 'Docker Desktop started but Docker Engine did not become available.'
}

function Assert-PortFree([int]$Port) {
    $listener = Get-NetTCPConnection -State Listen -LocalPort $Port -ErrorAction SilentlyContinue | Select-Object -First 1
    if ($listener) {
        throw "Required loopback pilot port $Port is already listening (PID $($listener.OwningProcess))."
    }
}

function Get-ContainerName([string]$Name) {
    return (& docker ps -a --filter "name=^/${Name}$" --format '{{.Names}}').Trim()
}

function Remove-PilotContainer([string]$Name) {
    if (Get-ContainerName $Name) {
        & docker rm -f $Name | Out-Null
    }
}

Set-StandardWindowsEnvironment
Start-DockerDesktopIfNeeded

foreach ($port in 8420, 8125, 8424) {
    if (-not (Get-ContainerName $CoreContainer) -and -not (Get-ContainerName $HubContainer)) {
        Assert-PortFree $port
    }
}

if (Get-ContainerName $CoreContainer -or Get-ContainerName $HubContainer) {
    if (-not $Recreate) {
        throw 'SC-AMF pilot containers already exist. Use -Recreate for an explicit container-only replacement; volumes are preserved.'
    }
    Remove-PilotContainer $HubContainer
    Remove-PilotContainer $CoreContainer
}

if (-not (& docker network ls --filter "name=^${Network}$" --format '{{.Name}}')) {
    & docker network create $Network | Out-Null
}
if (-not (& docker volume ls --filter "name=^${CoreVolume}$" --format '{{.Name}}')) {
    & docker volume create $CoreVolume | Out-Null
}
if (-not (& docker volume ls --filter "name=^${HubVolume}$" --format '{{.Name}}')) {
    & docker volume create $HubVolume | Out-Null
}

& docker run -d --name $CoreContainer `
    --restart unless-stopped `
    --network $Network --network-alias memory-core `
    -p '127.0.0.1:8420:8420' `
    -v "${CoreVolume}:/data/tdai-memory" `
    -e 'TDAI_GATEWAY_PORT=8420' `
    -e 'TDAI_GATEWAY_HOST=0.0.0.0' `
    -e 'TDAI_GATEWAY_API_KEY=' `
    -e 'TDAI_DATA_DIR=/data/tdai-memory' `
    $CoreImage | Out-Null

& docker run -d --name $HubContainer `
    --restart unless-stopped `
    --network $Network --network-alias memory-hub `
    --add-host 'host.docker.internal:host-gateway' `
    -p '127.0.0.1:8125:8125' `
    -p '127.0.0.1:8424:8424' `
    -v "${HubVolume}:/data/knowledge" `
    -e 'PANEL_PORT=8125' `
    -e 'KNOWLEDGE_PORT=8424' `
    -e 'KNOWLEDGE_PUBLIC_BASE_URL=http://host.docker.internal:8424/v3' `
    -e 'REMOTE_INSTANCE_ID=sc-amf-r1' `
    -e 'REMOTE_INSTANCE_NAME=SC-AMF-R1' `
    -e 'REMOTE_INSTANCE_URL=http://memory-core:8420' `
    -e 'REMOTE_INSTANCE_KEY=synthetic-local-no-secret' `
    -e 'REMOTE_INSTANCE_PROXY_URL=' `
    -e 'LLM_MODE=custom' `
    -e 'LLM_PROTOCOL=openai' `
    -e 'LLM_API_KEY=synthetic-no-secret' `
    -e 'LLM_BASE_URL=http://host.docker.internal:18499/v1' `
    -e 'LLM_MODEL=synthetic' `
    -e 'KNOWLEDGE_LLM_BINDING_SYNC=0' `
    $HubImage | Out-Null

$deadline = (Get-Date).AddSeconds(100)
do {
    Start-Sleep -Seconds 3
    $core = (& docker inspect $CoreContainer --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}').Trim()
    $hub = (& docker inspect $HubContainer --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}').Trim()
    if ($core -eq 'running|healthy' -and $hub -eq 'running|healthy') { break }
} while ((Get-Date) -lt $deadline)

if ($core -ne 'running|healthy' -or $hub -ne 'running|healthy') {
    throw "SC-AMF pilot failed readiness: core=$core hub=$hub"
}

[pscustomobject]@{
    status = 'healthy'
    mode = 'synthetic-smoke'
    core = 'http://127.0.0.1:8420/health'
    panel = 'http://127.0.0.1:8125/health'
    knowledge = 'http://127.0.0.1:8424/health'
    proxy = 'disabled'
    real_llm_configured = $false
    reusable_credentials_present = $false
    core_image = $CoreImage
    hub_image = $HubImage
} | ConvertTo-Json -Depth 3
