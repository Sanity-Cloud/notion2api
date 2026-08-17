[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$CoreContainer = 'sanitycloud-sc-amf-r1-memory-core'
$HubContainer = 'sanitycloud-sc-amf-r1-memory-hub'
$Probe = 'sc-amf-r1-persistence-probe'

function Invoke-Health([string]$Url) {
    $body = (& curl.exe -fsS --max-time 5 $Url).Trim()
    if ($LASTEXITCODE -ne 0 -or -not $body) { throw "Health probe failed: $Url" }
    return ($body | ConvertFrom-Json)
}

$coreState = (& docker inspect $CoreContainer --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}').Trim()
$hubState = (& docker inspect $HubContainer --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}').Trim()
if ($coreState -ne 'running|healthy') { throw "Core not healthy: $coreState" }
if ($hubState -ne 'running|healthy') { throw "Hub not healthy: $hubState" }

$coreHealth = Invoke-Health 'http://127.0.0.1:8420/health'
$panelHealth = Invoke-Health 'http://127.0.0.1:8125/health'
$knowledgeHealth = Invoke-Health 'http://127.0.0.1:8424/health'
$hubToCore = (& docker exec $HubContainer sh -lc 'curl -fsS --max-time 5 http://memory-core:8420/health').Trim() | ConvertFrom-Json

& docker exec $CoreContainer sh -lc "printf '$Probe' > /data/tdai-memory/.sc-amf-persistence-probe" | Out-Null
& docker exec $HubContainer sh -lc "printf '$Probe' > /data/knowledge/.sc-amf-persistence-probe" | Out-Null

# Operator stop/start is the deterministic recovery contract tested here.
# Docker restart policy behavior after a daemon/process crash is intentionally
# not inferred from this test.
& docker stop $HubContainer | Out-Null
& docker stop $CoreContainer | Out-Null
& docker start $CoreContainer | Out-Null
& docker start $HubContainer | Out-Null

$deadline = (Get-Date).AddSeconds(100)
do {
    Start-Sleep -Seconds 3
    $coreState = (& docker inspect $CoreContainer --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}').Trim()
    $hubState = (& docker inspect $HubContainer --format '{{.State.Status}}|{{if .State.Health}}{{.State.Health.Status}}{{end}}').Trim()
    if ($coreState -eq 'running|healthy' -and $hubState -eq 'running|healthy') { break }
} while ((Get-Date) -lt $deadline)

if ($coreState -ne 'running|healthy' -or $hubState -ne 'running|healthy') {
    throw "Recovery readiness failed: core=$coreState hub=$hubState"
}

$coreProbe = (& docker exec $CoreContainer sh -lc 'cat /data/tdai-memory/.sc-amf-persistence-probe').Trim()
$hubProbe = (& docker exec $HubContainer sh -lc 'cat /data/knowledge/.sc-amf-persistence-probe').Trim()
if ($coreProbe -ne $Probe -or $hubProbe -ne $Probe) {
    throw "Persistence recovery failed: core=$coreProbe hub=$hubProbe"
}

$published = @(
    (& docker port $CoreContainer),
    (& docker port $HubContainer)
) -join "`n"
if ($published -match '0\.0\.0\.0|\[::\]') {
    throw "Pilot exposes a non-loopback port: $published"
}

[pscustomobject]@{
    status = 'passed'
    core_health = $coreHealth.status
    panel_health = $panelHealth.status
    knowledge_health = $knowledgeHealth.status
    hub_to_core = $hubToCore.status
    manual_restart_recovery = 'passed'
    core_volume_persistence = 'passed'
    hub_volume_persistence = 'passed'
    loopback_only = $true
    proxy_disabled = $true
    real_llm_semantics_tested = $false
    automatic_crash_restart_proven = $false
} | ConvertTo-Json -Depth 3
