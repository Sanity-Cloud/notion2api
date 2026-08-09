[CmdletBinding()]
param(
    [switch]$Purge
)

$ErrorActionPreference = 'Stop'
$containers = @(
    'sanitycloud-sc-amf-r1-memory-hub',
    'sanitycloud-sc-amf-r1-memory-core'
)

foreach ($container in $containers) {
    $exists = @(& docker ps -a --filter "name=^/${container}$" --format '{{.Names}}') | Select-Object -First 1
    if ($exists) { & docker rm -f $container | Out-Null }
}

if ($Purge) {
    foreach ($volume in 'sanitycloud-sc-amf-r1-hub-data', 'sanitycloud-sc-amf-r1-core-data') {
        if (@(& docker volume ls --filter "name=^${volume}$" --format '{{.Name}}') | Select-Object -First 1) {
            & docker volume rm $volume | Out-Null
        }
    }
    if (@(& docker network ls --filter 'name=^sanitycloud-sc-amf-r1$' --format '{{.Name}}') | Select-Object -First 1) {
        & docker network rm 'sanitycloud-sc-amf-r1' | Out-Null
    }
}

[pscustomobject]@{
    status = 'stopped'
    volumes_preserved = (-not $Purge)
    purge = [bool]$Purge
} | ConvertTo-Json -Compress
