param([string]$Network = "hdg-cdc-net")
$ErrorActionPreference = "Stop"
# Inspect both containers before creating or attaching any network.
foreach ($container in @("hdg-c360-oracle", "hdg-c360-postgres")) {
    $null = & docker container inspect $container 2>$null
    if ($LASTEXITCODE -ne 0) { throw "Existing container not found: $container" }
}
$null = & docker network inspect $Network 2>$null
if ($LASTEXITCODE -ne 0) {
    & docker network create --driver bridge $Network
    if ($LASTEXITCODE -ne 0) { throw "Cannot create network $Network" }
}
foreach ($container in @("hdg-c360-oracle", "hdg-c360-postgres")) {
    $info = (& docker container inspect $container | ConvertFrom-Json)[0]
    if ($info.NetworkSettings.Networks.PSObject.Properties.Name -notcontains $Network) {
        & docker network connect $Network $container
        if ($LASTEXITCODE -ne 0) { throw "Cannot attach $container to $Network" }
    }
}
Write-Host "Existing containers attached; original networks and volumes retained."
