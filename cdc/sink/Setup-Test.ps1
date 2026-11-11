param([switch]$RunTests)
$ErrorActionPreference = "Stop"
$compose = Join-Path $PSScriptRoot "compose.test.yaml"
$envFile = Join-Path $PSScriptRoot ".env.test"
$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$project = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
if (-not (Test-Path -LiteralPath $envFile)) {
    $bytes = New-Object byte[] 32
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    $value = [Convert]::ToBase64String($bytes)
    Set-Content -LiteralPath $envFile -Value ("CDC_TEST_PASSWORD=" + $value) -Encoding ASCII
    $value = $null
}
$exists = & docker container ls --all --filter "name=^/hdg-cdc-sink-test$" --format "{{.Names}}"
if ($LASTEXITCODE -ne 0) { throw "Cannot inspect Docker" }
if ($exists) {
    $labels = & docker inspect --format '{{json .Config.Labels}}' hdg-cdc-sink-test | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $labels.'com.docker.compose.project' -ne "hdg-sink-prototype") {
        throw "Container name belongs to another project"
    }
}
& docker compose --env-file $envFile -f $compose config --quiet
if ($LASTEXITCODE -ne 0) { throw "Invalid test Compose" }
& docker compose --env-file $envFile -f $compose up -d --pull never
if ($LASTEXITCODE -ne 0) { throw "Cannot start isolated test PostgreSQL; ensure postgres:16 exists" }
$healthy = $false
for ($attempt = 0; $attempt -lt 30; $attempt++) {
    $status = & docker inspect --format '{{.State.Health.Status}}' hdg-cdc-sink-test
    if ($LASTEXITCODE -eq 0 -and $status.Trim() -eq "healthy") { $healthy = $true; break }
    Start-Sleep -Seconds 1
}
if (-not $healthy) { throw "Test PostgreSQL did not become healthy" }
Write-Host "Only hdg-cdc-sink-test prepared at localhost:55433."
if ($RunTests) {
    if (-not (Test-Path -LiteralPath $python)) { throw "Install the project-local venv first; see PROTOTYPE.md" }
    Push-Location $project
    try {
        & $python -B -m unittest discover -s cdc/sink/tests -p test_unit.py -v
        if ($LASTEXITCODE -ne 0) { throw "Unit tests failed" }
        & $python -B -m unittest discover -s cdc/sink/tests -p test_integration.py -v
        if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }
    } finally { Pop-Location }
}
