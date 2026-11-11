# Mục đích: Chuẩn bị riêng PostgreSQL test cho CDC Sink prototype.
# Đầu vào: Image postgres:16 có sẵn, .env.test local và tùy chọn -RunTests.
# Đầu ra: Container test cổng 55433; chỉ chạy unit/integration khi có -RunTests.
param([switch]$RunTests)
$ErrorActionPreference = "Stop"
$compose = Join-Path $PSScriptRoot "compose.test.yaml"
$envFile = Join-Path $PSScriptRoot ".env.test"
$python = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
$project = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
# BƯỚC 1: Tạo mật khẩu test một lần, giữ file và volume cũ khớp nhau.
if (-not (Test-Path -LiteralPath $envFile)) {
    $bytes = New-Object byte[] 32
    $rng = [Security.Cryptography.RandomNumberGenerator]::Create()
    try { $rng.GetBytes($bytes) } finally { $rng.Dispose() }
    $value = [Convert]::ToBase64String($bytes)
    Set-Content -LiteralPath $envFile -Value ("CDC_TEST_PASSWORD=" + $value) -Encoding ASCII
    $value = $null
}
# BƯỚC 2: Kiểm quyền sở hữu tên container trước khi dùng lại, tránh tác động dịch vụ khác.
$exists = & docker container ls --all --filter "name=^/hdg-cdc-sink-test$" --format "{{.Names}}"
if ($LASTEXITCODE -ne 0) { throw "Cannot inspect Docker" }
if ($exists) {
    $labels = & docker inspect --format '{{json .Config.Labels}}' hdg-cdc-sink-test | ConvertFrom-Json
    if ($LASTEXITCODE -ne 0 -or $labels.'com.docker.compose.project' -ne "hdg-sink-prototype") {
        throw "Container name belongs to another project"
    }
}
# BƯỚC 3: Kiểm Compose rồi mở riêng dịch vụ test, không tải image hoặc thay network CDC đang chạy.
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
# BƯỚC 4: Test chỉ định rõ DB test; integration có reset fixture nên không tự chạy ở lượt rút gọn source.
if ($RunTests) {
    if (-not (Test-Path -LiteralPath $python)) { throw "Install the project-local venv first; see PROTOTYPE.md" }
    Push-Location $project
    try {
        & $python -B -m unittest discover -s tests/cdc_sink -p test_unit.py -v
        if ($LASTEXITCODE -ne 0) { throw "Unit tests failed" }
        & $python -B -m unittest discover -s tests/cdc_sink -p test_integration.py -v
        if ($LASTEXITCODE -ne 0) { throw "Integration tests failed" }
    } finally { Pop-Location }
}
