# Mục đích: Nối hai container Oracle/PostgreSQL có sẵn vào network dùng cho CDC.
# Đầu vào: Tên network Docker; container nguồn/đích phải tồn tại.
# Đầu ra: Network và kết nối mạng nếu còn thiếu; không tạo lại container hay volume.
param([string]$Network = "hdg-cdc-net")
$ErrorActionPreference = "Stop"
# Inspect both containers before creating or attaching any network.
# BƯỚC 1: Kiểm hai container trước khi thay đổi network; bước nối phía dưới giữ các mạng cũ.
foreach ($container in @("hdg-c360-oracle", "hdg-c360-postgres")) {
    $null = & docker container inspect $container 2>$null
    if ($LASTEXITCODE -ne 0) { throw "Existing container not found: $container" }
}
# BƯỚC 2: Chỉ tạo network nếu chưa có, rồi nối các container chưa tham gia.
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
