# Mục đích: Đăng ký mới Oracle Debezium Source sau khi kiểm cấu hình.
# Đầu vào: connectors/oracle-source.json và URL REST Connect; mật khẩu do worker giải từ môi trường.
# Đầu ra: Một Source mới nếu validate đạt; không cập nhật Source đã có, không cấu hình Sink/cutover.
param([string]$ConnectUrl = "http://localhost:8083")
$ErrorActionPreference = "Stop"
$payload = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot "connectors/oracle-source.json")
$spec = $payload | ConvertFrom-Json
# Creation only: never overwrite or reset an existing connector/offset.
# BƯỚC 1: Dừng nếu connector đã có để không ghi đè cấu hình/offset đang vận hành.
$existing = @(Invoke-RestMethod -Uri "$ConnectUrl/connectors")
if ($existing -contains $spec.name) {
    throw "Connector already exists. Inspect its status; this script does not overwrite it."
}
# BƯỚC 2: Gửi cấu hình validate kèm tên connector; không in mật khẩu hoặc nội dung nhạy cảm.
$validationConfig = $spec.config | ConvertTo-Json -Depth 20 | ConvertFrom-Json -AsHashtable
$validationConfig["name"] = $spec.name
$validationBody = $validationConfig | ConvertTo-Json -Depth 20
$validation = Invoke-RestMethod -Method Put -Uri "$ConnectUrl/connector-plugins/io.debezium.connector.oracle.OracleConnector/config/validate" -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($validationBody))
if ($validation.error_count -ne 0) {
    throw "Connector validation failed. Inspect worker logs locally; do not share credentials."
}
# BƯỚC 3: Chỉ POST sau validate; RUNNING chưa chứng minh Full Load đã nối tiếp CDC an toàn.
$null = Invoke-RestMethod -Method Post -Uri "$ConnectUrl/connectors" -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($payload))
Write-Host "Source registered for observation only. No Sink and no Full Load SCN cutover."
Invoke-RestMethod -Uri "$ConnectUrl/connectors/$($spec.name)/status"
