# Mục đích: Tạo tài khoản LogMiner bằng template SQL khi chủ động chuẩn bị nguồn.
# Đầu vào: ORACLE_CDC_PASSWORD trong môi trường và template oracle/01_create_logminer_user.sql.
# Đầu ra: DDL chạy trong Oracle khi gọi script; không phải lệnh kiểm tra chỉ đọc.
param([string]$Container = "hdg-c360-oracle")
$ErrorActionPreference = "Stop"
# BƯỚC 1: Đọc và kiểm mật khẩu trong bộ nhớ, không ghi bản SQL chứa mật khẩu ra file.
$password = [Environment]::GetEnvironmentVariable("ORACLE_CDC_PASSWORD")
if ([string]::IsNullOrEmpty($password)) { throw "Set ORACLE_CDC_PASSWORD in this PowerShell process first." }
# Restrict SQL identifier quoting/substitution hazards; never write the rendered SQL.
if ($password -match '["\r\n\x00]' -or $password -notmatch '^[\x21-\x7E]+$') {
    throw 'Use a printable ASCII password without double quotes or whitespace for this SQLPlus helper.'
}
# BƯỚC 2: Điền placeholder vào template; user đã tồn tại sẽ bị SQL chặn, không reset mật khẩu.
$template = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot "oracle/01_create_logminer_user.sql")
$sql = $template.Replace("__CDC_PASSWORD__", $password)
# BƯỚC 3: Chạy SQL trong container đã có; DDL Oracle không rollback toàn bộ nếu một GRANT sau đó lỗi.
try {
    $sql | & docker exec -i $Container sqlplus -L -s / as sysdba
    if ($LASTEXITCODE -ne 0) { throw "Oracle account setup failed; inspect grants before any retry (Oracle DDL is not transactional)." }
} finally {
    $sql = $null
    $password = $null
}
