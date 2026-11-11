param([string]$Container = "hdg-c360-oracle")
$ErrorActionPreference = "Stop"
$password = [Environment]::GetEnvironmentVariable("ORACLE_CDC_PASSWORD")
if ([string]::IsNullOrEmpty($password)) { throw "Set ORACLE_CDC_PASSWORD in this PowerShell process first." }
# Restrict SQL identifier quoting/substitution hazards; never write the rendered SQL.
if ($password -match '["\r\n\x00]' -or $password -notmatch '^[\x21-\x7E]+$') {
    throw 'Use a printable ASCII password without double quotes or whitespace for this SQLPlus helper.'
}
$template = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot "oracle/01_create_logminer_user.sql")
$sql = $template.Replace("__CDC_PASSWORD__", $password)
try {
    $sql | & docker exec -i $Container sqlplus -L -s / as sysdba
    if ($LASTEXITCODE -ne 0) { throw "Oracle account setup failed; inspect grants before any retry (Oracle DDL is not transactional)." }
} finally {
    $sql = $null
    $password = $null
}
