param([string]$ConnectUrl = "http://localhost:8083")
$ErrorActionPreference = "Stop"
$payload = Get-Content -Raw -LiteralPath (Join-Path $PSScriptRoot "connectors/oracle-source.json")
$spec = $payload | ConvertFrom-Json
# Creation only: never overwrite or reset an existing connector/offset.
$existing = @(Invoke-RestMethod -Uri "$ConnectUrl/connectors")
if ($existing -contains $spec.name) {
    throw "Connector already exists. Inspect its status; this script does not overwrite it."
}
$validationConfig = $spec.config | ConvertTo-Json -Depth 20 | ConvertFrom-Json -AsHashtable
$validationConfig["name"] = $spec.name
$validationBody = $validationConfig | ConvertTo-Json -Depth 20
$validation = Invoke-RestMethod -Method Put -Uri "$ConnectUrl/connector-plugins/io.debezium.connector.oracle.OracleConnector/config/validate" -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($validationBody))
if ($validation.error_count -ne 0) {
    throw "Connector validation failed. Inspect worker logs locally; do not share credentials."
}
$null = Invoke-RestMethod -Method Post -Uri "$ConnectUrl/connectors" -ContentType "application/json" -Body ([Text.Encoding]::UTF8.GetBytes($payload))
Write-Host "Source registered for observation only. No Sink and no Full Load SCN cutover."
Invoke-RestMethod -Uri "$ConnectUrl/connectors/$($spec.name)/status"
