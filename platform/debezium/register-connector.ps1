$DEBEZIUM_URL = "http://localhost:8083"
$CONNECTOR_FILE = "./connectors/mysql-connector.json"

Write-Host "=== Waiting for Debezium Connect to be ready ==="
$MAX_RETRIES = 30
$RETRY_COUNT = 0
while ($RETRY_COUNT -lt $MAX_RETRIES) {
    try {
        $null = Invoke-WebRequest -Uri $DEBEZIUM_URL -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
        Write-Host "Debezium Connect is ready!"
        break
    }
    catch {
        $RETRY_COUNT++
        Write-Host "Waiting for Debezium Connect... ($RETRY_COUNT/$MAX_RETRIES)"
        Start-Sleep -Seconds 5
    }
}

if ($RETRY_COUNT -ge $MAX_RETRIES) {
    Write-Host "Debezium Connect did not become ready in time"
    exit 1
}

Write-Host ""
Write-Host "=== Registering MySQL CDC Connector ==="
$BODY = Get-Content $CONNECTOR_FILE -Raw
try {
    $RESPONSE = Invoke-RestMethod -Uri "$DEBEZIUM_URL/connectors" -Method Post -ContentType "application/json" -Body $BODY
    Write-Host "Connector registered successfully"
    $RESPONSE | ConvertTo-Json -Depth 10
}
catch {
    if ($_.Exception.Response.StatusCode -eq 409) {
        Write-Host "Connector already exists (409 Conflict)"
    }
    else {
        Write-Host "Failed to register connector: $_"
    }
}

Write-Host ""
Write-Host "=== Active Connectors ==="
try {
    $CONNECTORS = Invoke-RestMethod -Uri "$DEBEZIUM_URL/connectors"
    $CONNECTORS | ConvertTo-Json
}
catch {
    Write-Host "Could not list connectors"
}

Write-Host ""
Write-Host "=== Connector Status ==="
try {
    $STATUS = Invoke-RestMethod -Uri "$DEBEZIUM_URL/connectors/ecommerce-mysql-connector/status"
    $STATUS | ConvertTo-Json -Depth 10
}
catch {
    Write-Host "Could not get connector status"
}
