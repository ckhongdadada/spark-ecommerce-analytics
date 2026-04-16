#!/bin/bash
set -e

DEBEZIUM_URL="http://localhost:8083"
CONNECTOR_FILE="./connectors/mysql-connector.json"

echo "=== Waiting for Debezium Connect to be ready ==="
MAX_RETRIES=30
RETRY_COUNT=0
until curl -s "${DEBEZIUM_URL}/" > /dev/null 2>&1; do
    RETRY_COUNT=$((RETRY_COUNT + 1))
    if [ $RETRY_COUNT -ge $MAX_RETRIES ]; then
        echo "Debezium Connect did not become ready in time"
        exit 1
    fi
    echo "Waiting for Debezium Connect... ($RETRY_COUNT/$MAX_RETRIES)"
    sleep 5
done
echo "Debezium Connect is ready!"

echo ""
echo "=== Registering MySQL CDC Connector ==="
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST \
    -H "Content-Type: application/json" \
    --data @"${CONNECTOR_FILE}" \
    "${DEBEZIUM_URL}/connectors")

HTTP_CODE=$(echo "$RESPONSE" | tail -n1)
BODY=$(echo "$RESPONSE" | sed '$d')

if [ "$HTTP_CODE" -eq 201 ] || [ "$HTTP_CODE" -eq 409 ]; then
    echo "Connector registered successfully (HTTP $HTTP_CODE)"
else
    echo "Failed to register connector (HTTP $HTTP_CODE)"
    echo "$BODY"
fi

echo ""
echo "=== Active Connectors ==="
curl -s "${DEBEZIUM_URL}/connectors" | python3 -m json.tool 2>/dev/null || curl -s "${DEBEZIUM_URL}/connectors"

echo ""
echo "=== Connector Status ==="
curl -s "${DEBEZIUM_URL}/connectors/ecommerce-mysql-connector/status" | python3 -m json.tool 2>/dev/null || \
    curl -s "${DEBEZIUM_URL}/connectors/ecommerce-mysql-connector/status"
