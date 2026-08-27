#!/usr/bin/env bash

set -euo pipefail

# ============================================================
# Real-time-Systems-Monitoring
# Register Debezium PostgreSQL connector
# ============================================================

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ENV_FILE="${PROJECT_ROOT}/.env"
CONNECTOR_TEMPLATE="${PROJECT_ROOT}/connectors/postgres-cdc.json"

# ============================================================
# Validate required files
# ============================================================

if [[ ! -f "${ENV_FILE}" ]]; then
    echo "ERROR: .env file not found:"
    echo "${ENV_FILE}"
    echo
    echo "Create it with:"
    echo "  cp .env.example .env"
    exit 1
fi

if [[ ! -f "${CONNECTOR_TEMPLATE}" ]]; then
    echo "ERROR: Connector configuration not found:"
    echo "${CONNECTOR_TEMPLATE}"
    exit 1
fi

# ============================================================
# Load .env
# ============================================================

set -a
source "${ENV_FILE}"
set +a

# ============================================================
# Validate required variables
# ============================================================

required_vars=(
    POSTGRES_USER
    POSTGRES_PASSWORD
    POSTGRES_DB
    CONNECT_PORT
    DEBEZIUM_CONNECTOR_NAME
    DEBEZIUM_SLOT_NAME
    DEBEZIUM_TOPIC_PREFIX
    DEBEZIUM_TABLE_INCLUDE_LIST
)

for var in "${required_vars[@]}"; do
    if [[ -z "${!var:-}" ]]; then
        echo "ERROR: Required variable ${var} is not defined in .env"
        exit 1
    fi
done

CONNECT_URL="http://localhost:${CONNECT_PORT}"
CONNECTOR_NAME="${DEBEZIUM_CONNECTOR_NAME}"

# ============================================================
# Check Kafka Connect
# ============================================================

echo "Checking Kafka Connect..."

if ! curl -sf "${CONNECT_URL}/" > /dev/null; then
    echo "ERROR: Kafka Connect is not available at ${CONNECT_URL}"
    echo
    echo "Check:"
    echo "  docker compose ps"
    echo "  docker compose logs connect"
    exit 1
fi

echo "Kafka Connect is available."

# ============================================================
# Check Debezium PostgreSQL connector
# ============================================================

echo "Checking Debezium PostgreSQL connector..."

if ! curl -sf "${CONNECT_URL}/connector-plugins" \
    | grep -q "io.debezium.connector.postgresql.PostgresConnector"; then

    echo "ERROR: Debezium PostgreSQL connector was not found."
    echo
    echo "Check:"
    echo "  docker compose logs connect"
    exit 1
fi

echo "Debezium PostgreSQL connector is available."

# ============================================================
# Generate connector configuration
# ============================================================

TEMP_CONFIG="$(mktemp)"

trap 'rm -f "${TEMP_CONFIG}"' EXIT

echo "Generating connector configuration..."

envsubst < "${CONNECTOR_TEMPLATE}" > "${TEMP_CONFIG}"

# ============================================================
# Validate generated JSON
# ============================================================

if ! jq empty "${TEMP_CONFIG}" 2>/dev/null; then
    echo "ERROR: Generated connector configuration is not valid JSON."
    echo
    echo "Generated configuration:"
    cat "${TEMP_CONFIG}"
    exit 1
fi

echo "Connector configuration is valid JSON."

# ============================================================
# Check if connector already exists
# ============================================================

echo
echo "Checking if connector '${CONNECTOR_NAME}' already exists..."

HTTP_STATUS="$(
    curl -s \
        -o /dev/null \
        -w "%{http_code}" \
        "${CONNECT_URL}/connectors/${CONNECTOR_NAME}"
)"

if [[ "${HTTP_STATUS}" == "200" ]]; then

    echo "Connector '${CONNECTOR_NAME}' already exists."

    echo
    read -r -p "Do you want to update it? [y/N] " ANSWER

    if [[ "${ANSWER}" =~ ^[Yy]$ ]]; then

        echo
        echo "Updating connector '${CONNECTOR_NAME}'..."

        if curl -sS \
            --fail-with-body \
            -X PUT \
            -H "Content-Type: application/json" \
            --data "$(jq -c '.config' "${TEMP_CONFIG}")" \
            "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"; then

            echo
            echo "Connector updated successfully."

        else

            echo
            echo "ERROR: Failed to update connector."
            exit 1

        fi

    else

        echo "No changes made."
        exit 0

    fi

elif [[ "${HTTP_STATUS}" == "404" ]]; then

    echo "Connector does not exist."

    echo
    echo "Creating connector '${CONNECTOR_NAME}'..."

    if curl -sS \
        --fail-with-body \
        -X POST \
        -H "Content-Type: application/json" \
        --data @"${TEMP_CONFIG}" \
        "${CONNECT_URL}/connectors"; then

        echo
        echo "Connector created successfully."

    else

        echo
        echo "ERROR: Failed to create connector."
        exit 1

    fi

else

    echo
    echo "ERROR: Unexpected HTTP status: ${HTTP_STATUS}"
    exit 1

fi

# ============================================================
# Wait for connector initialization
# ============================================================

echo
echo "Waiting for connector to initialize..."

sleep 3

# ============================================================
# Check connector status
# ============================================================

echo
echo "Connector status:"
echo

STATUS_RESPONSE="$(
    curl -sS \
        "${CONNECT_URL}/connectors/${CONNECTOR_NAME}/status"
)"

echo "${STATUS_RESPONSE}" | jq .

# ============================================================
# Determine connector state
# ============================================================

CONNECTOR_STATE="$(
    echo "${STATUS_RESPONSE}" \
    | jq -r '.connector.state // "UNKNOWN"'
)"

TASK_STATE="$(
    echo "${STATUS_RESPONSE}" \
    | jq -r '.tasks[0].state // "UNKNOWN"'
)"

echo
echo "Connector state: ${CONNECTOR_STATE}"
echo "Task state:      ${TASK_STATE}"

if [[ "${CONNECTOR_STATE}" == "RUNNING" && "${TASK_STATE}" == "RUNNING" ]]; then

    echo
    echo "============================================================"
    echo "Debezium connector is RUNNING."
    echo "============================================================"

else

    echo
    echo "============================================================"
    echo "WARNING: Connector is not RUNNING."
    echo "============================================================"
    echo
    echo "Check the connector logs with:"
    echo
    echo "  docker compose logs connect"
    echo

    exit 1

fi
