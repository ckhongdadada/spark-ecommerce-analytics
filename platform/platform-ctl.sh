#!/bin/bash
# ==============================================================================
# Ecommerce Data Platform - Master Control Script
# ==============================================================================

set -e

MODE="docker"
COMMAND=""
SERVICE=""

# ANSI Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

function print_usage() {
    echo -e "Usage: $0 [options] <command> [service]"
    echo -e ""
    echo -e "Options:"
    echo -e "  -m, --mode <mode>   Deployment mode: 'docker' (default) or 'k8s'"
    echo -e "  -h, --help          Show this help message"
    echo -e ""
    echo -e "Commands:"
    echo -e "  start               Start all services (or specific service if provided)"
    echo -e "  stop                Stop all services (or specific service if provided)"
    echo -e "  restart             Restart all services (or specific service if provided)"
    echo -e "  status              Show status of all services"
    echo -e "  init                Initialize platform components (Kafka topics, MinIO buckets, etc.)"
    echo -e "  logs <service>      View logs for a specific service"
    echo -e "  health              Check health of core dependencies"
    echo -e ""
    echo -e "Examples:"
    echo -e "  $0 start                  # Start all services in Docker Compose mode"
    echo -e "  $0 -m k8s start          # Apply all K8s manifests"
    echo -e "  $0 logs kafka             # View Kafka logs in Docker Compose mode"
}

# Parse options
while [[ $# -gt 0 ]]; do
    case $1 in
        -m|--mode)
            MODE="$2"
            shift 2
            ;;
        -h|--help)
            print_usage
            exit 0
            ;;
        *)
            if [ -z "$COMMAND" ]; then
                COMMAND="$1"
            elif [ -z "$SERVICE" ]; then
                SERVICE="$1"
            else
                echo -e "${RED}Error: Too many arguments${NC}"
                print_usage
                exit 1
            fi
            shift
            ;;
    esac
done

if [ -z "$COMMAND" ]; then
    print_usage
    exit 1
fi

if [[ "$MODE" != "docker" && "$MODE" != "k8s" ]]; then
    echo -e "${RED}Error: Invalid mode '$MODE'. Must be 'docker' or 'k8s'.${NC}"
    exit 1
fi

echo -e "${YELLOW}>>> Running in $MODE mode <<<${NC}"

# ==================== Docker Mode Implementations ====================
function docker_start() {
    if [ -n "$SERVICE" ]; then
        echo "Starting Docker service: $SERVICE"
        docker compose up -d "$SERVICE"
    else
        echo "Starting all Docker services..."
        docker compose up -d
    fi
}

function docker_stop() {
    if [ -n "$SERVICE" ]; then
        echo "Stopping Docker service: $SERVICE"
        docker compose stop "$SERVICE"
    else
        echo "Stopping all Docker services..."
        docker compose down
    fi
}

function docker_status() {
    echo "Docker Compose status:"
    docker compose ps
}

function docker_logs() {
    if [ -z "$SERVICE" ]; then
        echo -e "${RED}Error: Service name required for 'logs' command${NC}"
        exit 1
    fi
    docker compose logs -f "$SERVICE"
}

function docker_init() {
    echo "Triggering initialization jobs in Docker Compose..."
    docker compose start minio-init kafka-init airflow-init superset-init
    
    echo "Registering Debezium connectors..."
    if [ -f "./debezium/connectors/register-connector.sh" ]; then
        bash ./debezium/connectors/register-connector.sh
    else
        echo -e "${YELLOW}Warning: register-connector.sh not found${NC}"
    fi
}

function docker_health() {
    echo "Checking health of core Docker services..."
    docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "dpl-mysql|dpl-postgres|dpl-kafka|dpl-clickhouse" || echo "Services not running."
}

# ==================== K8s Mode Implementations ====================
function k8s_start() {
    echo "Applying K8s manifests..."
    kubectl apply -f k8s/namespace.yml
    # Apply secrets first
    kubectl apply -f k8s/secrets.yml
    # Storage & DBs
    kubectl apply -f k8s/mysql.yml
    kubectl apply -f k8s/postgres.yml
    kubectl apply -f k8s/minio.yml
    kubectl apply -f k8s/clickhouse.yml
    # Messaging & Compute
    kubectl apply -f k8s/zookeeper.yml
    kubectl apply -f k8s/kafka.yml
    kubectl apply -f k8s/flink.yml
    kubectl apply -f k8s/debezium.yml
    # Apps
    kubectl apply -f k8s/airflow.yml
    kubectl apply -f k8s/superset.yml
    kubectl apply -f k8s/atlas.yml
    kubectl apply -f k8s/fastapi.yml
    kubectl apply -f k8s/monitoring.yml
    echo -e "${GREEN}K8s resources applied successfully.${NC}"
}

function k8s_stop() {
    echo "Deleting K8s resources..."
    kubectl delete -f k8s/ --ignore-not-found=true
    echo -e "${GREEN}K8s resources deleted successfully.${NC}"
}

function k8s_status() {
    echo "Kubernetes status for namespace 'ecommerce-data-platform':"
    kubectl get pods,svc,pvc -n ecommerce-data-platform
}

function k8s_logs() {
    if [ -z "$SERVICE" ]; then
        echo -e "${RED}Error: Service name required for 'logs' command${NC}"
        exit 1
    fi
    # Stream logs from first pod matching label app=$SERVICE
    POD=$(kubectl get pod -n ecommerce-data-platform -l app=$SERVICE -o jsonpath="{.items[0].metadata.name}" 2>/dev/null)
    if [ -n "$POD" ]; then
        echo "Tailing logs for $POD..."
        kubectl logs -f $POD -n ecommerce-data-platform
    else
        echo -e "${RED}No pod found with label app=$SERVICE${NC}"
    fi
}

function k8s_init() {
    echo "Re-running K8s init jobs..."
    kubectl delete job minio-init kafka-init airflow-init superset-init -n ecommerce-data-platform --ignore-not-found
    kubectl apply -f k8s/minio.yml
    kubectl apply -f k8s/kafka.yml
    kubectl apply -f k8s/airflow.yml
    kubectl apply -f k8s/superset.yml
}

function k8s_health() {
    echo "Wait for core K8s services to be ready..."
    kubectl get events -n ecommerce-data-platform --sort-by='.metadata.creationTimestamp' | tail -n 10
}

# ==================== Main Router ====================
case "$COMMAND" in
    start)
        if [ "$MODE" == "docker" ]; then docker_start; else k8s_start; fi
        ;;
    stop)
        if [ "$MODE" == "docker" ]; then docker_stop; else k8s_stop; fi
        ;;
    restart)
        if [ "$MODE" == "docker" ]; then 
            docker_stop
            docker_start
        else 
            k8s_stop
            k8s_start
        fi
        ;;
    status)
        if [ "$MODE" == "docker" ]; then docker_status; else k8s_status; fi
        ;;
    init)
        if [ "$MODE" == "docker" ]; then docker_init; else k8s_init; fi
        ;;
    logs)
        if [ "$MODE" == "docker" ]; then docker_logs; else k8s_logs; fi
        ;;
    health)
        if [ "$MODE" == "docker" ]; then docker_health; else k8s_health; fi
        ;;
    *)
        echo -e "${RED}Error: Unknown command '$COMMAND'${NC}"
        print_usage
        exit 1
        ;;
esac

exit 0
