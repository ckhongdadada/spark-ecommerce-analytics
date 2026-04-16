<#
.SYNOPSIS
Ecommerce Data Platform - Master Control Script (PowerShell)

.DESCRIPTION
Manages the startup, shutdown, and status of the Ecommerce Data Platform services
in both Docker Compose and Kubernetes modes.

.PARAMETER Mode
The deployment mode: 'docker' (default) or 'k8s'.

.PARAMETER Command
The action to perform: 'start', 'stop', 'restart', 'status', 'init', 'logs', 'health'.

.PARAMETER Service
Optional target service name (e.g. for logs or starting a specific service).
#>

param (
    [Parameter(Mandatory=$false)][string]$Mode = "docker",
    [Parameter(Mandatory=$true, Position=0)][string]$Command,
    [Parameter(Mandatory=$false, Position=1)][string]$Service = ""
)

$ErrorActionPreference = "Stop"

if ($Mode -notin @("docker", "k8s")) {
    Write-Error "Invalid mode '$Mode'. Must be 'docker' or 'k8s'."
    exit 1
}

Write-Host ">>> Running in $Mode mode <<<" -ForegroundColor Yellow

# ==================== Docker Functions ====================
function Docker-Start {
    if ($Service) {
        Write-Host "Starting Docker service: $Service"
        docker compose up -d $Service
    } else {
        Write-Host "Starting all Docker services..."
        docker compose up -d
    }
}

function Docker-Stop {
    if ($Service) {
        Write-Host "Stopping Docker service: $Service"
        docker compose stop $Service
    } else {
        Write-Host "Stopping all Docker services..."
        docker compose down
    }
}

function Docker-Status {
    Write-Host "Docker Compose status:"
    docker compose ps
}

function Docker-Logs {
    if ([string]::IsNullOrWhiteSpace($Service)) {
        Write-Error "Service name required for 'logs' command"
        exit 1
    }
    docker compose logs -f $Service
}

function Docker-Init {
    Write-Host "Triggering initialization jobs in Docker Compose..."
    docker compose start minio-init kafka-init airflow-init superset-init
    
    Write-Host "Registering Debezium connectors..."
    if (Test-Path ".\debezium\connectors\register-connector.ps1") {
        & ".\debezium\connectors\register-connector.ps1"
    } else {
        Write-Host "Warning: register-connector.ps1 not found" -ForegroundColor Yellow
    }
}

function Docker-Health {
    Write-Host "Checking health of core Docker services..."
    docker ps --format "table {{.Names}}\t{{.Status}}" | Select-String "dpl-mysql|dpl-postgres|dpl-kafka|dpl-clickhouse"
}

# ==================== K8s Functions ====================
function K8s-Start {
    Write-Host "Applying K8s manifests..."
    kubectl apply -f k8s\namespace.yml
    kubectl apply -f k8s\secrets.yml
    
    # DBs and Storage
    kubectl apply -f k8s\mysql.yml
    kubectl apply -f k8s\postgres.yml
    kubectl apply -f k8s\minio.yml
    kubectl apply -f k8s\clickhouse.yml
    
    # Messaging & Compute
    kubectl apply -f k8s\zookeeper.yml
    kubectl apply -f k8s\kafka.yml
    kubectl apply -f k8s\flink.yml
    kubectl apply -f k8s\debezium.yml
    
    # Apps
    kubectl apply -f k8s\airflow.yml
    kubectl apply -f k8s\superset.yml
    kubectl apply -f k8s\atlas.yml
    kubectl apply -f k8s\fastapi.yml
    kubectl apply -f k8s\frontend.yml
    kubectl apply -f k8s\monitoring.yml
    
    Write-Host "K8s resources applied successfully." -ForegroundColor Green
}

function K8s-Stop {
    Write-Host "Deleting K8s resources..."
    kubectl delete -f k8s\ --ignore-not-found=true
    Write-Host "K8s resources deleted successfully." -ForegroundColor Green
}

function K8s-Status {
    Write-Host "Kubernetes status for namespace 'ecommerce-data-platform':"
    kubectl get pods,svc,pvc -n ecommerce-data-platform
}

function K8s-Logs {
    if ([string]::IsNullOrWhiteSpace($Service)) {
        Write-Error "Service name required for 'logs' command"
        exit 1
    }
    $PodInfo = kubectl get pod -n ecommerce-data-platform -l app=$Service -o jsonpath="{.items[0].metadata.name}" 2>$null
    if ($PodInfo) {
        Write-Host "Tailing logs for $PodInfo..."
        kubectl logs -f $PodInfo -n ecommerce-data-platform
    } else {
        Write-Host "No pod found with label app=$Service" -ForegroundColor Red
    }
}

function K8s-Init {
    Write-Host "Re-running K8s init jobs..."
    kubectl delete job minio-init kafka-init airflow-init superset-init -n ecommerce-data-platform --ignore-not-found
    kubectl apply -f k8s\minio.yml
    kubectl apply -f k8s\kafka.yml
    kubectl apply -f k8s\airflow.yml
    kubectl apply -f k8s\superset.yml
}

function K8s-Health {
    Write-Host "Recent K8s events in ecommerce-data-platform namespace:"
    kubectl get events -n ecommerce-data-platform --sort-by=".metadata.creationTimestamp" | Select-Object -Last 10
}

# ==================== Main Router ====================
switch ($Command.ToLower()) {
    "start" {
        if ($Mode -eq "docker") { Docker-Start } else { K8s-Start }
    }
    "stop" {
        if ($Mode -eq "docker") { Docker-Stop } else { K8s-Stop }
    }
    "restart" {
        if ($Mode -eq "docker") { 
            Docker-Stop
            Docker-Start 
        } else { 
            K8s-Stop
            K8s-Start 
        }
    }
    "status" {
        if ($Mode -eq "docker") { Docker-Status } else { K8s-Status }
    }
    "init" {
        if ($Mode -eq "docker") { Docker-Init } else { K8s-Init }
    }
    "logs" {
        if ($Mode -eq "docker") { Docker-Logs } else { K8s-Logs }
    }
    "health" {
        if ($Mode -eq "docker") { Docker-Health } else { K8s-Health }
    }
    default {
        Write-Error "Unknown command '$Command'. Supported commands: start, stop, restart, status, init, logs, health"
        exit 1
    }
}

exit 0
