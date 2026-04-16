#!/bin/bash
set -e

FLINK_URL="http://localhost:8081"
JOBS_DIR="./jobs"

submit_job() {
    local job_file=$1
    local job_name=$(basename "$job_file" .py)
    echo "=== Submitting Flink job: $job_name ==="
    
    /opt/flink/bin/flink run \
        -d \
        -py "$job_file" \
        -pyfs "$JOBS_DIR" \
        -c "org.apache.flink.client.cli.CliFrontend"
    
    echo "Job $job_name submitted"
}

echo "=== Submitting Flink Streaming Jobs ==="

submit_job "$JOBS_DIR/realtime_etl.py"
submit_job "$JOBS_DIR/order_analytics.py"

echo ""
echo "=== Running Flink Jobs ==="
curl -s "$FLINK_URL/jobs/overview" | python3 -m json.tool 2>/dev/null || \
    curl -s "$FLINK_URL/jobs/overview"
