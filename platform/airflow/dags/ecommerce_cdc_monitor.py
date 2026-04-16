"""
电商数据平台 - Debezium CDC 监控 DAG
每10分钟检查一次 Debezium 连接器状态
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "ecommerce",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_cdc_monitor",
    default_args=default_args,
    description="Debezium CDC 连接器状态监控",
    schedule_interval="*/10 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["cdc", "debezium", "monitoring"],
    max_active_runs=1,
) as dag:

    check_debezium_status = BashOperator(
        task_id="check_debezium_connector",
        bash_command="""
            STATUS=$(curl -s http://debezium:8083/connectors/ecommerce-mysql-connector/status)
            echo "$STATUS"
            STATE=$(echo "$STATUS" | python3 -c "
            import sys, json
            data = json.load(sys.stdin)
            state = data.get('connector', {}).get('state', 'UNKNOWN')
            print(state)
            " 2>/dev/null || echo "FAILED")
            
            if [ "$STATE" != "RUNNING" ]; then
                echo "ALERT: Debezium connector is $STATE"
                exit 1
            fi
            echo "Debezium connector is RUNNING"
        """,
    )

    check_debezium_tasks = BashOperator(
        task_id="check_debezium_tasks",
        bash_command="""
            curl -s http://debezium:8083/connectors/ecommerce-mysql-connector/status | python3 -c "
            import sys, json
            data = json.load(sys.stdin)
            tasks = data.get('tasks', [])
            for task in tasks:
                if task.get('state') != 'RUNNING':
                    print(f'ALERT: Task {task.get(\"id\")} is {task.get(\"state\")}')
                    sys.exit(1)
            print(f'All {len(tasks)} Debezium tasks running')
            "
        """,
    )

    check_debezium_status >> check_debezium_tasks
