"""
电商数据平台 - 数据质量监控 DAG
每小时运行一次，检查实时数据质量
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "ecommerce",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="ecommerce_data_quality",
    default_args=default_args,
    description="数据质量监控与校验",
    schedule_interval="0 * * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["quality", "monitoring"],
    max_active_runs=1,
) as dag:

    check_kafka_lag = BashOperator(
        task_id="check_kafka_consumer_lag",
        bash_command="""
            cd /opt/airflow/scripts
            python3 check_kafka_lag.py
        """,
    )

    check_flink_jobs = BashOperator(
        task_id="check_flink_job_status",
        bash_command="""
            curl -s http://flink-jobmanager:8081/jobs/overview | python3 -c "
            import sys, json
            data = json.load(sys.stdin)
            for job in data.get('jobs', []):
                if job['state'] != 'RUNNING':
                    print(f'ALERT: Flink job {job[\"name\"]} is {job[\"state\"]}')
                    sys.exit(1)
            print('All Flink jobs running')
            "
        """,
    )

    check_clickhouse_freshness = BashOperator(
        task_id="check_clickhouse_data_freshness",
        bash_command="""
            cd /opt/airflow/scripts
            python3 check_ch_freshness.py
        """,
    )

    run_great_expectations = BashOperator(
        task_id="run_ge_validation",
        bash_command="""
            cd /opt/airflow/scripts
            great_expectations checkpoint run realtime_checkpoint
        """,
    )

    [check_kafka_lag, check_flink_jobs, check_clickhouse_freshness] >> run_great_expectations
