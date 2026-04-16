"""
电商数据平台 - 每日离线 ETL 调度 DAG
ODS -> DWD -> DWS -> ADS 全流程
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "ecommerce",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="ecommerce_daily_etl",
    default_args=default_args,
    description="每日离线ETL: ODS->DWD->DWS->ADS",
    schedule_interval="0 2 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["etl", "spark", "iceberg"],
    max_active_runs=1,
) as dag:

    build_dwd = BashOperator(
        task_id="build_dwd_layer",
        bash_command="""
            cd /opt/airflow/scripts
            python3 iceberg_etl_dwd.py {{ ds }}
        """,
    )

    build_dws = BashOperator(
        task_id="build_dws_layer",
        bash_command="""
            cd /opt/airflow/scripts
            python3 iceberg_etl_dws.py {{ ds }}
        """,
    )

    build_ads = BashOperator(
        task_id="build_ads_layer",
        bash_command="""
            cd /opt/airflow/scripts
            python3 iceberg_etl_ads.py {{ ds }}
        """,
    )

    sync_clickhouse = BashOperator(
        task_id="sync_to_clickhouse",
        bash_command="""
            cd /opt/airflow/scripts
            python3 clickhouse_sync.py
        """,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="""
            cd /opt/airflow/scripts/dbt_project
            dbt run --target prod
        """,
    )

    run_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command="""
            cd /opt/airflow/scripts
            python3 great_expectations_check.py {{ ds }}
        """,
    )

    build_dwd >> build_dws >> build_ads >> sync_clickhouse >> run_dbt >> run_quality_check
