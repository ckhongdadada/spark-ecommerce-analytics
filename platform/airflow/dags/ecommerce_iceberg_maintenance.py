"""
电商数据平台 - Iceberg 表维护 DAG
每天凌晨3点执行表维护（快照过期、孤儿文件清理）
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


default_args = {
    "owner": "ecommerce",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

TABLES = [
    "ecommerce.ods.user_behavior",
    "ecommerce.dwd.user_behavior_detail",
    "ecommerce.dwd.user_profile",
    "ecommerce.dwd.item_profile",
    "ecommerce.dws.user_daily",
    "ecommerce.dws.item_daily",
    "ecommerce.dws.category_daily",
    "ecommerce.ads.user_metrics",
    "ecommerce.ads.item_metrics",
    "ecommerce.ads.category_metrics",
]

with DAG(
    dag_id="ecommerce_iceberg_maintenance",
    default_args=default_args,
    description="Iceberg 表维护：快照过期、孤儿文件清理",
    schedule_interval="0 3 * * *",
    start_date=days_ago(1),
    catchup=False,
    tags=["iceberg", "maintenance"],
    max_active_runs=1,
) as dag:

    for table in TABLES:
        safe_name = table.replace(".", "_")
        
        expire_snapshots = BashOperator(
            task_id=f"expire_snapshots_{safe_name}",
            bash_command=f"""
                spark-sql \
                    --master local[2] \
                    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                    --conf spark.sql.catalog.ecommerce=org.apache.iceberg.spark.SparkCatalog \
                    --conf spark.sql.catalog.ecommerce.type=hadoop \
                    --conf spark.sql.catalog.ecommerce.warehouse=s3a://iceberg/warehouse \
                    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                    --conf spark.hadoop.fs.s3a.access.key=admin \
                    --conf spark.hadoop.fs.s3a.secret.key=admin123456 \
                    --conf spark.hadoop.fs.s3a.path.style.access=true \
                    -e "CALL ecommerce.system.expire_snapshots('{table}', TIMESTAMP '{{{{ ds }}}}' || ' 00:00:00.000', 100);"
            """,
        )

        remove_orphan_files = BashOperator(
            task_id=f"remove_orphan_files_{safe_name}",
            bash_command=f"""
                spark-sql \
                    --master local[2] \
                    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
                    --conf spark.sql.catalog.ecommerce=org.apache.iceberg.spark.SparkCatalog \
                    --conf spark.sql.catalog.ecommerce.type=hadoop \
                    --conf spark.sql.catalog.ecommerce.warehouse=s3a://iceberg/warehouse \
                    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
                    --conf spark.hadoop.fs.s3a.access.key=admin \
                    --conf spark.hadoop.fs.s3a.secret.key=admin123456 \
                    --conf spark.hadoop.fs.s3a.path.style.access=true \
                    -e "CALL ecommerce.system.remove_orphan_files('{table}', TIMESTAMP '{{{{ ds }}}}' || ' 00:00:00.000');"
            """,
        )

        expire_snapshots >> remove_orphan_files
