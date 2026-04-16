from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
from pyflink.table.window import Tumble
from pyflink.common import Duration


def create_kafka_source(t_env):
    t_env.execute_sql("""
        CREATE TABLE kafka_user_behavior (
            user_id STRING,
            item_id STRING,
            category STRING,
            behavior STRING,
            behavior_time TIMESTAMP(3),
            op STRING METADATA FROM 'value.op',
            source_table STRING METADATA FROM 'value.source.table',
            ts_ms BIGINT METADATA FROM 'value.ts_ms',
            proctime AS PROCTIME(),
            WATERMARK FOR behavior_time AS behavior_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ecommerce.user_behavior_log',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-ecommerce-etl',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)


def create_iceberg_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE iceberg_ods_user_behavior (
            user_id STRING,
            item_id STRING,
            category STRING,
            behavior STRING,
            behavior_time TIMESTAMP(3),
            op STRING,
            source_table STRING,
            ts_ms BIGINT,
            dt STRING,
            load_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'iceberg',
            'catalog-name' = 'ecommerce',
            'catalog-impl' = 'org.apache.iceberg.nessie.NessieCatalog',
            'warehouse' = 's3://iceberg/warehouse',
            's3.endpoint' = 'http://minio:9000',
            's3.access-key-id' = 'admin',
            's3.secret-access-key' = 'admin123456',
            's3.path-style-access' = 'true',
            'table-name' = 'ecommerce.ods.user_behavior',
            'write.format.default' = 'parquet',
            'write.metadata.delete-after-commit.enabled' = 'true',
            'write.metadata.previous-versions-max' = '10'
        )
    """)


def create_clickhouse_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE clickhouse_realtime_metrics (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            category STRING,
            total_events BIGINT,
            unique_users BIGINT,
            view_count BIGINT,
            cart_count BIGINT,
            buy_count BIGINT,
            fav_count BIGINT,
            conversion_rate DOUBLE
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:clickhouse://clickhouse:8123/ecommerce',
            'table-name' = 'realtime_category_metrics',
            'username' = 'default',
            'password' = 'clickhouse123',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '5s',
            'sink.max-retries' = '3'
        )
    """)


def create_kafka_alert_sink(t_env):
    t_env.execute_sql("""
        CREATE TABLE kafka_alert_events (
            alert_type STRING,
            alert_level STRING,
            category STRING,
            metric_name STRING,
            metric_value DOUBLE,
            threshold DOUBLE,
            alert_time TIMESTAMP(3),
            description STRING
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'flink.alert_events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'format' = 'json'
        )
    """)


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(60000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    t_env.get_config().set_local_timezone("Asia/Shanghai")

    create_kafka_source(t_env)
    create_iceberg_sink(t_env)
    create_clickhouse_sink(t_env)
    create_kafka_alert_sink(t_env)

    t_env.execute_sql("""
        INSERT INTO iceberg_ods_user_behavior
        SELECT
            user_id,
            item_id,
            category,
            behavior,
            behavior_time,
            op,
            source_table,
            ts_ms,
            DATE_FORMAT(behavior_time, 'yyyy-MM-dd') AS dt,
            CURRENT_TIMESTAMP AS load_time
        FROM kafka_user_behavior
        WHERE behavior IN ('pv', 'buy', 'cart', 'fav')
          AND user_id IS NOT NULL
          AND item_id IS NOT NULL
    """)

    t_env.execute_sql("""
        INSERT INTO clickhouse_realtime_metrics
        SELECT
            TUMBLE_START(proctime, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(proctime, INTERVAL '1' MINUTE) AS window_end,
            category,
            COUNT(*) AS total_events,
            COUNT(DISTINCT user_id) AS unique_users,
            SUM(CASE WHEN behavior = 'pv' THEN 1 ELSE 0 END) AS view_count,
            SUM(CASE WHEN behavior = 'cart' THEN 1 ELSE 0 END) AS cart_count,
            SUM(CASE WHEN behavior = 'buy' THEN 1 ELSE 0 END) AS buy_count,
            SUM(CASE WHEN behavior = 'fav' THEN 1 ELSE 0 END) AS fav_count,
            ROUND(
                CAST(SUM(CASE WHEN behavior = 'buy' THEN 1 ELSE 0 END) AS DOUBLE)
                / NULLIF(CAST(SUM(CASE WHEN behavior = 'pv' THEN 1 ELSE 0 END) AS DOUBLE), 0)
                * 100, 2
            ) AS conversion_rate
        FROM kafka_user_behavior
        GROUP BY
            TUMBLE(proctime, INTERVAL '1' MINUTE),
            category
    """)

    t_env.execute_sql("""
        INSERT INTO kafka_alert_events
        SELECT
            'low_conversion' AS alert_type,
            CASE
                WHEN conversion_rate < 1.0 THEN 'CRITICAL'
                WHEN conversion_rate < 2.0 THEN 'WARNING'
                ELSE 'INFO'
            END AS alert_level,
            category,
            'conversion_rate' AS metric_name,
            conversion_rate AS metric_value,
            2.0 AS threshold,
            TUMBLE_START(proctime, INTERVAL '5' MINUTE) AS alert_time,
            CONCAT('Category ', category, ' conversion rate ', CAST(conversion_rate AS STRING), '% below threshold 2%') AS description
        FROM (
            SELECT
                category,
                ROUND(
                    CAST(SUM(CASE WHEN behavior = 'buy' THEN 1 ELSE 0 END) AS DOUBLE)
                    / NULLIF(CAST(SUM(CASE WHEN behavior = 'pv' THEN 1 ELSE 0 END) AS DOUBLE), 0)
                    * 100, 2
                ) AS conversion_rate,
                TUMBLE_START(proctime, INTERVAL '5' MINUTE) AS window_start
            FROM kafka_user_behavior
            GROUP BY
                TUMBLE(proctime, INTERVAL '5' MINUTE),
                category
        )
        WHERE conversion_rate < 2.0
    """)

    print("Flink streaming job submitted successfully")


if __name__ == "__main__":
    main()
