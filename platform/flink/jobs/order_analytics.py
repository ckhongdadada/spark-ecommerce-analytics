from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings


def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(2)
    env.enable_checkpointing(30000)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    t_env.get_config().set_local_timezone("Asia/Shanghai")

    t_env.execute_sql("""
        CREATE TABLE kafka_order_events (
            order_id STRING,
            user_id STRING,
            item_id STRING,
            category STRING,
            amount DECIMAL(10, 2),
            status STRING,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            op STRING METADATA FROM 'value.op',
            WATERMARK FOR updated_at AS updated_at - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'ecommerce.order_events',
            'properties.bootstrap.servers' = 'kafka:29092',
            'properties.group.id' = 'flink-order-analytics',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE clickhouse_order_metrics (
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            category STRING,
            order_count BIGINT,
            total_amount DECIMAL(20, 2),
            unique_buyers BIGINT,
            avg_order_amount DECIMAL(20, 2),
            completed_count BIGINT,
            cancelled_count BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = 'jdbc:clickhouse://clickhouse:8123/ecommerce',
            'table-name' = 'realtime_order_metrics',
            'username' = 'default',
            'password' = 'clickhouse123',
            'sink.buffer-flush.max-rows' = '1000',
            'sink.buffer-flush.interval' = '5s'
        )
    """)

    t_env.execute_sql("""
        CREATE TABLE iceberg_dwd_order (
            order_id STRING,
            user_id STRING,
            item_id STRING,
            category STRING,
            amount DECIMAL(10, 2),
            status STRING,
            created_at TIMESTAMP(3),
            updated_at TIMESTAMP(3),
            dt STRING,
            load_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'iceberg',
            'catalog-name' = 'ecommerce',
            'warehouse' = 's3://iceberg/warehouse',
            's3.endpoint' = 'http://minio:9000',
            's3.access-key-id' = 'admin',
            's3.secret-access-key' = 'admin123456',
            's3.path-style-access' = 'true',
            'table-name' = 'ecommerce.dwd.order_detail',
            'write.format.default' = 'parquet'
        )
    """)

    t_env.execute_sql("""
        INSERT INTO iceberg_dwd_order
        SELECT
            order_id,
            user_id,
            item_id,
            category,
            amount,
            status,
            created_at,
            updated_at,
            DATE_FORMAT(updated_at, 'yyyy-MM-dd') AS dt,
            CURRENT_TIMESTAMP AS load_time
        FROM kafka_order_events
        WHERE order_id IS NOT NULL
          AND user_id IS NOT NULL
    """)

    t_env.execute_sql("""
        INSERT INTO clickhouse_order_metrics
        SELECT
            TUMBLE_START(updated_at, INTERVAL '5' MINUTE) AS window_start,
            TUMBLE_END(updated_at, INTERVAL '5' MINUTE) AS window_end,
            category,
            COUNT(*) AS order_count,
            SUM(amount) AS total_amount,
            COUNT(DISTINCT user_id) AS unique_buyers,
            AVG(amount) AS avg_order_amount,
            SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) AS completed_count,
            SUM(CASE WHEN status = 'cancelled' THEN 1 ELSE 0 END) AS cancelled_count
        FROM kafka_order_events
        WHERE status IN ('created', 'paid', 'shipped', 'completed', 'cancelled')
        GROUP BY
            TUMBLE(updated_at, INTERVAL '5' MINUTE),
            category
    """)

    print("Flink order analytics job submitted successfully")


if __name__ == "__main__":
    main()
