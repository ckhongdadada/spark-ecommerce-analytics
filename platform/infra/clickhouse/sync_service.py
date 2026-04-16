"""
ClickHouse 同步服务
从 Iceberg 数据湖同步 ADS 层数据到 ClickHouse
"""
import logging
import time
import sys
from clickhouse_driver import Client
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


CH_HOST = "clickhouse"
CH_PORT = 9000
CH_DB = "ecommerce"
CH_USER = "default"
CH_PASSWORD = "clickhouse123"


def get_ch_client():
    return Client(
        host=CH_HOST, port=CH_PORT, database=CH_DB,
        user=CH_USER, password=CH_PASSWORD,
        settings={"use_numpy": True}
    )


def get_spark():
    return SparkSession.builder \
        .appName("ClickHouse-Sync") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.ecommerce", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.ecommerce.type", "hadoop") \
        .config("spark.sql.catalog.ecommerce.warehouse", "s3a://iceberg/warehouse") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.session.timeZone", "Asia/Shanghai") \
        .getOrCreate()


def sync_table(spark, ch_client, iceberg_table, ch_table, columns):
    logger.info(f"Syncing {iceberg_table} -> {ch_table}")
    df = spark.table(iceberg_table).select(*columns)
    rows = df.collect()
    if not rows:
        logger.info(f"No data to sync for {iceberg_table}")
        return

    data = [list(row) for row in rows]
    ch_client.insert_dataframe(f"INSERT INTO {ch_table} VALUES", data)
    logger.info(f"Synced {len(data)} rows to {ch_table}")


def sync_ads_user_metrics(spark, ch_client):
    sync_table(
        spark, ch_client,
        "ecommerce.ads.user_metrics",
        "ads_user_metrics",
        ["user_id", "user_level", "active_days", "total_behaviors",
         "total_views", "total_carts", "total_buys", "avg_conversion_rate",
         "avg_behaviors_per_day", "user_value_score", "last_active_date"]
    )


def sync_ads_item_metrics(spark, ch_client):
    sync_table(
        spark, ch_client,
        "ecommerce.ads.item_metrics",
        "ads_item_metrics",
        ["item_id", "category", "item_popularity", "active_days",
         "total_interactions", "total_users", "total_views", "total_carts",
         "total_buys", "avg_conversion_rate", "avg_interactions_per_day",
         "item_heat_score"]
    )


def sync_ads_category_metrics(spark, ch_client):
    sync_table(
        spark, ch_client,
        "ecommerce.ads.category_metrics",
        "ads_category_metrics",
        ["category", "active_days", "total_interactions", "total_users",
         "total_items", "total_views", "total_carts", "total_buys",
         "avg_conversion_rate", "avg_interactions_per_user",
         "category_rank", "market_share"]
    )


def main():
    logger.info("Starting ClickHouse sync service")
    spark = get_spark()
    ch_client = get_ch_client()

    try:
        sync_ads_user_metrics(spark, ch_client)
        sync_ads_item_metrics(spark, ch_client)
        sync_ads_category_metrics(spark, ch_client)
        logger.info("ClickHouse sync completed successfully")
    except Exception as e:
        logger.error(f"Sync failed: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
