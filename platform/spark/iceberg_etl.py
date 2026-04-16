#!/usr/bin/env python3
"""
Spark + Iceberg + S3 离线数仓 ETL 脚本
从 Iceberg 数据湖读取 ODS 层数据，构建 DWD/DWS/ADS 层
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *


def create_spark_session():
    return SparkSession.builder \
        .appName("Ecommerce-Iceberg-ETL") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.ecommerce", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.ecommerce.type", "hadoop") \
        .config("spark.sql.catalog.ecommerce.warehouse", "s3a://iceberg/warehouse") \
        .config("spark.sql.catalog.ecommerce.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "admin123456") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.session.timeZone", "Asia/Shanghai") \
        .config("spark.sql.shuffle.partitions", "8") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()


def create_iceberg_tables(spark):
    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.ods.user_behavior (
            user_id STRING,
            item_id STRING,
            category STRING,
            behavior STRING,
            behavior_time TIMESTAMP,
            op STRING,
            source_table STRING,
            ts_ms BIGINT,
            dt STRING,
            load_time TIMESTAMP
        ) USING iceberg
        PARTITIONED BY (dt)
        TBLPROPERTIES ('write.format.default' = 'parquet')
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.dwd.user_behavior_detail (
            user_id STRING,
            item_id STRING,
            category STRING,
            behavior STRING,
            behavior_time TIMESTAMP,
            behavior_date DATE,
            behavior_hour INT,
            behavior_day_of_week INT,
            is_weekend INT,
            session_id STRING,
            dt STRING
        ) USING iceberg
        PARTITIONED BY (dt)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.dwd.user_profile (
            user_id STRING,
            first_behavior_time TIMESTAMP,
            last_behavior_time TIMESTAMP,
            total_behaviors BIGINT,
            unique_items BIGINT,
            unique_categories BIGINT,
            view_count BIGINT,
            cart_count BIGINT,
            buy_count BIGINT,
            fav_count BIGINT,
            user_level STRING,
            conversion_rate DOUBLE
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.dwd.item_profile (
            item_id STRING,
            category STRING,
            total_interactions BIGINT,
            unique_users BIGINT,
            view_count BIGINT,
            cart_count BIGINT,
            buy_count BIGINT,
            fav_count BIGINT,
            item_popularity STRING,
            conversion_rate DOUBLE
        ) USING iceberg
        PARTITIONED BY (category)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.dws.user_daily (
            user_id STRING,
            behavior_date DATE,
            total_behaviors BIGINT,
            unique_items BIGINT,
            unique_categories BIGINT,
            session_count BIGINT,
            view_count BIGINT,
            cart_count BIGINT,
            buy_count BIGINT,
            fav_count BIGINT,
            first_active_hour INT,
            last_active_hour INT,
            active_hours INT,
            conversion_rate DOUBLE
        ) USING iceberg
        PARTITIONED BY (behavior_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.dws.item_daily (
            item_id STRING,
            category STRING,
            behavior_date DATE,
            total_interactions BIGINT,
            unique_users BIGINT,
            session_count BIGINT,
            view_count BIGINT,
            cart_count BIGINT,
            buy_count BIGINT,
            fav_count BIGINT,
            conversion_rate DOUBLE
        ) USING iceberg
        PARTITIONED BY (behavior_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.dws.category_daily (
            category STRING,
            behavior_date DATE,
            total_interactions BIGINT,
            unique_users BIGINT,
            unique_items BIGINT,
            session_count BIGINT,
            view_count BIGINT,
            cart_count BIGINT,
            buy_count BIGINT,
            fav_count BIGINT,
            conversion_rate DOUBLE,
            avg_interactions_per_user DOUBLE
        ) USING iceberg
        PARTITIONED BY (behavior_date)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.ads.user_metrics (
            user_id STRING,
            user_level STRING,
            active_days BIGINT,
            total_behaviors BIGINT,
            total_views BIGINT,
            total_carts BIGINT,
            total_buys BIGINT,
            avg_conversion_rate DOUBLE,
            avg_behaviors_per_day DOUBLE,
            user_value_score DOUBLE,
            last_active_date DATE
        ) USING iceberg
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.ads.item_metrics (
            item_id STRING,
            category STRING,
            item_popularity STRING,
            active_days BIGINT,
            total_interactions BIGINT,
            total_users BIGINT,
            total_views BIGINT,
            total_carts BIGINT,
            total_buys BIGINT,
            avg_conversion_rate DOUBLE,
            avg_interactions_per_day DOUBLE,
            item_heat_score DOUBLE
        ) USING iceberg
        PARTITIONED BY (category)
    """)

    spark.sql("""
        CREATE TABLE IF NOT EXISTS ecommerce.ads.category_metrics (
            category STRING,
            active_days BIGINT,
            total_interactions BIGINT,
            total_users BIGINT,
            total_items BIGINT,
            total_views BIGINT,
            total_carts BIGINT,
            total_buys BIGINT,
            avg_conversion_rate DOUBLE,
            avg_interactions_per_user DOUBLE,
            category_rank BIGINT,
            market_share DOUBLE
        ) USING iceberg
    """)

    print("All Iceberg tables created successfully")


def build_dwd_layer(spark, dt):
    print(f"Building DWD layer for dt={dt}...")

    ods_df = spark.table("ecommerce.ods.user_behavior").filter(F.col("dt") == dt)

    dwd_behavior = ods_df \
        .filter(F.col("user_id").isNotNull() & F.col("item_id").isNotNull()) \
        .filter(F.col("behavior").isin("pv", "buy", "cart", "fav")) \
        .withColumn("behavior_date", F.to_date("behavior_time")) \
        .withColumn("behavior_hour", F.hour("behavior_time")) \
        .withColumn("behavior_day_of_week", F.dayofweek("behavior_time")) \
        .withColumn("is_weekend", F.when(F.col("behavior_day_of_week").isin(1, 7), 1).otherwise(0)) \
        .withColumn("session_id", F.concat("user_id", F.lit("_"), F.date_format("behavior_time", "yyyyMMddHH"))) \
        .select(
            "user_id", "item_id", "category", "behavior", "behavior_time",
            "behavior_date", "behavior_hour", "behavior_day_of_week", "is_weekend",
            "session_id", "dt"
        )

    dwd_behavior.writeTo("ecommerce.dwd.user_behavior_detail").overwritePartitions()
    print(f"DWD user_behavior_detail: {dwd_behavior.count()} rows")

    dwd_user_profile = ods_df.groupBy("user_id").agg(
        F.min("behavior_time").alias("first_behavior_time"),
        F.max("behavior_time").alias("last_behavior_time"),
        F.count("*").alias("total_behaviors"),
        F.countDistinct("item_id").alias("unique_items"),
        F.countDistinct("category").alias("unique_categories"),
        F.sum(F.when(F.col("behavior") == "pv", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("behavior") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("behavior") == "buy", 1).otherwise(0)).alias("buy_count"),
        F.sum(F.when(F.col("behavior") == "fav", 1).otherwise(0)).alias("fav_count"),
    ).withColumn("user_level",
        F.when(F.col("total_behaviors") >= 100, "high_active")
         .when(F.col("total_behaviors") >= 50, "medium_active")
         .when(F.col("total_behaviors") >= 10, "low_active")
         .otherwise("new_user")
    ).withColumn("conversion_rate",
        F.round(F.col("buy_count") * 100.0 / F.when(F.col("view_count") == 0, None).otherwise(F.col("view_count")), 2)
    )

    dwd_user_profile.writeTo("ecommerce.dwd.user_profile").overwritePartitions()
    print(f"DWD user_profile: {dwd_user_profile.count()} rows")

    dwd_item_profile = ods_df.groupBy("item_id", "category").agg(
        F.count("*").alias("total_interactions"),
        F.countDistinct("user_id").alias("unique_users"),
        F.sum(F.when(F.col("behavior") == "pv", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("behavior") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("behavior") == "buy", 1).otherwise(0)).alias("buy_count"),
        F.sum(F.when(F.col("behavior") == "fav", 1).otherwise(0)).alias("fav_count"),
    ).withColumn("item_popularity",
        F.when(F.col("unique_users") >= 100, "hot")
         .when(F.col("unique_users") >= 50, "warm")
         .when(F.col("unique_users") >= 10, "normal")
         .otherwise("cold")
    ).withColumn("conversion_rate",
        F.round(F.col("buy_count") * 100.0 / F.when(F.col("view_count") == 0, None).otherwise(F.col("view_count")), 2)
    )

    dwd_item_profile.writeTo("ecommerce.dwd.item_profile").overwritePartitions()
    print(f"DWD item_profile: {dwd_item_profile.count()} rows")


def build_dws_layer(spark, dt):
    print(f"Building DWS layer for dt={dt}...")

    dwd = spark.table("ecommerce.dwd.user_behavior_detail").filter(F.col("dt") == dt)

    dws_user_daily = dwd.groupBy("user_id", "behavior_date").agg(
        F.count("*").alias("total_behaviors"),
        F.countDistinct("item_id").alias("unique_items"),
        F.countDistinct("category").alias("unique_categories"),
        F.countDistinct("session_id").alias("session_count"),
        F.sum(F.when(F.col("behavior") == "pv", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("behavior") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("behavior") == "buy", 1).otherwise(0)).alias("buy_count"),
        F.sum(F.when(F.col("behavior") == "fav", 1).otherwise(0)).alias("fav_count"),
        F.min("behavior_hour").alias("first_active_hour"),
        F.max("behavior_hour").alias("last_active_hour"),
    ).withColumn("active_hours", F.col("last_active_hour") - F.col("first_active_hour") + 1) \
     .withColumn("conversion_rate",
        F.round(F.col("buy_count") * 100.0 / F.when(F.col("view_count") == 0, None).otherwise(F.col("view_count")), 2))

    dws_user_daily.writeTo("ecommerce.dws.user_daily").overwritePartitions()
    print(f"DWS user_daily: {dws_user_daily.count()} rows")

    dws_item_daily = dwd.groupBy("item_id", "category", "behavior_date").agg(
        F.count("*").alias("total_interactions"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("session_id").alias("session_count"),
        F.sum(F.when(F.col("behavior") == "pv", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("behavior") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("behavior") == "buy", 1).otherwise(0)).alias("buy_count"),
        F.sum(F.when(F.col("behavior") == "fav", 1).otherwise(0)).alias("fav_count"),
    ).withColumn("conversion_rate",
        F.round(F.col("buy_count") * 100.0 / F.when(F.col("view_count") == 0, None).otherwise(F.col("view_count")), 2))

    dws_item_daily.writeTo("ecommerce.dws.item_daily").overwritePartitions()
    print(f"DWS item_daily: {dws_item_daily.count()} rows")

    dws_category_daily = dwd.groupBy("category", "behavior_date").agg(
        F.count("*").alias("total_interactions"),
        F.countDistinct("user_id").alias("unique_users"),
        F.countDistinct("item_id").alias("unique_items"),
        F.countDistinct("session_id").alias("session_count"),
        F.sum(F.when(F.col("behavior") == "pv", 1).otherwise(0)).alias("view_count"),
        F.sum(F.when(F.col("behavior") == "cart", 1).otherwise(0)).alias("cart_count"),
        F.sum(F.when(F.col("behavior") == "buy", 1).otherwise(0)).alias("buy_count"),
        F.sum(F.when(F.col("behavior") == "fav", 1).otherwise(0)).alias("fav_count"),
    ).withColumn("conversion_rate",
        F.round(F.col("buy_count") * 100.0 / F.when(F.col("view_count") == 0, None).otherwise(F.col("view_count")), 2)) \
     .withColumn("avg_interactions_per_user",
        F.round(F.col("total_interactions") / F.when(F.col("unique_users") == 0, None).otherwise(F.col("unique_users")), 2))

    dws_category_daily.writeTo("ecommerce.dws.category_daily").overwritePartitions()
    print(f"DWS category_daily: {dws_category_daily.count()} rows")


def build_ads_layer(spark, dt):
    print(f"Building ADS layer for dt={dt}...")

    dws_user_daily = spark.table("ecommerce.dws.user_daily")
    dwd_user_profile = spark.table("ecommerce.dwd.user_profile")
    dws_item_daily = spark.table("ecommerce.dws.item_daily")
    dwd_item_profile = spark.table("ecommerce.dwd.item_profile")
    dws_category_daily = spark.table("ecommerce.dws.category_daily")

    user_daily_summary = dws_user_daily.groupBy("user_id").agg(
        F.count("*").alias("active_days"),
        F.sum("total_behaviors").alias("total_behaviors"),
        F.sum("view_count").alias("total_views"),
        F.sum("cart_count").alias("total_carts"),
        F.sum("buy_count").alias("total_buys"),
        F.avg("conversion_rate").alias("avg_conversion_rate"),
        F.max("behavior_date").alias("last_active_date"),
    )

    ads_user_metrics = dwd_user_profile.join(user_daily_summary, "user_id", "left") \
        .withColumn("avg_behaviors_per_day",
            F.round(F.col("total_behaviors") / F.when(F.col("active_days") == 0, None).otherwise(F.col("active_days")), 2)) \
        .withColumn("user_value_score",
            F.round((F.col("total_buys") * 10 + F.col("total_carts") * 5 + F.col("total_views")) / 100.0, 2)) \
        .select(
            "user_id", "user_level", "active_days", "total_behaviors",
            "total_views", "total_carts", "total_buys", "avg_conversion_rate",
            "avg_behaviors_per_day", "user_value_score", "last_active_date"
        )

    ads_user_metrics.writeTo("ecommerce.ads.user_metrics").overwritePartitions()
    print(f"ADS user_metrics: {ads_user_metrics.count()} rows")

    item_daily_summary = dws_item_daily.groupBy("item_id", "category").agg(
        F.count("*").alias("active_days"),
        F.sum("total_interactions").alias("total_interactions"),
        F.sum("unique_users").alias("total_users"),
        F.sum("view_count").alias("total_views"),
        F.sum("cart_count").alias("total_carts"),
        F.sum("buy_count").alias("total_buys"),
        F.avg("conversion_rate").alias("avg_conversion_rate"),
    )

    ads_item_metrics = dwd_item_profile.join(item_daily_summary, ["item_id", "category"], "left") \
        .withColumn("avg_interactions_per_day",
            F.round(F.col("total_interactions") / F.when(F.col("active_days") == 0, None).otherwise(F.col("active_days")), 2)) \
        .withColumn("item_heat_score",
            F.round((F.col("total_buys") * 10 + F.col("total_carts") * 5 + F.col("total_views")) / 100.0, 2)) \
        .select(
            "item_id", "category", "item_popularity", "active_days",
            "total_interactions", "total_users", "total_views", "total_carts",
            "total_buys", "avg_conversion_rate", "avg_interactions_per_day", "item_heat_score"
        )

    ads_item_metrics.writeTo("ecommerce.ads.item_metrics").overwritePartitions()
    print(f"ADS item_metrics: {ads_item_metrics.count()} rows")

    total_buys = dws_category_daily.agg(F.sum("buy_count").alias("total")).collect()[0]["total"]

    ads_category_metrics = dws_category_daily.groupBy("category").agg(
        F.count("*").alias("active_days"),
        F.sum("total_interactions").alias("total_interactions"),
        F.sum("unique_users").alias("total_users"),
        F.sum("unique_items").alias("total_items"),
        F.sum("view_count").alias("total_views"),
        F.sum("cart_count").alias("total_carts"),
        F.sum("buy_count").alias("total_buys"),
        F.avg("conversion_rate").alias("avg_conversion_rate"),
        F.avg("avg_interactions_per_user").alias("avg_interactions_per_user"),
    ).withColumn("category_rank",
        F.row_number().over(Window.orderBy(F.desc("total_buys")))
    ).withColumn("market_share",
        F.round(F.col("total_buys") * 100.0 / F.lit(total_buys if total_buys else 1), 2)
    )

    ads_category_metrics.writeTo("ecommerce.ads.category_metrics").overwritePartitions()
    print(f"ADS category_metrics: {ads_category_metrics.count()} rows")


def main():
    dt = sys.argv[1] if len(sys.argv) > 1 else None

    spark = create_spark_session()
    create_iceberg_tables(spark)

    if dt:
        print(f"Running ETL for dt={dt}")
        build_dwd_layer(spark, dt)
        build_dws_layer(spark, dt)
        build_ads_layer(spark, dt)
    else:
        print("Running full ETL (all dates)")
        dts = spark.table("ecommerce.ods.user_behavior").select("dt").distinct().collect()
        for row in dts:
            current_dt = row["dt"]
            print(f"\n{'='*60}")
            print(f"Processing dt={current_dt}")
            print(f"{'='*60}")
            build_dwd_layer(spark, current_dt)
            build_dws_layer(spark, current_dt)
            build_ads_layer(spark, current_dt)

    print("\nIceberg ETL completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main()
