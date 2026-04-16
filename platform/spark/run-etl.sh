#!/bin/bash
set -e

SPARK_MASTER="${SPARK_MASTER:-local[*]}"
DT="${1:-}"

echo "=== Submitting Spark Iceberg ETL Job ==="
echo "Master: $SPARK_MASTER"
echo "Date partition: ${DT:-all}"

spark-submit \
    --master "$SPARK_MASTER" \
    --name "Ecommerce-Iceberg-ETL" \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
    --repositories https://repo1.maven.org/maven2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.ecommerce=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.ecommerce.type=hadoop \
    --conf spark.sql.catalog.ecommerce.warehouse=s3a://iceberg/warehouse \
    --conf spark.sql.catalog.ecommerce.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
    --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
    --conf spark.hadoop.fs.s3a.access.key=admin \
    --conf spark.hadoop.fs.s3a.secret.key=admin123456 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.sql.session.timeZone=Asia/Shanghai \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.sql.shuffle.partitions=8 \
    iceberg_etl.py "$DT"

echo "=== Spark Iceberg ETL completed ==="
