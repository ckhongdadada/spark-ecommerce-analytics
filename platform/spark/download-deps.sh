#!/bin/bash
set -e

JAR_PATH="/opt/spark/jars"
ICEBERG_VERSION="1.4.3"
SPARK_VERSION="3.4"

echo "=== Downloading Iceberg Spark Runtime ==="
if [ ! -f "${JAR_PATH}/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar" ]; then
    wget -q -P "${JAR_PATH}" \
        "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_VERSION}_2.12-${ICEBERG_VERSION}.jar"
fi

echo "=== Downloading AWS SDK for S3 ==="
if [ ! -f "${JAR_PATH}/aws-java-sdk-bundle-1.12.262.jar" ]; then
    wget -q -P "${JAR_PATH}" \
        "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"
fi

if [ ! -f "${JAR_PATH}/hadoop-aws-3.3.4.jar" ]; then
    wget -q -P "${JAR_PATH}" \
        "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"
fi

echo "=== All Spark Iceberg dependencies ready ==="
ls -la "${JAR_PATH}"/
