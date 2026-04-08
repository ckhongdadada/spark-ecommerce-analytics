package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.util.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * Module 3: Structured Streaming.
 */
object Module3_Streaming extends Logging {

  private val streamingOutputRoot = AppConfig.STREAM_OUTPUT_PATH

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("3")}")
    logger.info("=" * 60)

    val useKafka = System.getProperty("streaming.source", "socket") == "kafka"
    if (useKafka) runWithKafka() else runWithSocket()
  }

  def runWithKafka(): Unit = {
    logger.info("Streaming source: Kafka")
    val spark = SparkSessionFactory.getSession()

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.KAFKA_BROKERS)
      .option("subscribe", AppConfig.KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .load()

    val schema = StructType(Seq(
      StructField("userId", StringType, nullable = false),
      StructField("itemId", StringType, nullable = false),
      StructField("behavior", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    ))

    val eventDF = rawStream
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json(col("json_str"), schema).alias("data"))
      .select("data.*")
      .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))

    val windowedDF = aggregateMetricsByWindow(
      eventDF.withWatermark("event_time", "10 seconds"),
      s"${AppConfig.STREAM_WINDOW} seconds",
      s"${AppConfig.STREAM_SLIDE} seconds"
    )

    val query = writeWindowReport(
      flattenWindowedMetrics(windowedDF),
      s"$streamingOutputRoot/kafka",
      s"${AppConfig.CHECKPOINT_PATH}/kafka",
      "10 seconds"
    )

    logger.info(s"Kafka output path: $streamingOutputRoot/kafka")
    query.awaitTermination(AppConfig.STREAM_TIMEOUT)
    if (query.isActive) query.stop()
  }

  def runWithSocket(): Unit = {
    logger.info(s"Streaming source: Socket (${AppConfig.SOCKET_HOST}:${AppConfig.SOCKET_PORT})")
    logger.info("Input format: userId,itemId,category,behavior,timestamp")

    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", AppConfig.SOCKET_HOST)
      .option("port", AppConfig.SOCKET_PORT)
      .load()

    val eventDF = lines
      .as[String]
      .map(_.split(",").map(_.trim))
      .filter(_.length == 5)
      .flatMap { fields =>
        scala.util.Try(fields(4).toLong).toOption.map { ts =>
          (fields(0), fields(1), fields(2), fields(3), ts)
        }
      }
      .toDF("userId", "itemId", "category", "behavior", "timestamp")
      .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))

    val pvuvDF = aggregateMetricsByWindow(
      eventDF.withWatermark("event_time", "20 seconds"),
      "1 minute",
      "30 seconds"
    )

    val query = writeWindowReport(
      flattenWindowedMetrics(pvuvDF),
      s"$streamingOutputRoot/socket",
      s"${AppConfig.CHECKPOINT_PATH}/socket",
      "10 seconds"
    )

    logger.info(s"Socket output path: $streamingOutputRoot/socket")
    query.awaitTermination(AppConfig.STREAM_TIMEOUT * 2L)
    if (query.isActive) query.stop()
    logger.info("Module 3 completed")
  }

  private[module] def aggregateMetricsByWindow(df: DataFrame, windowDuration: String, slideDuration: String): DataFrame = {
    df.groupBy(
      window(col("event_time"), windowDuration, slideDuration),
      col("behavior")
    )
      .agg(
        count("*").alias("pv"),
        approx_count_distinct(col("userId")).alias("uv")
      )
  }

  private[module] def flattenWindowedMetrics(df: DataFrame): DataFrame = {
    df.select(
      col("window.start").alias("window_start"),
      col("window.end").alias("window_end"),
      col("behavior"),
      col("pv"),
      col("uv")
    )
  }

  private def writeWindowReport(df: DataFrame, outputPath: String, checkpointPath: String, triggerInterval: String) = {
    df.writeStream
      .format("parquet")
      .outputMode("append")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointPath)
      .trigger(Trigger.ProcessingTime(triggerInterval))
      .start()
  }
}
