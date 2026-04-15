package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import com.ecommerce.util.{DataQualityGuard, Logging}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

/**
 * Module 3: Structured Streaming.
 *
 * Production-oriented improvements:
 * 1) source/sink fully config-driven (file / kafka / mysql)
 * 2) valid/invalid stream split with durable invalid sink
 * 3) window-level quality alerts persisted to file sink
 * 4) MySQL foreachBatch sink for dashboard integration
 */
object Module3_Streaming extends Logging {

  private val streamingOutputRoot = AppConfig.STREAM_OUTPUT_PATH

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("3")}")
    logger.info("=" * 60)

    AppConfig.STREAM_SOURCE match {
      case "kafka" => runWithKafka()
      case "socket" => runWithSocket()
      case unknown =>
        logger.warn(s"Unknown STREAM_SOURCE '$unknown', fallback to socket.")
        runWithSocket()
    }
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
      StructField("userId", StringType, nullable = true),
      StructField("itemId", StringType, nullable = true),
      StructField("category", StringType, nullable = true),
      StructField("behavior", StringType, nullable = true),
      StructField("timestamp", StringType, nullable = true)
    ))

    val eventDF = rawStream
      .selectExpr("CAST(value AS STRING) AS json_str")
      .select(from_json(col("json_str"), schema).alias("data"))
      .select("data.*")

    runStreamingPipeline(eventDF, sourceTag = "kafka")
  }

  def runWithSocket(): Unit = {
    logger.info(s"Streaming source: Socket (${AppConfig.SOCKET_HOST}:${AppConfig.SOCKET_PORT})")
    logger.info("Input format: userId,itemId,category,behavior,timestamp")

    val spark = SparkSessionFactory.getSession()

    val lines = spark.readStream
      .format("socket")
      .option("host", AppConfig.SOCKET_HOST)
      .option("port", AppConfig.SOCKET_PORT)
      .load()

    val eventDF = lines
      .select(split(col("value"), ",").alias("parts"))
      .select(
        when(size(col("parts")) >= 1, trim(element_at(col("parts"), 1))).alias("userId"),
        when(size(col("parts")) >= 2, trim(element_at(col("parts"), 2))).alias("itemId"),
        when(size(col("parts")) >= 3, trim(element_at(col("parts"), 3))).alias("category"),
        when(size(col("parts")) >= 4, trim(element_at(col("parts"), 4))).alias("behavior"),
        when(size(col("parts")) >= 5, trim(element_at(col("parts"), 5))).alias("timestamp")
      )

    runStreamingPipeline(eventDF, sourceTag = "socket")
  }

  private def runStreamingPipeline(inputDF: DataFrame, sourceTag: String): Unit = {
    val watermarkDelay = s"${AppConfig.STREAM_WINDOW + AppConfig.STREAM_SLIDE} seconds"
    val windowDuration = s"${AppConfig.STREAM_WINDOW} seconds"
    val slideDuration = s"${AppConfig.STREAM_SLIDE} seconds"

    val (validDF, invalidDF) = DataQualityGuard.splitValidAndInvalid(inputDF)
    val validWithWatermark = validDF.withWatermark("event_time", watermarkDelay)

    val metricsDF = flattenWindowedMetrics(
      aggregateMetricsByWindow(validWithWatermark, windowDuration, slideDuration)
    )

    val alertsDF = buildWindowAlerts(validWithWatermark, windowDuration, slideDuration)

    val metricsQuery = writeMetricsSink(metricsDF, sourceTag)
    val invalidQuery = writeFileSink(
      invalidDF,
      s"${AppConfig.STREAM_INVALID_OUTPUT_PATH}/$sourceTag",
      s"${AppConfig.CHECKPOINT_PATH}/${sourceTag}_invalid",
      s"${sourceTag}_invalid"
    )
    val alertQuery = writeFileSink(
      alertsDF,
      s"${AppConfig.STREAM_ALERT_OUTPUT_PATH}/$sourceTag",
      s"${AppConfig.CHECKPOINT_PATH}/${sourceTag}_alerts",
      s"${sourceTag}_alerts"
    )

    logger.info(s"Metrics sink: ${AppConfig.STREAM_SINK}")
    logger.info(s"Invalid rows path: ${AppConfig.STREAM_INVALID_OUTPUT_PATH}/$sourceTag")
    logger.info(s"Alert path: ${AppConfig.STREAM_ALERT_OUTPUT_PATH}/$sourceTag")

    awaitAndStop(metricsQuery, Seq(invalidQuery, alertQuery))
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

  private[module] def buildWindowAlerts(df: DataFrame, windowDuration: String, slideDuration: String): DataFrame = {
    df.groupBy(window(col("event_time"), windowDuration, slideDuration))
      .agg(
        count("*").alias("total_events"),
        sum(when(col("behavior") === Behaviors.BUY, 1).otherwise(0)).alias("buy_events"),
        approx_count_distinct(col("userId")).alias("uv")
      )
      .withColumn(
        "buy_rate",
        when(col("total_events") === 0, lit(0.0))
          .otherwise(col("buy_events").cast("double") / col("total_events").cast("double"))
      )
      .withColumn(
        "alert_reason",
        when(col("total_events") < lit(AppConfig.STREAM_ALERT_MIN_WINDOW_EVENTS), lit("low_window_events"))
          .when(col("buy_rate") < lit(AppConfig.DQ_MIN_BUY_RATE), lit("buy_rate_too_low"))
          .when(col("buy_rate") > lit(AppConfig.DQ_MAX_BUY_RATE), lit("buy_rate_too_high"))
      )
      .filter(col("alert_reason").isNotNull)
      .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("total_events"),
        col("buy_events"),
        col("buy_rate"),
        col("uv"),
        col("alert_reason"),
        current_timestamp().alias("detected_at")
      )
  }

  // --------------- Sink writers ---------------

  private def writeMetricsSink(df: DataFrame, sourceTag: String): StreamingQuery = {
    val checkpoint = s"${AppConfig.CHECKPOINT_PATH}/${sourceTag}_metrics_${AppConfig.STREAM_SINK}"
    AppConfig.STREAM_SINK match {
      case "kafka" =>
        val payloadDF = df.select(
          col("behavior").cast("string").alias("key"),
          to_json(
            struct(
              col("window_start"),
              col("window_end"),
              col("behavior"),
              col("pv"),
              col("uv")
            )
          ).alias("value")
        ).selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

        payloadDF.writeStream
          .format("kafka")
          .outputMode("append")
          .option("kafka.bootstrap.servers", AppConfig.KAFKA_BROKERS)
          .option("topic", AppConfig.KAFKA_OUTPUT_TOPIC)
          .option("checkpointLocation", checkpoint)
          .queryName(s"${sourceTag}_metrics_kafka")
          .trigger(Trigger.ProcessingTime(AppConfig.STREAM_TRIGGER_INTERVAL))
          .start()

      case "mysql" =>
        writeMySQLSink(
          df,
          AppConfig.MYSQL_TABLE_STREAM_METRICS,
          checkpoint,
          s"${sourceTag}_metrics_mysql"
        )

      case _ =>
        writeFileSink(
          df,
          s"$streamingOutputRoot/$sourceTag/metrics",
          checkpoint,
          s"${sourceTag}_metrics_file"
        )
    }
  }

  /**
   * MySQL sink via foreachBatch.
   *
   * Each micro-batch is written to MySQL using JDBC append mode,
   * enabling real-time dashboard queries.
   */
  private def writeMySQLSink(
    df: DataFrame,
    table: String,
    checkpointPath: String,
    queryName: String
  ): StreamingQuery = {
    df.writeStream
      .outputMode("append")
      .option("checkpointLocation", checkpointPath)
      .queryName(queryName)
      .trigger(Trigger.ProcessingTime(AppConfig.STREAM_TRIGGER_INTERVAL))
      .foreachBatch { (batchDF: DataFrame, batchId: Long) =>
        if (!batchDF.isEmpty) {
          batchDF.write
            .mode(SaveMode.Append)
            .format("jdbc")
            .option("url", AppConfig.MYSQL_URL)
            .option("dbtable", table)
            .option("user", AppConfig.MYSQL_USER)
            .option("password", AppConfig.MYSQL_PASSWORD)
            .option("batchsize", AppConfig.MYSQL_BATCH_SIZE)
            .option("numPartitions", AppConfig.MYSQL_PARTITIONS)
            .save()
          logger.info(s"Batch $batchId: ${batchDF.count()} rows written to MySQL table '$table'.")
        }
      }
      .start()
  }

  private def writeFileSink(df: DataFrame, outputPath: String, checkpointPath: String, queryName: String): StreamingQuery = {
    df.writeStream
      .format(AppConfig.DATA_FORMAT)
      .outputMode("append")
      .option("path", outputPath)
      .option("checkpointLocation", checkpointPath)
      .queryName(queryName)
      .trigger(Trigger.ProcessingTime(AppConfig.STREAM_TRIGGER_INTERVAL))
      .start()
  }

  private def awaitAndStop(primaryQuery: StreamingQuery, sideQueries: Seq[StreamingQuery]): Unit = {
    val allQueries = primaryQuery +: sideQueries
    try {
      if (AppConfig.STREAM_TIMEOUT > 0L) {
        primaryQuery.awaitTermination(AppConfig.STREAM_TIMEOUT)
        logger.info(s"Streaming timeout reached: ${AppConfig.STREAM_TIMEOUT} ms")
      } else {
        primaryQuery.awaitTermination()
      }
    } finally {
      allQueries.foreach { query =>
        if (query.isActive) query.stop()
      }
    }
  }
}
