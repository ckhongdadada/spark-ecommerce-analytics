package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.util.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

/**
 * ══════════════════════════════════════════════════════════
 * 模块三：实时监控（Spark Streaming / Structured Streaming）
 * 对应课程：第3章 Spark进阶
 *   - 基于Spark Streaming的流式计算框架
 *   - DStream / Structured Streaming
 *   - 滑动窗口计算
 *   - UV / PV 实时统计
 *
 * 本模块提供两种实现：
 *   A. Structured Streaming（推荐，基于 DataFrame API）
 *   B. Socket 接入（本地调试用，不依赖 Kafka）
 * ══════════════════════════════════════════════════════════
 */
object Module3_Streaming extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info("  模块三：Structured Streaming 实时监控")
    logger.info("=" * 60)

    // 根据配置决定数据源：优先 Kafka，降级 Socket
    val useKafka = System.getProperty("streaming.source", "socket") == "kafka"
    if (useKafka) runWithKafka() else runWithSocket()
  }

  // ─────────────── A. Kafka 数据源（生产部署推荐）───────────────
  def runWithKafka(): Unit = {
    logger.info("使用 Kafka 数据源")
    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    val rawStream = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", AppConfig.KAFKA_BROKERS)
      .option("subscribe", AppConfig.KAFKA_TOPIC)
      .option("startingOffsets", "latest")
      .load()

    // 解析 JSON 消息：{"userId":"U1","itemId":"I1","behavior":"pv","timestamp":1700000000}
    val schema = StructType(Seq(
      StructField("userId", StringType, nullable = false),
      StructField("itemId", StringType, nullable = false),
      StructField("behavior", StringType, nullable = false),
      StructField("timestamp", LongType, nullable = false)
    ))
    val eventDF = rawStream
      .selectExpr("CAST(value AS STRING) as json_str")
      .select(from_json(col("json_str"), schema).alias("data"))
      .select("data.*")
      .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))

    // 滑动窗口 PV/UV 统计
    val windowedDF = eventDF
      .withWatermark("event_time", "10 seconds")
      .groupBy(
        window(col("event_time"),
               s"${AppConfig.STREAM_WINDOW} seconds",
               s"${AppConfig.STREAM_SLIDE} seconds"),
        col("behavior")
      )
      .agg(
        count("*").alias("pv"),
        approx_count_distinct(col("userId")).alias("uv")
      )

    val query = windowedDF.writeStream
      .outputMode("update")
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", s"${AppConfig.CHECKPOINT_PATH}/kafka")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .start()

    logger.info("Streaming 任务已启动，等待数据... (Ctrl+C 停止)")
    query.awaitTermination(AppConfig.STREAM_TIMEOUT)
    if (query.isActive) query.stop()
  }

  // ─────────────── B. Socket 数据源（本地调试）───────────────
  // 启动 nc -lk 9999，手动输入: userId,itemId,category,behavior,timestamp
  def runWithSocket(): Unit = {
    logger.info(s"使用 Socket 数据源 ${AppConfig.SOCKET_HOST}:${AppConfig.SOCKET_PORT}")
    logger.info("请先启动: nc -lk 9999")
    logger.info("输入格式: U1,I1,Electronics,pv,1700000001")

    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", AppConfig.SOCKET_HOST)
      .option("port", AppConfig.SOCKET_PORT)
      .load()

    val eventDF = lines
      .as[String]
      .map(_.split(","))
      .filter(_.length == 5)
      .map(f => (f(0), f(1), f(2), f(3), f(4).toLong))
      .toDF("userId", "itemId", "category", "behavior", "timestamp")
      .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))

    // 实时 PV/UV（追加模式）
    val pvuvQuery = eventDF
      .groupBy(
        window(col("event_time"), "1 minute", "30 seconds"),
        col("behavior")
      )
      .agg(count("*").as("pv"), approx_count_distinct(col("userId")).as("uv"))
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("numRows", 20)
      .option("checkpointLocation", s"${AppConfig.CHECKPOINT_PATH}/socket")
      .start()

    pvuvQuery.awaitTermination(AppConfig.STREAM_TIMEOUT * 2)
    if (pvuvQuery.isActive) pvuvQuery.stop()
    logger.info("模块三执行完毕 ✓")
  }
}
