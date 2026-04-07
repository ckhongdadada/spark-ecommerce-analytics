package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.util.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

/**
 * 模块二：Spark SQL 业务报表
 * 包含转化漏斗、小时级 PV/UV 趋势和每类目热销商品分析。
 */
object Module2_SparkSQL extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info("  模块二：Spark SQL 业务报表")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.RAW_LOG_PATH)

    rawDF.printSchema()
    logger.info(s"总记录数: ${rawDF.count()}")

    val funnelDF = buildFunnelReport(rawDF)
    logger.info("类目转化漏斗:")
    funnelDF.show(20, truncate = false)

    val hourTrendDF = buildHourTrend(rawDF)
    logger.info("24 小时 PV/UV 趋势:")
    hourTrendDF.show(24, truncate = false)

    val top3DF = buildTop3ItemsByCategory(rawDF)
    logger.info("各类目 Top3 热销商品:")
    top3DF.show(30, truncate = false)

    funnelDF.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${AppConfig.SQL_OUTPUT_PATH}/funnel_report")

    try {
      funnelDF.write
        .mode(SaveMode.Overwrite)
        .format("jdbc")
        .option("url", AppConfig.MYSQL_URL)
        .option("dbtable", AppConfig.MYSQL_TABLE)
        .option("user", AppConfig.MYSQL_USER)
        .option("password", AppConfig.MYSQL_PASSWORD)
        .option("batchsize", AppConfig.MYSQL_BATCH_SIZE)
        .option("numPartitions", AppConfig.MYSQL_PARTITIONS)
        .option("truncate", "true")
        .save()
      logger.info("报表已写入 MySQL")
    } catch {
      case e: Exception =>
        logger.warn(s"MySQL 写出跳过（可能未配置连接）: ${e.getMessage}")
    }

    logger.info("模块二执行完毕")
  }

  private[module] def buildFunnelReport(rawDF: DataFrame): DataFrame = {
    val spark = rawDF.sparkSession
    rawDF.createOrReplaceTempView("user_behavior")

    val funnelSQL =
      """
        |SELECT
        |  category,
        |  COUNT(CASE WHEN behavior = 'pv' THEN 1 END) AS pv_cnt,
        |  COUNT(CASE WHEN behavior = 'cart' THEN 1 END) AS cart_cnt,
        |  COUNT(CASE WHEN behavior = 'buy' THEN 1 END) AS buy_cnt,
        |  ROUND(
        |    COUNT(CASE WHEN behavior = 'buy' THEN 1 END) * 100.0
        |    / NULLIF(COUNT(CASE WHEN behavior = 'pv' THEN 1 END), 0),
        |    2
        |  ) AS conv_rate_pct
        |FROM user_behavior
        |GROUP BY category
        |ORDER BY buy_cnt DESC, category ASC
        |""".stripMargin

    spark.sql(funnelSQL)
  }

  private[module] def buildHourTrend(rawDF: DataFrame): DataFrame = {
    rawDF
      .withColumn("hour", hour(from_unixtime(col("timestamp"))))
      .filter(col("behavior") === "pv")
      .groupBy("hour")
      .agg(
        count("*").alias("pv"),
        countDistinct("userId").alias("uv")
      )
      .orderBy("hour")
  }

  private[module] def buildTop3ItemsByCategory(rawDF: DataFrame): DataFrame = {
    val spark = rawDF.sparkSession
    rawDF.createOrReplaceTempView("user_behavior")

    val top3SQL =
      """
        |SELECT category, itemId, buy_cnt, rk
        |FROM (
        |  SELECT
        |    category,
        |    itemId,
        |    COUNT(*) AS buy_cnt,
        |    RANK() OVER (PARTITION BY category ORDER BY COUNT(*) DESC) AS rk
        |  FROM user_behavior
        |  WHERE behavior = 'buy'
        |  GROUP BY category, itemId
        |) ranked
        |WHERE rk <= 3
        |ORDER BY category, rk, itemId
        |""".stripMargin

    spark.sql(top3SQL)
  }
}
