package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import com.ecommerce.util.{DataQualityGuard, Logging}
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

/**
 * Module 2: Spark SQL analytics.
 */
object Module2_SparkSQL extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("2")}")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()
    val rawDF = DataQualityGuard.loadValidatedBatchEvents(
      spark,
      AppConfig.RAW_LOG_PATH,
      sourceTag = "module2_sql"
    )

    logger.info(s"Total rows: ${rawDF.count()}")

    val funnelDF = buildFunnelReport(rawDF)
    logger.info("Category conversion funnel:")
    funnelDF.show(20, truncate = false)

    val hourTrendDF = buildHourTrend(rawDF)
    logger.info(s"Hourly ${Behaviors.VIEW.toUpperCase}/${"uv".toUpperCase} trend:")
    hourTrendDF.show(24, truncate = false)

    val top3DF = buildTop3ItemsByCategory(rawDF)
    logger.info(s"Top 3 items per category by '${Behaviors.BUY}' count:")
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
      logger.info("Funnel report written to MySQL")
    } catch {
      case e: Exception =>
        logger.warn(s"MySQL write skipped: ${e.getMessage}")
    }

    logger.info("Module 2 completed")
  }

  private[module] def buildFunnelReport(rawDF: DataFrame): DataFrame = {
    val spark = rawDF.sparkSession
    rawDF.createOrReplaceTempView("user_behavior")

    val funnelSQL =
      s"""
         |SELECT
         |  category,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS pv_cnt,
         |  COUNT(CASE WHEN behavior = '${Behaviors.CART}' THEN 1 END) AS cart_cnt,
         |  COUNT(CASE WHEN behavior = '${Behaviors.BUY}' THEN 1 END) AS buy_cnt,
         |  ROUND(
         |    COUNT(CASE WHEN behavior = '${Behaviors.BUY}' THEN 1 END) * 100.0
         |    / NULLIF(COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END), 0),
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
      .filter(col("behavior") === Behaviors.VIEW)
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
      s"""
         |SELECT category, itemId, buy_cnt, rk
         |FROM (
         |  SELECT
         |    category,
         |    itemId,
         |    COUNT(*) AS buy_cnt,
         |    RANK() OVER (PARTITION BY category ORDER BY COUNT(*) DESC) AS rk
         |  FROM user_behavior
         |  WHERE behavior = '${Behaviors.BUY}'
         |  GROUP BY category, itemId
         |) ranked
         |WHERE rk <= 3
         |ORDER BY category, rk, itemId
         |""".stripMargin

    spark.sql(top3SQL)
  }
}
