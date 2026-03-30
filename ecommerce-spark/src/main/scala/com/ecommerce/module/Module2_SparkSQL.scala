package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.util.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

/**
 * ══════════════════════════════════════════════════════════
 * 模块二：业务报表（Spark SQL）
 * 对应课程：第3章 Spark进阶
 *   - Spark SQL / SparkSession
 *   - DataFrame 聚合查询
 *   - 临时视图（createOrReplaceTempView）
 *   - 结果导出 MySQL / Parquet
 * ══════════════════════════════════════════════════════════
 */
object Module2_SparkSQL extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info("  模块二：Spark SQL 业务报表")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    // ── Step 1：读取数据 → DataFrame ──
    val rawDF: DataFrame = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.RAW_LOG_PATH)

    rawDF.printSchema()
    logger.info(s"总记录数: ${rawDF.count()}")

    // ── Step 2：注册临时视图 ──
    rawDF.createOrReplaceTempView("user_behavior")

    // ── Step 3：SQL 查询 —— 类目转化漏斗 ──
    logger.info("类目转化漏斗（SQL风格）：")
    val funnelSQL =
      """
        |SELECT
        |  category,
        |  COUNT(CASE WHEN behavior='pv'   THEN 1 END) AS pv_cnt,
        |  COUNT(CASE WHEN behavior='cart' THEN 1 END) AS cart_cnt,
        |  COUNT(CASE WHEN behavior='buy'  THEN 1 END) AS buy_cnt,
        |  ROUND(COUNT(CASE WHEN behavior='buy' THEN 1 END) * 100.0
        |        / NULLIF(COUNT(CASE WHEN behavior='pv' THEN 1 END), 0), 2) AS conv_rate_pct
        |FROM user_behavior
        |GROUP BY category
        |ORDER BY buy_cnt DESC
      """.stripMargin

    val funnelDF = spark.sql(funnelSQL)
    funnelDF.show(20)

    // ── Step 4：DataFrame API —— 小时级 PV/UV 趋势 ──
    logger.info("24小时 PV/UV 趋势（DataFrame API风格）：")
    val hourTrendDF = rawDF
      .withColumn("hour", hour(from_unixtime(col("timestamp"))))
      .filter(col("behavior") === "pv")
      .groupBy("hour")
      .agg(
        count("*").alias("pv"),
        countDistinct("userId").alias("uv")
      )
      .orderBy("hour")

    hourTrendDF.show(24)

    // ── Step 5：窗口函数 —— 每类目 Top3 商品 ──
    logger.info("各类目 Top3 热销商品（窗口函数）：")
    rawDF.createOrReplaceTempView("user_behavior")
    val top3SQL =
      """
        |SELECT category, itemId, buy_cnt, rk
        |FROM (
        |  SELECT category, itemId,
        |         COUNT(*) AS buy_cnt,
        |         RANK() OVER (PARTITION BY category ORDER BY COUNT(*) DESC) AS rk
        |  FROM user_behavior
        |  WHERE behavior = 'buy'
        |  GROUP BY category, itemId
        |) t
        |WHERE rk <= 3
      """.stripMargin
    spark.sql(top3SQL).show(30)

    // ── Step 6：结果写出 ──
    // 写出到 Parquet（高效列式存储）
    funnelDF.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${AppConfig.SQL_OUTPUT_PATH}/funnel_report")

    // 写出到 MySQL（需要配置连接信息）
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
      logger.info("报表已写入 MySQL ✓")
    } catch {
      case e: Exception =>
        logger.warn(s"MySQL写出跳过（未配置连接）: ${e.getMessage}")
    }

    logger.info("模块二执行完毕 ✓")
  }
}
