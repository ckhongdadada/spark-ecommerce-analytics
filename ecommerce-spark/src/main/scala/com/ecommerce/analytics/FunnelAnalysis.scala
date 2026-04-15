package com.ecommerce.analytics

import com.ecommerce.config.AppConfig
import com.ecommerce.core.Behaviors
import com.ecommerce.util.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * 用户行为漏斗分析组件
 * 
 * 实现完整的"曝光 -> 点击 -> 加购 -> 支付"四步转化率分析
 * 提供多维度漏斗分析：
 * 1. 全局漏斗：整体转化情况
 * 2. 分类漏斗：按商品类目分析
 * 3. 用户漏斗：按用户群体分析
 * 4. 时段漏斗：按时间段分析
 */
object FunnelAnalysis extends Logging {

  /**
   * 全局漏斗分析
   * 计算整体的四步转化率和流失率
   */
  def buildGlobalFunnel(rawDF: DataFrame): DataFrame = {
    logger.info("Building global funnel analysis...")
    
    val spark = rawDF.sparkSession
    rawDF.createOrReplaceTempView("user_behavior")

    val funnelSQL =
      s"""
         |SELECT
         |  'Global' AS dimension,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.VIEW}' THEN userId END) AS step1_exposure_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.VIEW}' THEN userId END) AS step2_click_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.CART}' THEN userId END) AS step3_cart_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.BUY}' THEN userId END) AS step4_buy_users,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS step1_exposure_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS step2_click_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.CART}' THEN 1 END) AS step3_cart_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.BUY}' THEN 1 END) AS step4_buy_events
         |FROM user_behavior
         |""".stripMargin

    val baseDF = spark.sql(funnelSQL)
    
    // 计算转化率和流失率
    baseDF
      .withColumn("step1_to_step2_rate", 
        round(col("step2_click_users") * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
      .withColumn("step2_to_step3_rate", 
        round(col("step3_cart_users") * 100.0 / nullif(col("step2_click_users"), 0), 2))
      .withColumn("step3_to_step4_rate", 
        round(col("step4_buy_users") * 100.0 / nullif(col("step3_cart_users"), 0), 2))
      .withColumn("overall_conversion_rate", 
        round(col("step4_buy_users") * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
      .withColumn("step1_to_step2_loss_rate", 
        round((col("step1_exposure_users") - col("step2_click_users")) * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
      .withColumn("step2_to_step3_loss_rate", 
        round((col("step2_click_users") - col("step3_cart_users")) * 100.0 / nullif(col("step2_click_users"), 0), 2))
      .withColumn("step3_to_step4_loss_rate", 
        round((col("step3_cart_users") - col("step4_buy_users")) * 100.0 / nullif(col("step3_cart_users"), 0), 2))
  }

  /**
   * 分类漏斗分析
   * 按商品类目分析各类目的转化情况
   */
  def buildCategoryFunnel(rawDF: DataFrame): DataFrame = {
    logger.info("Building category funnel analysis...")
    
    val spark = rawDF.sparkSession
    rawDF.createOrReplaceTempView("user_behavior")

    val funnelSQL =
      s"""
         |SELECT
         |  category AS dimension,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.VIEW}' THEN userId END) AS step1_exposure_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.VIEW}' THEN userId END) AS step2_click_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.CART}' THEN userId END) AS step3_cart_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.BUY}' THEN userId END) AS step4_buy_users,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS step1_exposure_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS step2_click_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.CART}' THEN 1 END) AS step3_cart_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.BUY}' THEN 1 END) AS step4_buy_events
         |FROM user_behavior
         |GROUP BY category
         |ORDER BY step4_buy_users DESC
         |""".stripMargin

    val baseDF = spark.sql(funnelSQL)
    
    baseDF
      .withColumn("step1_to_step2_rate", 
        round(col("step2_click_users") * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
      .withColumn("step2_to_step3_rate", 
        round(col("step3_cart_users") * 100.0 / nullif(col("step2_click_users"), 0), 2))
      .withColumn("step3_to_step4_rate", 
        round(col("step4_buy_users") * 100.0 / nullif(col("step3_cart_users"), 0), 2))
      .withColumn("overall_conversion_rate", 
        round(col("step4_buy_users") * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
      .withColumn("step1_to_step2_loss_rate", 
        round((col("step1_exposure_users") - col("step2_click_users")) * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
      .withColumn("step2_to_step3_loss_rate", 
        round((col("step2_click_users") - col("step3_cart_users")) * 100.0 / nullif(col("step2_click_users"), 0), 2))
      .withColumn("step3_to_step4_loss_rate", 
        round((col("step3_cart_users") - col("step4_buy_users")) * 100.0 / nullif(col("step3_cart_users"), 0), 2))
  }

  /**
   * 时段漏斗分析
   * 按小时分析不同时段的转化情况
   */
  def buildHourlyFunnel(rawDF: DataFrame): DataFrame = {
    logger.info("Building hourly funnel analysis...")
    
    val dfWithHour = rawDF.withColumn("hour", hour(from_unixtime(col("timestamp"))))
    
    val spark = dfWithHour.sparkSession
    dfWithHour.createOrReplaceTempView("user_behavior_hourly")

    val funnelSQL =
      s"""
         |SELECT
         |  CAST(hour AS STRING) AS dimension,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.VIEW}' THEN userId END) AS step1_exposure_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.VIEW}' THEN userId END) AS step2_click_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.CART}' THEN userId END) AS step3_cart_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.BUY}' THEN userId END) AS step4_buy_users,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS step1_exposure_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS step2_click_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.CART}' THEN 1 END) AS step3_cart_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.BUY}' THEN 1 END) AS step4_buy_events
         |FROM user_behavior_hourly
         |GROUP BY hour
         |ORDER BY hour
         |""".stripMargin

    val baseDF = spark.sql(funnelSQL)
    
    baseDF
      .withColumn("step1_to_step2_rate", 
        round(col("step2_click_users") * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
      .withColumn("step2_to_step3_rate", 
        round(col("step3_cart_users") * 100.0 / nullif(col("step2_click_users"), 0), 2))
      .withColumn("step3_to_step4_rate", 
        round(col("step4_buy_users") * 100.0 / nullif(col("step3_cart_users"), 0), 2))
      .withColumn("overall_conversion_rate", 
        round(col("step4_buy_users") * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
  }

  /**
   * 用户分群漏斗分析
   * 按用户活跃度分析不同用户群的转化情况
   */
  def buildUserSegmentFunnel(rawDF: DataFrame): DataFrame = {
    logger.info("Building user segment funnel analysis...")
    
    val spark = rawDF.sparkSession
    
    // 计算用户活跃度（按行为次数分群）
    val userActivityDF = rawDF
      .groupBy("userId")
      .agg(count("*").alias("activity_count"))
      .withColumn("user_segment", 
        when(col("activity_count") >= 50, "高活跃用户")
        .when(col("activity_count") >= 20, "中活跃用户")
        .when(col("activity_count") >= 5, "低活跃用户")
        .otherwise("新用户"))
    
    val enrichedDF = rawDF
      .join(userActivityDF, Seq("userId"), "left")
    
    enrichedDF.createOrReplaceTempView("user_behavior_segmented")

    val funnelSQL =
      s"""
         |SELECT
         |  user_segment AS dimension,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.VIEW}' THEN userId END) AS step1_exposure_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.VIEW}' THEN userId END) AS step2_click_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.CART}' THEN userId END) AS step3_cart_users,
         |  COUNT(DISTINCT CASE WHEN behavior = '${Behaviors.BUY}' THEN userId END) AS step4_buy_users,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS step1_exposure_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.VIEW}' THEN 1 END) AS step2_click_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.CART}' THEN 1 END) AS step3_cart_events,
         |  COUNT(CASE WHEN behavior = '${Behaviors.BUY}' THEN 1 END) AS step4_buy_events
         |FROM user_behavior_segmented
         |GROUP BY user_segment
         |ORDER BY 
         |  CASE user_segment
         |    WHEN '高活跃用户' THEN 1
         |    WHEN '中活跃用户' THEN 2
         |    WHEN '低活跃用户' THEN 3
         |    WHEN '新用户' THEN 4
         |  END
         |""".stripMargin

    val baseDF = spark.sql(funnelSQL)
    
    baseDF
      .withColumn("step1_to_step2_rate", 
        round(col("step2_click_users") * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
      .withColumn("step2_to_step3_rate", 
        round(col("step3_cart_users") * 100.0 / nullif(col("step2_click_users"), 0), 2))
      .withColumn("step3_to_step4_rate", 
        round(col("step4_buy_users") * 100.0 / nullif(col("step3_cart_users"), 0), 2))
      .withColumn("overall_conversion_rate", 
        round(col("step4_buy_users") * 100.0 / nullif(col("step1_exposure_users"), 0), 2))
  }

  /**
   * 生成漏斗可视化数据
   * 输出适合前端漏斗图展示的格式
   */
  def generateFunnelVisualizationData(funnelDF: DataFrame): DataFrame = {
    logger.info("Generating funnel visualization data...")
    
    funnelDF
      .select(
        col("dimension"),
        col("step1_exposure_users").alias("曝光用户数"),
        col("step2_click_users").alias("点击用户数"),
        col("step3_cart_users").alias("加购用户数"),
        col("step4_buy_users").alias("支付用户数"),
        col("step1_to_step2_rate").alias("曝光到点击转化率(%)"),
        col("step2_to_step3_rate").alias("点击到加购转化率(%)"),
        col("step3_to_step4_rate").alias("加购到支付转化率(%)"),
        col("overall_conversion_rate").alias("整体转化率(%)")
      )
  }

  /**
   * 流失分析
   * 识别每个环节的流失用户和流失原因
   */
  def buildLossAnalysis(rawDF: DataFrame): DataFrame = {
    logger.info("Building loss analysis...")
    
    val spark = rawDF.sparkSession
    rawDF.createOrReplaceTempView("user_behavior")

    val lossSQL =
      s"""
         |SELECT
         |  'Step1->Step2: 曝光未点击' AS loss_stage,
         |  COUNT(DISTINCT u1.userId) AS loss_users,
         |  ROUND(COUNT(DISTINCT u1.userId) * 100.0 / 
         |    NULLIF((SELECT COUNT(DISTINCT userId) FROM user_behavior WHERE behavior = '${Behaviors.VIEW}'), 0), 2) AS loss_rate
         |FROM (
         |  SELECT DISTINCT userId FROM user_behavior WHERE behavior = '${Behaviors.VIEW}'
         |) u1
         |LEFT JOIN (
         |  SELECT DISTINCT userId FROM user_behavior WHERE behavior = '${Behaviors.VIEW}'
         |) u2 ON u1.userId = u2.userId
         |WHERE u2.userId IS NULL
         |
         |UNION ALL
         |
         |SELECT
         |  'Step2->Step3: 点击未加购' AS loss_stage,
         |  COUNT(DISTINCT u1.userId) AS loss_users,
         |  ROUND(COUNT(DISTINCT u1.userId) * 100.0 / 
         |    NULLIF((SELECT COUNT(DISTINCT userId) FROM user_behavior WHERE behavior = '${Behaviors.VIEW}'), 0), 2) AS loss_rate
         |FROM (
         |  SELECT DISTINCT userId FROM user_behavior WHERE behavior = '${Behaviors.VIEW}'
         |) u1
         |LEFT JOIN (
         |  SELECT DISTINCT userId FROM user_behavior WHERE behavior = '${Behaviors.CART}'
         |) u2 ON u1.userId = u2.userId
         |WHERE u2.userId IS NULL
         |
         |UNION ALL
         |
         |SELECT
         |  'Step3->Step4: 加购未支付' AS loss_stage,
         |  COUNT(DISTINCT u1.userId) AS loss_users,
         |  ROUND(COUNT(DISTINCT u1.userId) * 100.0 / 
         |    NULLIF((SELECT COUNT(DISTINCT userId) FROM user_behavior WHERE behavior = '${Behaviors.CART}'), 0), 2) AS loss_rate
         |FROM (
         |  SELECT DISTINCT userId FROM user_behavior WHERE behavior = '${Behaviors.CART}'
         |) u1
         |LEFT JOIN (
         |  SELECT DISTINCT userId FROM user_behavior WHERE behavior = '${Behaviors.BUY}'
         |) u2 ON u1.userId = u2.userId
         |WHERE u2.userId IS NULL
         |
         |ORDER BY loss_users DESC
         |""".stripMargin

    spark.sql(lossSQL)
  }

  /**
   * 保存漏斗分析结果
   */
  def saveFunnelReports(
    globalFunnel: DataFrame,
    categoryFunnel: DataFrame,
    hourlyFunnel: DataFrame,
    userSegmentFunnel: DataFrame,
    lossAnalysis: DataFrame,
    outputPath: String,
    format: String = "parquet"
  ): Unit = {
    logger.info(s"Saving funnel analysis reports to $outputPath in $format format...")
    
    globalFunnel.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(s"$outputPath/funnel_global")
    
    categoryFunnel.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(s"$outputPath/funnel_category")
    
    hourlyFunnel.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(s"$outputPath/funnel_hourly")
    
    userSegmentFunnel.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(s"$outputPath/funnel_user_segment")
    
    lossAnalysis.write
      .mode(SaveMode.Overwrite)
      .format(format)
      .save(s"$outputPath/funnel_loss_analysis")
    
    logger.info("Funnel analysis reports saved successfully")
  }

  /**
   * 保存到 MySQL
   */
  def saveFunnelReportsToMySQL(
    globalFunnel: DataFrame,
    categoryFunnel: DataFrame,
    hourlyFunnel: DataFrame,
    userSegmentFunnel: DataFrame,
    lossAnalysis: DataFrame
  ): Unit = {
    logger.info("Saving funnel analysis reports to MySQL...")
    
    try {
      writeToMySQL(globalFunnel, "funnel_global")
      writeToMySQL(categoryFunnel, "funnel_category")
      writeToMySQL(hourlyFunnel, "funnel_hourly")
      writeToMySQL(userSegmentFunnel, "funnel_user_segment")
      writeToMySQL(lossAnalysis, "funnel_loss_analysis")
      logger.info("All funnel reports written to MySQL successfully")
    } catch {
      case e: Exception =>
        logger.warn(s"MySQL write for funnel reports failed: ${e.getMessage}")
    }
  }

  private def writeToMySQL(df: DataFrame, table: String): Unit = {
    df.write
      .mode(SaveMode.Overwrite)
      .format("jdbc")
      .option("url", AppConfig.MYSQL_URL)
      .option("dbtable", table)
      .option("user", AppConfig.MYSQL_USER)
      .option("password", AppConfig.MYSQL_PASSWORD)
      .option("batchsize", AppConfig.MYSQL_BATCH_SIZE)
      .option("numPartitions", AppConfig.MYSQL_PARTITIONS)
      .option("truncate", "true")
      .save()
    logger.info(s"Funnel table '$table' written to MySQL")
  }
}
