package com.ecommerce.warehouse

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import com.ecommerce.util.Logging
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions._

/**
 * 数据仓库构建器
 * 
 * 实现完整的数据仓库分层架构：
 * - ODS (Operational Data Store): 操作数据层，原始数据
 * - DWD (Data Warehouse Detail): 明细数据层，清洗后的明细数据
 * - DWS (Data Warehouse Service): 汇总数据层，轻度聚合
 * - ADS (Application Data Service): 应用数据层，面向业务的聚合数据
 */
object DataWarehouseBuilder extends Logging {

  private val warehousePath = s"${AppConfig.OUTPUT_ROOT}/warehouse"
  private val format = AppConfig.DATA_FORMAT

  /**
   * 构建完整的数据仓库
   */
  def buildWarehouse(): Unit = {
    logger.info("=" * 60)
    logger.info("  Building Data Warehouse")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()

    // 1. ODS 层：加载原始数据
    val odsDF = buildODS(spark)
    logger.info(s"ODS layer: ${odsDF.count()} rows")

    // 2. DWD 层：构建明细数据
    val dwdUserBehavior = buildDWDUserBehavior(odsDF)
    val dwdUserProfile = buildDWDUserProfile(odsDF)
    val dwdItemProfile = buildDWDItemProfile(odsDF)
    logger.info(s"DWD layer built: user_behavior, user_profile, item_profile")

    // 3. DWS 层：构建汇总数据
    val dwsUserDaily = buildDWSUserDaily(dwdUserBehavior)
    val dwsItemDaily = buildDWSItemDaily(dwdUserBehavior)
    val dwsCategoryDaily = buildDWSCategoryDaily(dwdUserBehavior)
    logger.info(s"DWS layer built: user_daily, item_daily, category_daily")

    // 4. ADS 层：构建应用数据
    val adsUserMetrics = buildADSUserMetrics(dwsUserDaily, dwdUserProfile)
    val adsItemMetrics = buildADSItemMetrics(dwsItemDaily, dwdItemProfile)
    val adsCategoryMetrics = buildADSCategoryMetrics(dwsCategoryDaily)
    logger.info(s"ADS layer built: user_metrics, item_metrics, category_metrics")

    // 5. 保存所有层级数据
    saveWarehouseLayers(
      odsDF,
      dwdUserBehavior, dwdUserProfile, dwdItemProfile,
      dwsUserDaily, dwsItemDaily, dwsCategoryDaily,
      adsUserMetrics, adsItemMetrics, adsCategoryMetrics
    )

    // 6. 打印数据仓库统计信息
    printWarehouseStats(
      odsDF,
      dwdUserBehavior,
      dwsUserDaily,
      adsUserMetrics
    )

    logger.info("Data Warehouse build completed")
  }

  // ==================== ODS 层 ====================

  /**
   * ODS 层：操作数据层
   * 直接加载原始数据，不做任何转换
   */
  private def buildODS(spark: org.apache.spark.sql.SparkSession): DataFrame = {
    logger.info("Building ODS layer...")
    
    spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.RAW_LOG_PATH)
      .withColumn("load_time", current_timestamp())
      .withColumn("data_date", to_date(from_unixtime(col("timestamp"))))
  }

  // ==================== DWD 层 ====================

  /**
   * DWD 层：用户行为明细表
   * 清洗和标准化用户行为数据
   */
  private def buildDWDUserBehavior(odsDF: DataFrame): DataFrame = {
    logger.info("Building DWD user_behavior...")
    
    odsDF
      .filter(col("userId").isNotNull && col("itemId").isNotNull)
      .filter(col("behavior").isin(Behaviors.VALID_BEHAVIORS.toSeq: _*))
      .withColumn("behavior_time", from_unixtime(col("timestamp")))
      .withColumn("behavior_date", to_date(col("behavior_time")))
      .withColumn("behavior_hour", hour(col("behavior_time")))
      .withColumn("behavior_day_of_week", dayofweek(col("behavior_time")))
      .withColumn("is_weekend", when(col("behavior_day_of_week").isin(1, 7), 1).otherwise(0))
      .withColumn("session_id", concat(col("userId"), lit("_"), date_format(col("behavior_time"), "yyyyMMddHH")))
      .select(
        col("userId").alias("user_id"),
        col("itemId").alias("item_id"),
        col("category"),
        col("behavior"),
        col("timestamp"),
        col("behavior_time"),
        col("behavior_date"),
        col("behavior_hour"),
        col("behavior_day_of_week"),
        col("is_weekend"),
        col("session_id"),
        col("load_time")
      )
  }

  /**
   * DWD 层：用户画像表
   * 构建用户的基本属性和行为特征
   */
  private def buildDWDUserProfile(odsDF: DataFrame): DataFrame = {
    logger.info("Building DWD user_profile...")
    
    odsDF
      .groupBy("userId")
      .agg(
        min("timestamp").alias("first_behavior_time"),
        max("timestamp").alias("last_behavior_time"),
        count("*").alias("total_behaviors"),
        countDistinct("itemId").alias("unique_items"),
        countDistinct("category").alias("unique_categories"),
        sum(when(col("behavior") === Behaviors.VIEW, 1).otherwise(0)).alias("view_count"),
        sum(when(col("behavior") === Behaviors.CART, 1).otherwise(0)).alias("cart_count"),
        sum(when(col("behavior") === Behaviors.BUY, 1).otherwise(0)).alias("buy_count"),
        sum(when(col("behavior") === Behaviors.FAVORITE, 1).otherwise(0)).alias("fav_count")
      )
      .withColumn("user_level", 
        when(col("total_behaviors") >= 100, "高活跃")
        .when(col("total_behaviors") >= 50, "中活跃")
        .when(col("total_behaviors") >= 10, "低活跃")
        .otherwise("新用户"))
      .withColumn("conversion_rate", 
        round(col("buy_count") * 100.0 / expr("nullif(view_count, 0)"), 2))
      .select(
        col("userId").alias("user_id"),
        col("first_behavior_time"),
        col("last_behavior_time"),
        col("total_behaviors"),
        col("unique_items"),
        col("unique_categories"),
        col("view_count"),
        col("cart_count"),
        col("buy_count"),
        col("fav_count"),
        col("user_level"),
        col("conversion_rate")
      )
  }

  /**
   * DWD 层：商品画像表
   * 构建商品的基本属性和热度特征
   */
  private def buildDWDItemProfile(odsDF: DataFrame): DataFrame = {
    logger.info("Building DWD item_profile...")
    
    odsDF
      .groupBy("itemId", "category")
      .agg(
        count("*").alias("total_interactions"),
        countDistinct("userId").alias("unique_users"),
        sum(when(col("behavior") === Behaviors.VIEW, 1).otherwise(0)).alias("view_count"),
        sum(when(col("behavior") === Behaviors.CART, 1).otherwise(0)).alias("cart_count"),
        sum(when(col("behavior") === Behaviors.BUY, 1).otherwise(0)).alias("buy_count"),
        sum(when(col("behavior") === Behaviors.FAVORITE, 1).otherwise(0)).alias("fav_count")
      )
      .withColumn("item_popularity", 
        when(col("unique_users") >= 100, "爆款")
        .when(col("unique_users") >= 50, "热门")
        .when(col("unique_users") >= 10, "普通")
        .otherwise("冷门"))
      .withColumn("conversion_rate", 
        round(col("buy_count") * 100.0 / expr("nullif(view_count, 0)"), 2))
      .select(
        col("itemId").alias("item_id"),
        col("category"),
        col("total_interactions"),
        col("unique_users"),
        col("view_count"),
        col("cart_count"),
        col("buy_count"),
        col("fav_count"),
        col("item_popularity"),
        col("conversion_rate")
      )
  }

  // ==================== DWS 层 ====================

  /**
   * DWS 层：用户日汇总表
   * 按用户和日期汇总行为数据
   */
  private def buildDWSUserDaily(dwdDF: DataFrame): DataFrame = {
    logger.info("Building DWS user_daily...")
    
    dwdDF
      .groupBy("user_id", "behavior_date")
      .agg(
        count("*").alias("total_behaviors"),
        countDistinct("item_id").alias("unique_items"),
        countDistinct("category").alias("unique_categories"),
        countDistinct("session_id").alias("session_count"),
        sum(when(col("behavior") === Behaviors.VIEW, 1).otherwise(0)).alias("view_count"),
        sum(when(col("behavior") === Behaviors.CART, 1).otherwise(0)).alias("cart_count"),
        sum(when(col("behavior") === Behaviors.BUY, 1).otherwise(0)).alias("buy_count"),
        sum(when(col("behavior") === Behaviors.FAVORITE, 1).otherwise(0)).alias("fav_count"),
        min("behavior_hour").alias("first_active_hour"),
        max("behavior_hour").alias("last_active_hour")
      )
      .withColumn("active_hours", col("last_active_hour") - col("first_active_hour") + 1)
      .withColumn("conversion_rate", 
        round(col("buy_count") * 100.0 / expr("nullif(view_count, 0)"), 2))
  }

  /**
   * DWS 层：商品日汇总表
   * 按商品和日期汇总行为数据
   */
  private def buildDWSItemDaily(dwdDF: DataFrame): DataFrame = {
    logger.info("Building DWS item_daily...")
    
    dwdDF
      .groupBy("item_id", "category", "behavior_date")
      .agg(
        count("*").alias("total_interactions"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("session_id").alias("session_count"),
        sum(when(col("behavior") === Behaviors.VIEW, 1).otherwise(0)).alias("view_count"),
        sum(when(col("behavior") === Behaviors.CART, 1).otherwise(0)).alias("cart_count"),
        sum(when(col("behavior") === Behaviors.BUY, 1).otherwise(0)).alias("buy_count"),
        sum(when(col("behavior") === Behaviors.FAVORITE, 1).otherwise(0)).alias("fav_count")
      )
      .withColumn("conversion_rate", 
        round(col("buy_count") * 100.0 / expr("nullif(view_count, 0)"), 2))
  }

  /**
   * DWS 层：类目日汇总表
   * 按类目和日期汇总行为数据
   */
  private def buildDWSCategoryDaily(dwdDF: DataFrame): DataFrame = {
    logger.info("Building DWS category_daily...")
    
    dwdDF
      .groupBy("category", "behavior_date")
      .agg(
        count("*").alias("total_interactions"),
        countDistinct("user_id").alias("unique_users"),
        countDistinct("item_id").alias("unique_items"),
        countDistinct("session_id").alias("session_count"),
        sum(when(col("behavior") === Behaviors.VIEW, 1).otherwise(0)).alias("view_count"),
        sum(when(col("behavior") === Behaviors.CART, 1).otherwise(0)).alias("cart_count"),
        sum(when(col("behavior") === Behaviors.BUY, 1).otherwise(0)).alias("buy_count"),
        sum(when(col("behavior") === Behaviors.FAVORITE, 1).otherwise(0)).alias("fav_count")
      )
      .withColumn("conversion_rate", 
        round(col("buy_count") * 100.0 / expr("nullif(view_count, 0)"), 2))
      .withColumn("avg_interactions_per_user", 
        round(col("total_interactions") / expr("nullif(unique_users, 0)"), 2))
  }

  // ==================== ADS 层 ====================

  /**
   * ADS 层：用户指标表
   * 面向业务的用户分析指标
   */
  private def buildADSUserMetrics(dwsUserDaily: DataFrame, dwdUserProfile: DataFrame): DataFrame = {
    logger.info("Building ADS user_metrics...")
    
    val userDailySummary = dwsUserDaily
      .groupBy("user_id")
      .agg(
        count("*").alias("active_days"),
        sum("total_behaviors").alias("total_behaviors"),
        sum("view_count").alias("total_views"),
        sum("cart_count").alias("total_carts"),
        sum("buy_count").alias("total_buys"),
        avg("conversion_rate").alias("avg_conversion_rate"),
        max("behavior_date").alias("last_active_date")
      )
    
    dwdUserProfile
      .join(userDailySummary, Seq("user_id"), "left")
      .na.fill(0, Seq("active_days", "total_behaviors", "total_views", "total_carts", "total_buys"))
      .na.fill(0.0, Seq("avg_conversion_rate"))
      .withColumn("avg_behaviors_per_day", 
        round(col("total_behaviors") / expr("nullif(active_days, 0)"), 2))
      .withColumn("user_value_score", 
        round((col("total_buys") * 10 + col("total_carts") * 5 + col("total_views")) / 100.0, 2))
      .select(
        col("user_id"),
        col("user_level"),
        col("active_days"),
        col("total_behaviors"),
        col("total_views"),
        col("total_carts"),
        col("total_buys"),
        col("avg_conversion_rate"),
        col("avg_behaviors_per_day"),
        col("user_value_score"),
        col("last_active_date")
      )
  }

  /**
   * ADS 层：商品指标表
   * 面向业务的商品分析指标
   */
  private def buildADSItemMetrics(dwsItemDaily: DataFrame, dwdItemProfile: DataFrame): DataFrame = {
    logger.info("Building ADS item_metrics...")
    
    val itemDailySummary = dwsItemDaily
      .groupBy("item_id", "category")
      .agg(
        count("*").alias("active_days"),
        sum("total_interactions").alias("total_interactions"),
        sum("unique_users").alias("total_users"),
        sum("view_count").alias("total_views"),
        sum("cart_count").alias("total_carts"),
        sum("buy_count").alias("total_buys"),
        avg("conversion_rate").alias("avg_conversion_rate")
      )
    
    dwdItemProfile
      .join(itemDailySummary, Seq("item_id", "category"), "left")
      .na.fill(0, Seq("active_days", "total_interactions", "total_users", "total_views", "total_carts", "total_buys"))
      .na.fill(0.0, Seq("avg_conversion_rate"))
      .withColumn("avg_interactions_per_day", 
        round(col("total_interactions") / expr("nullif(active_days, 0)"), 2))
      .withColumn("item_heat_score", 
        round((col("total_buys") * 10 + col("total_carts") * 5 + col("total_views")) / 100.0, 2))
      .select(
        col("item_id"),
        col("category"),
        col("item_popularity"),
        col("active_days"),
        col("total_interactions"),
        col("total_users"),
        col("total_views"),
        col("total_carts"),
        col("total_buys"),
        col("avg_conversion_rate"),
        col("avg_interactions_per_day"),
        col("item_heat_score")
      )
  }

  /**
   * ADS 层：类目指标表
   * 面向业务的类目分析指标
   */
  private def buildADSCategoryMetrics(dwsCategoryDaily: DataFrame): DataFrame = {
    logger.info("Building ADS category_metrics...")
    
    dwsCategoryDaily
      .groupBy("category")
      .agg(
        count("*").alias("active_days"),
        sum("total_interactions").alias("total_interactions"),
        sum("unique_users").alias("total_users"),
        sum("unique_items").alias("total_items"),
        sum("view_count").alias("total_views"),
        sum("cart_count").alias("total_carts"),
        sum("buy_count").alias("total_buys"),
        avg("conversion_rate").alias("avg_conversion_rate"),
        avg("avg_interactions_per_user").alias("avg_interactions_per_user")
      )
      .withColumn("category_rank", 
        row_number().over(org.apache.spark.sql.expressions.Window.orderBy(col("total_buys").desc)))
      .withColumn("market_share", 
        round(col("total_buys") * 100.0 / sum("total_buys").over(), 2))
      .orderBy("category_rank")
  }

  // ==================== 保存和统计 ====================

  /**
   * 保存所有数据仓库层级
   */
  private def saveWarehouseLayers(
    odsDF: DataFrame,
    dwdUserBehavior: DataFrame,
    dwdUserProfile: DataFrame,
    dwdItemProfile: DataFrame,
    dwsUserDaily: DataFrame,
    dwsItemDaily: DataFrame,
    dwsCategoryDaily: DataFrame,
    adsUserMetrics: DataFrame,
    adsItemMetrics: DataFrame,
    adsCategoryMetrics: DataFrame
  ): Unit = {
    logger.info(s"Saving warehouse layers in $format format...")

    // ODS 层
    odsDF.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/ods/user_behavior")

    // DWD 层
    dwdUserBehavior.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/dwd/user_behavior")
    dwdUserProfile.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/dwd/user_profile")
    dwdItemProfile.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/dwd/item_profile")

    // DWS 层
    dwsUserDaily.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/dws/user_daily")
    dwsItemDaily.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/dws/item_daily")
    dwsCategoryDaily.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/dws/category_daily")

    // ADS 层
    adsUserMetrics.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/ads/user_metrics")
    adsItemMetrics.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/ads/item_metrics")
    adsCategoryMetrics.write.mode(SaveMode.Overwrite).format(format).save(s"$warehousePath/ads/category_metrics")

    logger.info("All warehouse layers saved successfully")
  }

  /**
   * 打印数据仓库统计信息
   */
  private def printWarehouseStats(
    odsDF: DataFrame,
    dwdUserBehavior: DataFrame,
    dwsUserDaily: DataFrame,
    adsUserMetrics: DataFrame
  ): Unit = {
    logger.info("\n" + "=" * 80)
    logger.info("  Data Warehouse Statistics")
    logger.info("=" * 80)

    logger.info(s"ODS Layer:")
    logger.info(s"  - Total rows: ${odsDF.count()}")

    logger.info(s"\nDWD Layer:")
    logger.info(s"  - User behaviors: ${dwdUserBehavior.count()}")

    logger.info(s"\nDWS Layer:")
    logger.info(s"  - User daily records: ${dwsUserDaily.count()}")

    logger.info(s"\nADS Layer:")
    logger.info(s"  - User metrics: ${adsUserMetrics.count()}")

    logger.info("\nTop 10 Users by Value Score:")
    adsUserMetrics
      .orderBy(col("user_value_score").desc)
      .select("user_id", "user_level", "total_buys", "user_value_score")
      .show(10, truncate = false)

    logger.info("=" * 80)
  }
}
