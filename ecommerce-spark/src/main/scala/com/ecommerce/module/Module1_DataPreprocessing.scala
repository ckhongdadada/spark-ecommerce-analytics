package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{SparkSessionFactory, UserBehaviorLog, CleanBehavior}
import com.ecommerce.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * ══════════════════════════════════════════════════════════
 * 模块一：数据预处理（Spark Core / RDD）
 * 对应课程：第2章 Spark开发
 *   - RDD概述
 *   - RDD编程（filter / map / flatMap）
 *   - 键值对操作（PairRDD / reduceByKey / sortByKey）
 *   - 数据读取与保存
 * ══════════════════════════════════════════════════════════
 */
object Module1_DataPreprocessing extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info("  模块一：RDD数据预处理")
    logger.info("=" * 60)

    val sc = SparkSessionFactory.getContext()

    // ── Step 1：读取原始 CSV（textFile → RDD[String]）──
    val rawRDD: RDD[String] = sc.textFile(AppConfig.RAW_LOG_PATH)

    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(_ != header).persist(StorageLevel.MEMORY_ONLY)

    // ── Step 2：解析 & 清洗（map + filter）──
    val parsedRDD: RDD[UserBehaviorLog] = dataRDD
      .map(_.split(","))
      .filter(fields => fields.length == 5)           // 字段数校验
      .filter(fields => fields.forall(_.nonEmpty))    // 空值过滤
      .map(f => UserBehaviorLog(
        userId    = f(0).trim,
        itemId    = f(1).trim,
        category  = f(2).trim,
        behavior  = f(3).trim,
        timestamp = f(4).trim.toLong
      ))
      .filter(log => Set("pv","buy","cart","fav").contains(log.behavior))  // 合法行为过滤
      .persist(StorageLevel.MEMORY_ONLY)

    // ── 优化：单次触发 Action 获取统计信息 ──
    val (rawCount, cleanCount) = (dataRDD.count(), parsedRDD.count())
    logger.info(s"原始数据总行数: $rawCount")
    logger.info(s"清洗后数据行数: $cleanCount")

    // ── Step 3：特征工程（map → CleanBehavior）──
    val cleanRDD: RDD[CleanBehavior] = parsedRDD.map { log =>
      import java.time.{Instant, ZoneId}
      val dt        = Instant.ofEpochSecond(log.timestamp).atZone(ZoneId.of(AppConfig.TIMEZONE))
      val hour      = dt.getHour
      val dayOfWeek = dt.getDayOfWeek.getValue   // 1=Mon ... 7=Sun
      val isWeekend = if (dayOfWeek >= 6) 1 else 0
      CleanBehavior(log.userId, log.itemId, log.category, log.behavior, hour, dayOfWeek, isWeekend)
    }.persist(StorageLevel.MEMORY_ONLY)

    // ── Step 4：键值对统计 —— 各行为 Top N ──
    logger.info("各行为统计：")
    val behaviorCount: RDD[(String, Int)] = cleanRDD
      .map(b => (b.behavior, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    behaviorCount.collect().foreach { case (beh, cnt) =>
      logger.info(f"  $beh%-6s -> $cnt%,d")
    }

    // ── Step 5：Top N 活跃用户（PairRDD + sortByKey）──
    logger.info("Top 10 活跃用户（PV行为）：")
    cleanRDD
      .filter(_.behavior == "pv")
      .map(b => (b.userId, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach { case (uid, cnt) => logger.info(s"  $uid -> $cnt次") }

    // ── Step 6：保存清洗结果（saveAsTextFile）──
    cleanRDD
      .map(b => s"${b.userId},${b.itemId},${b.category},${b.behavior},${b.hour},${b.dayOfWeek},${b.isWeekend}")
      .saveAsTextFile(AppConfig.CLEAN_OUTPUT_PATH)

    logger.info(s"清洗数据已保存至: ${AppConfig.CLEAN_OUTPUT_PATH}")

    // 广播变量示例（第4章性能调优知识点）
    val categoryBroadcast = sc.broadcast(Set("Electronics", "Clothing", "Food"))
    val filteredRDD = cleanRDD.filter(b => categoryBroadcast.value.contains(b.category))
    logger.info(s"过滤指定类目后数据量: ${filteredRDD.count()}")

    // ── 清理缓存 ──
    dataRDD.unpersist()
    parsedRDD.unpersist()
    cleanRDD.unpersist()
    categoryBroadcast.unpersist()

    logger.info("模块一执行完毕 ✓")
  }
}
