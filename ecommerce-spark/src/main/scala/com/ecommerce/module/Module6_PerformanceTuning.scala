package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.util.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

/**
 * ══════════════════════════════════════════════════════════
 * 模块六：性能调优（★ 原提纲遗漏的第4章核心内容）
 * 对应课程：第4章 Spark性能调优
 *   - 持久化策略（cache / persist / StorageLevel）
 *   - 广播变量（Broadcast Variable）
 *   - 分区优化（repartition / coalesce）
 *   - 数据倾斜处理（Skew Join）
 *   - Kryo 序列化
 *   - SQL 执行计划分析（explain）
 * ══════════════════════════════════════════════════════════
 */
object Module6_PerformanceTuning extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info("  模块六：Spark 性能调优实战")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()
    val sc    = spark.sparkContext
    import spark.implicits._

    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.RAW_LOG_PATH)

    // ──────────────────────────────────────────────────────
    // 知识点 1：持久化策略对比
    // ──────────────────────────────────────────────────────
    logger.info("持久化策略对比")

    val buyDF = rawDF.filter($"behavior" === "buy")

    // MEMORY_ONLY：预热后多次访问，观察稳定时延
    buyDF.persist(StorageLevel.MEMORY_ONLY)
    val c1 = buyDF.count()
    val memoryOnlySamples = measureRepeated(3) { buyDF.count() }
    logger.info(s"  MEMORY_ONLY     count=$c1  平均耗时=${average(memoryOnlySamples)}ms  样本=${memoryOnlySamples.mkString("[", ", ", "]")}")

    // MEMORY_AND_DISK：同样预热后多次访问
    buyDF.unpersist()
    buyDF.persist(StorageLevel.MEMORY_AND_DISK)
    val c2 = buyDF.count()
    val memoryAndDiskSamples = measureRepeated(3) { buyDF.count() }
    logger.info(s"  MEMORY_AND_DISK count=$c2  平均耗时=${average(memoryAndDiskSamples)}ms  样本=${memoryAndDiskSamples.mkString("[", ", ", "]")}")

    buyDF.unpersist()

    // ──────────────────────────────────────────────────────
    // 知识点 2：广播变量（避免大表 Shuffle Join）
    // ──────────────────────────────────────────────────────
    logger.info("广播变量 vs 普通 Join")

    // 小表：类目折扣映射
    val discountMap = Map("Electronics"->0.9, "Clothing"->0.8,
                          "Food"->0.95, "Books"->0.85, "Sports"->0.88, "Beauty"->0.75)
    val broadcastDiscount = sc.broadcast(discountMap)
    val discountDimDF = discountMap.toSeq.toDF("category", "discount")

    val regularJoinSamples = measureRepeated(3) {
      rawDF.join(discountDimDF, Seq("category"), "left").count()
    }
    logger.info(s"  普通 Join      平均耗时=${average(regularJoinSamples)}ms  样本=${regularJoinSamples.mkString("[", ", ", "]")}")

    // 显式广播小表，避免 Shuffle
    val discountDF = rawDF.map { row =>
      val cat      = row.getAs[String]("category")
      val discount = broadcastDiscount.value.getOrElse(cat, 1.0)
      (row.getAs[String]("userId"), cat, discount)
    }.toDF("userId", "category", "discount")

    val broadcastJoinSamples = measureRepeated(3) { discountDF.count() }
    logger.info(s"  广播 Join      平均耗时=${average(broadcastJoinSamples)}ms  样本=${broadcastJoinSamples.mkString("[", ", ", "]")}")

    // ──────────────────────────────────────────────────────
    // 知识点 3：分区优化
    // ──────────────────────────────────────────────────────
    logger.info("分区数优化")

    val defaultParts = rawDF.rdd.getNumPartitions
    logger.info(s"  默认分区数: $defaultParts")

    // 增大分区（适合大数据集并行计算）
    val repartitioned = rawDF.repartition(AppConfig.DEFAULT_PARALLELISM)
    logger.info(s"  repartition后: ${repartitioned.rdd.getNumPartitions}")

    // 减小分区（写出前合并，避免小文件）
    val coalesced = rawDF.coalesce(1)
    logger.info(s"  coalesce(1)后: ${coalesced.rdd.getNumPartitions} （写出前合并小文件）")

    // ──────────────────────────────────────────────────────
    // 知识点 4：数据倾斜处理（Skew Join 加盐法）
    // ──────────────────────────────────────────────────────
    logger.info("数据倾斜处理（加盐法 Salt）")

    val SALT = 10  // 盐粒数，根据倾斜程度调整

    // 左表：为热点 Key 加随机盐
    val saltedLeft = rawDF
      .withColumn("salt", (rand() * SALT).cast("int"))
      .withColumn("salted_key", concat($"category", lit("_"), $"salt"))

    // 右表：炸开盐（每个 Key 复制 SALT 份）
    val categoryRef = rawDF.select("category").distinct()
    val saltedRight = categoryRef
      .withColumn("salt", explode(array((0 until SALT).map(lit(_)): _*)))
      .withColumn("salted_key", concat($"category", lit("_"), $"salt"))

    val skewedJoinSamples = measureRepeated(3) {
      saltedLeft.join(saltedRight, "salted_key").count()
    }
    logger.info(s"  加盐 Join      平均耗时=${average(skewedJoinSamples)}ms  样本=${skewedJoinSamples.mkString("[", ", ", "]")}")

    // ──────────────────────────────────────────────────────
    // 知识点 5：执行计划分析
    // ──────────────────────────────────────────────────────
    logger.info("SQL 执行计划（explain）")

    val analyzedDF = rawDF
      .groupBy("category")
      .agg(count("*").alias("cnt"))
      .orderBy(desc("cnt"))

    logger.info("  --- 逻辑计划 ---")
    analyzedDF.explain(mode = "formatted")  // 输出完整执行计划

    broadcastDiscount.unpersist()
    logger.info("模块六执行完毕 ✓")
  }

  private def measureRepeated(runs: Int)(action: => Long): Seq[Long] = {
    (1 to runs).map { _ =>
      val startedAt = System.currentTimeMillis()
      action
      System.currentTimeMillis() - startedAt
    }
  }

  private def average(samples: Seq[Long]): Long = {
    if (samples.isEmpty) 0L else samples.sum / samples.size
  }
}
