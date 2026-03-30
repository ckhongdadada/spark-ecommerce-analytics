package com.ecommerce.core

import com.ecommerce.config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 * SparkSession / SparkContext 统一工厂
 * 所有模块通过此对象获取 Spark 入口，避免重复创建
 *
 * 使用方式：
 *   val spark = SparkSessionFactory.getSession()
 *   val sc    = SparkSessionFactory.getContext()
 */
object SparkSessionFactory {

  @volatile private var instance: SparkSession = _

  def getSession(enableHive: Boolean = false): SparkSession = {
    if (instance == null || instance.sparkContext.isStopped) {
      synchronized {
        if (instance == null || instance.sparkContext.isStopped) {
          instance = buildSession(enableHive)
        }
      }
    }
    instance
  }

  def getContext() = getSession().sparkContext

  private def buildSession(enableHive: Boolean): SparkSession = {
    val conf = new SparkConf()
      .setAppName(AppConfig.APP_NAME)
      .setMaster(AppConfig.MASTER)
      // ── 第4章 性能调优相关配置 ──
      .set("spark.serializer",               AppConfig.SERIALIZER)
      .set("spark.sql.shuffle.partitions",   AppConfig.SHUFFLE_PARTITIONS.toString)
      .set("spark.default.parallelism",      AppConfig.DEFAULT_PARALLELISM.toString)
      .set("spark.executor.memory",          AppConfig.EXECUTOR_MEMORY)
      .set("spark.driver.memory",            AppConfig.DRIVER_MEMORY)
      .set("spark.sql.autoBroadcastJoinThreshold",
           (AppConfig.BROADCAST_THRESHOLD_MB * 1024 * 1024).toString)
      .set("spark.sql.adaptive.enabled",     "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      // Kryo 注册（减少序列化开销）
      .registerKryoClasses(Array(
        classOf[UserBehaviorLog],
        classOf[CleanBehavior],
        classOf[SalesReport],
        classOf[UserNode],
        classOf[BuyEdge],
        classOf[Array[String]],
        classOf[scala.collection.mutable.WrappedArray[String]]
      ))

    val builder = SparkSession.builder().config(conf)
    if (enableHive) builder.enableHiveSupport()
    builder.getOrCreate()
  }

  def stop(): Unit = {
    if (instance != null && !instance.sparkContext.isStopped) {
      instance.stop()
    }
  }
}
