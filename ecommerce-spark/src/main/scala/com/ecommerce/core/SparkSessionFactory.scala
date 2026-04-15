package com.ecommerce.core

import com.ecommerce.config.AppConfig
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File
import java.nio.file.{Files, Paths}

/**
 * SparkSession / SparkContext 统一工厂。
 * 所有模块统一从这里获取 Spark 入口，避免重复配置和环境漂移。
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
    ensureRuntimeDirectories()
    val enableEventLog = eventLogSupported()

    val conf = new SparkConf()
      .setAppName(AppConfig.APP_NAME)
      .setMaster(AppConfig.MASTER)
      .set("spark.sql.session.timeZone", AppConfig.TIMEZONE)
      .set("spark.serializer", AppConfig.SERIALIZER)
      .set("spark.sql.shuffle.partitions", AppConfig.SHUFFLE_PARTITIONS.toString)
      .set("spark.default.parallelism", AppConfig.DEFAULT_PARALLELISM.toString)
      .set("spark.executor.memory", AppConfig.EXECUTOR_MEMORY)
      .set("spark.driver.memory", AppConfig.DRIVER_MEMORY)
      .set("spark.sql.autoBroadcastJoinThreshold", (AppConfig.BROADCAST_THRESHOLD_MB * 1024 * 1024).toString)
      .set("spark.sql.adaptive.enabled", "true")
      .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
      .set("spark.sql.adaptive.skewJoin.enabled", "true")
      .set("spark.eventLog.enabled", enableEventLog.toString)
      .set("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
      .set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .registerKryoClasses(Array(
        classOf[UserBehaviorLog],
        classOf[CleanBehavior],
        classOf[SalesReport],
        classOf[UserNode],
        classOf[BuyEdge],
        classOf[Array[String]],
        classOf[scala.collection.mutable.WrappedArray[String]]
      ))

    if (enableEventLog) {
      conf
        .set("spark.eventLog.compress", "true")
        .set("spark.eventLog.dir", absoluteFileUri(AppConfig.EVENT_LOG_DIR))
    }

    val builder = SparkSession.builder().config(conf)
    if (enableHive) builder.enableHiveSupport()
    builder.getOrCreate()
  }

  private def ensureRuntimeDirectories(): Unit = {
    Seq(
      AppConfig.OUTPUT_ROOT,
      AppConfig.EVENT_LOG_DIR,
      AppConfig.CHECKPOINT_PATH,
      AppConfig.STREAM_OUTPUT_PATH,
      AppConfig.STREAM_INVALID_OUTPUT_PATH,
      AppConfig.STREAM_ALERT_OUTPUT_PATH,
      AppConfig.BENCHMARK_OUTPUT_PATH,
      AppConfig.DQ_OUTPUT_PATH
    ).foreach { path =>
      Files.createDirectories(Paths.get(path))
    }
  }

  private def absoluteFileUri(path: String): String = {
    new File(path).getAbsoluteFile.toURI.toString
  }

  private def eventLogSupported(): Boolean = {
    val isWindows = System.getProperty("os.name", "").toLowerCase.contains("win")
    val hadoopHome = Option(System.getenv("HADOOP_HOME"))
      .filter(_.nonEmpty)
      .orElse(Option(System.getProperty("hadoop.home.dir")).filter(_.nonEmpty))

    !isWindows || hadoopHome.nonEmpty
  }

  def stop(): Unit = {
    if (instance != null && !instance.sparkContext.isStopped) {
      instance.stop()
    }
    instance = null
  }
}
