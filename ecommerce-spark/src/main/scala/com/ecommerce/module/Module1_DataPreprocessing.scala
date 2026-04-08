package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, CleanBehavior, SparkSessionFactory, UserBehaviorLog}
import com.ecommerce.util.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import java.io.File
import java.time.{Instant, ZoneId}
import scala.util.Try

/**
 * Module 1: data preprocessing with RDD.
 */
object Module1_DataPreprocessing extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("1")}")
    logger.info("=" * 60)

    val sc = SparkSessionFactory.getContext()
    val rawRDD = sc.textFile(AppConfig.RAW_LOG_PATH)
    val header = rawRDD.first()
    val dataRDD = rawRDD.filter(_ != header).persist(StorageLevel.MEMORY_ONLY)

    val parsedRDD = parseLogs(dataRDD).persist(StorageLevel.MEMORY_ONLY)
    val cleanRDD = enrichCleanBehavior(parsedRDD).persist(StorageLevel.MEMORY_ONLY)

    val rawCount = dataRDD.count()
    val cleanCount = parsedRDD.count()
    logger.info(s"Raw rows: $rawCount")
    logger.info(s"Clean rows: $cleanCount")

    logger.info("Behavior counts:")
    cleanRDD
      .map(record => (record.behavior, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .collect()
      .foreach { case (behavior, count) =>
        logger.info(f"  $behavior%-8s -> $count%,d")
      }

    logger.info(s"Top 10 active users by '${Behaviors.VIEW}':")
    cleanRDD
      .filter(_.behavior == Behaviors.VIEW)
      .map(record => (record.userId, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach { case (userId, count) =>
        logger.info(s"  $userId -> $count")
      }

    overwriteOutput(AppConfig.CLEAN_OUTPUT_PATH)
    cleanRDD
      .map(record =>
        s"${record.userId},${record.itemId},${record.category},${record.behavior},${record.hour},${record.dayOfWeek},${record.isWeekend}"
      )
      .saveAsTextFile(AppConfig.CLEAN_OUTPUT_PATH)
    logger.info(s"Cleaned output saved: ${AppConfig.CLEAN_OUTPUT_PATH}")

    val categoryBroadcast = sc.broadcast(AppConfig.HIGHLIGHT_CATEGORIES)
    val filteredRDD = cleanRDD.filter(record => categoryBroadcast.value.contains(record.category))
    logger.info(s"Rows in highlighted categories: ${filteredRDD.count()}")

    dataRDD.unpersist()
    parsedRDD.unpersist()
    cleanRDD.unpersist()
    categoryBroadcast.unpersist()

    logger.info("Module 1 completed")
  }

  private[module] def parseLogs(dataRDD: RDD[String]): RDD[UserBehaviorLog] = {
    dataRDD.flatMap { line =>
      val fields = line.split(",").map(_.trim)
      val maybeTimestamp = if (fields.length == 5) Try(fields(4).toLong).toOption else None

      if (
        fields.length == 5 &&
        fields.forall(_.nonEmpty) &&
        Behaviors.VALID_BEHAVIORS.contains(fields(3)) &&
        maybeTimestamp.exists(_ > 0)
      ) {
        Some(
          UserBehaviorLog(
            userId = fields(0),
            itemId = fields(1),
            category = fields(2),
            behavior = fields(3),
            timestamp = maybeTimestamp.get
          )
        )
      } else {
        None
      }
    }
  }

  private[module] def enrichCleanBehavior(parsedRDD: RDD[UserBehaviorLog]): RDD[CleanBehavior] = {
    parsedRDD.map { log =>
      val dateTime = Instant.ofEpochSecond(log.timestamp).atZone(ZoneId.of(AppConfig.TIMEZONE))
      val hour = dateTime.getHour
      val dayOfWeek = dateTime.getDayOfWeek.getValue
      val isWeekend = if (dayOfWeek >= 6) 1 else 0

      CleanBehavior(log.userId, log.itemId, log.category, log.behavior, hour, dayOfWeek, isWeekend)
    }
  }

  private def overwriteOutput(path: String): Unit = {
    val outputDir = new File(path)
    if (outputDir.exists()) {
      logger.warn(s"Output path exists, removing old results: $path")
      deleteRecursively(outputDir)
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      val children = file.listFiles()
      if (children != null) children.foreach(deleteRecursively)
    }
    file.delete()
  }
}
