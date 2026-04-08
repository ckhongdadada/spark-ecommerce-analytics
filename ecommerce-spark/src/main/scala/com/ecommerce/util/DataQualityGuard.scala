package com.ecommerce.util

import com.ecommerce.config.AppConfig
import com.ecommerce.core.Behaviors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Shared ingress quality guard.
 * It enforces baseline data rules and emits quality alerts to file.
 */
object DataQualityGuard extends Logging {

  case class IngressQualitySummary(
    sourceTag: String,
    runId: String,
    totalRows: Long,
    validRows: Long,
    invalidRows: Long,
    invalidRatio: Double,
    buyRate: Double,
    generatedAt: String
  )

  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val runIdFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")

  def loadValidatedBatchEvents(spark: SparkSession, inputPath: String, sourceTag: String): DataFrame = {
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(inputPath)

    val (validDF0, invalidDF0) = splitValidAndInvalid(rawDF)
    val validDF = validDF0.persist(StorageLevel.MEMORY_AND_DISK)
    val invalidDF = invalidDF0.persist(StorageLevel.MEMORY_AND_DISK)

    val validRows = validDF.count()
    val invalidRows = invalidDF.count()
    val totalRows = validRows + invalidRows
    val invalidRatio = if (totalRows == 0L) 0.0 else invalidRows.toDouble / totalRows
    val buyRows = validDF.filter(col("behavior") === Behaviors.BUY).count()
    val buyRate = if (validRows == 0L) 0.0 else buyRows.toDouble / validRows

    val summary = IngressQualitySummary(
      sourceTag = sourceTag,
      runId = LocalDateTime.now().format(runIdFormatter),
      totalRows = totalRows,
      validRows = validRows,
      invalidRows = invalidRows,
      invalidRatio = round4(invalidRatio),
      buyRate = round4(buyRate),
      generatedAt = LocalDateTime.now().format(timestampFormatter)
    )

    val alerts = buildAlerts(summary)
    logSummary(summary, alerts)
    persistSummary(summary, alerts)
    persistInvalidSample(invalidDF, summary)

    invalidDF.unpersist(blocking = false)
    validDF
  }

  def splitValidAndInvalid(df: DataFrame): (DataFrame, DataFrame) = {
    val withRequiredCols = ensureRequiredColumns(df, Seq("userId", "itemId", "category", "behavior", "timestamp"))

    val normalized = withRequiredCols
      .withColumn("userId", trim(col("userId").cast("string")))
      .withColumn("itemId", trim(col("itemId").cast("string")))
      .withColumn("category", trim(col("category").cast("string")))
      .withColumn("behavior", lower(trim(col("behavior").cast("string"))))
      .withColumn("timestamp", col("timestamp").cast("long"))

    val nonEmptyUser = col("userId").isNotNull && length(col("userId")) > 0
    val nonEmptyItem = col("itemId").isNotNull && length(col("itemId")) > 0
    val nonEmptyCategory = col("category").isNotNull && length(col("category")) > 0
    val validBehavior = col("behavior").isNotNull && col("behavior").isin(AppConfig.VALID_BEHAVIORS.toSeq: _*)
    val maxAllowedTs = unix_timestamp(current_timestamp()) + lit(AppConfig.DQ_MAX_FUTURE_SECONDS)
    val validTimestamp = col("timestamp").isNotNull && col("timestamp") > lit(0L) && col("timestamp") <= maxAllowedTs

    val invalidReason = when(!nonEmptyUser, lit("missing_userId"))
      .when(!nonEmptyItem, lit("missing_itemId"))
      .when(!nonEmptyCategory, lit("missing_category"))
      .when(!validBehavior, lit("invalid_behavior"))
      .when(!validTimestamp, lit("invalid_timestamp"))

    val invalidDF = normalized
      .withColumn("invalid_reason", invalidReason)
      .filter(col("invalid_reason").isNotNull)
      .withColumn("detected_at", current_timestamp())
      .select("userId", "itemId", "category", "behavior", "timestamp", "invalid_reason", "detected_at")

    val validDF = normalized
      .filter(nonEmptyUser && nonEmptyItem && nonEmptyCategory && validBehavior && validTimestamp)
      .withColumn("event_time", to_timestamp(from_unixtime(col("timestamp"))))
      .select("userId", "itemId", "category", "behavior", "timestamp", "event_time")

    (validDF, invalidDF)
  }

  private def ensureRequiredColumns(df: DataFrame, requiredCols: Seq[String]): DataFrame = {
    requiredCols.foldLeft(df) { case (acc, columnName) =>
      if (acc.columns.contains(columnName)) acc else acc.withColumn(columnName, lit(null).cast("string"))
    }
  }

  private def buildAlerts(summary: IngressQualitySummary): Seq[String] = {
    val alerts = scala.collection.mutable.ArrayBuffer.empty[String]
    if (summary.totalRows < AppConfig.DQ_MIN_REQUIRED_ROWS) {
      alerts += s"rows_below_threshold(${summary.totalRows} < ${AppConfig.DQ_MIN_REQUIRED_ROWS})"
    }
    if (summary.invalidRatio > AppConfig.DQ_MAX_INVALID_RATIO) {
      alerts += f"invalid_ratio_too_high(${summary.invalidRatio}%.4f > ${AppConfig.DQ_MAX_INVALID_RATIO}%.4f)"
    }
    if (summary.buyRate < AppConfig.DQ_MIN_BUY_RATE) {
      alerts += f"buy_rate_too_low(${summary.buyRate}%.4f < ${AppConfig.DQ_MIN_BUY_RATE}%.4f)"
    }
    if (summary.buyRate > AppConfig.DQ_MAX_BUY_RATE) {
      alerts += f"buy_rate_too_high(${summary.buyRate}%.4f > ${AppConfig.DQ_MAX_BUY_RATE}%.4f)"
    }
    alerts.toSeq
  }

  private def logSummary(summary: IngressQualitySummary, alerts: Seq[String]): Unit = {
    logger.info(
      s"[DQ][${summary.sourceTag}] total=${summary.totalRows}, valid=${summary.validRows}, " +
        f"invalidRatio=${summary.invalidRatio}%.4f, buyRate=${summary.buyRate}%.4f"
    )

    if (alerts.nonEmpty) {
      alerts.foreach(alert => logger.warn(s"[DQ][${summary.sourceTag}] alert: $alert"))
    } else {
      logger.info(s"[DQ][${summary.sourceTag}] quality gate passed.")
    }
  }

  private def persistSummary(summary: IngressQualitySummary, alerts: Seq[String]): Unit = {
    val summaryPath = Paths.get(AppConfig.DQ_OUTPUT_PATH, "ingress_summary.csv")
    val alertsPath = Paths.get(AppConfig.DQ_OUTPUT_PATH, "ingress_alerts.csv")

    writeCsvLine(
      summaryPath,
      "generated_at,source_tag,run_id,total_rows,valid_rows,invalid_rows,invalid_ratio,buy_rate",
      Seq(
        summary.generatedAt,
        summary.sourceTag,
        summary.runId,
        summary.totalRows.toString,
        summary.validRows.toString,
        summary.invalidRows.toString,
        f"${summary.invalidRatio}%.6f",
        f"${summary.buyRate}%.6f"
      )
    )

    if (alerts.nonEmpty) {
      alerts.foreach { alert =>
        writeCsvLine(
          alertsPath,
          "generated_at,source_tag,run_id,alert",
          Seq(summary.generatedAt, summary.sourceTag, summary.runId, alert)
        )
      }
    }
  }

  private def persistInvalidSample(invalidDF: DataFrame, summary: IngressQualitySummary): Unit = {
    if (summary.invalidRows <= 0L) return

    val path = Paths.get(AppConfig.DQ_OUTPUT_PATH, "invalid_rows", s"${sanitize(summary.sourceTag)}_${summary.runId}").toString
    invalidDF
      .limit(200)
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(path)
  }

  private def writeCsvLine(path: Path, header: String, values: Seq[String]): Unit = {
    Files.createDirectories(path.getParent)
    if (!Files.exists(path)) {
      Files.write(path, (header + System.lineSeparator()).getBytes(StandardCharsets.UTF_8))
    }

    val line = values.map(csvEscape).mkString(",") + System.lineSeparator()
    Files.write(path, line.getBytes(StandardCharsets.UTF_8), StandardOpenOption.APPEND)
  }

  private def csvEscape(value: String): String = {
    "\"" + value.replace("\"", "\"\"") + "\""
  }

  private def sanitize(value: String): String = {
    value.replaceAll("[^a-zA-Z0-9_-]", "_")
  }

  private def round4(value: Double): Double = {
    math.round(value * 10000.0) / 10000.0
  }
}
