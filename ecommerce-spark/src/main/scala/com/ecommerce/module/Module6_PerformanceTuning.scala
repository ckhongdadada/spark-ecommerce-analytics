package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import com.ecommerce.util.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
 * Module 6: performance tuning and benchmark.
 */
object Module6_PerformanceTuning extends Logging {

  case class BenchmarkMeasurement(
    scenario: String,
    variant: String,
    phase: String,
    runId: Int,
    durationMs: Long,
    rowCount: Long,
    partitionCount: Int,
    recordedAt: String
  )

  case class BenchmarkSummary(
    scenario: String,
    variant: String,
    measuredRuns: Int,
    warmupRuns: Int,
    rowCount: Long,
    partitionCount: Int,
    avgDurationMs: Long,
    minDurationMs: Long,
    medianDurationMs: Long,
    p95DurationMs: Long,
    maxDurationMs: Long
  )

  case class BenchmarkReport(
    measurements: Seq[BenchmarkMeasurement],
    summaries: Seq[BenchmarkSummary],
    explainPlan: String
  )

  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("6")}")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.RAW_LOG_PATH)
      .cache()

    rawDF.count()

    val benchmarkReport = runBenchmarks(spark, rawDF)
    persistBenchmarkReport(benchmarkReport)

    logger.info(s"Benchmark results written: ${AppConfig.BENCHMARK_OUTPUT_PATH}")
    benchmarkReport.summaries.foreach { summary =>
      logger.info(
        s"  ${summary.scenario} / ${summary.variant}: " +
          s"avg=${summary.avgDurationMs}ms, p95=${summary.p95DurationMs}ms, " +
          s"runs=${summary.measuredRuns}, rows=${summary.rowCount}"
      )
    }

    logger.info(s"Event log directory (if enabled): ${AppConfig.EVENT_LOG_DIR}")
    rawDF.unpersist()
    logger.info("Module 6 completed")
  }

  private[module] def runBenchmarks(spark: SparkSession, rawDF: DataFrame): BenchmarkReport = {
    import spark.implicits._

    val buyDFMemoryOnly = rawDF.filter($"behavior" === Behaviors.BUY).persist(StorageLevel.MEMORY_ONLY)
    val buyDFMemoryAndDisk = rawDF.filter($"behavior" === Behaviors.BUY).persist(StorageLevel.MEMORY_AND_DISK)

    val discountMap = AppConfig.DOMAIN_CATEGORIES.zipWithIndex.map { case (category, index) =>
      val discount = 0.75 + (index % 5) * 0.05
      category -> discount
    }.toMap
    val discountDimDF = discountMap.toSeq.toDF("category", "discount")

    val measurements = Seq(
      benchmarkScenario("cache_strategy", "memory_only", buyDFMemoryOnly, _.count()),
      benchmarkScenario("cache_strategy", "memory_and_disk", buyDFMemoryAndDisk, _.count()),
      benchmarkScenario("join_strategy", "regular_join", rawDF.join(discountDimDF, Seq("category"), "left"), _.count()),
      benchmarkScenario("join_strategy", "broadcast_join", rawDF.join(broadcast(discountDimDF), Seq("category"), "left"), _.count()),
      benchmarkScenario("partition_strategy", "default_partitions", rawDF, _.groupBy("category").count().count()),
      benchmarkScenario("partition_strategy", "repartitioned", rawDF.repartition(AppConfig.DEFAULT_PARALLELISM), _.groupBy("category").count().count()),
      benchmarkScenario("skew_strategy", "regular_key_join", buildSkewedFact(rawDF), _.join(buildSkewedDim(spark), Seq("join_key"), "inner").count()),
      benchmarkScenario("skew_strategy", "salted_join", buildSaltedFact(rawDF), _.join(buildSaltedDim(spark), Seq("join_key"), "inner").count())
    ).flatten

    buyDFMemoryOnly.unpersist(blocking = false)
    buyDFMemoryAndDisk.unpersist(blocking = false)

    val explainPlan = rawDF
      .groupBy("category")
      .agg(count("*").alias("cnt"))
      .orderBy(desc("cnt"))
      .queryExecution
      .executedPlan
      .toString()

    val summaries = summarizeMeasurements(measurements)
    BenchmarkReport(measurements, summaries, explainPlan)
  }

  private def benchmarkScenario(
    scenario: String,
    variant: String,
    df: DataFrame,
    action: DataFrame => Long
  ): Seq[BenchmarkMeasurement] = {
    val warmupRuns = AppConfig.BENCHMARK_WARMUP_RUNS
    val measuredRuns = AppConfig.BENCHMARK_MEASURED_RUNS

    val warmupSamples = (1 to warmupRuns).map { runId =>
      measure(df, scenario, variant, "warmup", runId, action)
    }

    val measuredSamples = (1 to measuredRuns).map { runId =>
      measure(df, scenario, variant, "measured", runId, action)
    }

    warmupSamples ++ measuredSamples
  }

  private def measure(
    df: DataFrame,
    scenario: String,
    variant: String,
    phase: String,
    runId: Int,
    action: DataFrame => Long
  ): BenchmarkMeasurement = {
    val startedAt = System.currentTimeMillis()
    val rowCount = action(df)
    val durationMs = System.currentTimeMillis() - startedAt

    BenchmarkMeasurement(
      scenario = scenario,
      variant = variant,
      phase = phase,
      runId = runId,
      durationMs = durationMs,
      rowCount = rowCount,
      partitionCount = df.rdd.getNumPartitions,
      recordedAt = LocalDateTime.now().format(timestampFormatter)
    )
  }

  private[module] def summarizeMeasurements(measurements: Seq[BenchmarkMeasurement]): Seq[BenchmarkSummary] = {
    measurements
      .filter(_.phase == "measured")
      .groupBy(m => (m.scenario, m.variant))
      .toSeq
      .sortBy { case ((scenario, variant), _) => (scenario, variant) }
      .map { case ((scenario, variant), samples) =>
        val durations = samples.map(_.durationMs)
        BenchmarkSummary(
          scenario = scenario,
          variant = variant,
          measuredRuns = samples.size,
          warmupRuns = AppConfig.BENCHMARK_WARMUP_RUNS,
          rowCount = samples.headOption.map(_.rowCount).getOrElse(0L),
          partitionCount = samples.headOption.map(_.partitionCount).getOrElse(0),
          avgDurationMs = durations.sum / math.max(1, durations.size),
          minDurationMs = durations.min,
          medianDurationMs = percentile(durations, 0.5),
          p95DurationMs = percentile(durations, 0.95),
          maxDurationMs = durations.max
        )
      }
  }

  private[module] def percentile(samples: Seq[Long], p: Double): Long = {
    if (samples.isEmpty) {
      0L
    } else {
      val sorted = samples.sorted
      val index = math.ceil(sorted.size * p).toInt - 1
      sorted(index.max(0).min(sorted.size - 1))
    }
  }

  private def buildSkewedFact(rawDF: DataFrame): DataFrame = {
    rawDF.withColumn(
      "join_key",
      when(col("category") === AppConfig.SKEW_HOT_CATEGORY, lit("hot_category")).otherwise(col("category"))
    )
  }

  private def buildSkewedDim(spark: SparkSession): DataFrame = {
    import spark.implicits._
    AppConfig.CATEGORY_DIMENSION_TAGS.toSeq
      .map { case (category, tag) =>
        val joinKey = if (category == AppConfig.SKEW_HOT_CATEGORY) "hot_category" else category
        (joinKey, tag)
      }
      .toDF("join_key", "tag")
  }

  private def buildSaltedFact(rawDF: DataFrame): DataFrame = {
    val saltBucketCount = 8
    rawDF
      .withColumn(
        "base_key",
        when(col("category") === AppConfig.SKEW_HOT_CATEGORY, lit("hot_category")).otherwise(col("category"))
      )
      .withColumn("salt", (rand(42) * saltBucketCount).cast("int"))
      .withColumn("join_key", concat(col("base_key"), lit("_"), col("salt")))
  }

  private def buildSaltedDim(spark: SparkSession): DataFrame = {
    import spark.implicits._
    val saltBucketCount = 8
    val baseKeys = AppConfig.DOMAIN_CATEGORIES.map { category =>
      if (category == AppConfig.SKEW_HOT_CATEGORY) "hot_category" else category
    }

    baseKeys
      .flatMap(key => (0 until saltBucketCount).map(salt => (s"${key}_$salt", key)))
      .toDF("join_key", "base_key")
  }

  private def persistBenchmarkReport(report: BenchmarkReport): Unit = {
    val basePath = Paths.get(AppConfig.BENCHMARK_OUTPUT_PATH)
    Files.createDirectories(basePath)

    writeTextFile(basePath.resolve("measurements.csv"), measurementCsv(report.measurements))
    writeTextFile(basePath.resolve("summaries.csv"), summaryCsv(report.summaries))
    writeTextFile(basePath.resolve("explain_plan.txt"), report.explainPlan)
    writeTextFile(basePath.resolve("README.txt"), benchmarkReadme(basePath))
  }

  private def writeTextFile(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def benchmarkReadme(basePath: Path): String = {
    s"""Benchmark output notes
       |=====================
       |1. measurements.csv stores each warmup/measured run.
       |2. summaries.csv stores aggregated stats per scenario.
       |3. explain_plan.txt stores the execution plan snapshot.
       |4. Event log is written to ${Paths.get(AppConfig.EVENT_LOG_DIR).toAbsolutePath} when environment supports it.
       |
       |Output directory: ${basePath.toAbsolutePath}
       |""".stripMargin
  }

  private def measurementCsv(rows: Seq[BenchmarkMeasurement]): String = {
    val header = "scenario,variant,phase,run_id,duration_ms,row_count,partition_count,recorded_at"
    val body = rows.map { row =>
      Seq(
        row.scenario,
        row.variant,
        row.phase,
        row.runId.toString,
        row.durationMs.toString,
        row.rowCount.toString,
        row.partitionCount.toString,
        row.recordedAt
      ).map(csvEscape).mkString(",")
    }

    (header +: body).mkString(System.lineSeparator())
  }

  private def summaryCsv(rows: Seq[BenchmarkSummary]): String = {
    val header = "scenario,variant,measured_runs,warmup_runs,row_count,partition_count,avg_duration_ms,min_duration_ms,median_duration_ms,p95_duration_ms,max_duration_ms"
    val body = rows.map { row =>
      Seq(
        row.scenario,
        row.variant,
        row.measuredRuns.toString,
        row.warmupRuns.toString,
        row.rowCount.toString,
        row.partitionCount.toString,
        row.avgDurationMs.toString,
        row.minDurationMs.toString,
        row.medianDurationMs.toString,
        row.p95DurationMs.toString,
        row.maxDurationMs.toString
      ).map(csvEscape).mkString(",")
    }

    (header +: body).mkString(System.lineSeparator())
  }

  private def csvEscape(value: String): String = {
    "\"" + value.replace("\"", "\"\"") + "\""
  }
}
