package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import com.ecommerce.util.{DataQualityGuard, Logging}
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
    maxDurationMs: Long,
    stddevDurationMs: Long,
    cvPercent: Double
  )

  case class BenchmarkReport(
    measurements: Seq[BenchmarkMeasurement],
    summaries: Seq[BenchmarkSummary],
    explainPlan: String
  )

  case class BenchmarkMetadata(
    benchmarkId: String,
    generatedAt: String,
    sparkVersion: String,
    appName: String,
    master: String,
    randomSeed: Long,
    sampleFraction: Double,
    rawRows: Long,
    benchmarkRows: Long,
    benchmarkPartitions: Int,
    warmupRuns: Int,
    measuredRuns: Int,
    shufflePartitions: String,
    defaultParallelism: String,
    executorMemory: String,
    driverMemory: String,
    adaptiveEnabled: String,
    eventLogEnabled: String,
    eventLogDir: String
  )

  private case class BenchmarkInput(
    df: DataFrame,
    rawRows: Long,
    benchmarkRows: Long,
    partitionCount: Int
  )

  private val timestampFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
  private val benchmarkIdFormatter = DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("6")}")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()

    val rawDF = DataQualityGuard.loadValidatedBatchEvents(
      spark,
      AppConfig.RAW_LOG_PATH,
      sourceTag = "module6_benchmark"
    )

    val benchmarkInput = prepareBenchmarkInput(rawDF)

    try {
      val benchmarkReport = runBenchmarks(spark, benchmarkInput.df)
      val metadata = buildMetadata(spark, benchmarkInput)
      val outputDir = persistBenchmarkReport(benchmarkReport, metadata)

      logger.info(s"Benchmark results written: ${outputDir.toAbsolutePath}")
      benchmarkReport.summaries.foreach { summary =>
        logger.info(
          s"  ${summary.scenario} / ${summary.variant}: " +
            s"avg=${summary.avgDurationMs}ms, p95=${summary.p95DurationMs}ms, " +
            f"stddev=${summary.stddevDurationMs}ms, cv=${summary.cvPercent}%.2f%%, " +
            s"runs=${summary.measuredRuns}, rows=${summary.rowCount}"
        )
      }

      logger.info(s"Event log directory (if enabled): ${AppConfig.EVENT_LOG_DIR}")
      logger.info("Module 6 completed")
    } finally {
      benchmarkInput.df.unpersist(blocking = false)
    }
  }

  private def prepareBenchmarkInput(rawDF: DataFrame): BenchmarkInput = {
    val rawRows = rawDF.count()
    val sampledDF =
      if (AppConfig.BENCHMARK_SAMPLE_FRACTION >= 1.0) rawDF
      else rawDF.sample(withReplacement = false, AppConfig.BENCHMARK_SAMPLE_FRACTION, AppConfig.BENCHMARK_RANDOM_SEED)

    val benchmarkDF = sampledDF
      .repartition(AppConfig.BENCHMARK_BASELINE_PARTITIONS)
      .persist(StorageLevel.MEMORY_AND_DISK)

    val benchmarkRows = benchmarkDF.count()
    val partitionCount = benchmarkDF.rdd.getNumPartitions

    logger.info(
      s"Benchmark input standardized: rawRows=$rawRows, benchmarkRows=$benchmarkRows, " +
        s"sampleFraction=${AppConfig.BENCHMARK_SAMPLE_FRACTION}, partitions=$partitionCount, seed=${AppConfig.BENCHMARK_RANDOM_SEED}"
    )

    BenchmarkInput(
      df = benchmarkDF,
      rawRows = rawRows,
      benchmarkRows = benchmarkRows,
      partitionCount = partitionCount
    )
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
    val skewedDimDF = buildSkewedDim(spark)
    val saltedDimDF = buildSaltedDim(spark)

    val measurements = Seq(
      benchmarkScenario("cache_strategy", "memory_only", buyDFMemoryOnly, _.count()),
      benchmarkScenario("cache_strategy", "memory_and_disk", buyDFMemoryAndDisk, _.count()),
      benchmarkScenario("join_strategy", "regular_join", rawDF.join(discountDimDF, Seq("category"), "left"), _.count()),
      benchmarkScenario("join_strategy", "broadcast_join", rawDF.join(broadcast(discountDimDF), Seq("category"), "left"), _.count()),
      benchmarkScenario("partition_strategy", "default_partitions", rawDF, _.groupBy("category").count().count()),
      benchmarkScenario("partition_strategy", "repartitioned", rawDF.repartition(AppConfig.DEFAULT_PARALLELISM), _.groupBy("category").count().count()),
      benchmarkScenario("skew_strategy", "regular_key_join", buildSkewedFact(rawDF), _.join(skewedDimDF, Seq("join_key"), "inner").count()),
      benchmarkScenario("skew_strategy", "salted_join", buildSaltedFact(rawDF), _.join(saltedDimDF, Seq("join_key"), "inner").count())
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
        val avgDurationDouble = durations.sum.toDouble / math.max(1, durations.size)
        val stddevMs = standardDeviation(durations, avgDurationDouble)
        val cvPercent = if (avgDurationDouble == 0.0) 0.0 else round4((stddevMs.toDouble / avgDurationDouble) * 100.0)

        BenchmarkSummary(
          scenario = scenario,
          variant = variant,
          measuredRuns = samples.size,
          warmupRuns = AppConfig.BENCHMARK_WARMUP_RUNS,
          rowCount = samples.headOption.map(_.rowCount).getOrElse(0L),
          partitionCount = samples.headOption.map(_.partitionCount).getOrElse(0),
          avgDurationMs = (durations.sum / math.max(1, durations.size)).toLong,
          minDurationMs = durations.min,
          medianDurationMs = percentile(durations, 0.5),
          p95DurationMs = percentile(durations, 0.95),
          maxDurationMs = durations.max,
          stddevDurationMs = stddevMs,
          cvPercent = cvPercent
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

  private def standardDeviation(samples: Seq[Long], avg: Double): Long = {
    if (samples.isEmpty) {
      0L
    } else {
      val variance = samples.map(sample => math.pow(sample - avg, 2)).sum / samples.size
      math.sqrt(variance).round
    }
  }

  private def round4(value: Double): Double = {
    math.round(value * 10000.0) / 10000.0
  }

  private def buildMetadata(spark: SparkSession, input: BenchmarkInput): BenchmarkMetadata = {
    BenchmarkMetadata(
      benchmarkId = LocalDateTime.now().format(benchmarkIdFormatter),
      generatedAt = LocalDateTime.now().format(timestampFormatter),
      sparkVersion = spark.version,
      appName = spark.sparkContext.appName,
      master = spark.sparkContext.master,
      randomSeed = AppConfig.BENCHMARK_RANDOM_SEED,
      sampleFraction = AppConfig.BENCHMARK_SAMPLE_FRACTION,
      rawRows = input.rawRows,
      benchmarkRows = input.benchmarkRows,
      benchmarkPartitions = input.partitionCount,
      warmupRuns = AppConfig.BENCHMARK_WARMUP_RUNS,
      measuredRuns = AppConfig.BENCHMARK_MEASURED_RUNS,
      shufflePartitions = spark.conf.get("spark.sql.shuffle.partitions", AppConfig.SHUFFLE_PARTITIONS.toString),
      defaultParallelism = spark.conf.get("spark.default.parallelism", AppConfig.DEFAULT_PARALLELISM.toString),
      executorMemory = spark.conf.get("spark.executor.memory", AppConfig.EXECUTOR_MEMORY),
      driverMemory = spark.conf.get("spark.driver.memory", AppConfig.DRIVER_MEMORY),
      adaptiveEnabled = spark.conf.get("spark.sql.adaptive.enabled", "true"),
      eventLogEnabled = spark.conf.get("spark.eventLog.enabled", "false"),
      eventLogDir = spark.conf.get("spark.eventLog.dir", AppConfig.EVENT_LOG_DIR)
    )
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
      .withColumn("salt", (rand(AppConfig.BENCHMARK_RANDOM_SEED) * saltBucketCount).cast("int"))
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

  private def persistBenchmarkReport(report: BenchmarkReport, metadata: BenchmarkMetadata): Path = {
    val rootPath = Paths.get(AppConfig.BENCHMARK_OUTPUT_PATH)
    Files.createDirectories(rootPath)

    val runPath = rootPath.resolve(s"run_${metadata.benchmarkId}")
    Files.createDirectories(runPath)

    writeTextFile(runPath.resolve("measurements.csv"), measurementCsv(report.measurements))
    writeTextFile(runPath.resolve("summaries.csv"), summaryCsv(report.summaries))
    writeTextFile(runPath.resolve("metadata.csv"), metadataCsv(metadata))
    writeTextFile(runPath.resolve("explain_plan.txt"), report.explainPlan)
    writeTextFile(runPath.resolve("README.txt"), benchmarkReadme(runPath, metadata))
    writeTextFile(rootPath.resolve("latest_run.txt"), latestRunText(runPath, metadata))

    runPath
  }

  private def writeTextFile(path: Path, content: String): Unit = {
    Files.createDirectories(path.getParent)
    Files.write(path, content.getBytes(StandardCharsets.UTF_8))
  }

  private def benchmarkReadme(runPath: Path, metadata: BenchmarkMetadata): String = {
    s"""Benchmark output notes
       |=====================
       |This run ID: ${metadata.benchmarkId}
       |Generated at: ${metadata.generatedAt}
       |Rows (raw -> benchmark): ${metadata.rawRows} -> ${metadata.benchmarkRows}
       |Sample fraction: ${metadata.sampleFraction}
       |Seed: ${metadata.randomSeed}
       |
       |1. measurements.csv stores each warmup/measured run.
       |2. summaries.csv stores aggregated stats per scenario.
       |3. metadata.csv stores fixed runtime/resource context.
       |4. explain_plan.txt stores the execution plan snapshot.
       |5. latest_run.txt in benchmark root points to current run directory.
       |
       |Run output directory: ${runPath.toAbsolutePath}
       |Event log directory: ${Paths.get(AppConfig.EVENT_LOG_DIR).toAbsolutePath}
       |""".stripMargin
  }

  private def latestRunText(runPath: Path, metadata: BenchmarkMetadata): String = {
    s"""benchmark_id=${metadata.benchmarkId}
       |generated_at=${metadata.generatedAt}
       |run_path=${runPath.toAbsolutePath}
       |""".stripMargin
  }

  private def metadataCsv(metadata: BenchmarkMetadata): String = {
    val header = "key,value"
    val rows = Seq(
      "benchmark_id" -> metadata.benchmarkId,
      "generated_at" -> metadata.generatedAt,
      "spark_version" -> metadata.sparkVersion,
      "app_name" -> metadata.appName,
      "master" -> metadata.master,
      "random_seed" -> metadata.randomSeed.toString,
      "sample_fraction" -> metadata.sampleFraction.toString,
      "raw_rows" -> metadata.rawRows.toString,
      "benchmark_rows" -> metadata.benchmarkRows.toString,
      "benchmark_partitions" -> metadata.benchmarkPartitions.toString,
      "warmup_runs" -> metadata.warmupRuns.toString,
      "measured_runs" -> metadata.measuredRuns.toString,
      "shuffle_partitions" -> metadata.shufflePartitions,
      "default_parallelism" -> metadata.defaultParallelism,
      "executor_memory" -> metadata.executorMemory,
      "driver_memory" -> metadata.driverMemory,
      "adaptive_enabled" -> metadata.adaptiveEnabled,
      "event_log_enabled" -> metadata.eventLogEnabled,
      "event_log_dir" -> metadata.eventLogDir
    ).map { case (k, v) =>
      Seq(k, v).map(csvEscape).mkString(",")
    }

    (header +: rows).mkString(System.lineSeparator())
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
    val header = "scenario,variant,measured_runs,warmup_runs,row_count,partition_count,avg_duration_ms,min_duration_ms,median_duration_ms,p95_duration_ms,max_duration_ms,stddev_duration_ms,cv_percent"
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
        row.maxDurationMs.toString,
        row.stddevDurationMs.toString,
        row.cvPercent.toString
      ).map(csvEscape).mkString(",")
    }

    (header +: body).mkString(System.lineSeparator())
  }

  private def csvEscape(value: String): String = {
    "\"" + value.replace("\"", "\"\"") + "\""
  }
}
