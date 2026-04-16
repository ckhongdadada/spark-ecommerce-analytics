package com.ecommerce.config

import scala.util.Try

/**
 * Global configuration hub.
 *
 * Config priority:
 * 1) JVM property: -DKEY=VALUE
 * 2) Environment variable: KEY=VALUE
 * 3) Default value in code
 */
object AppConfig {

  final case class ModuleDefinition(
    id: String,
    displayName: String,
    includeInAll: Boolean,
    enabled: Boolean = true
  )

  private val SupportedEnvironments = Set("local", "dev", "prod")
  private val SupportedStreamSources = Set("socket", "kafka")
  private val SupportedStreamSinks = Set("file", "kafka", "mysql")

  private def readRaw(key: String): Option[String] = {
    sys.props.get(key).orElse(sys.env.get(key)).map(_.trim).filter(_.nonEmpty)
  }

  private def cfgString(key: String, default: String): String = {
    readRaw(key).getOrElse(default)
  }

  private def cfgInt(key: String, default: Int): Int = {
    readRaw(key).flatMap(v => Try(v.toInt).toOption).getOrElse(default)
  }

  private def cfgLong(key: String, default: Long): Long = {
    readRaw(key).flatMap(v => Try(v.toLong).toOption).getOrElse(default)
  }

  private def cfgDouble(key: String, default: Double): Double = {
    readRaw(key).flatMap(v => Try(v.toDouble).toOption).getOrElse(default)
  }

  private def byEnv(local: String, dev: String, prod: String): String = {
    APP_ENV match {
      case "prod" => prod
      case "dev" => dev
      case _ => local
    }
  }

  // Deployment profile
  val APP_ENV: String = cfgString("APP_ENV", "local").toLowerCase

  // Project identity
  val PROJECT_DISPLAY_NAME = cfgString("PROJECT_DISPLAY_NAME", "电商全路径数据分析系统")
  val PROJECT_VERSION = cfgString("PROJECT_VERSION", "v1.0")
  val PROJECT_DOMAIN_LABEL = cfgString("PROJECT_DOMAIN_LABEL", "电商")
  val APP_NAME = PROJECT_DISPLAY_NAME

  // Spark base config
  val MASTER = cfgString("SPARK_MASTER", byEnv("local[*]", "local[4]", "yarn"))
  val TIMEZONE = cfgString("SPARK_TIMEZONE", "Asia/Shanghai")

  // Domain semantics: behavior labels
  val BEHAVIOR_VIEW = cfgString("BEHAVIOR_VIEW", "pv")
  val BEHAVIOR_BUY = cfgString("BEHAVIOR_BUY", "buy")
  val BEHAVIOR_CART = cfgString("BEHAVIOR_CART", "cart")
  val BEHAVIOR_FAVORITE = cfgString("BEHAVIOR_FAVORITE", "fav")
  val VALID_BEHAVIORS: Set[String] =
    Set(BEHAVIOR_VIEW, BEHAVIOR_BUY, BEHAVIOR_CART, BEHAVIOR_FAVORITE).map(_.toLowerCase)

  // Domain semantics: categories and demo dimensions
  val DOMAIN_CATEGORIES: Vector[String] =
    Vector("Electronics", "Clothing", "Food", "Books", "Sports", "Beauty")

  // Categories used by demo filters in Module 1
  val HIGHLIGHT_CATEGORIES: Set[String] = DOMAIN_CATEGORIES.take(3).toSet

  // Category-to-item generator setup
  val CATEGORY_ITEM_PREFIX: Map[String, String] = Map(
    "Electronics" -> "E",
    "Clothing" -> "C",
    "Food" -> "F",
    "Books" -> "B",
    "Sports" -> "S",
    "Beauty" -> "BE"
  )

  val CATEGORY_ITEM_SIZE: Map[String, Int] = Map(
    "Electronics" -> 1500,
    "Clothing" -> 1500,
    "Food" -> 1200,
    "Books" -> 1200,
    "Sports" -> 1000,
    "Beauty" -> 1000
  )

  // Performance module dimension labels
  val CATEGORY_DIMENSION_TAGS: Map[String, String] = Map(
    "Electronics" -> "high traffic category",
    "Clothing" -> "fashion category",
    "Food" -> "daily category",
    "Books" -> "content category",
    "Sports" -> "fitness category",
    "Beauty" -> "personal care category"
  )

  val SKEW_HOT_CATEGORY: String = DOMAIN_CATEGORIES.headOption.getOrElse("Electronics")

  // Paths
  val DATA_ROOT = cfgString("DATA_ROOT", byEnv("data/mock", "data/mock", "/var/lib/ecommerce/data/mock"))
  val OUTPUT_ROOT = cfgString("OUTPUT_ROOT", byEnv("output", "output", "/var/lib/ecommerce/output"))

  val RAW_LOG_PATH = cfgString("RAW_LOG_PATH", s"$DATA_ROOT/user_behavior_log.csv")
  val CLEAN_OUTPUT_PATH = cfgString("CLEAN_OUTPUT_PATH", s"$OUTPUT_ROOT/cleaned")
  val SQL_OUTPUT_PATH = cfgString("SQL_OUTPUT_PATH", s"$OUTPUT_ROOT/sql_report")
  val ML_MODEL_ROOT_PATH = cfgString("ML_MODEL_ROOT_PATH", s"$OUTPUT_ROOT/model")
  val ML_MODEL_PATH = cfgString("ML_MODEL_PATH", s"$ML_MODEL_ROOT_PATH/best_model")
  val ML_MODEL_SELECTION_REPORT_PATH = cfgString("ML_MODEL_SELECTION_REPORT_PATH", s"$ML_MODEL_ROOT_PATH/model_selection_report.csv")
  val CHECKPOINT_PATH = cfgString("CHECKPOINT_PATH", s"$OUTPUT_ROOT/checkpoint")
  val STREAM_OUTPUT_PATH = cfgString("STREAM_OUTPUT_PATH", s"$OUTPUT_ROOT/streaming")
  val STREAM_INVALID_OUTPUT_PATH = cfgString("STREAM_INVALID_OUTPUT_PATH", s"$STREAM_OUTPUT_PATH/invalid_events")
  val STREAM_ALERT_OUTPUT_PATH = cfgString("STREAM_ALERT_OUTPUT_PATH", s"$STREAM_OUTPUT_PATH/quality_alerts")
  val BENCHMARK_OUTPUT_PATH = cfgString("BENCHMARK_OUTPUT_PATH", s"$OUTPUT_ROOT/benchmark")
  val DQ_OUTPUT_PATH = cfgString("DQ_OUTPUT_PATH", s"$OUTPUT_ROOT/quality")
  val EVENT_LOG_DIR = cfgString("EVENT_LOG_DIR", s"$OUTPUT_ROOT/eventlog")

  // MySQL
  val MYSQL_URL = cfgString("MYSQL_URL", "jdbc:mysql://localhost:3306/ecommerce?useSSL=false&serverTimezone=UTC")
  val MYSQL_USER = cfgString("MYSQL_USER", "root")
  val MYSQL_PASSWORD = cfgString("MYSQL_PASSWORD", "123456")
  val MYSQL_TABLE_FUNNEL = cfgString("MYSQL_TABLE_FUNNEL", "report_funnel")
  val MYSQL_TABLE_TREND = cfgString("MYSQL_TABLE_TREND", "report_hour_trend")
  val MYSQL_TABLE_TOP3 = cfgString("MYSQL_TABLE_TOP3", "report_top3_items")
  val MYSQL_TABLE_STREAM_METRICS = cfgString("MYSQL_TABLE_STREAM_METRICS", "stream_metrics")
  val MYSQL_BATCH_SIZE = cfgInt("MYSQL_BATCH_SIZE", 1000)
  val MYSQL_PARTITIONS = cfgInt("MYSQL_PARTITIONS", 4)

  // Streaming
  val KAFKA_BROKERS = cfgString("KAFKA_BROKERS", byEnv("localhost:9092", "localhost:9092", "kafka:9092"))
  val KAFKA_TOPIC = cfgString("KAFKA_TOPIC", "user-behavior")
  val KAFKA_OUTPUT_TOPIC = cfgString("KAFKA_OUTPUT_TOPIC", s"$KAFKA_TOPIC-metrics")
  val STREAM_SOURCE = cfgString("STREAM_SOURCE", byEnv("socket", "kafka", "kafka")).toLowerCase
  val STREAM_SINK = cfgString("STREAM_SINK", "file").toLowerCase
  val STREAM_WINDOW = cfgInt("STREAM_WINDOW", 60)
  val STREAM_SLIDE = cfgInt("STREAM_SLIDE", 30)
  val STREAM_TRIGGER_INTERVAL = cfgString("STREAM_TRIGGER_INTERVAL", "10 seconds")
  val STREAM_TIMEOUT = cfgLong("STREAM_TIMEOUT_MS", if (APP_ENV == "local") 120000L else 0L)
  val SOCKET_HOST = cfgString("SOCKET_HOST", "localhost")
  val SOCKET_PORT = cfgInt("SOCKET_PORT", 9999)

  // Data quality
  val DQ_MIN_REQUIRED_ROWS = cfgLong("DQ_MIN_REQUIRED_ROWS", 100)
  val DQ_MAX_INVALID_RATIO = cfgDouble("DQ_MAX_INVALID_RATIO", 0.05)
  val DQ_MIN_BUY_RATE = cfgDouble("DQ_MIN_BUY_RATE", 0.001)
  val DQ_MAX_BUY_RATE = cfgDouble("DQ_MAX_BUY_RATE", 0.80)
  val DQ_MAX_FUTURE_SECONDS = cfgLong("DQ_MAX_FUTURE_SECONDS", 86400L)
  val STREAM_ALERT_MIN_WINDOW_EVENTS = cfgLong("STREAM_ALERT_MIN_WINDOW_EVENTS", 20L)

  // MLlib
  val ML_TRAIN_RATIO = cfgDouble("ML_TRAIN_RATIO", 0.8)
  val ML_TEST_RATIO = cfgDouble("ML_TEST_RATIO", 0.2)
  val ML_RANDOM_SEED = cfgLong("ML_RANDOM_SEED", 42L)
  val ML_CV_FOLDS = cfgInt("ML_CV_FOLDS", 3)
  val ML_CV_PARALLELISM = cfgInt("ML_CV_PARALLELISM", 2)
  val ML_MAX_ITER = cfgInt("ML_MAX_ITER", 100)
  val ML_REG_PARAM = cfgDouble("ML_REG_PARAM", 0.01)
  val ML_LR_REG_PARAMS: Array[Double] = readRaw("ML_LR_REG_PARAMS")
    .map(_.split(",").toSeq.map(_.trim).filter(_.nonEmpty).flatMap(v => Try(v.toDouble).toOption).toArray)
    .filter(_.nonEmpty)
    .getOrElse(Array(0.005, 0.01, 0.05))
  val ML_LR_ELASTIC_NET_PARAMS: Array[Double] = readRaw("ML_LR_ELASTIC_NET_PARAMS")
    .map(_.split(",").toSeq.map(_.trim).filter(_.nonEmpty).flatMap(v => Try(v.toDouble).toOption).toArray)
    .filter(_.nonEmpty)
    .getOrElse(Array(0.0, 0.5))
  val ML_RF_NUM_TREES_GRID: Array[Int] = readRaw("ML_RF_NUM_TREES_GRID")
    .map(_.split(",").toSeq.map(_.trim).filter(_.nonEmpty).flatMap(v => Try(v.toInt).toOption).toArray)
    .filter(_.nonEmpty)
    .getOrElse(Array(80, 120))
  val ML_RF_MAX_DEPTH_GRID: Array[Int] = readRaw("ML_RF_MAX_DEPTH_GRID")
    .map(_.split(",").toSeq.map(_.trim).filter(_.nonEmpty).flatMap(v => Try(v.toInt).toOption).toArray)
    .filter(_.nonEmpty)
    .getOrElse(Array(5, 8))

  // Performance / benchmark
  val SHUFFLE_PARTITIONS = cfgInt("SHUFFLE_PARTITIONS", 200)
  val DEFAULT_PARALLELISM = cfgInt("DEFAULT_PARALLELISM", 4)
  val EXECUTOR_MEMORY = cfgString("EXECUTOR_MEMORY", "2g")
  val DRIVER_MEMORY = cfgString("DRIVER_MEMORY", "1g")
  val SERIALIZER = cfgString("SERIALIZER", "org.apache.spark.serializer.KryoSerializer")
  val BROADCAST_THRESHOLD_MB = cfgInt("BROADCAST_THRESHOLD_MB", 10)
  val BENCHMARK_WARMUP_RUNS = cfgInt("BENCHMARK_WARMUP_RUNS", 1)
  val BENCHMARK_MEASURED_RUNS = cfgInt("BENCHMARK_MEASURED_RUNS", 5)
  val BENCHMARK_RANDOM_SEED = cfgLong("BENCHMARK_RANDOM_SEED", 42L)
  val BENCHMARK_SAMPLE_FRACTION = cfgDouble("BENCHMARK_SAMPLE_FRACTION", 1.0)
  val BENCHMARK_BASELINE_PARTITIONS = cfgInt("BENCHMARK_BASELINE_PARTITIONS", DEFAULT_PARALLELISM)

  // Module catalog: adjust names/switches here to match final assignment wording
  val MODULE_DEFINITIONS: Seq[ModuleDefinition] = Seq(
    ModuleDefinition("1", "RDD 数据预处理", includeInAll = true),
    ModuleDefinition("2", "Spark SQL 报表分析", includeInAll = true),
    ModuleDefinition("3", "Structured Streaming 实时统计", includeInAll = false),
    ModuleDefinition("4", "GraphX 图分析", includeInAll = true),
    ModuleDefinition("5", "MLlib 行为预测", includeInAll = true),
    ModuleDefinition("6", "性能调优与 Benchmark", includeInAll = true)
  )

  val MODULE_NAME_BY_ID: Map[String, String] =
    MODULE_DEFINITIONS.map(m => m.id -> m.displayName).toMap

  val ENABLED_MODULE_IDS: Set[String] =
    MODULE_DEFINITIONS.filter(_.enabled).map(_.id).toSet

  val ALL_MODE_MODULE_IDS: Seq[String] =
    MODULE_DEFINITIONS.filter(m => m.enabled && m.includeInAll).map(_.id)

  def moduleName(id: String): String = MODULE_NAME_BY_ID.getOrElse(id, s"Module-$id")

  def validate(): Unit = {
    require(SupportedEnvironments.contains(APP_ENV), s"APP_ENV must be one of ${SupportedEnvironments.mkString(",")}, got: $APP_ENV")
    require(PROJECT_DISPLAY_NAME.nonEmpty, "PROJECT_DISPLAY_NAME cannot be empty")
    require(VALID_BEHAVIORS.size == 4, s"VALID_BEHAVIORS should contain 4 unique values, got: ${VALID_BEHAVIORS.mkString(",")}")
    require(DOMAIN_CATEGORIES.nonEmpty, "DOMAIN_CATEGORIES cannot be empty")
    require(CATEGORY_ITEM_PREFIX.keySet == DOMAIN_CATEGORIES.toSet, "CATEGORY_ITEM_PREFIX keys must match DOMAIN_CATEGORIES")
    require(CATEGORY_ITEM_SIZE.keySet == DOMAIN_CATEGORIES.toSet, "CATEGORY_ITEM_SIZE keys must match DOMAIN_CATEGORIES")
    require(CATEGORY_DIMENSION_TAGS.keySet == DOMAIN_CATEGORIES.toSet, "CATEGORY_DIMENSION_TAGS keys must match DOMAIN_CATEGORIES")
    require(SKEW_HOT_CATEGORY.nonEmpty, "SKEW_HOT_CATEGORY cannot be empty")

    require(SupportedStreamSources.contains(STREAM_SOURCE), s"STREAM_SOURCE must be one of ${SupportedStreamSources.mkString(",")}, got: $STREAM_SOURCE")
    require(SupportedStreamSinks.contains(STREAM_SINK), s"STREAM_SINK must be one of ${SupportedStreamSinks.mkString(",")}, got: $STREAM_SINK")
    require(STREAM_WINDOW > 0, s"STREAM_WINDOW must be > 0, got: $STREAM_WINDOW")
    require(STREAM_SLIDE > 0, s"STREAM_SLIDE must be > 0, got: $STREAM_SLIDE")
    require(STREAM_TRIGGER_INTERVAL.nonEmpty, "STREAM_TRIGGER_INTERVAL cannot be empty")
    require(STREAM_TIMEOUT >= 0, s"STREAM_TIMEOUT must be >= 0, got: $STREAM_TIMEOUT")
    require(SOCKET_PORT > 0 && SOCKET_PORT <= 65535, s"SOCKET_PORT must be in 1..65535, got: $SOCKET_PORT")

    require(DQ_MIN_REQUIRED_ROWS >= 0, s"DQ_MIN_REQUIRED_ROWS must be >= 0, got: $DQ_MIN_REQUIRED_ROWS")
    require(DQ_MAX_INVALID_RATIO >= 0.0 && DQ_MAX_INVALID_RATIO <= 1.0, s"DQ_MAX_INVALID_RATIO must be in [0,1], got: $DQ_MAX_INVALID_RATIO")
    require(DQ_MIN_BUY_RATE >= 0.0 && DQ_MIN_BUY_RATE <= 1.0, s"DQ_MIN_BUY_RATE must be in [0,1], got: $DQ_MIN_BUY_RATE")
    require(DQ_MAX_BUY_RATE >= 0.0 && DQ_MAX_BUY_RATE <= 1.0, s"DQ_MAX_BUY_RATE must be in [0,1], got: $DQ_MAX_BUY_RATE")
    require(DQ_MIN_BUY_RATE <= DQ_MAX_BUY_RATE, s"DQ_MIN_BUY_RATE must be <= DQ_MAX_BUY_RATE, got: $DQ_MIN_BUY_RATE > $DQ_MAX_BUY_RATE")
    require(DQ_MAX_FUTURE_SECONDS > 0, s"DQ_MAX_FUTURE_SECONDS must be > 0, got: $DQ_MAX_FUTURE_SECONDS")
    require(STREAM_ALERT_MIN_WINDOW_EVENTS >= 0, s"STREAM_ALERT_MIN_WINDOW_EVENTS must be >= 0, got: $STREAM_ALERT_MIN_WINDOW_EVENTS")

    require(ML_TRAIN_RATIO > 0 && ML_TRAIN_RATIO < 1, s"ML_TRAIN_RATIO must be in (0,1), got: $ML_TRAIN_RATIO")
    require(ML_TEST_RATIO > 0 && ML_TEST_RATIO < 1, s"ML_TEST_RATIO must be in (0,1), got: $ML_TEST_RATIO")
    require(math.abs((ML_TRAIN_RATIO + ML_TEST_RATIO) - 1.0) <= 1e-9, s"ML_TRAIN_RATIO + ML_TEST_RATIO must equal 1.0, got: ${ML_TRAIN_RATIO + ML_TEST_RATIO}")
    require(ML_CV_FOLDS >= 2, s"ML_CV_FOLDS must be >= 2, got: $ML_CV_FOLDS")
    require(ML_CV_PARALLELISM > 0, s"ML_CV_PARALLELISM must be > 0, got: $ML_CV_PARALLELISM")
    require(ML_LR_REG_PARAMS.nonEmpty, "ML_LR_REG_PARAMS cannot be empty")
    require(ML_LR_ELASTIC_NET_PARAMS.nonEmpty, "ML_LR_ELASTIC_NET_PARAMS cannot be empty")
    require(ML_RF_NUM_TREES_GRID.nonEmpty, "ML_RF_NUM_TREES_GRID cannot be empty")
    require(ML_RF_MAX_DEPTH_GRID.nonEmpty, "ML_RF_MAX_DEPTH_GRID cannot be empty")

    require(BROADCAST_THRESHOLD_MB > 0, s"BROADCAST_THRESHOLD_MB must be > 0, got: $BROADCAST_THRESHOLD_MB")
    require(SHUFFLE_PARTITIONS > 0, s"SHUFFLE_PARTITIONS must be > 0, got: $SHUFFLE_PARTITIONS")
    require(DEFAULT_PARALLELISM > 0, s"DEFAULT_PARALLELISM must be > 0, got: $DEFAULT_PARALLELISM")
    require(MYSQL_BATCH_SIZE > 0, s"MYSQL_BATCH_SIZE must be > 0, got: $MYSQL_BATCH_SIZE")
    require(MYSQL_PARTITIONS > 0, s"MYSQL_PARTITIONS must be > 0, got: $MYSQL_PARTITIONS")
    require(BENCHMARK_WARMUP_RUNS >= 0, s"BENCHMARK_WARMUP_RUNS must be >= 0, got: $BENCHMARK_WARMUP_RUNS")
    require(BENCHMARK_MEASURED_RUNS > 0, s"BENCHMARK_MEASURED_RUNS must be > 0, got: $BENCHMARK_MEASURED_RUNS")
    require(BENCHMARK_SAMPLE_FRACTION > 0.0 && BENCHMARK_SAMPLE_FRACTION <= 1.0, s"BENCHMARK_SAMPLE_FRACTION must be in (0,1], got: $BENCHMARK_SAMPLE_FRACTION")
    require(BENCHMARK_BASELINE_PARTITIONS > 0, s"BENCHMARK_BASELINE_PARTITIONS must be > 0, got: $BENCHMARK_BASELINE_PARTITIONS")

    val moduleIds = MODULE_DEFINITIONS.map(_.id)
    require(moduleIds.distinct.size == moduleIds.size, s"MODULE_DEFINITIONS contains duplicate ids: ${moduleIds.mkString(",")}")
    require(ALL_MODE_MODULE_IDS.forall(ENABLED_MODULE_IDS.contains), "ALL_MODE_MODULE_IDS must be subset of ENABLED_MODULE_IDS")
  }
}
