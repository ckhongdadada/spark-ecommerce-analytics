package com.ecommerce.config

/**
 * Global configuration hub.
 *
 * To adapt this project for a different course topic, prioritize editing:
 * 1) project display fields
 * 2) behavior semantics
 * 3) category semantics
 * 4) module metadata (name / enabled / includeInAll)
 */
object AppConfig {

  final case class ModuleDefinition(
    id: String,
    displayName: String,
    includeInAll: Boolean,
    enabled: Boolean = true
  )

  // Project identity
  val PROJECT_DISPLAY_NAME = sys.env.getOrElse("PROJECT_DISPLAY_NAME", "电商全路径数据分析系统")
  val PROJECT_VERSION = sys.env.getOrElse("PROJECT_VERSION", "v1.0")
  val PROJECT_DOMAIN_LABEL = sys.env.getOrElse("PROJECT_DOMAIN_LABEL", "电商")
  val APP_NAME = PROJECT_DISPLAY_NAME

  // Spark base config
  val MASTER = sys.env.getOrElse("SPARK_MASTER", "local[*]")
  val TIMEZONE = sys.env.getOrElse("SPARK_TIMEZONE", "Asia/Shanghai")

  // Domain semantics: behavior labels
  val BEHAVIOR_VIEW = sys.env.getOrElse("BEHAVIOR_VIEW", "pv")
  val BEHAVIOR_BUY = sys.env.getOrElse("BEHAVIOR_BUY", "buy")
  val BEHAVIOR_CART = sys.env.getOrElse("BEHAVIOR_CART", "cart")
  val BEHAVIOR_FAVORITE = sys.env.getOrElse("BEHAVIOR_FAVORITE", "fav")
  val VALID_BEHAVIORS: Set[String] =
    Set(BEHAVIOR_VIEW, BEHAVIOR_BUY, BEHAVIOR_CART, BEHAVIOR_FAVORITE)

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
  val DATA_ROOT = "data/mock"
  val RAW_LOG_PATH = s"$DATA_ROOT/user_behavior_log.csv"
  val CLEAN_OUTPUT_PATH = "output/cleaned"
  val SQL_OUTPUT_PATH = "output/sql_report"
  val ML_MODEL_PATH = "output/model"
  val CHECKPOINT_PATH = "output/checkpoint"
  val STREAM_OUTPUT_PATH = "output/streaming"
  val BENCHMARK_OUTPUT_PATH = "output/benchmark"
  val EVENT_LOG_DIR = "output/eventlog"

  // MySQL
  val MYSQL_URL = "jdbc:mysql://localhost:3306/ecommerce?useSSL=false&serverTimezone=UTC"
  val MYSQL_USER = sys.env.getOrElse("MYSQL_USER", "root")
  val MYSQL_PASSWORD = sys.env.getOrElse("MYSQL_PASSWORD", "123456")
  val MYSQL_TABLE = "sales_report"
  val MYSQL_BATCH_SIZE = 1000
  val MYSQL_PARTITIONS = 4

  // Streaming
  val KAFKA_BROKERS = "localhost:9092"
  val KAFKA_TOPIC = "user-behavior"
  val STREAM_WINDOW = 60
  val STREAM_SLIDE = 30
  val STREAM_TIMEOUT = 60000L
  val SOCKET_HOST = "localhost"
  val SOCKET_PORT = 9999

  // MLlib
  val ML_TRAIN_RATIO = 0.8
  val ML_TEST_RATIO = 0.2
  val ML_MAX_ITER = 100
  val ML_REG_PARAM = 0.01

  // Performance / benchmark
  val SHUFFLE_PARTITIONS = 200
  val DEFAULT_PARALLELISM = 4
  val EXECUTOR_MEMORY = "2g"
  val DRIVER_MEMORY = "1g"
  val SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  val BROADCAST_THRESHOLD_MB = 10
  val BENCHMARK_WARMUP_RUNS = 1
  val BENCHMARK_MEASURED_RUNS = 5

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
    require(PROJECT_DISPLAY_NAME.nonEmpty, "PROJECT_DISPLAY_NAME cannot be empty")
    require(VALID_BEHAVIORS.size == 4, s"VALID_BEHAVIORS should contain 4 unique values, got: ${VALID_BEHAVIORS.mkString(",")}")
    require(DOMAIN_CATEGORIES.nonEmpty, "DOMAIN_CATEGORIES cannot be empty")
    require(CATEGORY_ITEM_PREFIX.keySet == DOMAIN_CATEGORIES.toSet, "CATEGORY_ITEM_PREFIX keys must match DOMAIN_CATEGORIES")
    require(CATEGORY_ITEM_SIZE.keySet == DOMAIN_CATEGORIES.toSet, "CATEGORY_ITEM_SIZE keys must match DOMAIN_CATEGORIES")
    require(CATEGORY_DIMENSION_TAGS.keySet == DOMAIN_CATEGORIES.toSet, "CATEGORY_DIMENSION_TAGS keys must match DOMAIN_CATEGORIES")
    require(SKEW_HOT_CATEGORY.nonEmpty, "SKEW_HOT_CATEGORY cannot be empty")

    require(STREAM_WINDOW > 0, s"STREAM_WINDOW must be > 0, got: $STREAM_WINDOW")
    require(STREAM_SLIDE > 0, s"STREAM_SLIDE must be > 0, got: $STREAM_SLIDE")
    require(SOCKET_PORT > 0 && SOCKET_PORT <= 65535, s"SOCKET_PORT must be in 1..65535, got: $SOCKET_PORT")
    require(ML_TRAIN_RATIO > 0 && ML_TRAIN_RATIO < 1, s"ML_TRAIN_RATIO must be in (0,1), got: $ML_TRAIN_RATIO")
    require(BROADCAST_THRESHOLD_MB > 0, s"BROADCAST_THRESHOLD_MB must be > 0, got: $BROADCAST_THRESHOLD_MB")
    require(SHUFFLE_PARTITIONS > 0, s"SHUFFLE_PARTITIONS must be > 0, got: $SHUFFLE_PARTITIONS")
    require(DEFAULT_PARALLELISM > 0, s"DEFAULT_PARALLELISM must be > 0, got: $DEFAULT_PARALLELISM")
    require(MYSQL_BATCH_SIZE > 0, s"MYSQL_BATCH_SIZE must be > 0, got: $MYSQL_BATCH_SIZE")
    require(MYSQL_PARTITIONS > 0, s"MYSQL_PARTITIONS must be > 0, got: $MYSQL_PARTITIONS")
    require(BENCHMARK_WARMUP_RUNS >= 0, s"BENCHMARK_WARMUP_RUNS must be >= 0, got: $BENCHMARK_WARMUP_RUNS")
    require(BENCHMARK_MEASURED_RUNS > 0, s"BENCHMARK_MEASURED_RUNS must be > 0, got: $BENCHMARK_MEASURED_RUNS")

    val moduleIds = MODULE_DEFINITIONS.map(_.id)
    require(moduleIds.distinct.size == moduleIds.size, s"MODULE_DEFINITIONS contains duplicate ids: ${moduleIds.mkString(",")}")
    require(ALL_MODE_MODULE_IDS.forall(ENABLED_MODULE_IDS.contains), "ALL_MODE_MODULE_IDS must be subset of ENABLED_MODULE_IDS")
  }
}
