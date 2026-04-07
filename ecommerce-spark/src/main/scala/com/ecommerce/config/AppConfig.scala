package com.ecommerce.config

/**
 * 全局配置对象。
 * 当前项目默认仍以本地演示配置为主，但尽量将关键路径和性能参数统一收口。
 */
object AppConfig {

  // Spark 基础配置
  val APP_NAME = "电商全路径数据分析系统"
  val MASTER = "local[*]" // 部署到集群时可改为 yarn / spark://...
  val TIMEZONE = "Asia/Shanghai"

  // 数据路径配置
  val DATA_ROOT = "data/mock"
  val RAW_LOG_PATH = s"$DATA_ROOT/user_behavior_log.csv"
  val CLEAN_OUTPUT_PATH = "output/cleaned"
  val SQL_OUTPUT_PATH = "output/sql_report"
  val ML_MODEL_PATH = "output/model"
  val CHECKPOINT_PATH = "output/checkpoint"
  val STREAM_OUTPUT_PATH = "output/streaming"
  val BENCHMARK_OUTPUT_PATH = "output/benchmark"
  val EVENT_LOG_DIR = "output/eventlog"

  // MySQL 配置
  val MYSQL_URL = "jdbc:mysql://localhost:3306/ecommerce?useSSL=false&serverTimezone=UTC"
  val MYSQL_USER = sys.env.getOrElse("MYSQL_USER", "root")
  val MYSQL_PASSWORD = sys.env.getOrElse("MYSQL_PASSWORD", "123456")
  val MYSQL_TABLE = "sales_report"
  val MYSQL_BATCH_SIZE = 1000
  val MYSQL_PARTITIONS = 4

  // Streaming 配置
  val KAFKA_BROKERS = "localhost:9092"
  val KAFKA_TOPIC = "user-behavior"
  val STREAM_WINDOW = 60
  val STREAM_SLIDE = 30
  val STREAM_TIMEOUT = 60000L
  val SOCKET_HOST = "localhost"
  val SOCKET_PORT = 9999

  // MLlib 配置
  val ML_TRAIN_RATIO = 0.8
  val ML_TEST_RATIO = 0.2
  val ML_MAX_ITER = 100
  val ML_REG_PARAM = 0.01

  // 性能调优 / 基准测试配置
  val SHUFFLE_PARTITIONS = 200
  val DEFAULT_PARALLELISM = 4
  val EXECUTOR_MEMORY = "2g"
  val DRIVER_MEMORY = "1g"
  val SERIALIZER = "org.apache.spark.serializer.KryoSerializer"
  val BROADCAST_THRESHOLD_MB = 10
  val BENCHMARK_WARMUP_RUNS = 1
  val BENCHMARK_MEASURED_RUNS = 5

  def validate(): Unit = {
    require(STREAM_WINDOW > 0, s"STREAM_WINDOW 必须大于 0，当前值: $STREAM_WINDOW")
    require(STREAM_SLIDE > 0, s"STREAM_SLIDE 必须大于 0，当前值: $STREAM_SLIDE")
    require(SOCKET_PORT > 0 && SOCKET_PORT <= 65535, s"SOCKET_PORT 必须在 1-65535 范围内，当前值: $SOCKET_PORT")
    require(ML_TRAIN_RATIO > 0 && ML_TRAIN_RATIO < 1, s"ML_TRAIN_RATIO 必须在 (0, 1) 范围内，当前值: $ML_TRAIN_RATIO")
    require(BROADCAST_THRESHOLD_MB > 0, s"BROADCAST_THRESHOLD_MB 必须大于 0，当前值: $BROADCAST_THRESHOLD_MB")
    require(SHUFFLE_PARTITIONS > 0, s"SHUFFLE_PARTITIONS 必须大于 0，当前值: $SHUFFLE_PARTITIONS")
    require(DEFAULT_PARALLELISM > 0, s"DEFAULT_PARALLELISM 必须大于 0，当前值: $DEFAULT_PARALLELISM")
    require(MYSQL_BATCH_SIZE > 0, s"MYSQL_BATCH_SIZE 必须大于 0，当前值: $MYSQL_BATCH_SIZE")
    require(MYSQL_PARTITIONS > 0, s"MYSQL_PARTITIONS 必须大于 0，当前值: $MYSQL_PARTITIONS")
    require(BENCHMARK_WARMUP_RUNS >= 0, s"BENCHMARK_WARMUP_RUNS 不能小于 0，当前值: $BENCHMARK_WARMUP_RUNS")
    require(BENCHMARK_MEASURED_RUNS > 0, s"BENCHMARK_MEASURED_RUNS 必须大于 0，当前值: $BENCHMARK_MEASURED_RUNS")
  }
}
