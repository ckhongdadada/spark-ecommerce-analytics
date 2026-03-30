package com.ecommerce.config

/**
 * 全局配置对象
 * 所有模块共享的配置项统一在此管理，方便期末作业按需修改
 */
object AppConfig {

  // ─────────────── Spark 基础配置 ───────────────
  val APP_NAME     = "电商全路径数据分析系统"
  val MASTER       = "local[*]"          // 提交到集群时改为 yarn
  val TIMEZONE     = "Asia/Shanghai"     // 时区配置

  // ─────────────── 数据路径配置 ───────────────
  val DATA_ROOT         = "data/mock"
  val RAW_LOG_PATH      = s"$DATA_ROOT/user_behavior_log.csv"
  val CLEAN_OUTPUT_PATH = "output/cleaned"
  val SQL_OUTPUT_PATH   = "output/sql_report"
  val ML_MODEL_PATH     = "output/model"
  val CHECKPOINT_PATH   = "output/checkpoint"  // Streaming Checkpoint 路径

  // ─────────────── MySQL 配置（模块二写出结果用）───────────────
  val MYSQL_URL         = "jdbc:mysql://localhost:3306/ecommerce?useSSL=false&serverTimezone=UTC"
  val MYSQL_USER        = sys.env.getOrElse("MYSQL_USER", "root")
  val MYSQL_PASSWORD    = sys.env.getOrElse("MYSQL_PASSWORD", "123456")
  val MYSQL_TABLE       = "sales_report"
  val MYSQL_BATCH_SIZE  = 1000    // 批量写入大小
  val MYSQL_PARTITIONS  = 4       // 并行写入分区数

  // ─────────────── Streaming 配置 ───────────────
  val KAFKA_BROKERS    = "localhost:9092"
  val KAFKA_TOPIC      = "user-behavior"
  val STREAM_WINDOW    = 60    // 滑动窗口大小（秒）
  val STREAM_SLIDE     = 30    // 滑动步长（秒）
  val STREAM_TIMEOUT   = 60000 // Streaming 超时时间（毫秒）
  val SOCKET_HOST      = "localhost"
  val SOCKET_PORT      = 9999

  // ─────────────── MLlib 配置 ───────────────
  val ML_TRAIN_RATIO = 0.8
  val ML_TEST_RATIO  = 0.2
  val ML_MAX_ITER    = 100
  val ML_REG_PARAM   = 0.01

  // ─────────────── 性能调优配置（第4章）───────────────
  val SHUFFLE_PARTITIONS      = 200   // spark.sql.shuffle.partitions
  val DEFAULT_PARALLELISM     = 4     // spark.default.parallelism
  val EXECUTOR_MEMORY         = "2g"
  val DRIVER_MEMORY           = "1g"
  val SERIALIZER              = "org.apache.spark.serializer.KryoSerializer"
  val BROADCAST_THRESHOLD_MB  = 10    // 广播阈值(MB)

  // ─────────────── 配置验证 ───────────────
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
  }
}
