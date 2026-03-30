package com.ecommerce

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.module._
import com.ecommerce.util.{DataGenerator, Logging}

/**
 * ══════════════════════════════════════════════════════════
 * 电商全路径数据分析系统 —— 主入口
 *
 * 支持按模块单独运行，方便期末大作业分模块提交演示
 *
 * 用法：
 *   spark-submit --class com.ecommerce.Main xxx.jar [模块编号]
 *
 *   参数说明：
 *     all   运行全部模块（默认）
 *     gen   仅生成模拟数据
 *     1     模块一：RDD 数据预处理
 *     2     模块二：Spark SQL 业务报表
 *     3     模块三：Streaming 实时监控
 *     4     模块四：GraphX 社交挖掘
 *     5     模块五：MLlib 行为预测
 *     6     模块六：性能调优
 *
 * 示例：
 *   spark-submit --class com.ecommerce.Main target/xxx.jar 2
 * ══════════════════════════════════════════════════════════
 */
object Main extends Logging {

  def main(args: Array[String]): Unit = {
    AppConfig.validate()

    val mode = if (args.nonEmpty) args(0).trim.toLowerCase else "all"

    printBanner()

    // 确保数据文件存在
    ensureData()

    try {
      mode match {
        case "gen" =>
          logger.info("数据生成完毕，退出。")

        case "1" =>
          Module1_DataPreprocessing.run()

        case "2" =>
          Module2_SparkSQL.run()

        case "3" =>
          Module3_Streaming.run()

        case "4" =>
          Module4_GraphX.run()

        case "5" =>
          Module5_MLlib.run()

        case "6" =>
          Module6_PerformanceTuning.run()

        case "all" | _ =>
          logger.info("运行全部模块...")
          Module1_DataPreprocessing.run()
          Module2_SparkSQL.run()
          // Module3_Streaming.run()   // Streaming 需要持续运行，默认注释
          Module4_GraphX.run()
          Module5_MLlib.run()
          Module6_PerformanceTuning.run()
          logger.info("全部模块执行完毕 🎉")
      }
    } finally {
      SparkSessionFactory.stop()
    }
  }

  private def ensureData(): Unit = {
    val dataFile = new java.io.File(AppConfig.RAW_LOG_PATH)
    if (!dataFile.exists()) {
      logger.info(s"未找到数据文件，自动生成模拟数据...")
      DataGenerator.generate(AppConfig.RAW_LOG_PATH)
    } else {
      logger.info(s"数据文件已存在: ${AppConfig.RAW_LOG_PATH}")
    }
  }

  private def printBanner(): Unit = {
    logger.info(
      """
        |╔══════════════════════════════════════════════════════════╗
        |║         电商全路径数据分析系统  v1.0                      ║
        |║  Spark全栈: RDD / SQL / Streaming / GraphX / MLlib       ║
        |║  第4章 性能调优: Cache / Broadcast / Skew / Partition     ║
        |╚══════════════════════════════════════════════════════════╝
      """.stripMargin)
  }
}
