package com.ecommerce

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.module._
import com.ecommerce.util.{DataGenerator, Logging}

import java.io.File

/**
 * Main entry point.
 * This launcher is intentionally metadata-driven so future assignment wording changes
 * can be absorbed in AppConfig with minimal code changes.
 */
object Main extends Logging {

  private val moduleRunners: Map[String, () => Unit] = Map(
    "1" -> (() => Module1_DataPreprocessing.run()),
    "2" -> (() => Module2_SparkSQL.run()),
    "3" -> (() => Module3_Streaming.run()),
    "4" -> (() => Module4_GraphX.run()),
    "5" -> (() => Module5_MLlib.run()),
    "6" -> (() => Module6_PerformanceTuning.run())
  )

  def main(args: Array[String]): Unit = {
    AppConfig.validate()

    val mode = if (args.nonEmpty) args(0).trim.toLowerCase else "all"
    printBanner()
    ensureData()

    try {
      mode match {
        case "gen" =>
          logger.info("Data generated. Exit.")

        case "all" =>
          runAllEnabledBatchModules()

        case moduleId =>
          runSingleModule(moduleId)
      }
    } finally {
      SparkSessionFactory.stop()
    }
  }

  private def runAllEnabledBatchModules(): Unit = {
    logger.info(s"Running all configured batch modules: ${AppConfig.ALL_MODE_MODULE_IDS.mkString(", ")}")
    AppConfig.ALL_MODE_MODULE_IDS.foreach(runSingleModule)
    logger.info("Batch module run completed.")
  }

  private def runSingleModule(moduleId: String): Unit = {
    if (!AppConfig.ENABLED_MODULE_IDS.contains(moduleId)) {
      logger.warn(s"Module $moduleId is disabled by configuration, skip.")
      return
    }

    moduleRunners.get(moduleId) match {
      case Some(runFn) =>
        logger.info(s"Start module $moduleId - ${AppConfig.moduleName(moduleId)}")
        runFn()
      case None =>
        val supported = (moduleRunners.keys.toSeq.sorted :+ "all" :+ "gen").mkString(", ")
        logger.warn(s"Unknown mode '$moduleId'. Supported: $supported")
    }
  }

  private def ensureData(): Unit = {
    val dataFile = new File(AppConfig.RAW_LOG_PATH)
    if (!dataFile.exists()) {
      logger.info(s"Data file not found, generating demo data: ${AppConfig.RAW_LOG_PATH}")
      DataGenerator.generate(AppConfig.RAW_LOG_PATH)
    } else {
      logger.info(s"Data file found: ${AppConfig.RAW_LOG_PATH}")
    }
  }

  private def printBanner(): Unit = {
    val moduleSummary = AppConfig.MODULE_DEFINITIONS
      .filter(_.enabled)
      .map(m => s"${m.id}:${m.displayName}")
      .mkString(" | ")

    logger.info("=" * 72)
    logger.info(s"${AppConfig.PROJECT_DISPLAY_NAME} ${AppConfig.PROJECT_VERSION}")
    logger.info(s"Domain: ${AppConfig.PROJECT_DOMAIN_LABEL}")
    logger.info(s"Enabled modules: $moduleSummary")
    logger.info("=" * 72)
  }
}
