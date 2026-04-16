package com.ecommerce.analytics

import com.ecommerce.util.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.io.{File, PrintWriter}
import java.nio.charset.StandardCharsets

/**
 * 漏斗可视化工具
 * 
 * 生成适合前端展示的漏斗图数据和 ASCII 艺术漏斗图
 */
object FunnelVisualization extends Logging {

  /**
   * 生成 ASCII 艺术漏斗图
   * 用于命令行展示
   */
  def generateASCIIFunnel(funnelDF: DataFrame, dimension: String = "Global"): String = {
    val data = funnelDF
      .filter(col("dimension") === lit(dimension))
      .select(
        "step1_exposure_users",
        "step2_click_users",
        "step3_cart_users",
        "step4_buy_users",
        "step1_to_step2_rate",
        "step2_to_step3_rate",
        "step3_to_step4_rate",
        "overall_conversion_rate"
      )
      .collect()

    if (data.isEmpty) {
      return s"No data found for dimension: $dimension"
    }

    val row = data.head
    val step1 = row.getLong(0)
    val step2 = row.getLong(1)
    val step3 = row.getLong(2)
    val step4 = row.getLong(3)
    val rate12 = if (row.isNullAt(4)) 0.0 else row.getDouble(4)
    val rate23 = if (row.isNullAt(5)) 0.0 else row.getDouble(5)
    val rate34 = if (row.isNullAt(6)) 0.0 else row.getDouble(6)
    val overall = if (row.isNullAt(7)) 0.0 else row.getDouble(7)

    val maxWidth = 80
    val step1Width = maxWidth
    val step2Width = if (step1 > 0) (step2.toDouble / step1 * maxWidth).toInt else 0
    val step3Width = if (step1 > 0) (step3.toDouble / step1 * maxWidth).toInt else 0
    val step4Width = if (step1 > 0) (step4.toDouble / step1 * maxWidth).toInt else 0

    val sb = new StringBuilder
    sb.append("\n")
    sb.append("=" * 100).append("\n")
    sb.append(s"  用户行为漏斗分析 - $dimension\n")
    sb.append("=" * 100).append("\n\n")

    // Step 1: 曝光
    sb.append("┌").append("─" * step1Width).append("┐\n")
    sb.append("│").append(" " * ((step1Width - 20) / 2))
      .append(f"Step 1: 浏览 ($step1%,d 用户)")
      .append(" " * ((step1Width - 20) / 2)).append("│\n")
    sb.append("└").append("─" * step1Width).append("┘\n")
    sb.append("  " * ((step1Width - 20) / 2)).append(s"↓ 转化率: $rate12%.2f%%\n\n")

    // Step 2: 点击
    val padding2 = (step1Width - step2Width) / 2
    sb.append(" " * padding2).append("┌").append("─" * step2Width).append("┐\n")
    sb.append(" " * padding2).append("│").append(" " * ((step2Width - 20) / 2))
      .append(f"Step 2: 收藏 ($step2%,d 用户)")
      .append(" " * ((step2Width - 20) / 2)).append("│\n")
    sb.append(" " * padding2).append("└").append("─" * step2Width).append("┘\n")
    sb.append(" " * ((step1Width - 20) / 2)).append(s"↓ 转化率: $rate23%.2f%%\n\n")

    // Step 3: 加购
    val padding3 = (step1Width - step3Width) / 2
    sb.append(" " * padding3).append("┌").append("─" * step3Width).append("┐\n")
    sb.append(" " * padding3).append("│").append(" " * ((step3Width - 20) / 2))
      .append(f"Step 3: 加购 ($step3%,d 用户)")
      .append(" " * ((step3Width - 20) / 2)).append("│\n")
    sb.append(" " * padding3).append("└").append("─" * step3Width).append("┘\n")
    sb.append(" " * ((step1Width - 20) / 2)).append(s"↓ 转化率: $rate34%.2f%%\n\n")

    // Step 4: 支付
    val padding4 = (step1Width - step4Width) / 2
    sb.append(" " * padding4).append("┌").append("─" * step4Width).append("┐\n")
    sb.append(" " * padding4).append("│").append(" " * ((step4Width - 20) / 2))
      .append(f"Step 4: 支付 ($step4%,d 用户)")
      .append(" " * ((step4Width - 20) / 2)).append("│\n")
    sb.append(" " * padding4).append("└").append("─" * step4Width).append("┘\n\n")

    sb.append("=" * 100).append("\n")
    sb.append(f"  整体转化率: $overall%.2f%%\n")
    sb.append("=" * 100).append("\n")

    sb.toString()
  }

  /**
   * 生成 ECharts 配置 JSON
   * 用于前端漏斗图展示
   */
  def generateEChartsConfig(funnelDF: DataFrame, dimension: String = "Global"): String = {
    val data = funnelDF
      .filter(col("dimension") === lit(dimension))
      .select(
        "step1_exposure_users",
        "step2_click_users",
        "step3_cart_users",
        "step4_buy_users"
      )
      .collect()

    if (data.isEmpty) {
      return "{}"
    }

    val row = data.head
    val step1 = row.getLong(0)
    val step2 = row.getLong(1)
    val step3 = row.getLong(2)
    val step4 = row.getLong(3)

    s"""
       |{
       |  "title": {
       |    "text": "用户行为漏斗分析 - $dimension",
       |    "left": "center"
       |  },
       |  "tooltip": {
       |    "trigger": "item",
       |    "formatter": "{b}: {c} 用户 ({d}%)"
       |  },
       |  "series": [
       |    {
       |      "name": "用户行为漏斗",
       |      "type": "funnel",
       |      "left": "10%",
       |      "width": "80%",
       |      "label": {
       |        "show": true,
       |        "position": "inside",
       |        "formatter": "{b}: {c}"
       |      },
       |      "labelLine": {
       |        "show": false
       |      },
       |      "itemStyle": {
       |        "borderColor": "#fff",
       |        "borderWidth": 1
       |      },
       |      "emphasis": {
       |        "label": {
       |          "fontSize": 20
       |        }
       |      },
       |      "data": [
       |        { "value": $step1, "name": "浏览" },
       |        { "value": $step2, "name": "收藏" },
       |        { "value": $step3, "name": "加购" },
       |        { "value": $step4, "name": "支付" }
       |      ]
       |    }
       |  ]
       |}
       |""".stripMargin
  }

  /**
   * 保存可视化配置到文件
   */
  def saveVisualizationConfigs(
    globalFunnel: DataFrame,
    categoryFunnel: DataFrame,
    outputPath: String
  ): Unit = {
    logger.info(s"Saving visualization configs to $outputPath...")

    val outputDir = new File(outputPath)
    if (!outputDir.exists()) {
      outputDir.mkdirs()
    }

    // 保存全局漏斗 ASCII 图
    val globalASCII = generateASCIIFunnel(globalFunnel, "Global")
    val asciiFile = new File(s"$outputPath/funnel_ascii.txt")
    val asciiWriter = new PrintWriter(asciiFile, StandardCharsets.UTF_8.name())
    try {
      asciiWriter.write(globalASCII)
      logger.info(s"ASCII funnel saved to ${asciiFile.getAbsolutePath}")
    } finally {
      asciiWriter.close()
    }

    // 保存全局漏斗 ECharts 配置
    val globalECharts = generateEChartsConfig(globalFunnel, "Global")
    val echartsFile = new File(s"$outputPath/funnel_echarts_global.json")
    val echartsWriter = new PrintWriter(echartsFile, StandardCharsets.UTF_8.name())
    try {
      echartsWriter.write(globalECharts)
      logger.info(s"ECharts config saved to ${echartsFile.getAbsolutePath}")
    } finally {
      echartsWriter.close()
    }

    // 为每个类目生成 ECharts 配置
    val categories = categoryFunnel.select("dimension").distinct().collect().map(_.getString(0))
    categories.foreach { category =>
      val categoryECharts = generateEChartsConfig(categoryFunnel, category)
      val categoryFile = new File(s"$outputPath/funnel_echarts_${sanitizeFileName(category)}.json")
      val categoryWriter = new PrintWriter(categoryFile, StandardCharsets.UTF_8.name())
      try {
        categoryWriter.write(categoryECharts)
      } finally {
        categoryWriter.close()
      }
    }

    logger.info(s"Generated ${categories.length} category funnel configs")
  }

  /**
   * 打印漏斗分析摘要
   */
  def printFunnelSummary(
    globalFunnel: DataFrame,
    categoryFunnel: DataFrame,
    lossAnalysis: DataFrame
  ): Unit = {
    logger.info("\n" + "=" * 100)
    logger.info("  漏斗分析摘要报告")
    logger.info("=" * 100)

    // 打印全局漏斗 ASCII 图
    val asciiArt = generateASCIIFunnel(globalFunnel, "Global")
    println(asciiArt)

    // 打印流失分析
    logger.info("\n流失分析:")
    lossAnalysis.show(truncate = false)

    // 打印各类目转化率对比
    logger.info("\n各类目整体转化率对比:")
    categoryFunnel
      .select("dimension", "overall_conversion_rate")
      .orderBy(col("overall_conversion_rate").desc)
      .show(truncate = false)
  }

  private def sanitizeFileName(value: String): String = {
    value.replaceAll("[^\\p{L}\\p{N}_-]", "_")
  }
}
