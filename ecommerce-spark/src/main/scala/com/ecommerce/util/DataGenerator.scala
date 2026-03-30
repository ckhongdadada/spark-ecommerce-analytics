package com.ecommerce.util

import java.io.{File, PrintWriter}
import scala.util.Random

/**
 * 模拟数据生成器
 * 在没有真实数据集时自动生成测试用 CSV，方便本地调试
 * 运行命令：spark-submit ... --class com.ecommerce.util.DataGenerator
 */
object DataGenerator {

  val rng       = new Random(42)
  val behaviors = Array("pv", "pv", "pv", "cart", "fav", "buy")  // pv占比高模拟真实分布
  val categories = Array("Electronics", "Clothing", "Food", "Books", "Sports", "Beauty")
  val BASE_TS   = 1700000000L  // 2023-11-14 基准时间戳

  def generate(outputPath: String, rowCount: Int = 100000): Unit = {
    println(s"[DataGenerator] 生成 $rowCount 条模拟数据 → $outputPath")
    new File(outputPath).getParentFile.mkdirs()

    val pw = new PrintWriter(new File(outputPath))
    pw.println("userId,itemId,category,behavior,timestamp")

    (1 to rowCount).foreach { _ =>
      val userId    = s"U${rng.nextInt(5000) + 1}"
      val itemId    = s"I${rng.nextInt(20000) + 1}"
      val category  = categories(rng.nextInt(categories.length))
      val behavior  = behaviors(rng.nextInt(behaviors.length))
      val timestamp = BASE_TS + rng.nextInt(7 * 24 * 3600)  // 近7天随机时间
      pw.println(s"$userId,$itemId,$category,$behavior,$timestamp")
    }

    pw.close()
    println("[DataGenerator] 完成！")
  }

  def main(args: Array[String]): Unit = {
    val path = if (args.nonEmpty) args(0) else "data/mock/user_behavior_log.csv"
    val rows = if (args.length > 1) args(1).toInt else 100000
    generate(path, rows)
  }
}
