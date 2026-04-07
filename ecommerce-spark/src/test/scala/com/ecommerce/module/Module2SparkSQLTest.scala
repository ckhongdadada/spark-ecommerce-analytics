package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import org.apache.spark.sql.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import java.time.{ZoneId, ZonedDateTime}

class Module2SparkSQLTest {

  @Test
  def funnelReportShouldComputeCountsAndConversionRate(): Unit = {
    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    try {
      val ts = ZonedDateTime.of(2024, 1, 6, 10, 0, 0, 0, ZoneId.of(AppConfig.TIMEZONE)).toEpochSecond

      val rawDF = Seq(
        ("U1", "I1", "Electronics", "pv", ts),
        ("U1", "I1", "Electronics", "buy", ts),
        ("U2", "I2", "Electronics", "pv", ts),
        ("U3", "I3", "Books", "pv", ts),
        ("U3", "I3", "Books", "cart", ts)
      ).toDF("userId", "itemId", "category", "behavior", "timestamp")

      val rowsByCategory = Module2_SparkSQL.buildFunnelReport(rawDF).collect().map { row =>
        row.getAs[String]("category") -> row
      }.toMap

      assertEquals(2L, rowsByCategory("Electronics").getAs[Long]("pv_cnt"))
      assertEquals(0L, rowsByCategory("Electronics").getAs[Long]("cart_cnt"))
      assertEquals(1L, rowsByCategory("Electronics").getAs[Long]("buy_cnt"))
      assertEquals(50.0, decimalAsDouble(rowsByCategory("Electronics"), "conv_rate_pct"), 0.001)

      assertEquals(1L, rowsByCategory("Books").getAs[Long]("pv_cnt"))
      assertEquals(1L, rowsByCategory("Books").getAs[Long]("cart_cnt"))
      assertEquals(0L, rowsByCategory("Books").getAs[Long]("buy_cnt"))
      assertEquals(0.0, decimalAsDouble(rowsByCategory("Books"), "conv_rate_pct"), 0.001)
    } finally {
      SparkSessionFactory.stop()
    }
  }

  @Test
  def hourTrendShouldAggregatePvAndUvByHour(): Unit = {
    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    try {
      val hour10A = ZonedDateTime.of(2024, 1, 6, 10, 5, 0, 0, ZoneId.of(AppConfig.TIMEZONE)).toEpochSecond
      val hour10B = ZonedDateTime.of(2024, 1, 6, 10, 25, 0, 0, ZoneId.of(AppConfig.TIMEZONE)).toEpochSecond
      val hour10C = ZonedDateTime.of(2024, 1, 6, 10, 45, 0, 0, ZoneId.of(AppConfig.TIMEZONE)).toEpochSecond
      val hour11 = ZonedDateTime.of(2024, 1, 6, 11, 0, 0, 0, ZoneId.of(AppConfig.TIMEZONE)).toEpochSecond

      val rawDF = Seq(
        ("U1", "I1", "Electronics", "pv", hour10A),
        ("U1", "I2", "Electronics", "pv", hour10B),
        ("U2", "I3", "Books", "pv", hour10C),
        ("U3", "I4", "Beauty", "pv", hour11),
        ("U3", "I4", "Beauty", "buy", hour11)
      ).toDF("userId", "itemId", "category", "behavior", "timestamp")

      val rowsByHour = Module2_SparkSQL.buildHourTrend(rawDF).collect().map { row =>
        row.getAs[Int]("hour") -> row
      }.toMap

      assertEquals(3L, rowsByHour(10).getAs[Long]("pv"))
      assertEquals(2L, rowsByHour(10).getAs[Long]("uv"))
      assertEquals(1L, rowsByHour(11).getAs[Long]("pv"))
      assertEquals(1L, rowsByHour(11).getAs[Long]("uv"))
    } finally {
      SparkSessionFactory.stop()
    }
  }

  private def decimalAsDouble(row: Row, column: String): Double = {
    Option(row.getAs[Any](column)).map(_.toString.toDouble).getOrElse(0.0)
  }
}
