package com.ecommerce.module

import com.ecommerce.core.SparkSessionFactory
import org.junit.Assert.assertEquals
import org.junit.Test

import java.sql.Timestamp

class Module3StreamingTest {

  @Test
  def aggregateMetricsByWindowShouldComputePvAndUv(): Unit = {
    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    try {
      val eventDF = Seq(
        ("U1", "pv", Timestamp.valueOf("2024-01-06 10:00:05")),
        ("U1", "pv", Timestamp.valueOf("2024-01-06 10:00:20")),
        ("U2", "pv", Timestamp.valueOf("2024-01-06 10:00:40")),
        ("U2", "buy", Timestamp.valueOf("2024-01-06 10:00:50"))
      ).toDF("userId", "behavior", "event_time")

      val flattened = Module3_Streaming.flattenWindowedMetrics(
        Module3_Streaming.aggregateMetricsByWindow(eventDF, "1 minute", "1 minute")
      )

      val rowsByBehavior = flattened.collect().map { row =>
        row.getAs[String]("behavior") -> row
      }.toMap

      assertEquals(3L, rowsByBehavior("pv").getAs[Long]("pv"))
      assertEquals(2L, rowsByBehavior("pv").getAs[Long]("uv"))
      assertEquals(1L, rowsByBehavior("buy").getAs[Long]("pv"))
      assertEquals(1L, rowsByBehavior("buy").getAs[Long]("uv"))
    } finally {
      SparkSessionFactory.stop()
    }
  }
}
