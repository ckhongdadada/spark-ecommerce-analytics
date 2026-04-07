package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import org.junit.Assert.{assertEquals, assertNotNull}
import org.junit.Test

import java.time.{ZoneId, ZonedDateTime}

class Module1DataPreprocessingTest {

  @Test
  def parseAndEnrichShouldFilterInvalidRowsAndDeriveTimeFeatures(): Unit = {
    val spark = SparkSessionFactory.getSession()
    val sc = spark.sparkContext

    try {
      val saturdayTs = ZonedDateTime.of(2024, 1, 6, 10, 15, 0, 0, ZoneId.of(AppConfig.TIMEZONE)).toEpochSecond
      val mondayTs = ZonedDateTime.of(2024, 1, 8, 9, 30, 0, 0, ZoneId.of(AppConfig.TIMEZONE)).toEpochSecond

      val dataRDD = sc.parallelize(Seq(
        s"U1,I1,Electronics,pv,$saturdayTs",
        s"U2,I2,Books,buy,$mondayTs",
        s"U3,I3,Food,invalid,$mondayTs",
        "broken,row",
        "U4,I4,Beauty,pv,not_a_number"
      ))

      val cleanRows = Module1_DataPreprocessing
        .enrichCleanBehavior(Module1_DataPreprocessing.parseLogs(dataRDD))
        .collect()
        .sortBy(_.userId)

      assertEquals(2, cleanRows.length)

      val weekendRow = cleanRows.find(_.userId == "U1").orNull
      assertNotNull(weekendRow)
      assertEquals(10, weekendRow.hour)
      assertEquals(6, weekendRow.dayOfWeek)
      assertEquals(1, weekendRow.isWeekend)

      val weekdayRow = cleanRows.find(_.userId == "U2").orNull
      assertNotNull(weekdayRow)
      assertEquals(9, weekdayRow.hour)
      assertEquals(1, weekdayRow.dayOfWeek)
      assertEquals(0, weekdayRow.isWeekend)
    } finally {
      SparkSessionFactory.stop()
    }
  }
}
