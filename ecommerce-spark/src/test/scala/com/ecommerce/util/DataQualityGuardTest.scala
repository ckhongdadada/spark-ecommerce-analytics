package com.ecommerce.util

import com.ecommerce.core.SparkSessionFactory
import org.junit.Assert.assertEquals
import org.junit.Test

class DataQualityGuardTest {

  @Test
  def splitValidAndInvalidShouldFilterMalformedRows(): Unit = {
    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    try {
      val inputDF = Seq(
        ("U1", "I1", "Electronics", "pv", "1700000000"),
        ("U2", "I2", "Books", "buy", "1700000300"),
        ("U3", "I3", "Food", "invalid", "1700000400"),
        ("", "I4", "Beauty", "pv", "1700000500"),
        ("U5", "I5", "Sports", "pv", "-1"),
        ("U6", "I6", "", "cart", "1700000600")
      ).toDF("userId", "itemId", "category", "behavior", "timestamp")

      val (validDF, invalidDF) = DataQualityGuard.splitValidAndInvalid(inputDF)

      assertEquals(2L, validDF.count())
      assertEquals(4L, invalidDF.count())
    } finally {
      SparkSessionFactory.stop()
    }
  }
}
