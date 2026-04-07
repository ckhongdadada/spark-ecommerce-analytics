package com.ecommerce

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import org.junit.Assert.assertEquals
import org.junit.Test

class SparkSessionFactoryTest {

  @Test
  def sparkSessionShouldUseConfiguredTimezone(): Unit = {
    val spark = SparkSessionFactory.getSession()
    try {
      assertEquals(AppConfig.TIMEZONE, spark.conf.get("spark.sql.session.timeZone"))
    } finally {
      SparkSessionFactory.stop()
    }
  }
}
