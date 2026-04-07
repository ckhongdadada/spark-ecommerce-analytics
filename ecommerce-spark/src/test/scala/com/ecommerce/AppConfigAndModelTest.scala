package com.ecommerce

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{CleanBehavior, UserBehaviorLog}
import org.junit.Assert.assertThrows
import org.junit.Test

class AppConfigAndModelTest {

  @Test
  def appConfigShouldPassValidation(): Unit = {
    AppConfig.validate()
  }

  @Test
  def invalidBehaviorShouldBeRejected(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () =>
      UserBehaviorLog("U1", "I1", "Electronics", "invalid", 1700000000L)
    )
  }

  @Test
  def invalidWeekendFlagShouldBeRejected(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () =>
      CleanBehavior("U1", "I1", "Electronics", "pv", 12, 2, 2)
    )
  }
}
