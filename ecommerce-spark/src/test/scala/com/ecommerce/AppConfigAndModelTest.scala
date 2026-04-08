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
  def moduleCatalogShouldBeConsistent(): Unit = {
    val ids = AppConfig.MODULE_DEFINITIONS.map(_.id)
    org.junit.Assert.assertEquals(ids.size, ids.distinct.size)
    org.junit.Assert.assertTrue(AppConfig.ALL_MODE_MODULE_IDS.forall(AppConfig.ENABLED_MODULE_IDS.contains))
  }

  @Test
  def invalidBehaviorShouldBeRejected(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () =>
      UserBehaviorLog("U1", "I1", AppConfig.DOMAIN_CATEGORIES.head, "invalid", 1700000000L)
    )
  }

  @Test
  def invalidWeekendFlagShouldBeRejected(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () =>
      CleanBehavior("U1", "I1", AppConfig.DOMAIN_CATEGORIES.head, AppConfig.BEHAVIOR_VIEW, 12, 2, 2)
    )
  }
}
