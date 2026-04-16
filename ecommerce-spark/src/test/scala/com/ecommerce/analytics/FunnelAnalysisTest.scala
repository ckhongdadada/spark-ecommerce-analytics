package com.ecommerce.analytics

import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

class FunnelAnalysisTest {

  @Test
  def globalFunnelShouldUseFavoriteAsSecondStep(): Unit = {
    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    try {
      val rawDF = Seq(
        ("U1", "I1", "Electronics", Behaviors.VIEW, 1700000000L),
        ("U1", "I1", "Electronics", Behaviors.FAVORITE, 1700000010L),
        ("U1", "I1", "Electronics", Behaviors.CART, 1700000020L),
        ("U1", "I1", "Electronics", Behaviors.BUY, 1700000030L),
        ("U2", "I2", "Books", Behaviors.VIEW, 1700000040L),
        ("U3", "I3", "Food", Behaviors.VIEW, 1700000050L),
        ("U3", "I3", "Food", Behaviors.FAVORITE, 1700000060L)
      ).toDF("userId", "itemId", "category", "behavior", "timestamp")

      val row = FunnelAnalysis.buildGlobalFunnel(rawDF).head()

      assertEquals(3L, row.getAs[Long]("step1_exposure_users"))
      assertEquals(2L, row.getAs[Long]("step2_click_users"))
      assertEquals(1L, row.getAs[Long]("step3_cart_users"))
      assertEquals(1L, row.getAs[Long]("step4_buy_users"))
      assertEquals(66.67, row.getAs[Any]("step1_to_step2_rate").toString.toDouble, 0.01)
    } finally {
      SparkSessionFactory.stop()
    }
  }

  @Test
  def visualizationShouldHandleQuotedDimension(): Unit = {
    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    try {
      val funnelDF = Seq(
        ("Kid's Books", 10L, 3L, 2L, 1L, 30.0, 66.67, 50.0, 10.0)
      ).toDF(
        "dimension",
        "step1_exposure_users",
        "step2_click_users",
        "step3_cart_users",
        "step4_buy_users",
        "step1_to_step2_rate",
        "step2_to_step3_rate",
        "step3_to_step4_rate",
        "overall_conversion_rate"
      )

      val ascii = FunnelVisualization.generateASCIIFunnel(funnelDF, "Kid's Books")

      assertTrue(ascii.contains("Kid's Books"))
      assertTrue(ascii.contains("Step 2: 收藏"))
    } finally {
      SparkSessionFactory.stop()
    }
  }

  @Test
  def lossAnalysisShouldCountViewUsersWithoutFavorite(): Unit = {
    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    try {
      val rawDF = Seq(
        ("U1", "I1", "Electronics", Behaviors.VIEW, 1700000000L),
        ("U1", "I1", "Electronics", Behaviors.FAVORITE, 1700000010L),
        ("U2", "I2", "Books", Behaviors.VIEW, 1700000020L),
        ("U3", "I3", "Food", Behaviors.VIEW, 1700000030L),
        ("U3", "I3", "Food", Behaviors.FAVORITE, 1700000040L),
        ("U3", "I3", "Food", Behaviors.CART, 1700000050L)
      ).toDF("userId", "itemId", "category", "behavior", "timestamp")

      val firstLoss = FunnelAnalysis.buildLossAnalysis(rawDF)
        .collect()
        .find(_.getAs[String]("loss_stage").startsWith("Step1->Step2"))
        .get

      assertEquals(1L, firstLoss.getAs[Long]("loss_users"))
    } finally {
      SparkSessionFactory.stop()
    }
  }
}
