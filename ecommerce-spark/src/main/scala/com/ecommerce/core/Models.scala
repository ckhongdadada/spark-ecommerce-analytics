package com.ecommerce.core

import com.ecommerce.config.AppConfig

/**
 * Shared domain models.
 */
object Behaviors {
  val VIEW: String = AppConfig.BEHAVIOR_VIEW
  val BUY: String = AppConfig.BEHAVIOR_BUY
  val CART: String = AppConfig.BEHAVIOR_CART
  val FAVORITE: String = AppConfig.BEHAVIOR_FAVORITE
  val VALID_BEHAVIORS: Set[String] = AppConfig.VALID_BEHAVIORS
}

case class UserBehaviorLog(
  userId: String,
  itemId: String,
  category: String,
  behavior: String,
  timestamp: Long
) {
  require(userId.nonEmpty, "userId cannot be empty")
  require(itemId.nonEmpty, "itemId cannot be empty")
  require(category.nonEmpty, "category cannot be empty")
  require(
    Behaviors.VALID_BEHAVIORS.contains(behavior),
    s"behavior must be one of ${Behaviors.VALID_BEHAVIORS.mkString(",")}, got: $behavior"
  )
  require(timestamp > 0, s"timestamp must be > 0, got: $timestamp")
}

case class CleanBehavior(
  userId: String,
  itemId: String,
  category: String,
  behavior: String,
  hour: Int,
  dayOfWeek: Int,
  isWeekend: Int
) {
  require(userId.nonEmpty, "userId cannot be empty")
  require(itemId.nonEmpty, "itemId cannot be empty")
  require(category.nonEmpty, "category cannot be empty")
  require(
    Behaviors.VALID_BEHAVIORS.contains(behavior),
    s"behavior must be one of ${Behaviors.VALID_BEHAVIORS.mkString(",")}, got: $behavior"
  )
  require(hour >= 0 && hour <= 23, s"hour must be in 0..23, got: $hour")
  require(dayOfWeek >= 1 && dayOfWeek <= 7, s"dayOfWeek must be in 1..7, got: $dayOfWeek")
  require(isWeekend == 0 || isWeekend == 1, s"isWeekend must be 0 or 1, got: $isWeekend")
}

case class SalesReport(
  category: String,
  totalPV: Long,
  totalBuy: Long,
  convRate: Double,
  reportDate: String
) {
  require(category.nonEmpty, "category cannot be empty")
  require(totalPV >= 0, s"totalPV cannot be negative, got: $totalPV")
  require(totalBuy >= 0, s"totalBuy cannot be negative, got: $totalBuy")
  require(convRate >= 0.0 && convRate <= 100.0, s"convRate must be in 0..100, got: $convRate")
  require(reportDate.nonEmpty, "reportDate cannot be empty")
}

case class UserNode(userId: String, pvCount: Long) {
  require(userId.nonEmpty, "userId cannot be empty")
  require(pvCount >= 0, s"pvCount cannot be negative, got: $pvCount")
}

case class BuyEdge(srcUser: String, dstUser: String, weight: Double) {
  require(srcUser.nonEmpty, "srcUser cannot be empty")
  require(dstUser.nonEmpty, "dstUser cannot be empty")
  require(weight >= 0.0, s"weight cannot be negative, got: $weight")
}
