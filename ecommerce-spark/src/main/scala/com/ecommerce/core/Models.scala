package com.ecommerce.core

/**
 * 核心数据模型。
 * 统一定义各模块共享的 case class，便于 Kryo 序列化和 DataFrame Schema 推断。
 */
object Behaviors {
  val VALID_BEHAVIORS = Set("pv", "buy", "cart", "fav")
}

// 用户行为原始日志
case class UserBehaviorLog(
  userId: String,
  itemId: String,
  category: String,
  behavior: String,
  timestamp: Long
) {
  require(userId.nonEmpty, "userId 不能为空")
  require(itemId.nonEmpty, "itemId 不能为空")
  require(category.nonEmpty, "category 不能为空")
  require(Behaviors.VALID_BEHAVIORS.contains(behavior),
    s"behavior 必须是 ${Behaviors.VALID_BEHAVIORS.mkString(",")} 之一，当前值: $behavior")
  require(timestamp > 0, s"timestamp 必须大于 0，当前值: $timestamp")
}

// 清洗后的行为记录
case class CleanBehavior(
  userId: String,
  itemId: String,
  category: String,
  behavior: String,
  hour: Int,
  dayOfWeek: Int,
  isWeekend: Int
) {
  require(userId.nonEmpty, "userId 不能为空")
  require(itemId.nonEmpty, "itemId 不能为空")
  require(category.nonEmpty, "category 不能为空")
  require(Behaviors.VALID_BEHAVIORS.contains(behavior),
    s"behavior 必须是 ${Behaviors.VALID_BEHAVIORS.mkString(",")} 之一，当前值: $behavior")
  require(hour >= 0 && hour <= 23, s"hour 必须在 0-23 范围内，当前值: $hour")
  require(dayOfWeek >= 1 && dayOfWeek <= 7, s"dayOfWeek 必须在 1-7 范围内，当前值: $dayOfWeek")
  require(isWeekend == 0 || isWeekend == 1, s"isWeekend 必须是 0 或 1，当前值: $isWeekend")
}

// 模块二输出的销售报表
case class SalesReport(
  category: String,
  totalPV: Long,
  totalBuy: Long,
  convRate: Double,
  reportDate: String
) {
  require(category.nonEmpty, "category 不能为空")
  require(totalPV >= 0, s"totalPV 不能为负数，当前值: $totalPV")
  require(totalBuy >= 0, s"totalBuy 不能为负数，当前值: $totalBuy")
  require(convRate >= 0.0 && convRate <= 100.0, s"convRate 必须在 0-100 范围内，当前值: $convRate")
  require(reportDate.nonEmpty, "reportDate 不能为空")
}

// GraphX 顶点属性
case class UserNode(userId: String, pvCount: Long) {
  require(userId.nonEmpty, "userId 不能为空")
  require(pvCount >= 0, s"pvCount 不能为负数，当前值: $pvCount")
}

// GraphX 边属性
case class BuyEdge(srcUser: String, dstUser: String, weight: Double) {
  require(srcUser.nonEmpty, "srcUser 不能为空")
  require(dstUser.nonEmpty, "dstUser 不能为空")
  require(weight >= 0.0, s"weight 不能为负数，当前值: $weight")
}
