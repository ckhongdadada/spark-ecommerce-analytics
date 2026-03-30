package com.ecommerce.core

/**
 * 核心数据模型
 * 统一定义各模块共用的 case class，便于 Kryo 序列化和 DataFrame Schema 推断
 */

object Behaviors {
  val VALID_BEHAVIORS = Set("pv", "buy", "cart", "fav")
}

// 用户行为原始日志（CSV 每行对应此结构）
case class UserBehaviorLog(
  userId:    String,
  itemId:    String,
  category:  String,
  behavior:  String,
  timestamp: Long
) {
  require(userId.nonEmpty, s"userId 不能为空")
  require(itemId.nonEmpty, s"itemId 不能为空")
  require(category.nonEmpty, s"category 不能为空")
  require(Behaviors.VALID_BEHAVIORS.contains(behavior), 
    s"behavior 必须是 ${Behaviors.VALID_BEHAVIORS.mkString(",")} 之一，当前值: $behavior")
  require(timestamp > 0, s"timestamp 必须大于 0，当前值: $timestamp")
}

// 清洗后的行为记录（模块一输出 → 模块二/五输入）
case class CleanBehavior(
  userId:    String,
  itemId:    String,
  category:  String,
  behavior:  String,
  hour:      Int,
  dayOfWeek: Int,
  isWeekend: Int
) {
  require(userId.nonEmpty, s"userId 不能为空")
  require(itemId.nonEmpty, s"itemId 不能为空")
  require(category.nonEmpty, s"category 不能为空")
  require(Behaviors.VALID_BEHAVIORS.contains(behavior), 
    s"behavior 必须是 ${Behaviors.VALID_BEHAVIORS.mkString(",")} 之一，当前值: $behavior")
  require(hour >= 0 && hour <= 23, s"hour 必须在 0-23 范围内，当前值: $hour")
  require(dayOfWeek >= 1 && dayOfWeek <= 7, s"dayOfWeek 必须在 1-7 范围内，当前值: $dayOfWeek")
  require(isWeekend == 0 || isWeekend == 1, s"isWeekend 必须是 0 或 1，当前值: $isWeekend")
}

// 销售汇总报表（模块二输出 → MySQL）
case class SalesReport(
  category:   String,
  totalPV:    Long,
  totalBuy:   Long,
  convRate:   Double,
  reportDate: String
) {
  require(category.nonEmpty, s"category 不能为空")
  require(totalPV >= 0, s"totalPV 不能为负数，当前值: $totalPV")
  require(totalBuy >= 0, s"totalBuy 不能为负数，当前值: $totalBuy")
  require(convRate >= 0.0 && convRate <= 100.0, s"convRate 必须在 0-100 范围内，当前值: $convRate")
  require(reportDate.nonEmpty, s"reportDate 不能为空")
}

// GraphX 节点数据（模块四）
case class UserNode(userId: String, pvCount: Long) {
  require(userId.nonEmpty, s"userId 不能为空")
  require(pvCount >= 0, s"pvCount 不能为负数，当前值: $pvCount")
}

// GraphX 边数据（模块四：用户共同购买同类商品 → 形成社交关系）
case class BuyEdge(srcUser: String, dstUser: String, weight: Double) {
  require(srcUser.nonEmpty, s"srcUser 不能为空")
  require(dstUser.nonEmpty, s"dstUser 不能为空")
  require(weight >= 0.0, s"weight 不能为负数，当前值: $weight")
}
