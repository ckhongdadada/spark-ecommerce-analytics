package com.ecommerce.util

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * 模拟数据生成器。
 * 通过“用户画像 + 会话链路”生成更接近真实电商行为的数据，而不是完全独立随机采样。
 */
object DataGenerator {

  private[util] case class UserProfile(
    userId: String,
    preferredCategory: String,
    favoriteItems: Vector[String],
    buyAffinity: Double
  )

  private[util] case class GeneratedEvent(
    userId: String,
    itemId: String,
    category: String,
    behavior: String,
    timestamp: Long
  )

  private[util] val Categories: Vector[String] =
    Vector("Electronics", "Clothing", "Food", "Books", "Sports", "Beauty")

  private[util] val CategoryItems: Map[String, Vector[String]] = Map(
    "Electronics" -> (1 to 1500).map(i => s"E$i").toVector,
    "Clothing" -> (1 to 1500).map(i => s"C$i").toVector,
    "Food" -> (1 to 1200).map(i => s"F$i").toVector,
    "Books" -> (1 to 1200).map(i => s"B$i").toVector,
    "Sports" -> (1 to 1000).map(i => s"S$i").toVector,
    "Beauty" -> (1 to 1000).map(i => s"BE$i").toVector
  )

  private val BaseTimestamp = 1700000000L

  def generate(outputPath: String, rowCount: Int = 100000): Unit = {
    val rng = new Random(42L)
    val profiles = buildProfiles(userCount = 5000, rng)

    println(s"[DataGenerator] 生成 $rowCount 条模拟数据 -> $outputPath")
    val outputFile = new File(outputPath)
    val parent = outputFile.getParentFile
    if (parent != null) parent.mkdirs()

    val writer = new PrintWriter(outputFile)
    try {
      writer.println("userId,itemId,category,behavior,timestamp")

      var written = 0
      while (written < rowCount) {
        val profile = profiles(rng.nextInt(profiles.length))
        val sessionStart = BaseTimestamp + rng.nextInt(14 * 24 * 3600)
        val sessionEvents = buildSession(profile, sessionStart, rng)
        val remaining = rowCount - written

        sessionEvents.take(remaining).foreach { event =>
          writer.println(s"${event.userId},${event.itemId},${event.category},${event.behavior},${event.timestamp}")
        }

        written += math.min(remaining, sessionEvents.length)
      }
    } finally {
      writer.close()
    }

    println("[DataGenerator] 完成")
  }

  private[util] def buildProfiles(userCount: Int, rng: Random): Vector[UserProfile] = {
    (1 to userCount).map { i =>
      val preferredCategory = Categories(rng.nextInt(Categories.length))
      val itemPool = CategoryItems(preferredCategory)
      val favoriteItems = rng.shuffle(itemPool).take(8).toVector
      val buyAffinity = 0.18 + rng.nextDouble() * 0.42
      UserProfile(s"U$i", preferredCategory, favoriteItems, buyAffinity)
    }.toVector
  }

  private[util] def buildSession(profile: UserProfile, sessionStart: Long, rng: Random): Vector[GeneratedEvent] = {
    val events = ArrayBuffer.empty[GeneratedEvent]

    var currentTs = sessionStart
    val primaryCategory = chooseCategory(profile, rng)
    val primaryItem = chooseItem(primaryCategory, profile, rng)

    val pvCount = 1 + rng.nextInt(3)
    (1 to pvCount).foreach { _ =>
      events += GeneratedEvent(profile.userId, primaryItem, primaryCategory, "pv", currentTs)
      currentTs += 20 + rng.nextInt(90)
    }

    val favorite = rng.nextDouble() < 0.30
    if (favorite) {
      events += GeneratedEvent(profile.userId, primaryItem, primaryCategory, "fav", currentTs)
      currentTs += 30 + rng.nextInt(120)
    }

    val cartProbability =
      if (primaryCategory == profile.preferredCategory) 0.55 else 0.28
    val addToCart = rng.nextDouble() < cartProbability
    if (addToCart) {
      events += GeneratedEvent(profile.userId, primaryItem, primaryCategory, "cart", currentTs)
      currentTs += 30 + rng.nextInt(120)
    }

    val buyProbability =
      if (addToCart) profile.buyAffinity
      else profile.buyAffinity * 0.35
    if (rng.nextDouble() < buyProbability) {
      events += GeneratedEvent(profile.userId, primaryItem, primaryCategory, "buy", currentTs)
      currentTs += 60 + rng.nextInt(180)
    }

    val extraBrowseCount = rng.nextInt(3)
    (1 to extraBrowseCount).foreach { _ =>
      val category = chooseCategory(profile, rng)
      val itemId = chooseItem(category, profile, rng)
      events += GeneratedEvent(profile.userId, itemId, category, "pv", currentTs)
      currentTs += 15 + rng.nextInt(90)
    }

    events.sortBy(_.timestamp).toVector
  }

  private def chooseCategory(profile: UserProfile, rng: Random): String = {
    if (rng.nextDouble() < 0.65) profile.preferredCategory
    else Categories(rng.nextInt(Categories.length))
  }

  private def chooseItem(category: String, profile: UserProfile, rng: Random): String = {
    if (category == profile.preferredCategory && profile.favoriteItems.nonEmpty && rng.nextDouble() < 0.45) {
      profile.favoriteItems(rng.nextInt(profile.favoriteItems.length))
    } else {
      val pool = CategoryItems(category)
      pool(rng.nextInt(pool.length))
    }
  }

  def main(args: Array[String]): Unit = {
    val path = if (args.nonEmpty) args(0) else "data/mock/user_behavior_log.csv"
    val rows = if (args.length > 1) args(1).toInt else 100000
    generate(path, rows)
  }
}
