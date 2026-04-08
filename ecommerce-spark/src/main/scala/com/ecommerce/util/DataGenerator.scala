package com.ecommerce.util

import com.ecommerce.config.AppConfig
import com.ecommerce.core.Behaviors

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Demo data generator.
 * Uses "user profile + session chain" instead of pure iid random sampling.
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

  private[util] val Categories: Vector[String] = AppConfig.DOMAIN_CATEGORIES

  private[util] val CategoryItems: Map[String, Vector[String]] = {
    Categories.map { category =>
      val prefix = AppConfig.CATEGORY_ITEM_PREFIX(category)
      val size = AppConfig.CATEGORY_ITEM_SIZE(category)
      category -> (1 to size).map(i => s"$prefix$i").toVector
    }.toMap
  }

  private val BaseTimestamp = 1700000000L

  def generate(outputPath: String, rowCount: Int = 100000): Unit = {
    val rng = new Random(42L)
    val profiles = buildProfiles(userCount = 5000, rng)

    println(s"[DataGenerator] generate $rowCount rows -> $outputPath")
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

    println("[DataGenerator] done")
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
      events += GeneratedEvent(profile.userId, primaryItem, primaryCategory, Behaviors.VIEW, currentTs)
      currentTs += 20 + rng.nextInt(90)
    }

    val favorite = rng.nextDouble() < 0.30
    if (favorite) {
      events += GeneratedEvent(profile.userId, primaryItem, primaryCategory, Behaviors.FAVORITE, currentTs)
      currentTs += 30 + rng.nextInt(120)
    }

    val cartProbability =
      if (primaryCategory == profile.preferredCategory) 0.55 else 0.28
    val addToCart = rng.nextDouble() < cartProbability
    if (addToCart) {
      events += GeneratedEvent(profile.userId, primaryItem, primaryCategory, Behaviors.CART, currentTs)
      currentTs += 30 + rng.nextInt(120)
    }

    val buyProbability =
      if (addToCart) profile.buyAffinity
      else profile.buyAffinity * 0.35
    if (rng.nextDouble() < buyProbability) {
      events += GeneratedEvent(profile.userId, primaryItem, primaryCategory, Behaviors.BUY, currentTs)
      currentTs += 60 + rng.nextInt(180)
    }

    val extraBrowseCount = rng.nextInt(3)
    (1 to extraBrowseCount).foreach { _ =>
      val category = chooseCategory(profile, rng)
      val itemId = chooseItem(category, profile, rng)
      events += GeneratedEvent(profile.userId, itemId, category, Behaviors.VIEW, currentTs)
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
