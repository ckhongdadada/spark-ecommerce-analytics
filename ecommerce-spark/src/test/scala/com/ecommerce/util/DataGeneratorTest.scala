package com.ecommerce.util

import org.junit.Assert.{assertEquals, assertTrue}
import org.junit.Test

import java.nio.file.Files
import scala.io.Source
import scala.util.Random

class DataGeneratorTest {

  @Test
  def buildSessionShouldProduceOrderedPurchaseChain(): Unit = {
    val rng = new Random(42L)
    val profile = DataGenerator.buildProfiles(1, rng).head
    val session = DataGenerator.buildSession(profile, 1700000000L, new Random(7L))

    assertTrue(session.nonEmpty)
    assertEquals("pv", session.head.behavior)
    assertTrue(session.sliding(2).forall {
      case Seq(left, right) => left.timestamp <= right.timestamp
      case _ => true
    })

    val buyIndex = session.indexWhere(_.behavior == "buy")
    if (buyIndex >= 0) {
      val precedingBehaviors = session.take(buyIndex).map(_.behavior).toSet
      assertTrue(precedingBehaviors.contains("pv"))
      assertTrue(precedingBehaviors.contains("cart") || precedingBehaviors.contains("fav"))
    }
  }

  @Test
  def generateShouldWriteCsvWithRequestedRowCount(): Unit = {
    val tempFile = Files.createTempFile("data-generator-", ".csv").toFile

    try {
      DataGenerator.generate(tempFile.getAbsolutePath, rowCount = 25)

      val source = Source.fromFile(tempFile, "UTF-8")
      try {
        val lines = source.getLines().toVector
        assertEquals("userId,itemId,category,behavior,timestamp", lines.head)
        assertEquals(26, lines.length)
        assertTrue(lines.tail.forall(_.split(",").length == 5))
      } finally {
        source.close()
      }
    } finally {
      tempFile.delete()
    }
  }
}
