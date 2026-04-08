package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import com.ecommerce.util.{DataQualityGuard, Logging}
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * Module 4: GraphX analysis.
 */
object Module4_GraphX extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("4")}")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    val rawDF = DataQualityGuard.loadValidatedBatchEvents(
      spark,
      AppConfig.RAW_LOG_PATH,
      sourceTag = "module4_graphx"
    )

    val buyDF = rawDF
      .filter($"behavior" === Behaviors.BUY)
      .select("userId", "category")
      .distinct()

    val userIdToVertexIdRDD: RDD[(String, VertexId)] = rawDF
      .select("userId")
      .distinct()
      .rdd
      .map(row => row.getString(0))
      .zipWithUniqueId()
      .map { case (uid, vertexId) => (uid, vertexId) }
      .persist(StorageLevel.MEMORY_ONLY)

    val vertices: RDD[(VertexId, String)] = userIdToVertexIdRDD
      .map { case (uid, vertexId) => (vertexId, uid) }
      .persist(StorageLevel.MEMORY_ONLY)

    val categoryBuyersRDD = buyDF.rdd
      .map(row => (row.getString(0), row.getString(1)))
      .join(userIdToVertexIdRDD)
      .map { case (_, (category, vertexId)) => (category, vertexId) }
      .groupByKey()
      .mapValues(_.toList.distinct)
      .filter(_._2.size > 1)

    val edgesRDD: RDD[Edge[Double]] = categoryBuyersRDD.flatMap { case (_, vertexIds) =>
      for {
        i <- vertexIds.indices
        j <- (i + 1) until vertexIds.size
      } yield {
        val srcId = math.min(vertexIds(i), vertexIds(j))
        val dstId = math.max(vertexIds(i), vertexIds(j))
        Edge(srcId, dstId, 1.0)
      }
    }

    val mergedEdges = edgesRDD
      .map(e => ((e.srcId, e.dstId), e.attr))
      .reduceByKey(_ + _)
      .map { case ((src, dst), w) => Edge(src, dst, w) }
      .persist(StorageLevel.MEMORY_ONLY)

    val graph: Graph[String, Double] = Graph(vertices, mergedEdges, "Unknown")
      .persist(StorageLevel.MEMORY_ONLY)

    logger.info(s"Graph vertices: ${graph.vertices.count()}")
    logger.info(s"Graph edges: ${graph.edges.count()}")

    logger.info("PageRank top users:")
    val pageRankGraph = graph.pageRank(0.001)
    pageRankGraph.vertices
      .join(vertices)
      .map { case (_, (rank, uid)) => (uid, rank) }
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach { case (uid, rank) =>
        logger.info(f"  $uid%-10s rank=$rank%.4f")
      }

    logger.info("Connected component sizes:")
    val ccGraph = graph.connectedComponents()
    val componentSizes = ccGraph.vertices
      .map { case (_, compId) => (compId, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    logger.info(s"  Total components: ${componentSizes.count()}")
    componentSizes.take(5).foreach { case (id, size) =>
      logger.info(s"  Component $id -> size=$size")
    }

    logger.info("Top out-degree users:")
    graph.outDegrees
      .join(vertices)
      .map { case (_, (deg, uid)) => (uid, deg) }
      .sortBy(_._2, ascending = false)
      .take(5)
      .foreach { case (uid, deg) =>
        logger.info(s"  $uid -> outDegree=$deg")
      }

    userIdToVertexIdRDD.unpersist()
    vertices.unpersist()
    mergedEdges.unpersist()
    graph.unpersist()

    logger.info("Module 4 completed")
  }
}
