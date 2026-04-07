package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.util.Logging
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

/**
 * ══════════════════════════════════════════════════════════
 * 模块四：社交挖掘（GraphX）
 * 对应课程：第3章 Spark进阶
 *   - 基于GraphX的图计算框架
 *   - Vertex / Edge RDD 构建
 *   - PageRank 影响力分析
 *   - 连通分量（Connected Components）社群发现
 * ══════════════════════════════════════════════════════════
 *
 * 图建模思路：
 *   节点 = 用户（distinct userId → zipWithUniqueId → Long）
 *   边   = 两用户共同购买过同一类目商品 → 形成潜在社交关系
 *   边权 = 共同购买类目数量
 */
object Module4_GraphX extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info("  模块四：GraphX 社交挖掘")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    // ── Step 1：读取数据，提取用户-类目购买关系 ──
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.RAW_LOG_PATH)

    val buyDF = rawDF.filter($"behavior" === "buy")
      .select("userId", "category")
      .distinct()

    // ── Step 2：构建节点 RDD（为每个用户分配唯一顶点 ID）──
    val userIdToVertexIdRDD: RDD[(String, VertexId)] = rawDF
      .select("userId").distinct()
      .rdd
      .map(row => row.getString(0))
      .zipWithUniqueId()
      .map { case (uid, vertexId) => (uid, vertexId) }
      .persist(StorageLevel.MEMORY_ONLY)

    val vertices: RDD[(VertexId, String)] = userIdToVertexIdRDD
      .map { case (uid, vertexId) => (vertexId, uid) }
      .persist(StorageLevel.MEMORY_ONLY)

    // ── Step 3：构建边 RDD（共同购买同类目的用户对）──
    // 自连接：同一类目的购买用户两两配对
    val categoryBuyersRDD = buyDF.rdd
      .map(row => (row.getString(0), row.getString(1)))  // (userId, category)
      .join(userIdToVertexIdRDD)
      .map { case (userId, (category, vertexId)) => (category, (userId, vertexId)) }
      .groupByKey()
      .mapValues(_.toList.distinct)
      .filter(_._2.size > 1)   // 至少两人购买同类目

    val edgesRDD: RDD[Edge[Double]] = categoryBuyersRDD.flatMap { case (_, users) =>
      val vertexIds = users.map(_._2)
      for {
        i <- vertexIds.indices
        j <- (i + 1) until vertexIds.size
      } yield {
        val srcId = math.min(vertexIds(i), vertexIds(j))
        val dstId = math.max(vertexIds(i), vertexIds(j))
        Edge(srcId, dstId, 1.0)
      }
    }

    // 合并重复边（累加权重）
    val mergedEdges = edgesRDD
      .map(e => ((e.srcId, e.dstId), e.attr))
      .reduceByKey(_ + _)
      .map { case ((src, dst), w) => Edge(src, dst, w) }
      .persist(StorageLevel.MEMORY_ONLY)

    // ── Step 4：构建图 ──
    val graph: Graph[String, Double] = Graph(vertices, mergedEdges, "Unknown")
      .persist(StorageLevel.MEMORY_ONLY)
    logger.info(s"图节点数: ${graph.vertices.count()}")
    logger.info(s"图边数:   ${graph.edges.count()}")

    // ── Step 5：PageRank 影响力分析 ──
    logger.info("PageRank Top 10 用户（购买影响力）：")
    val pageRankGraph = graph.pageRank(0.001)

    pageRankGraph.vertices
      .join(vertices)
      .map { case (_, (rank, uid)) => (uid, rank) }
      .sortBy(_._2, ascending = false)
      .take(10)
      .foreach { case (uid, rank) => logger.info(f"  $uid%-10s PageRank = $rank%.4f") }

    // ── Step 6：连通分量 —— 社群发现 ──
    logger.info("连通分量（社群）统计：")
    val ccGraph = graph.connectedComponents()

    val componentSizes = ccGraph.vertices
      .map { case (_, compId) => (compId, 1) }
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)

    logger.info(s"  社群总数: ${componentSizes.count()}")
    logger.info("  最大社群 Top 5：")
    componentSizes.take(5).foreach { case (id, size) =>
      logger.info(s"  社群 $id -> 成员数 $size")
    }

    // ── Step 7：出度/入度分析 ──
    logger.info("出度 Top 5（高影响力节点）：")
    graph.outDegrees
      .join(vertices)
      .map { case (_, (deg, uid)) => (uid, deg) }
      .sortBy(_._2, ascending = false)
      .take(5)
      .foreach { case (uid, deg) => logger.info(s"  $uid -> 出度 $deg") }

    // ── 清理缓存 ──
    userIdToVertexIdRDD.unpersist()
    vertices.unpersist()
    mergedEdges.unpersist()
    graph.unpersist()

    logger.info("模块四执行完毕 ✓")
  }
}
