package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.SparkSessionFactory
import com.ecommerce.util.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler, StandardScaler}
import org.apache.spark.sql.functions._

/**
 * ══════════════════════════════════════════════════════════
 * 模块五：行为预测（MLlib）
 * 对应课程：第3章 Spark进阶
 *   - 基于MLlib的机器学习
 *   - 特征工程 / VectorAssembler
 *   - 逻辑回归 / 随机森林
 *   - 准确率与 AUC 评估
 *
 * 任务：预测用户是否会产生"购买"行为（二分类）
 * ══════════════════════════════════════════════════════════
 */
object Module5_MLlib extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info("  模块五：MLlib 购买行为预测")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()
    import spark.implicits._

    // ── Step 1：数据加载与特征工程 ──
    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.RAW_LOG_PATH)

    // 按用户聚合：生成用于预测的特征
    val userFeatureDF = rawDF
      .groupBy("userId")
      .agg(
        count(when(col("behavior") === "pv",   1)).alias("pv_cnt"),
        count(when(col("behavior") === "cart",  1)).alias("cart_cnt"),
        count(when(col("behavior") === "fav",   1)).alias("fav_cnt"),
        count(when(col("behavior") === "buy",   1)).alias("buy_cnt"),
        countDistinct("category").alias("category_diversity"),
        countDistinct("itemId").alias("item_diversity")
      )
      // 标签：buy_cnt > 0 → 1（有购买行为），否则 → 0
      .withColumn("label", when(col("buy_cnt") > 0, 1.0).otherwise(0.0))

    logger.info(s"特征样本数: ${userFeatureDF.count()}")
    logger.info("正负样本分布：")
    userFeatureDF.groupBy("label").count().show()

    // ── Step 2：处理类别不平衡（计算类别权重）──
    val labelCounts = userFeatureDF.groupBy("label").count().collect()
    val negCount = labelCounts.find(_.getDouble(0) == 0.0).map(_.getLong(1)).getOrElse(1L)
    val posCount = labelCounts.find(_.getDouble(0) == 1.0).map(_.getLong(1)).getOrElse(1L)
    val total = negCount + posCount
    val weightNeg = total.toDouble / (2 * negCount)
    val weightPos = total.toDouble / (2 * posCount)
    logger.info(s"类别权重 - 负类: $weightNeg, 正类: $weightPos")

    val balancedDF = userFeatureDF.withColumn("classWeight",
      when(col("label") === 0.0, weightNeg).otherwise(weightPos)
    )

    // ── Step 3：特征向量化 ──
    val featureCols = Array("pv_cnt", "cart_cnt", "fav_cnt",
                            "category_diversity", "item_diversity")

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("raw_features")

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    // ── Step 4：训练集/测试集划分 ──
    val Array(trainDF, testDF) = balancedDF.randomSplit(
      Array(AppConfig.ML_TRAIN_RATIO, AppConfig.ML_TEST_RATIO),
      seed = 42L
    )
    logger.info(s"训练集: ${trainDF.count()} | 测试集: ${testDF.count()}")

    // ── Step 5A：逻辑回归（带类别权重）──
    logger.info("=== 逻辑回归 ===")
    val lr = new LogisticRegression()
      .setMaxIter(AppConfig.ML_MAX_ITER)
      .setRegParam(AppConfig.ML_REG_PARAM)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setWeightCol("classWeight")

    val lrPipeline = new Pipeline().setStages(Array(assembler, scaler, lr))
    val lrModel    = lrPipeline.fit(trainDF)
    val lrPred     = lrModel.transform(testDF)
    evaluateModel("逻辑回归", lrPred)

    // ── Step 5B：随机森林（带类别权重）──
    logger.info("=== 随机森林 ===")
    val rf = new RandomForestClassifier()
      .setNumTrees(100)
      .setMaxDepth(5)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setSeed(42L)
      .setWeightCol("classWeight")

    val rfPipeline = new Pipeline().setStages(Array(assembler, scaler, rf))
    val rfModel    = rfPipeline.fit(trainDF)
    val rfPred     = rfModel.transform(testDF)
    evaluateModel("随机森林", rfPred)

    // ── Step 6：保存最优模型 ──
    rfModel.write.overwrite().save(AppConfig.ML_MODEL_PATH)
    logger.info(s"模型已保存至: ${AppConfig.ML_MODEL_PATH}")
    logger.info("模块五执行完毕 ✓")
  }

  /**
   * 统一评估函数：准确率 + AUC
   */
  private def evaluateModel(name: String, predictions: org.apache.spark.sql.DataFrame): Unit = {
    val accuracy = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
      .evaluate(predictions)

    val auc = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
      .evaluate(predictions)

    logger.info(f"  [$name] 准确率(Accuracy) = $accuracy%.4f")
    logger.info(f"  [$name] AUC             = $auc%.4f")

    // 混淆矩阵
    predictions.groupBy("label", "prediction").count()
      .orderBy("label", "prediction")
      .show()
  }
}
