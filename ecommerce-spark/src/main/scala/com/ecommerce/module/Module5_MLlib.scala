package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import com.ecommerce.util.Logging
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Module 5: MLlib behavior prediction.
 */
object Module5_MLlib extends Logging {

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("5")}")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()

    val rawDF = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(AppConfig.RAW_LOG_PATH)

    val userFeatureDF = rawDF
      .groupBy("userId")
      .agg(
        count(when(col("behavior") === Behaviors.VIEW, 1)).alias("pv_cnt"),
        count(when(col("behavior") === Behaviors.CART, 1)).alias("cart_cnt"),
        count(when(col("behavior") === Behaviors.FAVORITE, 1)).alias("fav_cnt"),
        count(when(col("behavior") === Behaviors.BUY, 1)).alias("buy_cnt"),
        countDistinct("category").alias("category_diversity"),
        countDistinct("itemId").alias("item_diversity")
      )
      .withColumn("label", when(col("buy_cnt") > 0, 1.0).otherwise(0.0))

    logger.info(s"Feature samples: ${userFeatureDF.count()}")
    logger.info("Label distribution:")
    userFeatureDF.groupBy("label").count().show()

    val labelCounts = userFeatureDF.groupBy("label").count().collect()
    val negCount = labelCounts.find(_.getDouble(0) == 0.0).map(_.getLong(1)).getOrElse(1L)
    val posCount = labelCounts.find(_.getDouble(0) == 1.0).map(_.getLong(1)).getOrElse(1L)
    val total = negCount + posCount
    val weightNeg = total.toDouble / (2 * negCount)
    val weightPos = total.toDouble / (2 * posCount)

    val balancedDF = userFeatureDF.withColumn(
      "classWeight",
      when(col("label") === 0.0, weightNeg).otherwise(weightPos)
    )

    val featureCols = Array(
      "pv_cnt",
      "cart_cnt",
      "fav_cnt",
      "category_diversity",
      "item_diversity"
    )

    val assembler = new VectorAssembler()
      .setInputCols(featureCols)
      .setOutputCol("raw_features")

    val scaler = new StandardScaler()
      .setInputCol("raw_features")
      .setOutputCol("features")
      .setWithStd(true)
      .setWithMean(true)

    val Array(trainDF, testDF) = balancedDF.randomSplit(
      Array(AppConfig.ML_TRAIN_RATIO, AppConfig.ML_TEST_RATIO),
      seed = 42L
    )
    logger.info(s"Train: ${trainDF.count()} | Test: ${testDF.count()}")

    val lr = new LogisticRegression()
      .setMaxIter(AppConfig.ML_MAX_ITER)
      .setRegParam(AppConfig.ML_REG_PARAM)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setWeightCol("classWeight")

    val lrPipeline = new Pipeline().setStages(Array(assembler, scaler, lr))
    val lrPred = lrPipeline.fit(trainDF).transform(testDF)
    evaluateModel("LogisticRegression", lrPred)

    val rf = new RandomForestClassifier()
      .setNumTrees(100)
      .setMaxDepth(5)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setSeed(42L)
      .setWeightCol("classWeight")

    val rfPipeline = new Pipeline().setStages(Array(assembler, scaler, rf))
    val rfModel = rfPipeline.fit(trainDF)
    val rfPred = rfModel.transform(testDF)
    evaluateModel("RandomForest", rfPred)

    rfModel.write.overwrite().save(AppConfig.ML_MODEL_PATH)
    logger.info(s"Model saved to: ${AppConfig.ML_MODEL_PATH}")
    logger.info("Module 5 completed")
  }

  private def evaluateModel(name: String, predictions: DataFrame): Unit = {
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

    logger.info(f"  [$name] Accuracy = $accuracy%.4f")
    logger.info(f"  [$name] AUC      = $auc%.4f")

    predictions.groupBy("label", "prediction")
      .count()
      .orderBy("label", "prediction")
      .show()
  }
}
