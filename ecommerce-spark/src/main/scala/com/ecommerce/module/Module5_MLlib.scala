package com.ecommerce.module

import com.ecommerce.config.AppConfig
import com.ecommerce.core.{Behaviors, SparkSessionFactory}
import com.ecommerce.util.{DataQualityGuard, Logging}
import org.apache.spark.ml.{Pipeline, Transformer}
import org.apache.spark.ml.classification.{LogisticRegression, RandomForestClassifier}
import org.apache.spark.ml.evaluation.{BinaryClassificationEvaluator, MulticlassClassificationEvaluator}
import org.apache.spark.ml.feature.{StandardScaler, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.util.MLWritable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}

/**
 * Module 5: MLlib behavior prediction.
 */
object Module5_MLlib extends Logging {

  private case class ModelEvaluation(
    name: String,
    auc: Double,
    accuracy: Double,
    f1: Double,
    precision: Double,
    recall: Double,
    cvBestAuc: Double,
    model: Transformer
  )

  def run(): Unit = {
    logger.info("=" * 60)
    logger.info(s"  ${AppConfig.moduleName("5")}")
    logger.info("=" * 60)

    val spark = SparkSessionFactory.getSession()

    val rawDF = DataQualityGuard.loadValidatedBatchEvents(
      spark,
      AppConfig.RAW_LOG_PATH,
      sourceTag = "module5_mllib"
    )

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

    val Array(trainValDF, testDF) = balancedDF.randomSplit(
      Array(AppConfig.ML_TRAIN_RATIO, AppConfig.ML_TEST_RATIO),
      seed = AppConfig.ML_RANDOM_SEED
    )
    val trainCount = trainValDF.count()
    val testCount = testDF.count()
    require(trainCount > 0, "Training split is empty. Please provide more data.")
    require(testCount > 0, "Test split is empty. Please provide more data.")
    logger.info(s"Train/CV: $trainCount | Holdout test: $testCount")

    val lr = new LogisticRegression()
      .setMaxIter(AppConfig.ML_MAX_ITER)
      .setRegParam(AppConfig.ML_REG_PARAM)
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setWeightCol("classWeight")

    val lrPipeline = new Pipeline().setStages(Array(assembler, scaler, lr))
    val lrGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, AppConfig.ML_LR_REG_PARAMS)
      .addGrid(lr.elasticNetParam, AppConfig.ML_LR_ELASTIC_NET_PARAMS)
      .build()

    val rf = new RandomForestClassifier()
      .setFeaturesCol("features")
      .setLabelCol("label")
      .setSeed(AppConfig.ML_RANDOM_SEED)
      .setWeightCol("classWeight")

    val rfPipeline = new Pipeline().setStages(Array(assembler, scaler, rf))
    val rfGrid = new ParamGridBuilder()
      .addGrid(rf.numTrees, AppConfig.ML_RF_NUM_TREES_GRID)
      .addGrid(rf.maxDepth, AppConfig.ML_RF_MAX_DEPTH_GRID)
      .build()

    val aucEvaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")

    val evaluations = Seq(
      trainAndEvaluate(
        modelName = "LogisticRegression",
        trainValDF = trainValDF,
        testDF = testDF,
        pipeline = lrPipeline,
        paramGrid = lrGrid,
        aucEvaluator = aucEvaluator
      ),
      trainAndEvaluate(
        modelName = "RandomForest",
        trainValDF = trainValDF,
        testDF = testDF,
        pipeline = rfPipeline,
        paramGrid = rfGrid,
        aucEvaluator = aucEvaluator
      )
    ).sortBy(metric => (-metric.auc, -metric.f1, -metric.accuracy))

    evaluations.foreach(logModelEvaluation)

    val best = evaluations.head
    persistBestModel(best)
    persistSelectionReport(evaluations)

    logger.info(
      f"Best model selected: ${best.name}, holdout AUC=${best.auc}%.4f, " +
        f"F1=${best.f1}%.4f, Accuracy=${best.accuracy}%.4f"
    )
    logger.info("Module 5 completed")
  }

  private def trainAndEvaluate(
    modelName: String,
    trainValDF: DataFrame,
    testDF: DataFrame,
    pipeline: Pipeline,
    paramGrid: Array[org.apache.spark.ml.param.ParamMap],
    aucEvaluator: BinaryClassificationEvaluator
  ): ModelEvaluation = {
    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramGrid)
      .setEvaluator(aucEvaluator)
      .setNumFolds(AppConfig.ML_CV_FOLDS)
      .setSeed(AppConfig.ML_RANDOM_SEED)
      .setParallelism(AppConfig.ML_CV_PARALLELISM)

    val cvModel = crossValidator.fit(trainValDF)
    val predictions = cvModel.bestModel.transform(testDF)
    val metrics = evaluatePredictions(modelName, predictions)
    val cvBestAuc = if (cvModel.avgMetrics.nonEmpty) cvModel.avgMetrics.max else 0.0

    ModelEvaluation(
      name = modelName,
      auc = metrics.auc,
      accuracy = metrics.accuracy,
      f1 = metrics.f1,
      precision = metrics.precision,
      recall = metrics.recall,
      cvBestAuc = cvBestAuc,
      model = cvModel.bestModel
    )
  }

  private case class EvalMetrics(
    auc: Double,
    accuracy: Double,
    f1: Double,
    precision: Double,
    recall: Double
  )

  private def evaluatePredictions(name: String, predictions: DataFrame): EvalMetrics = {
    val accuracy = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
      .evaluate(predictions)

    val f1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")
      .evaluate(predictions)

    val precision = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
      .evaluate(predictions)

    val recall = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
      .evaluate(predictions)

    val auc = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("rawPrediction")
      .setMetricName("areaUnderROC")
      .evaluate(predictions)

    logger.info(f"[$name] Holdout metrics: AUC=$auc%.4f, Accuracy=$accuracy%.4f, F1=$f1%.4f, Precision=$precision%.4f, Recall=$recall%.4f")

    predictions.groupBy("label", "prediction")
      .count()
      .orderBy("label", "prediction")
      .show()

    EvalMetrics(
      auc = auc,
      accuracy = accuracy,
      f1 = f1,
      precision = precision,
      recall = recall
    )
  }

  private def logModelEvaluation(evaluation: ModelEvaluation): Unit = {
    logger.info(
      f"[${evaluation.name}] CV best AUC=${evaluation.cvBestAuc}%.4f | " +
        f"Holdout AUC=${evaluation.auc}%.4f | F1=${evaluation.f1}%.4f | " +
        f"Accuracy=${evaluation.accuracy}%.4f"
    )
  }

  private def persistBestModel(evaluation: ModelEvaluation): Unit = {
    evaluation.model match {
      case writable: MLWritable =>
        writable.write.overwrite().save(AppConfig.ML_MODEL_PATH)
        logger.info(s"Best model saved to: ${AppConfig.ML_MODEL_PATH}")
      case _ =>
        logger.warn(s"Best model ${evaluation.name} is not writable; skipped model persistence")
    }
  }

  private def persistSelectionReport(evaluations: Seq[ModelEvaluation]): Unit = {
    val reportPath = Paths.get(AppConfig.ML_MODEL_SELECTION_REPORT_PATH)
    Files.createDirectories(reportPath.getParent)

    val header = "rank,model,cv_best_auc,holdout_auc,accuracy,f1,precision,recall"
    val rows = evaluations.zipWithIndex.map { case (evaluation, index) =>
      Seq(
        (index + 1).toString,
        evaluation.name,
        f"${evaluation.cvBestAuc}%.6f",
        f"${evaluation.auc}%.6f",
        f"${evaluation.accuracy}%.6f",
        f"${evaluation.f1}%.6f",
        f"${evaluation.precision}%.6f",
        f"${evaluation.recall}%.6f"
      ).map(csvEscape).mkString(",")
    }

    Files.write(reportPath, (header +: rows).mkString(System.lineSeparator()).getBytes(StandardCharsets.UTF_8))
    logger.info(s"Model selection report saved to: ${AppConfig.ML_MODEL_SELECTION_REPORT_PATH}")
  }

  private def csvEscape(value: String): String = {
    "\"" + value.replace("\"", "\"\"") + "\""
  }
}
