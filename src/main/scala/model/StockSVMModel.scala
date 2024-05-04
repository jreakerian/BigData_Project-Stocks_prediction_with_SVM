package model

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

/**
 * Object responsible for managing the SVM model for stock prediction.
 */
object StockSVMModel {
  /**
   * Trains an SVM model on the provided stock data.
   *
   * @param data DataFrame containing the stock features and labels.
   * @return Trained SVM model.
   */
  def splitData(data: DataFrame, trainingRatio: Double): (DataFrame, DataFrame) = {
    val Array(trainingData, testData) = data.randomSplit(Array(trainingRatio, 1 - trainingRatio))
    (trainingData, testData)
  }
  def prepareData(data: DataFrame): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("sma_15", "ema_15", "rsi_14","vwap_15","macd_hist_12_26_9","upperBand","lowerBand","roc_10","atr_14","stochastic_14")) // Features columns
      .setOutputCol("assembledFeatures")
      .setHandleInvalid("skip")

    val assembledData = assembler.transform(data)

    val scaler = new StandardScaler()
      .setInputCol("assembledFeatures")
      .setOutputCol("scaledFeatures")
      .setWithStd(true)
      .setWithMean(false)

    scaler.fit(assembledData).transform(assembledData)
  }

  def trainModel(data: DataFrame): LinearSVCModel = {
    // Specify the assembler for combining feature columns into a single vector

   val featureData = prepareData(data)

    featureData.show(20,truncate = 0)

    // Initialize an instance of LinearSVC
    val lsvc = new LinearSVC()
      .setLabelCol("label")  // Assuming there is a 'label' column in the DataFrame
      .setFeaturesCol("scaledFeatures")
      .setMaxIter(100)
      .setRegParam(0.01)
      .setTol(1e-6)  // Lower tolerance for more precise convergence
      .setFitIntercept(true)

    val pipeline = new Pipeline().setStages(Array(lsvc))

    val paramGrid = new ParamGridBuilder()
      .addGrid(lsvc.regParam, Array(0.01, 0.1, 1.0))
      .addGrid(lsvc.maxIter, Array(50, 100, 200))
      .addGrid(lsvc.tol, Array(1e-6, 1e-5, 1e-4)) // Adding tolerance parameter
      .addGrid(lsvc.fitIntercept, Array(true, false)) // Adding fitIntercept parameter
      .build()

    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5)

    val cvModel = cv.fit(featureData)

    cvModel.bestModel.asInstanceOf[PipelineModel].stages.last.asInstanceOf[LinearSVCModel]

  }

  /**
   * Evaluates the trained SVM model using a given test dataset.
   *
   * @param model Trained SVM model.
   * @param testData DataFrame containing the test data.
   * @return The accuracy of the model on the test data.
   */
  def evaluateModel(model: LinearSVCModel, testData: DataFrame): Double = {
    val featureTestData = prepareData(testData)

    val predictions = model.transform(featureTestData)

    predictions.show(20,0)

    predictions.select("prediction", "label").show(10)


    // Create an evaluator for binary classification, which expects two input columns: prediction and label.
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")  // Can change to "accuracy" if using a different evaluator

    evaluator.evaluate(predictions)
    // Evaluate the model and compute the accuracy

  }
  /**
   * Saves the trained SVM model to disk.
   *
   * @param model The trained SVM model.
   * @param path Path where the model should be saved.
   */
  def saveModel(model: LinearSVCModel, path: String): Unit = {
    model.write.overwrite().save(path)
  }
}