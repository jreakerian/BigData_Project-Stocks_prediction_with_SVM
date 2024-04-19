package model

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator

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

  def prepareData(data: DataFrame): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("sma_15", "ema_15", "rsi_14")) // Features columns
      .setOutputCol("assembledFeatures")
      .setHandleInvalid("skip")

    assembler.transform(data)
  }

  def trainModel(data: DataFrame): LinearSVCModel = {
    // Specify the assembler for combining feature columns into a single vector

   val featureData = prepareData(data)

    // Initialize an instance of LinearSVC
    val lsvc = new LinearSVC()
      .setLabelCol("label")  // Assuming there is a 'label' column in the DataFrame
      .setFeaturesCol("assembledFeatures")
      .setMaxIter(10)
      .setRegParam(0.1)

    lsvc.fit(featureData)
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