package model

import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.classification.{LinearSVC, LinearSVCModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
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


  def trainModel(data: DataFrame): LinearSVCModel = {
    // Specify the assembler for combining feature columns into a single vector
    val assembler = new VectorAssembler()
      .setInputCols(Array("sma_15", "ema_15", "rsi_14")) // Assuming these features are already computed
      .setOutputCol("features")

    // Initialize an instance of LinearSVC
    val lsvc = new LinearSVC()
      .setLabelCol("label")  // Assuming there is a 'label' column in the DataFrame
      .setFeaturesCol("features")
      .setMaxIter(10)
      .setRegParam(0.1)

    // Create a Pipeline which chains vector assembler and SVM model
    val pipeline = new Pipeline()
      .setStages(Array(assembler, lsvc))

    // Train the model
    val model = pipeline.fit(data)

    model.stages.last.asInstanceOf[LinearSVCModel]
  }

  /**
   * Evaluates the trained SVM model using a given test dataset.
   *
   * @param model Trained SVM model.
   * @param testData DataFrame containing the test data.
   * @return The accuracy of the model on the test data.
   */
  def evaluateModel(model: LinearSVCModel, testData: DataFrame): Double = {
    val predictions = model.transform(testData)

    // Create an evaluator for binary classification, which expects two input columns: prediction and label.
    val evaluator = new BinaryClassificationEvaluator()
      .setLabelCol("label")
      .setRawPredictionCol("prediction")
      .setMetricName("areaUnderROC")  // Can change to "accuracy" if using a different evaluator

    // Evaluate the model and compute the accuracy
    val accuracy = evaluator.evaluate(predictions)
    accuracy
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