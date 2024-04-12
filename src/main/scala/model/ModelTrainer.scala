package model

import org.apache.spark.ml.classification.LinearSVC
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder, CrossValidatorModel}
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.sql.DataFrame

object ModelTrainer {

  def trainSVMModel(df: DataFrame): PipelineModel = {
    val featureCols = Array("feature1", "feature2", "feature3")  // Replace with your feature columns
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val svm = new LinearSVC().setLabelCol("label").setFeaturesCol("features").setMaxIter(100).setRegParam(0.1)

    val pipeline = new Pipeline().setStages(Array(assembler, svm))
    pipeline.fit(df)
  }

  def trainSVMWithTuning(df: DataFrame): PipelineModel = {
    val featureCols = Array("feature1", "feature2", "feature3")  // Replace with your feature columns
    val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
    val svm = new LinearSVC().setLabelCol("label").setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(assembler, svm))

    val paramGrid = new ParamGridBuilder()
      .addGrid(svm.maxIter, Array(10, 100, 1000))
      .addGrid(svm.regParam, Array(0.01, 0.1, 1.0))
      .build()

    val evaluator = new BinaryClassificationEvaluator().setLabelCol("label").setRawPredictionCol("rawPrediction")

    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(evaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(3)

    val cvModel: CrossValidatorModel = cv.fit(df)
    cvModel.bestModel.asInstanceOf[PipelineModel]  // Extracting the best model as a PipelineModel
  }
}
