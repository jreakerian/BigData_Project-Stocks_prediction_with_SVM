import org.apache.spark.sql.SparkSession
import config.Configuration
import data.{DataLoader, DataPreprocessor}
import features.FeatureBuilder
import model.StockSVMModel

object Main {
  def main(args: Array[String]): Unit = {
    // Initialize Spark Session
    val spark = SparkSession.builder()
      .appName("Stock Market Prediction with SVM")
      .master("local[*]")
      .getOrCreate()

    // Enable log level setting from configuration
    spark.sparkContext.setLogLevel("ERROR")

    println("Starting the stock market prediction application...")

    // Load and preprocess data
    println(s"Loading data from directory: ${Configuration.stockDataDirectory}")
    var stockData = DataLoader.loadData(spark, Configuration.stockDataDirectory)
    stockData = DataPreprocessor.cleanData(stockData)

    logInfo("Adding label column...")
    stockData = stockData.withColumn("previousClose", lag(col("Close"), 1).over(Window.partitionBy("stockName").orderBy("date")))
      .withColumn("label", when(col("Close") > col("previousClose"), 1).otherwise(0))
      .drop("previousClose")

    // Generate features
    println("Generating features...")
    stockData = FeatureBuilder.addAllFeatures(stockData)

    // Train the SVM model
    println("Training the SVM model...")
    val model = StockSVMModel.trainModel(stockData)

    // Evaluate the model
    println("Evaluating model performance...")
    val accuracy = StockSVMModel.evaluateModel(model, stockData)
    println(s"Model accuracy: $accuracy")

    // Save the model if needed
    println("Saving the model...")
    model.save(Configuration.outputDirectory + "/svmModel")

    // Stop the Spark session
    spark.stop()
    println("Stock market prediction application completed.")
  }
}
