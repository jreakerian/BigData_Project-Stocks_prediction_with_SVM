package data

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
 * Object responsible for loading stock data.
 */
object DataLoader {

  /**
   * Load stock data from a specified directory.
   *
   * @param spark SparkSession instance.
   * @param path Path to the directory containing stock data files.
   * @return DataFrame containing the loaded stock data.
   */
  def loadData(spark: SparkSession, path: String): DataFrame = {
    import spark.implicits._

    println(s"Loading data from: $path")

    // Load all CSV files from the specified directory
    val stockData = spark.read
      .option("header", "true")  // Assuming files have a header row
      .option("inferSchema", "true")  // Enable schema inference
      .csv(path)

    // Add a column for the stock name by extracting it from the file path
    val dataWithStockName = stockData.withColumn("stockName", getStockName(input_file_name()))

    dataWithStockName
  }


  /**
   * Extract the stock name from the file path.
   *
   * @param filePath Column containing the full path of each file.
   * @return Column with extracted stock name.
   */
  def getStockName(filePath: org.apache.spark.sql.Column): org.apache.spark.sql.Column = {
    // Assuming file names are like 'AAPL.csv', extracts 'AAPL' as the stock name
    regexp_extract(filePath, ".*/([^/]+)\\.txt", 1)
  }
}
