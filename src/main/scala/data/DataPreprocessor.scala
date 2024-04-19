package data

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

/**
 * Object responsible for preprocessing stock data.
 */
object DataPreprocessor {

  /**
   * Cleans the data by handling missing values and ensuring data type consistency.
   *
   * @param data DataFrame containing the stock data.
   * @return DataFrame after applying cleaning operations.
   */
  def cleanData(data: DataFrame): DataFrame = {
    // Log the initiation of data cleaning
    println("Cleaning data...")

    // Handle missing values - Options could be 'drop', 'fill', or more sophisticated approaches
    val dataNoMissing = data.na.drop()

    // Ensure correct data types, e.g., converting 'date' from String to DateType if not already handled
    val dataCorrectTypes = dataNoMissing.withColumn("Date", to_date(col("Date"), "dd-MM-yyyy"))

    // Example of trimming extra spaces in string type columns if necessary
    val cleanedData = dataCorrectTypes.withColumn("stockName", trim(col("stockName")))

    // Additional cleaning steps can be added here
    cleanedData
  }

  /**
   * Further preprocess steps can be added such as normalizing or scaling certain columns,
   * generating new derived columns, or filtering specific rows based on certain criteria.
   */
}
