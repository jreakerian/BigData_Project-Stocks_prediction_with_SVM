package processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Transformer {

  /**
   * Applies basic transformations to the DataFrame such as normalization or creating new features.
   *
   * @param df The input DataFrame.
   * @return DataFrame with applied transformations.
   */
  def applyTransformations(df: DataFrame): DataFrame = {
    // Example transformation: Calculate the moving average
    df.withColumn("movingAverage", avg("Close").over(Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-10, 0)))
  }

  /**
   * Example of a more complex transformation: Calculate Relative Strength Index (RSI)
   *
   * @param df The input DataFrame.
   * @param period The period over which to calculate RSI.
   * @return DataFrame with the RSI column added.
   */
  def calculateRSI(df: DataFrame, period: Int = 14): DataFrame = {
    val windowSpec = Window.partitionBy("Symbol").orderBy("Date").rowsBetween(-period, -1)

    // Calculate price change
    val priceChange = df.withColumn("priceChange", col("Close") - lag("Close", 1).over(Window.partitionBy("Symbol").orderBy("Date")))

    // Calculate gains and losses
    val gainsAndLosses = priceChange.withColumn("gain", when(col("priceChange") > 0, col("priceChange")).otherwise(0))
      .withColumn("loss", when(col("priceChange") < 0, -col("priceChange")).otherwise(0))

    // Calculate average gains and losses
    val avgGainsAndLosses = gainsAndLosses.withColumn("averageGain", avg("gain").over(windowSpec))
      .withColumn("averageLoss", avg("loss").over(windowSpec))

    // Calculate RS and RSI
    val rsAndRsi = avgGainsAndLosses.withColumn("RS", col("averageGain") / col("averageLoss"))
      .withColumn("RSI", lit(100) - (lit(100) / (col("RS") + 1)))

    rsAndRsi.drop("priceChange", "gain", "loss", "averageGain", "averageLoss", "RS")
  }

}
