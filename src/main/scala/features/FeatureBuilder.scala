package features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Object for building features necessary for the stock market prediction model.
 */
object FeatureBuilder {

  /**
   * Calculates the Simple Moving Average (SMA) for a specified number of days.
   *
   * @param data DataFrame containing the stock data with 'date' and 'close' prices.
   * @param days Number of days over which to calculate the SMA.
   * @return DataFrame with the SMA feature added.
   */
  def addSMA(data: DataFrame, days: Int): DataFrame = {
    val windowSpec = Window.partitionBy("stockName").orderBy("date").rowsBetween(-days + 1, 0)
    data.withColumn(s"sma_$days", avg("close").over(windowSpec))
  }

  /**
   * Calculates the Exponential Moving Average (EMA) for a specified number of days.
   *
   * @param data DataFrame containing the stock data with 'date' and 'close' prices.
   * @param days Number of days over which to calculate the EMA.
   * @return DataFrame with the EMA feature added.
   */
  def addEMA(data: DataFrame, days: Int): DataFrame = {
    val alpha = 2.0 / (days + 1)
    val windowSpec = Window.partitionBy("stockName").orderBy("date")
    data.withColumn(s"ema_$days", avg("close").over(windowSpec) * alpha + lag("close", 1).over(windowSpec) * (1 - alpha))
  }

  /**
   * Calculates the Relative Strength Index (RSI) for a specified number of days.
   *
   * @param data DataFrame containing the stock data.
   * @param days Number of days over which to calculate the RSI.
   * @return DataFrame with the RSI feature added.
   */
  /**
   * Calculates the Relative Strength Index (RSI) for a specified number of days.
   *
   * @param data DataFrame containing the stock data with columns like 'date', 'close'.
   * @param days Number of days over which to calculate the RSI.
   * @return DataFrame with the RSI feature added.
   */
  def addRSI(data: DataFrame, days: Int): DataFrame = {
    // Define window for calculating lagged difference
    val windowSpec = Window.partitionBy("stockName").orderBy("date")

    // Calculate daily change in closing prices
    val dataWithChange = data.withColumn("change", col("close") - lag("close", 1).over(windowSpec))

    // Define windows for calculating average gains and losses
    val gainLossWindow = Window.partitionBy("stockName").orderBy("date").rowsBetween(-days + 1, 0)

    // Calculate gains and losses
    val gains = when(col("change") > 0, col("change")).otherwise(0)
    val losses = when(col("change") < 0, -col("change")).otherwise(0)

    // Calculate average gains and losses
    val avgGain = avg(gains).over(gainLossWindow)
    val avgLoss = avg(losses).over(gainLossWindow)

    // Compute RS and RSI
    val rs = avgGain / avgLoss
    val rsi = lit(100) - (lit(100) / (lit(1) + rs))

    // Add RSI column to DataFrame
    dataWithChange.withColumn(s"rsi_$days", rsi)
  }


  /**
   * Combines all feature additions into a single DataFrame.
   *
   * @param data DataFrame containing the initial stock data.
   * @return DataFrame with all features added.
   */
  def addAllFeatures(data: DataFrame): DataFrame = {
    val withSMA = addSMA(data, 15)
    val withEMA = addEMA(withSMA, 15)
    val withRSI = addRSI(withEMA, 14)
    withRSI
  }
}
