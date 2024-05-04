package features

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, lit, stddev}

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
   * Calculates the Volume Weighted Average Price (VWAP) for stock data.
   *
   * VWAP is a trading benchmark used by traders that gives the average price a stock
   * has traded at throughout the day, based on both volume and price. It is important
   * because it provides traders with insight into both the trend and value of a security.
   *
   * @param data DataFrame containing the initial stock data.
   * @param days Number of days over which to calculate the VWAP.
   * @return DataFrame with VWAP added as a new column.
   */
  def addVWAP(data: DataFrame, days: Int): DataFrame = {
    val windowSpec = Window.partitionBy("stockName").orderBy("date").rowsBetween(-days + 1, 0)
    val vwap = sum(col("close") * col("volume")).over(windowSpec) / sum(col("volume")).over(windowSpec)
    data.withColumn(s"vwap_$days", vwap)
  }

  /**
   * Calculates the Moving Average Convergence Divergence (MACD) for stock data.
   *
   * @param data DataFrame containing the initial stock data.
   * @param shortPeriod the short period EMA days.
   * @param longPeriod the long period EMA days.
   * @param signalPeriod the signal line EMA days.
   * @return DataFrame with MACD and MACD Histogram added.
   */
  def addMACD(data: DataFrame, shortPeriod: Int, longPeriod: Int, signalPeriod: Int): DataFrame = {
    // Calculate short-term EMA
    val shortEMA = addEMA(data, shortPeriod)

    // Calculate long-term EMA
    val longEMA = addEMA(shortEMA, longPeriod)

    // Calculate MACD line
    val macdLine = longEMA.withColumn(s"macd_${shortPeriod}_${longPeriod}", col(s"ema_$shortPeriod") - col(s"ema_$longPeriod"))

    // Calculate signal line (9-day EMA of MACD line)
    val signalLine = addEMA(macdLine, signalPeriod)

    // Calculate MACD Histogram
    val macdHistogram = signalLine.withColumn(s"macd_hist_${shortPeriod}_${longPeriod}_${signalPeriod}", col(s"macd_${shortPeriod}_${longPeriod}") - col(s"ema_$signalPeriod"))

    macdHistogram
  }

  /**
   * Calculates Bollinger Bands for stock data.
   *
   * @param data DataFrame containing the initial stock data.
   * @param days Number of days to calculate the Simple Moving Average.
   * @param stddevs Number of standard deviations for bandwidth.
   * @return DataFrame with Bollinger Bands (upper and lower) added.
   */

  def addBollingerBands(data: DataFrame, days: Int, stddevs: Int): DataFrame = {
    val windowSpec = Window.partitionBy("stockName").orderBy("date").rowsBetween(-days + 1, 0)
    val sma = avg("close").over(windowSpec)
    val stdDev = stddev("close").over(windowSpec)

    val withSMA = data.withColumn("sma", sma)
    val withStdDev = withSMA.withColumn("stddev", stdDev)

    val upperBand = withStdDev.withColumn("upperBand", col("sma") + (lit(stddevs) * col("stddev")))
    val lowerBand = upperBand.withColumn("lowerBand", col("sma") - (lit(stddevs) * col("stddev")))

    lowerBand
  }

  def addROC(data: DataFrame, days: Int): DataFrame = {
    val windowSpec = Window.partitionBy("stockName").orderBy("date")
    val previousPrice = lag(col("close"), days).over(windowSpec)
    data.withColumn(s"roc_$days", (col("close") - previousPrice) / previousPrice * 100)
  }

  /**
   * Calculates the Average True Range (ATR) to measure stock volatility.
   *
   * @param data DataFrame containing the initial stock data.
   * @param days Number of days over which to calculate the ATR.
   * @return DataFrame with Average True Range added.
   */
  def addATR(data: DataFrame, days: Int): DataFrame = {
    val windowSpec = Window.partitionBy("stockName").orderBy("date")
    val windowSpecDays = Window.partitionBy("stockName").orderBy("date").rowsBetween(-days + 1, 0)

    val trueRange = greatest(
      col("high") - col("low"),
      abs(col("high") - lag(col("close"), 1).over(windowSpec)),
      abs(col("low") - lag(col("close"), 1).over(windowSpec))
    ).alias("trueRange")

    data.withColumn("trueRange", trueRange)
      .withColumn(s"atr_$days", avg(col("trueRange")).over(windowSpecDays))
      .drop("trueRange")
  }


  /**
   * Calculates the Stochastic Oscillator for stock data.
   *
   * @param data DataFrame containing the initial stock data.
   * @param days Number of days to calculate the percentage price position relative to the high-low range.
   * @return DataFrame with Stochastic Oscillator added.
   */
  def addStochasticOscillator(data: DataFrame, days: Int): DataFrame = {
    val windowSpec = Window.partitionBy("stockName").orderBy("date").rowsBetween(-days + 1, 0)

    val highestHigh = max(col("high")).over(windowSpec)
    val lowestLow = min(col("low")).over(windowSpec)

    data.withColumn(s"stochastic_$days", ((col("close") - lowestLow) / (highestHigh - lowestLow)) * 100)
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
    val withVWAP = addVWAP(withRSI, 15)
    val withMACD = addMACD(withVWAP, 12, 26, 9)
    val withBollingerBands = addBollingerBands(withMACD, 20, 2)
    withBollingerBands
  }
}
