package processing

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Aggregator {

  /**
   * Aggregates data by symbol to compute daily averages and total volumes.
   *
   * @param df The input DataFrame containing stock data.
   * @return DataFrame with daily averages and total volume per symbol.
   */
  def aggregateData(df: DataFrame): DataFrame = {
    df.groupBy("Symbol", "Date")
      .agg(
        avg("Close").as("AverageClose"),
        sum("Volume").as("TotalVolume"),
        max("High").as("DailyHigh"),
        min("Low").as("DailyLow")
      )
  }

  /**
   * Aggregates data to compute monthly statistics for each stock symbol.
   *
   * @param df The input DataFrame containing stock data.
   * @return DataFrame with monthly statistics including average close and total volume.
   */
  def monthlyStatistics(df: DataFrame): DataFrame = {
    val monthYear = date_format(col("Date"), "yyyy-MM")

    df.withColumn("MonthYear", monthYear)
      .groupBy("Symbol", "MonthYear")
      .agg(
        avg("Close").as("MonthlyAverageClose"),
        sum("Volume").as("MonthlyTotalVolume"),
        max("High").as("MonthlyHigh"),
        min("Low").as("MonthlyLow")
      )
  }
}
