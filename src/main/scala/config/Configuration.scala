package config

import com.typesafe.config.{Config, ConfigFactory}

/**
 * Singleton object to manage configurations for loading and processing stock data.
 */
object Configuration {
  // Load the default configurations from the application.conf file
  private val config: Config = ConfigFactory.load()

  // Paths to data sources
  val stockDataDirectory: String = config.getString("stockDataDirectory")
  //val etfDataDirectory: String = config.getString("C:\\Downloads\\archive\\ETFs\\*.txt")
  val outputDirectory: String = config.getString("outputDirectory")

  // Model parameters for the SVM
  val svmKernelType: String = config.getString("model.svm.kernelType")
  val svmRegularization: Double = config.getDouble("model.svm.regularization")

  // Logging configurations
  val logFilePath: String = config.getString("log.filePath")

  // Additional settings can be defined as needed
}