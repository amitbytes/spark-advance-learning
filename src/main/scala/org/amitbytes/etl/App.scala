package org.amitbytes.etl

import org.apache.log4j.{Logger, LogManager, Level}
import org.apache.spark.sql.SparkSession

object App {
  def main(args: Array[String]): Unit = {
    // Set the log level to ERROR to reduce the amount of log output
    LogManager.getLogger("org").setLevel(Level.ERROR)
    val logger: Logger = Logger.getLogger(App.getClass.getName)
    val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark Advance Learning Project")
      .master("local[*]")
      .getOrCreate()
    try {
      println(f"Spark-version: ${spark.version}")
      logger.error(s"spark---version: ${spark.version}")
    } catch {
      case e: Exception =>
        logger.error("An error occurred while running the application", e)
        System.exit(1)
    }
    finally {
      spark.stop()
    }
  }
}