package org.amitbytes.transformtions
import org.apache.spark.sql.{SparkSession, DataFrame}

trait CurrentTransformationLogger {
  def logCurrentTransformation(transformationName: String): Unit = {
    println(s"Current Transformation: $transformationName")
  }
}

trait BaseTransformation {
  /**
   * This method will be implemented by all the transformation classes
   * @param df Input DataFrame
   * @param spark Implicit SparkSession
   * @return Transformed DataFrame
   */
  def runTransformation(df: DataFrame)(implicit spark: SparkSession): DataFrame
  val transformationName: String
}
