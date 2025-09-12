package org.amitbytes.transformtions
import  org.apache.spark.sql.{SparkSession, DataFrame}
sealed class CustomCityTransforamtion extends BaseTransformation with CurrentTransformationLogger {
  override def runTransformation(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logCurrentTransformation("CustomEmailTransforamtion")
    import org.apache.spark.sql.functions._
    df.withColumn("city", lit("Ahmedabad"))
  }
  override def logCurrentTransformation(transformationName: String): Unit = println(s"Transformation log: ${this.transformationName}")
  override val transformationName: String = "CustomCityTransforamtion is in progress"
}
