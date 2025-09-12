package org.amitbytes.transformtions
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed class CustomTransformation extends BaseTransformation with CurrentTransformationLogger {
  override def runTransformation(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    logCurrentTransformation("CustomTransformation")
    df.withColumn("transformatoin_type", lit("CustomTransformation"))
  }
  override def logCurrentTransformation(transformationName: String): Unit = println(s"Transformation log: ${this.transformationName}")
  override val transformationName: String = "CustomTransformation is in progress"
}
