package org.amitbytes.transformtions
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, SparkSession}

sealed class DefaultTransformation extends BaseTransformation with CurrentTransformationLogger {
  override def runTransformation(df: DataFrame)(implicit spark: SparkSession): org.apache.spark.sql.DataFrame = {
    logCurrentTransformation("DefaultTransformation")
    df.withColumn("transformatoin_type", lit("DefaultTransformation"))
  }
  override def logCurrentTransformation(transformationName: String): Unit = println(s"Transformation log: ${this.transformationName}")
  override val transformationName: String = "DefaultTransformation is in progress"
}
