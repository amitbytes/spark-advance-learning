package org.amitbytes.transformtions

class CustomEmailTransforamtion extends BaseTransformation with CurrentTransformationLogger {
  override def runTransformation(df: org.apache.spark.sql.DataFrame)(implicit spark: org.apache.spark.sql.SparkSession): org.apache.spark.sql.DataFrame = {
    logCurrentTransformation("CustomEmailTransforamtion")
    import org.apache.spark.sql.functions._
    df.withColumn("email", concat(col("first_name"), lit("."), col("last_name"), lit("@example.com")))
  }
  override def logCurrentTransformation(transformationName: String): Unit = println(s"Transformation log: ${this.transformationName}")
  override val transformationName: String = "CustomEmailTransforamtion is in progress"

}
