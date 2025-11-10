package org.amitbytes.transformtions
import org.apache.spark.sql.{DataFrame, SparkSession}


class FindWordCountFreQuencyTransformation extends BaseTransformation {

  /**
   * This method will be implemented by all the transformation classes
   *
   * @param df    Input DataFrame
   * @param spark Implicit SparkSession
   * @return Transformed DataFrame
   */
  override def runTransformation(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._
    val wordsDF = df.selectExpr("explode(split(value, ' ')) as word")
    val wordCountDF = wordsDF.selectExpr("lower(word) as word").groupBy("word").count().withColumnRenamed("count", "frequency")
    wordCountDF.orderBy($"frequency".desc)
  }

  override val transformationName: String = "FindWordCountFreQuencyTransformation"
}
