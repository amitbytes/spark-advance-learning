package org.amitbytes.filequestions
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime

import java.sql.Date
import scala.collection.mutable.ListBuffer

case class User(id: String, name: String, age: Int, signup_date: String)

sealed class DataCleanAnalysis {
  def executeDataCleanAnalysis(filePath: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val sampleDataDF = spark.read.format("csv").option("header","true").option("inferSchema","false").load(filePath)
    val usersDF = sampleDataDF.withColumn("age",F.expr("try_cast(age as int)"))
      .withColumn("signup_date",F.expr("to_date(signup_date,'yyyy-MM-dd')"))
      .where($"age".isNotNull && $"signup_date".isNotNull)
    usersDF.show()
  }
}
