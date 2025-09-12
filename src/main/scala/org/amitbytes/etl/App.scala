package org.amitbytes.etl
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}
import org.amitbytes.common
import org.amitbytes.common.CommonHelper
import org.apache.spark.sql.expressions.Window
import org.amitbytes.transformtions.{BaseTransformation, TransformationFactory}
import org.slf4j.LoggerFactory
import org.amitbytes.filequestions.{DataCleanAnalysis, NestedJsonExample, SalesDataAnalysis, ScalaLearning, WordFrequencyCount}

import scala.io.Source

object App {
  def main(args: Array[String]): Unit = {
    val logger = LoggerFactory.getLogger(getClass)
    implicit lazy val spark: SparkSession = SparkSession
      .builder()
      .appName("Spark Advance Learning Project")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._
    try {
      logger.info("Transformation started")
      /*
      val schema ="employee_id STRING,department_id STRING,name STRING,age STRING,gender STRING,salary STRING,hire_date STRING"
      var emp_df = spark.read.format("csv").schema(schema).option("header","true").load("input/csvs/emp.csv")
      emp_df = emp_df.withColumn("name_gender",F.concat_ws(" ",$"name",$"gender"))
      val partition_window = Window.partitionBy($"department_id").orderBy($"salary".desc)
      emp_df.withColumn("rank",F.rank().over(partition_window)).filter(F.expr("rank=1")).show(false)*/
      val customers_df = CommonHelper.readSqlData("select * from customers")
      CommonHelper.writeSqlData(df=customers_df,targetDb = "tempdb", tableName = "customers", SaveMode.Overwrite)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        logger.error("Error occurred: ", e)
        System.exit(1)
    }
    finally {
      logger.info("Spark job completed")
      //spark.stop()
    }
    scala.io.StdIn.readLine("Hit enter to exit")
    spark.stop()
  }
}