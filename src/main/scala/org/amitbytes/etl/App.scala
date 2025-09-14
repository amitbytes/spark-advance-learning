package org.amitbytes.etl
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}
import org.amitbytes.common
import org.amitbytes.common.{CommonHelper, DataBases, SparkConfigLoader, SparkSessionFactory}
import org.apache.spark.sql.expressions.Window
import org.amitbytes.transformtions.{BaseTransformation, TransformationFactory}
import org.slf4j.LoggerFactory
import org.amitbytes.filequestions.{DataCleanAnalysis, NestedJsonExample, SalesDataAnalysis, ScalaLearning, WordFrequencyCount}
import org.amitbytes.data.JdbcDatabaseHelpers

object App extends SparkSessionFactory {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(getClass)

    try {
      logger.info("Transformation started")
      import spark.implicits._
      /*
      val schema ="employee_id STRING,department_id STRING,name STRING,age STRING,gender STRING,salary STRING,hire_date STRING"
      var emp_df = spark.read.format("csv").schema(schema).option("header","true").load("input/csvs/emp.csv")
      emp_df = emp_df.withColumn("name_gender",F.concat_ws(" ",$"name",$"gender"))
      val partition_window = Window.partitionBy($"department_id").orderBy($"salary".desc)
      emp_df.withColumn("rank",F.rank().over(partition_window)).filter(F.expr("rank=1")).show(false)*/
      val jdbcDatabaseHelpers = new JdbcDatabaseHelpers()
      var customers_df = jdbcDatabaseHelpers.readSqlData("select * from customers", DataBases.CLASSICMODELS).withColumn("time_stamp", F.current_timestamp())
      customers_df = customers_df.repartition(3)
      jdbcDatabaseHelpers.writePartitionBatchSqlData(customers_df, DataBases.TEMPDB,"customers",10)
    } catch {
      case e: Exception =>
        println(e.getMessage)
        logger.error("Error occurred: ", e)
        System.exit(1)
    }
    finally {
      scala.io.StdIn.readLine("Hit enter to exit")
      spark.stop()
    }
  }
}