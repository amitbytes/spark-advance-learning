package org.amitbytes.etl
import org.apache.spark.sql.{SaveMode, SparkSession, functions => F}
import org.amitbytes.common.{CommonHelper, DatabasesEnum, SparkConfigLoader, SparkSessionFactory}
import org.apache.spark.sql.expressions.Window
import org.amitbytes.transformtions.{BaseTransformation, TransformationFactory}
import org.slf4j.LoggerFactory
import org.amitbytes.exention.methods.{AllExtensionMethdods, DataFrameImplicits, SparkSessionExtensions}
import org.amitbytes.filequestions.{DataCleanAnalysis, NestedJsonExample, SalesDataAnalysis, ScalaLearning, WordFrequencyCount}
import org.amitbytes.data.SparkSessionWrapper
import org.amitbytes.exention.methods.SparkSessionExtensions._
import org.amitbytes.exention.methods.DataframeExtensions._

//scopt.OptionParser
//com.typesafe.scalalogging.LazyLogging

object App extends SparkSessionFactory {

  def main(args: Array[String]): Unit = {

    val logger = LoggerFactory.getLogger(getClass)

    try {
      logger.info("Transformation started")
      spark.printSparkVersion()
      import spark.implicits._

      val schema ="employee_id STRING,department_id STRING,name STRING,age STRING,gender STRING,salary STRING,hire_date STRING"
      var emp_df = spark.read.format("csv").schema(schema).option("header","true").load("input/csvs/emp.csv")
      emp_df = emp_df.withColumn("name_gender",F.concat_ws(" ",$"name",$"gender"))
      emp_df = spark.createEmptyDataFrame(schema)
      if(!emp_df.isEmpty()){
        println("Dataframe is not empty")
      }else{
        println("Dataframe is empty")
      }
      logger.info("Transformation completed")
      /*
      * --class org.amitbytes.etl.App --jars s3://emr-serverless-learning-studio/jarfiles/mysql-connector-j-8.0.33.jar,s3://emr-serverless-learning-studio/jarfiles/HikariCP-7.0.2.jar,s3://emr-serverless-learning-studio/jarfiles/config-1.4.3.jar --files s3://emr-serverless-learning-studio/appfiles/application.conf,s3://emr-serverless-learning-studio/appfiles/spark.conf --conf spark.hadoop.hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory
      * */
    } catch {
      case e: Exception =>
        println(e.getMessage)
        logger.error("Error occurred: ", e)
        System.exit(1)
    }
    finally {
      //scala.io.StdIn.readLine("Hit enter to exit")
      spark.stop()
    }
  }
}