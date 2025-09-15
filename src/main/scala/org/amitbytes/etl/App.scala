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
      var customers_df = jdbcDatabaseHelpers.readSqlData("select * from customers", DataBases.CLASSICMODELS)
      customers_df.cache()
      println(f"Total data count: ${customers_df.count()}")
      jdbcDatabaseHelpers.writeSqlData(customers_df.limit(1), DataBases.TEMPDB,"customers",SaveMode.Overwrite)
      customers_df = customers_df.repartition(3)
      jdbcDatabaseHelpers.writePartitionBatchSqlData(customers_df, DataBases.TEMPDB,"customers",10)
      customers_df.unpersist()



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