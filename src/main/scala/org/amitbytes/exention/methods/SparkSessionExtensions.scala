package org.amitbytes.exention.methods
import org.amitbytes.common.DatabasesEnum.DataBases
import org.amitbytes.common.JdbcConfigLoader
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.joda.time.DateTime
import java.text.SimpleDateFormat

/*
* This object contains extension methods for SparkSession
* */
object SparkSessionExtensions {

  private val DEFAULT_FETCH_SIZE: Int=50000
  private val DEFAULT_NUM_PARTITIONS: Int=10

  implicit class RichSparkMethods(val spark: SparkSession) extends AnyVal {
    /*
    * print spark version
    * */
    def printSparkVersion(): Unit = {
      println(f"spark version: ${spark.version}")
    }

    /*
    * read data from sql database
    * @param sqlQuery sql query to execute
    * @param database database to connect
    * @param fetchSize number of rows to fetch at a time
    * */
    def readSqlData(sqlQuery: String, database: DataBases, fetchSize: Int=DEFAULT_FETCH_SIZE): DataFrame = {
      val jdbcSettings = JdbcConfigLoader.loadDb(database)
      spark.read.format("jdbc")
        .option("url", jdbcSettings.url)
        .option("dbtable", s"($sqlQuery) as subquery")
        .option("user", jdbcSettings.user)
        .option("password", jdbcSettings.password)
        .option("driver", jdbcSettings.driver)
        .option("fetchsize", fetchSize)
        .load()
    }

    /*
    * read data from sql database by partitioning (when partition column is numeric)
    * @param sqlQuery sql query to execute
    * @param database to connect
    * @param partitionColumn column to partition the data
    * @param lowerBound lower bound of the partition column
    * @param upperBound upper bound of the partition column
    * @param numPartitions number of partitions
    * @param fetchSize number of rows to fetch at a time
    * */
    def readSqlDataByPartitions(sqlQuery: String, database: DataBases, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int=DEFAULT_NUM_PARTITIONS, fetchSize: Int=DEFAULT_FETCH_SIZE): DataFrame = {
      val jdbcSettings = JdbcConfigLoader.loadDb(database)
      spark.read.format("jdbc")
        .option("url", jdbcSettings.url)
        .option("dbtable", s"($sqlQuery) as subquery")
        .option("user", jdbcSettings.user)
        .option("password", jdbcSettings.password)
        .option("driver", jdbcSettings.driver)
        .option("partitionColumn", partitionColumn)
        .option("lowerBound", lowerBound)
        .option("upperBound", upperBound)
        .option("numPartitions", numPartitions)
        .load()
    }

    /*
    * read data from sql database by partitioning (when partition column is date or timestamp)
    * @param sqlQuery sql query to execute
    * @param database to connect
    * @param partitionColumn column to partition the data
    * @param lowerBound lower bound of the partition column
    * @param upperBound upper bound of the partition column
    * @param numPartitions number of partitions
    *
    * */
    def readSqlDataByPartitions(sqlQuery: String, database: DataBases, partitionColumn: String,lowerBound: DateTime, upperBound: DateTime, numPartitions: Int): DataFrame = {
      val jdbcSettings = JdbcConfigLoader.loadDb(database)
      val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val minDateStr = dateFormat.format(lowerBound.toDate)
      val maxDateStr = dateFormat.format(upperBound.toDate)
      spark.read.format("jdbc")
        .option("url", jdbcSettings.url)
        .option("dbtable", s"($sqlQuery) as subquery")
        .option("user", jdbcSettings.user)
        .option("password", jdbcSettings.password)
        .option("driver", jdbcSettings.driver)
        .option("partitionColumn", partitionColumn)
        .option("lowerBound", minDateStr)
        .option("upperBound", maxDateStr)
        .option("numPartitions", numPartitions)
        .option("fetchsize", DEFAULT_FETCH_SIZE)
        .load()
    }
  }
}
