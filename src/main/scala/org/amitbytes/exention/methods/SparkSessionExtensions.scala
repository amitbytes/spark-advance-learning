package org.amitbytes.exention.methods

import org.amitbytes.common.DatabasesEnum.DataBases
import org.amitbytes.common.JdbcConfigLoader
import org.apache.spark.sql.{DataFrame, SparkSession, Row}
org.apache.spark.sql.types.StructType
import org.joda.time.DateTime
import java.text.SimpleDateFormat

/*
* This object contains extension methods for SparkSession
* */
object SparkSessionExtensions {

  private final val DEFAULT_FETCH_SIZE: Int = 50000
  private final val DEFAULT_NUM_PARTITIONS: Int = 10

  implicit class RichSparkMethods(val spark: SparkSession) extends AnyVal {

    // <editor-fold desc="Spark DataFrame Debugging Helpers">

    /*
    * print spark version
    */
    def printSparkVersion(): Unit = {
      println(f"spark version: ${spark.version}")
    }
    // </editor-fold>

    def isSparkSessionActive: Boolean = {
      try {
        spark.range(1).count() // simple action to check if SparkSession is active
        true
      } catch {
        case _: Exception => false
      }
    }

    def dropTable(hiveTableName: String): Unit = {
      spark.sql(s"DROP TABLE IF EXISTS $hiveTableName")
    }

    def countTaleRows(hiveTableName: String): Long = {
      val df = spark.sql(s"SELECT COUNT(*) as count FROM $hiveTableName")
      df.collect().head.getAs[Long]("count")
    }

    def truncateTable(hiveTableName: String): Unit = {
      spark.sql(s"TRUNCATE TABLE $hiveTableName")
    }

    def tableExists(hiveTableName: String): Boolean = {
      spark.catalog.tableExists(hiveTableName)
    }

    def createDatabaseIfNotExists(databaseName: String, location: Option[String] = None): Unit = {
      val locationClause = location.map(loc => s"LOCATION '$loc'").getOrElse("")
      spark.sql(s"CREATE DATABASE IF NOT EXISTS $databaseName $locationClause")
    }

    def useDatabase(databaseName: String): Unit = {
      spark.sql(s"USE $databaseName")
    }

    def renameTable(oldTableName: String, newTableName: String): Boolean = {
      if (spark.tableExists(oldTableName)) {
        spark.sql(s"ALTER TABLE $oldTableName RENAME TO $newTableName")
        return true
      }
      false
    }

    def getCurrentDatabase: String = {
      spark.catalog.currentDatabase
    }

    def listTables(databaseName: String): Array[String] = {
      spark.catalog.listTables(databaseName).collect().map(_.name)
    }

    def listDatabases(): Array[String] = {
      spark.catalog.listDatabases().collect().map(_.name)
    }

    def dropDatabase(databaseName: String, cascade: Boolean = false): Unit = {
      val cascadeClause = if (cascade) "CASCADE" else "RESTRICT"
      spark.sql(s"DROP DATABASE IF EXISTS $databaseName $cascadeClause")
    }

    def createEmptyDataFrame(schema: String): DataFrame = {
      spark.createDataFrame(spark.sparkContext.emptyRDD[org.apache.spark.sql.Row], StructType.fromDDL(schema))
    }

    def createDataFrame(data: Seq[Product], schema: String): DataFrame = {
      val rdd = spark.sparkContext.parallelize(data.map { row => Row.fromSeq(row.productIterator.toSeq) })
      spark.createDataFrame(rdd, org.apache.spark.sql.types.StructType.fromDDL(schema))
    }

    /*
    * read data from sql database
    * @param sqlQuery sql query to execute
    * @param database database to connect
    * @param fetchSize number of rows to fetch at a time
    * */
    def readSqlData(sqlQuery: String, database: DataBases, fetchSize: Int = DEFAULT_FETCH_SIZE): DataFrame = {
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
    def readSqlDataByPartitions(sqlQuery: String, database: DataBases, partitionColumn: String, lowerBound: Long, upperBound: Long, numPartitions: Int = DEFAULT_NUM_PARTITIONS, fetchSize: Int = DEFAULT_FETCH_SIZE): DataFrame = {
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
    def readSqlDataByPartitions(sqlQuery: String, database: DataBases, partitionColumn: String, lowerBound: DateTime, upperBound: DateTime, numPartitions: Int): DataFrame = {
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
