package org.amitbytes.common

import org.amitbytes.common.DataBases.DataBases
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

object CommonHelper {
  private def getSqlServerConnectionProperties() : Properties= {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("jdbcMySqlUrl", "jdbc:mysql://localhost:3306/classicmodels")
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "admin@123")
    connectionProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    connectionProperties
  }

  def readSqlData(sqlQuery: String, dataBases: DataBases)(implicit spark: SparkSession): DataFrame = {
    val jdbcSettings: JdbcSettings =  JdbcConfigLoader.loadDb(dataBases)
    spark.read.jdbc(jdbcSettings.url, s"($sqlQuery) as subquery", jdbcSettings.toProperties)
  }
  def readSqlDataByPartition(sqlQuery: String, dataBases: DataBases, partitionColumn:String, lowerBound:Long, upperBound:Long, numPartitions:Int)(implicit spark: SparkSession): DataFrame = {
    val jdbcSettings: JdbcSettings =  JdbcConfigLoader.loadDb(dataBases)
    spark.read.jdbc(jdbcSettings.url, s"($sqlQuery) as subquery", partitionColumn, lowerBound, upperBound, numPartitions, jdbcSettings.toProperties)
  }
  def writeBatchSqlData(df:DataFrame, dataBases: DataBases, tableName:String, batchSize:Int, saveMode: SaveMode): Unit = {
    val jdbcSettings: JdbcSettings =  JdbcConfigLoader.loadDb(dataBases)
    df.write.mode(saveMode).option("batchsize", batchSize).jdbc(jdbcSettings.url, tableName, jdbcSettings.toProperties)
  }
  def writePartitionBatchSqlData(df:DataFrame, dataBases: DataBases, tableName:String, batchSize:Int): Unit = {
    var connection: Connection = null
    var preparedStatement: PreparedStatement = null
    try{
      val jdbcSettings: JdbcSettings =  JdbcConfigLoader.loadDb(dataBases)
      val columns = df.columns
      df.foreachPartition{partition: Iterator[org.apache.spark.sql.Row] => {
        connection = DriverManager.getConnection(jdbcSettings.url, jdbcSettings.toProperties)
        connection.setAutoCommit(false)
        val insertQuery = s"INSERT INTO $tableName (${columns.mkString(",")}) VALUES (${columns.map(_ => "?").mkString(",")})"
        preparedStatement = connection.prepareStatement(insertQuery)
        var count = 0
        partition.foreach(row => {
          for (i <- 1 to columns.length) {
            preparedStatement.setObject(i, row.getAs(columns(i-1)) )
          }
          preparedStatement.addBatch()
          count += 1
          if (count % batchSize == 0) {
            preparedStatement.executeBatch()
            connection.commit()
          }
        })
        if (count % batchSize != 0) {
          preparedStatement.executeBatch()
          connection.commit()
        }
      }}
    }
    catch {
      case e: Exception => {
        println(e.getMessage)
        throw e
      }
    }
    finally {
      if(preparedStatement != null) preparedStatement.close()
      if(connection != null) connection.close()
    }
  }
  def writeSqlData(df:DataFrame, dataBases: DataBases, tableName:String, saveMode: SaveMode): Unit = {
    val jdbcSettings: JdbcSettings =  JdbcConfigLoader.loadDb(dataBases)
    df.write.mode(saveMode).jdbc(jdbcSettings.url, tableName, jdbcSettings.toProperties)
  }
  def createEmptyDataFrame(schema: StructType)(implicit spark: SparkSession): DataFrame = {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row],schema)
  }
  def isDataFrameEmpty(df: DataFrame): Boolean = {
    df.head(1).length==0
  }
  def isDataFrameEmptyNew(df: DataFrame): Boolean = {
    df.isEmpty
  }
  def isRddEmpty(df: DataFrame): Boolean = {
    df.rdd.isEmpty()
  }
  def executeUpdateSqlQuery(sqlQuery: String, dataBases: DataBases): Unit = {
    var connection: Connection = null
    var statement: PreparedStatement = null
    try {
      val jdbcSettings = JdbcConfigLoader.loadDb(dataBases)
      connection = java.sql.DriverManager.getConnection(jdbcSettings.url, jdbcSettings.toProperties)
      statement = connection.prepareStatement(sqlQuery)
      statement.executeUpdate()
    }
    catch {
      case e: Exception => {
        println(e.getMessage)
        throw e
      }
    }
    finally {
      if (statement != null) statement.close()
      if (connection != null) connection.close()
    }
  }
}
