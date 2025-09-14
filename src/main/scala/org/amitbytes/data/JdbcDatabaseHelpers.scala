package org.amitbytes.data

import org.amitbytes.common.DataBases.DataBases
import org.amitbytes.common.{JdbcConfigLoader, JdbcSettings}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement}

class JdbDatabaseHelpers(implicit  val spark: SparkSession) {
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
