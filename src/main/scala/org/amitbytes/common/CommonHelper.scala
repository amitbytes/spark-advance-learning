package org.amitbytes.common

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.sql.{Connection, PreparedStatement}
import java.util.Properties

object CommonHelper {
  private def getSqlServerConnectionProperties() : Properties= {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("jdbcSqlServerUrl", "jdbc:sqlserver://your_server.database.windows.net:1433;database=your_database")
    connectionProperties.setProperty("user", "your_username")
    connectionProperties.setProperty("password", "your_password")
    connectionProperties.setProperty("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    connectionProperties
  }
  def readSqlData(sqlQuery: String)(implicit spark: SparkSession): DataFrame = {
    val connectionProperties = getSqlServerConnectionProperties()
    spark.read.jdbc(connectionProperties.getProperty("jdbcSqlServerUrl"), s"($sqlQuery) as subquery", connectionProperties)
  }
  def writeSqlData(df:DataFrame, tableName:String)(implicit spark: SparkSession): Unit = {
    if(df.isEmpty){
      throw new IllegalArgumentException("DataFrame is empty. Cannot write to SQL Server.")
    }
    val connectionProperties = getSqlServerConnectionProperties()
    df.write.mode("append").jdbc(connectionProperties.getProperty("jdbcSqlServerUrl"), tableName, connectionProperties)
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

  def executeUpdateSqlQuery(sqlQuery: String): Unit = {
    var connection: Connection = null
    var statement: PreparedStatement = null
    try {
      val connectionProperties = getSqlServerConnectionProperties()
      connection = java.sql.DriverManager.getConnection(connectionProperties.getProperty("jdbcSqlServerUrl"), connectionProperties)
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
