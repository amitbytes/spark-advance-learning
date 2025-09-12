package org.amitbytes.common

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.{Connection, PreparedStatement}
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
  def readSqlData(sqlQuery: String)(implicit spark: SparkSession): DataFrame = {
    val connectionProperties = getSqlServerConnectionProperties()
    spark.read.jdbc(connectionProperties.getProperty("jdbcMySqlUrl"), s"($sqlQuery) as subquery", connectionProperties)
  }
  def writeSqlData(df:DataFrame, tableName:String, saveMode: SaveMode): Unit = {
    if(df.isEmpty){
      throw new IllegalArgumentException("DataFrame is empty. Cannot write to SQL Server.")
    }
    val connectionProperties = getSqlServerConnectionProperties()
    df.write.mode(saveMode).jdbc(connectionProperties.getProperty("jdbcSqlServerUrl"), tableName, connectionProperties)
  }
  def writeSqlData(df:DataFrame, targetDb: String, tableName:String, saveMode: SaveMode): Unit = {
    if(df.isEmpty){
      throw new IllegalArgumentException("DataFrame is empty. Cannot write to SQL Server.")
    }
    val connectionProperties = getSqlServerConnectionProperties()
    val jdbUrl=s"jdbc:mysql://localhost:3306/${targetDb}"
    df.write.mode(saveMode).jdbc(jdbUrl, tableName, connectionProperties)
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
