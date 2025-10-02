package org.amitbytes.common

import org.amitbytes.common.DatabasesEnum.DataBases
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

object CommonHelper extends Serializable {
  private def getSqlServerConnectionProperties() : Properties= {
    val connectionProperties = new Properties()
    connectionProperties.setProperty("jdbcMySqlUrl", "jdbc:mysql://localhost:3306/classicmodels")
    connectionProperties.setProperty("user", "root")
    connectionProperties.setProperty("password", "admin@123")
    connectionProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    connectionProperties
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

}
