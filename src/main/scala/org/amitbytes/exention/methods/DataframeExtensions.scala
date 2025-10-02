package org.amitbytes.exention.methods

import org.amitbytes.common.DatabasesEnum.DataBases
import org.amitbytes.data.HikariCPDataSource
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.storage.StorageLevel

import java.sql.{Connection, PreparedStatement, SQLException}

/*
 * This object contains extension methods for DataFrame
 */
object DataframeExtensions {

  private final val DEFAULT_BATCH_SIZE: Int = 5000

  implicit class RichDataFrame(df: DataFrame) {

    /*
     * print schema and count of the dataframe
     * */
    def printSchemaAndCount(): Unit = {
      df.printSchema()
      println(df.count())
    }

    /*
     * print schema in pretty format
     * */
    def printSchemaPretty(): Unit = {
      println("Schema:")
      df.schema.treeString.split("\n").foreach(ln => println(s"  $ln"))
    }
    /*
     * show data of the dataframe
     * @param numRows number of rows to show
     * @param truncate if true truncate the data else show full data
     * */
    def showData(numRows: Int = 20, truncate: Boolean = false): Unit = {
      df.show(numRows, truncate)
    }
    /*
     * show full data of the dataframe
     * */
    def showData(): Unit = {
      df.show(false)
    }
    /*
     * check if dataframe is empty
     * */
    def isEmpty(): Boolean = {
      df.head(1).isEmpty
    }
    /*
     * get first row of the dataframe as different data types
     * */
    def getHeadInt(): Int = {
      if (df.head(1).isEmpty) 0 else df.head(1).head.getAs[Int](0)
    }
    /*
     * get first row of the dataframe as different data types
     * */
    def getHeadLong(): Long = {
      if (df.head(1).isEmpty) 0L else df.head(1).head.getAs[Long](0)
    }
    /*
     * get first row of the dataframe as different data types
     * */
    def getHeadString(): String = {
      if (df.head(1).isEmpty) "" else df.head(1).head.getAs[String](0)
    }
    /*
     * get first row of the dataframe as different data types
     * */
    def getHeadDouble(): Double = {
      if (df.head(1).isEmpty) 0.0 else df.head(1).head.getAs[Double](0)
    }
    /*
     * get first row of the dataframe as different data types
     * */
    def getHeadDateTime(): java.sql.Timestamp = {
      if (df.head(1).isEmpty) new java.sql.Timestamp(0) else df.head(1).head.getAs[java.sql.Timestamp](0)
    }
    /*
     * get first row of the dataframe as different data types
     * */
    def getHeadBoolean(): Boolean = {
      if (df.head(1).isEmpty) false else df.head(1).head.getAs[Boolean](0)
    }
    /*
     * get first row of the dataframe
     * */
    def getHeadRow(): Row = {
      if (df.head(1).isEmpty) null else df.head(1).head
    }
    /*
     * get first row of the dataframe as different data types
     * */
    def getHeadRowToType[T](): T = {
      if (df.head(1).isEmpty) null.asInstanceOf[T] else df.head(1).head.asInstanceOf[T]
    }
    /*
     * save dataframe as table
     * @param tableName name of the table
     * @param saveMode save mode (Append, Overwrite, Ignore, ErrorIfExists)
     * */
    def saveAsTable(tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("delta").mode(saveMode).saveAsTable(tableName)
    }
    def saveAsExternalTable(tableName: String, path: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      saveAsDeltaExternalTable(tableName, path, saveMode)
    }
    def saveAsDeltaTable(tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.saveAsTable(tableName, saveMode)
    }
    def saveAsDeltaExternalTable(tableName: String, path: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("delta").mode(saveMode).option("path", path).saveAsTable(tableName)
    }
    def saveAsParquetTable(tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("parquet").mode(saveMode).saveAsTable(tableName)
    }
    def saveAsParquetExternalTable(tableName: String, path: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("parquet").mode(saveMode).option("path", path).saveAsTable(tableName)
    }
    def saveAsCsvTable(tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("csv").mode(saveMode).option("header", "true").saveAsTable(tableName)
    }
    def saveAsCsvExternalTable(tableName: String, path: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("csv").mode(saveMode).option("header", "true").option("path", path).saveAsTable(tableName)
    }
    def saveAsJsonTable(tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("json").mode(saveMode).saveAsTable(tableName)
    }
    def saveAsJsonExternalTable(tableName: String, path: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("json").mode(saveMode).option("path", path).saveAsTable(tableName)
    }

    def saveAsAvroTable(tableName: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("avro").mode(saveMode).saveAsTable(tableName)
    }
    def saveAsAvroExternalTable(tableName: String, path: String, saveMode: SaveMode = SaveMode.Overwrite): Unit = {
      df.write.format("avro").mode(saveMode).option("path", path).saveAsTable(tableName)
    }
    /*
     * cache dataframe and create temp view
     * @param tempViewName name of the temp view
     * @param isGlobal if true create global temp view else create temp view
     * */
    def cacheAndCreateTempView(tempViewName: String, isGlobal: Boolean = false): Unit = {
      df.persist(StorageLevel.MEMORY_AND_DISK) // Cache the DataFrame in memory and disk
      if (isGlobal) df.createOrReplaceGlobalTempView(tempViewName) else df.createOrReplaceTempView(tempViewName)
    }

    /*
     * write data to sql database in batch mode
     * @param database to connect
     * @param tableName table to write data
     * @param batchSize number of rows to write at a time
     * @param saveMode save mode (Append, Overwrite, Ignore, ErrorIfExists)
     * */
    def writeSqlData(database: DataBases, tableName: String, batchSize: Int = DEFAULT_BATCH_SIZE, saveMode: SaveMode = SaveMode.Append): Unit = {
      val jdbcSettings: org.amitbytes.common.JdbcSettings = org.amitbytes.common.JdbcConfigLoader.loadDb(database)
      df.write.mode(saveMode).option("batchsize", batchSize).jdbc(jdbcSettings.url, tableName, jdbcSettings.toProperties)
    }

    /*
     * write data to sql database in batch mode using foreachPartition
     * @param database to connect
     * @param tableName table to write data
     * @param batchSize number of rows to write at a time
     * */
    def writeSqlDataByPartition(database: DataBases, tableName: String, batchSize: Int = DEFAULT_BATCH_SIZE): Unit = {
      var connection: Connection = null
      var preparedStatement: PreparedStatement = null

      try {
        val columns = df.columns
        val insertQuery = s"INSERT INTO $tableName (${columns.mkString(",")}) VALUES (${columns.map(_ => "?").mkString(",")})"

        // Use foreachPartition to write data in batches per partition
        df.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] => {

          val hikariCpDataSource = HikariCPDataSource.getDataSource(database) // Initialize HikariCP connection pool
          connection = hikariCpDataSource.getConnection()
          connection.setAutoCommit(false)
          preparedStatement = connection.prepareStatement(insertQuery)
          var count = 0
          partition.foreach(row => {
            for (i <- 1 to columns.length) {
              preparedStatement.setObject(i, row.getAs(columns(i - 1)))
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
        }
        }
      }
      catch {
        case e: SQLException => {
          println(e.getMessage)
          throw e
        }
      }
      finally {
        if (preparedStatement != null) preparedStatement.close()
        if (connection != null) connection.close()
      }
    }
  }
}
