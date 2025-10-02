package org.amitbytes.exention.methods
import org.amitbytes.common.DatabasesEnum.DataBases
import org.amitbytes.data.HikariCPDataSource
import org.apache.spark.sql.{DataFrame, SaveMode}
import java.sql.{Connection, PreparedStatement, SQLException}

/*
 * This object contains extension methods for DataFrame
 */
object DataframeExtensions {

  private val DEFAULT_BATCH_SIZE: Int = 5000

  implicit class RichDataFrame(df: DataFrame) {

    /*
     * print schema and count of the dataframe
     * */
    def printSchemaAndCount(): Unit = {
      df.printSchema()
      println(df.count())
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
    def writeSqlDataByPartition(database: DataBases, tableName: String, batchSize: Int=DEFAULT_BATCH_SIZE): Unit = {
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
