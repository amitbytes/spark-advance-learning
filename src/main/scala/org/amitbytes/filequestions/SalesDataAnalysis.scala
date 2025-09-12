package org.amitbytes.filequestions
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.SparkSession

sealed class SalesDataAnalysis {
  def executeAnalysis(filePath: String)(implicit spark: SparkSession): Unit = {
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    import spark.implicits._
    val salesCsvDataSchema = "transactiondate STRING,transactionid INT, retailerid STRING, description STRING, amount DOUBLE,cityid INT"
    val salesCsvDataDF = spark.read.format("csv").option("header", "true").schema(salesCsvDataSchema).load(filePath)
    val finalSalesDataDF = salesCsvDataDF.withColumn("order_date",F.to_date($"transactiondate","yyyy-MM-dd"))
      .withColumn("year_month", F.date_format($"order_date","yyyy-MM"))

    /*finalSalesDataDF.groupBy("year_month")
      .agg(F.round(F.sum("amount"),2).alias("total_sales_amount"))
      .orderBy(F.desc("year_month"))
      .limit(100)*/
      finalSalesDataDF.groupBy("cityid")
      .agg(F.round(F.sum("amount"),2).alias("total_sales_amount"),
        F.count("transactionid").alias("total_transactions"),
        F.round(F.avg("amount"),2).alias("avg_sales_amount"))
        .orderBy(F.desc("total_sales_amount"))
        .limit(50).show()

  }
}
