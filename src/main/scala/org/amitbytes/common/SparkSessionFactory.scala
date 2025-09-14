package org.amitbytes.common

import org.apache.spark.sql.SparkSession

trait SparkSessionFactory {
  implicit lazy val spark: SparkSession = SparkSession.builder()
    .config(SparkConfigLoader.getConfig)
    .enableHiveSupport() // if needed
    .getOrCreate()
}
