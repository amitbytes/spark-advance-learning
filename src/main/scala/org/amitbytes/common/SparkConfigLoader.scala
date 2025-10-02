package org.amitbytes.common

import org.apache.spark.SparkConf
import java.util.Properties
import scala.io.Source

object SparkConfigLoader {
  def getConfig: SparkConf = {
    val sparkConf = new SparkConf()
    val properties = new Properties
    properties.load(Source.fromFile("spark.conf").bufferedReader())
    properties.forEach((k, v) => sparkConf.set(k.toString, v.toString))
    sparkConf
  }
}
