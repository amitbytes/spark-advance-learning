package org.amitbytes.exention.methods

import org.apache.spark.sql.DataFrame

object AllExtensionMethdods {
  implicit class StringExtneionMethods(val s: String) extends AnyVal {
    def ToInteger(): Int = {
      try {
        s.toInt
      } catch {
        case _: Exception => 0
      }
    }

    def ToDouble(): Double = {
      try {
        s.toDouble
      } catch {
        case _: Exception => 0.0
      }
    }
  }
}

object DataFrameImplicits {
  implicit class RichDataFrame(df: DataFrame) {
    def printSchemaAndCount(): Unit = {
      df.printSchema()
      println(df.count())
    }
  }
}
