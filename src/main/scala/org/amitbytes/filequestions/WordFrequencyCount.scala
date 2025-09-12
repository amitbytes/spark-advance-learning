package org.amitbytes.filequestions
import org.apache.spark.sql.functions.{desc, explode, split, length, lower}

import org.apache.spark.sql.{DataFrame, SparkSession}

class WordFrequencyCount {
  def processQuestionToAnswer(filePath: String)(implicit spark: SparkSession): Unit = {
    import spark.implicits._
    val wordsDF = spark.read.text(filePath)
    //wordsDF.limit(5).show(false)
    wordsDF.select(explode(split(lower($"value"), "\\W+")).as("word"))
      //.withColumn("length", length($"word"))
      .filter(length($"word") >= 5)
      .groupBy("word")
      .count()
      .orderBy(desc("count"))
      .limit(100).show()
  }
}
