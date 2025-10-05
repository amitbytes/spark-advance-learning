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

/*
	spark.sql("SET -v").show()
	spark.sql("SET -v").where($"key".like("%spark.sql.adaptive%")).show(100,false)
	--Colaesceing broadscast join
	spark.sql.adaptive.autoBroadcastJoinThreshHold=10MB default we can set it -1 negate this feature
    spark.sql.adaptive.localShuffleReader.enabled=true  -- keep it true always if we are using broadcast join AEQ feature

	sc.parallelize(spark.sparkContext.getConf.getAll).toDF("key","value").where($"key".like("%spark.sql%")).show(false)
	sc.parallelize(spark.conf.getAll.toSeq).toDF("key","value").where($"key".like("%spark.sql%")).show(false)

	spark.sql("SET -v").where($"key".like("spark.sql.adaptive%")).select("key","value","since version").show(100,false)
	spark.sql("SET -v").where($"key".like("spark.sql.shuffle%")).select("key","value","since version").show(100,false)
	spark.sql("SET -v").where($"key".like("spark.sql.files%")).select("key","value","since version").show(100,false)
 */
