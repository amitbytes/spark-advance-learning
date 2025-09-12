package org.amitbytes.filequestions
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.types._

sealed class NestedJsonExample {
    def executeNestedJsonExample(filepath:String)(implicit spark: org.apache.spark.sql.SparkSession): Unit = {
      import spark.implicits._
      val jsonSchema = StructType(Seq(
        StructField("id", IntegerType, true),
        StructField("name", StringType, true),
        StructField("address", StructType(Seq(
          StructField("city", StringType, true),
          StructField("zip", StringType, true),
        ))),
        StructField("phones", ArrayType(StringType), true)
      ))

      val jsonData = spark.read.option("multiline", "true").schema(jsonSchema).json(filepath)

      jsonData.printSchema()

      jsonData.withColumn("city", $"address.city")
              .withColumn("zip", $"address.zip")
        .withColumn("phone", F.explode($"phones"))
        .select($"city", $"zip", $"phone", F.posexplode($"phones").as(Seq("pos", "phone_number")))
        .drop($"phones")
              .drop($"address")
              //.show(false)

      val jsonDataStr = Seq(
        """
          |  {
          |    "name": "Alice",
          |    "activities": [
          |      {"type": "sports", "name": "cycling"},
          |      {"type": "leisure", "name": "reading"}
          |    ]
          |  }
          """.stripMargin,
      """
          |  {
          |    "name": "Amit",
          |    "activities": [
          |      {"type": "reading", "name": "novels"},
          |      {"type": "leisure", "name": "reading"}
          |    ]
          |  }
          |""".stripMargin)

      val jsonDataDF = spark.read.json(spark.createDataset(jsonDataStr))
      jsonDataDF.printSchema()
      jsonDataDF.withColumn("activity", F.explode($"activities"))
        .select($"name", $"activity.type".as("actvityType"), $"activity.name".as("activityName"))
        .show(false)
      //jsonDataDF.show(false)


      //selectedDF.show(false)
    }
}
