package org.amitbytes.filequestions
import org.apache.spark.sql.{functions => F}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.Row
import org.amitbytes.exention.methods.AllExtensionMethdods._

case class NewUser(name: String, email: String, age: Int)
sealed class ScalaLearning {
  def executeScalaLearning()(implicit  spark: SparkSession): Unit ={
    import spark.implicits._
    val users = Seq(
      Row("Amit Singh", "amit@domain.com", 35),
      Row("Vaanya Singh", "vaanya@domain.com", 2),
      Row("Misha Singh", "misha@domain.com", 35)
    )
    //println(Encoders.product[User].schema.prettyJson)
    val usersData = spark.createDataFrame(spark.sparkContext.parallelize(users), Encoders.product[NewUser].schema)
    //usersData.show(false)
    println(s"Extension methods '10': ${"100".ToInteger()}")
    println(s"Extension methods '10': ${"100".ToDouble()}")

  }
}
