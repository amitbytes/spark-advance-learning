package org.amitbytes.common
import com.typesafe.config.{Config, ConfigFactory}
import org.amitbytes.common.DataBases.{DataBases}

import java.util.Properties

case class JdbcSettings(
                         url: String,
                         user: String,
                         password: String,
                         driver: String
                       ) {
  def toProperties: Properties = {
    val props = new Properties()
    props.setProperty("user", user)
    props.setProperty("password", password)
    props.setProperty("driver", driver)
    props
  }
}
object JdbcConfigLoader extends Serializable {
  private val config: Config = ConfigFactory.load() // loads application.conf
  def loadDb(dataBases: DataBases): JdbcSettings = {
    val dbConfig = config.getConfig(s"jdbc.${dataBases.toString.toLowerCase()}")
    JdbcSettings(
      url = dbConfig.getString("url"),
      user = dbConfig.getString("user"),
      password = dbConfig.getString("password"),
      driver = dbConfig.getString("driver")
    )
  }
}
