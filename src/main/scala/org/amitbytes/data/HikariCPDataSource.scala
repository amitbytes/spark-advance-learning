package org.amitbytes.data

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.amitbytes.common.DatabasesEnum.DataBases
import org.amitbytes.common.{DatabasesEnum, JdbcConfigLoader}
import java.util.concurrent.ConcurrentHashMap


object HikariCPDataSource {
  private val pools = new ConcurrentHashMap[DataBases, HikariDataSource]()

  def getDataSource(dataBases: DataBases): HikariDataSource = {
    pools.computeIfAbsent(dataBases, _ => {
      //Class.forName("com.mysql.cj.jdbc.Driver") //load the driver explicitly as spark loads driver from itself not for other applications
      val jdbcSettings = JdbcConfigLoader.loadDb(dataBases)
      val hikariConfig = new HikariConfig()
      hikariConfig.setJdbcUrl(jdbcSettings.url)
      hikariConfig.setUsername(jdbcSettings.user)
      hikariConfig.setPassword(jdbcSettings.password)
      //hikariConfig.setDriverClassName(jdbcSettings.driver)
      hikariConfig.setMaximumPoolSize(20) // Set the maximum pool size as needed TODO: we can change this according to need
      new HikariDataSource(hikariConfig)
    })

  }
}
