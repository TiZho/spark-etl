package com.github.spark.etl.core.app.utils

import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig}
import com.github.spark.etl.core.app.processor.LocalSparkSession
import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig}
import org.h2.tools.Server
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigReader

import scala.reflect.ClassTag

abstract class H2TestUtils[Conf <: AppConfig: ConfigReader: ClassTag]
    extends AnyFlatSpec
    with Matchers
    with LocalSparkSession
    with BeforeAndAfterAll {

  lazy val config: Conf   = AbstractConfiguration.extractConfig[Conf]
  lazy val server: Server = Server.createTcpServer().start()

  // private def properties(source: DatabaseSource): Properties = {
  //   val jdbcUrl = s"jdbc:h2:tcp://localhost:${server.getPort}/mem:testdb"
  //   val connectionProperties = new java.util.Properties()
  //   connectionProperties.setProperty("driver", "org.h2.Driver")
  //   connectionProperties.setProperty("url", jdbcUrl)
  //   connectionProperties.setProperty("user", "sa")
  //   connectionProperties.setProperty("password", "")
  // }

  // Stop the Spark session and H2 server after the tests
  override def afterAll(): Unit = {
    spark.stop()
    server.stop()
  }
}
