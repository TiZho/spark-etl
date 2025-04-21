package com.github.spark.etl.core.app.config

import pureconfig.{ ConfigReader, ConfigSource }

import scala.reflect.ClassTag

object AbstractConfiguration {
  def extractConfig[Conf <: AppConfig: ClassTag: ConfigReader]: Conf =
    ConfigSource.default.loadOrThrow[Conf]
}
