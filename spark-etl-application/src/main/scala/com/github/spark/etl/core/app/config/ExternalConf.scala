package com.github.spark.etl.core.app.config

import better.files.File
import com.github.spark.etl.core.app.config.FileSource.ExternalDatasource
import com.github.spark.etl.core.app.utils.AwsS3Utils
import FileSource.ExternalDatasource
import com.typesafe.scalalogging.LazyLogging
import pureconfig.{ConfigReader, ConfigSource}

class ExternalConf[T: ConfigReader](datasource: ExternalDatasource) extends LazyLogging {
  def config(implicit env: Env): T =
    (for {
      jsonString <- processReadFile(datasource)
      config     <- processReadConfig(jsonString)
    } yield config)
      .getOrElse(
        throw new IllegalArgumentException(s"Can not read S3 config ${datasource.toString}")
      )

  private def processReadFile(datasource: ExternalDatasource)(implicit env: Env): Option[String] =
    env.value match {
      case Env.Local => Some(processReadLocalFile(datasource))
      case _         => AwsS3Utils.readFile(datasource)
    }

  private def processReadLocalFile(datasource: ExternalDatasource)(implicit env: Env): String = {
    val file = File(datasource.uri)
    file.contentAsString
  }

  private def processReadConfig(jsonString: String): Option[T] =
    ConfigSource.string(jsonString).load[T] match {
      case Right(value) => Some(value)
      case Left(err) =>
        logger.warn(s"Can not read config for json $jsonString: ${err.toString}")
        None
    }
}
