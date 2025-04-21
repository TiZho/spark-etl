package com.github.spark.etl.core.app.application

import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig, Env}
import com.github.spark.etl.core.app.processor.{AbstractProcessor, TransformProcessor}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession
import pureconfig.ConfigReader

import scala.reflect.ClassTag

abstract class AbstractApplication[Conf <: AppConfig: ConfigReader: ClassTag] extends LazyLogging {
  implicit lazy val parameters: Conf = AbstractConfiguration.extractConfig[Conf]
  def processor: AbstractProcessor[Conf]
  def builder: SparkSessionBuilder = loadSparkSession(parameters.app.env)

  def main(args: Array[String]): Unit = {
    val app              = parameters.app
    val appName          = app.appName
    val input            = app.source.input
    val output           = app.dest.output
    val dataframeEnabled = app.dataframeEnabled
    val debugMode        = app.debugMode

    implicit val sparkSession: SparkSession = builder.buildSparkSession(appName)
    implicit val env                        = app.env
    processor.run(input, output, dataframeEnabled, debugMode)

  }

  private def loadSparkSession(env: Env): SparkSessionBuilder = env match {
    case Env(envType) if envType == Env.Local => SparkSessionBuilder.LocalBuilder
    case _                                    => SparkSessionBuilder.DatabricksBuilder
  }

}

object AbstractApplication {

  abstract class TransformApplication[
      I <: Product: ClassTag,
      O <: Product: ClassTag,
      Conf <: AppConfig: ConfigReader: ClassTag]
      extends AbstractApplication[Conf] {
    override lazy val processor: AbstractProcessor[Conf] = transformProcessor
    def transformProcessor: TransformProcessor[I, O, Conf]
  }

}
