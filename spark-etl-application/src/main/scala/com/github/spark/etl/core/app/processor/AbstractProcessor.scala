package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AppConfig, Datasource, Env}
import com.github.spark.etl.core.app.config.{AppConfig, Datasource, Env}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.SparkSession

import scala.util.{Failure, Success, Try}

abstract class AbstractProcessor[Conf <: AppConfig] extends LazyLogging {
  def processRun(
      input: Datasource,
      output: Datasource,
      debugMode: Boolean
    )(
      implicit
      sparkSession: SparkSession,
      env: Env,
      config: Conf
    ): Unit

  def processRunAsDataframe(
      source: Datasource,
      dest: Datasource,
      debugMode: Boolean
    )(
      implicit
      spark: SparkSession,
      env: Env,
      config: Conf
    ): Unit

  def run(
      input: Datasource,
      output: Datasource,
      dataframeEnabled: Boolean,
      enableDebugMode: Boolean
    )(
      implicit
      spark: SparkSession,
      env: Env,
      config: Conf
    ): Unit =
    Try {
      dataframeEnabled match {
        case true => processRunAsDataframe(input, output, enableDebugMode)
        case _    => processRun(input, output, enableDebugMode)
      }
    } match {
      case Success(_) =>
        logger.info("Job successfully completed")
      case Failure(exception) =>
        logger.error("An error occured during the processing", exception)
        throw exception
    }
}
