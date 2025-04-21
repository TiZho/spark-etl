package com.github.spark.etl.core.app.utils

import com.amazonaws.services.s3.model.S3ObjectInputStream
import com.amazonaws.services.s3.{AmazonS3, AmazonS3ClientBuilder}
import com.github.spark.etl.core.app.config.Env
import com.github.spark.etl.core.app.config.FileSource.ExternalDatasource
import com.github.spark.etl.core.app.config.FileSource.ExternalDatasource
import com.typesafe.scalalogging.LazyLogging

import java.io.{BufferedReader, InputStreamReader}
import scala.util.{Failure, Success, Try}

object AwsS3Utils extends LazyLogging {
  def builder(): AmazonS3 = AmazonS3ClientBuilder.standard().build()
  def readFile(fileSource: ExternalDatasource)(implicit env: Env): Option[String] =
    Try {
      val s3Object = builder.getObject(fileSource.bucket, fileSource.key)
      val objectInputStream: S3ObjectInputStream = s3Object.getObjectContent()
      val reader: BufferedReader = new BufferedReader(new InputStreamReader(objectInputStream))
      val linesIterator: Iterator[String] =
        Iterator.continually(reader.readLine()).takeWhile(_ != null)
      linesIterator.mkString
    } match {
      case Success(value) =>
        Some(value)
      case Failure(err) =>
        logger.warn(s"can not read resource: $fileSource: ${err.getMessage}")
        None
    }
}
