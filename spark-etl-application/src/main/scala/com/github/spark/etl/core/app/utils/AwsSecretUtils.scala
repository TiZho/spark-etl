package com.github.spark.etl.core.app.utils

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging
import com.amazonaws.services.secretsmanager.model.GetSecretValueRequest
import com.amazonaws.services.secretsmanager.{ AWSSecretsManager, AWSSecretsManagerClientBuilder }

import scala.util.Try

object AwsSecretUtils extends LazyLogging {
  def readSecret[T](secret: Secret[T], region: Regions): Either[Throwable, T] =
    secret.defaultValue match {
      case Some(value) => Right(value)
      case _           => extractSecret(secret, region)
    }

  def readStrictSecret[T](secret: Secret[T], region: Regions): T =
    secret.defaultValue match {
      case Some(value) => value
      case _           => extractStrictSecret(secret, region)
    }

  def connect(region: Regions = Regions.EU_WEST_1): AWSSecretsManager =
    AWSSecretsManagerClientBuilder
      .standard()
      .withRegion(region)
      .build()

  def getSecretValue(secretName: String, client: AWSSecretsManager): String = client
    .getSecretValue(new GetSecretValueRequest().withSecretId(secretName))
    .getSecretString

  def extractSecret[T](secret: Secret[T], region: Regions): Either[Throwable, T] = {
    val connection = connect(region)
    val result = Try {
      secret.parser(getSecretValue(secret.keyName, connection))
    }.toEither
    connection.shutdown()
    result
  }

  def extractStrictSecret[T](secret: Secret[T], region: Regions): T =
    extractSecret(secret, region) match {
      case Left(err) =>
        logger.error(s"can not read secret key ${secret.keyName} : ${err.getMessage}")
        throw err
      case Right(value) => value
    }
}
