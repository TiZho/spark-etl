package com.github.spark.etl.core.app.utils

import com.amazonaws.regions.Regions
import com.typesafe.scalalogging.LazyLogging

object AwsUtils extends LazyLogging {

  def loadRegion(regionKey: String): Either[Throwable, Regions] = {
    val region           = Regions.fromName(regionKey.toLowerCase)
    val availableRegions = Regions.values()
    if (!availableRegions.contains(region)) {
      val message = s"Region provided $regionKey is not currently supported"
      logger.error(message)
      Left(new IllegalArgumentException(message))
    }
    Right(region)
  }

  def loadRegionStrict(regionKey: String): Regions = loadRegion(regionKey) match {
    case Right(value) => value
    case Left(err)    => throw err
  }

}
