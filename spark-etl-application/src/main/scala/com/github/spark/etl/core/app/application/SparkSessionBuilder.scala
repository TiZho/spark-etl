package com.github.spark.etl.core.app.application

import org.apache.spark.sql.SparkSession

sealed trait SparkSessionBuilder {
  def buildSparkSession(appName: String): SparkSession
}

object SparkSessionBuilder {
  case object LocalBuilder extends SparkSessionBuilder {
    override def buildSparkSession(appName: String): SparkSession =
      SparkSession
        .builder()
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .appName(appName)
        .getOrCreate()
  }

  case object DatabricksBuilder extends SparkSessionBuilder {
    override def buildSparkSession(appName: String): SparkSession =
      SparkSession
        .builder()
        .appName(appName)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
          "spark.sql.catalog.spark_catalog",
          "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
  }
}
