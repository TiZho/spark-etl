package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.application.SparkSessionBuilder
import com.github.spark.etl.core.app.config.Env
import org.apache.spark.sql.SparkSession

trait LocalSparkSession {
  implicit lazy val env: Env = Env(Env.Local)
  implicit lazy val spark: SparkSession =
    SparkSessionBuilder.LocalBuilder
      .buildSparkSession("local_application")
}
