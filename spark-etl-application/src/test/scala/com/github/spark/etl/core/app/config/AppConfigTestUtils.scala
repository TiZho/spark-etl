package com.github.spark.etl.core.app.config

trait AppConfigTestUtils[Conf <: AppConfig] {
  def config: Conf
}
