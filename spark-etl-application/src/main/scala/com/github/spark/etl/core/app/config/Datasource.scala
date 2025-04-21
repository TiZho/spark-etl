package com.github.spark.etl.core.app.config

trait Datasource {
  def uri(implicit env: Env): String
  def datasourceType: DatasourceType
}
