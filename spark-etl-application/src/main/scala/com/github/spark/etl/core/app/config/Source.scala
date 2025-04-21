package com.github.spark.etl.core.app.config

import com.github.spark.etl.core.app.config.DatabaseSource.DefaultDatabaseSource
import com.github.spark.etl.core.app.config.FileSource.{BronzeAreaDatasource, ExternalDatasource, GoldAreaDatasource, SilverAreaDatasource}
import DatabaseSource.DefaultDatabaseSource
import com.github.spark.etl.core.app.config.FileSource.{BronzeAreaDatasource, ExternalDatasource, GoldAreaDatasource, SilverAreaDatasource}

trait Source {
  def input: Datasource
}

object Source {
  case class DefaultSource(input: ExternalDatasource)              extends Source
  case class BronzeSource(input: BronzeAreaDatasource)             extends Source
  case class SilverSource(input: SilverAreaDatasource)             extends Source
  case class GoldSource(input: GoldAreaDatasource)                 extends Source
  case class JdbcSource(override val input: DefaultDatabaseSource) extends Source
}
