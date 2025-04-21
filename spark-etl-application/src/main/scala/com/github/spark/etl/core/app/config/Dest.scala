package com.github.spark.etl.core.app.config

import com.github.spark.etl.core.app.config.FileSource.{BronzeAreaDatasource, ExternalDatasource, GoldAreaDatasource, SilverAreaDatasource}
import com.github.spark.etl.core.app.config.FileSource.{BronzeAreaDatasource, ExternalDatasource, GoldAreaDatasource, SilverAreaDatasource}

trait Dest {
  def output: Datasource
}

object Dest {
  final case class DefaultDest(output: ExternalDatasource) extends Dest
  final case class BronzeDest(output: BronzeAreaDatasource, mergeSchemaEnabled: Boolean = false)
      extends Dest
  final case class SilverDest(output: SilverAreaDatasource, mergeSchemaEnabled: Boolean = false)
      extends Dest
  final case class GoldDest(output: GoldAreaDatasource, mergeSchemaEnabled: Boolean = false)
      extends Dest
}
