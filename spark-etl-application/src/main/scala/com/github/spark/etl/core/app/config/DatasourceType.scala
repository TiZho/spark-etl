package com.github.spark.etl.core.app.config

sealed trait DatasourceType {
  def name: String
}

object DatasourceType {
  case object JSON                       extends DatasourceType { val name: String = "json"    }
  case object CSV                        extends DatasourceType { val name: String = "csv"     }
  case object Parquet                    extends DatasourceType { val name: String = "parquet" }
  case object Delta                      extends DatasourceType { val name: String = "delta"   }
  case object Excel                      extends DatasourceType { val name: String = "excel"   }
  case object Avro                       extends DatasourceType { val name: String = "avro"    }
  case object Jdbc                       extends DatasourceType { val name: String = "jdbc"    }
  case class NotIdentified(name: String) extends DatasourceType

  def extractSourceType(sourceType: String): DatasourceType =
    sourceType match {
      case JSON.name    => JSON
      case CSV.name     => CSV
      case Parquet.name => Parquet
      case Delta.name   => Delta
      case Excel.name   => Excel
      case Avro.name    => Avro
      case Jdbc.name    => Jdbc
      case value        => NotIdentified(value)
    }
}
