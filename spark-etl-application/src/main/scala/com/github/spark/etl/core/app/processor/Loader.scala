package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.DatasourceType.{Avro, CSV, Delta, Excel, JSON, Jdbc, Parquet}
import com.github.spark.etl.core.app.config.{DatabaseSource, Datasource, Env}
import org.apache.spark.sql._

import scala.reflect.runtime.universe._

trait Loader[I] extends Serializable {
  def load(source: Datasource)(implicit spark: SparkSession, env: Env, e1: Encoder[I]): Dataset[I]
  def loadAsDataFrame(
      source: Datasource
    )(
      implicit
      spark: SparkSession,
      env: Env,
      e1: Encoder[I]
    ): DataFrame
}

object Loader {
  trait DataFrameLoader[I] extends Loader[I] {
    def mappingCols: Map[String, Column]
    def load(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): Dataset[I] =
      convertMappingColumns.isEmpty match {
        case true => loadAsDataFrame(source).as[I]
        case _    => loadAsDataFrame(source).select(convertMappingColumns: _*).as[I]
      }
    protected def convertMappingColumns: Array[Column] =
      mappingCols.map { case (colname, colexpr) => colexpr.as(colname) }.toArray
  }

  case class DummyLoader[I]() extends Loader[I] {
    override def load(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): Dataset[I] = {
      import spark.implicits._
      Seq.empty[I].toDS()
    }
    override def loadAsDataFrame(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): DataFrame = {
      import spark.implicits._
      Seq.empty[I].toDF
    }
  }

  case class DefaultJsonLoader[I <: Product: TypeTag](descriptor: StorableDescriptor[I])
      extends Loader[I] {
    override def load(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): Dataset[I] = {
      val df = spark.read.options(descriptor.readOptions).json(source.uri)
      df.as[I]
    }

    override def loadAsDataFrame(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): DataFrame =
      spark.read.schema(e1.schema).options(descriptor.readOptions).json(source.uri)

  }

  case class DefaultParquetLoader[I]() extends Loader[I] {
    override def load(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): Dataset[I] =
      spark.read.parquet(source.uri).as[I]

    override def loadAsDataFrame(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): DataFrame =
      spark.read.schema(e1.schema).parquet(source.uri)
  }

  case class DefaultDeltaLoader[I]() extends Loader[I] {
    override def load(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): Dataset[I] =
      spark.read.format("delta").load(source.uri).as[I]

    override def loadAsDataFrame(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): DataFrame =
      spark.read.format("delta").load(source.uri)
  }

  case class DefaultDataFrameJsonLoader[I](mappingCols: Map[String, Column])
      extends DataFrameLoader[I] {
    override def loadAsDataFrame(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): DataFrame =
      spark.read.schema(e1.schema).json(source.uri)
  }

  case class DefaultJdbcLoader[I]() extends Loader[I] {
    override def load(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): Dataset[I] = source match {
      case src: DatabaseSource =>
        spark.read.format("jdbc").options(src.readOptions).load.as[I]
      case src =>
        throw new IllegalArgumentException(s"can not read source $src as a JDBC source")
    }

    override def loadAsDataFrame(
        source: Datasource
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e1: Encoder[I]
      ): DataFrame =
      source match {
        case src: DatabaseSource =>
          spark.read.format("jdbc").options(src.readOptions).load()
        case src =>
          throw new IllegalArgumentException(s"can not read source $src as a JDBC source")
      }
  }

  def extractLoader[I <: Product: TypeTag](
      source: Datasource,
      descriptor: StorableDescriptor[I]
    ): Option[Loader[I]] =
    source.datasourceType match {
      case JSON    => Some(DefaultJsonLoader(descriptor))
      case CSV     => None
      case Parquet => Some(DefaultDeltaLoader())
      case Delta   => Some(DefaultDeltaLoader())
      case Jdbc    => Some(DefaultJdbcLoader())
      case Avro    => None
      case Excel   => None
      case _                      => None
    }

}
