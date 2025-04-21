package com.github.spark.etl.core.app.processor

import io.delta.tables.DeltaTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import DataFrameImplicits._
import com.github.spark.etl.core.app.config.DatasourceType.{Avro, CSV, Delta, Excel, JSON, Jdbc, Parquet}
import com.github.spark.etl.core.app.config.{DatabaseSource, Datasource, Env}
import WriterUtils.AbstractPredicate
import com.typesafe.scalalogging.LazyLogging

import scala.collection.immutable.ListMap
import scala.reflect.runtime.universe._
trait Writer[O] {
  def dest: Option[Datasource] = None
  def save(
      dest: Datasource,
      ds: Dataset[O],
      saveMode: SaveMode = SaveMode.Overwrite
    )(
      implicit
      spark: SparkSession,
      env: Env,
      e2: Encoder[O]
    ): Unit

  def saveAsDataFrame(
      dest: Datasource,
      df: DataFrame,
      saveMode: SaveMode = SaveMode.Overwrite
    )(
      implicit
      spark: SparkSession,
      env: Env
    ): Unit
}

object Writer extends LazyLogging {
  class PairWriter[O](x: Writer[O], y: Writer[O]) extends Writer[O] {
    override def save(
        dest: Datasource,
        ds: Dataset[O],
        saveMode: SaveMode
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e2: Encoder[O]
      ): Unit = {
      cacheDataset(x, ds)
      findSource(x, Some(dest)) foreach { output =>
        x.save(output, ds, saveMode)
      }
      findSource(y, None) foreach { output =>
        y.save(output, ds, saveMode)
      }
    }

    override def saveAsDataFrame(
        dest: Datasource,
        df: DataFrame,
        saveMode: SaveMode
      )(
        implicit
        spark: SparkSession,
        env: Env
      ): Unit = {
      cacheDataset(x, df)
      findSource(x, Some(dest)) foreach { output =>
        x.saveAsDataFrame(output, df, saveMode)
      }

      findSource(y, None) foreach { output =>
        y.saveAsDataFrame(output, df, saveMode)
      }
    }

    private def extractSource(
        writer: Writer[O],
        defaultValue: Option[Datasource]
      ): Option[Datasource] =
      (for {
        dest <- writer.dest
      } yield dest).orElse(defaultValue)

    private def findSource(
        writer: Writer[O],
        defaultValue: Option[Datasource]
      ): Option[Datasource] =
      isPairWriter(writer) match {
        case false => extractSource(writer, defaultValue)
        case true  => None
      }

    private def isPairWriter(writer: Writer[O]): Boolean = writer match {
      case _: PairWriter[_] => true
      case _                => false
    }

    def cacheDataset(x: Writer[O], ds: Dataset[_]): Unit = if (!isPairWriter(x)) ds.cache()
  }

  trait PartitionedWriter[O] extends Writer[O] {
    def dateCol: String
    def partitionsCols(dateColumn: String): Map[String, Column]
    def partitionsColsKeys: Array[String] = partitionsCols(dateCol).keys.toArray
  }

  object PartitionedWriter {
    def defaultPartitionsCols(dateColumn: String): ListMap[String, Column] =
      ListMap(
        PartitionGranularity.Year.name  -> year(col(dateColumn)),
        PartitionGranularity.Month.name -> month(col(dateColumn)),
        PartitionGranularity.Day.name   -> dayofmonth(col(dateColumn)),
        PartitionGranularity.Hour.name  -> hour(col(dateColumn))
      )
  }

  case class DefaultParquetWriter[O]() extends Writer[O] {
    override def save(
        dest: Datasource,
        ds: Dataset[O],
        saveMode: SaveMode = SaveMode.Overwrite
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e2: Encoder[O]
      ): Unit = ds.write.mode(saveMode).parquet(dest.uri)

    override def saveAsDataFrame(
        dest: Datasource,
        df: DataFrame,
        saveMode: SaveMode
      )(
        implicit
        spark: SparkSession,
        env: Env
      ): Unit = df.write.mode(saveMode).parquet(dest.uri)
  }

  case class DefaultJsonWriter[O <: Product: TypeTag](
      descriptor: StorableDescriptor[O])
      extends Writer[O] {
    override def save(
        dest: Datasource,
        ds: Dataset[O],
        saveMode: SaveMode
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e2: Encoder[O]
      ): Unit =
      ds.write.mode(saveMode).options(descriptor.writeOptions).json(dest.uri)

    override def saveAsDataFrame(
        dest: Datasource,
        df: DataFrame,
        saveMode: SaveMode
      )(
        implicit
        spark: SparkSession,
        env: Env
      ): Unit =
      df.write.mode(saveMode).options(descriptor.writeOptions).json(dest.uri)
  }

  case class DefaultPartitioned[O](dateCol: String) extends PartitionedWriter[O] {
    override def save(
        dest: Datasource,
        ds: Dataset[O],
        saveMode: SaveMode = SaveMode.Overwrite
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e2: Encoder[O]
      ): Unit = saveAsDataFrame(dest, ds.toDF)

    override def saveAsDataFrame(
        dest: Datasource,
        df: DataFrame,
        saveMode: SaveMode
      )(
        implicit
        spark: SparkSession,
        env: Env
      ): Unit =
      df
        .withPartitionColumns(partitionsCols(dateCol))
        .write
        .partitionBy(partitionsCols(dateCol).keys.toSeq: _*)
        .mode(saveMode)
        .parquet(dest.uri)

    def partitionsCols(dateColumn: String): ListMap[String, Column] =
      PartitionedWriter.defaultPartitionsCols(dateColumn)

  }

  case class DeltaLakeWriter[O](
      columnIDs: Seq[String],
      timeColumnPartitioning: Option[String] = None,
      customColumnPartitions: Seq[String] = Nil,
      overwriteCondition: Option[AbstractPredicate],
      mergeSchemaEnabled: Boolean)
      extends Writer[O]
      with LazyLogging {
    private val tableAliasName  = "table"
    private val sourceAliasName = "source"

    override def saveAsDataFrame(
        dest: Datasource,
        df: DataFrame,
        saveMode: SaveMode
      )(
        implicit
        spark: SparkSession,
        env: Env
      ): Unit = {
      logger.info(s"Writing into delta table ${dest.uri}")
      if (DeltaTable.isDeltaTable(spark, dest.uri)) {
        val table = DeltaTable.forPath(spark, dest.uri)
        mergeTable(dest.uri, table, df)
      } else {
        createTable(dest, df)
      }
    }

    override def save(
        dest: Datasource,
        ds: Dataset[O],
        saveMode: SaveMode
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e2: Encoder[O]
      ): Unit = saveAsDataFrame(dest, ds.toDF)

    private def createTable(
        dest: Datasource,
        df: DataFrame
      )(
        implicit
        env: Env
      ): Unit = {
      logger.info(s"Creating delta table")
      (timeColumnPartitioning, customColumnPartitions) match {
        case (Some(dtCol), _) =>
          val partitionCols = PartitionedWriter.defaultPartitionsCols(dtCol).keys.toSeq
          createTable(dest, df, partitionCols)
        case (None, cols) => createTable(dest, df, cols)
      }
    }

    private def mergeTable(
        destUri: String,
        dest: DeltaTable,
        df: DataFrame
      ): Unit = {
      logger.info(s"Preparing to merge into delta table ${destUri}")
      (
        timeColumnPartitioning,
        customColumnPartitions,
        overwriteCondition,
        mergeSchemaEnabled
      ) match {
        case (_, _, Some(condition), isMergeSchema) =>
          overwriteTable(destUri, df, condition, isMergeSchema)
        case (Some(dtCol), _, _, _) =>
          val partitionCols: ListMap[String, Column] =
            PartitionedWriter.defaultPartitionsCols(dtCol)
          mergeTable(dest, df, partitionCols)
        case (None, cols, _, _) =>
          val partitionCols: Map[String, Column] =
            cols.map(colName => (colName, col(colName))).toMap
          mergeTable(dest, df, partitionCols)
        case _ =>
          new IllegalStateException(
            "timeColumnPartitioning or customColumnPartitions should be set"
          )
      }
    }

    private def createTable(
        dest: Datasource,
        df: DataFrame,
        partitionCols: Seq[String]
      )(
        implicit
        env: Env
      ): Unit = {
      val writer = df.write
        .format("delta")

      val partitionedWriter = if (partitionCols.nonEmpty) {
        writer.partitionBy(partitionCols: _*)
      } else {
        writer
      }
      partitionedWriter.save(dest.uri)
    }

    private def overwriteTable(
        tableUri: String,
        source: DataFrame,
        overwriteClause: AbstractPredicate,
        mergeSchemaEnabled: Boolean
      ): Unit = {
      logger.info(s"Overwrite table $tableUri with clause ${overwriteClause.buildCondition}")
      source.write
        .mode(SaveMode.Overwrite)
        .option("replaceWhere", overwriteClause.buildCondition)
        .option("mergeSchema", mergeSchemaEnabled)
        .save(tableUri)
    }

    private def mergeTable(
        table: DeltaTable,
        source: DataFrame,
        partitionCols: Map[String, Column]
      ): Unit = {
      logger.info("Merging into delta table")
      val condition = timeColumnPartitioning match {
        case Some(dtColumn) =>
          s"$joinCondition AND $tableAliasName.$dtColumn < $sourceAliasName.$dtColumn"
        case _ => joinCondition
      }
      val sourceDf = source.as(sourceAliasName)

      val partitionedSource = if (partitionCols.nonEmpty) {
        partitionCols.foldLeft(sourceDf) { case (newDF, (colName, colValue)) =>
          newDF.withColumn(colName, colValue)
        }
      } else {
        sourceDf
      }

      table
        .as(tableAliasName)
        .merge(partitionedSource, condition)
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()
    }

    private def joinCondition: String =
      columnIDs
        .map(elem => s"$tableAliasName.$elem = $sourceAliasName.$elem")
        .reduce(_ + " AND " + _)

  }

  object DeltaLakeWriter {

    def apply[O](columnIDs: Seq[String], customColumnPartitions: Seq[String]): DeltaLakeWriter[O] =
      DeltaLakeWriter(
        columnIDs = columnIDs,
        timeColumnPartitioning = None,
        customColumnPartitions = customColumnPartitions,
        overwriteCondition = None,
        mergeSchemaEnabled = false
      )

    def apply[O <: Product](descriptor: StorableDescriptor[O]): DeltaLakeWriter[O] =
      DeltaLakeWriter(
        columnIDs = descriptor.columnIds,
        timeColumnPartitioning = None,
        customColumnPartitions = descriptor.partitionCols,
        overwriteCondition = None,
        mergeSchemaEnabled = false
      )

    def apply[O <: Product](
        descriptor: StorableDescriptor[O],
        overwriteClause: AbstractPredicate,
        mergeSchemaEnabled: Boolean
      ): DeltaLakeWriter[O] =
      DeltaLakeWriter(
        columnIDs = descriptor.columnIds,
        timeColumnPartitioning = None,
        customColumnPartitions = descriptor.partitionCols,
        overwriteCondition = Some(overwriteClause),
        mergeSchemaEnabled = mergeSchemaEnabled
      )
  }

  case class DefaultJdbcWriter[O]() extends Writer[O] {
    override def save(
        dest: Datasource,
        ds: Dataset[O],
        saveMode: SaveMode = SaveMode.Overwrite
      )(
        implicit
        spark: SparkSession,
        env: Env,
        e2: Encoder[O]
      ): Unit = dest match {
      case src: DatabaseSource =>
        ds.write.mode(SaveMode.Overwrite).jdbc(src.uri, src.tablename, src.properties)
      case src =>
        throw new IllegalArgumentException(s"can not write source $src as a JDBC source")
    }

    override def saveAsDataFrame(
        dest: Datasource,
        df: DataFrame,
        saveMode: SaveMode = SaveMode.Overwrite
      )(
        implicit
        spark: SparkSession,
        env: Env
      ): Unit = dest match {
      case src: DatabaseSource =>
        df.write.mode(SaveMode.Overwrite).jdbc(src.uri, src.tablename, src.properties)
      case src =>
        throw new IllegalArgumentException(s"can not write source $src as a JDBC source")
    }
  }

  def extractWriter[I <: Product: TypeTag](
      source: Datasource,
      descriptor: StorableDescriptor[I]
    ): Option[Writer[I]] =
    source.datasourceType match {
      case JSON    => Some(DefaultJsonWriter(descriptor))
      case CSV     => None
      case Parquet => Some(DefaultParquetWriter())
      case Delta   => Some(DeltaLakeWriter(descriptor))
      case Jdbc    => Some(DefaultJdbcWriter())
      case Avro    => None
      case Excel   => None
      case _                      => None
    }

}
