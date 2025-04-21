package com.github.spark.etl.core.app.processor

import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}
import DataFrameImplicits._
import com.github.spark.etl.core.app.config.Env
import Indicator.IndicatorColumnMapping

import scala.collection.immutable.ListMap

abstract class Transformer[I: Encoder, O: Encoder] extends Serializable {
  def transform(ds: Dataset[I])(implicit sparkSession: SparkSession, env: Env): Dataset[O]
  def transformAsDataFrame(df: DataFrame)(implicit sparkSession: SparkSession, env: Env): DataFrame
}

object Transformer {
  case class Identity[I: Encoder]() extends Transformer[I, I] {
    override def transform(
        ds: Dataset[I]
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): Dataset[I] = ds
    override def transformAsDataFrame(
        df: DataFrame
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): DataFrame = df
  }

  class PairTransformer[A: Encoder, B: Encoder, C: Encoder](
      x: Transformer[A, B],
      y: Transformer[B, C])
      extends Transformer[A, C] {
    override def transform(
        ds: Dataset[A]
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): Dataset[C] = {
      val f1: Dataset[A] => Dataset[B] = x.transform
      val f2: Dataset[B] => Dataset[C] = y.transform
      (f1 andThen f2)(ds)
    }
    override def transformAsDataFrame(
        df: DataFrame
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): DataFrame = {
      val f1: DataFrame => DataFrame = x.transformAsDataFrame
      val f2: DataFrame => DataFrame = y.transformAsDataFrame
      (f1 andThen f2)(df)
    }
  }

  abstract class MappingTransformer[I: Encoder, O: Encoder] extends Transformer[I, O] {
    def mapping(input: I): O
    def mappingCols(): ListMap[String, Column]
    override def transform(
        ds: Dataset[I]
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): Dataset[O] =
      ds.map(mapping)
    override def transformAsDataFrame(
        df: DataFrame
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): DataFrame =
      df.withPartitionColumns(mappingCols())
  }

  abstract class OptionalMappingTransformer[I: Encoder, O: Encoder] extends Transformer[I, O] {
    def mapping(input: I): Option[O]
    def mappingCols: ListMap[String, Column]
    override def transform(
        ds: Dataset[I]
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): Dataset[O] =
      ds.flatMap(mapping)
    override def transformAsDataFrame(
        df: DataFrame
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): DataFrame =
      df.withPartitionColumns(mappingCols)
  }

  abstract class ComputeIndicatorsTransformer[T: Encoder] extends Transformer[T, T] {
    def indicators: List[Indicator[T, _]]
    private def indicatorAsColumns: List[IndicatorColumnMapping] =
      indicators.map(_.column).map(_.computeColumn)
    private def applyComputations(elem: T): T =
      Function.chain(indicators.map(_.field).map(_.processCompute))(elem)
    override def transform(
        ds: Dataset[T]
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): Dataset[T] =
      ds.mapPartitions(it => it.map(applyComputations))
    override def transformAsDataFrame(
        df: DataFrame
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): DataFrame =
      indicatorAsColumns.foldLeft(df) { case (newDF, indicatorCol) =>
        newDF.withColumn(indicatorCol.name, indicatorCol.value)
      }
  }

  abstract class WindowAggregatorTransformer[I: Encoder, O: Encoder] extends Transformer[I, O] {
    def groupByCols: Seq[String]
    def orderByCols: Seq[Column]
    def droppableCols: Seq[String]
    def postFilter: Option[Column]
    def computeCols: ListMap[String, Column]
    def window: WindowSpec = Window.partitionBy(groupByCols.map(col): _*).orderBy(orderByCols: _*)
    def processComputeCols: ListMap[String, Column] = computeCols.map { case (k, v) => k -> v }
    override def transform(
        ds: Dataset[I]
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): Dataset[O] =
      transformAsDataFrame(ds.toDF()).as[O]

    override def transformAsDataFrame(
        df: DataFrame
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): DataFrame = {
      postFilter match {
        case Some(filter) =>
          df
            .withPartitionColumns(processComputeCols)
            .where(filter)
            .drop(droppableCols: _*)
        case None => df.withPartitionColumns(processComputeCols).drop(droppableCols: _*)
      }
    }
  }

  abstract class GroupByTransformer[I: Encoder, O: Encoder] extends Transformer[I, O] {
    def idColumns: Seq[String]
    def computations: Map[String, Column]
    def extraColumns: Map[String, Column]

    private def computationsList: List[Column] =
      computations.map { case (colName, computation) => computation.as(colName) }.toList

    private def addExtraColumns(df: DataFrame): DataFrame =
      extraColumns.foldLeft(df) { case (newDF, (colName, colValue)) =>
        newDF.withColumn(colName, colValue)
      }

    /*
    private def projectExtraColumns(df: DataFrame): DataFrame = {
        val newColumns = df.columns.map(s => col(s).as(s)) ++
          extraColumns.map { case (colName,colValue) => colValue.as(colName)}
        df.select(newColumns: _*)
    }
     */

    override def transform(
        ds: Dataset[I]
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): Dataset[O] =
      transformAsDataFrame(ds.toDF).as[O]

    override def transformAsDataFrame(
        df: DataFrame
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): DataFrame = {
      val aggregatedDf = df
        .groupBy(idColumns.head, idColumns.tail: _*)
        .agg(computationsList.head, computationsList.tail: _*)
      addExtraColumns(aggregatedDf)
    }
  }

  abstract class LimitTransformer[T: Encoder] extends Transformer[T, T] {
    def rowsNumber: Int
    override def transform(
        ds: Dataset[T]
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): Dataset[T] =
      ds.limit(rowsNumber)

    override def transformAsDataFrame(
        df: DataFrame
      )(
        implicit
        sparkSession: SparkSession,
        env: Env
      ): DataFrame =
      df.limit(rowsNumber)
  }
}
