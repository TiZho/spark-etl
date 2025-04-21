package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AppConfig, Datasource, Env}
import com.github.spark.etl.core.app.config.{AppConfig, Datasource, Env}
import TransformPipeline.DatasourceJoiner._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame, Dataset, Encoder, SparkSession}

import scala.collection.immutable.ListMap
import scala.reflect.ClassTag

abstract class TransformPipeline[
    I <: Product: Encoder: ClassTag,
    O <: Product: Encoder: ClassTag,
    Conf <: AppConfig
  ](val source: Datasource)
    extends LazyLogging
    with Serializable {
  def loader: Loader[I]
  def preFilter: Filter[I, Conf]
  def transformer: Transformer[I, O]
  def postFilter: Filter[O, Conf]
  def inputClass: ClassTag[I]  = implicitly[ClassTag[I]]
  def outputClass: ClassTag[O] = implicitly[ClassTag[O]]

  def process(implicit sparkSession: SparkSession, config: Conf, env: Env): Dataset[O] = {
    import config._
    val inputType  = inputClass.runtimeClass.getCanonicalName
    val outputType = outputClass.runtimeClass.getCanonicalName

    logger.info(s"Loading data from source: ${source.uri}")
    val inputDs: Dataset[I] = loader.load(source)

    logger.info(s"Applying filter on [$inputType]")
    val filterInputDs: Dataset[I] = preFilter.filter(inputDs)

    logger.info(s"Applying mapping from [$inputType] to [$outputType]")
    val tranformedDs: Dataset[O] = transformer.transform(filterInputDs)

    logger.info(s"Applying filter on [$outputType]")
    val filterTranformedDs: Dataset[O] = postFilter.filter(tranformedDs)

    if (app.debugMode) {
      logger.info("Execution plan from input source")
      inputDs.explain(true)
      logger.info("Count data from input source: ", inputDs.count())
      logger.info("Count data from input source: ")
      tranformedDs.explain(true)
      logger.info("Count data from output source: ", tranformedDs.count())
    }

    filterTranformedDs
  }

  def processAsDataframe(
      implicit
      spark: SparkSession,
      env: Env,
      config: Conf
    ): DataFrame = {
    import config._
    val inputType  = implicitly[ClassTag[I]].runtimeClass.getCanonicalName
    val outputType = implicitly[ClassTag[O]].runtimeClass.getCanonicalName

    logger.info(s"Loading data from source: ${source.uri}")
    val inputDf: DataFrame = loader.loadAsDataFrame(source)

    logger.info(s"Applying filter on [$inputType]")
    val filterInputDf: DataFrame = preFilter.filterAsDataFrame(inputDf)

    logger.info(s"Applying mapping from [$inputType] to [$outputType]")
    val tranformedDf: DataFrame = transformer.transformAsDataFrame(filterInputDf)

    logger.info(s"Applying filter on [$outputType]")
    val filterTranformedDf: DataFrame = postFilter.filterAsDataFrame(tranformedDf)

    if (app.debugMode) {
      logger.info("Execution plan from input source")
      inputDf.explain(true)
      logger.info("Count data from input source: ", inputDf.count())
      logger.info("Count data from input source: ")
      tranformedDf.explain(true)
      logger.info("Count data from output source: ", tranformedDf.count())
    }
    filterTranformedDf
  }
}

object TransformPipeline {

  abstract class DatasourceJoiner[O1: Encoder: ClassTag, O2: Encoder: ClassTag]
      extends Serializable {
    def mapping: (O1, O2) => O1
    def mappingColumn: ListMap[String, Column]
    def conditions: Map[String, String]
    def joinType: String
    def projectColums: List[Column] =
      mappingColumn.map { case (key, value) => value.as(key) }.toList

    def conditionDataset(ds1: Dataset[_], ds2: Dataset[_]): Column =
      conditions.map { case (src, targ) => ds1(src) === ds2(targ) }.reduce(_ && _)
    def conditionDataframe: Column =
      conditions.map { case (src, targ) => source(src) === target(targ) }.reduce(_ && _)
    protected def source(field: String): Column = col(s"$sourcePrefix$field")
    protected def target(field: String): Column = col(s"$targetPrefix$field")
  }

  object DatasourceJoiner {
    lazy val delimiter: Char      = '$'
    lazy val sourcePrefix: String = s"source$delimiter"
    lazy val targetPrefix: String = s"target$delimiter"

    def prefixColumns(df: DataFrame, prefix: String): DataFrame = {
      val existingColumns = df.columns
      val aliasedColumns =
        existingColumns.map(columnName => col(columnName).alias(s"$prefix$columnName"))
      df.select(aliasedColumns: _*)
    }

    def renameColumnsAfterJoin(joinedDF: DataFrame, mapping: ListMap[String, Column]): DataFrame =
      mapping.foldLeft(joinedDF) { case (accDF, (key, value)) => accDF.withColumn(key, value) }

    @deprecated("Not used anymore", since = "1.4.0")
    def extractColumnsAfterJoin(joinedDF: DataFrame): List[Column] =
      joinedDF.columns
        .map {
          case s if s.contains(delimiter) => s.split(delimiter)(1)
          case s                          => s
        }
        .toSet
        .map(col)
        .toList
  }

  trait JoinableSource[A, B] {
    def joiner: DatasourceJoiner[A, B]
  }

  abstract class JoinableTransformPipeline[
      I <: Product: Encoder: ClassTag,
      O <: Product: Encoder: ClassTag,
      O1 <: Product: Encoder: ClassTag,
      Conf <: AppConfig
    ](source: Datasource)
      extends TransformPipeline[I, O, Conf](source)
      with JoinableSource[O1, O] {
    def joiner: DatasourceJoiner[O1, O]
  }

  class JoinedTransformPipeline[
      I1 <: Product: Encoder: ClassTag,
      I2 <: Product: Encoder: ClassTag,
      O1 <: Product: Encoder: ClassTag,
      O2 <: Product: Encoder: ClassTag,
      Conf <: AppConfig
    ](val x: TransformPipeline[I1, O1, Conf],
      val y: TransformPipeline[I2, O2, Conf] with JoinableSource[O1, O2])
      extends TransformPipeline[I1, O1, Conf](x.source) {
    override def loader: Loader[I1]               = ???
    override def preFilter: Filter[I1, Conf]      = ???
    override def transformer: Transformer[I1, O1] = ???
    override def postFilter: Filter[O1, Conf]     = ???
    override def process(
        implicit
        sparkSession: SparkSession,
        config: Conf,
        env: Env
      ): Dataset[O1] = {
      val ds1 = x.process
      val ds2 = y.process
      ds1
        .joinWith(ds2, y.joiner.conditionDataset(ds1, ds2), y.joiner.joinType)
        .map { case (o1, o2) => y.joiner.mapping(o1, o2) }
    }
    override def processAsDataframe(
        implicit
        spark: SparkSession,
        env: Env,
        config: Conf
      ): DataFrame = {
      val ds1 = prefixColumns(x.processAsDataframe, sourcePrefix)
      val ds2 = prefixColumns(y.processAsDataframe, targetPrefix)
      val cols = y.joiner.mappingColumn.map { case (colName, colValue) =>
        colValue.as(colName)
      }.toList
      ds1.join(ds2, y.joiner.conditionDataframe, y.joiner.joinType).select(cols: _*)
    }
  }
  class EnrichedJoinableTransformPipeline[
      I1 <: Product: Encoder: ClassTag,
      I2 <: Product: Encoder: ClassTag,
      O1 <: Product: Encoder: ClassTag,
      O2 <: Product: Encoder: ClassTag,
      O <: Product: Encoder: ClassTag,
      Conf <: AppConfig
    ](x: TransformPipeline[I1, O1, Conf] with JoinableSource[O, O1],
      y: TransformPipeline[I2, O2, Conf] with JoinableSource[O1, O2])
      extends JoinedTransformPipeline[I1, I2, O1, O2, Conf](x, y)
      with JoinableSource[O, O1] {

    override def loader: Loader[I1]               = ???
    override def preFilter: Filter[I1, Conf]      = ???
    override def transformer: Transformer[I1, O1] = ???
    override def postFilter: Filter[O1, Conf]     = ???

    override def process(
        implicit
        sparkSession: SparkSession,
        config: Conf,
        env: Env
      ): Dataset[O1] = {
      val ds1 = x.process
      val ds2 = y.process

      ds1
        .joinWith(ds2, y.joiner.conditionDataset(ds1, ds2), y.joiner.joinType)
        .map { case (o1, o2) => y.joiner.mapping(o1, o2) }
    }

    override def processAsDataframe(
        implicit
        spark: SparkSession,
        env: Env,
        config: Conf
      ): DataFrame = {
      val ds1 = prefixColumns(x.processAsDataframe, sourcePrefix)
      val ds2 = prefixColumns(y.processAsDataframe, targetPrefix)

      val cols = y.joiner.mappingColumn.map { case (colName, colValue) =>
        colValue.as(colName)
      }.toList
      ds1.join(ds2, y.joiner.conditionDataframe, y.joiner.joinType).select(cols: _*)
    }

    def joiner: DatasourceJoiner[O, O1] = x.joiner
  }

  class UnionTransformPipeline[
      I1 <: Product: Encoder: ClassTag,
      I2 <: Product: Encoder: ClassTag,
      O <: Product: Encoder: ClassTag,
      Conf <: AppConfig
    ](x: TransformPipeline[I1, O, Conf],
      y: TransformPipeline[I2, O, Conf])
      extends TransformPipeline[I1, O, Conf](x.source) {
    override def loader: Loader[I1]              = ???
    override def preFilter: Filter[I1, Conf]     = ???
    override def transformer: Transformer[I1, O] = ???
    override def postFilter: Filter[O, Conf]     = ???

    override def process(
        implicit
        sparkSession: SparkSession,
        config: Conf,
        env: Env
      ): Dataset[O] = {
      val ds1 = x.process
      val ds2 = y.process
      ds1 union ds2
    }

    override def processAsDataframe(
        implicit
        spark: SparkSession,
        env: Env,
        config: Conf
      ): DataFrame = {
      val df1 = x.processAsDataframe
      val df2 = y.processAsDataframe
      df1 union df2
    }
  }
}
