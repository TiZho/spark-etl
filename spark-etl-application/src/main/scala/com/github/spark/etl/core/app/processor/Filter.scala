package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AppConfig, Env}
import com.github.spark.etl.core.app.config.{AppConfig, Env}
import org.apache.spark.sql.{Column, DataFrame, Dataset, SparkSession}

trait Filter[T, Conf <: AppConfig] extends Serializable {
  def filter(implicit config: Conf, env: Env, spark: SparkSession): T => Boolean
  def filterColumn(implicit config: Conf, env: Env, spark: SparkSession): Column
  def filterAsDataFrame(
      df: DataFrame
    )(
      implicit
      config: Conf,
      env: Env,
      spark: SparkSession
    ): DataFrame =
    df.where(filterColumn)
  def filter(ds: Dataset[T])(implicit config: Conf, env: Env, spark: SparkSession): Dataset[T] =
    ds.filter(filter)
}

object Filter {
  case class Identity[T, Conf <: AppConfig]() extends Filter[T, Conf] {
    override def filter(implicit config: Conf, env: Env, spark: SparkSession): T => Boolean = ???
    override def filter(
        ds: Dataset[T]
      )(
        implicit
        config: Conf,
        env: Env,
        spark: SparkSession
      ): Dataset[T] = ds
    override def filterColumn(implicit config: Conf, env: Env, spark: SparkSession): Column = ???
    override def filterAsDataFrame(
        df: DataFrame
      )(
        implicit
        config: Conf,
        env: Env,
        spark: SparkSession
      ): DataFrame = df
  }

  class PairFilter[T, Conf <: AppConfig](x: Filter[T, Conf], y: Filter[T, Conf])
      extends Filter[T, Conf] {
    override def filter(implicit config: Conf, env: Env, spark: SparkSession): T => Boolean = ???
    override def filterColumn(implicit config: Conf, env: Env, spark: SparkSession): Column = ???
    override def filter(
        ds: Dataset[T]
      )(
        implicit
        config: Conf,
        env: Env,
        spark: SparkSession
      ): Dataset[T] = {
      val f1: Dataset[T] => Dataset[T] = x.filter
      val f2: Dataset[T] => Dataset[T] = y.filter
      (f1 andThen f2)(ds)
    }
    override def filterAsDataFrame(
        df: DataFrame
      )(
        implicit
        config: Conf,
        env: Env,
        spark: SparkSession
      ): DataFrame = {
      val f1: DataFrame => DataFrame = x.filterAsDataFrame
      val f2: DataFrame => DataFrame = y.filterAsDataFrame
      (f1 andThen f2)(df)
    }
  }
}
