package com.github.spark.etl.core.app.processor

import TransformPipeline.{EnrichedJoinableTransformPipeline, JoinableSource, JoinedTransformPipeline}
import com.github.spark.etl.core.app.config.AppConfig
import org.apache.spark.sql.Encoder

import scala.reflect.ClassTag

object TransfomDatasourceImplicits {
  implicit class JoinDatasources[
      I1 <: Product: Encoder: ClassTag,
      O1 <: Product: Encoder: ClassTag,
      Conf <: AppConfig
    ](x: TransformPipeline[I1, O1, Conf]) {
    def ~>[I2 <: Product: Encoder: ClassTag, O2 <: Product: Encoder: ClassTag](
        y: TransformPipeline[I2, O2, Conf] with JoinableSource[O1, O2]
      ) =
      new JoinedTransformPipeline[I1, I2, O1, O2, Conf](x, y)

  }

  implicit class JoinJoinableDatasources[
      I1 <: Product: Encoder: ClassTag,
      O1 <: Product: Encoder: ClassTag,
      O <: Product: Encoder: ClassTag,
      Conf <: AppConfig
    ](x: TransformPipeline[I1, O1, Conf] with JoinableSource[O, O1]) {
    def ~>[I2 <: Product: Encoder: ClassTag, O2 <: Product: Encoder: ClassTag](
        y: TransformPipeline[I2, O2, Conf] with JoinableSource[O1, O2]
      ) =
      new EnrichedJoinableTransformPipeline[I1, I2, O1, O2, O, Conf](x, y)
  }
}
