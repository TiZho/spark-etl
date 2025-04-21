package com.github.spark.etl.examples.processors

import com.github.spark.etl.core.app.processor.CustomEncoders.customEncoder
import com.github.spark.etl.core.app.processor.{Filter, TransformPipeline, TransformProcessor, Transformer, Writer}
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import com.github.spark.etl.examples.pipelines.SalesPipeline

class SalesProcessor(implicit config: SalesConfig) extends TransformProcessor[RawSale, Sale, SalesConfig]{
  lazy val pipeline: TransformPipeline[RawSale, Sale, SalesConfig] =
    new SalesPipeline()

  lazy val postTransformer: Transformer[Sale, Sale] =
    Transformer.Identity()

  lazy val postFilter: Filter[Sale, SalesConfig] =
    Filter.Identity()

  lazy val writer: Writer[Sale] =
    Writer.DefaultParquetWriter()
}
