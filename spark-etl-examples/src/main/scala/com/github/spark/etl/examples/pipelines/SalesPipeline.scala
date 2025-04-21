package com.github.spark.etl.examples.pipelines

import com.github.spark.etl.core.app.processor.CustomEncoders._
import com.github.spark.etl.core.app.processor.{Filter, Loader, TransformPipeline, Transformer}
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import com.github.spark.etl.examples.filters.SaleFilter
import com.github.spark.etl.examples.transformers.RawSaleToSalesTransformer

class SalesPipeline(implicit config: SalesConfig)
 extends TransformPipeline[RawSale, Sale, SalesConfig](
   config.app.source.input
 ){
  override lazy val loader: Loader[RawSale] =
    Loader.DefaultParquetLoader()

  override lazy val preFilter: Filter[RawSale, SalesConfig] =
    Filter.Identity()

  override lazy val transformer: Transformer[RawSale, Sale] =
    new RawSaleToSalesTransformer()

  override lazy val postFilter: Filter[Sale, SalesConfig] =
    new SaleFilter()
}
