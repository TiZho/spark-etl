package com.github.spark.etl.examples.transformers

import com.github.spark.etl.core.app.processor.CustomEncoders.customEncoder
import com.github.spark.etl.core.app.processor.ScenarioCase
import com.github.spark.etl.core.app.processor.ScenarioCase.CompareScenarioCase
import com.github.spark.etl.core.app.processor.TransformerTestUtils.MappingTransformerTestUtils
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import pureconfig.generic.auto._
import com.github.spark.etl.examples.utils.TestUtils

class RawSaleToSalesTransformerSpec
extends MappingTransformerTestUtils[RawSale, Sale, SalesConfig](new RawSaleToSalesTransformer) with TestUtils{
  override lazy val scenarioCases: List[ScenarioCase[Seq[RawSale], Seq[Sale], SalesConfig]] =
    List(
      CompareScenarioCase(
        input = defaultRawSale :: Nil,
        expected = defaultSale :: Nil,
        configuration = config,
        caseTitle = "map a defaultRawSale into a defaultSale",
        caseDescription = "All fields should be equal"
      )
    )
}
