package com.github.spark.etl.examples.pipelines

import com.github.spark.etl.core.app.processor.CustomEncoders.customEncoder
import com.github.spark.etl.core.app.processor.ScenarioCase.CompareScenarioCase
import com.github.spark.etl.core.app.processor.{ScenarioCase, TransformPipeline, TransformPipelineTestUtils, Writer}
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import com.github.spark.etl.examples.utils.TimeUtils
import pureconfig.generic.auto._
import monocle.macros.syntax.lens._
import com.github.spark.etl.examples.utils.TestUtils

import java.sql.Timestamp
import java.time.LocalDateTime

class SalesPipelineSpec
extends TransformPipelineTestUtils[RawSale, Sale, SalesConfig] with TestUtils {
  override lazy val deleteBeforeEachCase: Boolean = true

  override lazy val pipeline: TransformPipeline[RawSale, Sale, SalesConfig] =
    new SalesPipeline

  override lazy val writeSource: Writer[RawSale] =
    Writer.DefaultParquetWriter()

  override lazy val scenarioCases: List[ScenarioCase[Seq[RawSale], Seq[Sale], SalesConfig]] =
    List(
      CompareScenarioCase(
        input =
          defaultRawSale ::
          defaultRawSale
            .lens(_.timestamp_str)
            .set(TimeUtils.toString(TimeUtils.atStartOfDay(LocalDateTime.now))) :: Nil,
        expected =
            defaultSale
              .lens(_.timestamp)
              .set(Timestamp.valueOf(TimeUtils.atStartOfDay(LocalDateTime.now))) :: Nil,
        configuration = config,
        caseTitle = "read defaultRawSale source and tranform it into a defaultSale",
        caseDescription = "read defaultRawSale source and tranform it into a defaultSale"
      )
    )
}
