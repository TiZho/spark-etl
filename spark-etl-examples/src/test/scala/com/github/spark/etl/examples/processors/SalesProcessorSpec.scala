package com.github.spark.etl.examples.processors

import com.github.spark.etl.core.app.processor.CustomEncoders.customEncoder
import com.github.spark.etl.core.app.processor.ScenarioCase.CompareScenarioCase
import com.github.spark.etl.core.app.processor.{Loader, ScenarioCase, TransformProcessor, TransformProcessorTestUtils}
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.{RawSale, Sale}
import com.github.spark.etl.examples.utils.TimeUtils
import com.github.spark.etl.examples.utils.StorableDescriptorImplicits._
import pureconfig.generic.auto._
import monocle.macros.syntax.lens._
import com.github.spark.etl.examples.utils.TestUtils

import java.sql.Timestamp
import java.time.LocalDateTime

class SalesProcessorSpec
 extends TransformProcessorTestUtils[RawSale, Sale, SalesConfig] with TestUtils {
  override lazy val deleteBeforeEachCase: Boolean = true

  override lazy val processor: TransformProcessor[RawSale, Sale, SalesConfig] =
    new SalesProcessor

  override lazy val loadDest: Loader[Sale] =
    Loader.DefaultParquetLoader()

  override lazy val scenarioCases: List[ScenarioCase[SourceMapping, Seq[Sale], SalesConfig]] =
    List(
      CompareScenarioCase(
        input =
          (config.app.source.input,
          defaultRawSale ::
            defaultRawSale
              .lens(_.timestamp_str)
              .set(TimeUtils.toString(TimeUtils.atStartOfDay(LocalDateTime.now))) :: Nil
          ) :: Nil,
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
