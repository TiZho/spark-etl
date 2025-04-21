package com.github.spark.etl.examples.filters

import com.github.spark.etl.core.app.processor.CustomEncoders._
import com.github.spark.etl.core.app.processor.ScenarioCase.CompareScenarioCase
import com.github.spark.etl.core.app.processor.{FilterTestUtils, ScenarioCase}
import com.github.spark.etl.examples.config.SalesConfig
import com.github.spark.etl.examples.domain.Sale
import com.github.spark.etl.examples.utils.TimeUtils
import pureconfig.generic.auto._
import monocle.macros.syntax.lens._
import com.github.spark.etl.examples.utils.TestUtils

import java.sql.Timestamp
import java.time.LocalDateTime

class SalesFilterSpec
  extends FilterTestUtils[Sale, SalesConfig](new SaleFilter) with TestUtils {
  override lazy val scenarioCases: List[ScenarioCase[Seq[Sale], Seq[Sale], SalesConfig]] =
    List(
      CompareScenarioCase(
        input =
          defaultSale ::
            defaultSale
              .lens(_.timestamp)
              .set(Timestamp.valueOf(TimeUtils.atStartOfDay(LocalDateTime.now))) :: Nil,
        expected =
          defaultSale
          .lens(_.timestamp)
          .set(Timestamp.valueOf(TimeUtils.atStartOfDay(LocalDateTime.now))) :: Nil,
        configuration = config,
        caseTitle = "filter a list of sales according their timestamp",
        caseDescription = "filter a list of sales according their timestamp"
      )
    )
}
