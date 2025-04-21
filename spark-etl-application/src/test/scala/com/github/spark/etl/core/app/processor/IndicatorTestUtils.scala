package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig}
import com.github.spark.etl.core.app.processor.ScenarioCase.{CheckScenarioCase, CompareScenarioCase, ErrorScenarioCase}
import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig}
import IndicatorTestUtils.IndicatorScenarioCase
import com.github.spark.etl.core.app.processor.ScenarioCase.{CheckScenarioCase, CompareScenarioCase, ErrorScenarioCase}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Encoder
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigReader

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

abstract class IndicatorTestUtils[T: Encoder, Conf <: AppConfig: ConfigReader: ClassTag]
    extends AnyFlatSpec
    with Matchers
    with LazyLogging
    with LocalSparkSession {

  lazy val config: Conf = AbstractConfiguration.extractConfig[Conf]
  def scenarioCases: List[IndicatorScenarioCase[T, Conf]]

  // Dynamically generate test cases using reflection
  scenarioCases.foreach { case IndicatorScenarioCase(indicator, testCase) =>
    testCase.caseTitle should s"${testCase.caseDescription}" in {
      val input: List[T] = testCase.input.toList
      val compare: Try[(List[T], List[T])] = for {
        resultDF <- testIndicatorColumns(input)(indicator).toTry
        resultDS <- testIndicators(input)(indicator).toTry
      } yield (resultDF, resultDS)
      logger.info(s"Preparing to assert resulting lists: $compare")
      compare match {
        case Success((listDF, listDS)) =>
          checkScenarioCase(listDS)(testCase)
          logger.info(s"Checking if transform and transformAsDataFrame are equal")
          logger.info(s"transform", listDS)
          logger.info(s"transformAsDataFrame", listDF)
          listDF should contain theSameElementsAs listDS
        case Failure(err) =>
          checkErrorScenarioCase(err)(testCase)
      }
    }
  }

  private def checkScenarioCase(
      result: List[T]
    )(scenarioCase: ScenarioCase[Seq[T], Seq[T], Conf]
    ): Assertion =
    scenarioCase match {
      case sc: CheckScenarioCase[_, _, _] =>
        logger.info(s"Applying check function on value $result")
        val fun = sc.checkCondition.asInstanceOf[List[T] => Boolean]
        assert(fun(result))
      case sc: CompareScenarioCase[_, _, _] =>
        logger.info(
          s"Comparing scenario case between result $result with ${sc.expected.asInstanceOf[T]}"
        )
        result shouldEqual sc.expected
      case sc: ErrorScenarioCase[_, _, _, _] =>
        val errorType    = sc.returningError.getClass.getCanonicalName
        val errorMessage = s"An error ${errorType} was expected but no error was found"
        logger.warn(errorMessage, result)
        throw new org.scalatest.exceptions.TestFailedException(errorMessage, 0)
    }

  private def checkErrorScenarioCase[B](
      err: Throwable
    )(scenarioCase: ScenarioCase[Seq[T], Seq[T], Conf]
    ): Assertion = scenarioCase match {
    case sc: ErrorScenarioCase[_, _, _, _] =>
      logger.info(sc.errorMessage)
      err.getClass shouldEqual sc.returningError.getClass
    case _: CheckScenarioCase[_, _, _] =>
      val errorMessage =
        s"Expecting checking output result but an exception was found: ${err.getMessage}"
      logger.warn(errorMessage, err)
      throw new org.scalatest.exceptions.TestFailedException(errorMessage, 0)
    case sc: CompareScenarioCase[_, _, _] =>
      val errorMessage =
        s"Expected value ${sc.expected} but an exception was found: ${err.getMessage}"
      logger.warn(errorMessage, err)
      throw new org.scalatest.exceptions.TestFailedException(errorMessage, 0)
  }

  private def testIndicators(
      list: List[T]
    )(indicator: Indicator[T, _]
    ): Either[Throwable, List[T]] =
    Try {
      import spark.implicits._
      val ds        = list.toDS
      val field     = indicator.field
      val computeDs = ds.mapPartitions(it => it.map(field.processCompute))
      computeDs.collect.toList
    }.toEither

  private def testIndicatorColumns(
      list: List[T]
    )(indicator: Indicator[T, _]
    ): Either[Throwable, List[T]] =
    Try {
      import spark.implicits._
      val df        = list.toDF
      val column    = indicator.column
      val computeDf = df.withColumn(column.computeColumn.name, column.computeColumn.value)
      val res       = computeDf.as[T]
      res.collect.toList
    }.toEither
}

object IndicatorTestUtils {
  final case class IndicatorScenarioCase[T, Conf <: AppConfig](
      indicator: Indicator[T, _],
      scenarioCase: ScenarioCase[Seq[T], Seq[T], Conf])
}
