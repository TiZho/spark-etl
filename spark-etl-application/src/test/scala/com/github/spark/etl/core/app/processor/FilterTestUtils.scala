package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig}
import com.github.spark.etl.core.app.processor.ScenarioCase.{CheckScenarioCase, CompareScenarioCase, ErrorScenarioCase}
import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig}
import ScenarioCase._
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.Encoder
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigReader

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

abstract class FilterTestUtils[T: Encoder, Conf <: AppConfig: ConfigReader: ClassTag](
    f: Filter[T, Conf])
    extends AnyFlatSpec
    with Matchers
    with LazyLogging
    with LocalSparkSession {

  lazy val config: Conf            = AbstractConfiguration.extractConfig[Conf]
  lazy val filter: Filter[T, Conf] = f

  // A list containing all tests cases to execute
  def scenarioCases: List[ScenarioCase[Seq[T], Seq[T], Conf]]

  initSourceIfNotExists()

  // Dynamically generate test cases using reflection
  scenarioCases.foreach { testCase =>
    testCase.caseTitle should s"should ${testCase.caseDescription}" in {
      val input: List[T] = testCase.input.toList
      val compare: Try[(List[T], List[T])] = for {
        resultDS <- testFilters(input).toTry
        resultDF <- testFilterColumns(input).toTry
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
      listDS: Seq[T]
    )(scenarioCase: ScenarioCase[Seq[T], Seq[T], Conf]
    ): Assertion =
    scenarioCase match {
      case sc: CheckScenarioCase[_, _, _] =>
        logger.info(s"Applying check function on value $listDS")
        val fun = sc.checkCondition.asInstanceOf[Seq[T] => Boolean]
        assert(fun(listDS))
      case sc: CompareScenarioCase[_, _, _] =>
        logger.info(
          s"Comparing scenario case between result $listDS with ${sc.expected.asInstanceOf[Seq[T]]}"
        )
        listDS should contain theSameElementsAs sc.expected.asInstanceOf[Seq[T]]
      case sc: ErrorScenarioCase[_, _, _, _] =>
        val errorType    = sc.returningError.getClass.getCanonicalName
        val errorMessage = s"An error ${errorType} was expected but no error was found"
        logger.warn(errorMessage, listDS)
        throw new org.scalatest.exceptions.TestFailedException(errorMessage, 0)
    }

  private def checkErrorScenarioCase(
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

  private def testFilters(list: List[T]): Either[Throwable, List[T]] =
    Try {
      import spark.implicits._
      val ds         = list.toDS
      val filteredDs = filter.filter(ds)(config, env, spark)
      filteredDs.collect.toList
    }.toEither

  private def testFilterColumns(list: List[T]): Either[Throwable, List[T]] =
    Try {
      import spark.implicits._
      val df         = list.toDF
      val filteredDf = filter.filterAsDataFrame(df)(config, env, spark)
      val res        = filteredDf.as[T]
      res.collect.toList
    }.toEither

  protected def initSourceIfNotExists(): Unit = {}
}
