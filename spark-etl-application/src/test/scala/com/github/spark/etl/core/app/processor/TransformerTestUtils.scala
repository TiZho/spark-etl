package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig}
import com.github.spark.etl.core.app.utils.ScalaReflectUtils
import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig}
import com.github.spark.etl.core.app.processor.ScenarioCase.{CheckScenarioCase, CompareScenarioCase, ErrorScenarioCase}
import Transformer.MappingTransformer
import com.typesafe.scalalogging.LazyLogging

import scala.reflect.runtime.universe._
import org.apache.spark.sql.Encoder
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigReader

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

abstract class TransformerTestUtils[
    I: Encoder,
    O: Encoder: ClassTag: TypeTag,
    Conf <: AppConfig: ConfigReader: ClassTag
  ](tf: Transformer[I, O])
    extends AnyFlatSpec
    with Matchers
    with LazyLogging
    with LocalSparkSession {

  lazy val transformer: Transformer[I, O] = tf
  lazy val config: Conf                   = AbstractConfiguration.extractConfig[Conf]

  def scenarioCases: List[ScenarioCase[Seq[I], Seq[O], Conf]]
  def timestampsAutoCopy: Boolean = true

  // Dynamically generate test cases using reflection
  scenarioCases.foreach { testCase =>
    testCase.caseTitle should s"should ${testCase.caseDescription}" in {
      val input: List[I] = testCase.input.toList
      val compare: Try[(List[O], List[O])] = for {
        resultDS <- testTransformers(input).toTry
        resultDF <- testTransformersAsDataFrame(input).toTry
      } yield (resultDF, resultDS)
      logger.info(s"Preparing to assert resulting lists: $compare")
      compare match {
        case Success((listDF, listDS)) =>
          if (timestampsAutoCopy && listDF.nonEmpty) {
            ScalaReflectUtils.copyFields(listDF.head, listDS)
          }
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
      listDS: Seq[O]
    )(scenarioCase: ScenarioCase[Seq[I], Seq[O], Conf]
    ): Assertion =
    scenarioCase match {
      case sc: CheckScenarioCase[_, _, _] =>
        logger.info(s"Applying check function on value $listDS")
        val fun = sc.checkCondition.asInstanceOf[Seq[O] => Boolean]
        assert(fun(listDS))
      case sc: CompareScenarioCase[_, _, _] =>
        logger.info(
          s"Comparing scenario case between result $listDS with ${sc.expected.asInstanceOf[Seq[O]]}"
        )
        val expected = sc.expected.asInstanceOf[Seq[O]].toList
        if (timestampsAutoCopy && listDS.nonEmpty) {
          ScalaReflectUtils.copyFields(listDS.head, expected)
        }
        listDS should contain theSameElementsAs expected
      case sc: ErrorScenarioCase[_, _, _, _] =>
        val errorType    = sc.returningError.getClass.getCanonicalName
        val errorMessage = s"An error ${errorType} was expected but no error was found"
        logger.warn(errorMessage, listDS)
        throw new org.scalatest.exceptions.TestFailedException(errorMessage, 0)
    }

  private def checkErrorScenarioCase(
      err: Throwable
    )(scenarioCase: ScenarioCase[Seq[I], Seq[O], Conf]
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

  protected def testTransformers(list: List[I]): Either[Throwable, List[O]] =
    Try {
      import spark.implicits._
      val ds          = list.toDS
      val transformDs = transformer.transform(ds)(spark, env)
      transformDs.collect.toList
    }.toEither

  protected def testTransformersAsDataFrame(list: List[I]): Either[Throwable, List[O]] =
    Try {
      import spark.implicits._
      val df          = list.toDF
      val transformDf = transformer.transformAsDataFrame(df)(spark, env)
      val res         = transformDf.as[O]
      res.collect.toList
    }.toEither
}

object TransformerTestUtils {
  abstract class MappingTransformerTestUtils[
      I: Encoder,
      O: Encoder: ClassTag: TypeTag,
      Conf <: AppConfig: ConfigReader: ClassTag
    ](tf: MappingTransformer[I, O])
      extends TransformerTestUtils[I, O, Conf](tf) {
    override lazy val transformer: MappingTransformer[I, O] = tf
    override def testTransformers(list: List[I]): Either[Throwable, List[O]] =
      Try(list.map(transformer.mapping)).toEither

    override def testTransformersAsDataFrame(list: List[I]): Either[Throwable, List[O]] =
      Try {
        import spark.implicits._
        val df          = list.toDF
        val transformDf = transformer.transformAsDataFrame(df)(spark, env)
        val res         = transformDf.as[O]
        res.collect.toList
      }.toEither
  }
}
