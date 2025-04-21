package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig, DatabaseSource, Env, FileSource}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigReader
import ScenarioCase._
import org.scalatest.Assertion

import scala.reflect.runtime.universe._
import java.sql.{Connection, DriverManager, Statement}
import better.files._
import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig, DatabaseSource, Env, FileSource}
import com.github.spark.etl.core.app.utils.ScalaReflectUtils

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

abstract class TransformPipelineTestUtils[
    I <: Product: Encoder,
    O <: Product: Encoder: ClassTag: TypeTag,
    Conf <: AppConfig: ConfigReader: ClassTag]
    extends AnyFlatSpec
    with Matchers
    with LazyLogging
    with LocalSparkSession {

  lazy implicit val config: Conf = AbstractConfiguration.extractConfig[Conf]

  def deleteBeforeEachCase: Boolean
  def pipeline: TransformPipeline[I, O, Conf]
  def writeSource: Writer[I]
  def scenarioCases: List[ScenarioCase[Seq[I], Seq[O], Conf]]
  def timestampsAutoCopy: Boolean = true

  deleteSourceIfExists()
  initSourceIfNotExists()

  // Dynamically generate test cases using reflection
  scenarioCases.foreach { testCase =>
    testCase.caseTitle should s"should ${testCase.caseDescription}" in {
      if (deleteBeforeEachCase) { deleteSourceIfExists() }
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
      deleteSourceIfExists()
      val inputDs = list.toDS()
      writeSource.save(pipeline.source, inputDs)
      val result: Dataset[O] = pipeline.process(spark, config, env)
      result.collect.toList
    }.toEither

  protected def testTransformersAsDataFrame(list: List[I]): Either[Throwable, List[O]] =
    Try {
      import spark.implicits._
      deleteSourceIfExists()
      val inputDs = list.toDS()
      writeSource.save(pipeline.source, inputDs)
      val result: DataFrame = pipeline.processAsDataframe(spark, env, config)
      result.as[O].collect.toList
    }.toEither

  private def deleteSourceIfExists(): Unit =
    pipeline.source match {
      case src: DatabaseSource => processDeleteDatabaseSource(src)
      case src: FileSource     => processDeleteFileDatasource(src)
      case src                 => logger.warn(s"this kind of source is not supported $src")
    }

  protected def initSourceIfNotExists(): Unit =
    pipeline.source match {
      case src: DatabaseSource => processInitDatabaseSource(src)
      case _                   => logger.info(s"No database to ignore")
    }

  private def processDeleteDatabaseSource(src: DatabaseSource): Unit =
    Try {
      val protocol = s"jdbc:${src.databaseType}:"
      val filepath = src.uri.split(protocol)(1)
      File(filepath).parent.delete()
    } match {
      case Failure(err) => logger.info(s"Can not delete database $src: $err")
      case Success(_)   => logger.info(s"Database deleted")
    }

  private def processDeleteFileDatasource(src: FileSource): Unit =
    Try {
      val filepath = src.uri
      val f        = File(filepath)
      if (f.exists) f.parent.delete()
    } match {
      case Failure(err) => logger.info(s"Can not delete source $src: $err")
      case Success(_)   => logger.info(s"Source deleted")
    }

  private def processInitDatabaseSource(src: DatabaseSource): Unit =
    Try {
      // Register the SQLite JDBC driver (make sure the JDBC driver is in your classpath)
      Class.forName(src.driverType)
      // Establish a JDBC connection
      val connection: Connection = DriverManager.getConnection(src.uri)
      // Create a Statement object
      val statement: Statement = connection.createStatement()
      // Read the SQL script from the resource file
      val createTableScript: String = Resource.getAsString(s"init_${src.tablename}.sql")
      // Execute the SQL script
      statement.execute(createTableScript)
      // Close resources
      statement.close()
      connection.close()
      logger.info(s"${src.tablename} table created successfully")
    } match {
      case Failure(err) => logger.info(s"Can not initialize database $err")
      case Success(_)   => logger.info(s"Database initilized")
    }

}

object TransformPipelineTestUtils {
  class MockedTransformPipeline[
      I <: Product: Encoder: ClassTag,
      O <: Product: Encoder: ClassTag,
      Conf <: AppConfig
    ](elems: Seq[O])
      extends TransformPipeline[I, O, Conf](null) {
    override def loader: Loader[I]              = ???
    override def preFilter: Filter[I, Conf]     = ???
    override def postFilter: Filter[O, Conf]    = ???
    override def transformer: Transformer[I, O] = ???
    override def process(
        implicit
        sparkSession: SparkSession,
        config: Conf,
        env: Env
      ): Dataset[O] = {
      import sparkSession.implicits._
      elems.toDS
    }
    override def processAsDataframe(
        implicit
        spark: SparkSession,
        env: Env,
        config: Conf
      ): DataFrame = {
      import spark.implicits._
      elems.toDF
    }
  }
}
