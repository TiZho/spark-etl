package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig, DatabaseSource, Datasource, FileSource}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, Row}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import pureconfig.ConfigReader

import scala.reflect.runtime.universe._
import ScenarioCase._
import better.files.{File, Resource}
import com.github.spark.etl.core.app.config.{AbstractConfiguration, AppConfig, DatabaseSource, Datasource, FileSource}
import com.github.spark.etl.core.app.processor.TransformPipeline.{EnrichedJoinableTransformPipeline, JoinedTransformPipeline}
import com.github.spark.etl.core.app.utils.ScalaReflectUtils
import com.github.spark.etl.core.app.processor.TransformPipeline.{EnrichedJoinableTransformPipeline, JoinedTransformPipeline}

import scala.collection.JavaConverters._
import org.apache.spark.sql.types.StructType
import org.scalatest.Assertion

import java.sql.{Connection, DriverManager, Statement}
import scala.collection.Seq
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

abstract class TransformProcessorTestUtils[
    I <: Product: Encoder: TypeTag,
    O <: Product: Encoder: TypeTag: ClassTag,
    Conf <: AppConfig: ConfigReader: ClassTag
  ](
    implicit
    descriptors: Seq[StorableDescriptor[_ <: Product]])
    extends AnyFlatSpec
    with Matchers
    with LazyLogging
    with SparkTestUtils
    with LocalSparkSession {

  type SourceMapping = Seq[(Datasource, Seq[_])]
  def deleteBeforeEachCase: Boolean
  def processor: TransformProcessor[I, O, Conf]
  def loadDest: Loader[O]
  def customWriters: Map[String, Writer[_]] = Map.empty[String, Writer[_]]
  def timestampsAutoCopy: Boolean           = true
  def scenarioCases: List[ScenarioCase[SourceMapping, Seq[O], Conf]]

  implicit lazy val config: Conf = AbstractConfiguration.extractConfig[Conf]
  import config._

  lazy val writers: Map[Datasource, (Writer[_], StorableDescriptor[_ <: Product])] =
    findWriters(processor.pipeline)
  lazy val sources: List[Datasource] = writers.keys.toList
  lazy val input: Datasource         = processor.pipeline.source
  lazy val output: Datasource        = app.dest.output

  initPreProcess()

  sources.foreach { src =>
    deleteSourceIfExists(src)
    initSourceIfNotExists(src)
  }

  deleteSourceIfExists(output)
  // Dynamically generate test cases using reflection
  scenarioCases.foreach { testCase =>
    testCase.caseTitle should s"should ${testCase.caseDescription}" in {
      if (deleteBeforeEachCase) { deleteSourceIfExists(output) }
      val input: SourceMapping = testCase.input
      val compare: Try[(List[O], List[O])] = for {
        resultDS <- testTransformers(input).toTry
        _        <- Try(deleteSourceIfExists(output))
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
    )(scenarioCase: ScenarioCase[_, Seq[O], Conf]
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
    )(scenarioCase: ScenarioCase[_, Seq[O], Conf]
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

  protected def saveSources(sourceMapping: SourceMapping): Unit =
    sourceMapping.foreach { case (source, input) => saveSource(source, input) }

  protected def saveSource(source: Datasource, input: Seq[_]): Unit =
    Try {
      val (writer, descriptor): (Writer[_], StorableDescriptor[_ <: Product]) =
        writers.getOrElse(
          source,
          throw new IllegalArgumentException(s"Can not find writer for source $source")
        )
      saveSource(source, input, writer, descriptor)
    } match {
      case Success(_) => logger.info(s"Saving data into source $source")
      case Failure(err) =>
        logger.warn(s"Can not save into source $source")
        throw err
    }

  protected def saveSource(
      source: Datasource,
      input: Seq[_],
      writer: Writer[_],
      descriptor: StorableDescriptor[_]
    ): Unit = {
    val schema: StructType = descriptor.schema
    val list: List[Row]    = convertListToRows(input.toList, schema)
    val df                 = spark.createDataFrame(list.asJava, schema)
    writer.saveAsDataFrame(source, df)(spark, config.app.env)
  }

  protected def testTransformers(map: SourceMapping): Either[Throwable, List[O]] =
    Try {
      sources.foreach(deleteSourceIfExists)
      saveSources(map)
      processor.processRun(input, output, app.debugMode)(spark, env, config)
      val ds: Dataset[O] = loadDest.load(output)
      ds.collect.toList
    }.toEither

  protected def testTransformersAsDataFrame(map: SourceMapping): Either[Throwable, List[O]] =
    Try {
      import spark.implicits._
      sources.foreach(deleteSourceIfExists)
      saveSources(map)
      processor.processRunAsDataframe(input, output, app.debugMode)(spark, env, config)
      val df: DataFrame = loadDest.loadAsDataFrame(output)
      df.as[O].collect.toList
    }.toEither

  protected def initPreProcess(): Unit = {}

  protected def deleteSourceIfExists(source: Datasource): Unit =
    source match {
      case src: DatabaseSource => processDeleteDatabaseSource(src)
      case src: FileSource     => processDeleteFileDatasource(src)
      case src                 => logger.warn(s"this kind of source is not supported $src")
    }

  protected def initSourceIfNotExists(source: Datasource): Unit =
    source match {
      case src: DatabaseSource => processInitDatabaseSource(src)
      case _                   => logger.info(s"No database to ignore")
    }

  protected def processDeleteDatabaseSource(source: DatabaseSource): Unit =
    Try {
      val protocol = s"jdbc:${source.databaseType}:"
      val filepath = source.uri.split(protocol)(1)
      val f        = File(filepath)
      if (f.exists) f.parent.delete()
    } match {
      case Failure(err) => logger.info(s"Can not delete database $source: $err")
      case Success(_)   => logger.info(s"Database deleted")
    }

  protected def processDeleteFileDatasource(source: FileSource): Unit =
    Try {
      val filepath = source.uri
      File(filepath).parent.delete(true)
    } match {
      case Failure(err) => logger.info(s"Can not delete source $source: $err")
      case Success(_)   => logger.info(s"Source deleted")
    }

  protected def processInitDatabaseSource(source: DatabaseSource): Unit =
    Try {
      // Register the SQLite JDBC driver (make sure the JDBC driver is in your classpath)
      Class.forName(source.driverType)
      // Establish a JDBC connection
      val connection: Connection = DriverManager.getConnection(source.uri)
      // Create a Statement object
      val statement: Statement = connection.createStatement()
      // Read the SQL script from the resource file
      val createTableScript: String = Resource.getAsString(s"init_${source.tablename}.sql")
      // Execute the SQL script
      statement.execute(createTableScript)
      // Close resources
      statement.close()
      connection.close()
      logger.info(s"${source.tablename} table created successfully")
    } match {
      case Failure(err) => logger.info(s"Can not initialize database $err")
      case Success(_)   => logger.info(s"Database initilized")
    }

  protected def findWriters(
      tp: TransformPipeline[_, _, _]
    )(
      implicit
      descriptors: Seq[StorableDescriptor[_ <: Product]]
    ): Map[Datasource, (Writer[_], StorableDescriptor[_ <: Product])] =
    tp match {
      case p: EnrichedJoinableTransformPipeline[_, _, _, _, _, _] =>
        findWriters(p.y) + extractWriter(p.x)
      case p: JoinedTransformPipeline[_, _, _, _, _] =>
        findWriters(p.y) + extractWriter(p.x)
      case p =>
        Map.empty[Datasource, (Writer[_], StorableDescriptor[_ <: Product])] + extractWriter(p)
    }

  protected def extractWriter(
      tp: TransformPipeline[_, _, _]
    )(
      implicit
      customDescriptors: Seq[StorableDescriptor[_ <: Product]]
    ): (Datasource, (Writer[_], StorableDescriptor[_ <: Product])) = {
    val inputClassName: String = tp.inputClass.runtimeClass.getCanonicalName
    val descriptor: StorableDescriptor[_ <: Product] = customDescriptors
      .find(_.typetag.tpe.toString == inputClassName)
      .getOrElse(
        throw new IllegalArgumentException(
          s"Can not find a descriptor for source class $inputClassName"
        )
      )
    val writer: Writer[_] =
      Writer
        .extractWriter(tp.source, descriptor)
        .orElse(findCustomWriter(tp.source))
        .getOrElse(
          throw new IllegalArgumentException(s"Can not find writer for transformDatasource $tp")
        )
    tp.source -> (writer -> descriptor)
  }

  protected def findCustomWriter(ds: Datasource): Option[Writer[_]] =
    customWriters.get(ds.datasourceType.name)

}
