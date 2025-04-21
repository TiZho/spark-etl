package com.github.spark.etl.core.app.processor

import com.github.spark.etl.core.app.config.{AppConfig, Datasource, Env}
import com.github.spark.etl.core.app.config.Datasource
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.sql._

import scala.reflect.ClassTag

abstract class TransformProcessor[
    I <: Product: Encoder: ClassTag,
    O <: Product: Encoder: ClassTag,
    Conf <: AppConfig]
    extends AbstractProcessor[Conf]
    with LazyLogging {
  def pipeline: TransformPipeline[I, O, Conf]
  def postTransformer: Transformer[O, O]
  def postFilter: Filter[O, Conf]
  def writer: Writer[O]

  override def processRun(
      source: Datasource,
      dest: Datasource,
      debugMode: Boolean
    )(
      implicit
      spark: SparkSession,
      env: Env,
      config: Conf
    ): Unit = {
    val inputSource = pipeline.source
    logger.info(s"Applying transform on input source: ${inputSource.uri}")
    val outputDs: Dataset[O] = pipeline.process

    logger.info("Applying post transform on resulting transformSource")
    val postTransfomedDs: Dataset[O] = postTransformer.transform(outputDs)

    logger.info("Applying post filter on resulting final dataset")
    val postFilteredDs: Dataset[O] = postFilter.filter(postTransfomedDs)

    logger.info(s"Writing into output: ${dest.uri}")
    writer.save(dest, postFilteredDs)
  }

  override def processRunAsDataframe(
      source: Datasource,
      dest: Datasource,
      debugMode: Boolean
    )(
      implicit
      spark: SparkSession,
      env: Env,
      config: Conf
    ): Unit = {
    val inputSource = pipeline.source
    logger.info(s"Applying transform on input source: ${inputSource.uri}")
    val outputDf: DataFrame = pipeline.processAsDataframe

    logger.info("Applying post transform on resulting transformSource")
    val postTransfomedDf: DataFrame = postTransformer.transformAsDataFrame(outputDf)

    logger.info("Applying post filter on resulting final dataset")
    val postFilteredDf: DataFrame = postFilter.filterAsDataFrame(postTransfomedDf)

    logger.info(s"Writing into output: ${dest.uri}")
    writer.saveAsDataFrame(dest, postFilteredDf)
  }
}

object TransformProcessor {

  abstract class TransformProcessorController[O <: Product: Encoder: ClassTag, Conf <: AppConfig] {
    def controlFilter: Filter[O, Conf]
    def buildException: String => Throwable
    def errorMessage: String
    def errorCause: O => String
  }

  abstract class ControlTransformProcessor[
      I <: Product: Encoder: ClassTag,
      O <: Product: Encoder: ClassTag,
      Conf <: AppConfig]
      extends TransformProcessor[I, O, Conf]
      with LazyLogging {
    def controller: TransformProcessorController[O, Conf]

    override def processRun(
        source: Datasource,
        dest: Datasource,
        debugMode: Boolean
      )(
        implicit
        spark: SparkSession,
        env: Env,
        config: Conf
      ): Unit = {
      val inputSource = pipeline.source
      logger.info(s"Applying transform on input source: ${inputSource.uri}")
      val outputDs: Dataset[O] = pipeline.process

      logger.info("Applying post transform on resulting transformSource")
      val postTransfomedDs: Dataset[O] = postTransformer.transform(outputDs)

      logger.info("Applying post filter on resulting final dataset")
      val postFilteredDs: Dataset[O] = postFilter.filter(postTransfomedDs).cache()

      logger.info(s"Writing into output: ${dest.uri}")
      writer.save(dest, postFilteredDs)

      logger.info(s"Control the output of resulting final dataset")
      val controlDs: Dataset[O] = controller.controlFilter.filter(postFilteredDs)
      if (!controlDs.isEmpty) {
        val cause   = controller.errorCause(controlDs.as[O].head)
        val message = controller.errorMessage
        throw controller.buildException(s"$message: $cause")
      }
    }

    override def processRunAsDataframe(
        source: Datasource,
        dest: Datasource,
        debugMode: Boolean
      )(
        implicit
        spark: SparkSession,
        env: Env,
        config: Conf
      ): Unit = {
      val inputSource = pipeline.source
      logger.info(s"Applying transform on input source: ${inputSource.uri}")
      val outputDf: DataFrame = pipeline.processAsDataframe

      logger.info("Applying post transform on resulting transformSource")
      val postTransfomedDf: DataFrame = postTransformer.transformAsDataFrame(outputDf)

      logger.info("Applying post filter on resulting final dataframe")
      val postFilteredDf: DataFrame = postFilter.filterAsDataFrame(postTransfomedDf).cache()

      logger.info(s"Writing into output: ${dest.uri}")
      writer.saveAsDataFrame(dest, postFilteredDf)

      logger.info(s"Control the output of resulting final dataframe")
      val controlDf: DataFrame = controller.controlFilter.filterAsDataFrame(postFilteredDf)
      if (!controlDf.isEmpty) {
        val cause   = controller.errorCause(controlDf.as[O].head)
        val message = controller.errorMessage
        throw controller.buildException(s"$message: $cause")
      }
    }
  }

}
