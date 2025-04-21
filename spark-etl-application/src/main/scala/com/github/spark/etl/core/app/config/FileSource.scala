package com.github.spark.etl.core.app.config

import Area.{Bronze, Gold, Silver}
import better.files.File
import com.github.spark.etl.core.app.config.Env.Local
import Platform.S3Platform
import Env._
trait FileSource extends Datasource {
  def platform: Platform
  def sourceName: String
  def sourceType: String

  override def datasourceType: DatasourceType =
    DatasourceType.extractSourceType(sourceType)

  def uri(implicit env: Env): String = env.value match {
    case Local =>
      val subdirectory: File = File("./data/files/")
      subdirectory.createDirectories()
      (subdirectory / sourceName).pathAsString
    case _ => s"${platform.relativeUri}/$sourceName"
  }
}

object FileSource {
  trait AbstractAreaDatasource[A <: Area] extends FileSource {
    def area: A

    override def uri(implicit env: Env): String = env.value match {
      case Local =>
        val subdirectory: File = File("./data/files/")
        subdirectory.createDirectories()
        (subdirectory / area.relativeUri / sourceName).pathAsString
      case _ => s"${platform.relativeUri}/${area.relativeUri}/$sourceName"
    }
  }

  final case class GoldAreaDatasource(
      platform: S3Platform,
      area: Gold,
      sourceName: String,
      sourceType: String)
      extends AbstractAreaDatasource[Gold]

  final case class SilverAreaDatasource(
      platform: S3Platform,
      area: Silver,
      sourceName: String,
      sourceType: String)
      extends AbstractAreaDatasource[Silver]

  final case class BronzeAreaDatasource(
      platform: S3Platform,
      area: Bronze,
      sourceName: String,
      sourceType: String)
      extends AbstractAreaDatasource[Bronze]

  final case class ExternalDatasource(platform: S3Platform, sourceName: String, sourceType: String)
      extends FileSource {
    def bucket(implicit env: Env): String = platform.bucketName
    def key: String = platform.path match {
      case Some(path) => s"$path/$sourceName"
      case None       => s"$sourceName"
    }
  }
}
